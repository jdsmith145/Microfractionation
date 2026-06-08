#!/usr/bin/env python3
"""CustomTkinter GUI for script 02 two-sided chromatogram plots."""
from __future__ import annotations

import json
import logging
import queue
import sys
import threading
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))
_SHARED_DIR = _THIS_DIR.parent
if str(_SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(_SHARED_DIR))

from shared.gui_help_popover import HelpPopoverController

try:
    import p_02_00_two_sided_plot_core as core
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "ERROR: Could not import p_02_00_two_sided_plot_core.py.\n"
        "Place this GUI script in the same folder as the core script.\n\n"
        f"Details: {exc}"
    )


def main() -> int:
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox
    except Exception as exc:
        print("ERROR: Tkinter is not available in this Python environment.", file=sys.stderr)
        print(f"Details: {exc}", file=sys.stderr)
        return 2

    try:
        import customtkinter as ctk
    except Exception as exc:
        print("ERROR: CustomTkinter is not installed. Install it with: pip install customtkinter", file=sys.stderr)
        print(f"Details: {exc}", file=sys.stderr)
        return 2

    ctk.set_appearance_mode("dark")
    ctk.set_default_color_theme("blue")

    colors = {
        "bg": "#17191d",
        "surface": "#20242a",
        "card": "#252a31",
        "card_alt": "#2d333c",
        "entry": "#191d22",
        "border": "#3d4652",
        "text": "#f3f6fa",
        "muted": "#aab4c0",
        "accent": "#2563eb",
        "accent_hover": "#3b82f6",
        "success": "#2f8f5b",
        "success_hover": "#37a96a",
        "warning": "#b7791f",
        "danger": "#b54848",
        "danger_hover": "#9e3b3b",
    }
    font_header = ("Segoe UI", 23, "bold")
    font_subtitle = ("Segoe UI", 12)
    font_card_title = ("Segoe UI", 15, "bold")
    font_label = ("Segoe UI", 12)
    font_small = ("Segoe UI", 11)
    font_mono = ("Consolas", 11)
    state_file = _THIS_DIR / ".p_02_01_two_sided_plot_gui_state.json"
    table_patterns = [("Tables", "*.csv *.tsv *.txt *.xlsx *.xls"), ("All files", "*.*")]
    mzml_patterns = [("mzML files", "*.mzML *.mzml"), ("All files", "*.*")]
    csv_patterns = [("CSV files", "*.csv"), ("All files", "*.*")]
    config_patterns = [("JSON configuration", "*.json"), ("All files", "*.*")]

    def load_json_safe(path: Path) -> dict[str, Any]:
        try:
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return {}

    def save_json_safe(path: Path, data: dict[str, Any]) -> None:
        try:
            path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    def split_csv(text: str) -> list[str]:
        return [piece.strip() for piece in text.split(",") if piece.strip()]

    def join_csv(values: list[str] | tuple[str, ...]) -> str:
        return ", ".join(str(value).strip() for value in values if str(value).strip())

    def split_paths(text: str) -> list[str]:
        normalized = text.replace("\n", ";").replace(",", ";")
        return [piece.strip().strip('"') for piece in normalized.split(";") if piece.strip().strip('"')]

    plate_scale_labels = {
        "Raw fluorescence (no scaling)": "none",
        "Percent of positive control": "positive_control_pct",
        "Relative range within each plate (0-100)": "positive_control_then_minmax_0_100",
    }
    display_mode_labels = {
        "Scale tallest signal to 100%": "percent_of_max",
        "Show signal values": "raw",
        "Show activity (low signal = high bar)": "inhibition_from_max",
    }
    automatic_activity_labels = {
        "",
        "Activity",
        "Bioactivity",
        "Normalized activity (%)",
        "Normalized bioactivity (%)",
    }

    def plate_scale_label(value: str) -> str:
        if value in {"positive_control_then_minmax_0_100", "minmax_0_100", "control_then_minmax_0_100"}:
            return "Relative range within each plate (0-100)"
        return next((label for label, code in plate_scale_labels.items() if code == value), value)

    def plate_scale_code(value: str) -> str:
        return plate_scale_labels.get(value, value)

    def display_mode_label(value: str) -> str:
        return next((label for label, code in display_mode_labels.items() if code == value), value)

    def display_mode_code(value: str) -> str:
        return display_mode_labels.get(value, value)

    def inferred_activity_label() -> str:
        display_code = display_mode_code(var_display_mode.get().strip()) or "raw"
        input_type = var_activity_input_type.get()
        if display_code == "inhibition_from_max":
            return "Inhibition from signal (%)"
        if display_code == "percent_of_max":
            return "Relative signal (% of maximum)"
        if input_type == "Plate reader":
            scale_code = plate_scale_code(var_plate_scale_mode.get().strip())
            if scale_code == "positive_control_pct":
                return "Fluorescence (% of positive control)"
            if scale_code in {"positive_control_then_minmax_0_100", "minmax_0_100", "control_then_minmax_0_100"}:
                return "Normalized fluorescence (0-100)"
            return "Raw fluorescence"
        if var_control_source.get() != "No control":
            return "Signal relative to control"
        return "Signal value"

    def resolved_activity_label() -> str:
        typed = var_activity_label.get().strip()
        if typed and typed not in automatic_activity_labels:
            return typed
        return inferred_activity_label()

    def refresh_activity_label_placeholder() -> None:
        if var_activity_label.get().strip() in automatic_activity_labels:
            var_activity_label.set("")

    app_state = load_json_safe(state_file)
    root = ctk.CTk()
    root.title("Two-sided fraction plot")
    root.geometry("1380x900")
    root.minsize(1220, 760)
    root.configure(fg_color=colors["bg"])
    help_popovers = HelpPopoverController(root, ctk, colors, font_small)

    # ---------------------------
    # Variables
    # ---------------------------
    var_status = tk.StringVar(value="Ready.")
    var_sample_name = tk.StringVar(value=str(app_state.get("sample_name", "")))
    var_mzml = tk.StringVar(value=str(app_state.get("chrom_mzml", "")))
    var_filtered = tk.StringVar(value=str(app_state.get("filtered_csv", "")))
    var_outdir = tk.StringVar(value=str(app_state.get("out_dir", str(_THIS_DIR / "outputs"))))
    var_output_preview = tk.StringVar(value="")
    var_rt_start = tk.StringVar(value=str(app_state.get("fraction_windows", {}).get("rt_start", 2.0)))
    var_fraction_width = tk.StringVar(value=str(app_state.get("fraction_windows", {}).get("fraction_width", 0.375)))
    var_n_fractions = tk.StringVar(value=str(app_state.get("fraction_windows", {}).get("n_fractions", 96)))

    activity_state = app_state.get("activity_table", {}) or {}
    var_activity_input_type = tk.StringVar(value="Plate reader" if str(activity_state.get("input_type", "table")).lower() == "plate_reader" else "Table")
    var_activity_path = tk.StringVar(value=str(activity_state.get("path", "")))
    var_sheet_name = tk.StringVar(value=str(activity_state.get("sheet_name", "")))
    var_fraction_column = tk.StringVar(value=str(activity_state.get("fraction_column", "fraction")))
    var_start_column = tk.StringVar(value=str(activity_state.get("start_column", "")))
    var_end_column = tk.StringVar(value=str(activity_state.get("end_column", "")))
    var_value_column = tk.StringVar(value=str(activity_state.get("value_column", "average")))
    var_replicate_columns = tk.StringVar(value=join_csv(activity_state.get("replicate_columns", [])))
    def initial_control_source(state: dict[str, Any]) -> str:
        if state.get("control_scalar_column"):
            return "Separate column"
        if state.get("control_mode") == "column_value":
            return "Named row"
        if state.get("control_mode") == "row_numbers" and state.get("control_row_indices"):
            return "Manual rows"
        return "No control"

    var_control_source = tk.StringVar(value=initial_control_source(activity_state))
    var_control_rows = tk.StringVar(value=join_csv([str(value) for value in activity_state.get("control_row_indices", [])]))
    var_control_column = tk.StringVar(value=str(activity_state.get("control_column", "")))
    var_control_value = tk.StringVar(value=str(activity_state.get("control_value", "")))
    var_control_scalar_column = tk.StringVar(value=str(activity_state.get("control_scalar_column", "")))
    var_exclude_control_rows = tk.BooleanVar(value=bool(activity_state.get("exclude_control_rows", True)))
    var_display_mode = tk.StringVar(value=display_mode_label(str(activity_state.get("display_mode", "percent_of_max"))))
    var_activity_label = tk.StringVar(value=str(activity_state.get("label", "Activity")))
    var_plate_files = tk.StringVar(value="; ".join(str(x) for x in (activity_state.get("plate_files") or [])) if not isinstance(activity_state.get("plate_files") or "", str) else str(activity_state.get("plate_files") or ""))
    var_plate_rows = tk.StringVar(value=str(activity_state.get("plate_rows", 8)))
    var_plate_columns = tk.StringVar(value=str(activity_state.get("plate_columns", 12)))
    var_plate_controls = tk.StringVar(value=str(activity_state.get("plate_positive_control_wells", "H11, H12")))
    var_plate_controls_by_file = tk.StringVar(value=str(activity_state.get("plate_positive_control_wells_by_file", "") or ""))
    var_plate_scale_mode = tk.StringVar(value=plate_scale_label(str(activity_state.get("plate_scale_mode", "none"))))
    refresh_activity_label_placeholder()

    plot_state = app_state.get("plot", {}) or {}
    style_state = plot_state.get("style", {}) or {}
    var_x_max = tk.StringVar(value="" if plot_state.get("x_max") is None else str(plot_state.get("x_max")))
    var_log_total = tk.BooleanVar(value=bool(plot_state.get("log_total_area", False)))
    var_save_svg = tk.BooleanVar(value=bool(plot_state.get("save_svg", True)))
    var_save_png = tk.BooleanVar(value=bool(plot_state.get("save_png", False)))
    var_title = tk.StringVar(value=str(style_state.get("title", "")))
    var_top_ylabel = tk.StringVar(value=str(style_state.get("top_y_label", "MS intensity (BPC)")))
    var_bottom_ylabel = tk.StringVar(value=str(style_state.get("bottom_y_label", "")))
    var_x_label = tk.StringVar(value=str(style_state.get("x_label", "Retention time [min]")))
    var_activity_ylabel = tk.StringVar(value=str(style_state.get("activity_y_label", "")))
    var_chrom_color = tk.StringVar(value=str(style_state.get("chrom_color", "#101b73")))
    var_dominant_color = tk.StringVar(value=str(style_state.get("dominant_feature_color", "#101b73")))
    var_remainder_color = tk.StringVar(value=str(style_state.get("remainder_feature_color", "#ddd6cc")))
    var_activity_color = tk.StringVar(value=str(style_state.get("activity_color", "#2f9e44")))
    var_activity_alpha = tk.StringVar(value=str(style_state.get("activity_alpha", 0.28)))
    var_width = tk.StringVar(value=str((style_state.get("figsize") or [14.0, 8.0])[0]))
    var_height = tk.StringVar(value=str((style_state.get("figsize") or [14.0, 8.0])[1]))

    activity_df: pd.DataFrame | None = None
    activity_columns: list[str] = []
    current_canvas: dict[str, Any] = {"canvas": None, "toolbar": None}
    figure_records: list[dict[str, Any]] = []
    log_queue: queue.Queue[str] = queue.Queue()
    result_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
    active_tab = {"name": "Workflow"}
    unread_log_events = {"count": 0}
    unread_figure_events = {"count": 0}
    var_log_badge = tk.StringVar(value="")
    var_figure_badge = tk.StringVar(value="")
    var_show_activity_math = tk.BooleanVar(value=False)

    class QueueLogHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                log_queue.put(self.format(record))
            except Exception:
                pass

    logger = logging.getLogger("two_sided_plot_core")
    logger.setLevel(logging.INFO)
    handler = QueueLogHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(handler)

    # ---------------------------
    # Helpers
    # ---------------------------
    def make_help(parent: Any, text: str) -> Any:
        return help_popovers.create_bubble(parent, text)

    def make_button(parent: Any, text: str, command: Callable[[], None], *, primary: bool = False, success: bool = False, danger: bool = False, width: int | None = None) -> Any:
        color = colors["success"] if success else colors["danger"] if danger else colors["accent"] if primary else colors["card_alt"]
        hover = colors["success_hover"] if success else colors["danger_hover"] if danger else colors["accent_hover"] if primary else "#39414c"
        return ctk.CTkButton(parent, text=text, command=command, width=width or 112, height=38, corner_radius=10, fg_color=color, hover_color=hover)

    def make_entry(parent: Any, var: tk.StringVar, placeholder: str = "") -> Any:
        return ctk.CTkEntry(parent, textvariable=var, placeholder_text=placeholder, fg_color=colors["entry"], border_color=colors["border"], text_color=colors["text"], height=36, corner_radius=8)

    def make_combo(parent: Any, var: tk.StringVar, values: list[str]) -> Any:
        return ctk.CTkComboBox(
            parent,
            variable=var,
            values=values or [""],
            fg_color=colors["entry"],
            border_color=colors["border"],
            button_color=colors["accent"],
            button_hover_color=colors["accent_hover"],
            dropdown_fg_color=colors["card"],
            dropdown_hover_color=colors["card_alt"],
            text_color=colors["text"],
            dropdown_text_color=colors["text"],
            height=36,
            corner_radius=8,
        )

    def make_checkbox(parent: Any, text: str, variable: tk.BooleanVar) -> Any:
        return ctk.CTkCheckBox(parent, text=text, variable=variable, font=font_small, text_color=colors["text"], fg_color=colors["accent"], hover_color=colors["accent_hover"], border_color=colors["border"], checkbox_width=20, checkbox_height=20)

    def labeled_widget(parent: Any, label: str, widget: Any, row: int, col: int, *, help_text: str, colspan: int = 1, padx_right: int = 18) -> None:
        label_frame = ctk.CTkFrame(parent, fg_color="transparent")
        label_frame.grid(row=row, column=col, sticky="ew", padx=(0, 10), pady=8)
        label_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(label_frame, text=label, font=font_label, text_color=colors["muted"], anchor="w").grid(row=0, column=0, sticky="w")
        make_help(label_frame, help_text).grid(row=0, column=1, sticky="e", padx=(6, 0))
        widget.grid(row=row, column=col + 1, columnspan=colspan, sticky="ew", padx=(0, padx_right), pady=8)

    def file_row(parent: Any, row: int, label: str, var: tk.StringVar, browse_cmd: Callable[[], None], help_text: str, *, load_cmd: Callable[[], None] | None = None) -> None:
        label_frame = ctk.CTkFrame(parent, fg_color="transparent")
        label_frame.grid(row=row, column=0, sticky="ew", padx=(0, 10), pady=8)
        label_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(label_frame, text=label, font=font_label, text_color=colors["muted"], anchor="w").grid(row=0, column=0, sticky="w")
        make_help(label_frame, help_text).grid(row=0, column=1, sticky="e", padx=(6, 0))
        make_entry(parent, var).grid(row=row, column=1, columnspan=3 if load_cmd else 4, sticky="ew", padx=(0, 10), pady=8)
        make_button(parent, "Browse", browse_cmd, width=88).grid(row=row, column=4 if load_cmd else 5, sticky="ew", padx=(0, 10) if load_cmd else 0, pady=8)
        if load_cmd:
            make_button(parent, "Load", load_cmd, primary=True, width=78).grid(row=row, column=5, sticky="ew", pady=8)

    def make_card(parent: Any, title: str) -> Any:
        card = ctk.CTkFrame(parent, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=16)
        card.pack(fill="x", padx=6, pady=(0, 12))
        card.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(card, text=title, font=font_card_title, text_color=colors["text"], anchor="w").grid(row=0, column=0, sticky="ew", padx=18, pady=(16, 10))
        body = ctk.CTkFrame(card, fg_color="transparent")
        body.grid(row=1, column=0, sticky="ew", padx=18, pady=(0, 18))
        for idx in range(6):
            body.grid_columnconfigure(idx, weight=1 if idx % 2 == 1 else 0)
        return body

    def show_toast(message: str, *, kind: str = "info") -> None:
        color = colors["success"] if kind == "success" else colors["danger"] if kind == "error" else colors["warning"] if kind == "warning" else colors["accent"]
        toast = ctk.CTkFrame(root, fg_color=color, corner_radius=14)
        ctk.CTkLabel(toast, text=message, font=font_small, text_color="white", justify="left", wraplength=420, padx=16, pady=11).pack(fill="both", expand=True)
        toast.place(relx=1.0, rely=0.0, x=-26, y=86, anchor="ne")
        toast.lift()
        root.after(3600, lambda: toast.destroy() if toast.winfo_exists() else None)

    def set_status(text: str) -> None:
        var_status.set(text)
        root.update_idletasks()

    def update_log_badge() -> None:
        count = unread_log_events["count"]
        if count <= 0:
            var_log_badge.set("")
            log_badge.place_forget()
            return
        var_log_badge.set(str(count if count < 100 else "99+"))
        log_badge.place(relx=1.0, rely=0.0, x=-2, y=-3, anchor="ne")
        log_badge.lift()

    def update_figure_badge() -> None:
        count = unread_figure_events["count"]
        if count <= 0:
            var_figure_badge.set("")
            figure_badge.place_forget()
            return
        var_figure_badge.set(str(count if count < 100 else "99+"))
        figure_badge.place(relx=1.0, rely=0.0, x=-2, y=-3, anchor="ne")
        figure_badge.lift()

    def mark_log_unread() -> None:
        if active_tab["name"] != "Log":
            unread_log_events["count"] += 1
            update_log_badge()

    def mark_figure_unread() -> None:
        if active_tab["name"] != "Figures":
            unread_figure_events["count"] += 1
            update_figure_badge()

    def append_log(message: str, *, notify: bool = True) -> None:
        log_box.configure(state="normal")
        log_box.insert("end", message + "\n")
        log_box.see("end")
        log_box.configure(state="disabled")
        if notify:
            mark_log_unread()

    def browse_file(var: tk.StringVar, title: str, patterns: list[tuple[str, str]]) -> None:
        path = filedialog.askopenfilename(title=title, initialdir=str(_THIS_DIR), filetypes=patterns)
        if path:
            var.set(path)

    def browse_plate_files() -> None:
        paths = filedialog.askopenfilenames(title="Choose replicate plate-reader files", initialdir=str(_THIS_DIR), filetypes=table_patterns)
        if paths:
            var_plate_files.set("; ".join(paths))
            save_state()

    def browse_dir(var: tk.StringVar, title: str) -> None:
        path = filedialog.askdirectory(title=title, initialdir=str(_THIS_DIR))
        if path:
            var.set(path)

    def parse_float(label: str, value: str, *, required: bool = True) -> float | None:
        text = value.strip()
        if not text:
            if required:
                raise ValueError(f"Enter {label}.")
            return None
        try:
            return float(text)
        except Exception as exc:
            raise ValueError(f"{label} must be numeric.") from exc

    def parse_int(label: str, value: str, *, required: bool = True) -> int | None:
        text = value.strip()
        if not text:
            if required:
                raise ValueError(f"Enter {label}.")
            return None
        try:
            return int(text)
        except Exception as exc:
            raise ValueError(f"{label} must be an integer.") from exc

    def current_config() -> dict[str, Any]:
        if var_activity_input_type.get() == "Plate reader":
            activity_config = {
                "input_type": "plate_reader",
                "plate_files": split_paths(var_plate_files.get()),
                "plate_rows": var_plate_rows.get().strip(),
                "plate_columns": var_plate_columns.get().strip(),
                "plate_positive_control_wells": var_plate_controls.get().strip(),
                "plate_positive_control_wells_by_file": var_plate_controls_by_file.get().strip(),
                "plate_scale_mode": plate_scale_code(var_plate_scale_mode.get().strip()),
                "display_mode": display_mode_code(var_display_mode.get().strip()),
                "label": var_activity_label.get().strip(),
            }
        else:
            activity_config = {
                "input_type": "table",
                "path": var_activity_path.get().strip(),
                "sheet_name": var_sheet_name.get().strip(),
                "fraction_column": var_fraction_column.get().strip(),
                "start_column": var_start_column.get().strip(),
                "end_column": var_end_column.get().strip(),
                "value_column": var_value_column.get().strip(),
                "replicate_columns": split_csv(var_replicate_columns.get()),
                "control_mode": "row_numbers" if var_control_source.get() == "Manual rows" else "column_value" if var_control_source.get() == "Named row" else "none",
                "control_row_indices": core.parse_row_numbers(var_control_rows.get()) if var_control_source.get() == "Manual rows" else [],
                "control_column": var_control_column.get().strip(),
                "control_value": var_control_value.get().strip(),
                "control_query": "",
                "control_scalar_column": var_control_scalar_column.get().strip() if var_control_source.get() == "Separate column" else "",
                "explicit_control_value": None,
                "exclude_control_rows": bool(var_exclude_control_rows.get()) if var_control_source.get() in {"Manual rows", "Named row"} else False,
                "normalization_mode": "control" if var_control_source.get() != "No control" else "none",
                "display_mode": display_mode_code(var_display_mode.get().strip()),
                "label": var_activity_label.get().strip(),
            }
        return {
            "sample_name": var_sample_name.get().strip(),
            "chrom_mzml": var_mzml.get().strip(),
            "filtered_csv": var_filtered.get().strip(),
            "out_dir": var_outdir.get().strip(),
            "fraction_windows": {
                "rt_start": var_rt_start.get().strip(),
                "fraction_width": var_fraction_width.get().strip(),
                "n_fractions": var_n_fractions.get().strip(),
            },
            "activity_table": activity_config,
            "plot": {
                "x_max": parse_float("x max", var_x_max.get(), required=False),
                "log_total_area": bool(var_log_total.get()),
                "save_svg": bool(var_save_svg.get()),
                "save_png": bool(var_save_png.get()),
                "style": {
                    "title": var_title.get().strip() or None,
                    "top_y_label": var_top_ylabel.get().strip() or "MS intensity (BPC)",
                    "bottom_y_label": var_bottom_ylabel.get().strip() or None,
                    "x_label": var_x_label.get().strip() or "Retention time [min]",
                    "activity_y_label": var_activity_ylabel.get().strip() or None,
                    "chrom_color": var_chrom_color.get().strip() or "#101b73",
                    "dominant_feature_color": var_dominant_color.get().strip() or "#101b73",
                    "remainder_feature_color": var_remainder_color.get().strip() or "#ddd6cc",
                    "activity_color": var_activity_color.get().strip() or "#2f9e44",
                    "activity_alpha": parse_float("activity alpha", var_activity_alpha.get()) or 0.28,
                    "figsize": [parse_float("figure width", var_width.get()) or 14.0, parse_float("figure height", var_height.get()) or 8.0],
                },
            },
        }

    def save_state() -> None:
        try:
            save_json_safe(state_file, current_config())
        except Exception:
            pass

    def update_output_preview(*_args: Any) -> None:
        base = core.slugify(var_sample_name.get().strip() or "sample")
        suffix = "_log" if var_log_total.get() else ""
        outdir = Path(var_outdir.get().strip() or (_THIS_DIR / "outputs")).expanduser()
        var_output_preview.set(str(outdir / f"{base}{suffix}.svg"))
        save_state()

    def available_columns() -> list[str]:
        return ["", *activity_columns]

    def refresh_column_controls() -> None:
        values = available_columns()
        for combo in [combo_fraction, combo_start, combo_end, combo_value, combo_control_column, combo_control_scalar]:
            combo.configure(values=values)

    def render_activity_preview(df: pd.DataFrame) -> None:
        display = df.copy()
        display.insert(0, "row", range(1, len(display) + 1))
        with pd.option_context("display.max_columns", 16, "display.width", 180, "display.max_colwidth", 28):
            text = display.head(12).to_string(index=False)
        activity_preview.configure(state="normal")
        activity_preview.delete("1.0", "end")
        activity_preview.insert("end", text)
        activity_preview.configure(state="disabled")

    def load_activity_table() -> None:
        nonlocal activity_df, activity_columns
        path_text = var_activity_path.get().strip()
        if not path_text:
            show_toast("Choose an activity table first.", kind="warning")
            return
        path = Path(path_text).expanduser()
        if not path.exists():
            messagebox.showerror("Activity table", f"File not found:\n{path}")
            return
        try:
            sheets = core.excel_sheet_names(path)
            if sheets:
                sheet_combo.configure(values=sheets)
                if var_sheet_name.get().strip() not in sheets:
                    var_sheet_name.set(sheets[0])
            else:
                sheet_combo.configure(values=[""])
                var_sheet_name.set("")
            activity_df = core.read_table(path, sheet_name=var_sheet_name.get().strip() or None)
            activity_columns = [str(column) for column in activity_df.columns]
            refresh_column_controls()
            refresh_column_list()
            render_activity_preview(activity_df)
            append_log(f"Loaded activity table: {path} ({len(activity_df)} rows x {len(activity_columns)} columns).")
            show_toast("Activity table loaded.", kind="success")
        except Exception as exc:
            messagebox.showerror("Activity table", str(exc))

    def reload_selected_sheet(_choice: str | None = None) -> None:
        if var_activity_path.get().strip():
            load_activity_table()

    def use_selected_replicates() -> None:
        selected = [column_list.get(index) for index in column_list.curselection()]
        if not selected:
            show_toast("Select one or more columns first.", kind="warning")
            return
        var_replicate_columns.set(join_csv(selected))
        append_log(f"Selected replicate columns: {join_csv(selected)}")
        show_toast("Replicate columns applied.", kind="success")

    def refresh_column_list() -> None:
        column_list.delete(0, "end")
        for column in activity_columns:
            column_list.insert("end", column)

    def build_activity_settings() -> core.ActivityTableSettings | None:
        if var_activity_input_type.get() == "Plate reader":
            plate_files = split_paths(var_plate_files.get())
            if not plate_files:
                return None
            return core.ActivityTableSettings(
                path="",
                input_type="plate_reader",
                display_mode=display_mode_code(var_display_mode.get().strip()) or "percent_of_max",
                label=resolved_activity_label(),
                plate_files=tuple(plate_files),
                plate_rows=parse_int("plate rows", var_plate_rows.get()) or 8,
                plate_columns=parse_int("plate columns", var_plate_columns.get()) or 12,
                plate_positive_control_wells=var_plate_controls.get().strip(),
                plate_positive_control_wells_by_file=var_plate_controls_by_file.get().strip() or None,
                plate_scale_mode=plate_scale_code(var_plate_scale_mode.get().strip()) or "none",
            )
        path = var_activity_path.get().strip()
        if not path:
            return None
        source = var_control_source.get()
        return core.ActivityTableSettings(
            path=path,
            sheet_name=var_sheet_name.get().strip() or None,
            fraction_column=var_fraction_column.get().strip() or None,
            start_column=var_start_column.get().strip() or None,
            end_column=var_end_column.get().strip() or None,
            value_column=var_value_column.get().strip() or None,
            replicate_columns=tuple(split_csv(var_replicate_columns.get())),
            control_mode="row_numbers" if source == "Manual rows" else "column_value" if source == "Named row" else "none",
            control_row_indices=tuple(core.parse_row_numbers(var_control_rows.get())) if source == "Manual rows" else (),
            control_column=(var_control_column.get().strip() or None) if source == "Named row" else None,
            control_value=(var_control_value.get().strip() or None) if source == "Named row" else None,
            control_query=None,
            control_scalar_column=(var_control_scalar_column.get().strip() or None) if source == "Separate column" else None,
            explicit_control_value=None,
            exclude_control_rows=bool(var_exclude_control_rows.get()) if source in {"Manual rows", "Named row"} else False,
            normalization_mode="control" if source != "No control" else "none",
            display_mode=display_mode_code(var_display_mode.get().strip()) or "raw",
            label=resolved_activity_label(),
        )

    def build_plot_style() -> core.PlotStyle:
        return core.PlotStyle(
            title=var_title.get().strip() or None,
            top_y_label=var_top_ylabel.get().strip() or "MS intensity (BPC)",
            bottom_y_label=var_bottom_ylabel.get().strip() or None,
            x_label=var_x_label.get().strip() or "Retention time [min]",
            activity_y_label=var_activity_ylabel.get().strip() or None,
            chrom_color=var_chrom_color.get().strip() or "#101b73",
            dominant_feature_color=var_dominant_color.get().strip() or "#101b73",
            remainder_feature_color=var_remainder_color.get().strip() or "#ddd6cc",
            activity_color=var_activity_color.get().strip() or "#2f9e44",
            activity_alpha=parse_float("activity alpha", var_activity_alpha.get()) or 0.28,
            figsize=(parse_float("figure width", var_width.get()) or 14.0, parse_float("figure height", var_height.get()) or 8.0),
        )

    def validate_common_inputs() -> dict[str, Any]:
        sample_name = var_sample_name.get().strip()
        if not sample_name:
            raise ValueError("Enter a sample name.")
        mzml = Path(var_mzml.get().strip()).expanduser()
        filtered = Path(var_filtered.get().strip()).expanduser()
        if not mzml.exists():
            raise ValueError("Choose an existing chromatogram mzML file.")
        if not filtered.exists():
            raise ValueError("Choose an existing filtered CSV from script 01.")
        outdir = Path(var_outdir.get().strip() or (_THIS_DIR / "outputs")).expanduser()
        outdir.mkdir(parents=True, exist_ok=True)
        return {
            "sample_name": sample_name,
            "mzml": mzml,
            "filtered": filtered,
            "outdir": outdir,
            "rt_start": parse_float("RT start", var_rt_start.get()),
            "fraction_width": parse_float("fraction width", var_fraction_width.get()),
            "n_fractions": parse_int("number of fractions", var_n_fractions.get()),
            "x_max": parse_float("x max", var_x_max.get(), required=False),
            "activity_settings": build_activity_settings(),
            "plot_style": build_plot_style(),
        }

    def clear_figure_canvas() -> None:
        if current_canvas["canvas"] is not None:
            current_canvas["canvas"].get_tk_widget().destroy()
            current_canvas["canvas"] = None
        if current_canvas["toolbar"] is not None:
            current_canvas["toolbar"].destroy()
            current_canvas["toolbar"] = None

    def show_figure(fig: Any) -> None:
        clear_figure_canvas()
        figure_placeholder.grid_remove()
        canvas = FigureCanvasTkAgg(fig, master=figure_canvas_host)
        canvas.draw()
        canvas.get_tk_widget().grid(row=0, column=0, sticky="nsew")
        toolbar = NavigationToolbar2Tk(canvas, figure_canvas_host, pack_toolbar=False)
        toolbar.update()
        toolbar.grid(row=1, column=0, sticky="ew")
        current_canvas["canvas"] = canvas
        current_canvas["toolbar"] = toolbar

    def add_figure_record(fig: Any, label: str, summary: dict[str, Any], saved_paths: list[Path] | None = None) -> None:
        figure_records.append({"figure": fig, "label": label, "summary": summary, "saved_paths": saved_paths or []})
        figure_list.insert("end", label)
        figure_list.selection_clear(0, "end")
        figure_list.selection_set("end")
        figure_list.see("end")
        mark_figure_unread()
        show_selected_figure()

    def show_selected_figure() -> None:
        selection = figure_list.curselection()
        if not selection:
            return
        record = figure_records[selection[0]]
        show_figure(record["figure"])
        summary = record["summary"]
        saved = record["saved_paths"]
        summary_text = [
            f"Feature rows: {summary.get('feature_rows', 0)}",
            f"Fractions: {summary.get('fraction_count', 0)}",
            f"Activity rows: {summary.get('activity_rows', 0)}",
        ]
        if saved:
            summary_text.append("Saved:")
            summary_text.extend(str(path) for path in saved)
        figure_summary.configure(state="normal")
        figure_summary.delete("1.0", "end")
        figure_summary.insert("end", "\n".join(summary_text))
        figure_summary.configure(state="disabled")

    def run_plot(*, export: bool) -> None:
        try:
            cfg = validate_common_inputs()
        except Exception as exc:
            show_toast("Settings are incomplete.", kind="error")
            messagebox.showerror("Settings problem", str(exc))
            return
        if export and not any([var_save_svg.get(), var_save_png.get()]):
            messagebox.showerror("Export problem", "Choose SVG, PNG, or both before exporting.")
            return
        save_state()
        preview_button.configure(state="disabled")
        export_button.configure(state="disabled")
        set_status("Building figure...")
        append_log("Building figure...")
        show_toast("Figure generation started.", kind="info")

        def worker() -> None:
            try:
                result_queue.put(
                    (
                        "ok",
                        core.make_two_sided_plot(
                            chrom_mzml=cfg["mzml"],
                            filtered_csv=cfg["filtered"],
                            sample_name=cfg["sample_name"],
                            out_dir=cfg["outdir"],
                            activity_settings=cfg["activity_settings"],
                            rt_start=cfg["rt_start"],
                            fraction_width=cfg["fraction_width"],
                            n_fractions=cfg["n_fractions"],
                            x_max=cfg["x_max"],
                            log_total_area=bool(var_log_total.get()),
                            save_svg=bool(var_save_svg.get()) if export else False,
                            save_png=bool(var_save_png.get()) if export else False,
                            plot_style=cfg["plot_style"],
                        ),
                    )
                )
            except Exception as exc:
                result_queue.put(("err", (exc, traceback.format_exc())))

        threading.Thread(target=worker, daemon=True).start()

    def poll_queues() -> None:
        try:
            while True:
                append_log(log_queue.get_nowait())
        except queue.Empty:
            pass
        try:
            while True:
                kind, payload = result_queue.get_nowait()
                preview_button.configure(state="normal")
                export_button.configure(state="normal")
                if kind == "ok":
                    fig, _axes, saved_paths, summary = payload
                    add_figure_record(fig, f"{'Export' if saved_paths else 'Preview'} {datetime.now().strftime('%H:%M:%S')}", summary, saved_paths)
                    set_status("Figure ready.")
                    append_log(json.dumps(summary, indent=2, ensure_ascii=False))
                    show_toast("Figure ready.", kind="success")
                    set_active_tab("Figures")
                else:
                    exc, tb = payload
                    set_status("Figure failed.")
                    append_log(f"ERROR: {exc}\n{tb}")
                    show_toast("Figure generation failed.", kind="error")
                    messagebox.showerror("Figure error", str(exc))
        except queue.Empty:
            pass
        root.after(120, poll_queues)

    def save_config_dialog() -> None:
        try:
            config = current_config()
        except Exception as exc:
            messagebox.showerror("Config problem", str(exc))
            return
        path = filedialog.asksaveasfilename(
            title="Save configuration",
            initialdir=str(_THIS_DIR),
            defaultextension=".json",
            filetypes=config_patterns,
            initialfile="two_sided_plot_config.json",
        )
        if path:
            Path(path).write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
            show_toast("Configuration saved.", kind="success")
            append_log(f"Saved config: {path}")

    def apply_config(config: dict[str, Any]) -> None:
        var_sample_name.set(str(config.get("sample_name", "")))
        var_mzml.set(str(config.get("chrom_mzml", "")))
        var_filtered.set(str(config.get("filtered_csv", "")))
        var_outdir.set(str(config.get("out_dir", str(_THIS_DIR / "outputs"))))
        windows = config.get("fraction_windows", {}) or {}
        var_rt_start.set(str(windows.get("rt_start", 2.0)))
        var_fraction_width.set(str(windows.get("fraction_width", 0.375)))
        var_n_fractions.set(str(windows.get("n_fractions", 96)))
        activity = config.get("activity_table", {}) or {}
        var_activity_input_type.set("Plate reader" if str(activity.get("input_type", "table")).lower() == "plate_reader" else "Table")
        var_activity_path.set(str(activity.get("path", "")))
        var_sheet_name.set(str(activity.get("sheet_name", "")))
        var_fraction_column.set(str(activity.get("fraction_column", "fraction")))
        var_start_column.set(str(activity.get("start_column", "")))
        var_end_column.set(str(activity.get("end_column", "")))
        var_value_column.set(str(activity.get("value_column", "average")))
        var_replicate_columns.set(join_csv(activity.get("replicate_columns", [])))
        var_control_source.set(initial_control_source(activity))
        var_control_rows.set(join_csv([str(value) for value in activity.get("control_row_indices", [])]))
        var_control_column.set(str(activity.get("control_column", "")))
        var_control_value.set(str(activity.get("control_value", "")))
        var_control_scalar_column.set(str(activity.get("control_scalar_column", "")))
        var_exclude_control_rows.set(bool(activity.get("exclude_control_rows", True)))
        var_display_mode.set(display_mode_label(str(activity.get("display_mode", "percent_of_max"))))
        var_activity_label.set(str(activity.get("label", "Activity")))
        var_plate_files.set("; ".join(str(x) for x in (activity.get("plate_files") or [])) if not isinstance(activity.get("plate_files") or "", str) else str(activity.get("plate_files") or ""))
        var_plate_rows.set(str(activity.get("plate_rows", 8)))
        var_plate_columns.set(str(activity.get("plate_columns", 12)))
        var_plate_controls.set(str(activity.get("plate_positive_control_wells", "H11, H12")))
        var_plate_controls_by_file.set(str(activity.get("plate_positive_control_wells_by_file", "") or ""))
        var_plate_scale_mode.set(plate_scale_label(str(activity.get("plate_scale_mode", "none"))))
        refresh_activity_label_placeholder()
        plot = config.get("plot", {}) or {}
        style = plot.get("style", {}) or {}
        var_x_max.set("" if plot.get("x_max") is None else str(plot.get("x_max")))
        var_log_total.set(bool(plot.get("log_total_area", False)))
        var_save_svg.set(bool(plot.get("save_svg", True)))
        var_save_png.set(bool(plot.get("save_png", False)))
        var_title.set(str(style.get("title", "") or ""))
        var_top_ylabel.set(str(style.get("top_y_label", "MS intensity (BPC)")))
        var_bottom_ylabel.set(str(style.get("bottom_y_label", "") or ""))
        var_x_label.set(str(style.get("x_label", "Retention time [min]")))
        var_activity_ylabel.set(str(style.get("activity_y_label", "") or ""))
        var_chrom_color.set(str(style.get("chrom_color", "#101b73")))
        var_dominant_color.set(str(style.get("dominant_feature_color", "#101b73")))
        var_remainder_color.set(str(style.get("remainder_feature_color", "#ddd6cc")))
        var_activity_color.set(str(style.get("activity_color", "#2f9e44")))
        var_activity_alpha.set(str(style.get("activity_alpha", 0.28)))
        figsize = style.get("figsize") or [14.0, 8.0]
        var_width.set(str(figsize[0]))
        var_height.set(str(figsize[1]))
        update_output_preview()
        update_control_source_visibility()
        update_activity_input_visibility()

    def load_config_dialog() -> None:
        path = filedialog.askopenfilename(title="Load configuration", initialdir=str(_THIS_DIR), filetypes=config_patterns)
        if not path:
            return
        try:
            config = json.loads(Path(path).read_text(encoding="utf-8"))
            apply_config(config)
            if var_activity_input_type.get() == "Table" and var_activity_path.get().strip():
                load_activity_table()
            save_state()
            show_toast("Configuration loaded.", kind="success")
            append_log(f"Loaded config: {path}")
        except Exception as exc:
            messagebox.showerror("Load problem", str(exc))

    # ---------------------------
    # Layout
    # ---------------------------
    root.grid_columnconfigure(0, weight=1)
    root.grid_rowconfigure(2, weight=1)

    header = ctk.CTkFrame(root, fg_color="transparent")
    header.grid(row=0, column=0, sticky="ew", padx=24, pady=(20, 12))
    header.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(header, text="Two-sided fraction plot", font=font_header, text_color=colors["text"], anchor="w").grid(row=0, column=0, sticky="w")
    ctk.CTkLabel(header, text="Chromatogram, retained fraction features, and optional activity overlay in one publication figure.", font=font_subtitle, text_color=colors["muted"], anchor="w").grid(row=1, column=0, sticky="w", pady=(4, 0))
    header_actions = ctk.CTkFrame(header, fg_color="transparent")
    header_actions.grid(row=0, column=1, rowspan=2, sticky="e")
    preview_button = make_button(header_actions, "Preview figure", lambda: run_plot(export=False), width=128)
    preview_button.pack(side="left", padx=(0, 8))
    export_button = make_button(header_actions, "Run + export", lambda: run_plot(export=True), primary=True, width=128)
    export_button.pack(side="left", padx=(0, 8))
    make_button(header_actions, "Save config", save_config_dialog, width=112).pack(side="left", padx=(0, 8))
    make_button(header_actions, "Load config", load_config_dialog, width=112).pack(side="left", padx=(0, 8))
    make_button(header_actions, "Close", root.destroy, danger=True, width=82).pack(side="left")

    tab_bar = ctk.CTkFrame(root, fg_color="transparent")
    tab_bar.grid(row=1, column=0, sticky="ew", padx=24, pady=(0, 12))
    tab_buttons: dict[str, Any] = {}
    tab_frames: dict[str, Any] = {}

    content = ctk.CTkFrame(root, fg_color=colors["surface"], corner_radius=16)
    content.grid(row=2, column=0, sticky="nsew", padx=24, pady=(0, 12))
    content.grid_columnconfigure(0, weight=1)
    content.grid_rowconfigure(0, weight=1)

    def set_active_tab(name: str) -> None:
        active_tab["name"] = name
        for tab_name, frame in tab_frames.items():
            if tab_name == name:
                frame.grid(row=0, column=0, sticky="nsew")
            else:
                frame.grid_remove()
        for tab_name, button in tab_buttons.items():
            selected = tab_name == name
            button.configure(fg_color=colors["accent"] if selected else colors["card_alt"], hover_color=colors["accent_hover"] if selected else "#39414c")
        if name == "Figures":
            unread_figure_events["count"] = 0
            update_figure_badge()
        elif name == "Log":
            unread_log_events["count"] = 0
            update_log_badge()

    for name, width in [("Workflow", 150), ("Activity data", 170), ("Figures", 140), ("Log", 120)]:
        holder = ctk.CTkFrame(tab_bar, fg_color="transparent")
        holder.pack(side="left", padx=(0, 8))
        button = make_button(holder, name, lambda tab=name: set_active_tab(tab), width=width)
        button.pack()
        tab_buttons[name] = button
        tab_frames[name] = ctk.CTkFrame(content, fg_color="transparent")
        if name == "Figures":
            figure_badge = ctk.CTkLabel(holder, textvariable=var_figure_badge, width=20, height=20, corner_radius=10, fg_color=colors["danger"], text_color="white", font=("Segoe UI", 10, "bold"))
            figure_badge.place_forget()
        if name == "Log":
            log_badge = ctk.CTkLabel(holder, textvariable=var_log_badge, width=20, height=20, corner_radius=10, fg_color=colors["danger"], text_color="white", font=("Segoe UI", 10, "bold"))
            log_badge.place_forget()

    workflow_scroll = ctk.CTkScrollableFrame(tab_frames["Workflow"], fg_color="transparent")
    workflow_scroll.pack(fill="both", expand=True, padx=8, pady=8)
    card = make_card(workflow_scroll, "Inputs")
    file_row(card, 0, "Sample chromatogram mzML", var_mzml, lambda: browse_file(var_mzml, "Select mzML", mzml_patterns), "Raw HPLC-MS file for the fractionated sample. The core reads its base-peak chromatogram to form the upper trace.")
    file_row(card, 1, "Filtered fraction table", var_filtered, lambda: browse_file(var_filtered, "Select filtered feature CSV", csv_patterns), "CSV produced by script 01. It should contain the retained fraction features with fraction index, retention time, and area columns.")
    labeled_widget(card, "Sample name", make_entry(card, var_sample_name, "e.g. Hypericum hookerianum S19"), 2, 0, help_text="Used in the figure title and exported file names.")
    file_row(card, 3, "Output directory", var_outdir, lambda: browse_dir(var_outdir, "Select output directory"), "Folder where SVG and PNG exports are written.")
    labeled_widget(card, "Output preview", make_entry(card, var_output_preview), 4, 0, help_text="Generated from the sample name, output directory, and log-area option. This field is a preview only.")

    card = make_card(workflow_scroll, "Fraction windows")
    labeled_widget(card, "RT start (min)", make_entry(card, var_rt_start, "2.0"), 0, 0, help_text="Start of the first collected HPLC fraction. Used whenever the activity table does not provide explicit fraction start/end columns.")
    labeled_widget(card, "Fraction width (min)", make_entry(card, var_fraction_width, "0.375"), 0, 2, help_text="Width of each collected fraction. With 2.0 min start, 0.375 min width, and 96 fractions, the windows span 2.0-38.0 min.")
    labeled_widget(card, "Number of fractions", make_entry(card, var_n_fractions, "96"), 1, 0, help_text="Total number of uniform collected fractions. This should match the fractionation method and the indices in the script 01 output.")

    card = make_card(workflow_scroll, "Plot styling")
    labeled_widget(card, "Figure title", make_entry(card, var_title, "Leave blank to use the sample name"), 0, 0, help_text="Optional title override. Leave blank to use the sample name.")
    labeled_widget(card, "X-axis label", make_entry(card, var_x_label), 0, 2, help_text="Label printed under the retention-time axis.")
    labeled_widget(card, "Top Y label", make_entry(card, var_top_ylabel), 1, 0, help_text="Label for the upper chromatogram axis.")
    labeled_widget(card, "Bottom Y label", make_entry(card, var_bottom_ylabel, "Leave blank for automatic"), 1, 2, help_text="Label for the lower feature-area axis. Leave blank to use Feature area or log10(total area) automatically.")
    labeled_widget(card, "Activity Y label", make_entry(card, var_activity_ylabel, "Leave blank to use activity label"), 2, 0, help_text="Optional label for the right-side overlay axis.")
    labeled_widget(card, "X max (min)", make_entry(card, var_x_max, "Auto"), 2, 2, help_text="Optional right limit for the retention-time axis. Leave blank to infer it from the chromatogram and fractions.")
    labeled_widget(card, "Figure width", make_entry(card, var_width, "14"), 3, 0, help_text="Figure width in inches.")
    labeled_widget(card, "Figure height", make_entry(card, var_height, "8"), 3, 2, help_text="Figure height in inches.")
    labeled_widget(card, "Chromatogram color", make_entry(card, var_chrom_color, "#101b73"), 4, 0, help_text="Hex color for the chromatogram line.")
    labeled_widget(card, "Dominant feature color", make_entry(card, var_dominant_color, "#101b73"), 4, 2, help_text="Hex color for the largest matched feature in each fraction.")
    labeled_widget(card, "Remaining feature color", make_entry(card, var_remainder_color, "#ddd6cc"), 5, 0, help_text="Hex color for the sum of all remaining matched feature areas in the same fraction.")
    labeled_widget(card, "Activity color", make_entry(card, var_activity_color, "#2f9e44"), 5, 2, help_text="Hex color for the activity overlay bars.")
    labeled_widget(card, "Activity transparency", make_entry(card, var_activity_alpha, "0.28"), 6, 0, help_text="Alpha for the overlay bars, from 0 transparent to 1 opaque.")
    options = ctk.CTkFrame(card, fg_color="transparent")
    options.grid(row=7, column=0, columnspan=6, sticky="ew", pady=(8, 0))
    make_checkbox(options, "Use log10(total area)", var_log_total).pack(side="left", padx=(0, 18))
    make_checkbox(options, "Save SVG", var_save_svg).pack(side="left", padx=(0, 18))
    make_checkbox(options, "Save PNG", var_save_png).pack(side="left")

    activity_scroll = ctk.CTkScrollableFrame(tab_frames["Activity data"], fg_color="transparent")
    activity_scroll.pack(fill="both", expand=True, padx=8, pady=8)
    input_mode_card = make_card(activity_scroll, "Activity input type")
    input_type_combo = make_combo(input_mode_card, var_activity_input_type, ["Table", "Plate reader"])
    input_type_combo.configure(command=lambda _choice: update_activity_input_visibility(reset_for_plate=True))
    labeled_widget(input_mode_card, "Input format", input_type_combo, 0, 0, help_text="Choose one input format. Table uses an already summarized fraction table. Plate reader detects and combines replicate raw well-plate exports directly.")

    activity_math_card = make_card(activity_scroll, "Scaling calculations")
    activity_math_header = ctk.CTkFrame(activity_math_card, fg_color="transparent")
    activity_math_header.grid(row=0, column=0, columnspan=6, sticky="ew")
    activity_math_header.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(
        activity_math_header,
        text="Open this only when you want to check how plate values, controls, and display transforms are calculated.",
        font=font_small,
        text_color=colors["muted"],
        anchor="w",
        justify="left",
        wraplength=900,
    ).grid(row=0, column=0, sticky="ew", padx=(0, 10))
    activity_math_button = make_button(activity_math_header, "Show calculations", lambda: toggle_activity_math(), width=150)
    activity_math_button.grid(row=0, column=1, sticky="e")
    activity_math_state: dict[str, Any] = {"body": None, "built": False}

    def get_activity_math_body() -> Any:
        body = activity_math_state["body"]
        if body is None:
            body = ctk.CTkFrame(activity_math_card, fg_color=colors["entry"], border_color=colors["border"], border_width=1, corner_radius=10)
            body.grid_columnconfigure(0, weight=1)
            activity_math_state["body"] = body
        return body

    def render_formula_label(parent: Any, formula_text: str, row: int) -> int:
        formula = ctk.CTkFrame(parent, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=8)
        formula.grid(row=row, column=0, sticky="ew", padx=18, pady=(2, 8))
        formula.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(
            formula,
            text=formula_text,
            font=("Cambria Math", 15),
            text_color=colors["text"],
            anchor="w",
            justify="left",
            wraplength=920,
        ).grid(row=0, column=0, sticky="ew", padx=12, pady=8)
        return row + 1

    def add_math_text(parent: Any, row: int, text: str, *, title: bool = False, pady: tuple[int, int] = (8, 2)) -> int:
        ctk.CTkLabel(
            parent,
            text=text,
            font=("Segoe UI", 12, "bold") if title else ("Segoe UI", 12),
            text_color=colors["text"] if title else colors["muted"],
            justify="left",
            anchor="w",
            wraplength=980,
        ).grid(row=row, column=0, sticky="ew", padx=18, pady=pady)
        return row + 1

    def build_activity_math_content() -> None:
        if activity_math_state["built"]:
            return
        activity_math_state["built"] = True
        activity_math_body = get_activity_math_body()
        math_row = 0
        math_row = add_math_text(activity_math_body, math_row, "How to read these calculations", title=True, pady=(14, 2))
        math_row = add_math_text(
            activity_math_body,
            math_row,
            "Each plate-reader file is treated as one replicate plate. A well is one position on that plate, such as A1 or H12. The app first prepares one value for each well on each replicate plate, then averages the matching well across replicate plates.",
            pady=(0, 6),
        )

        math_row = add_math_text(activity_math_body, math_row, "Direct plate-reader input", title=True)
        math_row = add_math_text(activity_math_body, math_row, "Recommended starting point: keep plate scaling as raw fluorescence, then use Scale tallest signal to 100% if you want an easy 0-100% overlay.")
        math_row = add_math_text(activity_math_body, math_row, "Raw fluorescence (no scaling)", title=True, pady=(6, 0))
        math_row = add_math_text(activity_math_body, math_row, "On one replicate plate, the raw number from each well is kept unchanged. No positive-control correction and no min-max scaling are applied.", pady=(0, 4))
        math_row = render_formula_label(activity_math_body, "value on one replicate plate = raw fluorescence read from that well", math_row)
        math_row = add_math_text(activity_math_body, math_row, "If several replicate plate files are selected, the same well position is averaged across plates. For fractions, this means A1 from each plate becomes fraction 1, A2 becomes fraction 2, and so on.", pady=(0, 4))
        math_row = render_formula_label(activity_math_body, "final value used for plotting = mean(raw A1 from plate 1, raw A1 from plate 2, ...)", math_row)

        math_row = add_math_text(activity_math_body, math_row, "Percent of positive control", title=True, pady=(6, 0))
        math_row = add_math_text(activity_math_body, math_row, "On each replicate plate, the app calculates the mean of the selected positive-control wells. Every well on that same plate is then converted to percent of that plate's control average.", pady=(0, 4))
        math_row = render_formula_label(activity_math_body, "control average on this plate = mean(raw fluorescence in selected positive-control wells)", math_row)
        math_row = render_formula_label(activity_math_body, "value on one replicate plate = 100 * raw fluorescence of this well / control average on this plate", math_row)
        math_row = add_math_text(activity_math_body, math_row, "After this conversion, matching wells are averaged across replicate plates.", pady=(0, 4))
        math_row = render_formula_label(activity_math_body, "final value used for plotting = mean(percent-control value from plate 1, percent-control value from plate 2, ...)", math_row)

        math_row = add_math_text(activity_math_body, math_row, "Relative range within each plate (0-100)", title=True, pady=(6, 0))
        math_row = add_math_text(activity_math_body, math_row, "On each replicate plate, values are first converted to percent of the positive control. Then the lowest non-control sample well on that plate becomes 0 and the highest non-control sample well becomes 100.", pady=(0, 4))
        math_row = render_formula_label(activity_math_body, "percent-control value = 100 * raw fluorescence of this well / control average on this plate", math_row)
        math_row = render_formula_label(activity_math_body, "value on one replicate plate = 100 * (percent-control value - lowest sample value on this plate) / (highest sample value on this plate - lowest sample value on this plate)", math_row)
        math_row = add_math_text(activity_math_body, math_row, "Only non-control wells are used to find the lowest and highest sample value. After this plate-level 0-100 scaling, matching wells are averaged across replicate plates.")
        math_row = render_formula_label(activity_math_body, "final value used for plotting = mean(0-100 value from plate 1, 0-100 value from plate 2, ...)", math_row)

        math_row = add_math_text(activity_math_body, math_row, "Prepared activity-table input", title=True)
        math_row = add_math_text(activity_math_body, math_row, "If replicate columns are selected, the app first averages them row by row. If a value column is selected, that column is used directly.")
        math_row = render_formula_label(activity_math_body, "row signal = mean(selected replicate columns in this row)", math_row)
        math_row = add_math_text(activity_math_body, math_row, "If a control reference is selected, the signal is divided by that control. With no control reference, the signal is left unchanged.")
        math_row = render_formula_label(activity_math_body, "normalized row signal = row signal / selected control value", math_row)
        math_row = render_formula_label(activity_math_body, "without control normalization: normalized row signal = row signal", math_row)

        math_row = add_math_text(activity_math_body, math_row, "Display meaning", title=True)
        math_row = add_math_text(activity_math_body, math_row, "This step happens after plate scaling and replicate averaging. It changes how the final values are drawn on the right Y-axis; it does not change the original plate files.")
        math_row = add_math_text(activity_math_body, math_row, "Show signal values", title=True, pady=(6, 0))
        math_row = add_math_text(activity_math_body, math_row, "Plot the final value directly.", pady=(0, 4))
        math_row = render_formula_label(activity_math_body, "plotted bar height = final value used for plotting", math_row)
        math_row = add_math_text(activity_math_body, math_row, "Scale tallest signal to 100%", title=True, pady=(6, 0))
        math_row = add_math_text(activity_math_body, math_row, "The largest final value becomes 100. All other bars keep their relative proportions.", pady=(0, 4))
        math_row = render_formula_label(activity_math_body, "plotted bar height = 100 * final value / highest final value", math_row)
        math_row = add_math_text(activity_math_body, math_row, "Show activity (low signal = high bar)", title=True, pady=(6, 0))
        math_row = add_math_text(activity_math_body, math_row, "Use this only when lower signal means stronger biological effect. The strongest signal becomes 0 activity; lower signals become taller activity bars.", pady=(0, 4))
        math_row = render_formula_label(activity_math_body, "plotted bar height = 100 - (100 * final value / highest final value)", math_row)
        add_math_text(
            activity_math_body,
            math_row,
            "If lower signal does not mean stronger activity in your assay, use Scale tallest signal to 100% or Show signal values instead.",
            pady=(0, 14),
        )

    def toggle_activity_math() -> None:
        var_show_activity_math.set(not var_show_activity_math.get())
        if var_show_activity_math.get():
            activity_math_body = get_activity_math_body()
            build_activity_math_content()
            activity_math_body.grid(row=1, column=0, columnspan=6, sticky="ew", pady=(12, 0))
            activity_math_button.configure(text="Hide calculations")
        else:
            activity_math_body = activity_math_state["body"]
            if activity_math_body is not None:
                activity_math_body.grid_remove()
            activity_math_button.configure(text="Show calculations")

    plate_mode_card = make_card(activity_scroll, "Direct plate-reader input")
    plate_picker = ctk.CTkFrame(plate_mode_card, fg_color="transparent")
    plate_picker.grid(row=1, column=1, columnspan=5, sticky="ew", padx=(0, 18), pady=7)
    plate_picker.grid_columnconfigure(0, weight=1)
    make_entry(plate_picker, var_plate_files, "A_blue.xlsx; B_blue.xlsx").grid(row=0, column=0, sticky="ew", padx=(0, 8))
    make_button(plate_picker, "Browse", browse_plate_files, width=82).grid(row=0, column=1)
    ctk.CTkLabel(plate_mode_card, text="Plate files", font=font_label, text_color=colors["muted"], anchor="w").grid(row=1, column=0, sticky="w", padx=(0, 10), pady=7)
    labeled_widget(plate_mode_card, "Plate rows", make_entry(plate_mode_card, var_plate_rows, "8"), 2, 0, help_text="Number of lettered well rows in the assay plate. Use 8 for 96 wells, 4 for 24 wells, or 16 for 384 wells.")
    labeled_widget(plate_mode_card, "Plate columns", make_entry(plate_mode_card, var_plate_columns, "12"), 2, 2, help_text="Number of numbered well columns in the assay plate. Use 12 for 96 wells, 6 for 24 wells, or 24 for 384 wells.")
    labeled_widget(plate_mode_card, "Positive-control wells", make_entry(plate_mode_card, var_plate_controls, "H11, H12"), 3, 0, help_text="Wells containing positive controls on each replicate plate. Their mean is calculated from the selected raw plate files; no separate table is needed.")
    plate_scale_combo = make_combo(plate_mode_card, var_plate_scale_mode, list(plate_scale_labels))
    plate_scale_combo.configure(command=lambda _choice: save_state())
    labeled_widget(plate_mode_card, "Plate scaling", plate_scale_combo, 3, 2, help_text="This prepares fluorescence values from replicate plates before plotting. Relative range within each plate makes the weakest and strongest non-control wells span 0 to 100 on every plate, which helps compare plates with different overall intensity. Percent of positive control preserves each value relative to the selected control wells. Raw fluorescence performs no plate-level scaling.")
    labeled_widget(plate_mode_card, "Per-file controls", make_entry(plate_mode_card, var_plate_controls_by_file, "optional: 1:H11,H12; plate_2.xlsx:H10,H11"), 4, 0, help_text="Optional override when one replicate plate uses different positive-control wells. Use semicolon-separated entries keyed by replicate number or file name.", colspan=3)
    labeled_widget(plate_mode_card, "Display meaning", make_combo(plate_mode_card, var_display_mode, list(display_mode_labels)), 5, 0, help_text="This controls how the prepared fluorescence values appear in the figure. Choose Show activity when lower fluorescence means stronger biological inhibition: wells with less signal become taller activity bars. Choose Show signal values when fluorescence itself is the desired readout.")
    labeled_widget(plate_mode_card, "Overlay label", make_entry(plate_mode_card, var_activity_label, "Auto from display settings"), 5, 2, help_text="Optional right-axis label for the overlay bars. Leave blank to label the axis from the selected plate scaling and display meaning, for example normalized fluorescence, percent of positive control, or inhibition from signal.")

    table_input_card = make_card(activity_scroll, "Activity table")
    file_row(table_input_card, 0, "Activity table", var_activity_path, lambda: browse_file(var_activity_path, "Select activity table", table_patterns), "Prepared table input only. One row should represent one fraction unless that row is a control row you exclude from plotting.", load_cmd=load_activity_table)
    labeled_widget(table_input_card, "Excel sheet", make_combo(table_input_card, var_sheet_name, [""]), 1, 0, help_text="For Excel workbooks, choose the worksheet containing the fraction assay data. CSV/TSV files do not use this field.")
    sheet_combo = table_input_card.grid_slaves(row=1, column=1)[0]
    sheet_combo.configure(command=reload_selected_sheet)

    table_column_card = make_card(activity_scroll, "Map activity columns")
    card = table_column_card
    labeled_widget(card, "Fraction ID column", make_combo(card, var_fraction_column, [""]), 0, 0, help_text="Column that identifies collected fraction numbers, such as 1, 2, 3... Use this whenever control rows are mixed into the same table.")
    combo_fraction = card.grid_slaves(row=0, column=1)[0]
    labeled_widget(card, "Start-time column", make_combo(card, var_start_column, [""]), 0, 2, help_text="Optional explicit fraction start times in minutes. If blank, the uniform RT start/width from Workflow are used.")
    combo_start = card.grid_slaves(row=0, column=3)[0]
    labeled_widget(card, "End-time column", make_combo(card, var_end_column, [""]), 1, 0, help_text="Optional explicit fraction end times in minutes. Provide both start and end, or leave both blank.")
    combo_end = card.grid_slaves(row=1, column=1)[0]
    labeled_widget(card, "Value column", make_combo(card, var_value_column, [""]), 1, 2, help_text="Use a single precomputed measurement column such as average. Leave blank if you prefer to select replicate columns and let the app compute the mean.")
    combo_value = card.grid_slaves(row=1, column=3)[0]
    labeled_widget(card, "Replicate columns", make_entry(card, var_replicate_columns, "plate_1, plate_2"), 2, 0, help_text="Comma-separated replicate readout columns. When provided, their row-wise mean is used instead of the single value column.", colspan=3)
    select_frame = ctk.CTkFrame(card, fg_color="transparent")
    select_frame.grid(row=3, column=0, columnspan=6, sticky="ew", pady=(8, 0))
    select_frame.grid_columnconfigure(0, weight=1)
    column_list = tk.Listbox(select_frame, selectmode="extended", bg=colors["entry"], fg=colors["text"], selectbackground=colors["accent"], selectforeground="white", relief="flat", highlightthickness=1, highlightbackground=colors["border"], height=5, exportselection=False)
    column_list.grid(row=0, column=0, sticky="ew", padx=(0, 10))
    make_button(select_frame, "Use selected as replicates", use_selected_replicates, width=190).grid(row=0, column=1, sticky="n")

    table_control_card = make_card(activity_scroll, "Controls and activity scaling")
    card = table_control_card
    source_frame = ctk.CTkFrame(card, fg_color="transparent")
    source_frame.grid(row=0, column=0, columnspan=6, sticky="ew", pady=(0, 8))
    source_frame.grid_columnconfigure(0, weight=1)
    source_label = ctk.CTkFrame(source_frame, fg_color="transparent")
    source_label.grid(row=0, column=0, sticky="ew", padx=(0, 10))
    ctk.CTkLabel(source_label, text="Where is the control reference?", font=font_label, text_color=colors["muted"]).pack(side="left")
    make_help(
        source_label,
        "Choose how the control measurements are represented in your activity table. Use `Manual rows` when you want to point to control rows by their row numbers. Use `Separate column` when the table already contains a column with control-reference values. Use `Named row` when controls are ordinary rows that can be identified by a label such as control, blank, or positive_control.",
    ).pack(side="left", padx=(6, 0))
    control_source_switch = ctk.CTkSegmentedButton(
        source_frame,
        values=["No control", "Manual rows", "Separate column", "Named row"],
        variable=var_control_source,
        fg_color=colors["card_alt"],
        selected_color=colors["accent"],
        selected_hover_color=colors["accent_hover"],
        unselected_color=colors["card_alt"],
        unselected_hover_color="#39414c",
        text_color=colors["text"],
        height=36,
    )
    control_source_switch.grid(row=1, column=0, sticky="ew", pady=(8, 0))

    control_mode_host = ctk.CTkFrame(card, fg_color="transparent")
    control_mode_host.grid(row=1, column=0, columnspan=6, sticky="ew")
    control_mode_host.grid_columnconfigure(1, weight=1)
    control_mode_host.grid_columnconfigure(3, weight=1)

    no_control_frame = ctk.CTkFrame(control_mode_host, fg_color=colors["card_alt"], corner_radius=10)
    no_control_frame.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(
        no_control_frame,
        text="No control normalization will be applied. The overlay will use the activity values directly before the display transform below.",
        font=font_small,
        text_color=colors["muted"],
        anchor="w",
        justify="left",
        wraplength=980,
    ).grid(row=0, column=0, sticky="ew", padx=12, pady=10)

    manual_control_frame = ctk.CTkFrame(control_mode_host, fg_color="transparent")
    manual_control_frame.grid_columnconfigure(1, weight=1)
    labeled_widget(
        manual_control_frame,
        "Control row numbers",
        make_entry(manual_control_frame, var_control_rows, "e.g. 95, 96 or 95-98"),
        0,
        0,
        help_text="Enter the 1-based row numbers of rows that contain control measurements instead of fraction measurements. These rows should use the same selected value or replicate columns as the fraction rows.",
    )
    make_checkbox(manual_control_frame, "Exclude the selected control rows from plotted fractions", var_exclude_control_rows).grid(row=1, column=1, sticky="w", pady=(6, 0))

    separate_column_frame = ctk.CTkFrame(control_mode_host, fg_color="transparent")
    separate_column_frame.grid_columnconfigure(1, weight=1)
    labeled_widget(
        separate_column_frame,
        "Control summary column",
        make_combo(separate_column_frame, var_control_scalar_column, [""]),
        0,
        0,
        help_text="Choose the column that contains the control reference values used for normalization. This is appropriate when control measurements have already been summarized elsewhere in the sheet instead of being stored as ordinary fraction rows.",
    )
    combo_control_scalar = separate_column_frame.grid_slaves(row=0, column=1)[0]

    named_row_frame = ctk.CTkFrame(control_mode_host, fg_color="transparent")
    named_row_frame.grid_columnconfigure(1, weight=1)
    named_row_frame.grid_columnconfigure(3, weight=1)
    labeled_widget(
        named_row_frame,
        "Column with row names",
        make_combo(named_row_frame, var_control_column, [""]),
        0,
        0,
        help_text="Choose the column that tells you what each row represents, for example a sample-name column, well-type column, or row-label column.",
    )
    combo_control_column = named_row_frame.grid_slaves(row=0, column=1)[0]
    labeled_widget(
        named_row_frame,
        "Control row name",
        make_entry(named_row_frame, var_control_value, "e.g. positive_control"),
        0,
        2,
        help_text="Enter the exact label used for rows that are controls rather than fraction replicates, for example control, blank, or positive_control.",
    )
    make_checkbox(named_row_frame, "Exclude the named control row from plotted fractions", var_exclude_control_rows).grid(row=1, column=1, columnspan=3, sticky="w", pady=(6, 0))

    labeled_widget(card, "Display meaning", make_combo(card, var_display_mode, list(display_mode_labels)), 2, 0, help_text="Choose Show signal values to plot the processed measurement directly. Choose Scale tallest signal to 100% for a relative signal view. Choose Show activity when lower signal indicates stronger inhibition; it converts low signal into high activity bars.")
    labeled_widget(card, "Overlay label", make_entry(card, var_activity_label, "Auto from display settings"), 3, 0, help_text="Optional right-axis label for the overlay bars. Leave blank to label the axis from the selected data transformation instead of calling every overlay bioactivity.", colspan=3)

    def update_control_source_visibility(_choice: str | None = None) -> None:
        for frame in [no_control_frame, manual_control_frame, separate_column_frame, named_row_frame]:
            frame.grid_remove()
        source = var_control_source.get()
        if source == "Manual rows":
            manual_control_frame.grid(row=0, column=0, columnspan=6, sticky="ew")
        elif source == "Separate column":
            separate_column_frame.grid(row=0, column=0, columnspan=6, sticky="ew")
        elif source == "Named row":
            named_row_frame.grid(row=0, column=0, columnspan=6, sticky="ew")
        else:
            no_control_frame.grid(row=0, column=0, columnspan=6, sticky="ew")

    control_source_switch.configure(command=update_control_source_visibility)

    table_preview_card = make_card(activity_scroll, "Preview loaded activity data")
    card = table_preview_card
    activity_preview = ctk.CTkTextbox(card, height=220, fg_color=colors["entry"], border_color=colors["border"], border_width=1, text_color=colors["text"], font=font_mono, corner_radius=10, wrap="none")
    activity_preview.grid(row=0, column=0, columnspan=6, sticky="ew")
    activity_preview.insert("end", "Load an activity table to inspect rows, columns, and control locations.")
    activity_preview.configure(state="disabled")

    def update_activity_input_visibility(*_args: Any, reset_for_plate: bool = False) -> None:
        is_plate = var_activity_input_type.get() == "Plate reader"
        visible = [plate_mode_card] if is_plate else [table_input_card, table_column_card, table_control_card, table_preview_card]
        hidden = [table_input_card, table_column_card, table_control_card, table_preview_card] if is_plate else [plate_mode_card]
        for body in hidden:
            body.master.pack_forget()
        for body in visible:
            body.master.pack(fill="x", padx=6, pady=(0, 12))
        if is_plate and reset_for_plate:
            var_activity_path.set("")
            var_sheet_name.set("")
            var_start_column.set("")
            var_end_column.set("")
            var_replicate_columns.set("")
            var_control_source.set("No control")
            var_plate_scale_mode.set(plate_scale_label("none"))
            var_display_mode.set(display_mode_label("percent_of_max"))
            refresh_activity_label_placeholder()
        save_state()

    figures_frame = tab_frames["Figures"]
    figures_frame.grid_columnconfigure(1, weight=1)
    figures_frame.grid_rowconfigure(0, weight=1)
    left = ctk.CTkFrame(figures_frame, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=16)
    left.grid(row=0, column=0, sticky="ns", padx=(8, 6), pady=8)
    ctk.CTkLabel(left, text="Generated figures", font=font_card_title, text_color=colors["text"]).pack(anchor="w", padx=16, pady=(16, 10))
    figure_list = tk.Listbox(left, width=28, bg=colors["entry"], fg=colors["text"], selectbackground=colors["accent"], selectforeground="white", relief="flat", highlightthickness=1, highlightbackground=colors["border"], exportselection=False)
    figure_list.pack(fill="both", expand=True, padx=16, pady=(0, 12))
    figure_list.bind("<<ListboxSelect>>", lambda _event: show_selected_figure())
    figure_summary = ctk.CTkTextbox(left, width=250, height=150, fg_color=colors["entry"], border_color=colors["border"], border_width=1, text_color=colors["muted"], font=font_small, corner_radius=10, wrap="word")
    figure_summary.pack(fill="x", padx=16, pady=(0, 16))
    figure_summary.configure(state="disabled")
    figure_canvas_host = ctk.CTkFrame(figures_frame, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=16)
    figure_canvas_host.grid(row=0, column=1, sticky="nsew", padx=(6, 8), pady=8)
    figure_canvas_host.grid_columnconfigure(0, weight=1)
    figure_canvas_host.grid_rowconfigure(0, weight=1)
    figure_placeholder = ctk.CTkLabel(figure_canvas_host, text="Preview a figure to inspect it here.", font=font_subtitle, text_color=colors["muted"])
    figure_placeholder.grid(row=0, column=0, sticky="nsew")

    log_frame = tab_frames["Log"]
    log_frame.grid_columnconfigure(0, weight=1)
    log_frame.grid_rowconfigure(0, weight=1)
    log_box = ctk.CTkTextbox(log_frame, fg_color=colors["card"], border_color=colors["border"], border_width=1, text_color=colors["text"], font=font_mono, corner_radius=16, wrap="word")
    log_box.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
    log_box.insert("end", "Ready. Configure inputs and preview a figure.\n")
    log_box.configure(state="disabled")

    status_bar = ctk.CTkFrame(root, fg_color=colors["surface"], corner_radius=0)
    status_bar.grid(row=3, column=0, sticky="ew")
    status_bar.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(status_bar, textvariable=var_status, font=font_small, text_color=colors["muted"], anchor="w").grid(row=0, column=0, sticky="ew", padx=18, pady=8)

    # Startup and traces
    refresh_column_controls()
    refresh_column_list()
    update_control_source_visibility()
    update_activity_input_visibility()
    for variable in [var_sample_name, var_outdir, var_log_total]:
        variable.trace_add("write", update_output_preview)
    update_output_preview()
    if var_activity_input_type.get() == "Table" and var_activity_path.get().strip():
        load_activity_table()
        refresh_column_list()
    set_active_tab("Workflow")
    poll_queues()

    try:
        root.mainloop()
    finally:
        save_state()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
