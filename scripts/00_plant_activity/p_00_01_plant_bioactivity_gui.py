#!/usr/bin/env python3
"""CustomTkinter GUI for plant bioactivity plotting.

Script 00 GUI, modernized to follow the shared workflow GUI design:
- Workflow / Preview-column picker / Figures / Log tabs
- In-window help bubbles
- Toast-style user notifications
- Publication-friendly dark UI with separated cards

Examples
--------
python p_00_01_plant_bioactivity_gui.py
python p_00_01_plant_bioactivity_gui.py --input data/plant_extract_fluorescence_graph.xlsx
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import subprocess
import sys
import threading
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
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
    import p_00_00_plant_bioactivity_core as core  # type: ignore
except Exception as e:  # pragma: no cover
    raise SystemExit(
        "ERROR: Could not import p_00_00_plant_bioactivity_core.py.\n"
        "Place this GUI script in the same folder as p_00_00_plant_bioactivity_core.py.\n\n"
        f"Details: {e}"
    )


def main() -> int:
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox
    except Exception as e:
        print("ERROR: Tkinter is not available in this Python environment.", file=sys.stderr)
        print(f"Details: {e}", file=sys.stderr)
        return 2

    try:
        import customtkinter as ctk
    except Exception as e:
        print(
            "ERROR: CustomTkinter is not installed.\n"
            "Install it in your environment with: pip install customtkinter",
            file=sys.stderr,
        )
        print(f"Details: {e}", file=sys.stderr)
        return 2

    ctk.set_appearance_mode("dark")
    ctk.set_default_color_theme("blue")

    COLORS = {
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
        "tile_selected": "#1d5fc4",
    }

    FONT_HEADER = ("Segoe UI", 23, "bold")
    FONT_SUBTITLE = ("Segoe UI", 12)
    FONT_CARD_TITLE = ("Segoe UI", 15, "bold")
    FONT_LABEL = ("Segoe UI", 12)
    FONT_SMALL = ("Segoe UI", 11)
    FONT_MONO = ("Consolas", 11)

    TABLE_PATTERNS = [("Tables", "*.csv *.tsv *.txt *.xlsx *.xls"), ("All files", "*.*")]
    CONFIG_PATTERNS = [("JSON configuration", "*.json"), ("All files", "*.*")]
    APP_STATE_FILE = _THIS_DIR / ".p_00_01_plant_bioactivity_gui_state.json"

    def load_state() -> dict[str, Any]:
        if APP_STATE_FILE.exists():
            try:
                return json.loads(APP_STATE_FILE.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def save_state(data: dict[str, Any]) -> None:
        try:
            APP_STATE_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    def split_csv(text: str | None) -> list[str]:
        if not text:
            return []
        return [x.strip() for x in str(text).split(",") if x.strip()]

    def join_csv(values: list[str] | tuple[str, ...]) -> str:
        return ", ".join(str(v).strip() for v in values if str(v).strip())

    def split_paths(text: str) -> list[str]:
        normalized = text.replace("\n", ";").replace(",", ";")
        return [part.strip().strip('"') for part in normalized.split(";") if part.strip().strip('"')]

    plate_scale_labels = {
        "Relative range within each plate (0-100)": "positive_control_then_minmax_0_100",
        "Percent of positive control": "positive_control_pct",
        "Raw fluorescence (no scaling)": "none",
    }

    def plate_scale_label(value: str) -> str:
        if value in {"positive_control_then_minmax_0_100", "minmax_0_100", "control_then_minmax_0_100"}:
            return "Relative range within each plate (0-100)"
        return next((label for label, code in plate_scale_labels.items() if code == value), value)

    def plate_scale_code(value: str) -> str:
        return plate_scale_labels.get(value, value)

    def parse_float(label: str, value: str) -> float:
        try:
            return float(value.strip())
        except Exception as exc:
            raise ValueError(f"Invalid number for {label}: {value!r}") from exc

    def parse_optional_float(label: str, value: str) -> float | None:
        text = value.strip()
        if not text:
            return None
        return parse_float(label, text)

    def parse_row_numbers(text: str) -> list[int]:
        values: list[int] = []
        for part in [p.strip() for p in text.split(",") if p.strip()]:
            if "-" in part:
                left, right = [x.strip() for x in part.split("-", 1)]
                start = int(left)
                end = int(right)
                if end < start:
                    raise ValueError(f"Invalid row range: {part}")
                values.extend(list(range(start, end + 1)))
            else:
                values.append(int(part))
        return values

    def open_path(path: str | Path) -> None:
        p = Path(path).expanduser()
        if not p.exists():
            raise FileNotFoundError(f"Path does not exist: {p}")
        if sys.platform.startswith("win"):
            os.startfile(str(p))  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(p)])
        else:
            subprocess.Popen(["xdg-open", str(p)])

    def guess_sample_column(columns: list[str], current: str = "") -> str:
        if current and current in columns:
            return current
        priorities = ["species", "sample", "plant", "name", "extract"]
        for needle in priorities:
            for col in columns:
                if needle in col.lower():
                    return col
        return columns[0] if columns else ""

    def table_preview_text(df: pd.DataFrame, n_rows: int = 8) -> str:
        if df.empty:
            return "Table loaded, but it contains no rows."
        preview = df.head(n_rows).copy()
        with pd.option_context("display.max_columns", 40, "display.width", 180, "display.max_colwidth", 32):
            return preview.to_string(index=False)

    app_state = load_state()

    root = ctk.CTk()
    root.title("Plant bioactivity plotter")
    root.geometry("1360x880")
    root.minsize(1220, 740)
    root.configure(fg_color=COLORS["bg"])
    help_popovers = HelpPopoverController(root, ctk, COLORS, FONT_SMALL)

    # ---------------------------
    # Variables and state
    # ---------------------------
    parser = build_parser()
    args = parser.parse_args()

    var_status = tk.StringVar(value="Ready.")
    var_input_type = tk.StringVar(value=app_state.get("input_type", "Table"))
    var_input_path = tk.StringVar(value=args.input or app_state.get("input_path", ""))
    var_order_path = tk.StringVar(value=app_state.get("order_path", ""))
    var_output_prefix = tk.StringVar(value=app_state.get("output_prefix", str(_THIS_DIR / "output" / "plant_bioactivity")))

    var_sample_column = tk.StringVar(value=app_state.get("sample_column", ""))
    var_replicate_columns = tk.StringVar(value=app_state.get("replicate_columns", ""))
    var_order_column = tk.StringVar(value=app_state.get("order_column", ""))

    var_control_mode = tk.StringVar(value=app_state.get("control_mode", "None"))
    var_control_entry_1 = tk.StringVar(value=app_state.get("control_entry_1", ""))
    var_control_entry_2 = tk.StringVar(value=app_state.get("control_entry_2", ""))
    var_exclude_control = tk.BooleanVar(value=bool(app_state.get("exclude_control", True)))

    var_use_threshold = tk.BooleanVar(value=bool(app_state.get("use_threshold", False)))
    var_threshold = tk.StringVar(value=app_state.get("threshold", ""))
    var_threshold_mode = tk.StringVar(value=app_state.get("threshold_mode", "ge"))

    var_title = tk.StringVar(value=app_state.get("title", "Sample activity"))
    var_ylabel = tk.StringVar(value=app_state.get("ylabel", ""))
    var_figure_width = tk.StringVar(value=app_state.get("figure_width", "16"))
    var_figure_height = tk.StringVar(value=app_state.get("figure_height", "6"))
    var_rotate = tk.StringVar(value=app_state.get("rotate", "45"))
    var_font_family = tk.StringVar(value=app_state.get("font_family", "Arial"))
    var_title_size = tk.StringVar(value=app_state.get("title_size", "16"))
    var_axis_label_size = tk.StringVar(value=app_state.get("axis_label_size", "20"))
    var_xtick_label_size = tk.StringVar(value=app_state.get("xtick_label_size", "8"))
    var_ytick_label_size = tk.StringVar(value=app_state.get("ytick_label_size", "16"))
    var_legend_size = tk.StringVar(value=app_state.get("legend_size", "12"))

    var_export_table = tk.BooleanVar(value=bool(app_state.get("export_table", True)))
    var_export_png = tk.BooleanVar(value=bool(app_state.get("export_png", True)))
    var_export_svg = tk.BooleanVar(value=bool(app_state.get("export_svg", True)))
    var_plate_files = tk.StringVar(value=app_state.get("plate_files", ""))
    var_plate_rows = tk.StringVar(value=app_state.get("plate_rows", "8"))
    var_plate_columns = tk.StringVar(value=app_state.get("plate_columns", "12"))
    var_plate_controls = tk.StringVar(value=app_state.get("plate_controls", "H11, H12"))
    var_plate_controls_by_file = tk.StringVar(value=app_state.get("plate_controls_by_file", ""))
    var_plate_scale_mode = tk.StringVar(value=plate_scale_label(str(app_state.get("plate_scale_mode", "positive_control_then_minmax_0_100"))))
    var_plate_mapping_file = tk.StringVar(value=app_state.get("plate_mapping_file", ""))
    var_plate_mapping_column = tk.StringVar(value=app_state.get("plate_mapping_column", ""))

    var_preview_source = tk.StringVar(value="Input table")
    var_column_filter = tk.StringVar(value="")
    var_column_summary = tk.StringVar(value="Load an input table to inspect columns.")
    var_preview_summary = tk.StringVar(value="No figure generated yet.")
    var_figure_path_text = tk.StringVar(value="")
    var_log_badge = tk.StringVar(value="")
    var_figure_badge = tk.StringVar(value="")

    input_df: pd.DataFrame | None = None
    plate_preview_df: pd.DataFrame | None = None
    order_df: pd.DataFrame | None = None
    current_preview_df: pd.DataFrame | None = None
    all_columns: list[str] = []
    visible_columns: list[str] = []
    figure_records: list[dict[str, Any]] = []
    current_canvas: dict[str, Any] = {"canvas": None, "toolbar": None, "figure": None}
    log_queue: queue.Queue[str] = queue.Queue()
    active_tab = {"name": "Workflow"}
    unread_log_events = {"count": 0}
    unread_figure_events = {"count": 0}
    app_closing = {"value": False}
    scheduled_after_ids: set[str] = set()

    # ---------------------------
    # UI helpers
    # ---------------------------
    def run_if_open(callback: Any) -> Any:
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            if app_closing["value"]:
                return None
            try:
                if not root.winfo_exists():
                    return None
            except Exception:
                return None
            return callback(*args, **kwargs)

        return wrapped

    def schedule_after(delay_ms: int, callback: Any) -> str | None:
        if app_closing["value"]:
            return None
        after_id: str | None = None

        def wrapped() -> None:
            if after_id is not None:
                scheduled_after_ids.discard(after_id)
            if app_closing["value"]:
                return
            try:
                if root.winfo_exists():
                    callback()
            except tk.TclError:
                return

        after_id = root.after(delay_ms, wrapped)
        scheduled_after_ids.add(after_id)
        return after_id

    def cancel_scheduled_callbacks() -> None:
        for after_id in list(scheduled_after_ids):
            try:
                root.after_cancel(after_id)
            except Exception:
                pass
            scheduled_after_ids.discard(after_id)
        try:
            after_ids = root.tk.call("after", "info")
        except Exception:
            after_ids = ()
        for after_id in after_ids:
            try:
                root.after_cancel(after_id)
            except Exception:
                pass

    def make_help(parent: Any, text: str) -> Any:
        return help_popovers.create_bubble(parent, text)

    def make_button(parent: Any, text: str, command: Any, *, primary: bool = False, success: bool = False, danger: bool = False, width: int | None = None) -> Any:
        color = COLORS["success"] if success else COLORS["danger"] if danger else COLORS["accent"] if primary else COLORS["card_alt"]
        hover = COLORS["success_hover"] if success else COLORS["danger_hover"] if danger else COLORS["accent_hover"] if primary else "#39414c"
        return ctk.CTkButton(parent, text=text, command=command, width=width or 112, height=38, corner_radius=10, fg_color=color, hover_color=hover)

    def make_entry(parent: Any, var: tk.StringVar, placeholder: str = "") -> Any:
        return ctk.CTkEntry(
            parent,
            textvariable=var,
            placeholder_text=placeholder,
            fg_color=COLORS["entry"],
            border_color=COLORS["border"],
            text_color=COLORS["text"],
            height=36,
            corner_radius=8,
        )

    def make_combo(parent: Any, var: tk.StringVar, values: list[str]) -> Any:
        return ctk.CTkComboBox(
            parent,
            variable=var,
            values=values or [""],
            fg_color=COLORS["entry"],
            border_color=COLORS["border"],
            button_color=COLORS["accent"],
            button_hover_color=COLORS["accent_hover"],
            dropdown_fg_color=COLORS["card"],
            dropdown_hover_color=COLORS["card_alt"],
            text_color=COLORS["text"],
            dropdown_text_color=COLORS["text"],
            height=36,
            corner_radius=8,
        )

    def make_checkbox(parent: Any, text: str, variable: tk.BooleanVar, command: Any | None = None) -> Any:
        return ctk.CTkCheckBox(
            parent,
            text=text,
            variable=variable,
            command=command,
            font=FONT_SMALL,
            text_color=COLORS["text"],
            fg_color=COLORS["accent"],
            hover_color=COLORS["accent_hover"],
            border_color=COLORS["border"],
            checkbox_width=20,
            checkbox_height=20,
        )

    def labeled_widget(parent: Any, label_text: str, widget: Any, row: int, col: int, *, help_text: str = "", colspan: int = 1, padx_right: int = 18) -> None:
        label_frame = ctk.CTkFrame(parent, fg_color="transparent")
        label_frame.grid(row=row, column=col, sticky="ew", padx=(0, 10), pady=8)
        label_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(label_frame, text=label_text, font=FONT_LABEL, text_color=COLORS["muted"], anchor="w").grid(row=0, column=0, sticky="w")
        if help_text:
            make_help(label_frame, help_text).grid(row=0, column=1, sticky="e", padx=(6, 0))
        widget.grid(row=row, column=col + 1, columnspan=colspan, sticky="ew", padx=(0, padx_right), pady=8)

    def make_card(parent: Any, title: str, subtitle: str = "") -> Any:
        card = ctk.CTkFrame(parent, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, corner_radius=16)
        card.pack(fill="x", padx=6, pady=(0, 12))
        card.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(card, text=title, font=FONT_CARD_TITLE, text_color=COLORS["text"], anchor="w").grid(row=0, column=0, sticky="ew", padx=18, pady=(16, 2))
        if subtitle:
            ctk.CTkLabel(card, text=subtitle, font=FONT_SMALL, text_color=COLORS["muted"], anchor="w", justify="left", wraplength=1040).grid(row=1, column=0, sticky="ew", padx=18, pady=(0, 12))
        body = ctk.CTkFrame(card, fg_color="transparent")
        body.grid(row=2, column=0, sticky="ew", padx=18, pady=(0, 18))
        for col in range(6):
            body.grid_columnconfigure(col, weight=1 if col % 2 == 1 else 0)
        return body

    def file_row(parent: Any, row: int, label: str, var: tk.StringVar, browse_cmd: Any, help_text: str, *, load_cmd: Any | None = None) -> None:
        label_frame = ctk.CTkFrame(parent, fg_color="transparent")
        label_frame.grid(row=row, column=0, sticky="ew", padx=(0, 10), pady=8)
        label_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(label_frame, text=label, font=FONT_LABEL, text_color=COLORS["muted"], anchor="w").grid(row=0, column=0, sticky="w")
        make_help(label_frame, help_text).grid(row=0, column=1, sticky="e", padx=(6, 0))
        make_entry(parent, var).grid(row=row, column=1, columnspan=3 if load_cmd else 4, sticky="ew", padx=(0, 10), pady=8)
        make_button(parent, "Browse", browse_cmd, width=88).grid(row=row, column=4 if load_cmd else 5, sticky="ew", padx=(0, 10) if load_cmd else 0, pady=8)
        if load_cmd:
            make_button(parent, "Load", load_cmd, primary=True, width=78).grid(row=row, column=5, sticky="ew", pady=8)

    def show_toast(message: str, *, kind: str = "info", timeout_ms: int = 3600) -> None:
        color = COLORS["success"] if kind == "success" else COLORS["danger"] if kind == "error" else COLORS["warning"] if kind == "warning" else COLORS["accent"]
        toast = ctk.CTkFrame(root, fg_color=color, corner_radius=14)
        label = ctk.CTkLabel(toast, text=message, font=FONT_SMALL, text_color="white", justify="left", wraplength=420, padx=16, pady=11)
        label.pack(fill="both", expand=True)
        root.update_idletasks()
        toast.place(relx=1.0, rely=0.0, x=-26, y=86, anchor="ne")
        toast.lift()
        schedule_after(timeout_ms, lambda: toast.destroy() if toast.winfo_exists() else None)

    def set_status(text: str) -> None:
        var_status.set(text)
        root.update_idletasks()

    def append_log(message: str, *, notify: bool = True) -> None:
        txt_log.configure(state="normal")
        txt_log.insert("end", message + "\n")
        txt_log.see("end")
        txt_log.configure(state="disabled")
        if notify and active_tab["name"] != "Log":
            unread_log_events["count"] += 1
            update_log_badge()

    def poll_log_queue() -> None:
        if app_closing["value"]:
            return
        try:
            while True:
                append_log(log_queue.get_nowait())
        except queue.Empty:
            pass
        schedule_after(140, poll_log_queue)

    def current_config_dict() -> dict[str, Any]:
        """Return only user-editable GUI settings, suitable for app state or a portable config file."""
        return {
            "input_type": var_input_type.get().strip(),
            "input_path": var_input_path.get().strip(),
            "order_path": var_order_path.get().strip(),
            "output_prefix": var_output_prefix.get().strip(),
            "sample_column": var_sample_column.get().strip(),
            "replicate_columns": var_replicate_columns.get().strip(),
            "order_column": var_order_column.get().strip(),
            "control_mode": var_control_mode.get().strip(),
            "control_entry_1": var_control_entry_1.get().strip(),
            "control_entry_2": var_control_entry_2.get().strip(),
            "exclude_control": bool(var_exclude_control.get()),
            "use_threshold": bool(var_use_threshold.get()),
            "threshold": var_threshold.get().strip(),
            "threshold_mode": var_threshold_mode.get().strip(),
            "title": var_title.get().strip(),
            "ylabel": var_ylabel.get().strip(),
            "figure_width": var_figure_width.get().strip(),
            "figure_height": var_figure_height.get().strip(),
            "rotate": var_rotate.get().strip(),
            "font_family": var_font_family.get().strip(),
            "title_size": var_title_size.get().strip(),
            "axis_label_size": var_axis_label_size.get().strip(),
            "xtick_label_size": var_xtick_label_size.get().strip(),
            "ytick_label_size": var_ytick_label_size.get().strip(),
            "legend_size": var_legend_size.get().strip(),
            "export_table": bool(var_export_table.get()),
            "export_png": bool(var_export_png.get()),
            "export_svg": bool(var_export_svg.get()),
            "plate_files": var_plate_files.get().strip(),
            "plate_rows": var_plate_rows.get().strip(),
            "plate_columns": var_plate_columns.get().strip(),
            "plate_controls": var_plate_controls.get().strip(),
            "plate_controls_by_file": var_plate_controls_by_file.get().strip(),
            "plate_scale_mode": plate_scale_code(var_plate_scale_mode.get().strip()),
            "plate_mapping_file": var_plate_mapping_file.get().strip(),
            "plate_mapping_column": var_plate_mapping_column.get().strip(),
        }

    def save_current_state() -> None:
        save_state(current_config_dict())

    def apply_config_dict(settings: dict[str, Any]) -> None:
        """Apply settings from app state or a user-saved JSON config."""
        var_input_type.set(str(settings.get("input_type", "Table") or "Table"))
        var_input_path.set(str(settings.get("input_path", "") or ""))
        var_order_path.set(str(settings.get("order_path", "") or ""))
        var_output_prefix.set(str(settings.get("output_prefix", str(_THIS_DIR / "output" / "plant_bioactivity")) or ""))
        var_sample_column.set(str(settings.get("sample_column", "") or ""))
        var_replicate_columns.set(str(settings.get("replicate_columns", "") or ""))
        var_order_column.set(str(settings.get("order_column", "") or ""))
        var_control_mode.set(str(settings.get("control_mode", "None") or "None"))
        var_control_entry_1.set(str(settings.get("control_entry_1", "") or ""))
        var_control_entry_2.set(str(settings.get("control_entry_2", "") or ""))
        var_exclude_control.set(bool(settings.get("exclude_control", True)))
        var_use_threshold.set(bool(settings.get("use_threshold", False)))
        var_threshold.set(str(settings.get("threshold", "") or ""))
        var_threshold_mode.set(str(settings.get("threshold_mode", "ge") or "ge"))
        var_title.set(str(settings.get("title", "Sample activity") or "Sample activity"))
        var_ylabel.set(str(settings.get("ylabel", "") or ""))
        var_figure_width.set(str(settings.get("figure_width", "16") or "16"))
        var_figure_height.set(str(settings.get("figure_height", "6") or "6"))
        var_rotate.set(str(settings.get("rotate", "45") or "45"))
        var_font_family.set(str(settings.get("font_family", "Arial") or "Arial"))
        var_title_size.set(str(settings.get("title_size", "16") or "16"))
        var_axis_label_size.set(str(settings.get("axis_label_size", "20") or "20"))
        var_xtick_label_size.set(str(settings.get("xtick_label_size", "8") or "8"))
        var_ytick_label_size.set(str(settings.get("ytick_label_size", "16") or "16"))
        var_legend_size.set(str(settings.get("legend_size", "12") or "12"))
        var_export_table.set(bool(settings.get("export_table", True)))
        var_export_png.set(bool(settings.get("export_png", True)))
        var_export_svg.set(bool(settings.get("export_svg", True)))
        var_plate_files.set(str(settings.get("plate_files", "") or ""))
        var_plate_rows.set(str(settings.get("plate_rows", "8") or "8"))
        var_plate_columns.set(str(settings.get("plate_columns", "12") or "12"))
        var_plate_controls.set(str(settings.get("plate_controls", "H11, H12") or "H11, H12"))
        var_plate_controls_by_file.set(str(settings.get("plate_controls_by_file", "") or ""))
        var_plate_scale_mode.set(plate_scale_label(str(settings.get("plate_scale_mode", "positive_control_then_minmax_0_100") or "positive_control_then_minmax_0_100")))
        var_plate_mapping_file.set(str(settings.get("plate_mapping_file", "") or ""))
        var_plate_mapping_column.set(str(settings.get("plate_mapping_column", "") or ""))

        update_threshold_state()
        update_control_inputs(preserve_exclude=True)
        update_column_summary()
        update_input_mode_visibility()

    def dialog_initial_dir(current: str = "") -> str:
        text = current.strip()
        if text:
            p = Path(text).expanduser()
            if p.exists():
                return str(p if p.is_dir() else p.parent)
        return str(_THIS_DIR)

    def default_output_dir() -> Path:
        out = _THIS_DIR / "output"
        out.mkdir(parents=True, exist_ok=True)
        return out

    def default_output_prefix(input_path: Path) -> Path:
        return default_output_dir() / input_path.stem

    # ---------------------------
    # Layout
    # ---------------------------
    root.grid_columnconfigure(0, weight=1)
    root.grid_rowconfigure(1, weight=1)

    header = ctk.CTkFrame(root, fg_color="transparent")
    header.grid(row=0, column=0, sticky="ew", padx=24, pady=(20, 12))
    header.grid_columnconfigure(0, weight=1)

    ctk.CTkLabel(header, text="Plant bioactivity plotter", font=FONT_HEADER, text_color=COLORS["text"], anchor="w").grid(row=0, column=0, sticky="w")
    ctk.CTkLabel(
        header,
        text="Normalize replicate plate-reader data to controls, classify activity, preview the figure, and export publication-ready outputs.",
        font=FONT_SUBTITLE,
        text_color=COLORS["muted"],
        anchor="w",
    ).grid(row=1, column=0, sticky="w", pady=(4, 0))

    header_actions = ctk.CTkFrame(header, fg_color="transparent")
    header_actions.grid(row=0, column=1, rowspan=2, sticky="e")

    main_body = ctk.CTkFrame(root, fg_color="transparent")
    main_body.grid(row=1, column=0, sticky="nsew", padx=24, pady=(0, 12))
    main_body.grid_columnconfigure(0, weight=1)
    main_body.grid_rowconfigure(1, weight=1)

    tab_bar_wrap = ctk.CTkFrame(main_body, fg_color="transparent")
    tab_bar_wrap.grid(row=0, column=0, sticky="ew", pady=(0, 10))
    tab_bar_wrap.grid_columnconfigure(0, weight=1)
    tab_bar = ctk.CTkFrame(tab_bar_wrap, fg_color=COLORS["card"], corner_radius=12)
    tab_bar.grid(row=0, column=0)

    tab_content = ctk.CTkFrame(main_body, fg_color="transparent")
    tab_content.grid(row=1, column=0, sticky="nsew")
    tab_content.grid_columnconfigure(0, weight=1)
    tab_content.grid_rowconfigure(0, weight=1)

    workflow_tab = ctk.CTkFrame(tab_content, fg_color="transparent")
    preview_tab = ctk.CTkFrame(tab_content, fg_color="transparent")
    figures_tab = ctk.CTkFrame(tab_content, fg_color="transparent")
    log_tab = ctk.CTkFrame(tab_content, fg_color="transparent")

    tabs: dict[str, Any] = {
        "Workflow": workflow_tab,
        "Preview / column picker": preview_tab,
        "Figures": figures_tab,
        "Log": log_tab,
    }
    tab_buttons: dict[str, Any] = {}

    def style_tab_button(name: str) -> None:
        selected = active_tab["name"] == name
        tab_buttons[name].configure(
            fg_color=COLORS["accent"] if selected else COLORS["card"],
            hover_color=COLORS["accent_hover"] if selected else COLORS["card_alt"],
            text_color=COLORS["text"],
        )

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

    def set_active_tab(name: str) -> None:
        active_tab["name"] = name
        for tab_name, frame in tabs.items():
            if tab_name == name:
                frame.grid(row=0, column=0, sticky="nsew")
            else:
                frame.grid_remove()
            if tab_name in tab_buttons:
                style_tab_button(tab_name)
        if name == "Figures":
            unread_figure_events["count"] = 0
            update_figure_badge()
        if name == "Log":
            unread_log_events["count"] = 0
            update_log_badge()

    for idx, name in enumerate(tabs):
        holder = ctk.CTkFrame(tab_bar, fg_color="transparent")
        holder.grid(row=0, column=idx, padx=4, pady=4)
        btn = make_button(holder, name, lambda n=name: set_active_tab(n), width=170 if name != "Preview / column picker" else 230)
        btn.pack()
        tab_buttons[name] = btn
        if name == "Figures":
            figure_badge = ctk.CTkLabel(holder, textvariable=var_figure_badge, width=20, height=20, corner_radius=10, fg_color=COLORS["danger"], text_color="white", font=("Segoe UI", 10, "bold"))
        if name == "Log":
            log_badge = ctk.CTkLabel(holder, textvariable=var_log_badge, width=20, height=20, corner_radius=10, fg_color=COLORS["danger"], text_color="white", font=("Segoe UI", 10, "bold"))

    # Header action buttons are wired after functions are defined.

    # ---------------------------
    # Workflow tab
    # ---------------------------
    workflow_tab.grid_columnconfigure(0, weight=1)
    workflow_tab.grid_rowconfigure(0, weight=1)
    workflow_scroll = ctk.CTkScrollableFrame(workflow_tab, fg_color="transparent", scrollbar_button_color=COLORS["card_alt"], scrollbar_button_hover_color=COLORS["accent"])
    workflow_scroll.grid(row=0, column=0, sticky="nsew")

    input_mode_card = make_card(
        workflow_scroll,
        "1. Choose input format",
        "Select one source for the bioactivity values. A summarized table and direct raw plate exports are alternatives; the script does not require both.",
    )
    input_type_combo = make_combo(input_mode_card, var_input_type, ["Table", "Plate reader"])
    input_type_combo.configure(command=lambda _choice: update_input_mode_visibility(reset_for_plate=True))
    labeled_widget(input_mode_card, "Input format", input_type_combo, 0, 0, help_text="Choose Table when rows are already samples with replicate columns. Choose Plate reader when you have raw well-plate exports and want the app to calculate replicate values from well positions.")

    data_card = make_card(
        workflow_scroll,
        "Prepared input table and columns",
        "Load the plate-reader table, choose the sample/species column, and define which columns are biological or technical replicates. The optional order table below is only needed when you want a specific sample order in the final figure.",
    )
    file_row(
        data_card,
        0,
        "Input table",
        var_input_path,
        lambda: browse_input(),
        "CSV, TSV, TXT, XLSX or XLS table. Rows should be samples/extracts/plants; replicate fluorescence/readout columns should be numeric.",
        load_cmd=lambda: load_input_columns(),
    )

    combo_sample = make_combo(data_card, var_sample_column, [var_sample_column.get() or ""])
    labeled_widget(
        data_card,
        "Sample / species column",
        combo_sample,
        1,
        0,
        help_text="Column used for x-axis labels. Typical names are Species, Sample, Plant species, sample_name, etc.",
        colspan=4,
    )

    entry_reps = make_entry(data_card, var_replicate_columns, "plate_1, plate_2")
    labeled_widget(
        data_card,
        "Replicate columns",
        entry_reps,
        2,
        0,
        help_text="Comma-separated numeric columns. These are averaged row-by-row before optional normalization to control rows.",
        colspan=4,
    )

    rep_buttons = ctk.CTkFrame(data_card, fg_color="transparent")
    rep_buttons.grid(row=3, column=1, columnspan=5, sticky="ew", padx=(0, 18), pady=(0, 8))
    make_button(rep_buttons, "Select numeric", lambda: select_numeric_columns_from_input(), width=130).pack(side="left", padx=(0, 8))
    make_button(rep_buttons, "Open column picker", lambda: set_active_tab("Preview / column picker"), width=160).pack(side="left", padx=(0, 8))
    make_button(rep_buttons, "Clear replicates", lambda: var_replicate_columns.set(""), width=130).pack(side="left")

    file_row(
        data_card,
        4,
        "Order table",
        var_order_path,
        lambda: browse_order(),
        "Optional. This is a small CSV/Excel file that tells the script how samples should be ordered on the x-axis. Use it when alphabetical/input-table order is not meaningful, for example when you want plants grouped by taxonomy, activity, extract type, or your thesis/publication layout.",
        load_cmd=lambda: load_order_columns(),
    )
    combo_order = make_combo(data_card, var_order_column, [var_order_column.get() or ""])
    labeled_widget(
        data_card,
        "Order column",
        combo_order,
        5,
        0,
        help_text="Column in the optional order table that contains the same sample/species names as the main input table. Rows in this column define the final x-axis order; samples not present in the order table are removed from the ordered plot.",
        colspan=4,
    )

    plate_card = make_card(
        workflow_scroll,
        "Direct plate-reader input",
        "Use this when the input is one or more raw well-plate exports instead of an already summarized sample table. The importer detects the numeric well grid, uses positive-control wells, scales each plate from 0 to 100, and creates replicate columns automatically.",
    )
    plate_picker = ctk.CTkFrame(plate_card, fg_color="transparent")
    plate_picker.grid(row=1, column=1, columnspan=5, sticky="ew", padx=(0, 18), pady=7)
    plate_picker.grid_columnconfigure(0, weight=1)
    make_entry(plate_picker, var_plate_files, "A_blue.xlsx; B_blue.xlsx").grid(row=0, column=0, sticky="ew", padx=(0, 8))
    make_button(plate_picker, "Browse", lambda: browse_plate_files(), width=82).grid(row=0, column=1)
    ctk.CTkLabel(plate_card, text="Plate files", font=FONT_LABEL, text_color=COLORS["muted"], anchor="w").grid(row=1, column=0, sticky="w", padx=(0, 10), pady=7)
    labeled_widget(plate_card, "Plate rows", make_entry(plate_card, var_plate_rows, "8"), 2, 0, help_text="Number of lettered rows in the assay plate. Use 8 for 96 wells, 4 for 24 wells, or 16 for 384 wells.")
    labeled_widget(plate_card, "Plate columns", make_entry(plate_card, var_plate_columns, "12"), 2, 2, help_text="Number of numbered columns in the assay plate. Use 12 for 96 wells, 6 for 24 wells, or 24 for 384 wells.")
    labeled_widget(plate_card, "Positive-control wells", make_entry(plate_card, var_plate_controls, "H11, H12"), 3, 0, help_text="Wells containing positive controls on each replicate plate. Use the per-file controls field below only for replicate plates that differ from this default.")
    plate_scale_combo = make_combo(plate_card, var_plate_scale_mode, list(plate_scale_labels))
    labeled_widget(plate_card, "Plate scaling", plate_scale_combo, 3, 2, help_text="Relative range within each plate makes the weakest and strongest non-control wells span 0 to 100 on every plate, which helps compare plates with different overall intensity. Percent of positive control preserves each measurement relative to the selected positive-control wells. Raw fluorescence performs no plate-level scaling.")
    labeled_widget(plate_card, "Per-file controls", make_entry(plate_card, var_plate_controls_by_file, "optional: 1:H11,H12; plate_2.xlsx:H10,H11"), 4, 0, help_text="Optional override when one replicate plate uses different positive-control wells. Use semicolon-separated entries keyed by replicate number or file name.", colspan=3)
    mapping_picker = ctk.CTkFrame(plate_card, fg_color="transparent")
    mapping_picker.grid(row=5, column=1, columnspan=5, sticky="ew", padx=(0, 18), pady=7)
    mapping_picker.grid_columnconfigure(0, weight=1)
    make_entry(mapping_picker, var_plate_mapping_file, "well_to_plant_mapping.xlsx").grid(row=0, column=0, sticky="ew", padx=(0, 8))
    make_button(mapping_picker, "Browse", lambda: browse_plate_mapping(), width=82).grid(row=0, column=1)
    ctk.CTkLabel(plate_card, text="Well mapping", font=FONT_LABEL, text_color=COLORS["muted"], anchor="w").grid(row=5, column=0, sticky="w", padx=(0, 10), pady=7)
    labeled_widget(plate_card, "Mapping sample column", make_entry(plate_card, var_plate_mapping_column, "plant"), 6, 0, help_text="Optional column in the mapping file that contains sample or plant names. The mapping file must also contain a well column such as A1, A2, etc. Leave mapping empty to use well IDs as sample names.", colspan=3)
    make_button(plate_card, "Preview plate import", lambda: preview_plate_import(), primary=True, width=170).grid(row=7, column=1, sticky="w", pady=(8, 0))

    def update_input_mode_visibility(*_args: Any, reset_for_plate: bool = False) -> None:
        is_plate = var_input_type.get() == "Plate reader"
        if is_plate:
            data_card.master.pack_forget()
            plate_card.master.pack(fill="x", padx=6, pady=(0, 12))
            if reset_for_plate:
                var_control_mode.set("None")
                var_control_entry_1.set("")
                var_control_entry_2.set("")
                var_exclude_control.set(False)
        else:
            plate_card.master.pack_forget()
            data_card.master.pack(fill="x", padx=6, pady=(0, 12))
        for row in range(2, 6):
            for widget in control_card.grid_slaves(row=row):
                if is_plate:
                    widget.grid_remove()
                else:
                    widget.grid()
        if not is_plate:
            update_control_inputs(preserve_exclude=True)
        save_current_state()

    control_card = make_card(
        workflow_scroll,
        "2. Activity highlighting and optional table controls",
        "The threshold highlights samples of interest. In prepared-table mode, control rows can also be selected here; in plate-reader mode, control wells are defined with the raw plates above.",
    )
    make_checkbox(
        control_card,
        "Highlight samples using an activity threshold",
        var_use_threshold,
        command=lambda: on_threshold_toggle(),
    ).grid(row=0, column=1, columnspan=3, sticky="w", pady=8)
    make_help(
        control_card,
        "Turn this on when you want the final figure to visually mark samples that pass your activity cutoff. The script adds Active/Inactive labels and colors the bars, so interesting samples stand out immediately. It does not remove any samples or change the measured values.",
    ).grid(row=0, column=4, sticky="w", pady=8)

    entry_threshold = make_entry(control_card, var_threshold, "80")
    labeled_widget(
        control_card,
        "Threshold",
        entry_threshold,
        1,
        0,
        help_text="Enter a number in the same units as the plotted y-axis. If controls are selected, the y-axis is Activity (% of control), so values like 80, 100, or 150 mean percent of the control. If no controls are selected, the threshold is applied to the raw mean signal, so enter an absolute fluorescence/readout value.",
    )
    combo_threshold_mode = make_combo(control_card, var_threshold_mode, ["ge", "le"])
    labeled_widget(
        control_card,
        "Threshold rule",
        combo_threshold_mode,
        1,
        2,
        help_text="Choose which side of the cutoff should be highlighted. Use ge when higher values mean stronger activity. Use le when lower values mean stronger inhibition, for example if active samples reduce the fluorescence signal.",
        colspan=2,
    )

    combo_control_mode = make_combo(control_card, var_control_mode, ["None", "Row numbers", "Sample names", "Column = value", "Query"])
    combo_control_mode.configure(command=lambda _value: (update_control_inputs(), on_preview_affecting_setting_changed("Control selection changed.")))
    labeled_widget(
        control_card,
        "Control mode",
        combo_control_mode,
        2,
        0,
        help_text="Choose how the script should find your control rows. Controls are averaged and used as 100% reference. Row numbers are 1-based like in Excel; sample names must match the sample/species column; Column = value is useful when your table has a column such as sample_type = control.",
        colspan=4,
    )

    control_label_frame_1 = ctk.CTkFrame(control_card, fg_color="transparent")
    control_label_frame_1.grid(row=3, column=0, sticky="ew", padx=(0, 10), pady=8)
    control_label_frame_1.grid_columnconfigure(0, weight=1)
    lbl_control_1 = ctk.CTkLabel(control_label_frame_1, text="Control values", font=FONT_LABEL, text_color=COLORS["muted"], anchor="w")
    lbl_control_1.grid(row=0, column=0, sticky="w")
    make_help(
        control_label_frame_1,
        "Enter controls according to the selected mode. For row numbers, use 1-based table rows such as 1, 2 or 1-3. For sample names, type exact names from the sample/species column. For Column = value, this field is the column that identifies control rows.",
    ).grid(row=0, column=1, sticky="e", padx=(6, 0))
    entry_control_1 = make_entry(control_card, var_control_entry_1)
    entry_control_1.grid(row=3, column=1, columnspan=5, sticky="ew", padx=(0, 18), pady=8)

    control_label_frame_2 = ctk.CTkFrame(control_card, fg_color="transparent")
    control_label_frame_2.grid(row=4, column=0, sticky="ew", padx=(0, 10), pady=8)
    control_label_frame_2.grid_columnconfigure(0, weight=1)
    lbl_control_2 = ctk.CTkLabel(control_label_frame_2, text="Control value", font=FONT_LABEL, text_color=COLORS["muted"], anchor="w")
    lbl_control_2.grid(row=0, column=0, sticky="w")
    make_help(
        control_label_frame_2,
        "Used only for Column = value mode. Enter the value that marks control rows in the selected control column, for example control, positive_control, or TRUE.",
    ).grid(row=0, column=1, sticky="e", padx=(6, 0))
    entry_control_2 = make_entry(control_card, var_control_entry_2)
    entry_control_2.grid(row=4, column=1, columnspan=5, sticky="ew", padx=(0, 18), pady=8)

    make_checkbox(
        control_card,
        "Exclude control rows from plot and exported table",
        var_exclude_control,
        command=lambda: on_preview_affecting_setting_changed("Control-row exclusion changed."),
    ).grid(row=5, column=1, columnspan=4, sticky="w", pady=(8, 0))
    make_help(
        control_card,
        "Recommended for final publication figures. Controls are still used to calculate 100% activity, but they are hidden from the final graph and exported CSV. Note: if you use an order table and the controls are not listed in its order column, they will disappear from the ordered plot anyway.",
    ).grid(row=5, column=4, sticky="w", padx=(8, 0), pady=(8, 0))

    plot_card = make_card(
        workflow_scroll,
        "3. Plot appearance and export",
        "Preview uses the same settings as export. SVG is recommended for publication editing; PNG is useful for quick inspection.",
    )
    labeled_widget(plot_card, "Title", make_entry(plot_card, var_title), 0, 0, help_text="Main figure title.", colspan=4)
    labeled_widget(plot_card, "Y label", make_entry(plot_card, var_ylabel), 1, 0, help_text="Leave empty to use the automatic label: Activity (% of control) or Mean signal.", colspan=4)

    labeled_widget(plot_card, "Width", make_entry(plot_card, var_figure_width), 2, 0, help_text="Figure width in inches.")
    labeled_widget(plot_card, "Height", make_entry(plot_card, var_figure_height), 2, 2, help_text="Figure height in inches.", colspan=2)
    labeled_widget(plot_card, "Bottom label tilt", make_entry(plot_card, var_rotate), 3, 0, help_text="X-axis label rotation in degrees. 45 is a good default for many plant names.")
    combo_font = make_combo(plot_card, var_font_family, ["Arial", "DejaVu Sans", "Liberation Sans", "Helvetica", "Calibri", "Times New Roman"])
    labeled_widget(plot_card, "Font family", combo_font, 3, 2, help_text="Arial is kept as default for consistency with your other publication figures.", colspan=2)

    labeled_widget(plot_card, "Title size", make_entry(plot_card, var_title_size), 4, 0, help_text="Font size for the plot title.")
    labeled_widget(plot_card, "Axis label size", make_entry(plot_card, var_axis_label_size), 4, 2, help_text="Font size for x/y axis titles.", colspan=2)
    labeled_widget(plot_card, "Bottom label size", make_entry(plot_card, var_xtick_label_size), 5, 0, help_text="Font size for plant/sample names on x-axis.")
    labeled_widget(plot_card, "Y tick size", make_entry(plot_card, var_ytick_label_size), 5, 2, help_text="Font size for y-axis tick labels.", colspan=2)
    labeled_widget(plot_card, "Legend size", make_entry(plot_card, var_legend_size), 6, 0, help_text="Font size for the optional activity/control legend.")

    file_row(
        plot_card,
        7,
        "Output prefix",
        var_output_prefix,
        lambda: choose_output_prefix(),
        "Path without extension. The GUI will append .csv, .png and/or .svg based on selected export formats.",
    )
    exports = ctk.CTkFrame(plot_card, fg_color=COLORS["card_alt"], corner_radius=12)
    exports.grid(row=8, column=1, columnspan=5, sticky="ew", padx=(0, 18), pady=(10, 4))
    exports.grid_columnconfigure((0, 1, 2), weight=1)
    make_checkbox(exports, "Export table (.csv)", var_export_table).grid(row=0, column=0, sticky="w", padx=12, pady=10)
    make_checkbox(exports, "Export PNG", var_export_png).grid(row=0, column=1, sticky="w", padx=12, pady=10)
    make_checkbox(exports, "Export SVG", var_export_svg).grid(row=0, column=2, sticky="w", padx=12, pady=10)

    # ---------------------------
    # Preview / column picker tab
    # ---------------------------
    preview_tab.grid_columnconfigure(0, weight=0)
    preview_tab.grid_columnconfigure(1, weight=1)
    preview_tab.grid_rowconfigure(0, weight=1)

    picker_panel = ctk.CTkFrame(preview_tab, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, corner_radius=16)
    picker_panel.grid(row=0, column=0, sticky="nsw", padx=(6, 12), pady=10)
    picker_panel.grid_columnconfigure(0, weight=1)
    picker_panel.grid_rowconfigure(5, weight=1)

    ctk.CTkLabel(picker_panel, text="Column picker", font=FONT_CARD_TITLE, text_color=COLORS["text"], anchor="w").grid(row=0, column=0, sticky="ew", padx=18, pady=(18, 4))
    ctk.CTkLabel(
        picker_panel,
        text="Inspect headers and send selected columns into the workflow fields. This is safer than typing long column names by hand.",
        font=FONT_SMALL,
        text_color=COLORS["muted"],
        wraplength=325,
        justify="left",
        anchor="w",
    ).grid(row=1, column=0, sticky="ew", padx=18, pady=(0, 14))

    combo_preview_source = make_combo(picker_panel, var_preview_source, ["Input table", "Order table"])
    combo_preview_source.configure(command=lambda _value: refresh_column_picker())
    combo_preview_source.grid(row=2, column=0, sticky="ew", padx=18, pady=(0, 8))
    entry_filter = make_entry(picker_panel, var_column_filter, "Filter columns...")
    entry_filter.grid(row=3, column=0, sticky="ew", padx=18, pady=(0, 8))
    var_column_filter.trace_add("write", lambda *_args: refresh_column_picker())

    picker_actions_top = ctk.CTkFrame(picker_panel, fg_color="transparent")
    picker_actions_top.grid(row=4, column=0, sticky="ew", padx=18, pady=(0, 8))
    make_button(picker_actions_top, "Load input", lambda: load_input_columns(), width=104).pack(side="left", padx=(0, 8))
    make_button(picker_actions_top, "Load order", lambda: load_order_columns(), width=104).pack(side="left", padx=(0, 8))
    make_button(picker_actions_top, "Numeric", lambda: select_numeric_columns_from_input(), width=90).pack(side="left")

    listbox_frame = ctk.CTkFrame(picker_panel, fg_color=COLORS["entry"], border_color=COLORS["border"], border_width=1, corner_radius=10)
    listbox_frame.grid(row=5, column=0, sticky="nsew", padx=18, pady=(0, 10))
    listbox_frame.grid_columnconfigure(0, weight=1)
    listbox_frame.grid_rowconfigure(0, weight=1)
    column_list = tk.Listbox(
        listbox_frame,
        selectmode=tk.EXTENDED,
        bg=COLORS["entry"],
        fg=COLORS["text"],
        selectbackground=COLORS["tile_selected"],
        selectforeground=COLORS["text"],
        activestyle="none",
        highlightthickness=0,
        relief="flat",
        font=FONT_MONO,
        exportselection=False,
    )
    column_list.grid(row=0, column=0, sticky="nsew", padx=(12, 0), pady=12)
    column_y_scroll = tk.Scrollbar(listbox_frame, orient="vertical", command=column_list.yview)
    column_y_scroll.grid(row=0, column=1, sticky="ns", padx=(0, 12), pady=12)
    column_x_scroll = tk.Scrollbar(listbox_frame, orient="horizontal", command=column_list.xview)
    column_x_scroll.grid(row=1, column=0, sticky="ew", padx=(12, 0), pady=(0, 12))
    column_list.configure(yscrollcommand=column_y_scroll.set, xscrollcommand=column_x_scroll.set)

    picker_actions = ctk.CTkFrame(picker_panel, fg_color="transparent")
    picker_actions.grid(row=6, column=0, sticky="ew", padx=18, pady=(0, 18))
    picker_actions.grid_columnconfigure((0, 1), weight=1)
    make_button(picker_actions, "Set sample column", lambda: apply_selected_columns("sample"), width=150).grid(row=0, column=0, sticky="ew", padx=(0, 8), pady=(0, 8))
    make_button(picker_actions, "Set order column", lambda: apply_selected_columns("order"), width=150).grid(row=0, column=1, sticky="ew", pady=(0, 8))
    make_button(picker_actions, "Replace replicates", lambda: apply_selected_columns("rep_replace"), width=150).grid(row=1, column=0, sticky="ew", padx=(0, 8))
    make_button(picker_actions, "Add to replicates", lambda: apply_selected_columns("rep_add"), width=150).grid(row=1, column=1, sticky="ew")

    preview_right = ctk.CTkFrame(preview_tab, fg_color="transparent")
    preview_right.grid(row=0, column=1, sticky="nsew", padx=(0, 6), pady=10)
    preview_right.grid_columnconfigure(0, weight=1)
    preview_right.grid_rowconfigure(3, weight=1)

    summary_card = ctk.CTkFrame(preview_right, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, corner_radius=16)
    summary_card.grid(row=0, column=0, sticky="ew", pady=(0, 12))
    summary_card.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(summary_card, text="Current selection", font=FONT_CARD_TITLE, text_color=COLORS["text"], anchor="w").grid(row=0, column=0, sticky="ew", padx=18, pady=(16, 4))
    ctk.CTkLabel(summary_card, textvariable=var_column_summary, font=FONT_SMALL, text_color=COLORS["muted"], anchor="w", justify="left", wraplength=850).grid(row=1, column=0, sticky="ew", padx=18, pady=(0, 16))

    ctk.CTkLabel(preview_right, text="Table preview", font=FONT_CARD_TITLE, text_color=COLORS["text"], anchor="w").grid(row=1, column=0, sticky="ew", pady=(0, 6))
    ctk.CTkLabel(preview_right, text="Shows the first rows of the selected source table. Use this to verify that the correct file was loaded.", font=FONT_SMALL, text_color=COLORS["muted"], anchor="w").grid(row=2, column=0, sticky="ew", pady=(0, 6))
    txt_table_preview = ctk.CTkTextbox(preview_right, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, text_color=COLORS["text"], font=FONT_MONO, corner_radius=16, wrap="none")
    txt_table_preview.grid(row=3, column=0, sticky="nsew")
    txt_table_preview.insert("end", "Load a table to preview rows here.\n")
    txt_table_preview.configure(state="disabled")

    # ---------------------------
    # Figures tab
    # ---------------------------
    figures_tab.grid_columnconfigure(0, weight=0)
    figures_tab.grid_columnconfigure(1, weight=1)
    figures_tab.grid_rowconfigure(0, weight=1)

    figure_side = ctk.CTkFrame(figures_tab, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, corner_radius=16)
    figure_side.grid(row=0, column=0, sticky="nsw", padx=(6, 12), pady=10)
    figure_side.grid_columnconfigure(0, weight=1)
    figure_side.grid_rowconfigure(2, weight=1)
    ctk.CTkLabel(figure_side, text="Generated figures", font=FONT_CARD_TITLE, text_color=COLORS["text"]).grid(row=0, column=0, sticky="w", padx=18, pady=(18, 4))
    ctk.CTkLabel(
        figure_side,
        text="Preview and export runs appear here. Select one to display it again or open saved files.",
        font=FONT_SMALL,
        text_color=COLORS["muted"],
        wraplength=285,
        justify="left",
    ).grid(row=1, column=0, sticky="w", padx=18, pady=(0, 14))

    figure_list_box = ctk.CTkFrame(figure_side, fg_color=COLORS["entry"], border_color=COLORS["border"], border_width=1, corner_radius=10)
    figure_list_box.grid(row=2, column=0, sticky="nsew", padx=18, pady=(0, 12))
    figure_list_box.grid_columnconfigure(0, weight=1)
    figure_list_box.grid_rowconfigure(0, weight=1)
    figure_list = tk.Listbox(
        figure_list_box,
        selectmode=tk.BROWSE,
        bg=COLORS["entry"],
        fg=COLORS["text"],
        selectbackground=COLORS["tile_selected"],
        selectforeground=COLORS["text"],
        activestyle="none",
        highlightthickness=0,
        relief="flat",
        font=FONT_SMALL,
        exportselection=False,
    )
    figure_list.grid(row=0, column=0, sticky="nsew", padx=(12, 0), pady=12)
    figure_scroll = tk.Scrollbar(figure_list_box, orient="vertical", command=figure_list.yview)
    figure_scroll.grid(row=0, column=1, sticky="ns", padx=(0, 12), pady=12)
    figure_list.configure(yscrollcommand=figure_scroll.set)
    figure_list.bind("<<ListboxSelect>>", lambda _event: show_selected_figure())

    make_button(figure_side, "Open selected file", lambda: open_selected_figure_file(), width=150).grid(row=3, column=0, sticky="ew", padx=18, pady=(4, 8))
    make_button(figure_side, "Open output folder", lambda: open_output_folder(), width=150).grid(row=4, column=0, sticky="ew", padx=18, pady=(0, 18))

    figure_preview_panel = ctk.CTkFrame(figures_tab, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, corner_radius=16)
    figure_preview_panel.grid(row=0, column=1, sticky="nsew", padx=(0, 6), pady=10)
    figure_preview_panel.grid_columnconfigure(0, weight=1)
    figure_preview_panel.grid_rowconfigure(2, weight=1)
    figure_title_row = ctk.CTkFrame(figure_preview_panel, fg_color="transparent")
    figure_title_row.grid(row=0, column=0, sticky="ew", padx=18, pady=(18, 4))
    figure_title_row.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(figure_title_row, text="Figure preview", font=FONT_CARD_TITLE, text_color=COLORS["text"], anchor="w").grid(row=0, column=0, sticky="w")
    make_button(figure_title_row, "Reset zoom", lambda: reset_preview_zoom(), width=105).grid(row=0, column=1, sticky="e", padx=(8, 0))
    make_help(figure_title_row, "Resets the interactive preview to the full figure. This is the same action as the house-shaped Home icon in the Matplotlib toolbar below the plot.").grid(row=0, column=2, sticky="e", padx=(8, 0))
    ctk.CTkLabel(figure_preview_panel, textvariable=var_preview_summary, font=FONT_SMALL, text_color=COLORS["muted"], anchor="w", justify="left", wraplength=900).grid(row=1, column=0, sticky="ew", padx=18, pady=(0, 8))
    figure_canvas_host = ctk.CTkFrame(figure_preview_panel, fg_color="#ffffff", corner_radius=10)
    figure_canvas_host.grid(row=2, column=0, sticky="nsew", padx=18, pady=(0, 10))
    figure_canvas_host.grid_columnconfigure(0, weight=1)
    figure_canvas_host.grid_rowconfigure(0, weight=1)
    figure_placeholder = ctk.CTkLabel(figure_canvas_host, text="No figure generated yet.", text_color="#404040", font=("Segoe UI", 13))
    figure_placeholder.grid(row=0, column=0, sticky="nsew")
    ctk.CTkLabel(figure_preview_panel, textvariable=var_figure_path_text, font=FONT_SMALL, text_color=COLORS["muted"], anchor="w", justify="left", wraplength=900).grid(row=3, column=0, sticky="ew", padx=18, pady=(0, 18))

    # ---------------------------
    # Log tab
    # ---------------------------
    log_tab.grid_columnconfigure(0, weight=1)
    log_tab.grid_rowconfigure(0, weight=1)
    txt_log = ctk.CTkTextbox(log_tab, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, text_color=COLORS["text"], font=FONT_MONO, corner_radius=16, wrap="word")
    txt_log.grid(row=0, column=0, sticky="nsew", padx=6, pady=10)
    txt_log.insert("end", "Ready. Load an input table, inspect columns, then preview or export the plant bioactivity figure.\n")
    txt_log.configure(state="disabled")

    status_bar = ctk.CTkFrame(root, fg_color=COLORS["surface"], corner_radius=0)
    status_bar.grid(row=2, column=0, sticky="ew")
    status_bar.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(status_bar, textvariable=var_status, font=FONT_SMALL, text_color=COLORS["muted"], anchor="w").grid(row=0, column=0, sticky="ew", padx=18, pady=8)

    # ---------------------------
    # Logic
    # ---------------------------
    def update_column_summary() -> None:
        rep_cols = split_csv(var_replicate_columns.get())
        lines = [
            f"Input columns loaded: {len(input_df.columns) if input_df is not None else 0}",
            f"Sample column: {var_sample_column.get().strip() or 'not selected'}",
            f"Replicate columns: {len(rep_cols)}" + (f"  ({join_csv(rep_cols[:6])}{' ...' if len(rep_cols) > 6 else ''})" if rep_cols else ""),
        ]
        if order_df is not None:
            lines.append(f"Order table loaded: {len(order_df)} rows; order column: {var_order_column.get().strip() or 'not selected'}")
        var_column_summary.set("\n".join(lines))

    def set_table_preview(df: pd.DataFrame | None, source_name: str) -> None:
        txt_table_preview.configure(state="normal")
        txt_table_preview.delete("1.0", "end")
        if df is None:
            txt_table_preview.insert("end", f"No {source_name.lower()} loaded yet.\n")
        else:
            txt_table_preview.insert("end", f"{source_name}: {len(df)} rows × {len(df.columns)} columns\n\n")
            txt_table_preview.insert("end", table_preview_text(df))
        txt_table_preview.configure(state="disabled")

    def refresh_column_picker() -> None:
        nonlocal all_columns, visible_columns, current_preview_df
        source = var_preview_source.get()
        current_preview_df = input_df if source == "Input table" else order_df
        all_columns = list(current_preview_df.columns) if current_preview_df is not None else []
        query = var_column_filter.get().strip().lower()
        visible_columns = [c for c in all_columns if not query or query in c.lower()]
        column_list.delete(0, tk.END)
        for col in visible_columns:
            marker = ""
            if current_preview_df is not None:
                series = current_preview_df[col]
                if pd.api.types.is_numeric_dtype(series):
                    marker = "  [numeric]"
                else:
                    numeric_values = pd.to_numeric(series, errors="coerce")
                    if numeric_values.notna().sum() >= max(1, int(0.5 * len(series))):
                        marker = "  [numeric-like]"
            column_list.insert(tk.END, f"{col}{marker}")
        set_table_preview(current_preview_df, source)
        update_column_summary()

    def selected_columns_from_picker() -> list[str]:
        out: list[str] = []
        for i in column_list.curselection():
            if 0 <= i < len(visible_columns):
                out.append(visible_columns[i])
        return out

    def apply_selected_columns(mode: str) -> None:
        selected = selected_columns_from_picker()
        if not selected:
            show_toast("Select one or more columns first.", kind="warning")
            return
        if mode == "sample":
            var_sample_column.set(selected[0])
            show_toast(f"Sample column set to: {selected[0]}", kind="success")
        elif mode == "order":
            var_order_column.set(selected[0])
            show_toast(f"Order column set to: {selected[0]}", kind="success")
        elif mode == "rep_replace":
            var_replicate_columns.set(join_csv(selected))
            show_toast(f"Replicate list replaced ({len(selected)} columns).", kind="success")
        elif mode == "rep_add":
            existing = split_csv(var_replicate_columns.get())
            merged = existing + [c for c in selected if c not in existing]
            var_replicate_columns.set(join_csv(merged))
            show_toast(f"Added {len([c for c in selected if c not in existing])} replicate column(s).", kind="success")
        save_current_state()
        update_column_summary()

    def suggest_output_prefix(path: Path) -> None:
        current = var_output_prefix.get().strip()
        default_start = str(_THIS_DIR / "output" / "plant_bioactivity")
        if not current or current == default_start:
            var_output_prefix.set(str(default_output_prefix(path)))

    def browse_input() -> None:
        path = filedialog.askopenfilename(title="Choose input table", initialdir=dialog_initial_dir(var_input_path.get()), filetypes=TABLE_PATTERNS)
        if path:
            var_input_path.set(path)
            suggest_output_prefix(Path(path))
            load_input_columns(auto_log=False)

    def browse_order() -> None:
        path = filedialog.askopenfilename(title="Choose order table", initialdir=dialog_initial_dir(var_order_path.get()), filetypes=TABLE_PATTERNS)
        if path:
            var_order_path.set(path)
            load_order_columns(auto_log=False)

    def browse_plate_files() -> None:
        paths = filedialog.askopenfilenames(title="Choose replicate plate-reader files", initialdir=str(_THIS_DIR), filetypes=TABLE_PATTERNS)
        if paths:
            var_plate_files.set("; ".join(paths))
            save_current_state()

    def browse_plate_mapping() -> None:
        path = filedialog.askopenfilename(title="Choose well-to-sample mapping table", initialdir=dialog_initial_dir(var_plate_mapping_file.get()), filetypes=TABLE_PATTERNS)
        if path:
            var_plate_mapping_file.set(path)
            save_current_state()

    def choose_output_prefix() -> None:
        current = Path(var_output_prefix.get().strip()).expanduser() if var_output_prefix.get().strip() else (_THIS_DIR / "output" / "plant_bioactivity")
        path = filedialog.asksaveasfilename(
            title="Choose output prefix",
            initialdir=str(current.parent if current.parent.exists() else default_output_dir()),
            initialfile=current.name,
            defaultextension="",
            filetypes=[("All files", "*.*")],
        )
        if path:
            out = Path(path)
            if out.suffix:
                out = out.with_suffix("")
            var_output_prefix.set(str(out))
            save_current_state()

    def save_config_file() -> None:
        suggested = f"plant_bioactivity_config_{datetime.now().strftime('%Y%m%d')}.json"
        path = filedialog.asksaveasfilename(
            title="Save GUI configuration",
            initialdir=str(default_output_dir()),
            initialfile=suggested,
            defaultextension=".json",
            filetypes=CONFIG_PATTERNS,
        )
        if not path:
            return
        payload = {
            "script": "p_00_plant_bioactivity",
            "config_version": 1,
            "saved_at": datetime.now().isoformat(timespec="seconds"),
            "settings": current_config_dict(),
        }
        try:
            Path(path).write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:
            show_toast("Could not save config.", kind="error")
            messagebox.showerror("Could not save config", str(exc))
            return
        save_current_state()
        append_log(f"Saved GUI configuration: {path}")
        show_toast("Configuration saved.", kind="success")

    def load_config_file() -> None:
        path = filedialog.askopenfilename(
            title="Load GUI configuration",
            initialdir=str(default_output_dir()),
            filetypes=CONFIG_PATTERNS,
        )
        if not path:
            return
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
            settings = payload.get("settings", payload)
            if not isinstance(settings, dict):
                raise ValueError("The selected JSON file does not contain GUI settings.")
            apply_config_dict(settings)
            if var_input_path.get().strip() and Path(var_input_path.get().strip()).expanduser().exists():
                load_input_columns(auto_log=False, auto_select_replicates=False)
                # Keep values exactly as stored in the config after column menus are refreshed.
                var_sample_column.set(str(settings.get("sample_column", var_sample_column.get()) or var_sample_column.get()))
                var_replicate_columns.set(str(settings.get("replicate_columns", var_replicate_columns.get()) or var_replicate_columns.get()))
            if var_order_path.get().strip() and Path(var_order_path.get().strip()).expanduser().exists():
                load_order_columns(auto_log=False)
                var_order_column.set(str(settings.get("order_column", var_order_column.get()) or var_order_column.get()))
            update_threshold_state()
            update_control_inputs(preserve_exclude=True)
            refresh_column_picker()
            save_current_state()
        except Exception as exc:
            show_toast("Could not load config.", kind="error")
            messagebox.showerror("Could not load config", str(exc))
            return
        append_log(f"Loaded GUI configuration: {path}")
        set_status("Configuration loaded.")
        show_toast("Configuration loaded.", kind="success")

    def on_preview_affecting_setting_changed(message: str = "Setting changed.") -> None:
        save_current_state()
        if figure_records:
            show_toast(f"{message} Click Preview figure to refresh the graph.", kind="info")
        else:
            set_status(message)

    def on_threshold_toggle() -> None:
        update_threshold_state()
        on_preview_affecting_setting_changed("Activity-threshold setting changed.")

    def load_input_columns(auto_log: bool = True, auto_select_replicates: bool | None = None) -> None:
        nonlocal input_df
        path_text = var_input_path.get().strip()
        if not path_text:
            show_toast("Choose an input table first.", kind="warning")
            return
        try:
            df = core.read_table(Path(path_text))
        except Exception as exc:
            show_toast("Could not read input table.", kind="error")
            messagebox.showerror("Could not read input table", str(exc))
            return

        input_df = df
        columns = list(df.columns)
        combo_sample.configure(values=columns or [""])
        if columns:
            var_sample_column.set(guess_sample_column(columns, var_sample_column.get()))
        if auto_select_replicates is None:
            auto_select_replicates = not bool(var_replicate_columns.get().strip())
        if auto_select_replicates:
            select_numeric_columns_from_input(show_message=False)
        suggest_output_prefix(Path(path_text))
        var_preview_source.set("Input table")
        refresh_column_picker()
        save_current_state()
        msg = f"Loaded input table: {len(df)} rows × {len(columns)} columns."
        append_log(msg)
        if auto_log:
            show_toast(msg, kind="success")
        else:
            show_toast("Input table loaded.", kind="success")

    def preview_plate_import() -> None:
        nonlocal input_df, plate_preview_df
        try:
            files = split_paths(var_plate_files.get())
            if not files:
                raise ValueError("Choose at least one replicate plate-reader file.")
            missing = [path for path in files if not Path(path).expanduser().exists()]
            if missing:
                raise ValueError(f"Plate-reader file not found: {missing[0]}")
            mapping_file = var_plate_mapping_file.get().strip() or None
            if mapping_file and not Path(mapping_file).expanduser().exists():
                raise ValueError("The selected well mapping file does not exist.")
            table, replicate_columns = core.build_plate_reader_sample_table(
                files,
                plate_rows=int(parse_float("plate rows", var_plate_rows.get())),
                plate_columns=int(parse_float("plate columns", var_plate_columns.get())),
                positive_control_wells=var_plate_controls.get().strip(),
                positive_control_wells_by_file=var_plate_controls_by_file.get().strip() or None,
                plate_scale_mode=plate_scale_code(var_plate_scale_mode.get().strip()) or "positive_control_then_minmax_0_100",
                mapping_file=mapping_file,
                mapping_sample_column=var_plate_mapping_column.get().strip() or None,
            )
        except Exception as exc:
            show_toast("Plate import failed.", kind="error")
            messagebox.showerror("Plate import failed", str(exc))
            return

        plate_preview_df = table
        input_df = table
        columns = list(table.columns)
        combo_sample.configure(values=columns or [""])
        var_sample_column.set("sample")
        var_replicate_columns.set(join_csv(replicate_columns))
        var_input_type.set("Plate reader")
        var_preview_source.set("Plate-reader import")
        refresh_column_picker()
        update_column_summary()
        save_current_state()
        append_log(f"Plate import ready: {len(table)} sample/well row(s), {len(replicate_columns)} replicate column(s).")
        show_toast("Plate-reader import ready.", kind="success")

    def load_order_columns(auto_log: bool = True) -> None:
        nonlocal order_df
        path_text = var_order_path.get().strip()
        if not path_text:
            show_toast("Choose an order table first.", kind="warning")
            return
        try:
            df = core.read_table(Path(path_text))
        except Exception as exc:
            show_toast("Could not read order table.", kind="error")
            messagebox.showerror("Could not read order table", str(exc))
            return
        order_df = df
        columns = list(df.columns)
        combo_order.configure(values=columns or [""])
        if columns:
            var_order_column.set(guess_sample_column(columns, var_order_column.get()))
        var_preview_source.set("Order table")
        refresh_column_picker()
        save_current_state()
        msg = f"Loaded order table: {len(df)} rows × {len(columns)} columns."
        append_log(msg)
        if auto_log:
            show_toast(msg, kind="success")

    def select_numeric_columns_from_input(show_message: bool = True) -> None:
        if input_df is None:
            if show_message:
                show_toast("Load an input table first.", kind="warning")
            return
        sample_col = var_sample_column.get().strip()
        candidates: list[str] = []
        for col in input_df.columns:
            if col == sample_col:
                continue
            series = input_df[col]
            is_candidate = pd.api.types.is_numeric_dtype(series)
            if not is_candidate:
                numeric_values = pd.to_numeric(series, errors="coerce")
                is_candidate = numeric_values.notna().sum() >= max(1, int(0.5 * len(series)))
            low = col.lower()
            if is_candidate or any(tag in low for tag in ["rep", "plate", "fluor", "signal", "readout", "avg"]):
                candidates.append(col)
        var_replicate_columns.set(join_csv(candidates))
        update_column_summary()
        save_current_state()
        if show_message:
            show_toast(f"Selected {len(candidates)} numeric/numeric-like replicate column(s).", kind="success")
            append_log(f"Selected replicate candidates: {join_csv(candidates)}")

    def update_threshold_state() -> None:
        enabled = var_use_threshold.get()
        entry_threshold.configure(state="normal" if enabled else "disabled")
        combo_threshold_mode.configure(state="normal" if enabled else "disabled")

    def update_control_inputs(*, preserve_exclude: bool = False) -> None:
        mode = var_control_mode.get()
        if mode == "None":
            lbl_control_1.configure(text="Control values")
            entry_control_1.configure(placeholder_text="Controls disabled")
            entry_control_1.configure(state="disabled")
            control_label_frame_2.grid_remove()
            entry_control_2.grid_remove()
            if not preserve_exclude:
                var_exclude_control.set(False)
        elif mode == "Row numbers":
            lbl_control_1.configure(text="Control row numbers")
            entry_control_1.configure(state="normal", placeholder_text="1, 2 or 1-3,5")
            control_label_frame_2.grid_remove()
            entry_control_2.grid_remove()
        elif mode == "Sample names":
            lbl_control_1.configure(text="Control sample names")
            entry_control_1.configure(state="normal", placeholder_text="positive_control, solvent_control")
            control_label_frame_2.grid_remove()
            entry_control_2.grid_remove()
        elif mode == "Column = value":
            lbl_control_1.configure(text="Control column")
            entry_control_1.configure(state="normal", placeholder_text="sample_type")
            lbl_control_2.configure(text="Control value")
            control_label_frame_2.grid()
            entry_control_2.grid()
            entry_control_2.configure(state="normal", placeholder_text="control")
        elif mode == "Query":
            lbl_control_1.configure(text="Control query / mask")
            entry_control_1.configure(state="normal", placeholder_text="sample_type == 'control'")
            control_label_frame_2.grid_remove()
            entry_control_2.grid_remove()

    def collect_settings() -> dict[str, Any]:
        input_path = var_input_path.get().strip()
        table_for_core: pd.DataFrame | None = None
        if var_input_type.get() == "Plate reader":
            files = split_paths(var_plate_files.get())
            if not files:
                raise ValueError("Choose at least one replicate plate-reader file.")
            missing = [path for path in files if not Path(path).expanduser().exists()]
            if missing:
                raise ValueError(f"Plate-reader file not found: {missing[0]}")
            mapping_file = var_plate_mapping_file.get().strip() or None
            table_for_core, replicate_columns = core.build_plate_reader_sample_table(
                files,
                plate_rows=int(parse_float("plate rows", var_plate_rows.get())),
                plate_columns=int(parse_float("plate columns", var_plate_columns.get())),
                positive_control_wells=var_plate_controls.get().strip(),
                positive_control_wells_by_file=var_plate_controls_by_file.get().strip() or None,
                plate_scale_mode=plate_scale_code(var_plate_scale_mode.get().strip()) or "positive_control_then_minmax_0_100",
                mapping_file=mapping_file,
                mapping_sample_column=var_plate_mapping_column.get().strip() or None,
            )
            input_path = files[0]
            sample_column = "sample"
        else:
            if not input_path:
                raise ValueError("Choose an input table.")
            sample_column = var_sample_column.get().strip()
            if not sample_column:
                raise ValueError("Choose the sample/species column.")
            replicate_columns = split_csv(var_replicate_columns.get())
            if not replicate_columns:
                raise ValueError("Choose at least one replicate column.")

        figure_size = (
            parse_float("figure width", var_figure_width.get()),
            parse_float("figure height", var_figure_height.get()),
        )
        rotate_labels = parse_float("bottom label tilt", var_rotate.get())
        plot_style = {
            "font_family": var_font_family.get().strip() or "Arial",
            "title_size": parse_float("title size", var_title_size.get()),
            "axis_label_size": parse_float("axis label size", var_axis_label_size.get()),
            "xtick_label_size": parse_float("bottom label size", var_xtick_label_size.get()),
            "ytick_label_size": parse_float("Y tick size", var_ytick_label_size.get()),
            "legend_size": parse_float("legend size", var_legend_size.get()),
        }

        threshold = None
        if var_use_threshold.get():
            threshold = parse_optional_float("threshold", var_threshold.get())
            if threshold is None:
                raise ValueError("Threshold is enabled, but no threshold value was entered.")

        control_mode = var_control_mode.get()
        control_row_indices = None
        control_sample_names = None
        control_column = None
        control_value = None
        control_query = None
        if control_mode == "Row numbers":
            text = var_control_entry_1.get().strip()
            if not text:
                raise ValueError("Enter at least one control row number.")
            try:
                control_row_indices = parse_row_numbers(text)
            except ValueError as exc:
                raise ValueError("Control row numbers must be integers or ranges like 1-3,5.") from exc
        elif control_mode == "Sample names":
            control_sample_names = split_csv(var_control_entry_1.get())
            if not control_sample_names:
                raise ValueError("Enter at least one control sample name.")
        elif control_mode == "Column = value":
            control_column = var_control_entry_1.get().strip()
            control_value = var_control_entry_2.get().strip()
            if not control_column or not control_value:
                raise ValueError("Fill both control column and control value.")
        elif control_mode == "Query":
            control_query = var_control_entry_1.get().strip()
            if not control_query:
                raise ValueError("Enter a control query / mask.")

        order_path = var_order_path.get().strip() or None
        order_column = var_order_column.get().strip() or None
        output_prefix = var_output_prefix.get().strip() or None
        return {
            "input_path": Path(input_path),
            "input_df": table_for_core,
            "sample_column": sample_column,
            "replicate_columns": replicate_columns,
            "order_file": Path(order_path) if order_path else None,
            "order_column": order_column,
            "threshold": threshold,
            "threshold_mode": var_threshold_mode.get().strip() or "ge",
            "control_row_indices": control_row_indices,
            "control_sample_names": control_sample_names,
            "control_column": control_column,
            "control_value": control_value,
            "control_query": control_query,
            "exclude_control_from_plot": bool(var_exclude_control.get()),
            "title": var_title.get().strip() or "Sample activity",
            "ylabel": var_ylabel.get().strip() or None,
            "figure_size": figure_size,
            "rotate_labels": rotate_labels,
            "plot_style": plot_style,
            "output_prefix": Path(output_prefix) if output_prefix else None,
            "export_table": bool(var_export_table.get()),
            "export_png": bool(var_export_png.get()),
            "export_svg": bool(var_export_svg.get()),
        }

    def set_preview_summary(processed_df: pd.DataFrame, value_column: str, control_mean: float | None, saved_paths: list[Path] | None = None) -> None:
        lines = [
            f"Rows plotted: {len(processed_df)}",
            f"Value column: {value_column}",
        ]
        if control_mean is not None:
            lines.append(f"Control mean: {control_mean:.4f}")
        if "activity_class" in processed_df.columns:
            counts = processed_df["activity_class"].value_counts().to_dict()
            lines.append(f"Classes: {counts}")
        if saved_paths:
            lines.append("Saved: " + "; ".join(str(p) for p in saved_paths))
        var_preview_summary.set("\n".join(lines))

    def clear_figure_canvas() -> None:
        if current_canvas["canvas"] is not None:
            try:
                current_canvas["canvas"].get_tk_widget().destroy()
            except Exception:
                pass
            current_canvas["canvas"] = None
        if current_canvas["toolbar"] is not None:
            try:
                current_canvas["toolbar"].destroy()
            except Exception:
                pass
            current_canvas["toolbar"] = None

    def reset_preview_zoom() -> None:
        toolbar = current_canvas.get("toolbar")
        canvas = current_canvas.get("canvas")
        if toolbar is None or canvas is None:
            show_toast("Generate a preview first.", kind="warning")
            return
        try:
            toolbar.home()
            canvas.draw_idle()
            show_toast("Preview zoom reset.", kind="success")
        except Exception as exc:
            show_toast(f"Could not reset preview zoom: {exc}", kind="error")

    def display_figure(fig: Any) -> None:
        figure_placeholder.grid_remove()
        clear_figure_canvas()
        current_canvas["figure"] = fig
        canvas = FigureCanvasTkAgg(fig, master=figure_canvas_host)
        canvas.draw()
        canvas.get_tk_widget().grid(row=0, column=0, sticky="nsew")
        toolbar = NavigationToolbar2Tk(canvas, figure_canvas_host, pack_toolbar=False)
        toolbar.update()
        toolbar.grid(row=1, column=0, sticky="ew")
        current_canvas["canvas"] = canvas
        current_canvas["toolbar"] = toolbar

    def add_figure_record(fig: Any, label: str, saved_paths: list[Path] | None = None) -> None:
        figure_records.append({"label": label, "figure": fig, "saved_paths": saved_paths or []})
        figure_list.insert(tk.END, label)
        figure_list.selection_clear(0, tk.END)
        figure_list.selection_set(tk.END)
        figure_list.see(tk.END)
        if active_tab["name"] != "Figures":
            unread_figure_events["count"] += 1
            update_figure_badge()
        show_selected_figure()

    def show_selected_figure() -> None:
        sel = figure_list.curselection()
        if not sel:
            return
        idx = sel[0]
        if idx < 0 or idx >= len(figure_records):
            return
        record = figure_records[idx]
        display_figure(record["figure"])
        saved = record.get("saved_paths") or []
        var_figure_path_text.set("\n".join(str(p) for p in saved) if saved else "Preview figure only; no files saved for this record.")

    def run_core_pipeline(settings: dict[str, Any]) -> tuple[pd.DataFrame, Any, str, float | None]:
        return core.run_pipeline(
            input_path=settings["input_path"],
            sample_column=settings["sample_column"],
            replicate_columns=settings["replicate_columns"],
            order_file=settings["order_file"],
            order_column=settings["order_column"],
            threshold=settings["threshold"],
            threshold_mode=settings["threshold_mode"],
            control_row_indices=settings["control_row_indices"],
            control_sample_names=settings["control_sample_names"],
            control_column=settings["control_column"],
            control_value=settings["control_value"],
            control_query=settings["control_query"],
            exclude_control_from_plot=settings["exclude_control_from_plot"],
            title=settings["title"],
            ylabel=settings["ylabel"],
            figure_size=settings["figure_size"],
            rotate_labels=settings["rotate_labels"],
            plot_style=settings["plot_style"],
            input_df=settings.get("input_df"),
        )

    def preview_figure() -> None:
        try:
            settings = collect_settings()
            save_current_state()
            set_status("Previewing figure...")
            append_log("Previewing figure...", notify=False)
            processed_df, fig, value_column, control_mean = run_core_pipeline(settings)
            set_preview_summary(processed_df, value_column, control_mean)
            add_figure_record(fig, f"Preview {datetime.now().strftime('%H:%M:%S')}")
            append_log(f"Preview ready: {len(processed_df)} rows plotted; value column = {value_column}.")
            set_status("Preview ready.")
            set_active_tab("Figures")
            show_toast("Preview ready.", kind="success")
        except Exception as exc:
            set_status("Preview failed.")
            append_log("ERROR during preview:\n" + traceback.format_exc())
            show_toast("Preview failed.", kind="error")
            messagebox.showerror("Preview failed", str(exc))

    def run_and_export() -> None:
        try:
            settings = collect_settings()
            if not any([settings["export_table"], settings["export_png"], settings["export_svg"]]):
                raise ValueError("Choose at least one export format.")
            save_current_state()
        except Exception as exc:
            show_toast("Settings are incomplete.", kind="error")
            messagebox.showerror("Settings problem", str(exc))
            return

        def worker() -> None:
            try:
                log_queue.put("Running analysis and exporting files...")
                processed_df, fig, value_column, control_mean = run_core_pipeline(settings)
                output_prefix = settings["output_prefix"] or default_output_prefix(settings["input_path"])
                saved_paths = core.save_outputs(
                    fig=fig,
                    df=processed_df,
                    output_prefix=output_prefix,
                    export_table=settings["export_table"],
                    export_png=settings["export_png"],
                    export_svg=settings["export_svg"],
                )
                schedule_after(0, lambda: finish_export_success(processed_df, fig, value_column, control_mean, saved_paths))
            except Exception as exc:
                schedule_after(0, lambda e=exc: finish_export_error(e))

        set_status("Running analysis and exporting files...")
        make_button_preview.configure(state="disabled")
        make_button_run.configure(state="disabled")
        threading.Thread(target=worker, daemon=True).start()

    def finish_export_success(processed_df: pd.DataFrame, fig: Any, value_column: str, control_mean: float | None, saved_paths: list[Path]) -> None:
        make_button_preview.configure(state="normal")
        make_button_run.configure(state="normal")
        for path in saved_paths:
            append_log(f"Saved: {path}")
        set_preview_summary(processed_df, value_column, control_mean, saved_paths)
        add_figure_record(fig, f"Export {datetime.now().strftime('%H:%M:%S')}", saved_paths)
        set_status("Export finished.")
        set_active_tab("Figures")
        show_toast("Export finished successfully.", kind="success")

    def finish_export_error(exc: Exception) -> None:
        make_button_preview.configure(state="normal")
        make_button_run.configure(state="normal")
        set_status("Export failed.")
        append_log("ERROR during export:\n" + "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
        show_toast("Export failed.", kind="error")
        messagebox.showerror("Run failed", str(exc))

    def open_selected_figure_file() -> None:
        sel = figure_list.curselection()
        if not sel:
            show_toast("Select a generated figure first.", kind="warning")
            return
        paths = figure_records[sel[0]].get("saved_paths") or []
        if not paths:
            show_toast("This preview was not exported to a file.", kind="warning")
            return
        # Prefer SVG for publication editing, otherwise use first saved path.
        chosen = next((p for p in paths if Path(p).suffix.lower() == ".svg"), paths[0])
        try:
            open_path(chosen)
        except Exception as exc:
            messagebox.showerror("Could not open file", str(exc))

    def open_output_folder() -> None:
        path_text = var_output_prefix.get().strip()
        if not path_text:
            show_toast("No output prefix set.", kind="warning")
            return
        folder = Path(path_text).expanduser().parent
        try:
            folder.mkdir(parents=True, exist_ok=True)
            open_path(folder)
        except Exception as exc:
            messagebox.showerror("Could not open folder", str(exc))

    def reset_form() -> None:
        if not messagebox.askyesno("Reset form", "Clear the current GUI settings?"):
            return
        nonlocal input_df, order_df
        input_df = None
        order_df = None
        var_input_path.set("")
        var_order_path.set("")
        var_output_prefix.set(str(_THIS_DIR / "output" / "plant_bioactivity"))
        var_sample_column.set("")
        var_replicate_columns.set("")
        var_order_column.set("")
        var_control_mode.set("None")
        var_control_entry_1.set("")
        var_control_entry_2.set("")
        var_exclude_control.set(False)
        var_use_threshold.set(False)
        var_threshold.set("")
        var_threshold_mode.set("ge")
        var_title.set("Sample activity")
        var_ylabel.set("")
        var_figure_width.set("16")
        var_figure_height.set("6")
        var_rotate.set("45")
        var_font_family.set("Arial")
        var_title_size.set("16")
        var_axis_label_size.set("20")
        var_xtick_label_size.set("8")
        var_ytick_label_size.set("16")
        var_legend_size.set("12")
        var_export_table.set(True)
        var_export_png.set(True)
        var_export_svg.set(True)
        combo_sample.configure(values=[""])
        combo_order.configure(values=[""])
        update_threshold_state()
        update_control_inputs()
        refresh_column_picker()
        save_current_state()
        set_status("Form reset.")
        show_toast("Form reset.", kind="success")

    def on_close() -> None:
        if app_closing["value"]:
            return
        app_closing["value"] = True
        try:
            save_current_state()
        except Exception:
            pass
        try:
            help_popovers.close()
        except Exception:
            pass
        try:
            clear_figure_canvas()
        except Exception:
            pass
        cancel_scheduled_callbacks()
        try:
            root.quit()
        except Exception:
            pass
        try:
            root.destroy()
        except Exception:
            pass

    # Header action buttons now that callbacks exist.
    make_button_preview = make_button(header_actions, "Preview figure", preview_figure, width=135)
    make_button_preview.pack(side="left", padx=(0, 8))
    make_button_run = make_button(header_actions, "Run + export", run_and_export, primary=True, width=135)
    make_button_run.pack(side="left", padx=(0, 8))
    make_button(header_actions, "Load columns", load_input_columns, width=120).pack(side="left", padx=(0, 8))
    make_button(header_actions, "Save config", save_config_file, width=116).pack(side="left", padx=(0, 8))
    make_button(header_actions, "Load config", load_config_file, width=116).pack(side="left", padx=(0, 8))
    make_button(header_actions, "Reset", reset_form, width=86).pack(side="left", padx=(0, 8))
    make_button(header_actions, "Close", on_close, danger=True, width=78).pack(side="left")

    # Initial state.
    update_threshold_state()
    update_control_inputs()
    update_input_mode_visibility()
    set_active_tab("Workflow")
    root.protocol("WM_DELETE_WINDOW", on_close)
    poll_log_queue()
    update_column_summary()
    if var_input_path.get().strip():
        load_input_columns(auto_log=False)

    try:
        root.mainloop()
    finally:
        save_current_state()
        # Do not force-close figures while Tk is using one; close remaining at shutdown.
        try:
            plt.close("all")
        except Exception:
            pass
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch the CustomTkinter GUI for plant bioactivity plotting.")
    parser.add_argument("--input", help="Optional input CSV/TSV/TXT/XLSX file to pre-load.")
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
