#!/usr/bin/env python3
"""CustomTkinter GUI for fraction prediction and bioactivity mapping."""
from __future__ import annotations

import json
import logging
import queue
import sys
import tempfile
import threading
import traceback
from pathlib import Path
from typing import Any

import pandas as pd

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

try:
    import p_05_00_fraction_predictor_core as core
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "ERROR: Could not import p_05_00_fraction_predictor_core.py.\n"
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

    try:
        ctk.deactivate_automatic_dpi_awareness()
    except Exception:
        pass
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
        "danger": "#b54848",
        "danger_hover": "#9e3b3b",
        "warning": "#b7791f",
    }
    font_header = ("Segoe UI", 23, "bold")
    font_subtitle = ("Segoe UI", 12)
    font_card_title = ("Segoe UI", 15, "bold")
    font_label = ("Segoe UI", 12)
    font_small = ("Segoe UI", 11)
    font_mono = ("Consolas", 11)

    state_file = _THIS_DIR / ".p_05_01_fraction_predictor_gui_state.json"
    table_patterns = [("Tables", "*.csv *.xlsx *.xls"), ("All files", "*.*")]
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

    app_state = load_json_safe(state_file)
    root = ctk.CTk()
    root.title("Fraction predictor")
    root.geometry("1340x920")
    root.minsize(1160, 780)
    root.configure(fg_color=colors["bg"])

    var_status = tk.StringVar(value="Ready.")
    var_output_dir = tk.StringVar(value=str(app_state.get("output_dir", str(_THIS_DIR / "Outputs"))))
    var_feature_table = tk.StringVar(value=str(app_state.get("feature_table", "")))
    var_append_table = tk.StringVar(value=str(app_state.get("append_table", "")))
    var_feature_table_type = tk.StringVar(value=str(app_state.get("feature_table_type", "unannotated")))
    var_use_separate_annotation = tk.BooleanVar(value=bool(app_state.get("use_separate_annotation", bool(app_state.get("append_table", "")))))
    var_id_col = tk.StringVar(value=str(app_state.get("id_column", "row ID")))
    var_mz_col = tk.StringVar(value=str(app_state.get("mz_column", "row m/z")))
    var_rt_col = tk.StringVar(value=str(app_state.get("rt_column", "row retention time")))
    var_threshold = tk.StringVar(value=str(app_state.get("area_threshold", 10000)))
    var_predicted_rt_col = tk.StringVar(value=str(app_state.get("predicted_rt_column", "predicted_hplc_rt")))
    var_fraction_start = tk.StringVar(value=str(app_state.get("fraction_start", 2.0)))
    var_fraction_end = tk.StringVar(value=str(app_state.get("fraction_end", 38.0)))
    var_fraction_n = tk.StringVar(value=str(app_state.get("fraction_n", 96)))
    var_first_fraction = tk.StringVar(value=str(app_state.get("first_fraction_number", 1)))
    var_cutoffs = tk.StringVar(value=str(app_state.get("bioactivity_cutoffs", "16.5, 22.5")))
    var_calibration_mode = tk.StringVar(value=str(app_state.get("calibration_mode", "pairs")))
    var_pairs_file = tk.StringVar(value=str(app_state.get("pairs_file", "")))
    var_pairs_uplc_col = tk.StringVar(value=str(app_state.get("pairs_uplc_col", "UPLC RT")))
    var_pairs_hplc_col = tk.StringVar(value=str(app_state.get("pairs_hplc_col", "HPLC RT")))
    var_pairs_manual = tk.StringVar(value=str(app_state.get("pairs_manual", "")))
    var_equation_slope = tk.StringVar(value=str(app_state.get("equation_slope", 1.0)))
    var_equation_intercept = tk.StringVar(value=str(app_state.get("equation_intercept", 0.0)))
    var_runtime_uplc = tk.StringVar(value=str(app_state.get("runtime_uplc", 19.3)))
    var_runtime_hplc = tk.StringVar(value=str(app_state.get("runtime_hplc", 38.0)))
    var_feature_summary = tk.StringVar(value="Load a feature table to populate column pickers.")
    var_pairs_summary = tk.StringVar(value="Load a calibration table if you want to choose its columns.")

    feature_columns: list[str] = []
    pairs_columns: list[str] = []
    sample_column_vars: list[tk.StringVar] = []
    plant_cards: list[dict[str, Any]] = []
    worker_state = {"running": False}
    log_queue: queue.Queue[str] = queue.Queue()
    result_queue: queue.Queue[tuple[bool, Any]] = queue.Queue()

    class QueueLogHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                log_queue.put(self.format(record))
            except Exception:
                pass

    logger = logging.getLogger("fraction_predictor_core")
    logger.setLevel(logging.INFO)
    gui_log_handler = QueueLogHandler()
    gui_log_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(gui_log_handler)

    class ToolTip:
        tooltip_width = 460

        def __init__(self, widget: Any, text: str) -> None:
            self.widget = widget
            self.text = text
            self.frame: Any = None
            self.after_id: str | None = None
            widget.bind("<Enter>", self.schedule, add="+")
            widget.bind("<Leave>", self.hide, add="+")

        def schedule(self, _event: Any = None) -> None:
            if self.after_id is None:
                self.after_id = root.after(250, self.show)

        def show(self) -> None:
            self.after_id = None
            if self.frame is not None:
                return
            self.frame = ctk.CTkFrame(root, fg_color="#111318", border_color=colors["border"], border_width=1, corner_radius=14, width=self.tooltip_width)
            self.frame.pack_propagate(False)
            label = ctk.CTkLabel(self.frame, text=self.text, font=font_small, text_color=colors["text"], justify="left", wraplength=self.tooltip_width - 36, padx=16, pady=12)
            label.pack(fill="both", expand=True)
            self.frame.update_idletasks()
            height = max(58, label.winfo_reqheight() + 16)
            self.frame.configure(width=self.tooltip_width, height=height)
            x = self.widget.winfo_rootx() - root.winfo_rootx() + self.widget.winfo_width() + 10
            y = self.widget.winfo_rooty() - root.winfo_rooty() + self.widget.winfo_height() + 4
            self.frame.place(x=min(max(12, int(x)), max(12, root.winfo_width() - self.tooltip_width - 14)), y=min(max(12, int(y)), max(12, root.winfo_height() - height - 14)))
            self.frame.lift()

        def hide(self, _event: Any = None) -> None:
            if self.after_id:
                try:
                    root.after_cancel(self.after_id)
                except Exception:
                    pass
                self.after_id = None
            if self.frame is not None:
                self.frame.destroy()
                self.frame = None

    def make_help(parent: Any, text: str) -> Any:
        bubble = ctk.CTkLabel(parent, text="?", width=23, height=23, corner_radius=12, fg_color=colors["card_alt"], text_color=colors["muted"], font=("Segoe UI", 11, "bold"))
        ToolTip(bubble, text)
        return bubble

    def make_button(parent: Any, text: str, command: Any, *, primary: bool = False, success: bool = False, danger: bool = False, width: int | None = None) -> Any:
        color = colors["success"] if success else colors["danger"] if danger else colors["accent"] if primary else colors["card_alt"]
        hover = colors["success_hover"] if success else colors["danger_hover"] if danger else colors["accent_hover"] if primary else "#39414c"
        return ctk.CTkButton(parent, text=text, command=command, width=width or 110, height=36, corner_radius=9, fg_color=color, hover_color=hover)

    def make_entry(parent: Any, var: tk.StringVar, placeholder: str = "") -> Any:
        return ctk.CTkEntry(parent, textvariable=var, placeholder_text=placeholder, fg_color=colors["entry"], border_color=colors["border"], text_color=colors["text"], height=35, corner_radius=8)

    def make_combo(parent: Any, var: tk.StringVar, values: list[str]) -> Any:
        return ctk.CTkComboBox(parent, variable=var, values=values or [""], fg_color=colors["entry"], border_color=colors["border"], button_color=colors["accent"], button_hover_color=colors["accent_hover"], dropdown_fg_color=colors["card"], dropdown_hover_color=colors["card_alt"], text_color=colors["text"], dropdown_text_color=colors["text"], height=35, corner_radius=8)

    def make_card(parent: Any, title: str, hint: str | None = None) -> Any:
        card = ctk.CTkFrame(parent, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=16)
        card.pack(fill="x", padx=6, pady=(0, 12))
        card.grid_columnconfigure(0, weight=1)
        title_row = ctk.CTkFrame(card, fg_color="transparent")
        title_row.grid(row=0, column=0, sticky="ew", padx=18, pady=(16, 10))
        ctk.CTkLabel(title_row, text=title, font=font_card_title, text_color=colors["text"], anchor="w").pack(side="left")
        if hint:
            make_help(title_row, hint).pack(side="left", padx=(8, 0))
        body = ctk.CTkFrame(card, fg_color="transparent")
        body.grid(row=1, column=0, sticky="ew", padx=18, pady=(0, 18))
        for col in range(6):
            body.grid_columnconfigure(col, weight=1 if col % 2 == 1 else 0)
        return body

    def labeled_widget(parent: Any, label: str, widget: Any, row: int, col: int, *, help_text: str, colspan: int = 1) -> None:
        label_frame = ctk.CTkFrame(parent, fg_color="transparent")
        label_frame.grid(row=row, column=col, sticky="ew", padx=(0, 10), pady=7)
        ctk.CTkLabel(label_frame, text=label, font=font_label, text_color=colors["muted"], anchor="w").pack(side="left")
        make_help(label_frame, help_text).pack(side="left", padx=(6, 0))
        widget.grid(row=row, column=col + 1, columnspan=colspan, sticky="ew", padx=(0, 18), pady=7)

    def show_toast(message: str, *, kind: str = "info") -> None:
        color = colors["success"] if kind == "success" else colors["danger"] if kind == "error" else colors["warning"] if kind == "warning" else colors["accent"]
        toast = ctk.CTkFrame(root, fg_color=color, corner_radius=14)
        ctk.CTkLabel(toast, text=message, font=font_small, text_color="white", justify="left", wraplength=430, padx=16, pady=11).pack(fill="both", expand=True)
        toast.place(relx=1.0, rely=0.0, x=-26, y=86, anchor="ne")
        toast.lift()
        root.after(3600, lambda: toast.destroy() if toast.winfo_exists() else None)

    def append_log(message: str) -> None:
        txt_log.configure(state="normal")
        txt_log.insert("end", message + "\n")
        txt_log.see("end")
        txt_log.configure(state="disabled")

    def clear_log() -> None:
        txt_log.configure(state="normal")
        txt_log.delete("1.0", "end")
        txt_log.configure(state="disabled")

    def parse_float(label: str, value: str, *, required: bool = False) -> float | None:
        text = str(value).strip()
        if not text:
            if required:
                raise ValueError(f"{label} is required.")
            return None
        try:
            return float(text)
        except ValueError as exc:
            raise ValueError(f"{label} must be a number.") from exc

    def parse_int(label: str, value: str, *, required: bool = False) -> int | None:
        val = parse_float(label, value, required=required)
        if val is None:
            return None
        return int(val)

    def parse_cutoffs(text: str) -> list[float]:
        values = [float(x.strip()) for x in text.split(",") if x.strip()]
        if not values:
            raise ValueError("Enter at least one bioactivity cutoff.")
        return values

    def browse_file(var: tk.StringVar, title: str, patterns: list[tuple[str, str]] | None = None, *, after: Any = None) -> None:
        path = filedialog.askopenfilename(title=title, initialdir=str(_THIS_DIR), filetypes=patterns or table_patterns)
        if path:
            var.set(path)
            if after:
                after()

    def browse_output_dir() -> None:
        path = filedialog.askdirectory(title="Choose output folder", initialdir=str(_THIS_DIR))
        if path:
            var_output_dir.set(path)

    def refresh_annotation_section(*_args: Any) -> None:
        if var_use_separate_annotation.get():
            separate_annotation_frame.grid(row=5, column=0, columnspan=6, sticky="ew", pady=(8, 0))
        else:
            separate_annotation_frame.grid_remove()
            var_append_table.set("")
        save_state()

    def refresh_feature_column_widgets() -> None:
        values = ["", *feature_columns]
        for combo in [combo_id_col, combo_mz_col, combo_rt_col, combo_sample_add]:
            combo.configure(values=values)
        refresh_sample_list()
        refresh_plants()

    def load_feature_columns() -> None:
        nonlocal feature_columns
        path_text = var_feature_table.get().strip()
        if not path_text:
            show_toast("Choose a feature table first.", kind="warning")
            return
        path = Path(path_text).expanduser()
        if not path.exists():
            messagebox.showerror("Feature table", f"File not found:\n{path}")
            return
        try:
            df = core.read_table(path)
            feature_columns = [str(col) for col in df.columns]
            var_feature_summary.set(f"Loaded {len(df)} row(s) and {len(feature_columns)} column(s).")
            refresh_feature_column_widgets()
            show_toast("Feature table columns loaded.", kind="success")
        except Exception as exc:
            messagebox.showerror("Feature table", str(exc))

    def load_pairs_columns() -> None:
        nonlocal pairs_columns
        path_text = var_pairs_file.get().strip()
        if not path_text:
            show_toast("Choose a calibration pairs file first.", kind="warning")
            return
        path = Path(path_text).expanduser()
        if not path.exists():
            messagebox.showerror("Calibration pairs", f"File not found:\n{path}")
            return
        try:
            df = core.read_table(path)
            pairs_columns = [str(col) for col in df.columns]
            combo_pairs_uplc.configure(values=["", *pairs_columns])
            combo_pairs_hplc.configure(values=["", *pairs_columns])
            var_pairs_summary.set(f"Loaded {len(df)} calibration row(s).")
            show_toast("Calibration columns loaded.", kind="success")
        except Exception as exc:
            messagebox.showerror("Calibration pairs", str(exc))

    def selected_sample_columns() -> list[str]:
        return [v.get().strip() for v in sample_column_vars if v.get().strip()]

    def plant_sample_column_values() -> list[str]:
        ordered: list[str] = []
        for value in [*selected_sample_columns(), *feature_columns]:
            text = str(value).strip()
            if text and text not in ordered:
                ordered.append(text)
        return ["", *ordered]

    def guess_plant_name_from_sample_column(sample_column: str) -> str:
        text = sample_column.strip()
        suffixes = [" Peak area", " peak area", ".mzML", ".mzXML", ".raw"]
        for suffix in suffixes:
            text = text.replace(suffix, "")
        text = text.replace("_", " ").replace("-", " ")
        parts = [part for part in text.split() if part]
        if 2 <= len(parts) <= 4 and all(part[:1].isalpha() for part in parts):
            return " ".join(parts)
        return ""

    def plant_card_is_empty_draft(card: dict[str, Any], sample_column: str) -> bool:
        auto_name = guess_plant_name_from_sample_column(sample_column)
        current_name = card["name"].get().strip()
        return (
            bool(card.get("auto_from_sample"))
            and card["sample_column"].get().strip() == sample_column
            and current_name in {"", auto_name}
            and not card["fluorescence_file"].get().strip()
            and card["fraction_column"].get().strip() == "fraction"
            and card["average_column"].get().strip() == "average"
            and card["positive_column"].get().strip() == "pos_avg"
        )

    def ensure_plant_for_sample(sample_column: str) -> None:
        if any(card["sample_column"].get().strip() == sample_column for card in plant_cards):
            return
        add_plant(
            {
                "name": guess_plant_name_from_sample_column(sample_column),
                "sample_column": sample_column,
                "_auto_from_sample": True,
            },
            save=False,
        )

    def add_sample_column(value: str | None = None) -> None:
        text = (value or var_sample_add.get()).strip()
        if not text:
            show_toast("Choose or type a sample column first.", kind="warning")
            return
        if text not in selected_sample_columns():
            sample_column_vars.append(tk.StringVar(value=text))
            ensure_plant_for_sample(text)
            refresh_sample_list()
            refresh_plants()
            save_state()
            show_toast("Sample column added and plant card prepared.", kind="success")
        else:
            ensure_plant_for_sample(text)
            refresh_plants()
            save_state()

    def remove_sample_column(index: int) -> None:
        if 0 <= index < len(sample_column_vars):
            removed = sample_column_vars.pop(index).get().strip()
            plant_cards[:] = [
                card for card in plant_cards
                if not plant_card_is_empty_draft(card, removed)
            ]
            refresh_sample_list()
            refresh_plants()
            save_state()

    def refresh_sample_list() -> None:
        for child in sample_list_frame.winfo_children():
            child.destroy()
        if not sample_column_vars:
            ctk.CTkLabel(sample_list_frame, text="No sample columns selected yet.", font=font_small, text_color=colors["muted"], anchor="w").pack(anchor="w", pady=4)
            return
        for idx, var in enumerate(sample_column_vars):
            row = ctk.CTkFrame(sample_list_frame, fg_color=colors["card_alt"], corner_radius=10)
            row.pack(fill="x", pady=(0, 6))
            ctk.CTkLabel(row, text=var.get(), font=font_small, text_color=colors["text"], anchor="w").pack(side="left", fill="x", expand=True, padx=12, pady=8)
            make_button(row, "Remove", lambda i=idx: remove_sample_column(i), danger=True, width=82).pack(side="right", padx=8, pady=6)

    def refresh_calibration_mode(_choice: str | None = None) -> None:
        mode = var_calibration_mode.get()
        cal_pairs_frame.grid_remove()
        cal_eq_frame.grid_remove()
        cal_runtime_frame.grid_remove()
        if mode == "pairs":
            cal_pairs_frame.grid(row=2, column=0, columnspan=6, sticky="ew", pady=(8, 0))
            var_cal_hint.set("Recommended when matched compounds or manual guesses are available. Use at least five pairs when possible; the table should contain one UPLC retention time and one matching HPLC retention time per row.")
        elif mode == "equation":
            cal_eq_frame.grid(row=2, column=0, columnspan=6, sticky="ew", pady=(8, 0))
            var_cal_hint.set("Use this only if the regression has already been calculated elsewhere. The script applies HPLC RT = slope * UPLC RT + intercept.")
        else:
            cal_runtime_frame.grid(row=2, column=0, columnspan=6, sticky="ew", pady=(8, 0))
            var_cal_hint.set("This is the roughest option. It scales retention time by total gradient runtime and assumes the two methods are comparable.")
        save_state()

    def add_plant(initial: dict[str, Any] | None = None, *, save: bool = True) -> None:
        info = initial or {}
        plant_cards.append({
            "name": tk.StringVar(value=str(info.get("name", ""))),
            "sample_column": tk.StringVar(value=str(info.get("sample_column", ""))),
            "fluorescence_file": tk.StringVar(value=str(info.get("fluorescence_file", ""))),
            "fraction_column": tk.StringVar(value=str(info.get("fluorescence_fraction_column", "fraction"))),
            "average_column": tk.StringVar(value=str(info.get("fluorescence_average_column", "average"))),
            "positive_column": tk.StringVar(value=str(info.get("fluorescence_positive_control_column", "pos_avg"))),
            "auto_from_sample": bool(info.get("_auto_from_sample", False)),
        })
        refresh_plants()
        if save:
            save_state()

    def remove_plant(index: int) -> None:
        if 0 <= index < len(plant_cards):
            plant_cards.pop(index)
            refresh_plants()
            save_state()

    def refresh_plants() -> None:
        for child in plants_container.winfo_children():
            child.destroy()
        if not plant_cards:
            ctk.CTkLabel(plants_container, text="No plants added yet.", font=font_small, text_color=colors["muted"], anchor="w").pack(anchor="w", pady=6)
            return
        for idx, card in enumerate(plant_cards):
            box = ctk.CTkFrame(plants_container, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=14)
            box.pack(fill="x", pady=(0, 12))
            box.grid_columnconfigure(1, weight=1)
            title = ctk.CTkFrame(box, fg_color="transparent")
            title.grid(row=0, column=0, columnspan=4, sticky="ew", padx=16, pady=(14, 6))
            ctk.CTkLabel(title, text=f"Plant {idx + 1}", font=font_card_title, text_color=colors["text"]).pack(side="left")
            make_button(title, "Remove", lambda i=idx: remove_plant(i), danger=True, width=86).pack(side="right")
            labeled_widget(box, "Plant name", make_entry(box, card["name"], "Macleaya microcarpa"), 1, 0, help_text="Name used in output column names and in plant-specific CSV files. Use the biological sample or species name that should appear in the final table.")
            combo_plant_sample = make_combo(box, card["sample_column"], plant_sample_column_values())
            combo_plant_sample.configure(command=lambda _choice: save_state())
            labeled_widget(box, "Sample column", combo_plant_sample, 1, 2, help_text="Peak-area column for this plant in the feature table. Columns selected for filtering appear first because they are usually the same samples that need plant cards; other loaded columns remain available for non-standard workflows.")
            fluoro_picker = ctk.CTkFrame(box, fg_color="transparent")
            fluoro_picker.grid(row=2, column=1, columnspan=3, sticky="ew", padx=(0, 18), pady=7)
            fluoro_picker.grid_columnconfigure(0, weight=1)
            make_entry(fluoro_picker, card["fluorescence_file"], "fractions_84_UV_clean.xlsx").grid(row=0, column=0, sticky="ew", padx=(0, 8))
            make_button(fluoro_picker, "Browse", lambda v=card["fluorescence_file"]: browse_file(v, "Choose fluorescence table"), width=82).grid(row=0, column=1)
            label_frame = ctk.CTkFrame(box, fg_color="transparent")
            label_frame.grid(row=2, column=0, sticky="ew", padx=(0, 10), pady=7)
            ctk.CTkLabel(label_frame, text="Fluorescence table", font=font_label, text_color=colors["muted"]).pack(side="left")
            make_help(label_frame, "CSV or Excel table with one row per collected fraction. It must contain a fraction number column, a measured average fluorescence column, and a positive-control value or column used for normalization.").pack(side="left", padx=(6, 0))
            labeled_widget(box, "Fraction column", make_entry(box, card["fraction_column"], "fraction"), 3, 0, help_text="Column in the fluorescence table that identifies the collected fraction number. These numbers are matched to predicted feature fractions.")
            labeled_widget(box, "Average column", make_entry(box, card["average_column"], "average"), 3, 2, help_text="Column containing the measured fluorescence average for each fraction. The core converts this to percent signal and then to bioactivity.")
            labeled_widget(box, "Positive control", make_entry(box, card["positive_column"], "pos_avg"), 4, 0, help_text="Column or value used as the positive control. The script normalizes the fraction average against the first numeric control value it finds in this column.")

    def collect_current_state() -> dict[str, Any]:
        return {
            "output_dir": var_output_dir.get().strip(),
            "feature_table": var_feature_table.get().strip(),
            "append_table": var_append_table.get().strip(),
            "feature_table_type": var_feature_table_type.get(),
            "use_separate_annotation": bool(var_use_separate_annotation.get()),
            "id_column": var_id_col.get().strip(),
            "mz_column": var_mz_col.get().strip(),
            "rt_column": var_rt_col.get().strip(),
            "area_threshold": var_threshold.get().strip(),
            "predicted_rt_column": var_predicted_rt_col.get().strip(),
            "fraction_start": var_fraction_start.get().strip(),
            "fraction_end": var_fraction_end.get().strip(),
            "fraction_n": var_fraction_n.get().strip(),
            "first_fraction_number": var_first_fraction.get().strip(),
            "bioactivity_cutoffs": var_cutoffs.get().strip(),
            "calibration_mode": var_calibration_mode.get().strip(),
            "pairs_file": var_pairs_file.get().strip(),
            "pairs_uplc_col": var_pairs_uplc_col.get().strip(),
            "pairs_hplc_col": var_pairs_hplc_col.get().strip(),
            "pairs_manual": var_pairs_manual.get().strip(),
            "equation_slope": var_equation_slope.get().strip(),
            "equation_intercept": var_equation_intercept.get().strip(),
            "runtime_uplc": var_runtime_uplc.get().strip(),
            "runtime_hplc": var_runtime_hplc.get().strip(),
            "sample_columns": [v.get().strip() for v in sample_column_vars if v.get().strip()],
            "plants": [
                {
                    "name": card["name"].get().strip(),
                    "sample_column": card["sample_column"].get().strip(),
                    "fluorescence_file": card["fluorescence_file"].get().strip(),
                    "fluorescence_fraction_column": card["fraction_column"].get().strip(),
                    "fluorescence_average_column": card["average_column"].get().strip(),
                    "fluorescence_positive_control_column": card["positive_column"].get().strip(),
                }
                for card in plant_cards
            ],
        }

    def save_state() -> None:
        save_json_safe(state_file, collect_current_state())

    def manual_pairs_to_temp_file(output_dir: Path) -> str:
        rows = []
        for line_number, line in enumerate(var_pairs_manual.get().splitlines(), start=1):
            text = line.strip()
            if not text:
                continue
            parts = [part.strip() for part in text.split(",")]
            if len(parts) != 2:
                raise ValueError("Manual calibration pairs must use one pair per line in the format UPLC,HPLC.")
            try:
                rows.append({"UPLC RT": float(parts[0]), "HPLC RT": float(parts[1])})
            except ValueError as exc:
                raise ValueError(f"Invalid calibration pair on line {line_number}: {line}") from exc
        if len(rows) < 2:
            raise ValueError("At least two manual calibration pairs are required.")
        output_dir.mkdir(parents=True, exist_ok=True)
        tmp = output_dir / "_gui_calibration_pairs.csv"
        pd.DataFrame(rows).to_csv(tmp, index=False)
        return str(tmp)

    def validate_and_build_config() -> dict[str, Any]:
        feature_path = Path(var_feature_table.get().strip().strip('"')).expanduser()
        if not feature_path.exists():
            raise ValueError("Choose an existing main feature table.")
        output_dir = Path(var_output_dir.get().strip() or (_THIS_DIR / "Outputs")).expanduser()
        samples = [v.get().strip() for v in sample_column_vars if v.get().strip()]
        if not samples:
            raise ValueError("Add at least one sample peak-area column.")
        plants: list[dict[str, Any]] = []
        for idx, card in enumerate(plant_cards, start=1):
            name = card["name"].get().strip()
            sample = card["sample_column"].get().strip()
            fluoro = card["fluorescence_file"].get().strip().strip('"')
            if not name:
                raise ValueError(f"Plant {idx}: enter a plant name.")
            if not sample:
                raise ValueError(f"Plant {idx}: choose the sample peak-area column for this plant.")
            if not fluoro or not Path(fluoro).expanduser().exists():
                raise ValueError(f"Plant {idx}: choose an existing fluorescence table.")
            plants.append({
                "name": name,
                "sample_column": sample,
                "fluorescence_file": str(Path(fluoro).expanduser().resolve()),
                "fluorescence_fraction_column": card["fraction_column"].get().strip() or "fraction",
                "fluorescence_average_column": card["average_column"].get().strip() or "average",
                "fluorescence_positive_control_column": card["positive_column"].get().strip() or "pos_avg",
            })

        mode = var_calibration_mode.get().strip()
        if mode == "pairs":
            if var_pairs_manual.get().strip():
                pairs_file = manual_pairs_to_temp_file(output_dir)
                calibration = {
                    "method": "pairs",
                    "pairs_file": pairs_file,
                    "uplc_rt_column": "UPLC RT",
                    "hplc_rt_column": "HPLC RT",
                    "minimum_points": 2,
                    "recommended_points": 5,
                }
            else:
                pairs_path = Path(var_pairs_file.get().strip().strip('"')).expanduser()
                if not pairs_path.exists():
                    raise ValueError("Choose a calibration pairs file or enter manual pairs.")
                calibration = {
                    "method": "pairs",
                    "pairs_file": str(pairs_path.resolve()),
                    "uplc_rt_column": var_pairs_uplc_col.get().strip() or "UPLC RT",
                    "hplc_rt_column": var_pairs_hplc_col.get().strip() or "HPLC RT",
                    "minimum_points": 2,
                    "recommended_points": 5,
                }
        elif mode == "equation":
            calibration = {
                "method": "equation",
                "slope": parse_float("Calibration slope", var_equation_slope.get(), required=True),
                "intercept": parse_float("Calibration intercept", var_equation_intercept.get(), required=True),
            }
        else:
            calibration = {
                "method": "runtime_scale",
                "uplc_total_runtime": parse_float("UPLC total runtime", var_runtime_uplc.get(), required=True),
                "hplc_total_runtime": parse_float("HPLC total runtime", var_runtime_hplc.get(), required=True),
            }

        config: dict[str, Any] = {
            "base_dir": str(_THIS_DIR),
            "output_dir": str(output_dir.resolve()),
            "predicted_rt_column": var_predicted_rt_col.get().strip() or "predicted_hplc_rt",
            "feature_table": {
                "path": str(feature_path.resolve()),
                "id_column": var_id_col.get().strip() or None,
                "mz_column": var_mz_col.get().strip() or "row m/z",
                "rt_column": var_rt_col.get().strip() or "row retention time",
                "area_threshold": parse_float("Area threshold", var_threshold.get(), required=True),
                "sample_columns": samples,
            },
            "calibration": calibration,
            "fractions": {
                "start_time": parse_float("Fraction start time", var_fraction_start.get(), required=True),
                "end_time": parse_float("Fraction end time", var_fraction_end.get(), required=True),
                "n_fractions": parse_int("Number of fractions", var_fraction_n.get(), required=True),
                "first_fraction_number": parse_int("First fraction number", var_first_fraction.get(), required=True),
            },
            "bioactivity": {"cutoffs": parse_cutoffs(var_cutoffs.get())},
            "plants": plants,
        }

        if var_use_separate_annotation.get():
            append_path = var_append_table.get().strip().strip('"')
        elif var_feature_table_type.get() == "annotated":
            append_path = str(feature_path)
        else:
            append_path = ""
        if append_path:
            append_file = Path(append_path).expanduser()
            if not append_file.exists():
                raise ValueError("Annotation table path was provided but the file does not exist.")
            config["append_to_feature_table"] = {
                "path": str(append_file.resolve()),
                "id_column": var_id_col.get().strip() or None,
            }
        return config

    def save_config_dialog() -> None:
        try:
            config = validate_and_build_config()
        except Exception as exc:
            messagebox.showerror("Cannot save config", str(exc))
            return
        path = filedialog.asksaveasfilename(title="Save fraction predictor config", initialdir=str(_THIS_DIR), defaultextension=".json", filetypes=config_patterns, initialfile="fraction_predictor_config.json")
        if path:
            Path(path).write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
            save_state()
            show_toast("Configuration saved.", kind="success")

    def apply_config(config: dict[str, Any]) -> None:
        sample_column_vars.clear()
        plant_cards.clear()
        var_output_dir.set(str(config.get("output_dir", str(_THIS_DIR / "Outputs"))))
        feature_cfg = config.get("feature_table", {}) or {}
        var_feature_table.set(str(feature_cfg.get("path", "")))
        var_id_col.set(str(feature_cfg.get("id_column", "") or ""))
        var_mz_col.set(str(feature_cfg.get("mz_column", "row m/z")))
        var_rt_col.set(str(feature_cfg.get("rt_column", "row retention time")))
        var_threshold.set(str(feature_cfg.get("area_threshold", 10000)))
        for sample in feature_cfg.get("sample_columns", []) or []:
            sample_column_vars.append(tk.StringVar(value=str(sample)))
        var_predicted_rt_col.set(str(config.get("predicted_rt_column", "predicted_hplc_rt")))
        fractions = config.get("fractions", {}) or {}
        var_fraction_start.set(str(fractions.get("start_time", 2.0)))
        var_fraction_end.set(str(fractions.get("end_time", 38.0)))
        var_fraction_n.set(str(fractions.get("n_fractions", 96)))
        var_first_fraction.set(str(fractions.get("first_fraction_number", 1)))
        var_cutoffs.set(", ".join(str(x) for x in (config.get("bioactivity", {}) or {}).get("cutoffs", [16.5, 22.5])))
        cal = config.get("calibration", {}) or {}
        mode = str(cal.get("method", "pairs"))
        var_calibration_mode.set(mode if mode in {"pairs", "equation", "runtime_scale"} else "pairs")
        var_pairs_file.set(str(cal.get("pairs_file", "")))
        var_pairs_uplc_col.set(str(cal.get("uplc_rt_column", "UPLC RT")))
        var_pairs_hplc_col.set(str(cal.get("hplc_rt_column", "HPLC RT")))
        var_equation_slope.set(str(cal.get("slope", 1.0)))
        var_equation_intercept.set(str(cal.get("intercept", 0.0)))
        var_runtime_uplc.set(str(cal.get("uplc_total_runtime", 19.3)))
        var_runtime_hplc.set(str(cal.get("hplc_total_runtime", 38.0)))
        append_cfg = config.get("append_to_feature_table", {}) or {}
        var_append_table.set(str(append_cfg.get("path", "")))
        append_path = str(append_cfg.get("path", ""))
        feature_path = str(feature_cfg.get("path", ""))
        same_table_append = bool(append_path) and Path(append_path).expanduser() == Path(feature_path).expanduser()
        var_use_separate_annotation.set(bool(append_path) and not same_table_append)
        var_feature_table_type.set("annotated" if same_table_append else "unannotated")
        for plant in config.get("plants", []) or []:
            add_plant(plant)
        refresh_sample_list()
        refresh_calibration_mode()
        refresh_annotation_section()
        refresh_plants()

    def load_config_dialog() -> None:
        path = filedialog.askopenfilename(title="Load fraction predictor config", initialdir=str(_THIS_DIR), filetypes=config_patterns)
        if not path:
            return
        try:
            apply_config(json.loads(Path(path).read_text(encoding="utf-8")))
            save_state()
            if var_feature_table.get().strip():
                try:
                    load_feature_columns()
                except Exception:
                    pass
            show_toast("Configuration loaded.", kind="success")
        except Exception as exc:
            messagebox.showerror("Load config", str(exc))

    def run_pipeline_from_gui() -> None:
        if worker_state["running"]:
            return
        try:
            config = validate_and_build_config()
        except Exception as exc:
            messagebox.showerror("Cannot start", str(exc))
            return
        save_state()
        clear_log()
        append_log("New fraction predictor run started.")
        append_log(f"Output folder: {config['output_dir']}")
        worker_state["running"] = True
        run_button.configure(state="disabled")
        progress.start()
        var_status.set("Running fraction prediction...")

        def worker() -> None:
            try:
                result_queue.put((True, core.run_pipeline(config)))
            except Exception as exc:
                result_queue.put((False, (exc, traceback.format_exc())))

        threading.Thread(target=worker, daemon=True).start()

    def poll_queues() -> None:
        try:
            while True:
                append_log(log_queue.get_nowait())
        except queue.Empty:
            pass
        try:
            while True:
                ok, payload = result_queue.get_nowait()
                worker_state["running"] = False
                run_button.configure(state="normal")
                progress.stop()
                if ok:
                    summary = payload
                    var_status.set("Finished.")
                    append_log("")
                    append_log("Run finished successfully.")
                    append_log(json.dumps(summary.get("files", {}), indent=2, ensure_ascii=False))
                    show_toast("Fraction prediction finished.", kind="success")
                    messagebox.showinfo("Finished", f"Run finished.\n\nOutput folder:\n{summary.get('output_dir', '')}")
                else:
                    exc, tb = payload
                    var_status.set("Error.")
                    append_log(f"ERROR: {exc}\n{tb}")
                    show_toast("Fraction prediction failed.", kind="error")
                    messagebox.showerror("Run failed", str(exc))
        except queue.Empty:
            pass
        root.after(150, poll_queues)

    # Layout
    root.grid_columnconfigure(0, weight=1)
    root.grid_rowconfigure(1, weight=1)

    header = ctk.CTkFrame(root, fg_color="transparent")
    header.grid(row=0, column=0, sticky="ew", padx=24, pady=(20, 12))
    header.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(header, text="Fraction predictor", font=font_header, text_color=colors["text"], anchor="w").grid(row=0, column=0, sticky="w")
    ctk.CTkLabel(header, text="Map LC-MS features to collected HPLC fractions, combine them with fluorescence bioactivity, and write publication-ready result tables.", font=font_subtitle, text_color=colors["muted"], anchor="w").grid(row=1, column=0, sticky="w", pady=(4, 0))
    actions = ctk.CTkFrame(header, fg_color="transparent")
    actions.grid(row=0, column=1, rowspan=2, sticky="e")
    run_button = make_button(actions, "Run", run_pipeline_from_gui, primary=True, width=92)
    run_button.pack(side="left", padx=(0, 8))
    make_button(actions, "Save config", save_config_dialog, width=112).pack(side="left", padx=(0, 8))
    make_button(actions, "Load config", load_config_dialog, width=112).pack(side="left", padx=(0, 8))
    make_button(actions, "Clear log", clear_log, width=100).pack(side="left", padx=(0, 8))
    make_button(actions, "Close", root.destroy, danger=True, width=82).pack(side="left")

    tabs = ctk.CTkTabview(root, fg_color=colors["surface"], segmented_button_fg_color=colors["card_alt"], segmented_button_selected_color=colors["accent"], segmented_button_selected_hover_color=colors["accent_hover"], segmented_button_unselected_color=colors["card_alt"], segmented_button_unselected_hover_color="#39414c", text_color=colors["text"], corner_radius=14)
    tabs.grid(row=1, column=0, sticky="nsew", padx=24, pady=(0, 12))
    workflow_tab = tabs.add("Workflow")
    plants_tab = tabs.add("Plants")
    info_tab = tabs.add("Information")
    log_tab = tabs.add("Log")
    for tab in [workflow_tab, plants_tab, info_tab, log_tab]:
        tab.grid_columnconfigure(0, weight=1)
        tab.grid_rowconfigure(0, weight=1)

    workflow_scroll = ctk.CTkScrollableFrame(workflow_tab, fg_color="transparent")
    workflow_scroll.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)

    card = make_card(workflow_scroll, "Files", "Choose the feature table that contains the columns needed for prediction. Most users only need this one table. Use the advanced annotation option only when annotations are stored in a separate file.")
    file_picker = ctk.CTkFrame(card, fg_color="transparent")
    file_picker.grid(row=0, column=1, columnspan=5, sticky="ew", padx=(0, 18), pady=7)
    file_picker.grid_columnconfigure(0, weight=1)
    make_entry(file_picker, var_feature_table, "Feature table CSV/XLSX").grid(row=0, column=0, sticky="ew", padx=(0, 8))
    make_button(file_picker, "Browse", lambda: browse_file(var_feature_table, "Choose main feature table", after=load_feature_columns), width=82).grid(row=0, column=1, padx=(0, 8))
    make_button(file_picker, "Load columns", load_feature_columns, primary=True, width=112).grid(row=0, column=2)
    label = ctk.CTkFrame(card, fg_color="transparent")
    label.grid(row=0, column=0, sticky="ew", padx=(0, 10), pady=7)
    ctk.CTkLabel(label, text="Feature table", font=font_label, text_color=colors["muted"]).pack(side="left")
    make_help(label, "The table used for prediction. It must contain feature identifiers, m/z, UPLC retention time, and one or more peak-area sample columns. It may already be annotated; annotations will simply stay in the output tables.").pack(side="left", padx=(6, 0))
    ctk.CTkLabel(card, textvariable=var_feature_summary, font=font_small, text_color=colors["muted"], anchor="w").grid(row=1, column=1, columnspan=5, sticky="ew", padx=(0, 18), pady=(0, 7))
    type_row = ctk.CTkFrame(card, fg_color="transparent")
    type_row.grid(row=2, column=1, columnspan=5, sticky="ew", padx=(0, 18), pady=7)
    type_switch = ctk.CTkSegmentedButton(type_row, values=["unannotated", "annotated"], variable=var_feature_table_type, fg_color=colors["card_alt"], selected_color=colors["accent"], selected_hover_color=colors["accent_hover"], unselected_color=colors["card_alt"], unselected_hover_color="#39414c", text_color=colors["text"], command=lambda _v: save_state())
    type_switch.pack(fill="x")
    label = ctk.CTkFrame(card, fg_color="transparent")
    label.grid(row=2, column=0, sticky="ew", padx=(0, 10), pady=7)
    ctk.CTkLabel(label, text="Table type", font=font_label, text_color=colors["muted"]).pack(side="left")
    make_help(label, "Choose annotated if this same table already contains formula/class/network/compound annotations. This does not change the computation; it tells the interface how to describe the output and avoids asking for a second table.").pack(side="left", padx=(6, 0))
    out_frame = ctk.CTkFrame(card, fg_color="transparent")
    out_frame.grid(row=3, column=1, columnspan=5, sticky="ew", padx=(0, 18), pady=7)
    out_frame.grid_columnconfigure(0, weight=1)
    make_entry(out_frame, var_output_dir, "Outputs").grid(row=0, column=0, sticky="ew", padx=(0, 8))
    make_button(out_frame, "Browse", browse_output_dir, width=82).grid(row=0, column=1)
    label = ctk.CTkFrame(card, fg_color="transparent")
    label.grid(row=3, column=0, sticky="ew", padx=(0, 10), pady=7)
    ctk.CTkLabel(label, text="Output folder", font=font_label, text_color=colors["muted"]).pack(side="left")
    make_help(label, "Folder where the script writes filtered features, fraction windows, feature predictions, bioactivity tables, the optional human-readable report, Post_run_analysis charts, and run_summary.json.").pack(side="left", padx=(6, 0))
    advanced_row = ctk.CTkFrame(card, fg_color="transparent")
    advanced_row.grid(row=4, column=1, columnspan=5, sticky="w", padx=(0, 18), pady=(8, 0))
    ctk.CTkCheckBox(advanced_row, text="Use a separate annotation table", variable=var_use_separate_annotation, command=refresh_annotation_section, font=font_small, text_color=colors["text"], fg_color=colors["accent"], hover_color=colors["accent_hover"], border_color=colors["border"], checkbox_width=20, checkbox_height=20).pack(side="left")
    make_help(advanced_row, "Enable this only if your RT/mz/peak-area table and your annotation table are two different files. The script will match them by feature ID and write a combined annotated output table.").pack(side="left", padx=(8, 0))
    separate_annotation_frame = ctk.CTkFrame(card, fg_color=colors["card_alt"], corner_radius=12)
    separate_annotation_frame.grid_columnconfigure(1, weight=1)
    append_picker = ctk.CTkFrame(separate_annotation_frame, fg_color="transparent")
    append_picker.grid(row=0, column=1, columnspan=5, sticky="ew", padx=(0, 14), pady=(12, 7))
    append_picker.grid_columnconfigure(0, weight=1)
    make_entry(append_picker, var_append_table, "Separate annotation table CSV/XLSX").grid(row=0, column=0, sticky="ew", padx=(0, 8))
    make_button(append_picker, "Browse", lambda: browse_file(var_append_table, "Choose separate annotation table"), width=82).grid(row=0, column=1)
    label = ctk.CTkFrame(separate_annotation_frame, fg_color="transparent")
    label.grid(row=0, column=0, sticky="ew", padx=14, pady=(12, 7))
    ctk.CTkLabel(label, text="Annotation table", font=font_label, text_color=colors["muted"]).pack(side="left")
    make_help(label, "Optional separate table that contains annotations but not necessarily the raw peak-area and retention-time columns. It must share the same feature ID column as the prediction table.").pack(side="left", padx=(6, 0))

    card = make_card(workflow_scroll, "Feature columns and filtering", "Tell the script which columns contain feature identity, m/z, UPLC retention time, and sample peak areas.")
    combo_id_col = make_combo(card, var_id_col, ["row ID"])
    combo_mz_col = make_combo(card, var_mz_col, ["row m/z"])
    combo_rt_col = make_combo(card, var_rt_col, ["row retention time"])
    labeled_widget(card, "ID column", combo_id_col, 0, 0, help_text="Feature identifier column used for outputs and for matching prediction results back to the same annotated table or a separate annotation table. If missing, the core creates a row-number ID, but a real stable ID is better.")
    labeled_widget(card, "m/z column", combo_mz_col, 0, 2, help_text="Column containing feature m/z values. It is retained in output tables for interpretation and notebook QC.")
    labeled_widget(card, "UPLC RT column", combo_rt_col, 1, 0, help_text="Column containing UPLC retention time. This is converted to predicted HPLC retention time using the selected calibration.")
    labeled_widget(card, "Area threshold", make_entry(card, var_threshold, "10000"), 1, 2, help_text="A feature is kept if at least one selected sample peak-area column is greater than this threshold. Use this to remove weak/noisy features before fraction prediction.")
    labeled_widget(card, "Predicted RT column", make_entry(card, var_predicted_rt_col, "predicted_hplc_rt"), 2, 0, help_text="Name of the output column that stores predicted HPLC retention time.")
    sample_row = ctk.CTkFrame(card, fg_color="transparent")
    sample_row.grid(row=3, column=0, columnspan=6, sticky="ew", pady=(8, 4))
    sample_row.grid_columnconfigure(0, weight=1)
    var_sample_add = tk.StringVar(value="")
    combo_sample_add = make_combo(sample_row, var_sample_add, [""])
    combo_sample_add.grid(row=0, column=0, sticky="ew", padx=(0, 8))
    make_button(sample_row, "Add sample column", lambda: add_sample_column(), primary=True, width=150).grid(row=0, column=1)
    make_help(sample_row, "Add the peak-area columns used to decide which features pass the area threshold. In most workflows, each selected sample is also one plant or extract, so the GUI prepares a matching plant card automatically.").grid(row=0, column=2, padx=(8, 0))
    sample_list_frame = ctk.CTkFrame(card, fg_color="transparent")
    sample_list_frame.grid(row=4, column=0, columnspan=6, sticky="ew", pady=(4, 0))

    card = make_card(workflow_scroll, "Calibration", "Convert UPLC retention time to predicted HPLC retention time. This is the key step that connects LC-MS features to collected HPLC fractions.")
    var_cal_hint = tk.StringVar(value="")
    mode_switch = ctk.CTkSegmentedButton(card, values=["pairs", "equation", "runtime_scale"], variable=var_calibration_mode, command=refresh_calibration_mode, fg_color=colors["card_alt"], selected_color=colors["accent"], selected_hover_color=colors["accent_hover"], unselected_color=colors["card_alt"], unselected_hover_color="#39414c", text_color=colors["text"])
    mode_switch.grid(row=0, column=0, columnspan=6, sticky="ew", pady=(0, 8))
    ctk.CTkLabel(card, textvariable=var_cal_hint, font=font_small, text_color=colors["muted"], wraplength=980, justify="left").grid(row=1, column=0, columnspan=6, sticky="ew", pady=(0, 8))
    cal_pairs_frame = ctk.CTkFrame(card, fg_color="transparent")
    cal_pairs_frame.grid_columnconfigure(1, weight=1)
    combo_pairs_uplc = make_combo(cal_pairs_frame, var_pairs_uplc_col, ["UPLC RT"])
    combo_pairs_hplc = make_combo(cal_pairs_frame, var_pairs_hplc_col, ["HPLC RT"])
    pairs_picker = ctk.CTkFrame(cal_pairs_frame, fg_color="transparent")
    pairs_picker.grid(row=0, column=1, columnspan=5, sticky="ew", padx=(0, 18), pady=7)
    pairs_picker.grid_columnconfigure(0, weight=1)
    make_entry(pairs_picker, var_pairs_file, "manual_guesses.xlsx").grid(row=0, column=0, sticky="ew", padx=(0, 8))
    make_button(pairs_picker, "Browse", lambda: browse_file(var_pairs_file, "Choose calibration pairs table", after=load_pairs_columns), width=82).grid(row=0, column=1, padx=(0, 8))
    make_button(pairs_picker, "Load columns", load_pairs_columns, width=112).grid(row=0, column=2)
    label = ctk.CTkFrame(cal_pairs_frame, fg_color="transparent")
    label.grid(row=0, column=0, sticky="ew", padx=(0, 10), pady=7)
    ctk.CTkLabel(label, text="Pairs file", font=font_label, text_color=colors["muted"]).pack(side="left")
    make_help(label, "Optional if manual pairs are entered below. The table should contain matched UPLC and HPLC retention times for the same compounds or trusted manual guesses.").pack(side="left", padx=(6, 0))
    ctk.CTkLabel(cal_pairs_frame, textvariable=var_pairs_summary, font=font_small, text_color=colors["muted"], anchor="w").grid(row=1, column=1, columnspan=5, sticky="ew", padx=(0, 18), pady=(0, 7))
    labeled_widget(cal_pairs_frame, "UPLC RT column", combo_pairs_uplc, 2, 0, help_text="Column in the calibration table containing UPLC retention times.")
    labeled_widget(cal_pairs_frame, "HPLC RT column", combo_pairs_hplc, 2, 2, help_text="Column in the calibration table containing the corresponding HPLC retention times.")
    labeled_widget(cal_pairs_frame, "Manual pairs", make_entry(cal_pairs_frame, var_pairs_manual, "4.12,8.05"), 3, 0, help_text="Optional. Enter one UPLC,HPLC pair per line. If filled, these manual pairs are used instead of the pairs file.")
    cal_eq_frame = ctk.CTkFrame(card, fg_color="transparent")
    labeled_widget(cal_eq_frame, "Slope", make_entry(cal_eq_frame, var_equation_slope, "1.0"), 0, 0, help_text="Multiplier in HPLC RT = slope * UPLC RT + intercept.")
    labeled_widget(cal_eq_frame, "Intercept", make_entry(cal_eq_frame, var_equation_intercept, "0.0"), 0, 2, help_text="Offset in HPLC RT = slope * UPLC RT + intercept.")
    cal_runtime_frame = ctk.CTkFrame(card, fg_color="transparent")
    labeled_widget(cal_runtime_frame, "UPLC runtime", make_entry(cal_runtime_frame, var_runtime_uplc, "19.3"), 0, 0, help_text="Total UPLC gradient runtime. Used only for approximate runtime scaling.")
    labeled_widget(cal_runtime_frame, "HPLC runtime", make_entry(cal_runtime_frame, var_runtime_hplc, "38.0"), 0, 2, help_text="Total HPLC gradient runtime. Used only for approximate runtime scaling.")

    card = make_card(workflow_scroll, "Fractions and bioactivity", "Define the collected fraction windows and the bioactivity grouping thresholds.")
    labeled_widget(card, "Start time", make_entry(card, var_fraction_start, "2.0"), 0, 0, help_text="HPLC time at the beginning of fraction collection. Fractions before this time will not receive a fraction number.")
    labeled_widget(card, "End time", make_entry(card, var_fraction_end, "38.0"), 0, 2, help_text="HPLC time at the end of fraction collection. Together with start time and number of fractions, this creates fraction windows.")
    labeled_widget(card, "Number of fractions", make_entry(card, var_fraction_n, "96"), 1, 0, help_text="Total number of collected fractions. For a 96-well plate, this is often 96.")
    labeled_widget(card, "First fraction #", make_entry(card, var_first_fraction, "1"), 1, 2, help_text="Label of the first fraction. Use 1 unless your fraction table starts from another number.")
    labeled_widget(card, "Bioactivity cutoffs", make_entry(card, var_cutoffs, "16.5, 22.5"), 2, 0, help_text="Comma-separated bioactivity thresholds used to group fractions. Example: 16.5, 22.5 creates low, middle, and high activity groups.")

    plants_body = ctk.CTkFrame(plants_tab, fg_color="transparent")
    plants_body.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
    plants_body.grid_columnconfigure(0, weight=1)
    plants_body.grid_rowconfigure(1, weight=1)
    plants_header = ctk.CTkFrame(plants_body, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=16)
    plants_header.grid(row=0, column=0, sticky="ew", pady=(0, 12))
    plants_header.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(plants_header, text="Plant-specific bioactivity inputs", font=font_card_title, text_color=colors["text"], anchor="w").grid(row=0, column=0, sticky="ew", padx=16, pady=(14, 4))
    ctk.CTkLabel(plants_header, text="Plant cards are created from selected filtering samples. Complete the plant name and fluorescence table for each sample. Use Add plant only for non-standard workflows.", font=font_small, text_color=colors["muted"], anchor="w", justify="left", wraplength=980).grid(row=1, column=0, sticky="ew", padx=16, pady=(0, 14))
    make_button(plants_header, "Add plant", lambda: add_plant(), primary=True, width=112).grid(row=0, column=1, rowspan=2, sticky="e", padx=16, pady=14)
    plants_scroll = ctk.CTkScrollableFrame(plants_body, fg_color="transparent")
    plants_scroll.grid(row=1, column=0, sticky="nsew")
    plants_container = ctk.CTkFrame(plants_scroll, fg_color="transparent")
    plants_container.pack(fill="x")

    info_scroll = ctk.CTkScrollableFrame(info_tab, fg_color="transparent")
    info_scroll.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
    info_card = ctk.CTkFrame(info_scroll, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=16)
    info_card.pack(fill="x", padx=6, pady=(0, 12))
    ctk.CTkLabel(info_card, text="What this script does", font=font_card_title, text_color=colors["text"], anchor="w").pack(anchor="w", padx=18, pady=(16, 8))
    info_paragraphs = [
        "This script connects LC-MS feature tables to HPLC microfractions. It starts from a feature table, keeps features that pass a peak-area threshold in selected plant or extract columns, converts UPLC retention time into predicted HPLC retention time, and assigns each feature to the collected fraction window where it should elute.",
        "Most users should provide one feature table. If that table is already annotated, choose the annotated table type and the GUI will also use it as the annotation target for the final combined output. Use the separate annotation table option only when the quantitative table used for prediction and the annotation table used for interpretation are two different files.",
        "The calibration step is the most important decision. If matched compounds or trusted manual retention-time guesses are available, use the pairs mode. Equation mode is useful when a regression has already been calculated elsewhere. Runtime scaling is included as a fallback, but it is only appropriate when the UPLC and HPLC gradients are comparable enough that simple proportional scaling is defensible.",
        "For each plant card, the script reads a fluorescence table with fraction numbers, average fluorescence, and positive-control values. It normalizes the fluorescence, converts it to bioactivity, groups fractions by the cutoffs you choose, and then transfers the strongest mapped fraction activity back onto the features predicted to appear in those fractions.",
        "The main output is a set of CSV files in the output folder: filtered features, generated fraction windows, feature-to-fraction predictions, features with bioactivity columns, one bioactivity-by-fraction table per plant, and a run_summary.json file. If the main table is marked as annotated or a separate annotation table is supplied, the script also writes a compact human-readable feature report with target-plant presence, absence, fraction, and bioactivity columns near the front.",
        "After each run, the Post_run_analysis folder gives a quick overview for human review. It contains compact summary CSV files plus pastel PNG and SVG figures: a feature-prioritization funnel, feature counts per predicted fraction, and plant-specific presence/activity summaries. Use these first to understand whether the run produced biologically useful contrasts before reading every feature row.",
        "The notebook is the analysis and visualization companion for this core. Use the GUI or JSON config to create a run, then open the notebook to inspect calibration quality, feature counts per fraction, plant-specific bioactivity plots, group counts, and top features. The notebook should be used for review and figure generation, while the core script remains the reproducible processing engine.",
    ]
    for paragraph in info_paragraphs:
        ctk.CTkLabel(info_card, text=paragraph, font=font_label, text_color=colors["text"], justify="left", wraplength=1050, anchor="w").pack(anchor="w", fill="x", padx=18, pady=(0, 12))
    ctk.CTkLabel(info_card, text="Notebook file", font=font_card_title, text_color=colors["text"], anchor="w").pack(anchor="w", padx=18, pady=(8, 8))
    ctk.CTkLabel(info_card, text=str(_THIS_DIR / "jn_05_02_fraction_visualization_notebook.ipynb"), font=font_mono, text_color=colors["muted"], justify="left", wraplength=1050, anchor="w").pack(anchor="w", fill="x", padx=18, pady=(0, 18))

    log_panel = ctk.CTkFrame(log_tab, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=16)
    log_panel.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
    log_panel.grid_columnconfigure(0, weight=1)
    log_panel.grid_rowconfigure(1, weight=1)
    ctk.CTkLabel(log_panel, text="Run log", font=font_card_title, text_color=colors["text"], anchor="w").grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
    txt_log = ctk.CTkTextbox(log_panel, fg_color=colors["entry"], border_color=colors["border"], border_width=1, text_color=colors["text"], font=font_mono, corner_radius=10, wrap="word")
    txt_log.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 16))
    txt_log.insert("end", "Ready. Run messages will appear here.\n")
    txt_log.configure(state="disabled")

    progress = ctk.CTkProgressBar(root, mode="indeterminate", height=10, progress_color=colors["accent"])
    progress.grid(row=2, column=0, sticky="ew", padx=24, pady=(0, 8))
    progress.stop()
    status_bar = ctk.CTkFrame(root, fg_color=colors["surface"], corner_radius=0)
    status_bar.grid(row=3, column=0, sticky="ew")
    ctk.CTkLabel(status_bar, textvariable=var_status, font=font_small, text_color=colors["muted"], anchor="w").pack(fill="x", padx=18, pady=8)

    for sample in app_state.get("sample_columns", []) or []:
        sample_column_vars.append(tk.StringVar(value=str(sample)))
    for plant in app_state.get("plants", []) or []:
        add_plant(plant)
    refresh_sample_list()
    refresh_plants()
    refresh_calibration_mode()
    refresh_annotation_section()
    if var_feature_table.get().strip():
        try:
            load_feature_columns()
        except Exception:
            pass
    if var_pairs_file.get().strip():
        try:
            load_pairs_columns()
        except Exception:
            pass
    poll_queues()

    try:
        root.mainloop()
    finally:
        save_state()
        logger.removeHandler(gui_log_handler)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
