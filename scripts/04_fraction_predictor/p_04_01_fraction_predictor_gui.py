#!/usr/bin/env python3
"""CustomTkinter GUI for fraction prediction and bioactivity mapping."""
from __future__ import annotations

import json
import logging
import queue
import sys
import threading
import traceback
from pathlib import Path
from typing import Any

import pandas as pd

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))
_SHARED_DIR = _THIS_DIR.parent
if str(_SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(_SHARED_DIR))

from shared.gui_help_popover import HelpPopoverController

try:
    import p_04_00_fraction_predictor_core as core
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "ERROR: Could not import p_04_00_fraction_predictor_core.py.\n"
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

    state_file = _THIS_DIR / ".p_04_01_fraction_predictor_gui_state.json"
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

    plate_scale_labels = {
        "Relative range within each plate (0-100)": "positive_control_then_minmax_0_100",
        "Percent of positive control": "positive_control_pct",
        "Raw fluorescence (no scaling)": "none",
    }
    grouping_value_labels = {
        "Derived activity (100 - relative signal)": "bioactivity",
        "Relative fluorescence / signal (% of max)": "fluorescence_percent",
        "Raw or plate-processed signal average": "average",
    }
    calibration_mode_labels = {
        "Matched RT pairs": "pairs",
        "Equation": "equation",
        "Runtime scaling": "runtime_scale",
        "Feature-order alignment": "feature_order_alignment",
    }

    def plate_scale_label(value: str) -> str:
        if value in {"positive_control_then_minmax_0_100", "minmax_0_100", "control_then_minmax_0_100"}:
            return "Relative range within each plate (0-100)"
        return next((label for label, code in plate_scale_labels.items() if code == value), value)

    def plate_scale_code(value: str) -> str:
        return plate_scale_labels.get(value, value)

    def grouping_value_label(value: str) -> str:
        aliases = {
            "activity": "bioactivity",
            "derived_activity": "bioactivity",
            "relative_fluorescence": "fluorescence_percent",
            "fluorescence": "fluorescence_percent",
            "raw": "average",
            "raw_signal": "average",
        }
        normalized = aliases.get(str(value).strip().lower(), str(value).strip())
        return next((label for label, code in grouping_value_labels.items() if code == normalized), value)

    def grouping_value_code(value: str) -> str:
        return grouping_value_labels.get(value, value)

    def calibration_mode_label(value: str) -> str:
        normalized = str(value).strip().lower().replace(" ", "_").replace("-", "_")
        aliases = {
            "matched_rt_pairs": "pairs",
            "matched_pairs": "pairs",
            "runtime_scaling": "runtime_scale",
            "feature_order": "feature_order_alignment",
            "feature_order_alignment": "feature_order_alignment",
        }
        normalized = aliases.get(normalized, normalized)
        return next((label for label, code in calibration_mode_labels.items() if code == normalized), value)

    def calibration_mode_code(value: str) -> str:
        return calibration_mode_labels.get(value, str(value).strip())

    app_state = load_json_safe(state_file)
    root = ctk.CTk()
    root.title("Fraction predictor")
    root.geometry("1340x920")
    root.minsize(1160, 780)
    root.configure(fg_color=colors["bg"])
    help_popovers = HelpPopoverController(root, ctk, colors, font_small)

    var_status = tk.StringVar(value="Ready.")
    var_output_dir = tk.StringVar(value=str(app_state.get("output_dir", str(_THIS_DIR / "Outputs"))))
    var_feature_table = tk.StringVar(value=str(app_state.get("feature_table", "")))
    var_append_table = tk.StringVar(value=str(app_state.get("append_table", "")))
    var_annotation_feature_match_col = tk.StringVar(value=str(app_state.get("annotation_feature_match_column", app_state.get("id_column", "row ID"))))
    var_annotation_table_match_col = tk.StringVar(value=str(app_state.get("annotation_table_match_column", app_state.get("id_column", "row ID"))))
    var_annotation_add_col = tk.StringVar(value="")
    var_annotation_columns_to_add = tk.StringVar(value=str(app_state.get("annotation_columns_to_add", "")))
    var_annotation_summary = tk.StringVar(value="Load the annotation table to choose matching and annotation columns.")
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
    var_grouping_value = tk.StringVar(value=grouping_value_label(str(app_state.get("grouping_value", "bioactivity"))))
    var_calibration_mode = tk.StringVar(value=calibration_mode_label(str(app_state.get("calibration_mode", "pairs"))))
    var_pairs_file = tk.StringVar(value=str(app_state.get("pairs_file", "")))
    var_pairs_uplc_col = tk.StringVar(value=str(app_state.get("pairs_uplc_col", "UPLC RT")))
    var_pairs_hplc_col = tk.StringVar(value=str(app_state.get("pairs_hplc_col", "HPLC RT")))
    var_pairs_manual = tk.StringVar(value=str(app_state.get("pairs_manual", "")))
    var_equation_slope = tk.StringVar(value=str(app_state.get("equation_slope", 1.0)))
    var_equation_intercept = tk.StringVar(value=str(app_state.get("equation_intercept", 0.0)))
    var_runtime_uplc = tk.StringVar(value=str(app_state.get("runtime_uplc", 19.3)))
    var_runtime_hplc = tk.StringVar(value=str(app_state.get("runtime_hplc", 38.0)))
    var_hplc_filtered_table = tk.StringVar(value=str(app_state.get("hplc_filtered_table", "")))
    var_hplc_id_col = tk.StringVar(value=str(app_state.get("hplc_id_column", "row ID")))
    var_hplc_fraction_col = tk.StringVar(value=str(app_state.get("hplc_fraction_column", "fraction_index")))
    var_hplc_summary = tk.StringVar(value="Load the script 01 filtered table to choose HPLC landmarks.")
    var_alignment_hrms_search = tk.StringVar(value="")
    var_alignment_hplc_search = tk.StringVar(value="")
    var_alignment_sample_col = tk.StringVar(value="")
    var_alignment_sample_threshold = tk.StringVar(value="")
    var_alignment_rt_min = tk.StringVar(value="")
    var_alignment_rt_max = tk.StringVar(value="")
    var_alignment_mz_min = tk.StringVar(value="")
    var_alignment_mz_max = tk.StringVar(value="")
    var_alignment_rows_per_page = tk.StringVar(value="100")
    var_alignment_hrms_page_info = tk.StringVar(value="HRMS: load a feature table.")
    var_alignment_hplc_page_info = tk.StringVar(value="HPLC: load a filtered table.")
    var_feature_summary = tk.StringVar(value="Load a feature table to populate column pickers.")
    var_pairs_summary = tk.StringVar(value="Load a calibration table if you want to choose its columns.")

    feature_columns: list[str] = []
    annotation_columns: list[str] = []
    pairs_columns: list[str] = []
    hplc_columns: list[str] = []
    feature_preview_df: pd.DataFrame | None = None
    hplc_preview_df: pd.DataFrame | None = None
    landmark_rows: list[dict[str, Any]] = list(app_state.get("feature_order_landmarks", []) or [])
    feature_order_index_maps: dict[str, list[int]] = {"workflow_hrms": [], "workflow_hplc": [], "alignment_hrms": [], "alignment_hplc": []}
    alignment_pages = {"hrms": 1, "hplc": 1}
    sample_column_vars: list[tk.StringVar] = []
    plant_cards: list[dict[str, Any]] = []
    worker_state = {"running": False, "previewing": False}
    log_queue: queue.Queue[str] = queue.Queue()
    result_queue: queue.Queue[tuple[bool, Any]] = queue.Queue()
    preview_queue: queue.Queue[tuple[bool, Any]] = queue.Queue()
    preview_state: dict[str, Any] = {"data": None, "canvas": None}

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

    def make_help(parent: Any, text: str) -> Any:
        return help_popovers.create_bubble(parent, text)

    def make_button(parent: Any, text: str, command: Any, *, primary: bool = False, success: bool = False, danger: bool = False, width: int | None = None) -> Any:
        color = colors["success"] if success else colors["danger"] if danger else colors["accent"] if primary else colors["card_alt"]
        hover = colors["success_hover"] if success else colors["danger_hover"] if danger else colors["accent_hover"] if primary else "#39414c"
        return ctk.CTkButton(parent, text=text, command=command, width=width or 110, height=36, corner_radius=9, fg_color=color, hover_color=hover)

    def make_entry(parent: Any, var: tk.StringVar, placeholder: str = "") -> Any:
        return ctk.CTkEntry(parent, textvariable=var, placeholder_text=placeholder, width=80, fg_color=colors["entry"], border_color=colors["border"], text_color=colors["text"], height=35, corner_radius=8)

    def make_combo(parent: Any, var: tk.StringVar, values: list[str]) -> Any:
        return ctk.CTkComboBox(parent, variable=var, values=values or [""], width=80, fg_color=colors["entry"], border_color=colors["border"], button_color=colors["accent"], button_hover_color=colors["accent_hover"], dropdown_fg_color=colors["card"], dropdown_hover_color=colors["card_alt"], text_color=colors["text"], dropdown_text_color=colors["text"], height=35, corner_radius=8)

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
        label_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(label_frame, text=label, font=font_label, text_color=colors["muted"], anchor="w").grid(row=0, column=0, sticky="w")
        make_help(label_frame, help_text).grid(row=0, column=1, sticky="e", padx=(6, 0))
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

    def browse_plate_files(var: tk.StringVar) -> None:
        paths = filedialog.askopenfilenames(
            title="Choose replicate plate-reader files",
            initialdir=str(_THIS_DIR),
            filetypes=table_patterns,
        )
        if paths:
            var.set("; ".join(paths))
            save_state()

    def split_paths(text: str) -> list[str]:
        normalized = text.replace("\n", ";").replace(",", ";")
        return [part.strip().strip('"') for part in normalized.split(";") if part.strip().strip('"')]

    def split_column_names(text: str) -> list[str]:
        normalized = text.replace("\n", ",").replace(";", ",")
        return [part.strip() for part in normalized.split(",") if part.strip()]

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
        for combo in [combo_id_col, combo_mz_col, combo_rt_col, combo_sample_add, combo_annotation_feature_match_col]:
            combo.configure(values=values)
        try:
            combo_alignment_sample_col.configure(values=values)
        except NameError:
            pass
        if not var_annotation_feature_match_col.get().strip() and var_id_col.get().strip():
            var_annotation_feature_match_col.set(var_id_col.get().strip())
        refresh_sample_list()
        refresh_plants()

    def refresh_annotation_column_list() -> None:
        for child in annotation_selected_frame.winfo_children():
            child.destroy()
        selected = split_column_names(var_annotation_columns_to_add.get())
        if not selected:
            ctk.CTkLabel(
                annotation_selected_frame,
                text="No specific annotation columns selected. The final report will keep all columns from the annotation table.",
                font=font_small,
                text_color=colors["muted"],
                anchor="w",
                wraplength=760,
                justify="left",
            ).pack(anchor="w", pady=4)
            return
        for idx, col in enumerate(selected):
            row = ctk.CTkFrame(annotation_selected_frame, fg_color=colors["card"], corner_radius=9)
            row.pack(fill="x", pady=(0, 5))
            ctk.CTkLabel(row, text=col, font=font_small, text_color=colors["text"], anchor="w").pack(side="left", fill="x", expand=True, padx=10, pady=7)
            make_button(row, "Remove", lambda i=idx: remove_annotation_column(i), danger=True, width=76).pack(side="right", padx=6, pady=5)

    def refresh_annotation_column_widgets() -> None:
        values = ["", *annotation_columns]
        combo_annotation_table_match_col.configure(values=values)
        combo_annotation_add_col.configure(values=values)
        if not var_annotation_table_match_col.get().strip():
            var_annotation_table_match_col.set(var_id_col.get().strip() or "row ID")
        refresh_annotation_column_list()

    def add_annotation_column() -> None:
        col = var_annotation_add_col.get().strip()
        if not col:
            show_toast("Choose an annotation column first.", kind="warning")
            return
        selected = split_column_names(var_annotation_columns_to_add.get())
        if col not in selected:
            selected.append(col)
            var_annotation_columns_to_add.set(", ".join(selected))
            refresh_annotation_column_list()
            save_state()

    def remove_annotation_column(index: int) -> None:
        selected = split_column_names(var_annotation_columns_to_add.get())
        if 0 <= index < len(selected):
            selected.pop(index)
            var_annotation_columns_to_add.set(", ".join(selected))
            refresh_annotation_column_list()
            save_state()

    def use_all_annotation_columns() -> None:
        if not annotation_columns:
            show_toast("Load annotation columns first.", kind="warning")
            return
        match_col = var_annotation_table_match_col.get().strip()
        selected = [col for col in annotation_columns if col != match_col]
        var_annotation_columns_to_add.set(", ".join(selected))
        refresh_annotation_column_list()
        save_state()

    def clear_annotation_columns() -> None:
        var_annotation_columns_to_add.set("")
        refresh_annotation_column_list()
        save_state()

    def load_feature_columns() -> None:
        nonlocal feature_columns, feature_preview_df
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
            feature_preview_df = df
            feature_columns = [str(col) for col in df.columns]
            var_feature_summary.set(f"Loaded {len(df)} row(s) and {len(feature_columns)} column(s).")
            refresh_feature_column_widgets()
            try:
                refresh_feature_order_lists()
            except NameError:
                pass
            show_toast("Feature table columns loaded.", kind="success")
        except Exception as exc:
            messagebox.showerror("Feature table", str(exc))

    def load_annotation_columns() -> None:
        nonlocal annotation_columns
        path_text = var_append_table.get().strip()
        if not path_text:
            show_toast("Choose an annotation table first.", kind="warning")
            return
        path = Path(path_text).expanduser()
        if not path.exists():
            messagebox.showerror("Annotation table", f"File not found:\n{path}")
            return
        try:
            df = core.read_table(path)
            annotation_columns = [str(col) for col in df.columns]
            var_annotation_summary.set(f"Loaded {len(df)} row(s) and {len(annotation_columns)} annotation column(s).")
            if var_id_col.get().strip() in annotation_columns:
                var_annotation_table_match_col.set(var_id_col.get().strip())
            refresh_annotation_column_widgets()
            show_toast("Annotation table columns loaded.", kind="success")
        except Exception as exc:
            messagebox.showerror("Annotation table", str(exc))

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

    def _guess_column(columns: list[str], candidates: list[str], fallback: str = "") -> str:
        lower = {col.lower(): col for col in columns}
        for candidate in candidates:
            if candidate.lower() in lower:
                return lower[candidate.lower()]
        return fallback or (columns[0] if columns else "")

    def _row_label(row: pd.Series, *, id_col: str, rt_col: str | None = None, fraction_col: str | None = None, mz_col: str | None = None) -> str:
        parts = []
        if id_col and id_col in row.index:
            parts.append(f"ID {row.get(id_col)}")
        if rt_col and rt_col in row.index:
            parts.append(f"RT {row.get(rt_col)}")
        if fraction_col and fraction_col in row.index:
            parts.append(f"fraction {row.get(fraction_col)}")
        if mz_col and mz_col in row.index:
            parts.append(f"m/z {row.get(mz_col)}")
        return " | ".join(parts) if parts else str(row.name)

    def _parse_optional_float(text: str) -> float | None:
        stripped = str(text).strip()
        if not stripped:
            return None
        return float(stripped)

    def _rows_per_page() -> int:
        try:
            return max(20, min(1000, int(float(var_alignment_rows_per_page.get().strip() or "100"))))
        except ValueError:
            return 100

    def _text_search_mask(df: pd.DataFrame, query: str, labels: list[str] | None = None) -> pd.Series:
        query = query.strip().lower()
        if not query:
            return pd.Series(True, index=df.index)
        mask = pd.Series(False, index=df.index)
        columns = [col for col in (labels or []) if col and col in df.columns]
        if not columns:
            columns = list(df.columns)
        for col in columns:
            mask = mask | df[col].astype(str).str.lower().str.contains(query, na=False, regex=False)
        return mask

    def _numeric_range_mask(df: pd.DataFrame, column: str, low_text: str, high_text: str) -> pd.Series:
        if not column or column not in df.columns:
            return pd.Series(True, index=df.index)
        low = _parse_optional_float(low_text)
        high = _parse_optional_float(high_text)
        if low is None and high is None:
            return pd.Series(True, index=df.index)
        values = pd.to_numeric(df[column], errors="coerce")
        mask = values.notna()
        if low is not None:
            mask = mask & (values >= low)
        if high is not None:
            mask = mask & (values <= high)
        return mask

    def _hrms_filtered_indices() -> list[int]:
        if feature_preview_df is None or feature_preview_df.empty:
            return []
        df = feature_preview_df
        mask = _text_search_mask(df, var_alignment_hrms_search.get(), [var_id_col.get().strip(), var_rt_col.get().strip(), var_mz_col.get().strip()])
        sample_col = var_alignment_sample_col.get().strip()
        threshold = _parse_optional_float(var_alignment_sample_threshold.get())
        if sample_col and sample_col in df.columns and threshold is not None:
            mask = mask & (pd.to_numeric(df[sample_col], errors="coerce") >= threshold)
        mask = mask & _numeric_range_mask(df, var_rt_col.get().strip(), var_alignment_rt_min.get(), var_alignment_rt_max.get())
        mask = mask & _numeric_range_mask(df, var_mz_col.get().strip(), var_alignment_mz_min.get(), var_alignment_mz_max.get())
        return [position for position, keep in enumerate(mask.fillna(False).to_numpy()) if bool(keep)]

    def _hplc_filtered_indices() -> list[int]:
        if hplc_preview_df is None or hplc_preview_df.empty:
            return []
        df = hplc_preview_df
        search_cols = [var_hplc_id_col.get().strip(), var_hplc_fraction_col.get().strip()]
        rt_col = _guess_column(hplc_columns, ["rt", "retention_time", "matched_target_rt"], "")
        mz_col = _guess_column(hplc_columns, ["mz", "row m/z", "matched_target_mz"], "")
        mask = _text_search_mask(df, var_alignment_hplc_search.get(), [*search_cols, rt_col, mz_col])
        return [position for position, keep in enumerate(mask.fillna(False).to_numpy()) if bool(keep)]

    def _page_indices(indices: list[int], side: str) -> tuple[list[int], int, int, int]:
        page_size = _rows_per_page()
        total = len(indices)
        page_count = max(1, (total + page_size - 1) // page_size)
        page = max(1, min(alignment_pages[side], page_count))
        alignment_pages[side] = page
        start = (page - 1) * page_size
        return indices[start:start + page_size], page, page_count, total

    def _populate_feature_list(listbox: Any, key: str, indices: list[int], *, source: str) -> None:
        listbox.delete(0, "end")
        feature_order_index_maps[key] = indices
        if source == "hrms":
            df = feature_preview_df
            id_col = var_id_col.get().strip()
            rt_col = var_rt_col.get().strip()
            mz_col = var_mz_col.get().strip()
            fraction_col = None
        else:
            df = hplc_preview_df
            id_col = var_hplc_id_col.get().strip()
            rt_col = _guess_column(hplc_columns, ["rt", "retention_time", "matched_target_rt"], "")
            mz_col = _guess_column(hplc_columns, ["mz", "row m/z", "matched_target_mz"], "")
            fraction_col = var_hplc_fraction_col.get().strip()
        if df is None or df.empty:
            return
        for row_index in indices:
            row = df.iloc[row_index]
            listbox.insert("end", _row_label(row, id_col=id_col, rt_col=rt_col, fraction_col=fraction_col, mz_col=mz_col))

    def refresh_feature_order_lists(*, reset_pages: bool = False) -> None:
        try:
            list_hrms_features.delete(0, "end")
            list_hplc_features.delete(0, "end")
        except NameError:
            return
        if reset_pages:
            alignment_pages["hrms"] = 1
            alignment_pages["hplc"] = 1
        hrms_all = _hrms_filtered_indices()
        hplc_all = _hplc_filtered_indices()
        hrms_page, hrms_page_no, hrms_pages, hrms_total = _page_indices(hrms_all, "hrms")
        hplc_page, hplc_page_no, hplc_pages, hplc_total = _page_indices(hplc_all, "hplc")
        workflow_hrms = hrms_page[:100]
        workflow_hplc = hplc_page[:100]
        _populate_feature_list(list_hrms_features, "workflow_hrms", workflow_hrms, source="hrms")
        _populate_feature_list(list_hplc_features, "workflow_hplc", workflow_hplc, source="hplc")
        try:
            _populate_feature_list(list_alignment_hrms_features, "alignment_hrms", hrms_page, source="hrms")
            _populate_feature_list(list_alignment_hplc_features, "alignment_hplc", hplc_page, source="hplc")
        except NameError:
            pass
        shown_hrms_start = (hrms_page_no - 1) * _rows_per_page() + 1 if hrms_total else 0
        shown_hrms_end = shown_hrms_start + len(hrms_page) - 1 if hrms_total else 0
        shown_hplc_start = (hplc_page_no - 1) * _rows_per_page() + 1 if hplc_total else 0
        shown_hplc_end = shown_hplc_start + len(hplc_page) - 1 if hplc_total else 0
        var_alignment_hrms_page_info.set(f"HRMS: {shown_hrms_start}-{shown_hrms_end} of {hrms_total} filtered row(s), page {hrms_page_no}/{hrms_pages}")
        var_alignment_hplc_page_info.set(f"HPLC: {shown_hplc_start}-{shown_hplc_end} of {hplc_total} filtered row(s), page {hplc_page_no}/{hplc_pages}")

    def change_alignment_page(side: str, delta: int) -> None:
        alignment_pages[side] = max(1, alignment_pages[side] + delta)
        refresh_feature_order_lists()

    def apply_alignment_filters() -> None:
        try:
            _ = _parse_optional_float(var_alignment_sample_threshold.get())
            _ = _parse_optional_float(var_alignment_rt_min.get())
            _ = _parse_optional_float(var_alignment_rt_max.get())
            _ = _parse_optional_float(var_alignment_mz_min.get())
            _ = _parse_optional_float(var_alignment_mz_max.get())
        except ValueError:
            show_toast("Filter values must be numbers.", kind="warning")
            return
        refresh_feature_order_lists(reset_pages=True)

    def clear_alignment_filters() -> None:
        for var in [
            var_alignment_hrms_search,
            var_alignment_hplc_search,
            var_alignment_sample_col,
            var_alignment_sample_threshold,
            var_alignment_rt_min,
            var_alignment_rt_max,
            var_alignment_mz_min,
            var_alignment_mz_max,
        ]:
            var.set("")
        refresh_feature_order_lists(reset_pages=True)

    def load_hplc_filtered_columns() -> None:
        nonlocal hplc_columns, hplc_preview_df
        path_text = var_hplc_filtered_table.get().strip()
        if not path_text:
            show_toast("Choose the script 01 filtered HPLC table first.", kind="warning")
            return
        path = Path(path_text).expanduser()
        if not path.exists():
            messagebox.showerror("HPLC filtered table", f"File not found:\n{path}")
            return
        try:
            df = core.read_table(path)
            hplc_preview_df = df
            hplc_columns = [str(col) for col in df.columns]
            combo_hplc_id_col.configure(values=["", *hplc_columns])
            combo_hplc_fraction_col.configure(values=["", *hplc_columns])
            if not var_hplc_id_col.get().strip() or var_hplc_id_col.get().strip() not in hplc_columns:
                var_hplc_id_col.set(_guess_column(hplc_columns, ["row ID", "id", "feature_id", "matched_feature_id"], ""))
            if not var_hplc_fraction_col.get().strip() or var_hplc_fraction_col.get().strip() not in hplc_columns:
                var_hplc_fraction_col.set(_guess_column(hplc_columns, ["fraction_index", "fraction", "matched_fraction"], "fraction_index"))
            var_hplc_summary.set(f"Loaded {len(df)} HPLC row(s). Use Open Alignment tab to choose landmark pairs.")
            refresh_feature_order_lists()
            show_toast("HPLC filtered table loaded.", kind="success")
        except Exception as exc:
            messagebox.showerror("HPLC filtered table", str(exc))

    def render_landmark_rows() -> None:
        frames = []
        try:
            frames.append(landmark_table_frame)
        except NameError:
            pass
        try:
            frames.append(alignment_landmark_table_frame)
        except NameError:
            pass
        for frame in frames:
            for child in frame.winfo_children():
                child.destroy()
            if not landmark_rows:
                ctk.CTkLabel(
                frame,
                    text="No landmarks yet. Open the Alignment tab, select one HRMS feature and one HPLC filtered feature, then add a landmark.",
                    font=font_small,
                    text_color=colors["muted"],
                    anchor="w",
                    wraplength=900,
                    justify="left",
                ).pack(anchor="w", fill="x", pady=6)
                continue
            for index, row in enumerate(landmark_rows):
                item = ctk.CTkFrame(frame, fg_color=colors["card"], corner_radius=9)
                item.pack(fill="x", pady=(0, 6))
                text = (
                    f"{row.get('anchor_id', f'A{index + 1}')} | "
                    f"HRMS ID {row.get('hrms_feature_id', '')}, RT {row.get('hrms_rt', '')} -> "
                    f"HPLC ID {row.get('hplc_feature_id', '')}, fraction {row.get('hplc_fraction', '')}"
                )
                ctk.CTkLabel(item, text=text, font=font_small, text_color=colors["text"], anchor="w").pack(side="left", fill="x", expand=True, padx=10, pady=7)
                make_button(item, "Up", lambda i=index: move_landmark(i, -1), width=52).pack(side="right", padx=(0, 5), pady=5)
                make_button(item, "Down", lambda i=index: move_landmark(i, 1), width=62).pack(side="right", padx=(0, 5), pady=5)
                make_button(item, "Remove", lambda i=index: remove_landmark(i), danger=True, width=74).pack(side="right", padx=6, pady=5)

    def _selected_table_rows(hrms_listbox: Any, hplc_listbox: Any, hrms_key: str, hplc_key: str) -> tuple[pd.Series, pd.Series] | None:
        if feature_preview_df is None or feature_preview_df.empty:
            show_toast("Load the HRMS feature table first.", kind="warning")
            return None
        if hplc_preview_df is None or hplc_preview_df.empty:
            show_toast("Load the script 01 filtered HPLC table first.", kind="warning")
            return None
        hrms_sel = hrms_listbox.curselection()
        hplc_sel = hplc_listbox.curselection()
        if not hrms_sel or not hplc_sel:
            show_toast("Select one HRMS row and one HPLC row.", kind="warning")
            return None
        hrms_indices = feature_order_index_maps.get(hrms_key, [])
        hplc_indices = feature_order_index_maps.get(hplc_key, [])
        try:
            hrms_row = feature_preview_df.iloc[hrms_indices[int(hrms_sel[0])]]
            hplc_row = hplc_preview_df.iloc[hplc_indices[int(hplc_sel[0])]]
        except Exception:
            show_toast("Refresh the feature lists and select the rows again.", kind="warning")
            return None
        return hrms_row, hplc_row

    def add_landmark_from_selection(
        hrms_listbox: Any | None = None,
        hplc_listbox: Any | None = None,
        hrms_key: str = "workflow_hrms",
        hplc_key: str = "workflow_hplc",
    ) -> None:
        selected_rows = _selected_table_rows(hrms_listbox or list_hrms_features, hplc_listbox or list_hplc_features, hrms_key, hplc_key)
        if selected_rows is None:
            return
        hrms_row, hplc_row = selected_rows
        hrms_id_col = var_id_col.get().strip()
        hrms_rt_col = var_rt_col.get().strip()
        hplc_id_col = var_hplc_id_col.get().strip()
        hplc_fraction_col = var_hplc_fraction_col.get().strip()
        if hrms_rt_col not in hrms_row.index:
            messagebox.showerror("Feature-order alignment", "Choose a valid HRMS retention-time column first.")
            return
        if hplc_fraction_col not in hplc_row.index:
            messagebox.showerror("Feature-order alignment", "Choose the HPLC fraction-number column first.")
            return
        try:
            hrms_rt = float(pd.to_numeric(pd.Series([hrms_row[hrms_rt_col]]), errors="raise").iloc[0])
            hplc_fraction = int(float(pd.to_numeric(pd.Series([hplc_row[hplc_fraction_col]]), errors="raise").iloc[0]))
        except Exception as exc:
            messagebox.showerror("Feature-order alignment", f"Selected landmark has non-numeric RT or fraction value:\n{exc}")
            return
        landmark_rows.append({
            "anchor_id": f"A{len(landmark_rows) + 1}",
            "hrms_feature_id": str(hrms_row.get(hrms_id_col, hrms_row.name)) if hrms_id_col else str(hrms_row.name),
            "hrms_rt": hrms_rt,
            "hplc_feature_id": str(hplc_row.get(hplc_id_col, hplc_row.name)) if hplc_id_col else str(hplc_row.name),
            "hplc_fraction": hplc_fraction,
        })
        render_landmark_rows()
        save_state()
        show_toast("Landmark added.", kind="success")

    def remove_landmark(index: int) -> None:
        if 0 <= index < len(landmark_rows):
            landmark_rows.pop(index)
            for i, row in enumerate(landmark_rows, start=1):
                row["anchor_id"] = row.get("anchor_id") or f"A{i}"
            render_landmark_rows()
            save_state()

    def move_landmark(index: int, delta: int) -> None:
        new_index = index + delta
        if 0 <= index < len(landmark_rows) and 0 <= new_index < len(landmark_rows):
            landmark_rows[index], landmark_rows[new_index] = landmark_rows[new_index], landmark_rows[index]
            render_landmark_rows()
            save_state()

    def import_landmarks() -> None:
        path = filedialog.askopenfilename(title="Import feature-order landmarks", initialdir=str(_THIS_DIR), filetypes=table_patterns)
        if not path:
            return
        try:
            df = core.load_feature_order_landmarks({"calibration": {"landmarks_file": path}}, base_dir=_THIS_DIR)
            landmark_rows.clear()
            landmark_rows.extend(df.to_dict(orient="records"))
            render_landmark_rows()
            save_state()
            show_toast("Landmarks imported.", kind="success")
        except Exception as exc:
            messagebox.showerror("Import landmarks", str(exc))

    def export_landmarks() -> None:
        if not landmark_rows:
            show_toast("No landmarks to export.", kind="warning")
            return
        path = filedialog.asksaveasfilename(title="Export feature-order landmarks", initialdir=str(_THIS_DIR), defaultextension=".csv", filetypes=[("CSV", "*.csv")])
        if not path:
            return
        try:
            pd.DataFrame(landmark_rows).to_csv(path, index=False)
            show_toast("Landmarks exported.", kind="success")
        except Exception as exc:
            messagebox.showerror("Export landmarks", str(exc))

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

    def open_alignment_tab() -> None:
        var_calibration_mode.set("Feature-order alignment")
        refresh_calibration_mode()
        try:
            tabs.set("Alignment")
        except NameError:
            pass

    def refresh_calibration_mode(_choice: str | None = None) -> None:
        mode = calibration_mode_code(var_calibration_mode.get())
        cal_pairs_frame.grid_remove()
        cal_eq_frame.grid_remove()
        cal_runtime_frame.grid_remove()
        cal_feature_order_frame.grid_remove()
        if mode == "pairs":
            cal_pairs_frame.grid(row=2, column=0, columnspan=6, sticky="ew", pady=(8, 0))
            var_cal_hint.set("Recommended when matched compounds or manual guesses are available. Use at least five pairs when possible; the table should contain one UPLC retention time and one matching HPLC retention time per row.")
        elif mode == "equation":
            cal_eq_frame.grid(row=2, column=0, columnspan=6, sticky="ew", pady=(8, 0))
            var_cal_hint.set("Use this only if the regression has already been calculated elsewhere. The script applies HPLC RT = slope * UPLC RT + intercept.")
        elif mode == "runtime_scale":
            cal_runtime_frame.grid(row=2, column=0, columnspan=6, sticky="ew", pady=(8, 0))
            var_cal_hint.set("This is the roughest option. It scales retention time by total gradient runtime and assumes the two methods are comparable.")
        else:
            cal_feature_order_frame.grid(row=2, column=0, columnspan=6, sticky="ew", pady=(8, 0))
            var_cal_hint.set("Use this when HRMS and HPLC gradients are not comparable. It maps HRMS features only to candidate fraction intervals between manually paired landmark features; it does not predict an exact HPLC retention time.")
        save_state()

    def add_plant(initial: dict[str, Any] | None = None, *, save: bool = True) -> None:
        info = initial or {}
        input_is_plate = (
            str(info.get("fluorescence_input_type", "table")).lower() == "plate_reader"
            or bool(info.get("plate_files") or info.get("fluorescence_plate_files"))
        )
        plate_files_value = info.get("plate_files") or info.get("fluorescence_plate_files") or ""
        if not isinstance(plate_files_value, str):
            plate_files_value = "; ".join(str(path) for path in plate_files_value)
        plant_cards.append({
            "name": tk.StringVar(value=str(info.get("name", ""))),
            "sample_column": tk.StringVar(value=str(info.get("sample_column", ""))),
            "input_type": tk.StringVar(value="Plate reader" if input_is_plate else "Table"),
            "fluorescence_file": tk.StringVar(value=str(info.get("fluorescence_file", ""))),
            "fraction_column": tk.StringVar(value=str(info.get("fluorescence_fraction_column", "fraction"))),
            "average_column": tk.StringVar(value=str(info.get("fluorescence_average_column", "average"))),
            "positive_column": tk.StringVar(value=str(info.get("fluorescence_positive_control_column", "pos_avg"))),
            "plate_files": tk.StringVar(value=str(plate_files_value)),
            "plate_rows": tk.StringVar(value=str(info.get("plate_rows", 8))),
            "plate_columns": tk.StringVar(value=str(info.get("plate_columns", 12))),
            "plate_controls": tk.StringVar(value=str(info.get("plate_positive_control_wells", info.get("control_wells", "H11, H12")))),
            "plate_controls_by_file": tk.StringVar(value=str(info.get("plate_positive_control_wells_by_file", info.get("control_wells_by_plate", "")) or "")),
            "plate_scale_mode": tk.StringVar(value=plate_scale_label(str(info.get("plate_scale_mode", "positive_control_then_minmax_0_100")))),
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
            outer = ctk.CTkFrame(plants_container, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=14)
            outer.pack(fill="x", expand=True, padx=(0, 8), pady=(0, 12))
            box = ctk.CTkFrame(outer, fg_color="transparent")
            box.pack(fill="x", expand=True, padx=16, pady=14)
            box.grid_columnconfigure(0, minsize=150)
            box.grid_columnconfigure(1, weight=1)
            box.grid_columnconfigure(2, minsize=145)
            box.grid_columnconfigure(3, weight=1)
            title = ctk.CTkFrame(box, fg_color="transparent")
            title.grid(row=0, column=0, columnspan=4, sticky="ew", pady=(0, 6))
            ctk.CTkLabel(title, text=f"Plant {idx + 1}", font=font_card_title, text_color=colors["text"]).pack(side="left")
            make_button(title, "Remove", lambda i=idx: remove_plant(i), danger=True, width=86).pack(side="right")
            labeled_widget(box, "Plant name", make_entry(box, card["name"], "Macleaya microcarpa"), 1, 0, help_text="Name used in output column names and in plant-specific CSV files. Use the biological sample or species name that should appear in the final table.")
            combo_plant_sample = make_combo(box, card["sample_column"], plant_sample_column_values())
            combo_plant_sample.configure(command=lambda _choice: save_state())
            labeled_widget(box, "Sample column", combo_plant_sample, 1, 2, help_text="Peak-area column for this plant in the feature table. Columns selected for filtering appear first because they are usually the same samples that need plant cards; other loaded columns remain available for non-standard workflows.")
            input_combo = make_combo(box, card["input_type"], ["Table", "Plate reader"])
            input_combo.configure(command=lambda _choice: (save_state(), refresh_plants()))
            labeled_widget(box, "Bioactivity input", input_combo, 2, 0, help_text="Use Table when you already have one row per fraction. Use Plate reader when you have replicate raw well-plate exports and want the app to detect the plate grid, normalize controls, and map wells to fractions.")

            if card["input_type"].get() == "Plate reader":
                plate_row_offset = 3
                plate_files = ctk.CTkFrame(box, fg_color="transparent")
                plate_files.grid(row=plate_row_offset, column=1, columnspan=3, sticky="ew", padx=(0, 18), pady=7)
                plate_files.grid_columnconfigure(0, weight=1)
                make_entry(plate_files, card["plate_files"], "A_blue.xlsx; B_blue.xlsx").grid(row=0, column=0, sticky="ew", padx=(0, 8))
                make_button(plate_files, "Browse", lambda v=card["plate_files"]: browse_plate_files(v), width=82).grid(row=0, column=1)
                label_frame = ctk.CTkFrame(box, fg_color="transparent")
                label_frame.grid(row=plate_row_offset, column=0, sticky="ew", padx=(0, 10), pady=7)
                label_frame.grid_columnconfigure(0, weight=1)
                ctk.CTkLabel(label_frame, text="Plate files", font=font_label, text_color=colors["muted"], anchor="w").grid(row=0, column=0, sticky="w")
                make_help(label_frame, "Select all replicate plate-reader exports for this plant's fraction bioassay. The app searches each file for the numeric plate grid and uses the same well positions across replicates.").grid(row=0, column=1, sticky="e", padx=(6, 0))
                labeled_widget(box, "Plate rows", make_entry(box, card["plate_rows"], "8"), plate_row_offset + 1, 0, help_text="Number of lettered well rows in the plate layout. Use 8 for a 96-well plate, 4 for a 24-well plate, or 16 for a 384-well plate.")
                labeled_widget(box, "Plate columns", make_entry(box, card["plate_columns"], "12"), plate_row_offset + 1, 2, help_text="Number of numbered well columns in the plate layout. Use 12 for a 96-well plate, 6 for a 24-well plate, or 24 for a 384-well plate.")
                labeled_widget(box, "Positive-control wells", make_entry(box, card["plate_controls"], "H11, H12"), plate_row_offset + 2, 0, help_text="Well positions that contain positive controls on each replicate plate. These wells are excluded from the mapped fractions and used as the control reference. Use the per-file controls field below only for replicate plates that differ.")
                scale_combo = make_combo(box, card["plate_scale_mode"], list(plate_scale_labels))
                scale_combo.configure(command=lambda _choice: save_state())
                labeled_widget(box, "Plate scaling", scale_combo, plate_row_offset + 2, 2, help_text="Relative range within each plate makes the weakest and strongest non-control wells span 0 to 100 on every plate, which helps compare replicate plates with different overall intensity. Percent of positive control preserves each measurement relative to the selected control wells. Raw fluorescence performs no plate-level scaling.")
                labeled_widget(box, "Per-file controls", make_entry(box, card["plate_controls_by_file"], "optional: 1:H11,H12; B_blue.xlsx:H10,H11"), plate_row_offset + 3, 0, help_text="Optional override when one replicate plate uses different positive-control wells. Use semicolon-separated entries keyed by replicate number or file name, for example 1:H11,H12; plate_2.xlsx:H10,H11.", colspan=3)
            else:
                fluoro_picker = ctk.CTkFrame(box, fg_color="transparent")
                fluoro_picker.grid(row=3, column=1, columnspan=3, sticky="ew", padx=(0, 18), pady=7)
                fluoro_picker.grid_columnconfigure(0, weight=1)
                make_entry(fluoro_picker, card["fluorescence_file"], "fractions_84_UV_clean.xlsx").grid(row=0, column=0, sticky="ew", padx=(0, 8))
                make_button(fluoro_picker, "Browse", lambda v=card["fluorescence_file"]: browse_file(v, "Choose fluorescence table"), width=82).grid(row=0, column=1)
                label_frame = ctk.CTkFrame(box, fg_color="transparent")
                label_frame.grid(row=3, column=0, sticky="ew", padx=(0, 10), pady=7)
                label_frame.grid_columnconfigure(0, weight=1)
                ctk.CTkLabel(label_frame, text="Fluorescence table", font=font_label, text_color=colors["muted"], anchor="w").grid(row=0, column=0, sticky="w")
                make_help(label_frame, "CSV or Excel table with one row per collected fraction. It must contain a fraction number column, a measured average fluorescence column, and a positive-control value or column used for normalization.").grid(row=0, column=1, sticky="e", padx=(6, 0))
                labeled_widget(box, "Fraction column", make_entry(box, card["fraction_column"], "fraction"), 4, 0, help_text="Column in the fluorescence table that identifies collected fraction numbers. These numbers are matched to predicted feature fractions.")
                labeled_widget(box, "Average column", make_entry(box, card["average_column"], "average"), 4, 2, help_text="Column containing the measured fluorescence average for each fraction. The core converts this to percent signal and then to bioactivity.")
                labeled_widget(box, "Positive control", make_entry(box, card["positive_column"], "pos_avg"), 5, 0, help_text="Column or value used as the positive control. The script normalizes fraction averages against the first numeric control value it finds in this column.")

    def collect_current_state() -> dict[str, Any]:
        return {
            "output_dir": var_output_dir.get().strip(),
            "feature_table": var_feature_table.get().strip(),
            "append_table": var_append_table.get().strip(),
            "annotation_feature_match_column": var_annotation_feature_match_col.get().strip(),
            "annotation_table_match_column": var_annotation_table_match_col.get().strip(),
            "annotation_columns_to_add": var_annotation_columns_to_add.get().strip(),
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
            "grouping_value": grouping_value_code(var_grouping_value.get().strip()),
            "calibration_mode": calibration_mode_code(var_calibration_mode.get().strip()),
            "pairs_file": var_pairs_file.get().strip(),
            "pairs_uplc_col": var_pairs_uplc_col.get().strip(),
            "pairs_hplc_col": var_pairs_hplc_col.get().strip(),
            "pairs_manual": var_pairs_manual.get().strip(),
            "equation_slope": var_equation_slope.get().strip(),
            "equation_intercept": var_equation_intercept.get().strip(),
            "runtime_uplc": var_runtime_uplc.get().strip(),
            "runtime_hplc": var_runtime_hplc.get().strip(),
            "hplc_filtered_table": var_hplc_filtered_table.get().strip(),
            "hplc_id_column": var_hplc_id_col.get().strip(),
            "hplc_fraction_column": var_hplc_fraction_col.get().strip(),
            "feature_order_landmarks": landmark_rows,
            "sample_columns": [v.get().strip() for v in sample_column_vars if v.get().strip()],
            "plants": [
                {
                    "name": card["name"].get().strip(),
                    "sample_column": card["sample_column"].get().strip(),
                    "fluorescence_input_type": "plate_reader" if card["input_type"].get() == "Plate reader" else "table",
                    "fluorescence_file": card["fluorescence_file"].get().strip(),
                    "fluorescence_fraction_column": card["fraction_column"].get().strip(),
                    "fluorescence_average_column": card["average_column"].get().strip(),
                    "fluorescence_positive_control_column": card["positive_column"].get().strip(),
                    "plate_files": split_paths(card["plate_files"].get()),
                    "plate_rows": card["plate_rows"].get().strip(),
                    "plate_columns": card["plate_columns"].get().strip(),
                    "plate_positive_control_wells": card["plate_controls"].get().strip(),
                    "plate_positive_control_wells_by_file": card["plate_controls_by_file"].get().strip(),
                    "plate_scale_mode": plate_scale_code(card["plate_scale_mode"].get().strip()),
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
            plant_payload: dict[str, Any] = {
                "name": name,
                "sample_column": sample,
            }
            if card["input_type"].get() == "Plate reader":
                plate_files = split_paths(card["plate_files"].get())
                if not plate_files:
                    raise ValueError(f"Plant {idx}: choose at least one plate-reader replicate file.")
                missing = [path for path in plate_files if not Path(path).expanduser().exists()]
                if missing:
                    raise ValueError(f"Plant {idx}: plate-reader file not found: {missing[0]}")
                plant_payload.update({
                    "fluorescence_input_type": "plate_reader",
                    "plate_files": [str(Path(path).expanduser().resolve()) for path in plate_files],
                    "plate_rows": parse_int("plate rows", card["plate_rows"].get(), required=True) or 8,
                    "plate_columns": parse_int("plate columns", card["plate_columns"].get(), required=True) or 12,
                    "plate_positive_control_wells": card["plate_controls"].get().strip(),
                    "plate_positive_control_wells_by_file": card["plate_controls_by_file"].get().strip(),
                    "plate_scale_mode": plate_scale_code(card["plate_scale_mode"].get().strip()) or "positive_control_then_minmax_0_100",
                })
            else:
                if not fluoro or not Path(fluoro).expanduser().exists():
                    raise ValueError(f"Plant {idx}: choose an existing fluorescence table.")
                plant_payload.update({
                    "fluorescence_input_type": "table",
                    "fluorescence_file": str(Path(fluoro).expanduser().resolve()),
                    "fluorescence_fraction_column": card["fraction_column"].get().strip() or "fraction",
                    "fluorescence_average_column": card["average_column"].get().strip() or "average",
                    "fluorescence_positive_control_column": card["positive_column"].get().strip() or "pos_avg",
                })
            plants.append(plant_payload)

        mode = calibration_mode_code(var_calibration_mode.get().strip())
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
        elif mode == "runtime_scale":
            calibration = {
                "method": "runtime_scale",
                "uplc_total_runtime": parse_float("UPLC total runtime", var_runtime_uplc.get(), required=True),
                "hplc_total_runtime": parse_float("HPLC total runtime", var_runtime_hplc.get(), required=True),
            }
        elif mode == "feature_order_alignment":
            if len(landmark_rows) < 2:
                raise ValueError("Feature-order alignment requires at least two landmarks.")
            calibration = {
                "method": "feature_order_alignment",
                "landmarks": landmark_rows,
                "hplc_filtered_table": var_hplc_filtered_table.get().strip(),
                "hplc_id_column": var_hplc_id_col.get().strip(),
                "hplc_fraction_column": var_hplc_fraction_col.get().strip() or "fraction_index",
                "near_anchor_rt_tolerance": 0.02,
            }
        else:
            raise ValueError(f"Unknown calibration mode: {var_calibration_mode.get().strip()}")

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
            "bioactivity": {
                "cutoffs": parse_cutoffs(var_cutoffs.get()),
                "grouping_value": grouping_value_code(var_grouping_value.get().strip()) or "bioactivity",
            },
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
            append_config = {
                "path": str(append_file.resolve()),
                "id_column": var_id_col.get().strip() or None,
                "feature_match_column": var_annotation_feature_match_col.get().strip() or var_id_col.get().strip() or None,
                "annotation_match_column": var_annotation_table_match_col.get().strip() or var_id_col.get().strip() or None,
            }
            selected_annotation_cols = split_column_names(var_annotation_columns_to_add.get())
            if selected_annotation_cols:
                append_config["columns_to_add"] = selected_annotation_cols
            config["append_to_feature_table"] = append_config
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
        bio_cfg = config.get("bioactivity", {}) or {}
        var_cutoffs.set(", ".join(str(x) for x in bio_cfg.get("cutoffs", [16.5, 22.5])))
        var_grouping_value.set(grouping_value_label(str(bio_cfg.get("grouping_value", bio_cfg.get("grouping_metric", "bioactivity")))))
        cal = config.get("calibration", {}) or {}
        mode = calibration_mode_label(str(cal.get("method", "pairs")))
        var_calibration_mode.set(mode if mode in calibration_mode_labels else "Matched RT pairs")
        var_pairs_file.set(str(cal.get("pairs_file", "")))
        var_pairs_uplc_col.set(str(cal.get("uplc_rt_column", "UPLC RT")))
        var_pairs_hplc_col.set(str(cal.get("hplc_rt_column", "HPLC RT")))
        var_equation_slope.set(str(cal.get("slope", 1.0)))
        var_equation_intercept.set(str(cal.get("intercept", 0.0)))
        var_runtime_uplc.set(str(cal.get("uplc_total_runtime", 19.3)))
        var_runtime_hplc.set(str(cal.get("hplc_total_runtime", 38.0)))
        var_hplc_filtered_table.set(str(cal.get("hplc_filtered_table", "")))
        var_hplc_id_col.set(str(cal.get("hplc_id_column", "row ID")))
        var_hplc_fraction_col.set(str(cal.get("hplc_fraction_column", "fraction_index")))
        landmark_rows.clear()
        if cal.get("landmarks", []) or []:
            landmark_rows.extend(list(cal.get("landmarks", []) or []))
        elif cal.get("landmarks_file"):
            try:
                df = core.load_feature_order_landmarks({"calibration": {"landmarks_file": cal.get("landmarks_file")}}, base_dir=config.get("base_dir", _THIS_DIR))
                landmark_rows.extend(df.to_dict(orient="records"))
            except Exception:
                pass
        append_cfg = config.get("append_to_feature_table", {}) or {}
        var_append_table.set(str(append_cfg.get("path", "")))
        var_annotation_feature_match_col.set(str(append_cfg.get("feature_match_column", append_cfg.get("feature_id_column", feature_cfg.get("id_column", "row ID") or "row ID"))))
        var_annotation_table_match_col.set(str(append_cfg.get("annotation_match_column", append_cfg.get("id_column", feature_cfg.get("id_column", "row ID") or "row ID"))))
        var_annotation_columns_to_add.set(", ".join(str(col) for col in append_cfg.get("columns_to_add", append_cfg.get("annotation_columns", [])) or []))
        append_path = str(append_cfg.get("path", ""))
        feature_path = str(feature_cfg.get("path", ""))
        same_table_append = bool(append_path) and Path(append_path).expanduser() == Path(feature_path).expanduser()
        var_use_separate_annotation.set(bool(append_path) and not same_table_append)
        var_feature_table_type.set("annotated" if same_table_append else "unannotated")
        for plant in config.get("plants", []) or []:
            add_plant(plant)
        refresh_sample_list()
        refresh_calibration_mode()
        try:
            render_landmark_rows()
        except NameError:
            pass
        refresh_annotation_section()
        refresh_annotation_column_list()
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
            if var_hplc_filtered_table.get().strip():
                try:
                    load_hplc_filtered_columns()
                except Exception:
                    pass
            show_toast("Configuration loaded.", kind="success")
        except Exception as exc:
            messagebox.showerror("Load config", str(exc))

    def format_cutoffs(values: list[float] | tuple[float, ...]) -> str:
        return ", ".join(f"{float(value):.3g}" for value in values)

    def clean_group_label(value: str) -> str:
        text = str(value)
        if text.startswith("group_below_"):
            return "below " + text.removeprefix("group_below_")
        if text.startswith("group_above_"):
            return "above " + text.removeprefix("group_above_")
        if text.startswith("group_") and "_to_" in text:
            return text.removeprefix("group_").replace("_to_", " to ")
        return text

    def short_group_label(value: str) -> str:
        text = clean_group_label(value)
        if text.startswith("below "):
            return "< " + text.removeprefix("below ")
        if text.startswith("above "):
            return "> " + text.removeprefix("above ")
        return text.replace(" to ", "-")

    def render_preview_text(preview: dict[str, Any]) -> None:
        preview_text.configure(state="normal")
        preview_text.delete("1.0", "end")
        summary = preview.get("summary", {}) or {}
        suggested = preview.get("recommended_cutoffs", []) or []
        current = preview.get("current_cutoffs", []) or []
        metric_label = preview.get("grouping_metric_label", "selected response value")
        lines = [
            "Response cutoff preview",
            "",
            f"Grouping value: {metric_label}",
            f"Current cutoffs: {format_cutoffs(current) if current else 'none'}",
            f"Suggested cutoffs: {format_cutoffs(suggested) if suggested else 'not available'}",
            "Suggestion meaning: the first cutoff separates roughly the top 30% highest response fractions; the second cutoff separates roughly the top 10% highest response fractions.",
            "",
            f"Pooled fractions: {summary.get('count', 0)}",
        ]
        if summary.get("count"):
            lines.extend([
                f"Response range: {summary.get('min'):.2f} to {summary.get('max'):.2f}",
                f"Median: {summary.get('median'):.2f}; 70th percentile: {summary.get('q70'):.2f}; 90th percentile: {summary.get('q90'):.2f}",
                "",
                "Current group counts:",
            ])
            for label, count in (preview.get("current_group_counts", {}) or {}).items():
                lines.append(f"  {clean_group_label(label)}: {count}")
            lines.append("")
            lines.append("Suggested group counts:")
            for label, count in (preview.get("recommended_group_counts", {}) or {}).items():
                lines.append(f"  {clean_group_label(label)}: {count}")
        for plant in preview.get("plants", []) or []:
            plant_summary = plant.get("summary", {}) or {}
            lines.extend([
                "",
                f"{plant.get('plant', 'Plant')}",
                f"  Fractions with response values: {plant.get('fraction_count', 0)}",
                f"  Suggested for this plant: {format_cutoffs(plant.get('recommended_cutoffs', []) or []) or 'not available'}",
            ])
            if plant_summary.get("count"):
                lines.append(
                    f"  Range: {plant_summary.get('min'):.2f} to {plant_summary.get('max'):.2f}; "
                    f"median {plant_summary.get('median'):.2f}; q70 {plant_summary.get('q70'):.2f}; q90 {plant_summary.get('q90'):.2f}"
                )
        preview_text.insert("end", "\n".join(lines))
        preview_text.configure(state="disabled")

    def render_preview_figure(preview: dict[str, Any]) -> None:
        old_widget = preview_state.get("canvas")
        if old_widget is not None:
            try:
                old_widget.destroy()
            except Exception:
                pass
            preview_state["canvas"] = None
        for child in preview_figure_host.winfo_children():
            child.destroy()

        from PIL import Image, ImageDraw, ImageFont

        values: list[float] = []
        for plant in preview.get("plants", []) or []:
            values.extend(float(value) for value in plant.get("response_values", plant.get("bioactivity_values", [])) or [])

        preview_figure_host.update_idletasks()
        host_width = preview_figure_host.winfo_width()
        host_height = preview_figure_host.winfo_height()
        width = min(1040, max(760, host_width - 24 if host_width > 120 else 920))
        height = min(500, max(360, host_height - 24 if host_height > 120 else 420))
        img = Image.new("RGB", (width, height), "#f6f8fb")
        draw = ImageDraw.Draw(img)

        def font(size: int, bold: bool = False) -> Any:
            names = ["segoeuib.ttf", "arialbd.ttf"] if bold else ["segoeui.ttf", "arial.ttf"]
            for name in names:
                try:
                    return ImageFont.truetype(name, size)
                except Exception:
                    continue
            return ImageFont.load_default()

        title_font = font(22, True)
        axis_font = font(13)
        small_font = font(11)
        text_fill = "#1f2937"
        axis_fill = "#475569"
        grid_fill = "#d7dee8"

        def draw_text_fit(x: int, y: int, text: str, max_width: int, font_obj: Any, fill: str = text_fill) -> int:
            words = str(text).split()
            lines: list[str] = []
            line = ""
            for word in words:
                candidate = f"{line} {word}".strip()
                if draw.textlength(candidate, font=font_obj) <= max_width or not line:
                    line = candidate
                else:
                    lines.append(line)
                    line = word
            if line:
                lines.append(line)
            for line in lines[:2]:
                draw.text((x, y), line, font=font_obj, fill=fill)
                y += 15
            return y

        def value_label(value: float) -> str:
            value = float(value)
            if abs(value) >= 1000:
                return f"{value:.0f}"
            if abs(value) >= 10:
                return f"{value:.1f}"
            return f"{value:.2f}"

        if not values:
            draw.text((width // 2 - 95, height // 2 - 10), "No response values loaded", font=title_font, fill=text_fill)
        else:
            left = (58, 56, width // 2 - 28, height - 72)
            right = (width // 2 + 34, 56, width - 36, height - 48)
            draw.text((left[0], 16), "Response distribution", font=title_font, fill=text_fill)
            draw.text((right[0], 16), "Group sizes", font=title_font, fill=text_fill)

            min_value = min(values)
            max_value = max(values)
            if min_value == max_value:
                min_value -= 0.5
                max_value += 0.5
            bins = min(20, max(7, int(len(values) ** 0.5)))
            step = (max_value - min_value) / bins
            counts = [0] * bins
            for value in values:
                index = min(bins - 1, max(0, int((value - min_value) / step)))
                counts[index] += 1
            max_count = max(counts) or 1
            chart_w = left[2] - left[0]
            chart_h = left[3] - left[1]
            draw.rectangle(left, outline=axis_fill, width=1)
            for tick in range(1, 4):
                y = int(left[3] - chart_h * tick / 4)
                draw.line((left[0], y, left[2], y), fill=grid_fill, width=1)
                label = str(round(max_count * tick / 4))
                draw.text((left[0] - 34, y - 8), label, font=small_font, fill=axis_fill)
            for index, count in enumerate(counts):
                x0 = left[0] + int(chart_w * index / bins) + 2
                x1 = left[0] + int(chart_w * (index + 1) / bins) - 2
                y0 = left[3] - int(chart_h * count / max_count)
                draw.rectangle((x0, y0, x1, left[3]), fill="#9ecae1", outline="#ffffff")
            for cutoff, fill, dashed in [
                *[(float(value), "#d95f5f", True) for value in preview.get("current_cutoffs", []) or []],
                *[(float(value), "#2f8f5b", False) for value in preview.get("recommended_cutoffs", []) or []],
            ]:
                if min_value <= cutoff <= max_value:
                    x = int(left[0] + chart_w * (cutoff - min_value) / (max_value - min_value))
                    if dashed:
                        y = left[1]
                        while y < left[3]:
                            draw.line((x, y, x, min(y + 8, left[3])), fill=fill, width=2)
                            y += 14
                    else:
                        draw.line((x, left[1], x, left[3]), fill=fill, width=2)
            draw.text((left[0], left[3] + 8), value_label(min_value), font=small_font, fill=axis_fill)
            draw.text((left[2] - 52, left[3] + 8), value_label(max_value), font=small_font, fill=axis_fill)
            draw_text_fit(left[0], left[3] + 28, preview.get("grouping_metric_label", "response value"), chart_w, small_font, axis_fill)

            current_counts = preview.get("current_group_counts", {}) or {}
            suggested_counts = preview.get("recommended_group_counts", {}) or {}
            labels = list(dict.fromkeys([*current_counts.keys(), *suggested_counts.keys()]))
            max_bar = max([*current_counts.values(), *suggested_counts.values(), 1])
            row_h = max(34, min(52, (right[3] - right[1] - 34) // max(len(labels), 1)))
            label_w = min(210, max(150, (right[2] - right[0]) // 3))
            bar_x0 = right[0] + label_w
            bar_w = right[2] - bar_x0 - 54
            draw.text((bar_x0, right[1] - 24), "current", font=small_font, fill="#c77760")
            draw.rectangle((bar_x0 + 58, right[1] - 20, bar_x0 + 73, right[1] - 8), fill="#f2b6a0")
            draw.text((bar_x0 + 95, right[1] - 24), "suggested", font=small_font, fill="#427f5c")
            draw.rectangle((bar_x0 + 170, right[1] - 20, bar_x0 + 185, right[1] - 8), fill="#a8d5ba")
            for index, label in enumerate(labels):
                y = right[1] + 14 + index * row_h
                clean = short_group_label(label)
                draw_text_fit(right[0], y - 2, clean, label_w - 12, small_font, axis_fill)
                current = int(current_counts.get(label, 0) or 0)
                suggested = int(suggested_counts.get(label, 0) or 0)
                current_w = int(bar_w * current / max_bar)
                suggested_w = int(bar_w * suggested / max_bar)
                draw.rectangle((bar_x0, y, bar_x0 + current_w, y + 10), fill="#f2b6a0")
                draw.rectangle((bar_x0, y + 15, bar_x0 + suggested_w, y + 25), fill="#a8d5ba")
                draw.text((bar_x0 + current_w + 6, y - 2), str(current), font=small_font, fill=text_fill)
                draw.text((bar_x0 + suggested_w + 6, y + 13), str(suggested), font=small_font, fill=text_fill)
            draw.rectangle(right, outline=grid_fill, width=1)

        image = ctk.CTkImage(light_image=img, dark_image=img, size=(width, height))
        label = ctk.CTkLabel(preview_figure_host, text="", image=image, fg_color="#f6f8fb")
        label.pack(fill="both", expand=True, padx=10, pady=10)
        preview_state["image"] = image
        preview_state["canvas"] = label

    def apply_suggested_cutoffs() -> None:
        preview = preview_state.get("data") or {}
        suggested = preview.get("recommended_cutoffs", []) or []
        if not suggested:
            show_toast("No suggested cutoffs are available yet.", kind="warning")
            return
        var_cutoffs.set(format_cutoffs(suggested))
        save_state()
        show_toast("Suggested cutoffs applied.", kind="success")

    def preview_bioactivity_from_gui() -> None:
        if worker_state["running"] or worker_state["previewing"]:
            return
        try:
            config = validate_and_build_config()
        except Exception as exc:
            messagebox.showerror("Cannot preview bioactivity", str(exc))
            return
        save_state()
        worker_state["previewing"] = True
        run_button.configure(state="disabled")
        preview_button.configure(state="disabled")
        progress.start()
        var_status.set("Previewing response cutoffs...")
        append_log("Previewing response cutoffs without writing output files.")

        def worker() -> None:
            try:
                preview_queue.put((True, core.preview_bioactivity_cutoffs(config)))
            except Exception as exc:
                preview_queue.put((False, (exc, traceback.format_exc())))

        threading.Thread(target=worker, daemon=True).start()

    def run_pipeline_from_gui() -> None:
        if worker_state["running"] or worker_state["previewing"]:
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
        preview_button.configure(state="disabled")
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
                preview_button.configure(state="normal")
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
        try:
            while True:
                ok, payload = preview_queue.get_nowait()
                worker_state["previewing"] = False
                run_button.configure(state="normal")
                preview_button.configure(state="normal")
                progress.stop()
                if ok:
                    preview_state["data"] = payload
                    render_preview_text(payload)
                    render_preview_figure(payload)
                    tabs.set("Response preview")
                    var_status.set("Response preview ready.")
                    suggested = payload.get("recommended_cutoffs", []) or []
                    append_log(f"Response preview ready. Suggested cutoffs: {format_cutoffs(suggested) if suggested else 'not available'}")
                    show_toast("Response preview ready.", kind="success")
                else:
                    exc, tb = payload
                    var_status.set("Preview failed.")
                    append_log(f"PREVIEW ERROR: {exc}\n{tb}")
                    show_toast("Response preview failed.", kind="error")
                    messagebox.showerror("Preview failed", str(exc))
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
    ctk.CTkLabel(
        header,
        text="Predict HPLC fractions, combine LC-MS features with fraction response data, and export publication-ready tables.",
        font=font_subtitle,
        text_color=colors["muted"],
        anchor="w",
        justify="left",
        wraplength=720,
    ).grid(row=1, column=0, sticky="ew", pady=(4, 0), padx=(0, 18))
    actions = ctk.CTkFrame(header, fg_color="transparent")
    actions.grid(row=0, column=1, rowspan=2, sticky="e")
    preview_button = make_button(actions, "Preview response", preview_bioactivity_from_gui, width=136)
    preview_button.pack(side="left", padx=(0, 8))
    run_button = make_button(actions, "Run", run_pipeline_from_gui, primary=True, width=92)
    run_button.pack(side="left", padx=(0, 8))
    make_button(actions, "Save config", save_config_dialog, width=112).pack(side="left", padx=(0, 8))
    make_button(actions, "Load config", load_config_dialog, width=112).pack(side="left", padx=(0, 8))
    make_button(actions, "Clear log", clear_log, width=100).pack(side="left", padx=(0, 8))
    make_button(actions, "Close", root.destroy, danger=True, width=82).pack(side="left")

    tabs = ctk.CTkTabview(root, fg_color=colors["surface"], segmented_button_fg_color=colors["card_alt"], segmented_button_selected_color=colors["accent"], segmented_button_selected_hover_color=colors["accent_hover"], segmented_button_unselected_color=colors["card_alt"], segmented_button_unselected_hover_color="#39414c", text_color=colors["text"], corner_radius=14)
    tabs.grid(row=1, column=0, sticky="nsew", padx=24, pady=(0, 12))
    workflow_tab = tabs.add("Workflow")
    alignment_tab = tabs.add("Alignment")
    plants_tab = tabs.add("Plants")
    preview_tab = tabs.add("Response preview")
    info_tab = tabs.add("Information")
    log_tab = tabs.add("Log")
    for tab in [workflow_tab, alignment_tab, plants_tab, preview_tab, info_tab, log_tab]:
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
    label.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(label, text="Feature table", font=font_label, text_color=colors["muted"], anchor="w").grid(row=0, column=0, sticky="w")
    make_help(label, "The table used for prediction. It must contain feature identifiers, m/z, UPLC retention time, and one or more peak-area sample columns. It may already be annotated; annotations will simply stay in the output tables.").grid(row=0, column=1, sticky="e", padx=(6, 0))
    ctk.CTkLabel(card, textvariable=var_feature_summary, font=font_small, text_color=colors["muted"], anchor="w").grid(row=1, column=1, columnspan=5, sticky="ew", padx=(0, 18), pady=(0, 7))
    type_row = ctk.CTkFrame(card, fg_color="transparent")
    type_row.grid(row=2, column=1, columnspan=5, sticky="ew", padx=(0, 18), pady=7)
    type_switch = ctk.CTkSegmentedButton(type_row, values=["unannotated", "annotated"], variable=var_feature_table_type, fg_color=colors["card_alt"], selected_color=colors["accent"], selected_hover_color=colors["accent_hover"], unselected_color=colors["card_alt"], unselected_hover_color="#39414c", text_color=colors["text"], command=lambda _v: save_state())
    type_switch.pack(fill="x")
    label = ctk.CTkFrame(card, fg_color="transparent")
    label.grid(row=2, column=0, sticky="ew", padx=(0, 10), pady=7)
    label.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(label, text="Table type", font=font_label, text_color=colors["muted"], anchor="w").grid(row=0, column=0, sticky="w")
    make_help(label, "Choose annotated if this same table already contains formula/class/network/compound annotations. This does not change the computation; it tells the interface how to describe the output and avoids asking for a second table.").grid(row=0, column=1, sticky="e", padx=(6, 0))
    out_frame = ctk.CTkFrame(card, fg_color="transparent")
    out_frame.grid(row=3, column=1, columnspan=5, sticky="ew", padx=(0, 18), pady=7)
    out_frame.grid_columnconfigure(0, weight=1)
    make_entry(out_frame, var_output_dir, "Outputs").grid(row=0, column=0, sticky="ew", padx=(0, 8))
    make_button(out_frame, "Browse", browse_output_dir, width=82).grid(row=0, column=1)
    label = ctk.CTkFrame(card, fg_color="transparent")
    label.grid(row=3, column=0, sticky="ew", padx=(0, 10), pady=7)
    label.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(label, text="Output folder", font=font_label, text_color=colors["muted"], anchor="w").grid(row=0, column=0, sticky="w")
    make_help(label, "Folder where the script writes filtered features, fraction windows, feature predictions, bioactivity tables, the optional human-readable report, Post_run_analysis charts, and run_summary.json.").grid(row=0, column=1, sticky="e", padx=(6, 0))
    advanced_row = ctk.CTkFrame(card, fg_color="transparent")
    advanced_row.grid(row=4, column=1, columnspan=5, sticky="w", padx=(0, 18), pady=(8, 0))
    ctk.CTkCheckBox(advanced_row, text="Use a separate annotation table", variable=var_use_separate_annotation, command=refresh_annotation_section, font=font_small, text_color=colors["text"], fg_color=colors["accent"], hover_color=colors["accent_hover"], border_color=colors["border"], checkbox_width=20, checkbox_height=20).pack(side="left")
    make_help(advanced_row, "Enable this only if your RT/mz/peak-area table and your annotation table are two different files. The script will match them by feature ID and write a combined annotated output table.").pack(side="left", padx=(8, 0))
    separate_annotation_frame = ctk.CTkFrame(card, fg_color=colors["card_alt"], corner_radius=12)
    separate_annotation_frame.grid_columnconfigure(1, weight=1)
    separate_annotation_frame.grid_columnconfigure(3, weight=1)
    append_picker = ctk.CTkFrame(separate_annotation_frame, fg_color="transparent")
    append_picker.grid(row=0, column=1, columnspan=5, sticky="ew", padx=(0, 14), pady=(12, 7))
    append_picker.grid_columnconfigure(0, weight=1)
    make_entry(append_picker, var_append_table, "Separate annotation table CSV/XLSX").grid(row=0, column=0, sticky="ew", padx=(0, 8))
    make_button(append_picker, "Browse", lambda: browse_file(var_append_table, "Choose separate annotation table", after=load_annotation_columns), width=82).grid(row=0, column=1, padx=(0, 8))
    make_button(append_picker, "Load columns", load_annotation_columns, primary=True, width=112).grid(row=0, column=2)
    label = ctk.CTkFrame(separate_annotation_frame, fg_color="transparent")
    label.grid(row=0, column=0, sticky="ew", padx=14, pady=(12, 7))
    label.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(label, text="Annotation table", font=font_label, text_color=colors["muted"], anchor="w").grid(row=0, column=0, sticky="w")
    make_help(label, "Optional separate table with molecule names, formulas, classes, networks, or other annotations. It does not need the peak-area columns, but it must have at least one column that can be matched to a column in the prediction table.").grid(row=0, column=1, sticky="e", padx=(6, 0))
    ctk.CTkLabel(separate_annotation_frame, textvariable=var_annotation_summary, font=font_small, text_color=colors["muted"], anchor="w").grid(row=1, column=1, columnspan=5, sticky="ew", padx=(0, 14), pady=(0, 7))
    combo_annotation_feature_match_col = make_combo(separate_annotation_frame, var_annotation_feature_match_col, ["row ID"])
    labeled_widget(separate_annotation_frame, "Prediction match column", combo_annotation_feature_match_col, 2, 0, help_text="Column in the main prediction table used to match rows to the annotation table. Usually this is the same stable feature ID used in the Feature columns section.")
    combo_annotation_table_match_col = make_combo(separate_annotation_frame, var_annotation_table_match_col, ["row ID"])
    labeled_widget(separate_annotation_frame, "Annotation match column", combo_annotation_table_match_col, 2, 2, help_text="Column in the annotation table that contains the same IDs as the prediction match column. The names do not have to be identical, but the values must refer to the same features.")
    annotation_column_row = ctk.CTkFrame(separate_annotation_frame, fg_color="transparent")
    annotation_column_row.grid(row=3, column=1, columnspan=5, sticky="ew", padx=(0, 14), pady=7)
    annotation_column_row.grid_columnconfigure(0, weight=1)
    combo_annotation_add_col = make_combo(annotation_column_row, var_annotation_add_col, [""])
    combo_annotation_add_col.grid(row=0, column=0, sticky="ew", padx=(0, 8))
    make_button(annotation_column_row, "Add column", add_annotation_column, primary=True, width=112).grid(row=0, column=1, padx=(0, 8))
    make_button(annotation_column_row, "Use all", use_all_annotation_columns, width=82).grid(row=0, column=2, padx=(0, 8))
    make_button(annotation_column_row, "Clear", clear_annotation_columns, width=76).grid(row=0, column=3)
    label = ctk.CTkFrame(separate_annotation_frame, fg_color="transparent")
    label.grid(row=3, column=0, sticky="ew", padx=14, pady=7)
    label.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(label, text="Annotation columns", font=font_label, text_color=colors["muted"], anchor="w").grid(row=0, column=0, sticky="w")
    make_help(label, "Choose the annotation columns to include in the final human-readable report, for example compound name, formula, SMILES, class, network, or confidence. Leave this empty to keep every column from the annotation table.").grid(row=0, column=1, sticky="e", padx=(6, 0))
    annotation_selected_frame = ctk.CTkFrame(separate_annotation_frame, fg_color="transparent")
    annotation_selected_frame.grid(row=4, column=1, columnspan=5, sticky="ew", padx=(0, 14), pady=(0, 12))

    card = make_card(workflow_scroll, "Feature columns and filtering", "Tell the script which columns contain feature identity, m/z, UPLC retention time, and sample peak areas.")
    combo_id_col = make_combo(card, var_id_col, ["row ID"])
    combo_mz_col = make_combo(card, var_mz_col, ["row m/z"])
    combo_rt_col = make_combo(card, var_rt_col, ["row retention time"])
    labeled_widget(card, "ID column", combo_id_col, 0, 0, help_text="Feature identifier column used for outputs and for matching prediction results back to the same annotated table or a separate annotation table. If missing, the core creates a row-number ID, but a real stable ID is better.")
    labeled_widget(card, "m/z column", combo_mz_col, 0, 2, help_text="Column containing feature m/z values. It is retained in output tables for interpretation and notebook QC.")
    labeled_widget(card, "UPLC / HRMS RT column", combo_rt_col, 1, 0, help_text="Column containing retention time in the feature table. Matched pairs, equation, and runtime scaling convert this value to predicted HPLC RT. Feature-order alignment uses this value only to place features between landmark anchors.")
    labeled_widget(card, "Area threshold", make_entry(card, var_threshold, "10000"), 1, 2, help_text="A feature is kept if at least one selected sample peak-area column is greater than this threshold. Use this to remove weak/noisy features before fraction prediction.")
    labeled_widget(card, "Predicted RT column", make_entry(card, var_predicted_rt_col, "predicted_hplc_rt"), 2, 0, help_text="Name of the output column that stores predicted HPLC retention time for RT-based calibration modes. In feature-order alignment this column is left empty because no exact HPLC RT is predicted.")
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
    mode_switch = ctk.CTkSegmentedButton(card, values=list(calibration_mode_labels), variable=var_calibration_mode, command=refresh_calibration_mode, fg_color=colors["card_alt"], selected_color=colors["accent"], selected_hover_color=colors["accent_hover"], unselected_color=colors["card_alt"], unselected_hover_color="#39414c", text_color=colors["text"])
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
    label.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(label, text="Pairs file", font=font_label, text_color=colors["muted"], anchor="w").grid(row=0, column=0, sticky="w")
    make_help(label, "Optional if manual pairs are entered below. The table should contain matched UPLC and HPLC retention times for the same compounds or trusted manual guesses.").grid(row=0, column=1, sticky="e", padx=(6, 0))
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

    cal_feature_order_frame = ctk.CTkFrame(card, fg_color="transparent")
    cal_feature_order_frame.grid_columnconfigure(0, weight=1)
    cal_feature_order_frame.grid_columnconfigure(1, weight=1)
    cal_feature_order_frame.grid_columnconfigure(3, weight=1)
    hplc_picker = ctk.CTkFrame(cal_feature_order_frame, fg_color="transparent")
    hplc_picker.grid(row=0, column=0, columnspan=6, sticky="ew", pady=(0, 8))
    hplc_picker.grid_columnconfigure(1, weight=1)
    ctk.CTkLabel(hplc_picker, text="HPLC filtered table", font=font_label, text_color=colors["muted"], anchor="w").grid(row=0, column=0, sticky="w", padx=(0, 10))
    make_entry(hplc_picker, var_hplc_filtered_table, "sample_filtered.csv").grid(row=0, column=1, sticky="ew", padx=(0, 8))
    make_button(hplc_picker, "Browse", lambda: browse_file(var_hplc_filtered_table, "Choose script 01 filtered HPLC table", table_patterns, after=load_hplc_filtered_columns), width=82).grid(row=0, column=2, padx=(0, 8))
    make_button(hplc_picker, "Load", load_hplc_filtered_columns, width=74).grid(row=0, column=3, padx=(0, 8))
    make_help(hplc_picker, "Use the filtered CSV from script 01. It should contain the HPLC/LRMS features and a fraction-number column such as fraction_index. The selected HPLC row provides the fraction boundary for a landmark.").grid(row=0, column=4, padx=(0, 0))
    ctk.CTkLabel(cal_feature_order_frame, textvariable=var_hplc_summary, font=font_small, text_color=colors["muted"], anchor="w", wraplength=1000, justify="left").grid(row=1, column=0, columnspan=6, sticky="ew", pady=(0, 8))
    combo_hplc_id_col = make_combo(cal_feature_order_frame, var_hplc_id_col, ["row ID"])
    combo_hplc_fraction_col = make_combo(cal_feature_order_frame, var_hplc_fraction_col, ["fraction_index"])
    combo_hplc_id_col.configure(command=lambda _choice: (refresh_feature_order_lists(), save_state()))
    combo_hplc_fraction_col.configure(command=lambda _choice: (refresh_feature_order_lists(), save_state()))
    combo_id_col.configure(command=lambda _choice: (refresh_feature_column_widgets(), refresh_feature_order_lists(), save_state()))
    combo_rt_col.configure(command=lambda _choice: (refresh_feature_order_lists(), save_state()))
    combo_mz_col.configure(command=lambda _choice: (refresh_feature_order_lists(), save_state()))
    labeled_widget(cal_feature_order_frame, "HPLC ID column", combo_hplc_id_col, 2, 0, help_text="Identifier column in the script 01 filtered HPLC table. It is stored in the landmark table for traceability.")
    labeled_widget(cal_feature_order_frame, "HPLC fraction column", combo_hplc_fraction_col, 2, 2, help_text="Numeric fraction-number column in the script 01 filtered HPLC table. This is the value used to create candidate fraction intervals.")
    combo_hplc_id_col.grid_configure(sticky="ew")
    combo_hplc_fraction_col.grid_configure(sticky="ew")

    alignment_redirect = ctk.CTkFrame(cal_feature_order_frame, fg_color=colors["card_alt"], corner_radius=12)
    alignment_redirect.grid(row=3, column=0, columnspan=6, sticky="ew", pady=(8, 8))
    alignment_redirect.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(
        alignment_redirect,
        text="Pair HRMS and HPLC landmark features in the dedicated Alignment tab. The selected landmarks are summarized below and saved with the configuration.",
        font=font_small,
        text_color=colors["muted"],
        anchor="w",
        justify="left",
        wraplength=860,
    ).grid(row=0, column=0, sticky="ew", padx=14, pady=12)
    make_button(alignment_redirect, "Open Alignment tab", open_alignment_tab, success=True, width=150).grid(row=0, column=1, sticky="e", padx=14, pady=10)

    picker_grid = ctk.CTkFrame(cal_feature_order_frame, fg_color="transparent")
    picker_grid.grid(row=98, column=0, columnspan=6, sticky="nsew", pady=(8, 8))
    picker_grid.grid_columnconfigure(0, weight=1)
    picker_grid.grid_columnconfigure(1, weight=1)
    picker_grid.grid_rowconfigure(1, weight=1)
    ctk.CTkLabel(picker_grid, text="HRMS features", font=font_label, text_color=colors["text"], anchor="w").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=(0, 4))
    ctk.CTkLabel(picker_grid, text="HPLC filtered features", font=font_label, text_color=colors["text"], anchor="w").grid(row=0, column=1, sticky="w", padx=(6, 0), pady=(0, 4))

    def make_feature_list(parent: Any, row: int, col: int, padx: tuple[int, int]) -> tk.Listbox:
        frame = ctk.CTkFrame(parent, fg_color="transparent")
        frame.grid(row=row, column=col, sticky="nsew", padx=padx)
        frame.grid_rowconfigure(0, weight=1)
        frame.grid_columnconfigure(0, weight=1)
        listbox = tk.Listbox(
            frame,
            height=8,
            exportselection=False,
            bg=colors["entry"],
            fg=colors["text"],
            selectbackground=colors["accent"],
            selectforeground="white",
            highlightthickness=1,
            highlightbackground=colors["border"],
            relief="flat",
            activestyle="none",
        )
        scrollbar = tk.Scrollbar(frame, orient="vertical", command=listbox.yview)
        listbox.configure(yscrollcommand=scrollbar.set)
        listbox.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")

        def on_mousewheel(event: Any) -> str:
            if getattr(event, "num", None) == 4:
                listbox.yview_scroll(-3, "units")
            elif getattr(event, "num", None) == 5:
                listbox.yview_scroll(3, "units")
            else:
                delta = getattr(event, "delta", 0)
                if delta:
                    listbox.yview_scroll(int(-delta / 120) * 3, "units")
            return "break"

        listbox.bind("<MouseWheel>", on_mousewheel)
        listbox.bind("<Button-4>", on_mousewheel)
        listbox.bind("<Button-5>", on_mousewheel)
        return listbox

    list_hrms_features = make_feature_list(picker_grid, 1, 0, (0, 6))
    list_hplc_features = make_feature_list(picker_grid, 1, 1, (6, 0))
    picker_grid.grid_remove()
    landmark_actions = ctk.CTkFrame(cal_feature_order_frame, fg_color="transparent")
    landmark_actions.grid(row=99, column=0, columnspan=6, sticky="ew", pady=(4, 8))
    make_button(landmark_actions, "Refresh feature lists", refresh_feature_order_lists, width=150).pack(side="left", padx=(0, 8))
    make_button(landmark_actions, "Add landmark", add_landmark_from_selection, primary=True, width=120).pack(side="left", padx=(0, 8))
    make_button(landmark_actions, "Import landmarks", import_landmarks, width=130).pack(side="left", padx=(0, 8))
    make_button(landmark_actions, "Export landmarks", export_landmarks, width=130).pack(side="left", padx=(0, 8))
    landmark_actions.grid_remove()
    ctk.CTkLabel(cal_feature_order_frame, text="Selected landmarks", font=font_label, text_color=colors["text"], anchor="w").grid(row=4, column=0, columnspan=6, sticky="w", pady=(4, 4))
    landmark_table_frame = ctk.CTkScrollableFrame(cal_feature_order_frame, fg_color=colors["card_alt"], corner_radius=10, height=150)
    landmark_table_frame.grid(row=5, column=0, columnspan=6, sticky="ew")

    alignment_body = ctk.CTkFrame(alignment_tab, fg_color="transparent")
    alignment_body.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
    alignment_body.grid_columnconfigure(0, weight=1)
    alignment_body.grid_rowconfigure(1, weight=1)
    alignment_header = ctk.CTkFrame(alignment_body, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=16)
    alignment_header.grid(row=0, column=0, sticky="ew", pady=(0, 10))
    alignment_header.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(alignment_header, text="Feature-order landmark mapping", font=font_card_title, text_color=colors["text"], anchor="w").grid(row=0, column=0, sticky="ew", padx=16, pady=(14, 4))
    ctk.CTkLabel(
        alignment_header,
        text="Use this workspace when the HRMS feature table is large. Filter the HRMS side, search the HPLC filtered table, then pair matching landmark features that define candidate fraction intervals.",
        font=font_small,
        text_color=colors["muted"],
        anchor="w",
        justify="left",
        wraplength=1120,
    ).grid(row=1, column=0, sticky="ew", padx=16, pady=(0, 14))
    make_button(alignment_header, "Use this mode", open_alignment_tab, primary=True, width=118).grid(row=0, column=1, rowspan=2, sticky="e", padx=16, pady=14)

    alignment_content = ctk.CTkFrame(alignment_body, fg_color="transparent")
    alignment_content.grid(row=1, column=0, sticky="nsew")
    alignment_content.grid_columnconfigure(0, weight=1)
    alignment_content.grid_rowconfigure(1, weight=1)

    alignment_filter_card = ctk.CTkFrame(alignment_content, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=16)
    alignment_filter_card.grid(row=0, column=0, sticky="ew", pady=(0, 10))
    for col in range(8):
        alignment_filter_card.grid_columnconfigure(col, weight=1 if col in {1, 3, 5, 7} else 0)
    ctk.CTkLabel(alignment_filter_card, text="HRMS filters", font=font_card_title, text_color=colors["text"], anchor="w").grid(row=0, column=0, columnspan=7, sticky="w", padx=16, pady=(14, 8))
    ctk.CTkLabel(alignment_filter_card, text="Search HRMS", font=font_label, text_color=colors["muted"], anchor="w").grid(row=1, column=0, sticky="w", padx=(16, 8), pady=5)
    make_entry(alignment_filter_card, var_alignment_hrms_search, "ID, RT, m/z").grid(row=1, column=1, sticky="ew", padx=(0, 12), pady=5)
    ctk.CTkLabel(alignment_filter_card, text="Sample column", font=font_label, text_color=colors["muted"], anchor="w").grid(row=1, column=2, sticky="w", padx=(0, 8), pady=5)
    combo_alignment_sample_col = make_combo(alignment_filter_card, var_alignment_sample_col, ["", *feature_columns])
    combo_alignment_sample_col.grid(row=1, column=3, sticky="ew", padx=(0, 12), pady=5)
    ctk.CTkLabel(alignment_filter_card, text="Min area", font=font_label, text_color=colors["muted"], anchor="w").grid(row=1, column=4, sticky="w", padx=(0, 8), pady=5)
    make_entry(alignment_filter_card, var_alignment_sample_threshold, "10000").grid(row=1, column=5, sticky="ew", padx=(0, 12), pady=5)
    ctk.CTkLabel(alignment_filter_card, text="Rows/page", font=font_label, text_color=colors["muted"], anchor="w").grid(row=1, column=6, sticky="w", padx=(0, 8), pady=5)
    make_entry(alignment_filter_card, var_alignment_rows_per_page, "100").grid(row=1, column=7, sticky="ew", padx=(0, 16), pady=5)
    ctk.CTkLabel(alignment_filter_card, text="RT range", font=font_label, text_color=colors["muted"], anchor="w").grid(row=2, column=0, sticky="w", padx=(16, 8), pady=5)
    rt_filter = ctk.CTkFrame(alignment_filter_card, fg_color="transparent")
    rt_filter.grid(row=2, column=1, sticky="ew", padx=(0, 12), pady=5)
    rt_filter.grid_columnconfigure(0, weight=1)
    rt_filter.grid_columnconfigure(1, weight=1)
    make_entry(rt_filter, var_alignment_rt_min, "from").grid(row=0, column=0, sticky="ew", padx=(0, 4))
    make_entry(rt_filter, var_alignment_rt_max, "to").grid(row=0, column=1, sticky="ew", padx=(4, 0))
    ctk.CTkLabel(alignment_filter_card, text="m/z range", font=font_label, text_color=colors["muted"], anchor="w").grid(row=2, column=2, sticky="w", padx=(0, 8), pady=5)
    mz_filter = ctk.CTkFrame(alignment_filter_card, fg_color="transparent")
    mz_filter.grid(row=2, column=3, sticky="ew", padx=(0, 12), pady=5)
    mz_filter.grid_columnconfigure(0, weight=1)
    mz_filter.grid_columnconfigure(1, weight=1)
    make_entry(mz_filter, var_alignment_mz_min, "from").grid(row=0, column=0, sticky="ew", padx=(0, 4))
    make_entry(mz_filter, var_alignment_mz_max, "to").grid(row=0, column=1, sticky="ew", padx=(4, 0))
    ctk.CTkLabel(alignment_filter_card, text="Search HPLC", font=font_label, text_color=colors["muted"], anchor="w").grid(row=2, column=4, sticky="w", padx=(0, 8), pady=5)
    make_entry(alignment_filter_card, var_alignment_hplc_search, "ID or fraction").grid(row=2, column=5, sticky="ew", padx=(0, 12), pady=5)
    filter_buttons = ctk.CTkFrame(alignment_filter_card, fg_color="transparent")
    filter_buttons.grid(row=2, column=6, columnspan=2, sticky="e", padx=(0, 16), pady=5)
    make_button(filter_buttons, "Apply filters", apply_alignment_filters, primary=True, width=110).pack(side="left", padx=(0, 8))
    make_button(filter_buttons, "Clear", clear_alignment_filters, width=72).pack(side="left")
    make_help(alignment_filter_card, "Filtering does not change the exported data. It only helps you find good landmark features in a large HRMS table. Use the sample threshold to show features present in a chosen sample, then narrow by retention-time or m/z range.").grid(row=0, column=7, sticky="e", padx=(0, 16), pady=(14, 8))

    alignment_picker_card = ctk.CTkFrame(alignment_content, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=16)
    alignment_picker_card.grid(row=1, column=0, sticky="nsew", pady=(0, 10))
    alignment_picker_card.grid_columnconfigure(0, weight=1)
    alignment_picker_card.grid_columnconfigure(1, weight=1)
    alignment_picker_card.grid_rowconfigure(1, weight=1)
    ctk.CTkLabel(alignment_picker_card, text="HRMS features", font=font_card_title, text_color=colors["text"], anchor="w").grid(row=0, column=0, sticky="w", padx=(16, 8), pady=(14, 6))
    ctk.CTkLabel(alignment_picker_card, text="HPLC filtered features", font=font_card_title, text_color=colors["text"], anchor="w").grid(row=0, column=1, sticky="w", padx=(8, 16), pady=(14, 6))
    list_alignment_hrms_features = make_feature_list(alignment_picker_card, 1, 0, (16, 8))
    list_alignment_hplc_features = make_feature_list(alignment_picker_card, 1, 1, (8, 16))
    hrms_page_row = ctk.CTkFrame(alignment_picker_card, fg_color="transparent")
    hrms_page_row.grid(row=2, column=0, sticky="ew", padx=(16, 8), pady=(8, 12))
    hrms_page_row.grid_columnconfigure(1, weight=1)
    make_button(hrms_page_row, "Previous", lambda: change_alignment_page("hrms", -1), width=86).grid(row=0, column=0, padx=(0, 8))
    ctk.CTkLabel(hrms_page_row, textvariable=var_alignment_hrms_page_info, font=font_small, text_color=colors["muted"], anchor="w").grid(row=0, column=1, sticky="ew")
    make_button(hrms_page_row, "Next", lambda: change_alignment_page("hrms", 1), width=72).grid(row=0, column=2, padx=(8, 0))
    hplc_page_row = ctk.CTkFrame(alignment_picker_card, fg_color="transparent")
    hplc_page_row.grid(row=2, column=1, sticky="ew", padx=(8, 16), pady=(8, 12))
    hplc_page_row.grid_columnconfigure(1, weight=1)
    make_button(hplc_page_row, "Previous", lambda: change_alignment_page("hplc", -1), width=86).grid(row=0, column=0, padx=(0, 8))
    ctk.CTkLabel(hplc_page_row, textvariable=var_alignment_hplc_page_info, font=font_small, text_color=colors["muted"], anchor="w").grid(row=0, column=1, sticky="ew")
    make_button(hplc_page_row, "Next", lambda: change_alignment_page("hplc", 1), width=72).grid(row=0, column=2, padx=(8, 0))

    alignment_actions = ctk.CTkFrame(alignment_content, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=16)
    alignment_actions.grid(row=2, column=0, sticky="ew")
    alignment_actions.grid_columnconfigure(0, weight=1)
    action_buttons = ctk.CTkFrame(alignment_actions, fg_color="transparent")
    action_buttons.grid(row=0, column=0, sticky="ew", padx=16, pady=(14, 8))
    make_button(action_buttons, "Add selected landmark", lambda: add_landmark_from_selection(list_alignment_hrms_features, list_alignment_hplc_features, "alignment_hrms", "alignment_hplc"), primary=True, width=150).pack(side="left", padx=(0, 8))
    make_button(action_buttons, "Refresh lists", apply_alignment_filters, width=110).pack(side="left", padx=(0, 8))
    make_button(action_buttons, "Import landmarks", import_landmarks, width=130).pack(side="left", padx=(0, 8))
    make_button(action_buttons, "Export landmarks", export_landmarks, width=130).pack(side="left", padx=(0, 8))
    alignment_landmark_table_frame = ctk.CTkScrollableFrame(alignment_actions, fg_color=colors["card_alt"], corner_radius=10, height=150)
    alignment_landmark_table_frame.grid(row=1, column=0, sticky="ew", padx=16, pady=(0, 14))

    card = make_card(workflow_scroll, "Fractions and response grouping", "Define the collected fraction windows and choose which response value is used to group fractions.")
    labeled_widget(card, "Start time", make_entry(card, var_fraction_start, "2.0"), 0, 0, help_text="HPLC time at the beginning of fraction collection. Fractions before this time will not receive a fraction number.")
    labeled_widget(card, "End time", make_entry(card, var_fraction_end, "38.0"), 0, 2, help_text="HPLC time at the end of fraction collection. Together with start time and number of fractions, this creates fraction windows.")
    labeled_widget(card, "Number of fractions", make_entry(card, var_fraction_n, "96"), 1, 0, help_text="Total number of collected fractions. For a 96-well plate, this is often 96.")
    labeled_widget(card, "First fraction #", make_entry(card, var_first_fraction, "1"), 1, 2, help_text="Label of the first fraction. Use 1 unless your fraction table starts from another number.")
    grouping_combo = make_combo(card, var_grouping_value, list(grouping_value_labels))
    grouping_combo.configure(command=lambda _choice: save_state())
    labeled_widget(card, "Grouping value", grouping_combo, 2, 0, help_text="Choose which measured or derived value is used for fraction bins. Derived activity is 100 minus relative signal. Relative fluorescence/signal uses the signal scaled so the largest signal is 100. Raw or plate-processed signal average uses the average column directly.")
    labeled_widget(card, "Grouping cutoffs", make_entry(card, var_cutoffs, "16.5, 22.5"), 2, 2, help_text="Comma-separated thresholds used to group fractions based on the selected grouping value. Example: 31.5, 38.2 creates low, middle, and high response groups.")
    cutoff_actions = ctk.CTkFrame(card, fg_color="transparent")
    cutoff_actions.grid(row=3, column=1, columnspan=5, sticky="ew", padx=(0, 18), pady=(4, 0))
    make_button(cutoff_actions, "Preview cutoff statistics", preview_bioactivity_from_gui, primary=True, width=170).pack(side="left", padx=(0, 10))
    make_button(cutoff_actions, "Use suggested cutoffs", apply_suggested_cutoffs, success=True, width=150).pack(side="left", padx=(0, 10))
    make_help(cutoff_actions, "Preview reads the selected plant response inputs only. It does not write output tables. The suggested cutoffs use the 70th and 90th percentiles, so the upper groups represent roughly the top 30% and top 10% highest response fractions.").pack(side="left")

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
    plants_container.pack(fill="both", expand=True)

    def fit_plants_scroll_width(_event: Any = None) -> None:
        try:
            canvas = plants_scroll._parent_canvas
            window_id = plants_scroll._create_window_id
            canvas.itemconfigure(window_id, width=canvas.winfo_width())
        except Exception:
            pass

    plants_scroll.bind("<Configure>", fit_plants_scroll_width)

    preview_body = ctk.CTkFrame(preview_tab, fg_color="transparent")
    preview_body.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
    preview_body.grid_columnconfigure(0, weight=1)
    preview_body.grid_rowconfigure(1, weight=1)
    preview_header = ctk.CTkFrame(preview_body, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=16)
    preview_header.grid(row=0, column=0, sticky="ew", pady=(0, 12))
    preview_header.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(preview_header, text="Response cutoff preview", font=font_card_title, text_color=colors["text"], anchor="w").grid(row=0, column=0, sticky="ew", padx=16, pady=(14, 4))
    ctk.CTkLabel(
        preview_header,
        text="Preview reads the configured plant response inputs and shows whether the current bins split the fractions usefully. Suggested cutoffs use the 70th and 90th percentiles of the selected grouping value.",
        font=font_small,
        text_color=colors["muted"],
        anchor="w",
        justify="left",
        wraplength=1050,
    ).grid(row=1, column=0, sticky="ew", padx=16, pady=(0, 14))
    make_button(preview_header, "Refresh preview", preview_bioactivity_from_gui, primary=True, width=132).grid(row=0, column=1, sticky="e", padx=16, pady=(14, 4))
    make_button(preview_header, "Use suggested cutoffs", apply_suggested_cutoffs, success=True, width=152).grid(row=1, column=1, sticky="e", padx=16, pady=(0, 14))
    preview_content = ctk.CTkFrame(preview_body, fg_color="transparent")
    preview_content.grid(row=1, column=0, sticky="nsew")
    preview_content.grid_columnconfigure(0, weight=1)
    preview_content.grid_columnconfigure(1, weight=2)
    preview_content.grid_rowconfigure(0, weight=1)
    preview_text = ctk.CTkTextbox(preview_content, width=380, fg_color=colors["card"], border_color=colors["border"], border_width=1, text_color=colors["text"], font=font_mono, corner_radius=16, wrap="word")
    preview_text.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
    preview_text.insert("end", "Click Preview response to inspect the selected grouping-value distribution and get suggested cutoffs before exporting a full run.")
    preview_text.configure(state="disabled")
    preview_figure_host = ctk.CTkFrame(preview_content, fg_color="#f6f8fb", border_color=colors["border"], border_width=1, corner_radius=16)
    preview_figure_host.grid(row=0, column=1, sticky="nsew", padx=(8, 0))
    ctk.CTkLabel(preview_figure_host, text="Preview figures will appear here.", font=font_subtitle, text_color="#4b5563").pack(expand=True)

    info_scroll = ctk.CTkScrollableFrame(info_tab, fg_color="transparent")
    info_scroll.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
    info_card = ctk.CTkFrame(info_scroll, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=16)
    info_card.pack(fill="x", padx=6, pady=(0, 12))
    ctk.CTkLabel(info_card, text="What this script does", font=font_card_title, text_color=colors["text"], anchor="w").pack(anchor="w", padx=18, pady=(16, 8))
    info_paragraphs = [
        "This script connects LC-MS feature tables to HPLC microfractions. It starts from a feature table, keeps features that pass a peak-area threshold in selected plant or extract columns, converts UPLC retention time into predicted HPLC retention time, and assigns each feature to the collected fraction window where it should elute.",
        "Most users should provide one feature table. If that table is already annotated, choose the annotated table type and the GUI will also use it as the annotation target for the final combined output. Use the separate annotation table option only when the quantitative table used for prediction and the annotation table used for interpretation are two different files.",
        "The calibration step is the most important decision. Matched RT pairs are best when the same compounds can be recognized in both methods. Equation mode is useful when a regression has already been calculated elsewhere. Runtime scaling is only a rough fallback for comparable gradients. Feature-order alignment is for non-comparable gradients: the user selects landmark features in the HRMS table and in the script 01 filtered HPLC table, and the script assigns candidate fraction intervals between those landmarks instead of pretending to know one exact HPLC retention time.",
        "For each plant card, the script reads a fluorescence table with fraction numbers, average fluorescence, and positive-control values. It normalizes the fluorescence, converts it to bioactivity, groups fractions by the cutoffs you choose, and then transfers the strongest mapped fraction activity back onto the features predicted to appear in those fractions.",
        "The main output is a set of CSV files in the output folder: filtered features, generated fraction windows, feature-to-fraction predictions, features with bioactivity columns, one bioactivity-by-fraction table per plant, and a run_summary.json file. If the main table is marked as annotated or a separate annotation table is supplied, the script also writes a compact human-readable feature report with target-plant presence, absence, fraction, and bioactivity columns near the front.",
        "After each run, the Post_run_analysis folder gives a quick overview for human review. It contains compact summary CSV files plus pastel PNG and SVG figures: a feature-prioritization funnel, feature counts per predicted fraction, and plant-specific presence/activity summaries. Use these first to understand whether the run produced biologically useful contrasts before reading every feature row.",
        "The notebook is the analysis and visualization companion for this core. Use the GUI or JSON config to create a run, then open the notebook to inspect calibration quality, feature counts per fraction, plant-specific bioactivity plots, group counts, and top features. The notebook should be used for review and figure generation, while the core script remains the reproducible processing engine.",
    ]
    for paragraph in info_paragraphs:
        ctk.CTkLabel(info_card, text=paragraph, font=font_label, text_color=colors["text"], justify="left", wraplength=1050, anchor="w").pack(anchor="w", fill="x", padx=18, pady=(0, 12))
    ctk.CTkLabel(info_card, text="Notebook file", font=font_card_title, text_color=colors["text"], anchor="w").pack(anchor="w", padx=18, pady=(8, 8))
    ctk.CTkLabel(info_card, text=str(_THIS_DIR / "jn_04_02_fraction_visualization_notebook.ipynb"), font=font_mono, text_color=colors["muted"], justify="left", wraplength=1050, anchor="w").pack(anchor="w", fill="x", padx=18, pady=(0, 18))

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
    render_landmark_rows()
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
    if var_hplc_filtered_table.get().strip():
        try:
            load_hplc_filtered_columns()
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
