#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import queue
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any

import pandas as pd

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

try:
    from p_04_00_fraction_predictor_core import run_pipeline, slugify  # type: ignore
except Exception as e:  # pragma: no cover
    raise SystemExit(
        "ERROR: Could not import 'p_04_00_fraction_predictor_core.py'.\n"
        "Place this GUI script in the same folder as p_04_00_fraction_predictor_core.py, "
        "or ensure that folder is on PYTHONPATH.\n\n"
        f"Details: {e}"
    )


def main() -> int:
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox
    except Exception as e:
        print(
            "ERROR: Tkinter (GUI) is not available in this Python environment.\n"
            "Install a Python build that includes Tk.",
            file=sys.stderr,
        )
        print(f"Details: {e}", file=sys.stderr)
        return 2

    BG = "#2f2f2f"
    PANEL = "#353535"
    PANEL_ALT = "#393939"
    ENTRY_BG = "#3b3b3b"
    FG = "#f2f2f2"
    MUTED = "#c9c9c9"
    BLUE = "#2d7ff9"
    BLUE_DARK = "#2367cc"
    RED = "#b54848"
    GREEN = "#3c9957"
    BORDER = "#4a4a4a"

    APP_STATE_FILE = _THIS_DIR / ".fraction_core_gui_state.json"

    def load_state() -> dict[str, Any]:
        if APP_STATE_FILE.exists():
            try:
                return json.loads(APP_STATE_FILE.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def save_state(data: dict[str, Any]) -> None:
        try:
            APP_STATE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception:
            pass

    app_state = load_state()

    def _button(parent: tk.Widget, text: str, command, primary: bool = False, danger: bool = False, width: int | None = None) -> tk.Button:
        bg = RED if danger else (BLUE if primary else PANEL)
        abg = "#943838" if danger else (BLUE_DARK if primary else "#424242")
        btn = tk.Button(
            parent,
            text=text,
            command=command,
            bg=bg,
            fg=FG,
            activebackground=abg,
            activeforeground=FG,
            relief="flat",
            bd=0,
            padx=12,
            pady=8,
            cursor="hand2",
            highlightthickness=0,
        )
        if width is not None:
            btn.configure(width=width)
        return btn

    def _entry(parent: tk.Widget, textvariable: tk.StringVar | None = None, width: int | None = None) -> tk.Entry:
        ent = tk.Entry(
            parent,
            textvariable=textvariable,
            bg=ENTRY_BG,
            fg=FG,
            insertbackground=FG,
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground=BORDER,
            highlightcolor=BLUE,
        )
        if width is not None:
            ent.configure(width=width)
        return ent

    def _label(parent: tk.Widget, text: str, *, bold: bool = False, muted: bool = False, bg: str | None = None) -> tk.Label:
        return tk.Label(
            parent,
            text=text,
            bg=(PANEL if bg is None else bg),
            fg=(MUTED if muted else FG),
            font=("Segoe UI", 10, "bold" if bold else "normal"),
            anchor="w",
            justify="left",
        )

    def _checkbutton(parent: tk.Widget, text: str, variable) -> tk.Checkbutton:
        return tk.Checkbutton(
            parent,
            text=text,
            variable=variable,
            bg=PANEL,
            fg=FG,
            activebackground=PANEL,
            activeforeground=FG,
            selectcolor=ENTRY_BG,
            highlightthickness=0,
            bd=0,
        )

    root = tk.Tk()
    root.title("Fraction predictor")
    root.configure(bg=BG)
    root.minsize(1100, 760)

    var_status = tk.StringVar(value="Ready.")
    var_output_dir = tk.StringVar(value=app_state.get("output_dir", str(_THIS_DIR / "Outputs")))
    var_feature_table = tk.StringVar(value=app_state.get("feature_table", ""))
    var_append_table = tk.StringVar(value=app_state.get("append_table", ""))
    var_id_col = tk.StringVar(value=app_state.get("id_column", "row ID"))
    var_mz_col = tk.StringVar(value=app_state.get("mz_column", "row m/z"))
    var_rt_col = tk.StringVar(value=app_state.get("rt_column", "row retention time"))
    var_threshold = tk.StringVar(value=str(app_state.get("area_threshold", 10000)))
    var_predicted_rt_col = tk.StringVar(value=app_state.get("predicted_rt_column", "predicted_hplc_rt"))
    var_missing_fill_text = tk.StringVar(value=app_state.get("missing_fill_text", "Not present in target plants"))
    var_status_col = tk.StringVar(value=app_state.get("status_column", "target_plant_status"))

    var_fraction_start = tk.StringVar(value=str(app_state.get("fraction_start", 2.0)))
    var_fraction_end = tk.StringVar(value=str(app_state.get("fraction_end", 38.0)))
    var_fraction_n = tk.StringVar(value=str(app_state.get("fraction_n", 96)))
    var_first_fraction = tk.StringVar(value=str(app_state.get("first_fraction_number", 1)))

    var_cutoffs = tk.StringVar(value=app_state.get("bioactivity_cutoffs", "16.5, 22.5"))

    var_calibration_mode = tk.StringVar(value=app_state.get("calibration_mode", "pairs"))
    var_pairs_file = tk.StringVar(value=app_state.get("pairs_file", ""))
    var_pairs_uplc_col = tk.StringVar(value=app_state.get("pairs_uplc_col", "UPLC RT"))
    var_pairs_hplc_col = tk.StringVar(value=app_state.get("pairs_hplc_col", "HPLC RT"))
    var_pairs_manual = tk.StringVar(value=app_state.get("pairs_manual", ""))
    var_equation_slope = tk.StringVar(value=str(app_state.get("equation_slope", 1.0)))
    var_equation_intercept = tk.StringVar(value=str(app_state.get("equation_intercept", 0.0)))
    var_runtime_uplc = tk.StringVar(value=str(app_state.get("runtime_uplc", 19.3)))
    var_runtime_hplc = tk.StringVar(value=str(app_state.get("runtime_hplc", 38.0)))

    sample_columns_vars: list[tk.StringVar] = []
    plant_cards: list[dict[str, Any]] = []

    log_queue: queue.Queue[str] = queue.Queue()

    class QueueLogHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                msg = self.format(record)
                log_queue.put(msg)
            except Exception:
                pass

    logger = logging.getLogger("fraction_core")
    logger.setLevel(logging.INFO)
    gui_log_handler = QueueLogHandler()
    gui_log_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(gui_log_handler)

    outer = tk.Frame(root, bg=BG)
    outer.pack(fill="both", expand=True)

    header = tk.Frame(outer, bg=BG)
    header.pack(fill="x", padx=16, pady=(14, 8))

    tk.Label(
        header,
        text="Fraction predictor",
        bg=BG,
        fg=FG,
        font=("Segoe UI", 15, "bold"),
        anchor="w",
    ).pack(anchor="w")
    tk.Label(
        header,
        text="Feature filtering, HPLC↔UPLC calibration, fraction mapping, and fluorescence-derived bioactivity in one place.",
        bg=BG,
        fg=MUTED,
        font=("Segoe UI", 9),
        anchor="w",
    ).pack(anchor="w", pady=(2, 0))

    actions = tk.Frame(outer, bg=BG)
    actions.pack(fill="x", padx=16, pady=(0, 8))

    body_wrap = tk.Frame(outer, bg=BG)
    body_wrap.pack(fill="both", expand=True, padx=16)

    canvas = tk.Canvas(body_wrap, bg=BG, highlightthickness=0)
    scrollbar = tk.Scrollbar(body_wrap, orient="vertical", command=canvas.yview)
    scroll_frame = tk.Frame(canvas, bg=BG)

    canvas.configure(yscrollcommand=scrollbar.set)
    scrollbar.pack(side="right", fill="y")
    canvas.pack(side="left", fill="both", expand=True)

    canvas_window = canvas.create_window((0, 0), window=scroll_frame, anchor="nw")

    def _on_frame_configure(_event=None):
        canvas.configure(scrollregion=canvas.bbox("all"))

    def _on_canvas_configure(event):
        canvas.itemconfig(canvas_window, width=event.width)

    scroll_frame.bind("<Configure>", _on_frame_configure)
    canvas.bind("<Configure>", _on_canvas_configure)

    def _on_mousewheel(event):
        try:
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        except Exception:
            pass

    canvas.bind_all("<MouseWheel>", _on_mousewheel)

    def make_panel(parent: tk.Widget, title: str, subtitle: str | None = None) -> tk.Frame:
        panel = tk.Frame(parent, bg=PANEL, highlightthickness=1, highlightbackground=BORDER)
        hdr = tk.Frame(panel, bg=PANEL)
        hdr.pack(fill="x", padx=12, pady=(10, 6))
        tk.Label(hdr, text=title, bg=PANEL, fg=FG, font=("Segoe UI", 11, "bold"), anchor="w").pack(anchor="w")
        if subtitle:
            tk.Label(hdr, text=subtitle, bg=PANEL, fg=MUTED, font=("Segoe UI", 9), anchor="w", justify="left").pack(anchor="w", pady=(2, 0))
        return panel

    def browse_file(var: tk.StringVar, title: str, patterns: list[tuple[str, str]] | None = None) -> None:
        p = filedialog.askopenfilename(title=title, filetypes=patterns or [("All files", "*.*")])
        if p:
            var.set(p)

    def browse_directory(var: tk.StringVar, title: str) -> None:
        p = filedialog.askdirectory(title=title)
        if p:
            var.set(p)

    def parse_float(label: str, value: str, *, required: bool = True) -> float | None:
        text = (value or "").strip()
        if not text:
            if required:
                raise ValueError(f"Missing value: {label}")
            return None
        try:
            return float(text)
        except Exception:
            raise ValueError(f"Invalid number for {label}: '{text}'")

    def parse_int(label: str, value: str, *, required: bool = True) -> int | None:
        text = (value or "").strip()
        if not text:
            if required:
                raise ValueError(f"Missing value: {label}")
            return None
        try:
            return int(text)
        except Exception:
            raise ValueError(f"Invalid integer for {label}: '{text}'")

    def parse_cutoffs(text: str) -> list[float]:
        raw = [x.strip() for x in text.split(",") if x.strip()]
        return [float(x) for x in raw]

    def manual_pairs_to_temp_csv(text: str, output_dir: Path) -> Path:
        rows: list[dict[str, float]] = []
        for i, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if not stripped:
                continue
            if "," in stripped:
                left, right = stripped.split(",", 1)
            elif "\t" in stripped:
                left, right = stripped.split("\t", 1)
            else:
                raise ValueError(
                    "Manual calibration points must use one pair per line in the format 'UPLC,HPLC'."
                )
            try:
                rows.append({
                    "UPLC RT": float(left.strip()),
                    "HPLC RT": float(right.strip()),
                })
            except Exception:
                raise ValueError(f"Invalid calibration pair on line {i}: '{line}'")
        if len(rows) < 2:
            raise ValueError("At least 2 manual calibration pairs are required.")
        tmp_path = output_dir / "_gui_calibration_pairs.csv"
        pd.DataFrame(rows).to_csv(tmp_path, index=False)
        return tmp_path

    # ---------- sample columns panel ----------
    panel_general = make_panel(
        scroll_frame,
        "Common inputs",
        "Choose the main feature table first. Optional append table can be used to merge the new annotations back into a richer table such as feature_table_everything.csv.",
    )
    panel_general.pack(fill="x", pady=(0, 10))
    panel_general.grid_columnconfigure(1, weight=1)

    rows = tk.Frame(panel_general, bg=PANEL)
    rows.pack(fill="x", padx=12, pady=(0, 12))
    rows.grid_columnconfigure(1, weight=1)

    _label(rows, "Feature table", bold=True).grid(row=0, column=0, sticky="w", pady=6)
    ent_feature_table = _entry(rows, var_feature_table)
    ent_feature_table.grid(row=0, column=1, sticky="ew", padx=(12, 6), pady=6, ipady=4)
    _button(rows, "Browse", lambda: browse_file(var_feature_table, "Select feature table", [("CSV/Excel", "*.csv *.xlsx *.xls"), ("All files", "*.*")]), width=10).grid(row=0, column=2, padx=(6, 0), pady=6)

    _label(rows, "Append to table (optional)", bold=True).grid(row=1, column=0, sticky="w", pady=6)
    ent_append_table = _entry(rows, var_append_table)
    ent_append_table.grid(row=1, column=1, sticky="ew", padx=(12, 6), pady=6, ipady=4)
    _button(rows, "Browse", lambda: browse_file(var_append_table, "Select optional append table", [("CSV/Excel", "*.csv *.xlsx *.xls"), ("All files", "*.*")]), width=10).grid(row=1, column=2, padx=(6, 0), pady=6)

    _label(rows, "Output directory", bold=True).grid(row=2, column=0, sticky="w", pady=6)
    ent_output = _entry(rows, var_output_dir)
    ent_output.grid(row=2, column=1, sticky="ew", padx=(12, 6), pady=6, ipady=4)
    _button(rows, "Browse", lambda: browse_directory(var_output_dir, "Select output directory"), width=10).grid(row=2, column=2, padx=(6, 0), pady=6)

    cols_grid = tk.Frame(panel_general, bg=PANEL)
    cols_grid.pack(fill="x", padx=12, pady=(0, 12))
    for idx in [1, 3, 5]:
        cols_grid.grid_columnconfigure(idx, weight=1)

    _label(cols_grid, "ID column", muted=True).grid(row=0, column=0, sticky="w", pady=6)
    _entry(cols_grid, var_id_col).grid(row=0, column=1, sticky="ew", padx=(10, 18), pady=6, ipady=4)
    _label(cols_grid, "m/z column", muted=True).grid(row=0, column=2, sticky="w", pady=6)
    _entry(cols_grid, var_mz_col).grid(row=0, column=3, sticky="ew", padx=(10, 18), pady=6, ipady=4)
    _label(cols_grid, "RT column", muted=True).grid(row=0, column=4, sticky="w", pady=6)
    _entry(cols_grid, var_rt_col).grid(row=0, column=5, sticky="ew", padx=(10, 0), pady=6, ipady=4)

    _label(cols_grid, "Area threshold", muted=True).grid(row=1, column=0, sticky="w", pady=6)
    _entry(cols_grid, var_threshold).grid(row=1, column=1, sticky="ew", padx=(10, 18), pady=6, ipady=4)
    _label(cols_grid, "Predicted RT column", muted=True).grid(row=1, column=2, sticky="w", pady=6)
    _entry(cols_grid, var_predicted_rt_col).grid(row=1, column=3, sticky="ew", padx=(10, 18), pady=6, ipady=4)
    _label(cols_grid, "Missing-fill text", muted=True).grid(row=1, column=4, sticky="w", pady=6)
    _entry(cols_grid, var_missing_fill_text).grid(row=1, column=5, sticky="ew", padx=(10, 0), pady=6, ipady=4)

    _label(cols_grid, "Status column", muted=True).grid(row=2, column=0, sticky="w", pady=(6, 0))
    _entry(cols_grid, var_status_col).grid(row=2, column=1, sticky="ew", padx=(10, 18), pady=(6, 0), ipady=4)
    _label(cols_grid, "Leave ID empty to auto-generate row numbers.", muted=True).grid(row=2, column=2, columnspan=4, sticky="w", pady=(8, 0))

    panel_samples = make_panel(
        scroll_frame,
        "Target sample columns",
        "These columns are used for the initial area-threshold filtering. Add one or more LC-MS sample columns from the main feature table.",
    )
    panel_samples.pack(fill="x", pady=(0, 10))

    sample_top = tk.Frame(panel_samples, bg=PANEL)
    sample_top.pack(fill="x", padx=12, pady=(0, 8))
    sample_top.grid_columnconfigure(0, weight=1)

    var_new_sample_column = tk.StringVar()
    _entry(sample_top, var_new_sample_column).grid(row=0, column=0, sticky="ew", pady=8, ipady=4)
    _button(sample_top, "Add column", lambda: add_sample_column(), primary=True, width=12).grid(row=0, column=1, padx=(8, 0), pady=8)

    sample_list_frame = tk.Frame(panel_samples, bg=PANEL)
    sample_list_frame.pack(fill="x", padx=12, pady=(0, 12))

    def refresh_sample_columns() -> None:
        for widget in sample_list_frame.winfo_children():
            widget.destroy()
        if not sample_columns_vars:
            _label(sample_list_frame, "No sample columns added yet.", muted=True).pack(anchor="w")
            return
        for i, var in enumerate(sample_columns_vars):
            row = tk.Frame(sample_list_frame, bg=PANEL_ALT)
            row.pack(fill="x", pady=4)
            row.grid_columnconfigure(0, weight=1)
            _entry(row, var).grid(row=0, column=0, sticky="ew", padx=8, pady=8, ipady=4)
            _button(row, "Remove", lambda idx=i: remove_sample_column(idx), danger=True, width=10).grid(row=0, column=1, padx=(8, 8), pady=8)

    def add_sample_column(value: str | None = None) -> None:
        text = (value if value is not None else var_new_sample_column.get()).strip()
        if not text:
            return
        sample_columns_vars.append(tk.StringVar(value=text))
        var_new_sample_column.set("")
        refresh_sample_columns()

    def remove_sample_column(index: int) -> None:
        if 0 <= index < len(sample_columns_vars):
            sample_columns_vars.pop(index)
            refresh_sample_columns()

    for col in app_state.get("sample_columns", []):
        add_sample_column(str(col))
    refresh_sample_columns()

    panel_cal = make_panel(
        scroll_frame,
        "HPLC ↔ UPLC calibration",
        "Choose one calibration mode. Pairs mode accepts either a file or manual pairs entered directly in the GUI as one 'UPLC,HPLC' pair per line.",
    )
    panel_cal.pack(fill="x", pady=(0, 10))

    cal_modes = tk.Frame(panel_cal, bg=PANEL)
    cal_modes.pack(fill="x", padx=12, pady=(0, 8))
    tk.Radiobutton(cal_modes, text="Fit from RT pairs", variable=var_calibration_mode, value="pairs", bg=PANEL, fg=FG, selectcolor=ENTRY_BG, activebackground=PANEL, activeforeground=FG, command=lambda: refresh_calibration_mode()).pack(side="left", padx=(0, 18), pady=8)
    tk.Radiobutton(cal_modes, text="Use equation", variable=var_calibration_mode, value="equation", bg=PANEL, fg=FG, selectcolor=ENTRY_BG, activebackground=PANEL, activeforeground=FG, command=lambda: refresh_calibration_mode()).pack(side="left", padx=(0, 18), pady=8)
    tk.Radiobutton(cal_modes, text="Scale by runtime", variable=var_calibration_mode, value="runtime_scale", bg=PANEL, fg=FG, selectcolor=ENTRY_BG, activebackground=PANEL, activeforeground=FG, command=lambda: refresh_calibration_mode()).pack(side="left", pady=8)

    cal_pairs_frame = tk.Frame(panel_cal, bg=PANEL)
    cal_pairs_frame.pack(fill="x", padx=12, pady=(0, 8))
    cal_pairs_frame.grid_columnconfigure(1, weight=1)
    _label(cal_pairs_frame, "Pairs file (optional)", bold=True).grid(row=0, column=0, sticky="w", pady=6)
    _entry(cal_pairs_frame, var_pairs_file).grid(row=0, column=1, sticky="ew", padx=(12, 6), pady=6, ipady=4)
    _button(cal_pairs_frame, "Browse", lambda: browse_file(var_pairs_file, "Select RT pairs file", [("CSV/Excel", "*.csv *.xlsx *.xls"), ("All files", "*.*")]), width=10).grid(row=0, column=2, padx=(6, 0), pady=6)

    pairs_cols = tk.Frame(panel_cal, bg=PANEL)
    pairs_cols.pack(fill="x", padx=12, pady=(0, 8))
    for idx in [1, 3]:
        pairs_cols.grid_columnconfigure(idx, weight=1)
    _label(pairs_cols, "UPLC RT column", muted=True).grid(row=0, column=0, sticky="w", pady=6)
    _entry(pairs_cols, var_pairs_uplc_col).grid(row=0, column=1, sticky="ew", padx=(10, 18), pady=6, ipady=4)
    _label(pairs_cols, "HPLC RT column", muted=True).grid(row=0, column=2, sticky="w", pady=6)
    _entry(pairs_cols, var_pairs_hplc_col).grid(row=0, column=3, sticky="ew", padx=(10, 0), pady=6, ipady=4)

    manual_wrap = tk.Frame(panel_cal, bg=PANEL)
    manual_wrap.pack(fill="x", padx=12, pady=(0, 8))
    _label(manual_wrap, "Manual RT pairs (optional)", bold=True).pack(anchor="w", pady=(0, 6))
    tk.Label(manual_wrap, text="One pair per line: UPLC,HPLC", bg=PANEL, fg=MUTED, font=("Consolas", 9), anchor="w").pack(anchor="w")
    txt_pairs_manual = tk.Text(manual_wrap, height=5, bg=ENTRY_BG, fg=FG, insertbackground=FG, relief="flat", bd=0, highlightthickness=1, highlightbackground=BORDER, highlightcolor=BLUE)
    txt_pairs_manual.pack(fill="x", pady=(6, 0))
    if var_pairs_manual.get().strip():
        txt_pairs_manual.insert("1.0", var_pairs_manual.get())

    cal_eq_frame = tk.Frame(panel_cal, bg=PANEL)
    cal_eq_frame.pack(fill="x", padx=12, pady=(0, 8))
    for idx in [1, 3]:
        cal_eq_frame.grid_columnconfigure(idx, weight=1)
    _label(cal_eq_frame, "Slope", muted=True).grid(row=0, column=0, sticky="w", pady=6)
    _entry(cal_eq_frame, var_equation_slope).grid(row=0, column=1, sticky="ew", padx=(10, 18), pady=6, ipady=4)
    _label(cal_eq_frame, "Intercept", muted=True).grid(row=0, column=2, sticky="w", pady=6)
    _entry(cal_eq_frame, var_equation_intercept).grid(row=0, column=3, sticky="ew", padx=(10, 0), pady=6, ipady=4)

    cal_runtime_frame = tk.Frame(panel_cal, bg=PANEL)
    cal_runtime_frame.pack(fill="x", padx=12, pady=(0, 12))
    for idx in [1, 3]:
        cal_runtime_frame.grid_columnconfigure(idx, weight=1)
    _label(cal_runtime_frame, "UPLC total runtime", muted=True).grid(row=0, column=0, sticky="w", pady=6)
    _entry(cal_runtime_frame, var_runtime_uplc).grid(row=0, column=1, sticky="ew", padx=(10, 18), pady=6, ipady=4)
    _label(cal_runtime_frame, "HPLC total runtime", muted=True).grid(row=0, column=2, sticky="w", pady=6)
    _entry(cal_runtime_frame, var_runtime_hplc).grid(row=0, column=3, sticky="ew", padx=(10, 0), pady=6, ipady=4)

    cal_hint = tk.Label(panel_cal, bg=PANEL, fg=MUTED, font=("Segoe UI", 9), justify="left", anchor="w", wraplength=980)
    cal_hint.pack(fill="x", padx=12, pady=(0, 12))

    def refresh_calibration_mode() -> None:
        mode = var_calibration_mode.get()
        if mode == "pairs":
            cal_pairs_frame.pack(fill="x", padx=12, pady=(0, 8))
            pairs_cols.pack(fill="x", padx=12, pady=(0, 8))
            manual_wrap.pack(fill="x", padx=12, pady=(0, 8))
            cal_eq_frame.pack_forget()
            cal_runtime_frame.pack_forget()
            cal_hint.configure(text="Recommended mode. Use at least 5 matched compounds if possible. Manual pairs take priority over a file when both are filled.")
        elif mode == "equation":
            cal_pairs_frame.pack_forget()
            pairs_cols.pack_forget()
            manual_wrap.pack_forget()
            cal_runtime_frame.pack_forget()
            cal_eq_frame.pack(fill="x", padx=12, pady=(0, 8))
            cal_hint.configure(text="Useful when you already calculated the regression elsewhere. Expected form: HPLC = slope × UPLC + intercept.")
        else:
            cal_pairs_frame.pack_forget()
            pairs_cols.pack_forget()
            manual_wrap.pack_forget()
            cal_eq_frame.pack_forget()
            cal_runtime_frame.pack(fill="x", padx=12, pady=(0, 8))
            cal_hint.configure(text="Most approximate option. Use only when the gradient profile is geometrically comparable between UPLC and HPLC methods.")

    refresh_calibration_mode()

    panel_fracs = make_panel(
        scroll_frame,
        "Fraction settings",
        "Fraction windows are generated from start time, end time, and number of fractions. This replaces the old UV_times file dependency.",
    )
    panel_fracs.pack(fill="x", pady=(0, 10))
    frac_grid = tk.Frame(panel_fracs, bg=PANEL)
    frac_grid.pack(fill="x", padx=12, pady=(0, 12))
    for idx in [1, 3, 5, 7]:
        frac_grid.grid_columnconfigure(idx, weight=1)
    _label(frac_grid, "Start time", muted=True).grid(row=0, column=0, sticky="w", pady=6)
    _entry(frac_grid, var_fraction_start).grid(row=0, column=1, sticky="ew", padx=(10, 18), pady=6, ipady=4)
    _label(frac_grid, "End time", muted=True).grid(row=0, column=2, sticky="w", pady=6)
    _entry(frac_grid, var_fraction_end).grid(row=0, column=3, sticky="ew", padx=(10, 18), pady=6, ipady=4)
    _label(frac_grid, "# fractions", muted=True).grid(row=0, column=4, sticky="w", pady=6)
    _entry(frac_grid, var_fraction_n).grid(row=0, column=5, sticky="ew", padx=(10, 18), pady=6, ipady=4)
    _label(frac_grid, "First fraction #", muted=True).grid(row=0, column=6, sticky="w", pady=6)
    _entry(frac_grid, var_first_fraction).grid(row=0, column=7, sticky="ew", padx=(10, 0), pady=6, ipady=4)

    panel_bio = make_panel(
        scroll_frame,
        "Bioactivity settings",
        "Cutoffs define the group boundaries. Example: 16.5, 22.5 creates three groups.",
    )
    panel_bio.pack(fill="x", pady=(0, 10))
    bio_grid = tk.Frame(panel_bio, bg=PANEL)
    bio_grid.pack(fill="x", padx=12, pady=(0, 12))
    bio_grid.grid_columnconfigure(1, weight=1)
    _label(bio_grid, "Cutoffs", bold=True).grid(row=0, column=0, sticky="w", pady=6)
    _entry(bio_grid, var_cutoffs).grid(row=0, column=1, sticky="ew", padx=(12, 0), pady=6, ipady=4)

    panel_plants = make_panel(
        scroll_frame,
        "Plants / fluorescence inputs",
        "Add one card per plant species. Each card links one plant name, one sample column, and one fluorescence table.",
    )
    panel_plants.pack(fill="x", pady=(0, 10))

    plants_toolbar = tk.Frame(panel_plants, bg=PANEL)
    plants_toolbar.pack(fill="x", padx=12, pady=(0, 8))
    plants_container = tk.Frame(panel_plants, bg=PANEL)
    plants_container.pack(fill="x", padx=12, pady=(0, 12))

    def refresh_plants() -> None:
        for widget in plants_container.winfo_children():
            widget.destroy()
        if not plant_cards:
            _label(plants_container, "No plants added yet.", muted=True).pack(anchor="w")
            return
        for idx, card in enumerate(plant_cards):
            box = tk.Frame(plants_container, bg=PANEL_ALT, highlightthickness=1, highlightbackground=BORDER)
            box.pack(fill="x", pady=6)
            title_row = tk.Frame(box, bg=PANEL_ALT)
            title_row.pack(fill="x", padx=10, pady=(8, 4))
            tk.Label(title_row, text=f"Plant {idx + 1}", bg=PANEL_ALT, fg=FG, font=("Segoe UI", 10, "bold")).pack(side="left")
            _button(title_row, "Remove", lambda i=idx: remove_plant(i), danger=True, width=10).pack(side="right")

            grid = tk.Frame(box, bg=PANEL_ALT)
            grid.pack(fill="x", padx=10, pady=(0, 10))
            for col in [1, 3]:
                grid.grid_columnconfigure(col, weight=1)

            _label(grid, "Plant name", bold=True, bg=PANEL_ALT).grid(row=0, column=0, sticky="w", pady=6)
            _entry(grid, card["name"]).grid(row=0, column=1, sticky="ew", padx=(10, 18), pady=6, ipady=4)
            _label(grid, "Sample column", bold=True, bg=PANEL_ALT).grid(row=0, column=2, sticky="w", pady=6)
            _entry(grid, card["sample_column"]).grid(row=0, column=3, sticky="ew", padx=(10, 0), pady=6, ipady=4)

            _label(grid, "Fluorescence file", bold=True, bg=PANEL_ALT).grid(row=1, column=0, sticky="w", pady=6)
            _entry(grid, card["fluorescence_file"]).grid(row=1, column=1, columnspan=2, sticky="ew", padx=(10, 10), pady=6, ipady=4)
            _button(grid, "Browse", lambda var=card["fluorescence_file"]: browse_file(var, "Select fluorescence file", [("CSV/Excel", "*.csv *.xlsx *.xls"), ("All files", "*.*")]), width=10).grid(row=1, column=3, sticky="e", pady=6)

            _label(grid, "Fraction column", muted=True, bg=PANEL_ALT).grid(row=2, column=0, sticky="w", pady=6)
            _entry(grid, card["fraction_column"]).grid(row=2, column=1, sticky="ew", padx=(10, 18), pady=6, ipady=4)
            _label(grid, "Average column", muted=True, bg=PANEL_ALT).grid(row=2, column=2, sticky="w", pady=6)
            _entry(grid, card["average_column"]).grid(row=2, column=3, sticky="ew", padx=(10, 0), pady=6, ipady=4)

            _label(grid, "Positive control column", muted=True, bg=PANEL_ALT).grid(row=3, column=0, sticky="w", pady=6)
            _entry(grid, card["positive_column"]).grid(row=3, column=1, sticky="ew", padx=(10, 18), pady=6, ipady=4)
            _label(grid, "One shared control value is supported.", muted=True, bg=PANEL_ALT).grid(row=3, column=2, columnspan=2, sticky="w", pady=6)

    def add_plant(initial: dict[str, Any] | None = None) -> None:
        info = initial or {}
        plant_cards.append(
            {
                "name": tk.StringVar(value=info.get("name", "")),
                "sample_column": tk.StringVar(value=info.get("sample_column", "")),
                "fluorescence_file": tk.StringVar(value=info.get("fluorescence_file", "")),
                "fraction_column": tk.StringVar(value=info.get("fluorescence_fraction_column", "fraction")),
                "average_column": tk.StringVar(value=info.get("fluorescence_average_column", "average")),
                "positive_column": tk.StringVar(value=info.get("fluorescence_positive_control_column", "pos_avg")),
            }
        )
        refresh_plants()

    def remove_plant(index: int) -> None:
        if 0 <= index < len(plant_cards):
            plant_cards.pop(index)
            refresh_plants()

    _button(plants_toolbar, "Add plant", lambda: add_plant(), primary=True, width=12).pack(side="left", pady=8)

    for plant in app_state.get("plants", []):
        add_plant(plant)
    refresh_plants()

    panel_log = make_panel(
        scroll_frame,
        "Run log",
        "Useful messages from the core pipeline appear here.",
    )
    panel_log.pack(fill="both", expand=True, pady=(0, 10))
    txt_log = tk.Text(panel_log, height=12, bg=ENTRY_BG, fg=FG, insertbackground=FG, relief="flat", bd=0, highlightthickness=1, highlightbackground=BORDER, highlightcolor=BLUE)
    txt_log.pack(fill="both", expand=True, padx=12, pady=(0, 12))

    def append_log(message: str) -> None:
        txt_log.insert("end", message + "\n")
        txt_log.see("end")

    def poll_log_queue() -> None:
        try:
            while True:
                append_log(log_queue.get_nowait())
        except queue.Empty:
            pass
        root.after(120, poll_log_queue)

    def set_status(text: str) -> None:
        var_status.set(text)
        root.update_idletasks()

    def collect_current_state() -> dict[str, Any]:
        return {
            "output_dir": var_output_dir.get().strip(),
            "feature_table": var_feature_table.get().strip(),
            "append_table": var_append_table.get().strip(),
            "id_column": var_id_col.get().strip(),
            "mz_column": var_mz_col.get().strip(),
            "rt_column": var_rt_col.get().strip(),
            "area_threshold": var_threshold.get().strip(),
            "predicted_rt_column": var_predicted_rt_col.get().strip(),
            "missing_fill_text": var_missing_fill_text.get().strip(),
            "status_column": var_status_col.get().strip(),
            "fraction_start": var_fraction_start.get().strip(),
            "fraction_end": var_fraction_end.get().strip(),
            "fraction_n": var_fraction_n.get().strip(),
            "first_fraction_number": var_first_fraction.get().strip(),
            "bioactivity_cutoffs": var_cutoffs.get().strip(),
            "calibration_mode": var_calibration_mode.get().strip(),
            "pairs_file": var_pairs_file.get().strip(),
            "pairs_uplc_col": var_pairs_uplc_col.get().strip(),
            "pairs_hplc_col": var_pairs_hplc_col.get().strip(),
            "pairs_manual": txt_pairs_manual.get("1.0", "end").strip(),
            "equation_slope": var_equation_slope.get().strip(),
            "equation_intercept": var_equation_intercept.get().strip(),
            "runtime_uplc": var_runtime_uplc.get().strip(),
            "runtime_hplc": var_runtime_hplc.get().strip(),
            "sample_columns": [v.get().strip() for v in sample_columns_vars if v.get().strip()],
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

    def validate_and_build_config() -> dict[str, Any]:
        feature_path = Path(var_feature_table.get().strip().strip('"')).expanduser()
        if not feature_path.exists():
            raise ValueError("Please select an existing feature table.")

        output_dir = Path(var_output_dir.get().strip().strip('"') or (_THIS_DIR / "Outputs")).expanduser()
        output_dir.mkdir(parents=True, exist_ok=True)

        sample_columns = [v.get().strip() for v in sample_columns_vars if v.get().strip()]
        if not sample_columns:
            raise ValueError("Add at least one target sample column.")

        plants: list[dict[str, Any]] = []
        for i, card in enumerate(plant_cards, start=1):
            name = card["name"].get().strip()
            sample_col = card["sample_column"].get().strip()
            fluoro_file = Path(card["fluorescence_file"].get().strip().strip('"')).expanduser() if card["fluorescence_file"].get().strip() else None
            if not name:
                raise ValueError(f"Plant {i}: name is required.")
            if not sample_col:
                raise ValueError(f"Plant {i}: sample column is required.")
            if fluoro_file is None or not fluoro_file.exists():
                raise ValueError(f"Plant {i}: select an existing fluorescence file.")
            plants.append(
                {
                    "name": name,
                    "sample_column": sample_col,
                    "fluorescence_file": str(fluoro_file.resolve()),
                    "fluorescence_fraction_column": card["fraction_column"].get().strip() or "fraction",
                    "fluorescence_average_column": card["average_column"].get().strip() or "average",
                    "fluorescence_positive_control_column": card["positive_column"].get().strip() or "pos_avg",
                }
            )
        if not plants:
            raise ValueError("Add at least one plant card.")

        calibration_mode = var_calibration_mode.get().strip()
        calibration: dict[str, Any]
        if calibration_mode == "pairs":
            manual_text = txt_pairs_manual.get("1.0", "end").strip()
            if manual_text:
                pairs_file = manual_pairs_to_temp_csv(manual_text, output_dir)
                calibration = {
                    "method": "pairs",
                    "pairs_file": str(pairs_file.resolve()),
                    "uplc_rt_column": "UPLC RT",
                    "hplc_rt_column": "HPLC RT",
                    "minimum_points": 2,
                    "recommended_points": 5,
                }
            else:
                pairs_file = Path(var_pairs_file.get().strip().strip('"')).expanduser()
                if not pairs_file.exists():
                    raise ValueError("Pairs mode selected but no valid RT pairs file or manual pairs were provided.")
                calibration = {
                    "method": "pairs",
                    "pairs_file": str(pairs_file.resolve()),
                    "uplc_rt_column": var_pairs_uplc_col.get().strip() or "UPLC RT",
                    "hplc_rt_column": var_pairs_hplc_col.get().strip() or "HPLC RT",
                    "minimum_points": 2,
                    "recommended_points": 5,
                }
        elif calibration_mode == "equation":
            calibration = {
                "method": "equation",
                "slope": parse_float("equation slope", var_equation_slope.get(), required=True),
                "intercept": parse_float("equation intercept", var_equation_intercept.get(), required=True),
            }
        else:
            calibration = {
                "method": "runtime_scale",
                "uplc_total_runtime": parse_float("UPLC total runtime", var_runtime_uplc.get(), required=True),
                "hplc_total_runtime": parse_float("HPLC total runtime", var_runtime_hplc.get(), required=True),
            }

        config: dict[str, Any] = {
            "base_dir": str(_THIS_DIR.resolve()),
            "output_dir": str(output_dir.resolve()),
            "predicted_rt_column": var_predicted_rt_col.get().strip() or "predicted_hplc_rt",
            "feature_table": {
                "path": str(feature_path.resolve()),
                "id_column": var_id_col.get().strip() or None,
                "mz_column": var_mz_col.get().strip() or "row m/z",
                "rt_column": var_rt_col.get().strip() or "row retention time",
                "area_threshold": parse_float("area threshold", var_threshold.get(), required=True),
                "sample_columns": sample_columns,
            },
            "calibration": calibration,
            "fractions": {
                "start_time": parse_float("fraction start time", var_fraction_start.get(), required=True),
                "end_time": parse_float("fraction end time", var_fraction_end.get(), required=True),
                "n_fractions": parse_int("number of fractions", var_fraction_n.get(), required=True),
                "first_fraction_number": parse_int("first fraction number", var_first_fraction.get(), required=True),
            },
            "bioactivity": {
                "cutoffs": parse_cutoffs(var_cutoffs.get().strip()) if var_cutoffs.get().strip() else [],
            },
            "plants": plants,
        }

        append_path = var_append_table.get().strip().strip('"')
        if append_path:
            append_file = Path(append_path).expanduser()
            if not append_file.exists():
                raise ValueError("Append table path was provided but file does not exist.")
            config["append_to_feature_table"] = {
                "path": str(append_file.resolve()),
                "id_column": var_id_col.get().strip() or "row ID",
                "missing_fill_text": var_missing_fill_text.get().strip() or "Not present in target plants",
                "status_column": var_status_col.get().strip() or "target_plant_status",
            }
        return config

    def save_config_file() -> None:
        try:
            cfg = validate_and_build_config()
        except Exception as e:
            messagebox.showerror("Config error", str(e))
            return
        p = filedialog.asksaveasfilename(
            title="Save config JSON",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            initialdir=str(Path(var_output_dir.get().strip() or _THIS_DIR).expanduser()),
            initialfile="fraction_predictor_config.json",
        )
        if not p:
            return
        Path(p).write_text(json.dumps(cfg, indent=2), encoding="utf-8")
        save_state(collect_current_state())
        messagebox.showinfo("Saved", f"Config saved to:\n{p}")

    def load_config_file() -> None:
        p = filedialog.askopenfilename(
            title="Load config JSON",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if not p:
            return
        try:
            cfg = json.loads(Path(p).read_text(encoding="utf-8"))
        except Exception as e:
            messagebox.showerror("Load error", f"Could not read config file.\n\nDetails: {e}")
            return

        feature_cfg = cfg.get("feature_table", {})
        var_feature_table.set(str(feature_cfg.get("path", "")))
        var_id_col.set(str(feature_cfg.get("id_column", "") or ""))
        var_mz_col.set(str(feature_cfg.get("mz_column", "row m/z")))
        var_rt_col.set(str(feature_cfg.get("rt_column", "row retention time")))
        var_threshold.set(str(feature_cfg.get("area_threshold", 10000)))
        var_predicted_rt_col.set(str(cfg.get("predicted_rt_column", "predicted_hplc_rt")))
        var_output_dir.set(str(cfg.get("output_dir", str(_THIS_DIR / "Outputs"))))

        while sample_columns_vars:
            sample_columns_vars.pop()
        for col in feature_cfg.get("sample_columns", []):
            add_sample_column(str(col))
        refresh_sample_columns()

        fractions_cfg = cfg.get("fractions", {})
        var_fraction_start.set(str(fractions_cfg.get("start_time", 2.0)))
        var_fraction_end.set(str(fractions_cfg.get("end_time", 38.0)))
        var_fraction_n.set(str(fractions_cfg.get("n_fractions", 96)))
        var_first_fraction.set(str(fractions_cfg.get("first_fraction_number", 1)))

        var_cutoffs.set(", ".join(str(x) for x in cfg.get("bioactivity", {}).get("cutoffs", [16.5, 22.5])))

        cal_cfg = cfg.get("calibration", {})
        mode = str(cal_cfg.get("method", "pairs"))
        if mode not in {"pairs", "equation", "runtime_scale"}:
            mode = "runtime_scale" if "runtime" in mode else mode
        var_calibration_mode.set(mode)
        if mode == "pairs":
            var_pairs_file.set(str(cal_cfg.get("pairs_file", "")))
            var_pairs_uplc_col.set(str(cal_cfg.get("uplc_rt_column", "UPLC RT")))
            var_pairs_hplc_col.set(str(cal_cfg.get("hplc_rt_column", "HPLC RT")))
        elif mode == "equation":
            var_equation_slope.set(str(cal_cfg.get("slope", 1.0)))
            var_equation_intercept.set(str(cal_cfg.get("intercept", 0.0)))
        else:
            var_runtime_uplc.set(str(cal_cfg.get("uplc_total_runtime", 19.3)))
            var_runtime_hplc.set(str(cal_cfg.get("hplc_total_runtime", 38.0)))
        refresh_calibration_mode()

        append_cfg = cfg.get("append_to_feature_table", {})
        var_append_table.set(str(append_cfg.get("path", "")))
        var_missing_fill_text.set(str(append_cfg.get("missing_fill_text", "Not present in target plants")))
        var_status_col.set(str(append_cfg.get("status_column", "target_plant_status")))

        plant_cards.clear()
        for plant in cfg.get("plants", []):
            add_plant(plant)
        refresh_plants()

        save_state(collect_current_state())
        messagebox.showinfo("Loaded", f"Loaded config:\n{p}")

    def clear_log() -> None:
        txt_log.delete("1.0", "end")

    def open_output_dir() -> None:
        p = Path(var_output_dir.get().strip() or (_THIS_DIR / "Outputs")).expanduser()
        p.mkdir(parents=True, exist_ok=True)
        try:
            if sys.platform.startswith("win"):
                import os
                os.startfile(str(p))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                import subprocess
                subprocess.Popen(["open", str(p)])
            else:
                import subprocess
                subprocess.Popen(["xdg-open", str(p)])
        except Exception as e:
            messagebox.showerror("Open error", f"Could not open output directory.\n\nDetails: {e}")

    def run_from_gui() -> None:
        try:
            cfg = validate_and_build_config()
        except Exception as e:
            messagebox.showerror("Input error", str(e))
            return

        save_state(collect_current_state())
        clear_log()
        append_log("Starting run...")
        append_log(f"Output directory: {cfg['output_dir']}")
        set_status("Running pipeline...")
        btn_run.configure(state="disabled")

        def worker():
            try:
                summary = run_pipeline(cfg)
                result = ("ok", summary)
            except Exception as e:
                result = ("err", e)
            root.after(0, lambda: done(result))

        def done(result):
            kind, payload = result
            btn_run.configure(state="normal")
            if kind == "ok":
                summary = payload
                append_log("Run finished successfully.")
                append_log(json.dumps(summary.get("files", {}), indent=2))
                set_status("Done.")
                messagebox.showinfo(
                    "Complete",
                    "Run finished successfully.\n\n"
                    f"Filtered rows: {summary.get('n_rows_filtered')} / {summary.get('n_rows_input')}\n"
                    f"Output directory:\n{summary.get('output_dir')}"
                )
            else:
                append_log(f"ERROR: {payload}")
                set_status("Error.")
                messagebox.showerror("Pipeline error", str(payload))

        threading.Thread(target=worker, daemon=True).start()

    _button(actions, "Run", run_from_gui, primary=True, width=12).pack(side="left", padx=(0, 8))
    btn_run = actions.winfo_children()[-1]
    _button(actions, "Save config", save_config_file, width=12).pack(side="left", padx=(0, 8))
    _button(actions, "Load config", load_config_file, width=12).pack(side="left", padx=(0, 8))
    _button(actions, "Open output", open_output_dir, width=12).pack(side="left", padx=(0, 8))
    _button(actions, "Clear log", clear_log, width=12).pack(side="left", padx=(0, 8))
    _button(actions, "Close", root.destroy, width=12).pack(side="right")

    footer = tk.Frame(outer, bg=BG)
    footer.pack(fill="x", padx=16, pady=(4, 14))
    tk.Label(footer, textvariable=var_status, bg=BG, fg=MUTED, font=("Segoe UI", 9), anchor="w").pack(anchor="w")

    poll_log_queue()
    root.mainloop()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
