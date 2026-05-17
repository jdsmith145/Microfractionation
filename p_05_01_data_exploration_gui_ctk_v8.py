#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os
import subprocess
import queue
import sys
import threading
import traceback
from pathlib import Path
from typing import Any

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

try:
    from p_05_00_data_exploration_core_ctk_v7 import read_table, run_pipeline, template_config  # type: ignore
except Exception:
    try:
        from p_05_00_data_exploration_core import read_table, run_pipeline, template_config  # type: ignore
    except Exception as e:  # pragma: no cover
        raise SystemExit(
            "ERROR: Could not import the data-exploration core script.\n"
            "Place this GUI script in the same folder as p_05_00_data_exploration_core_ctk_v7.py "
            "or p_05_00_data_exploration_core.py.\n\n"
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
            "Install it with: pip install customtkinter",
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
        "danger": "#b54848",
        "tile": "#2b313a",
        "tile_hover": "#39414c",
        "tile_selected": "#1d5fc4",
    }

    FONT_HEADER = ("Segoe UI", 23, "bold")
    FONT_SUBTITLE = ("Segoe UI", 12)
    FONT_CARD_TITLE = ("Segoe UI", 15, "bold")
    FONT_LABEL = ("Segoe UI", 12)
    FONT_SMALL = ("Segoe UI", 11)
    FONT_MONO = ("Consolas", 11)

    TABLE_PATTERNS = [("Tables", "*.csv *.tsv *.txt *.xlsx *.xls"), ("All files", "*.*")]
    APP_STATE_FILE = _THIS_DIR / ".data_exploration_gui_ctk_v7_state.json"

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

    def split_csv(text: str) -> list[str]:
        return [x.strip() for x in str(text).split(",") if x.strip()]

    def join_csv(values: Any) -> str:
        if values is None:
            return ""
        if isinstance(values, str):
            return values
        return ", ".join(str(v) for v in values)

    def parse_float(label: str, value: str) -> float:
        try:
            return float(value.strip())
        except Exception:
            raise ValueError(f"Invalid number for {label}: {value!r}")

    def parse_int(label: str, value: str) -> int:
        try:
            return int(value.strip())
        except Exception:
            raise ValueError(f"Invalid integer for {label}: {value!r}")

    app_state = load_state()

    root = ctk.CTk()
    root.title("Data exploration")
    root.geometry("1260x860")
    root.minsize(1100, 720)
    root.configure(fg_color=COLORS["bg"])

    # ---------------------------
    # Variables
    # ---------------------------
    var_status = tk.StringVar(value="Ready.")
    var_step_prepared = tk.BooleanVar(value=bool(app_state.get("step_prepared", True)))
    var_step_annotation = tk.BooleanVar(value=bool(app_state.get("step_annotation", True)))
    var_step_analysis = tk.BooleanVar(value=bool(app_state.get("step_analysis", True)))
    var_apply_metadata = tk.BooleanVar(value=bool(app_state.get("apply_metadata", False)))

    var_output_dir = tk.StringVar(value=app_state.get("output_dir", str(_THIS_DIR / "Outputs_data_exploration")))
    var_feature_table = tk.StringVar(value=app_state.get("feature_table", ""))
    var_id_col = tk.StringVar(value=app_state.get("id_column", "row ID"))
    var_columns_to_keep = tk.StringVar(value=app_state.get("columns_to_keep", ""))

    var_sample_method = tk.StringVar(value=app_state.get("sample_method", "prefix_suffix"))
    var_sample_prefix = tk.StringVar(value=app_state.get("sample_prefix", ""))
    var_sample_suffix = tk.StringVar(value=app_state.get("sample_suffix", ".mzML Peak area"))
    var_sample_regex = tk.StringVar(value=app_state.get("sample_regex", ""))
    var_sample_columns = tk.StringVar(value=app_state.get("sample_columns", ""))
    var_exclude_columns = tk.StringVar(value=app_state.get("exclude_columns", "row ID"))
    var_numeric_only = tk.BooleanVar(value=bool(app_state.get("numeric_only", True)))

    var_metadata_path = tk.StringVar(value=app_state.get("metadata_path", ""))
    var_metadata_key = tk.StringVar(value=app_state.get("metadata_key", "Filename"))
    var_metadata_value = tk.StringVar(value=app_state.get("metadata_value", "Sample name"))
    var_metadata_strip_suffix = tk.StringVar(value=app_state.get("metadata_strip_suffix", ".mzML Peak area"))
    var_metadata_strip_prefix = tk.StringVar(value=app_state.get("metadata_strip_prefix", ""))
    var_metadata_fallback = tk.StringVar(value=app_state.get("metadata_fallback", "original"))
    var_aggregate_duplicates = tk.BooleanVar(value=bool(app_state.get("aggregate_duplicates", False)))
    var_duplicate_aggregation = tk.StringVar(value=app_state.get("duplicate_aggregation", "sum"))

    var_annotation_path = tk.StringVar(value=app_state.get("annotation_path", ""))
    var_annotation_feature_id = tk.StringVar(value=app_state.get("annotation_feature_id", "row ID"))
    var_annotation_id = tk.StringVar(value=app_state.get("annotation_id", "mappingFeatureId"))
    var_feature_id_parser = tk.StringVar(value=app_state.get("feature_id_parser", "numeric"))
    var_annotation_id_parser = tk.StringVar(value=app_state.get("annotation_id_parser", "numeric"))
    var_columns_to_add = tk.StringVar(value=app_state.get("columns_to_add", "molecularFormula, NPC#pathway, NPC#superclass, NPC#class"))
    var_missing_fill = tk.StringVar(value=app_state.get("missing_fill", ""))

    var_analysis_enabled = tk.BooleanVar(value=bool(app_state.get("analysis_enabled", True)))
    var_annotation_column = tk.StringVar(value=app_state.get("analysis_annotation_column", "NPC#class"))
    var_threshold = tk.StringVar(value=str(app_state.get("threshold", 100000)))
    var_top_n = tk.StringVar(value=str(app_state.get("top_n", 15)))
    var_terms = tk.StringVar(value=app_state.get("terms", "Isoquinoline alkaloids"))
    var_match_mode = tk.StringVar(value=app_state.get("match_mode", "exact"))
    var_case_sensitive = tk.BooleanVar(value=bool(app_state.get("case_sensitive", False)))
    var_selected_samples = tk.StringVar(value=app_state.get("selected_samples", ""))
    var_samples_for_top = tk.StringVar(value=app_state.get("samples_for_top", ""))
    var_make_counts = tk.BooleanVar(value=bool(app_state.get("make_counts", True)))
    var_make_terms_plot = tk.BooleanVar(value=bool(app_state.get("make_terms_plot", True)))
    var_make_top_plot = tk.BooleanVar(value=bool(app_state.get("make_top_plot", True)))
    var_plot_format = tk.StringVar(value=app_state.get("plot_format", "svg"))
    var_font_family = tk.StringVar(value=app_state.get("font_family", "Arial"))
    var_final_feature_table = tk.StringVar(value=app_state.get("final_feature_table", ""))

    var_preview_source = tk.StringVar(value="feature table")
    var_picker_target = tk.StringVar(value="Manual sample columns")
    var_picker_mode = tk.StringVar(value="replace")
    var_column_filter = tk.StringVar(value="")

    # ---------------------------
    # Logging bridge
    # ---------------------------
    log_queue: queue.Queue[str] = queue.Queue()

    class QueueLogHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                log_queue.put(self.format(record))
            except Exception:
                pass

    logger = logging.getLogger("data_exploration_core")
    logger.setLevel(logging.INFO)
    gui_log_handler = QueueLogHandler()
    gui_log_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(gui_log_handler)

    # ---------------------------
    # UI helpers
    # ---------------------------
    class ToolTip:
        """Rounded in-window tooltip for help bubbles.

        The tooltip is placed inside the main app window instead of using a
        separate top-level window. This avoids multi-monitor/DPI coordinate
        problems when the app is moved between screens.
        """

        TOOLTIP_WIDTH = 430
        SHOW_DELAY_MS = 240
        HIDE_DELAY_MS = 120

        def __init__(self, widget: Any, text: str) -> None:
            self.widget = widget
            self.text = text
            self.frame: Any = None
            self._show_after: str | None = None
            self._hide_after: str | None = None
            widget.bind("<Enter>", self.schedule_show, add="+")
            widget.bind("<Leave>", self.schedule_hide, add="+")

        def _cancel_after(self, after_id: str | None) -> None:
            if after_id:
                try:
                    root.after_cancel(after_id)
                except Exception:
                    pass

        def schedule_show(self, _event: Any = None) -> None:
            self._cancel_after(self._hide_after)
            self._hide_after = None
            if self.frame is not None or self._show_after is not None:
                return
            self._show_after = root.after(self.SHOW_DELAY_MS, self.show)

        def schedule_hide(self, _event: Any = None) -> None:
            self._cancel_after(self._show_after)
            self._show_after = None
            self._cancel_after(self._hide_after)
            self._hide_after = root.after(self.HIDE_DELAY_MS, self.hide_if_outside)

        def _pointer_inside(self, widget: Any) -> bool:
            try:
                x = widget.winfo_pointerx()
                y = widget.winfo_pointery()
                left = widget.winfo_rootx()
                top = widget.winfo_rooty()
                right = left + widget.winfo_width()
                bottom = top + widget.winfo_height()
                return left <= x <= right and top <= y <= bottom
            except Exception:
                return False

        def show(self) -> None:
            self._show_after = None
            if self.frame is not None:
                return

            root.update_idletasks()
            self.frame = ctk.CTkFrame(
                root,
                fg_color="#111318",
                border_color=COLORS["border"],
                border_width=1,
                corner_radius=16,
                width=self.TOOLTIP_WIDTH,
            )
            self.frame.pack_propagate(False)

            label = ctk.CTkLabel(
                self.frame,
                text=self.text,
                font=FONT_SMALL,
                text_color=COLORS["text"],
                justify="left",
                wraplength=self.TOOLTIP_WIDTH - 36,
                padx=16,
                pady=12,
                anchor="w",
            )
            label.pack(fill="both", expand=True)

            self.frame.update_idletasks()
            height = max(58, label.winfo_reqheight() + 16)
            self.frame.configure(width=self.TOOLTIP_WIDTH, height=height)

            x = self.widget.winfo_rootx() - root.winfo_rootx() + self.widget.winfo_width() + 10
            y = self.widget.winfo_rooty() - root.winfo_rooty() + self.widget.winfo_height() + 4
            max_x = max(12, root.winfo_width() - self.TOOLTIP_WIDTH - 14)
            max_y = max(12, root.winfo_height() - height - 14)
            x = min(max(12, int(x)), int(max_x))
            y = min(max(12, int(y)), int(max_y))

            self.frame.place(x=x, y=y)
            self.frame.lift()
            self.frame.bind("<Enter>", lambda _event: self._cancel_after(self._hide_after), add="+")
            self.frame.bind("<Leave>", self.schedule_hide, add="+")

        def hide_if_outside(self) -> None:
            self._hide_after = None
            if self._pointer_inside(self.widget) or (self.frame is not None and self._pointer_inside(self.frame)):
                return
            self.hide()

        def hide(self, _event: Any = None) -> None:
            self._cancel_after(self._show_after)
            self._cancel_after(self._hide_after)
            self._show_after = None
            self._hide_after = None
            if self.frame is not None:
                try:
                    self.frame.destroy()
                except Exception:
                    pass
                self.frame = None

    def make_help(parent: Any, text: str) -> Any:
        bubble = ctk.CTkLabel(
            parent,
            text="?",
            width=23,
            height=23,
            corner_radius=12,
            fg_color=COLORS["card_alt"],
            text_color=COLORS["muted"],
            font=("Segoe UI", 11, "bold"),
        )
        ToolTip(bubble, text)
        return bubble

    def labeled_widget(parent: Any, label_text: str, widget: Any, row: int, col: int, *, help_text: str = "", colspan: int = 1) -> None:
        label_frame = ctk.CTkFrame(parent, fg_color="transparent")
        label_frame.grid(row=row, column=col, sticky="ew", padx=(0, 10), pady=8)
        label_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(label_frame, text=label_text, font=FONT_LABEL, text_color=COLORS["muted"], anchor="w").grid(row=0, column=0, sticky="w")
        if help_text:
            make_help(label_frame, help_text).grid(row=0, column=1, sticky="e", padx=(6, 0))
        widget.grid(row=row, column=col + 1, columnspan=colspan, sticky="ew", padx=(0, 18), pady=8)

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
            values=values,
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

    def make_button(parent: Any, text: str, command: Any, *, primary: bool = False, success: bool = False, danger: bool = False, width: int | None = None) -> Any:
        color = COLORS["success"] if success else COLORS["danger"] if danger else COLORS["accent"] if primary else COLORS["card_alt"]
        hover = COLORS["success_hover"] if success else "#9e3b3b" if danger else COLORS["accent_hover"] if primary else COLORS["tile_hover"]
        return ctk.CTkButton(parent, text=text, command=command, width=width or 110, height=38, corner_radius=10, fg_color=color, hover_color=hover)

    def make_checkbox(parent: Any, text: str, variable: tk.BooleanVar) -> Any:
        return ctk.CTkCheckBox(
            parent,
            text=text,
            variable=variable,
            font=FONT_SMALL,
            text_color=COLORS["text"],
            fg_color=COLORS["accent"],
            hover_color=COLORS["accent_hover"],
            border_color=COLORS["border"],
            checkbox_width=20,
            checkbox_height=20,
        )

    def make_checkbox_with_help(parent: Any, text: str, variable: tk.BooleanVar, help_text: str) -> Any:
        box = ctk.CTkFrame(parent, fg_color="transparent")
        box.grid_columnconfigure(0, weight=1)
        make_checkbox(box, text, variable).grid(row=0, column=0, sticky="w")
        make_help(box, help_text).grid(row=0, column=1, sticky="e", padx=(8, 0))
        return box

    def section_note(parent: Any, title: str, text: str, row: int) -> None:
        frame = ctk.CTkFrame(parent, fg_color=COLORS["card_alt"], corner_radius=10)
        frame.grid(row=row, column=0, columnspan=6, sticky="ew", padx=(0, 18), pady=(8, 6))
        frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(frame, text=title, font=("Segoe UI", 12, "bold"), text_color=COLORS["text"], anchor="w").grid(row=0, column=0, sticky="ew", padx=12, pady=(8, 0))
        ctk.CTkLabel(frame, text=text, font=FONT_SMALL, text_color=COLORS["muted"], anchor="w", justify="left", wraplength=980).grid(row=1, column=0, sticky="ew", padx=12, pady=(2, 8))

    def browse_file(var: tk.StringVar, title: str, patterns: list[tuple[str, str]] | None = None) -> None:
        path = filedialog.askopenfilename(title=title, initialdir=str(_THIS_DIR), filetypes=patterns or [("All files", "*.*")])
        if path:
            var.set(path)

    def browse_directory(var: tk.StringVar, title: str) -> None:
        path = filedialog.askdirectory(title=title, initialdir=str(_THIS_DIR))
        if path:
            var.set(path)

    class CollapsibleCard(ctk.CTkFrame):
        def __init__(self, parent: Any, title: str, subtitle: str, *, step_var: tk.BooleanVar | None = None, open_by_default: bool = True) -> None:
            super().__init__(parent, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, corner_radius=16)
            self.open = open_by_default
            self.header = ctk.CTkFrame(self, fg_color="transparent")
            self.header.pack(fill="x", padx=18, pady=(16, 10))
            self.header.grid_columnconfigure(1, weight=1)
            if step_var is not None:
                make_checkbox(self.header, "", step_var).grid(row=0, column=0, rowspan=2, sticky="nw", padx=(0, 12), pady=(1, 0))
            else:
                ctk.CTkLabel(self.header, text="", width=1).grid(row=0, column=0, rowspan=2)
            ctk.CTkLabel(self.header, text=title, font=FONT_CARD_TITLE, text_color=COLORS["text"], anchor="w").grid(row=0, column=1, sticky="ew")
            ctk.CTkLabel(self.header, text=subtitle, font=FONT_SMALL, text_color=COLORS["muted"], anchor="w", justify="left", wraplength=930).grid(row=1, column=1, sticky="ew", pady=(4, 0))
            self.toggle_button = make_button(self.header, "−" if open_by_default else "+", self.toggle, width=38)
            self.toggle_button.grid(row=0, column=2, rowspan=2, sticky="ne", padx=(12, 0))
            self.body = ctk.CTkFrame(self, fg_color="transparent")
            if open_by_default:
                self.body.pack(fill="x", padx=18, pady=(0, 18))

        def toggle(self) -> None:
            self.open = not self.open
            if self.open:
                self.body.pack(fill="x", padx=18, pady=(0, 18))
                self.toggle_button.configure(text="−")
            else:
                self.body.pack_forget()
                self.toggle_button.configure(text="+")

    def card_grid(parent: Any) -> Any:
        grid = ctk.CTkFrame(parent, fg_color="transparent")
        grid.pack(fill="x")
        for col in range(6):
            grid.grid_columnconfigure(col, weight=1 if col % 2 == 1 else 0)
        return grid

    def file_row(parent: Any, row: int, label_text: str, var: tk.StringVar, command: Any, help_text: str) -> None:
        label_frame = ctk.CTkFrame(parent, fg_color="transparent")
        label_frame.grid(row=row, column=0, sticky="ew", padx=(0, 10), pady=8)
        ctk.CTkLabel(label_frame, text=label_text, font=FONT_LABEL, text_color=COLORS["muted"]).pack(side="left")
        make_help(label_frame, help_text).pack(side="left", padx=(6, 0))
        make_entry(parent, var).grid(row=row, column=1, columnspan=4, sticky="ew", padx=(0, 10), pady=8)
        make_button(parent, "Browse", command, width=90).grid(row=row, column=5, sticky="ew", pady=8)

    # ---------------------------
    # Layout
    # ---------------------------
    root.grid_columnconfigure(0, weight=1)
    root.grid_rowconfigure(1, weight=1)

    header = ctk.CTkFrame(root, fg_color="transparent")
    header.grid(row=0, column=0, sticky="ew", padx=24, pady=(20, 12))
    header.grid_columnconfigure(0, weight=1)

    ctk.CTkLabel(header, text="Data exploration", font=FONT_HEADER, text_color=COLORS["text"], anchor="w").grid(row=0, column=0, sticky="w")
    ctk.CTkLabel(
        header,
        text="Prepare LC–MS feature tables, merge annotations, inspect columns, and export count tables or figures.",
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
    active_tab = {"name": "Workflow"}
    unread_log_events = {"count": 0}
    unread_figure_events = {"count": 0}
    var_log_badge = tk.StringVar(value="")
    var_figure_badge = tk.StringVar(value="")

    def style_tab_button(name: str) -> None:
        selected = active_tab["name"] == name
        tab_buttons[name].configure(
            fg_color=COLORS["accent"] if selected else COLORS["card"],
            hover_color=COLORS["accent_hover"] if selected else COLORS["card_alt"],
            text_color=COLORS["text"],
        )

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

    def make_tab_button(parent: Any, name: str, text: str) -> Any:
        holder = ctk.CTkFrame(parent, fg_color="transparent")
        holder.pack(side="left", padx=2, pady=2)
        button = ctk.CTkButton(
            holder,
            text=text,
            width=150 if name != "Preview / column picker" else 210,
            height=30,
            corner_radius=9,
            fg_color=COLORS["card"],
            hover_color=COLORS["card_alt"],
            text_color=COLORS["text"],
            command=lambda n=name: set_active_tab(n),
        )
        button.pack(fill="both")
        tab_buttons[name] = button
        return holder

    make_tab_button(tab_bar, "Workflow", "Workflow")
    make_tab_button(tab_bar, "Preview / column picker", "Preview / column picker")
    figures_tab_holder = make_tab_button(tab_bar, "Figures", "Figures")
    figure_badge = ctk.CTkLabel(
        figures_tab_holder,
        textvariable=var_figure_badge,
        width=22,
        height=22,
        corner_radius=11,
        fg_color=COLORS["danger"],
        text_color=COLORS["text"],
        font=("Segoe UI", 10, "bold"),
    )
    figure_badge.place_forget()
    log_tab_holder = make_tab_button(tab_bar, "Log", "Log")
    log_badge = ctk.CTkLabel(
        log_tab_holder,
        textvariable=var_log_badge,
        width=22,
        height=22,
        corner_radius=11,
        fg_color=COLORS["danger"],
        text_color=COLORS["text"],
        font=("Segoe UI", 10, "bold"),
    )
    log_badge.place_forget()
    workflow_tab.grid(row=0, column=0, sticky="nsew")
    style_tab_button("Workflow")
    style_tab_button("Preview / column picker")
    style_tab_button("Figures")
    style_tab_button("Log")

    workflow_tab.grid_columnconfigure(0, weight=1)
    workflow_tab.grid_rowconfigure(0, weight=1)
    workflow_frame = ctk.CTkScrollableFrame(
        workflow_tab,
        fg_color="transparent",
        scrollbar_button_color=COLORS["card_alt"],
        scrollbar_button_hover_color=COLORS["tile_hover"],
    )
    workflow_frame.grid(row=0, column=0, sticky="nsew", padx=4, pady=10)
    workflow_frame.grid_columnconfigure(0, weight=1)

    # Card 1
    card1 = CollapsibleCard(
        workflow_frame,
        "1) Main feature table",
        "Select the wide feature table and choose where outputs should be written.",
        step_var=var_step_prepared,
        open_by_default=True,
    )
    card1.grid(row=0, column=0, sticky="ew", pady=(0, 14))
    g = card_grid(card1.body)
    file_row(g, 0, "Feature table", var_feature_table, lambda: browse_file(var_feature_table, "Select feature table", TABLE_PATTERNS), "Main input table. Expected layout: one feature per row and abundance/sample columns across the table.")
    file_row(g, 1, "Output directory", var_output_dir, lambda: browse_directory(var_output_dir, "Select output directory"), "Folder where generated CSV, SVG/PNG/PDF, and summary JSON outputs are saved.")
    labeled_widget(g, "Feature ID column", make_entry(g, var_id_col), 2, 0, help_text="Column with unique feature identifiers. Used for merging the feature table with annotation tables.")
    labeled_widget(g, "Other columns to keep", make_entry(g, var_columns_to_keep), 2, 2, help_text="Optional comma-separated columns already present in the feature table that should be preserved, such as existing annotations, metadata, notes, or grouping columns.", colspan=3)

    # Card 2
    card2 = CollapsibleCard(
        workflow_frame,
        "1b) Sample / abundance columns",
        "Part of main table preparation. Define which columns contain sample intensity or peak-area values.",
        open_by_default=True,
    )
    card2.grid(row=1, column=0, sticky="ew", pady=(0, 14))
    g = card_grid(card2.body)
    labeled_widget(g, "Selection method", make_combo(g, var_sample_method, ["manual", "prefix", "suffix", "prefix_suffix", "regex", "all_numeric"]), 0, 0, help_text="How sample columns are identified. Use manual when selecting exact columns from the picker.")
    labeled_widget(g, "Prefix", make_entry(g, var_sample_prefix), 0, 2, help_text="Automatic mode: selected column names must start with this text.")
    labeled_widget(g, "Suffix", make_entry(g, var_sample_suffix), 0, 4, help_text="Automatic mode: selected column names must end with this text.")
    labeled_widget(g, "Regex", make_entry(g, var_sample_regex), 1, 0, help_text="Regular expression used to match column names. Example: Peak area$ means names ending with 'Peak area'.")
    labeled_widget(g, "Manual columns", make_entry(g, var_sample_columns), 1, 2, help_text="Comma-separated sample/abundance columns. The column picker can fill this field.", colspan=3)
    labeled_widget(g, "Exclude columns", make_entry(g, var_exclude_columns), 2, 0, help_text="Comma-separated columns ignored during automatic selection, such as ID or annotation columns.", colspan=3)
    make_checkbox(g, "Keep only numeric abundance columns", var_numeric_only).grid(row=2, column=4, columnspan=2, sticky="w", pady=8)

    # Card 3
    card3 = CollapsibleCard(
        workflow_frame,
        "2) Optional sample metadata",
        "Optional part of table preparation. Check the box to apply metadata-based sample renaming.",
        step_var=var_apply_metadata,
        open_by_default=False,
    )
    card3.grid(row=2, column=0, sticky="ew", pady=(0, 14))
    g = card_grid(card3.body)
    file_row(g, 0, "Metadata table", var_metadata_path, lambda: browse_file(var_metadata_path, "Select metadata table", TABLE_PATTERNS), "Optional table used to map raw sample/file names to readable display names.")
    labeled_widget(g, "Key column", make_entry(g, var_metadata_key), 1, 0, help_text="Column in the metadata table that matches sample names after optional prefix/suffix stripping.")
    labeled_widget(g, "Display column", make_entry(g, var_metadata_value), 1, 2, help_text="Column in the metadata table used as the readable sample name in outputs.")
    labeled_widget(g, "Fallback", make_combo(g, var_metadata_fallback, ["original", "key", "empty"]), 1, 4, help_text="Value used when a sample column cannot be found in the metadata table.")
    labeled_widget(g, "Strip suffix", make_entry(g, var_metadata_strip_suffix), 2, 0, help_text="Text removed from the end of a sample column before matching to the metadata key.")
    labeled_widget(g, "Strip prefix", make_entry(g, var_metadata_strip_prefix), 2, 2, help_text="Text removed from the start of a sample column before matching to the metadata key.")
    labeled_widget(g, "Duplicate aggregation", make_combo(g, var_duplicate_aggregation, ["sum", "mean", "max"]), 2, 4, help_text="How to combine columns that receive the same display name.")
    make_checkbox(g, "Aggregate duplicate display labels", var_aggregate_duplicates).grid(row=3, column=0, columnspan=3, sticky="w", pady=8)

    # Card 4
    card4 = CollapsibleCard(
        workflow_frame,
        "3) Optional annotation merge",
        "Append selected annotation columns from SIRIUS/CANOPUS, ClassyFire, or another table.",
        step_var=var_step_annotation,
        open_by_default=True,
    )
    card4.grid(row=3, column=0, sticky="ew", pady=(0, 14))
    g = card_grid(card4.body)
    file_row(g, 0, "Annotation table", var_annotation_path, lambda: browse_file(var_annotation_path, "Select annotation table", TABLE_PATTERNS), "Optional table containing feature IDs and annotation columns to add.")
    labeled_widget(g, "Feature-table ID col", make_entry(g, var_annotation_feature_id), 1, 0, help_text="ID column in the prepared feature table used for matching.")
    labeled_widget(g, "Annotation ID col", make_entry(g, var_annotation_id), 1, 2, help_text="ID column in the annotation table used for matching.")
    labeled_widget(g, "Missing fill", make_entry(g, var_missing_fill), 1, 4, help_text="Optional text inserted into missing annotation cells after merging. Leave empty to keep missing values.")
    labeled_widget(g, "Feature ID parser", make_combo(g, var_feature_id_parser, ["none", "numeric", "sirius_v5_id"]), 2, 0, help_text="How IDs in the feature table are normalized before matching. Numeric is usually safest for row IDs.")
    labeled_widget(g, "Annotation ID parser", make_combo(g, var_annotation_id_parser, ["none", "numeric", "sirius_v5_id"]), 2, 2, help_text="How IDs in the annotation table are normalized before matching. sirius_v5_id extracts the final integer from an ID string.")
    labeled_widget(g, "Columns to add", make_entry(g, var_columns_to_add), 2, 4, help_text="Comma-separated annotation columns copied into the feature table, e.g. molecularFormula, NPC#class.")

    # Card 5
    card5 = CollapsibleCard(
        workflow_frame,
        "4) Analysis: choose a plot mode",
        "Create figures from the current workflow or directly from an existing final feature table.",
        step_var=var_step_analysis,
        open_by_default=True,
    )
    card5.grid(row=4, column=0, sticky="ew", pady=(0, 14))
    g = card_grid(card5.body)

    make_checkbox_with_help(
        g,
        "Enable analysis/figure outputs",
        var_analysis_enabled,
        "Master switch for analysis. Turn this off when you only want to prepare or merge tables without producing count tables or figures.",
    ).grid(row=0, column=0, columnspan=2, sticky="ew", pady=8)
    make_checkbox_with_help(
        g,
        "Full category × sample count table",
        var_make_counts,
        "Exports a CSV table where each row is one annotation category and each sample column contains the number of features above the threshold.",
    ).grid(row=0, column=2, columnspan=2, sticky="ew", pady=8)
    make_checkbox_with_help(
        g,
        "Plot selected category across samples",
        var_make_terms_plot,
        "Use this for questions such as: Which plants contain the most isoquinoline alkaloid features? X-axis = samples/plants; y-axis = feature count for the selected category.",
    ).grid(row=0, column=4, columnspan=2, sticky="ew", pady=8)
    make_checkbox_with_help(
        g,
        "Plot top categories within sample(s)",
        var_make_top_plot,
        "Use this for questions such as: What are the most common NPC classes in Ruta corsica? X-axis = annotation categories; y-axis = feature count within selected sample(s).",
    ).grid(row=1, column=0, columnspan=3, sticky="ew", pady=8)
    make_checkbox_with_help(
        g,
        "Case-sensitive text matching",
        var_case_sensitive,
        "When checked, uppercase/lowercase must match exactly in category-name searches. Usually leave this off.",
    ).grid(row=1, column=3, columnspan=3, sticky="ew", pady=8)

    labeled_widget(
        g,
        "Final feature table",
        make_entry(g, var_final_feature_table),
        2,
        0,
        help_text="Optional existing final feature table for figure-only runs. By default this points to 02_feature_table_with_annotations.csv in the output directory.",
        colspan=5,
    )
    labeled_widget(
        g,
        "Annotation/category column",
        make_entry(g, var_annotation_column),
        3,
        0,
        help_text="Column containing the labels to count, e.g. NPC#class, NPC#pathway, ClassyFire#class, or molecularFormula.",
    )
    labeled_widget(
        g,
        "Feature-count threshold",
        make_entry(g, var_threshold),
        3,
        2,
        help_text="A feature is counted in a sample only when its abundance value is greater than this threshold.",
    )
    labeled_widget(
        g,
        "Maximum bars / Top N",
        make_entry(g, var_top_n),
        3,
        4,
        help_text="Maximum number of samples or categories shown in the plotted output. The CSV count table can still contain more information.",
    )

    section_note(
        g,
        "Mode A — selected category across samples",
        "Example: count features annotated as ‘Isoquinoline alkaloids’ in each plant/sample. Fill the category name(s) below and enable ‘Plot selected category across samples’.",
        4,
    )
    labeled_widget(
        g,
        "Category name(s) to find",
        make_entry(g, var_terms),
        5,
        0,
        help_text="Comma-separated annotation values to search for in the annotation/category column. Example: Isoquinoline alkaloids.",
        colspan=3,
    )
    labeled_widget(
        g,
        "Text matching",
        make_combo(g, var_match_mode, ["exact", "contains", "regex"]),
        5,
        4,
        help_text="Exact = whole cell must match; contains = substring search; regex = regular expression search.",
    )
    labeled_widget(
        g,
        "Sample subset for Mode A",
        make_entry(g, var_selected_samples),
        6,
        0,
        help_text="Optional comma-separated sample columns used in across-sample outputs. Leave empty to use all detected sample columns.",
        colspan=5,
    )

    section_note(
        g,
        "Mode B — top categories within selected sample(s)",
        "Example: summarize Ruta corsica by NPC#class and plot the most common classes. Fill the sample name(s) below and enable ‘Plot top categories within sample(s)’.",
        7,
    )
    labeled_widget(
        g,
        "Sample(s) to summarize by category",
        make_entry(g, var_samples_for_top),
        8,
        0,
        help_text="Comma-separated sample columns for top-category plots. Each selected sample gets its own plot.",
        colspan=3,
    )
    labeled_widget(
        g,
        "Plot format",
        make_combo(g, var_plot_format, ["svg", "png", "pdf"]),
        8,
        4,
        help_text="SVG is usually best for vector editing and publication figures. PNG previews are created automatically for the Figures tab.",
    )
    labeled_widget(
        g,
        "Font family",
        make_entry(g, var_font_family),
        9,
        0,
        help_text="Font requested for exported matplotlib figures. If the font is unavailable, matplotlib will use a fallback.",
    )

    # ---------------------------
    # Preview tab
    # ---------------------------
    preview_tab.grid_columnconfigure(0, weight=0)
    preview_tab.grid_columnconfigure(1, weight=1)
    preview_tab.grid_rowconfigure(0, weight=1)

    picker_panel = ctk.CTkFrame(preview_tab, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, corner_radius=16)
    picker_panel.grid(row=0, column=0, sticky="nsw", padx=(6, 12), pady=10)
    picker_panel.grid_columnconfigure(0, weight=1)

    ctk.CTkLabel(picker_panel, text="Column picker", font=FONT_CARD_TITLE, text_color=COLORS["text"]).grid(row=0, column=0, sticky="w", padx=18, pady=(18, 4))
    ctk.CTkLabel(
        picker_panel,
        text="Preview a table, select one or many columns, then send them into a multi-column input field.",
        font=FONT_SMALL,
        text_color=COLORS["muted"],
        wraplength=285,
        justify="left",
    ).grid(row=1, column=0, sticky="w", padx=18, pady=(0, 14))
    make_button(picker_panel, "Preview columns...", lambda: open_preview_source_dialog(), primary=True).grid(row=2, column=0, sticky="ew", padx=18, pady=(0, 12))

    ctk.CTkLabel(picker_panel, text="Filter visible columns", font=FONT_SMALL, text_color=COLORS["muted"]).grid(row=3, column=0, sticky="w", padx=18, pady=(8, 2))
    make_entry(picker_panel, var_column_filter).grid(row=4, column=0, sticky="ew", padx=18, pady=(0, 12))

    ctk.CTkLabel(picker_panel, text="Send selected columns to", font=FONT_SMALL, text_color=COLORS["muted"]).grid(row=5, column=0, sticky="w", padx=18, pady=(8, 2))
    picker_targets = [
        "Manual sample columns",
        "Other columns to keep",
        "Annotation columns to add",
        "Annotation/category column",
        "Sample subset for Mode A",
        "Sample(s) for Mode B",
        "Exclude columns",
    ]
    if var_picker_target.get() not in picker_targets:
        var_picker_target.set("Manual sample columns")
    make_combo(picker_panel, var_picker_target, picker_targets).grid(row=6, column=0, sticky="ew", padx=18, pady=(0, 12))

    ctk.CTkLabel(picker_panel, text="Apply mode", font=FONT_SMALL, text_color=COLORS["muted"]).grid(row=7, column=0, sticky="w", padx=18, pady=(8, 2))
    make_combo(picker_panel, var_picker_mode, ["replace", "append"]).grid(row=8, column=0, sticky="ew", padx=18, pady=(0, 14))
    make_button(picker_panel, "Apply selected columns", lambda: apply_selected_columns(), success=True).grid(row=9, column=0, sticky="ew", padx=18, pady=(4, 10))
    make_button(picker_panel, "Clear selection", lambda: clear_selection()).grid(row=10, column=0, sticky="ew", padx=18, pady=(0, 18))

    preview_right = ctk.CTkFrame(preview_tab, fg_color="transparent")
    preview_right.grid(row=0, column=1, sticky="nsew", padx=(0, 6), pady=10)
    preview_right.grid_columnconfigure(0, weight=1)
    preview_right.grid_rowconfigure(1, weight=1)
    preview_right.grid_rowconfigure(3, weight=1)

    column_header = ctk.CTkFrame(preview_right, fg_color="transparent")
    column_header.grid(row=0, column=0, sticky="ew", pady=(0, 6))
    column_header.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(column_header, text="Columns", font=FONT_CARD_TITLE, text_color=COLORS["text"], anchor="w").grid(row=0, column=0, sticky="ew")
    make_button(column_header, "Select all visible", lambda: select_all_visible(), width=130).grid(row=0, column=1, sticky="e", padx=(8, 0))

    column_box = ctk.CTkFrame(preview_right, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, corner_radius=16)
    column_box.grid(row=1, column=0, sticky="nsew", pady=(0, 12))
    column_box.grid_columnconfigure(0, weight=1)
    column_box.grid_rowconfigure(0, weight=1)
    column_list = tk.Listbox(
        column_box,
        selectmode=tk.EXTENDED,
        bg=COLORS["entry"],
        fg=COLORS["text"],
        selectbackground=COLORS["tile_selected"],
        selectforeground=COLORS["text"],
        activestyle="none",
        highlightthickness=0,
        relief="flat",
        font=("Consolas", 10),
        exportselection=False,
    )
    column_list.grid(row=0, column=0, sticky="nsew", padx=(12, 0), pady=12)
    column_y_scroll = tk.Scrollbar(column_box, orient="vertical", command=column_list.yview)
    column_y_scroll.grid(row=0, column=1, sticky="ns", padx=(0, 12), pady=12)
    column_x_scroll = tk.Scrollbar(column_box, orient="horizontal", command=column_list.xview)
    column_x_scroll.grid(row=1, column=0, sticky="ew", padx=(12, 0), pady=(0, 12))
    column_list.configure(yscrollcommand=column_y_scroll.set, xscrollcommand=column_x_scroll.set)

    ctk.CTkLabel(preview_right, text="Table preview", font=FONT_CARD_TITLE, text_color=COLORS["text"], anchor="w").grid(row=2, column=0, sticky="ew", pady=(0, 6))
    txt_preview = ctk.CTkTextbox(preview_right, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, text_color=COLORS["text"], font=FONT_MONO, corner_radius=16, wrap="none")
    txt_preview.grid(row=3, column=0, sticky="nsew")
    txt_preview.insert("end", "Previewed table rows will appear here.\n")
    txt_preview.configure(state="disabled")

    # ---------------------------
    # Figures tab
    # ---------------------------
    figures_tab.grid_columnconfigure(0, weight=0)
    figures_tab.grid_columnconfigure(1, weight=1)
    figures_tab.grid_rowconfigure(0, weight=1)

    figure_side = ctk.CTkFrame(figures_tab, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, corner_radius=16)
    figure_side.grid(row=0, column=0, sticky="nsw", padx=(6, 12), pady=10)
    figure_side.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(figure_side, text="Generated figures", font=FONT_CARD_TITLE, text_color=COLORS["text"]).grid(row=0, column=0, sticky="w", padx=18, pady=(18, 4))
    ctk.CTkLabel(
        figure_side,
        text="Figures generated during this GUI session appear here. Select one to preview it.",
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
        font=("Segoe UI", 10),
        exportselection=False,
        width=38,
        height=16,
    )
    figure_list.grid(row=0, column=0, sticky="nsew", padx=(10, 0), pady=10)
    figure_scroll = tk.Scrollbar(figure_list_box, orient="vertical", command=figure_list.yview)
    figure_scroll.grid(row=0, column=1, sticky="ns", padx=(0, 10), pady=10)
    figure_list.configure(yscrollcommand=figure_scroll.set)

    make_button(figure_side, "Open figure file", lambda: open_selected_figure_file(), width=150).grid(row=3, column=0, sticky="ew", padx=18, pady=(4, 8))
    make_button(figure_side, "Open output folder", lambda: open_output_folder(), width=150).grid(row=4, column=0, sticky="ew", padx=18, pady=(0, 18))

    figure_preview_panel = ctk.CTkFrame(figures_tab, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, corner_radius=16)
    figure_preview_panel.grid(row=0, column=1, sticky="nsew", padx=(0, 6), pady=10)
    figure_preview_panel.grid_columnconfigure(0, weight=1)
    figure_preview_panel.grid_rowconfigure(1, weight=1)
    ctk.CTkLabel(figure_preview_panel, text="Figure preview", font=FONT_CARD_TITLE, text_color=COLORS["text"], anchor="w").grid(row=0, column=0, sticky="ew", padx=18, pady=(18, 6))
    figure_image_label = ctk.CTkLabel(figure_preview_panel, text="No figure generated yet.", font=FONT_SMALL, text_color=COLORS["muted"])
    figure_image_label.grid(row=1, column=0, sticky="nsew", padx=18, pady=10)
    var_figure_path_text = tk.StringVar(value="")
    ctk.CTkLabel(figure_preview_panel, textvariable=var_figure_path_text, font=FONT_SMALL, text_color=COLORS["muted"], anchor="w", wraplength=900).grid(row=2, column=0, sticky="ew", padx=18, pady=(0, 18))

    # ---------------------------
    # Log tab
    # ---------------------------
    log_tab.grid_columnconfigure(0, weight=1)
    log_tab.grid_rowconfigure(0, weight=1)
    txt_log = ctk.CTkTextbox(log_tab, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, text_color=COLORS["text"], font=FONT_MONO, corner_radius=16, wrap="word")
    txt_log.grid(row=0, column=0, sticky="nsew", padx=6, pady=10)
    txt_log.insert("end", "Ready. Click 'Preview columns...' to inspect a table or configure the workflow and run selected steps.\n")
    txt_log.configure(state="disabled")

    status_bar = ctk.CTkFrame(root, fg_color=COLORS["surface"], corner_radius=0)
    status_bar.grid(row=2, column=0, sticky="ew")
    status_bar.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(status_bar, textvariable=var_status, font=FONT_SMALL, text_color=COLORS["muted"], anchor="w").grid(row=0, column=0, sticky="ew", padx=18, pady=8)

    # ---------------------------
    # Preview logic
    # ---------------------------
    all_columns: list[str] = []
    visible_column_indices: list[int] = []
    figure_records: list[dict[str, str]] = []
    current_figure_image = {"image": None}

    def update_log_badge() -> None:
        count = unread_log_events["count"]
        if count <= 0:
            var_log_badge.set("")
            log_badge.place_forget()
            return
        var_log_badge.set(str(count if count < 100 else "99+"))
        # Badge is anchored inside the Log tab button holder, so it stays in
        # the correct position when resizing or moving between monitors.
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

    def mark_figure_unread() -> None:
        if active_tab["name"] == "Figures":
            return
        unread_figure_events["count"] += 1
        update_figure_badge()

    def mark_log_unread() -> None:
        if active_tab["name"] == "Log":
            return
        unread_log_events["count"] += 1
        update_log_badge()

    def append_log(message: str, *, notify: bool = False) -> None:
        txt_log.configure(state="normal")
        txt_log.insert("end", message + "\n")
        txt_log.see("end")
        txt_log.configure(state="disabled")
        if notify:
            mark_log_unread()

    def set_status(text: str) -> None:
        var_status.set(text)
        root.update_idletasks()

    def poll_log_queue() -> None:
        had_messages = False
        try:
            while True:
                append_log(log_queue.get_nowait())
                had_messages = True
        except queue.Empty:
            pass
        if had_messages:
            mark_log_unread()
        root.after(120, poll_log_queue)

    def default_final_feature_table_path() -> str:
        out = Path(var_output_dir.get().strip().strip('"') or (_THIS_DIR / "Outputs_data_exploration")).expanduser()
        return str(out / "02_feature_table_with_annotations.csv")

    def refresh_default_final_feature_table(*_args: Any) -> None:
        current = var_final_feature_table.get().strip()
        old_default = app_state.get("_last_default_final_feature_table", "")
        new_default = default_final_feature_table_path()
        if not current or current == old_default:
            var_final_feature_table.set(new_default)
            app_state["_last_default_final_feature_table"] = new_default

    def open_path_in_os(path: Path) -> None:
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(path))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(path)])
            else:
                subprocess.Popen(["xdg-open", str(path)])
        except Exception as e:
            messagebox.showerror("Open error", f"Could not open:\n{path}\n\nDetails: {e}")

    def open_selected_figure_file() -> None:
        sel = figure_list.curselection()
        if not sel:
            messagebox.showinfo("No figure selected", "Select a figure first.")
            return
        idx = int(sel[0])
        if 0 <= idx < len(figure_records):
            open_path_in_os(Path(figure_records[idx].get("plot", "")))

    def open_output_folder() -> None:
        out = Path(var_output_dir.get().strip().strip('"') or (_THIS_DIR / "Outputs_data_exploration")).expanduser()
        if out.exists():
            open_path_in_os(out)
        else:
            messagebox.showinfo("Folder not found", f"The output folder does not exist yet:\n{out}")

    def display_figure_record(index: int) -> None:
        if not (0 <= index < len(figure_records)):
            return
        record = figure_records[index]
        preview = Path(record.get("preview", ""))
        plot = Path(record.get("plot", ""))
        var_figure_path_text.set(str(plot))
        if not preview.exists():
            figure_image_label.configure(text=f"Preview image not found.\nFigure file:\n{plot}", image=None)
            current_figure_image["image"] = None
            return
        try:
            from PIL import Image, ImageTk
            img = Image.open(preview)
            figure_preview_panel.update_idletasks()
            max_w = max(500, figure_preview_panel.winfo_width() - 60)
            max_h = max(360, figure_preview_panel.winfo_height() - 120)
            img.thumbnail((max_w, max_h), Image.LANCZOS)
            tk_img = ImageTk.PhotoImage(img)
            current_figure_image["image"] = tk_img
            figure_image_label.configure(text="", image=tk_img)
        except Exception as e:
            figure_image_label.configure(text=f"Could not load figure preview:\n{preview}\n\nDetails: {e}", image=None)
            current_figure_image["image"] = None

    def on_figure_select(_event: Any = None) -> None:
        sel = figure_list.curselection()
        if not sel:
            return
        display_figure_record(int(sel[0]))

    def add_figure_records_from_summary(summary: dict[str, Any]) -> None:
        analysis = summary.get("analysis") or {}
        files = analysis.get("files") or {}
        new_records: list[dict[str, str]] = []

        def add_record(label: str, plot: Any, preview: Any = None) -> None:
            if not plot:
                return
            plot_path = Path(str(plot))
            preview_path = Path(str(preview)) if preview else (plot_path if plot_path.suffix.lower() == ".png" else plot_path.with_suffix(".preview.png"))
            if plot_path.exists() or preview_path.exists():
                new_records.append({"label": label, "plot": str(plot_path), "preview": str(preview_path)})

        add_record(
            "Selected-term plot",
            files.get("selected_terms_across_samples_plot"),
            files.get("selected_terms_across_samples_preview_png"),
        )
        top = files.get("top_annotations_for_samples") or {}
        if isinstance(top, dict):
            for sample, item in top.items():
                if isinstance(item, dict):
                    add_record(f"Top categories: {sample}", item.get("plot"), item.get("preview_png"))
                elif isinstance(item, str):
                    parts = [x.strip() for x in item.split(";")]
                    plot = parts[-1] if parts else ""
                    add_record(f"Top categories: {sample}", plot)

        if not new_records:
            return
        for rec in new_records:
            figure_records.append(rec)
            figure_list.insert("end", rec["label"])
        figure_list.selection_clear(0, "end")
        figure_list.selection_set(len(figure_records) - len(new_records))
        figure_list.see(len(figure_records) - len(new_records))
        display_figure_record(len(figure_records) - len(new_records))
        mark_figure_unread()
        append_log(f"Added {len(new_records)} figure preview(s) to the Figures tab.", notify=True)

    figure_list.bind("<<ListboxSelect>>", on_figure_select)
    var_output_dir.trace_add("write", refresh_default_final_feature_table)
    refresh_default_final_feature_table()

    def preview_source_path(source: str) -> tuple[Path, str]:
        source = source.lower().strip()
        if source == "feature table":
            value = var_feature_table.get().strip().strip('"')
            label = "feature table"
        elif source == "metadata table":
            value = var_metadata_path.get().strip().strip('"')
            label = "metadata table"
        elif source == "annotation table":
            value = var_annotation_path.get().strip().strip('"')
            label = "annotation table"
        else:
            raise ValueError(f"Unknown preview source: {source}")
        if not value:
            raise ValueError(f"No {label} has been selected.")
        path = Path(value).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"Selected {label} does not exist: {path}")
        return path, label

    def open_preview_source_dialog() -> None:
        dialog = ctk.CTkToplevel(root)
        dialog.title("Preview columns")
        dialog.geometry("410x245")
        dialog.configure(fg_color=COLORS["bg"])
        dialog.transient(root)
        dialog.grab_set()
        dialog.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(dialog, text="Which file should be previewed?", font=FONT_CARD_TITLE, text_color=COLORS["text"]).grid(row=0, column=0, sticky="w", padx=24, pady=(24, 8))
        ctk.CTkLabel(dialog, text="Column information will be shown in the Preview tab and also printed to the log.", font=FONT_SMALL, text_color=COLORS["muted"], wraplength=340, justify="left").grid(row=1, column=0, sticky="w", padx=24, pady=(0, 16))
        make_combo(dialog, var_preview_source, ["feature table", "metadata table", "annotation table"]).grid(row=2, column=0, sticky="ew", padx=24, pady=(0, 18))
        buttons = ctk.CTkFrame(dialog, fg_color="transparent")
        buttons.grid(row=3, column=0, sticky="ew", padx=24, pady=(0, 24))
        buttons.grid_columnconfigure((0, 1), weight=1)
        make_button(buttons, "Cancel", dialog.destroy).grid(row=0, column=0, sticky="ew", padx=(0, 8))
        make_button(buttons, "Preview", lambda: (dialog.destroy(), preview_columns(var_preview_source.get())), primary=True).grid(row=0, column=1, sticky="ew", padx=(8, 0))

    def preview_columns(source: str) -> None:
        nonlocal all_columns
        try:
            path, label = preview_source_path(source)
            df = read_table(path)
        except Exception as e:
            messagebox.showerror("Preview error", str(e))
            return

        all_columns = [str(c) for c in df.columns]
        render_column_list()

        append_log("Column preview requested. The column list is shown in the Preview / column picker tab and repeated here in the log.")
        append_log(f"Preview source: {label} — {path}")
        append_log(f"Rows: {len(df):,}; Columns: {len(df.columns):,}")
        for idx, col in enumerate(all_columns, start=1):
            append_log(f"  {idx:>3}. {col}")
        mark_log_unread()

        try:
            import pandas as pd
            with pd.option_context("display.max_columns", 20, "display.width", 260, "display.max_colwidth", 42):
                preview_text = df.head(12).to_string(index=False)
        except Exception:
            preview_text = str(df.head(12))

        txt_preview.configure(state="normal")
        txt_preview.delete("1.0", "end")
        txt_preview.insert("end", preview_text)
        txt_preview.configure(state="disabled")
        set_active_tab("Preview / column picker")
        set_status(f"Previewed {len(all_columns)} columns from {label}. See the log for the full list.")

    def current_selected_column_names() -> list[str]:
        names: list[str] = []
        for visible_pos in column_list.curselection():
            try:
                original_idx = visible_column_indices[int(visible_pos)]
                names.append(all_columns[original_idx])
            except Exception:
                continue
        return names

    def render_column_list() -> None:
        nonlocal visible_column_indices
        previously_selected = set(current_selected_column_names()) if all_columns else set()
        column_list.delete(0, "end")
        visible_column_indices = []
        filt = var_column_filter.get().strip().lower()
        for idx, col in enumerate(all_columns):
            if filt and filt not in col.lower():
                continue
            visible_column_indices.append(idx)
            column_list.insert("end", col)
            if col in previously_selected:
                column_list.selection_set("end")
        if not visible_column_indices:
            column_list.insert("end", "No columns to show. Click 'Preview columns...' first or change the filter.")
            column_list.itemconfig("end", foreground=COLORS["muted"])
        set_status(f"Showing {len(visible_column_indices)} visible column(s).")

    def select_all_visible() -> None:
        if not visible_column_indices:
            return
        column_list.selection_set(0, "end")
        set_status(f"Selected {len(visible_column_indices)} visible column(s).")

    def clear_selection() -> None:
        column_list.selection_clear(0, "end")
        set_status("Column selection cleared.")

    def selected_column_names() -> list[str]:
        return current_selected_column_names()

    def apply_selected_columns() -> None:
        cols = selected_column_names()
        if not cols:
            messagebox.showinfo("No selection", "Select one or more columns first. Ctrl-click and Shift-click are supported.")
            return
        targets: dict[str, tk.StringVar] = {
            "Manual sample columns": var_sample_columns,
            "Other columns to keep": var_columns_to_keep,
            "Annotation columns to add": var_columns_to_add,
            "Annotation/category column": var_annotation_column,
            "Analysis annotation column": var_annotation_column,
            "Sample subset for Mode A": var_selected_samples,
            "Selected sample columns": var_selected_samples,
            "Sample(s) for Mode B": var_samples_for_top,
            "Samples for top-category plot": var_samples_for_top,
            "Exclude columns": var_exclude_columns,
        }
        target_name = var_picker_target.get()
        if target_name not in targets:
            target_name = "Manual sample columns"
            var_picker_target.set(target_name)
        target_var = targets[target_name]
        if target_name in {"Annotation/category column", "Analysis annotation column"}:
            target_var.set(cols[0])
            message = f"Applied column to {target_name}: {cols[0]}"
        elif var_picker_mode.get() == "append" and target_var.get().strip():
            merged = list(dict.fromkeys([*split_csv(target_var.get()), *cols]))
            target_var.set(join_csv(merged))
            message = f"Appended {len(cols)} column(s) to {target_name}."
        else:
            target_var.set(join_csv(cols))
            message = f"Applied {len(cols)} column(s) to {target_name}."
        append_log(message, notify=True)
        set_status(message)

    var_column_filter.trace_add("write", lambda *_: render_column_list())

    # ---------------------------
    # Config and run logic
    # ---------------------------
    def collect_current_state() -> dict[str, Any]:
        return {
            "step_prepared": var_step_prepared.get(),
            "step_annotation": var_step_annotation.get(),
            "step_analysis": var_step_analysis.get(),
            "apply_metadata": var_apply_metadata.get(),
            "output_dir": var_output_dir.get().strip(),
            "feature_table": var_feature_table.get().strip(),
            "id_column": var_id_col.get().strip(),
            "columns_to_keep": var_columns_to_keep.get().strip(),
            "sample_method": var_sample_method.get().strip(),
            "sample_prefix": var_sample_prefix.get(),
            "sample_suffix": var_sample_suffix.get(),
            "sample_regex": var_sample_regex.get().strip(),
            "sample_columns": var_sample_columns.get().strip(),
            "exclude_columns": var_exclude_columns.get().strip(),
            "numeric_only": var_numeric_only.get(),
            "metadata_path": var_metadata_path.get().strip(),
            "metadata_key": var_metadata_key.get().strip(),
            "metadata_value": var_metadata_value.get().strip(),
            "metadata_strip_suffix": var_metadata_strip_suffix.get(),
            "metadata_strip_prefix": var_metadata_strip_prefix.get(),
            "metadata_fallback": var_metadata_fallback.get().strip(),
            "aggregate_duplicates": var_aggregate_duplicates.get(),
            "duplicate_aggregation": var_duplicate_aggregation.get().strip(),
            "annotation_path": var_annotation_path.get().strip(),
            "annotation_feature_id": var_annotation_feature_id.get().strip(),
            "annotation_id": var_annotation_id.get().strip(),
            "feature_id_parser": var_feature_id_parser.get().strip(),
            "annotation_id_parser": var_annotation_id_parser.get().strip(),
            "columns_to_add": var_columns_to_add.get().strip(),
            "missing_fill": var_missing_fill.get(),
            "analysis_enabled": var_analysis_enabled.get(),
            "analysis_annotation_column": var_annotation_column.get().strip(),
            "threshold": var_threshold.get().strip(),
            "top_n": var_top_n.get().strip(),
            "terms": var_terms.get().strip(),
            "match_mode": var_match_mode.get().strip(),
            "case_sensitive": var_case_sensitive.get(),
            "selected_samples": var_selected_samples.get().strip(),
            "samples_for_top": var_samples_for_top.get().strip(),
            "make_counts": var_make_counts.get(),
            "make_terms_plot": var_make_terms_plot.get(),
            "make_top_plot": var_make_top_plot.get(),
            "plot_format": var_plot_format.get().strip(),
            "font_family": var_font_family.get().strip(),
            "final_feature_table": var_final_feature_table.get().strip(),
        }

    def validate_and_build_config() -> dict[str, Any]:
        output_dir = Path(var_output_dir.get().strip().strip('"') or (_THIS_DIR / "Outputs_data_exploration")).expanduser()
        output_dir.mkdir(parents=True, exist_ok=True)
        direct_analysis_mode = bool(var_step_analysis.get()) and not bool(var_step_prepared.get()) and not bool(var_step_annotation.get())
        final_table_path = Path(var_final_feature_table.get().strip().strip('"') or default_final_feature_table_path()).expanduser()
        feature_raw = var_feature_table.get().strip().strip('"')
        feature_path = Path(feature_raw).expanduser() if feature_raw else final_table_path
        if direct_analysis_mode:
            if not final_table_path.exists():
                raise ValueError(
                    "Figure-only mode is selected, but the final feature table does not exist. "
                    "Choose an existing final feature table in the Analysis and plots card."
                )
        elif not feature_path.exists():
            raise ValueError("Please select an existing feature table.")

        config: dict[str, Any] = {
            "base_dir": str(_THIS_DIR.resolve()),
            "output_dir": str(output_dir.resolve()),
            "run_steps": {
                "write_prepared_table": bool(var_step_prepared.get()),
                "write_annotation_table": bool(var_step_annotation.get()),
                "run_analysis": bool(var_step_analysis.get()),
            },
            "feature_table": {
                "path": str(feature_path.resolve()),
                "id_column": var_id_col.get().strip() or "row ID",
                "columns_to_keep": split_csv(var_columns_to_keep.get()),
                "sample_selection": {
                    "method": var_sample_method.get().strip() or "manual",
                    "columns": split_csv(var_sample_columns.get()),
                    "prefix": var_sample_prefix.get() or None,
                    "suffix": var_sample_suffix.get() or None,
                    "regex": var_sample_regex.get().strip() or None,
                    "exclude_columns": split_csv(var_exclude_columns.get()),
                    "numeric_only": bool(var_numeric_only.get()),
                },
            },
        }

        metadata_path = var_metadata_path.get().strip().strip('"')
        if bool(var_apply_metadata.get()) and metadata_path:
            meta_file = Path(metadata_path).expanduser()
            if not meta_file.exists():
                raise ValueError("Metadata table path was provided but the file does not exist.")
            config["sample_metadata"] = {
                "enabled": True,
                "path": str(meta_file.resolve()),
                "key_column": var_metadata_key.get().strip() or "Filename",
                "value_column": var_metadata_value.get().strip() or "Sample name",
                "strip_suffix_before_mapping": var_metadata_strip_suffix.get() or None,
                "strip_prefix_before_mapping": var_metadata_strip_prefix.get() or None,
                "fallback": var_metadata_fallback.get().strip() or "original",
                "aggregate_duplicate_labels": bool(var_aggregate_duplicates.get()),
                "aggregation": var_duplicate_aggregation.get().strip() or "sum",
            }
        else:
            config["sample_metadata"] = {"enabled": False}

        annotation_path = var_annotation_path.get().strip().strip('"')
        if annotation_path:
            annot_file = Path(annotation_path).expanduser()
            if not annot_file.exists():
                raise ValueError("Annotation table path was provided but the file does not exist.")
            columns_to_add = split_csv(var_columns_to_add.get())
            if not columns_to_add:
                raise ValueError("Annotation table selected, but no columns to add were provided.")
            config["annotation_table"] = {
                "path": str(annot_file.resolve()),
                "feature_id_column": var_annotation_feature_id.get().strip() or var_id_col.get().strip() or "row ID",
                "annotation_id_column": var_annotation_id.get().strip() or "mappingFeatureId",
                "feature_id_parser": var_feature_id_parser.get().strip() or "numeric",
                "annotation_id_parser": var_annotation_id_parser.get().strip() or "numeric",
                "columns_to_add": columns_to_add,
                "missing_fill": var_missing_fill.get() if var_missing_fill.get().strip() else None,
            }
        else:
            config["annotation_table"] = {}

        config["analysis"] = {
            "enabled": bool(var_analysis_enabled.get()),
            "final_feature_table_path": str(final_table_path.resolve()) if final_table_path else "",
            "annotation_column": var_annotation_column.get().strip(),
            "threshold": parse_float("threshold", var_threshold.get()),
            "top_n": parse_int("top N", var_top_n.get()),
            "terms": split_csv(var_terms.get()),
            "match_mode": var_match_mode.get().strip() or "exact",
            "case_sensitive": bool(var_case_sensitive.get()),
            "selected_samples": split_csv(var_selected_samples.get()),
            "samples_for_top_annotations": split_csv(var_samples_for_top.get()),
            "make_counts_table": bool(var_make_counts.get()),
            "make_selected_terms_plot": bool(var_make_terms_plot.get()),
            "make_top_annotations_plot": bool(var_make_top_plot.get()),
            "drop_missing_annotation": True,
            "plot_format": var_plot_format.get().strip() or "svg",
            "font_family": var_font_family.get().strip() or "Arial",
            "svg_text": True,
            "selected_terms_figsize": [10, 5],
            "top_annotations_figsize": [10, 6],
            "selected_terms_rotation": 45,
            "top_annotations_rotation": 90,
        }
        return config

    def save_config_file() -> None:
        try:
            config = validate_and_build_config()
        except Exception as e:
            messagebox.showerror("Config error", str(e))
            return
        path = filedialog.asksaveasfilename(
            title="Save config JSON",
            initialdir=str(_THIS_DIR),
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            initialfile="data_exploration_config.json",
        )
        if path:
            Path(path).write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
            save_state(collect_current_state())
            messagebox.showinfo("Saved", f"Config saved to:\n{path}")

    def apply_config(config: dict[str, Any]) -> None:
        var_output_dir.set(str(config.get("output_dir", "Outputs_data_exploration")))
        steps = config.get("run_steps", {})
        var_step_prepared.set(bool(steps.get("write_prepared_table", True)))
        var_step_annotation.set(bool(steps.get("write_annotation_table", True)))
        var_step_analysis.set(bool(steps.get("run_analysis", True)))

        feature_cfg = config.get("feature_table", {})
        var_feature_table.set(str(feature_cfg.get("path", "")))
        var_id_col.set(str(feature_cfg.get("id_column", "row ID")))
        var_columns_to_keep.set(join_csv(feature_cfg.get("columns_to_keep", [])))
        sample_cfg = feature_cfg.get("sample_selection", {})
        var_sample_method.set(str(sample_cfg.get("method", "manual")))
        var_sample_prefix.set(str(sample_cfg.get("prefix", "") or ""))
        var_sample_suffix.set(str(sample_cfg.get("suffix", "") or ""))
        var_sample_regex.set(str(sample_cfg.get("regex", "") or ""))
        var_sample_columns.set(join_csv(sample_cfg.get("columns", [])))
        var_exclude_columns.set(join_csv(sample_cfg.get("exclude_columns", [])))
        var_numeric_only.set(bool(sample_cfg.get("numeric_only", True)))

        metadata_cfg = config.get("sample_metadata", {}) or {}
        var_apply_metadata.set(bool(metadata_cfg.get("enabled", bool(metadata_cfg.get("path", "")))))
        var_metadata_path.set(str(metadata_cfg.get("path", "")))
        var_metadata_key.set(str(metadata_cfg.get("key_column", "Filename")))
        var_metadata_value.set(str(metadata_cfg.get("value_column", "Sample name")))
        var_metadata_strip_suffix.set(str(metadata_cfg.get("strip_suffix_before_mapping", "") or ""))
        var_metadata_strip_prefix.set(str(metadata_cfg.get("strip_prefix_before_mapping", "") or ""))
        var_metadata_fallback.set(str(metadata_cfg.get("fallback", "original")))
        var_aggregate_duplicates.set(bool(metadata_cfg.get("aggregate_duplicate_labels", False)))
        var_duplicate_aggregation.set(str(metadata_cfg.get("aggregation", "sum")))

        annotation_cfg = config.get("annotation_table", {}) or {}
        var_annotation_path.set(str(annotation_cfg.get("path", "")))
        var_annotation_feature_id.set(str(annotation_cfg.get("feature_id_column", "row ID")))
        var_annotation_id.set(str(annotation_cfg.get("annotation_id_column", "mappingFeatureId")))
        var_feature_id_parser.set(str(annotation_cfg.get("feature_id_parser", "numeric")))
        var_annotation_id_parser.set(str(annotation_cfg.get("annotation_id_parser", "numeric")))
        var_columns_to_add.set(join_csv(annotation_cfg.get("columns_to_add", [])))
        var_missing_fill.set(str(annotation_cfg.get("missing_fill", "") or ""))

        analysis_cfg = config.get("analysis", {}) or {}
        var_final_feature_table.set(str(analysis_cfg.get("final_feature_table_path", "") or default_final_feature_table_path()))
        var_analysis_enabled.set(bool(analysis_cfg.get("enabled", True)))
        var_annotation_column.set(str(analysis_cfg.get("annotation_column", "NPC#class")))
        var_threshold.set(str(analysis_cfg.get("threshold", 100000)))
        var_top_n.set(str(analysis_cfg.get("top_n", 15)))
        var_terms.set(join_csv(analysis_cfg.get("terms", [])))
        var_match_mode.set(str(analysis_cfg.get("match_mode", "exact")))
        var_case_sensitive.set(bool(analysis_cfg.get("case_sensitive", False)))
        var_selected_samples.set(join_csv(analysis_cfg.get("selected_samples", [])))
        var_samples_for_top.set(join_csv(analysis_cfg.get("samples_for_top_annotations", [])))
        var_make_counts.set(bool(analysis_cfg.get("make_counts_table", True)))
        var_make_terms_plot.set(bool(analysis_cfg.get("make_selected_terms_plot", True)))
        var_make_top_plot.set(bool(analysis_cfg.get("make_top_annotations_plot", True)))
        var_plot_format.set(str(analysis_cfg.get("plot_format", "svg")))
        var_font_family.set(str(analysis_cfg.get("font_family", "Arial")))

    def load_config_file() -> None:
        path = filedialog.askopenfilename(title="Load config JSON", initialdir=str(_THIS_DIR), filetypes=[("JSON files", "*.json"), ("All files", "*.*")])
        if not path:
            return
        try:
            config = json.loads(Path(path).read_text(encoding="utf-8"))
            apply_config(config)
            save_state(collect_current_state())
            append_log(f"Loaded config: {path}")
        except Exception as e:
            messagebox.showerror("Load error", f"Could not read config file.\n\nDetails: {e}")

    def save_template_file() -> None:
        path = filedialog.asksaveasfilename(
            title="Save template config JSON",
            initialdir=str(_THIS_DIR),
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            initialfile="data_exploration_template.json",
        )
        if path:
            Path(path).write_text(json.dumps(template_config(), indent=2, ensure_ascii=False), encoding="utf-8")
            messagebox.showinfo("Saved", f"Template saved to:\n{path}")

    run_buttons: list[Any] = []

    def run_selected() -> None:
        try:
            config = validate_and_build_config()
        except Exception as e:
            messagebox.showerror("Config error", str(e))
            return
        save_state(collect_current_state())
        append_log("Starting selected workflow steps...")
        set_status("Running selected steps...")
        for button in run_buttons:
            button.configure(state="disabled")

        def worker() -> None:
            try:
                summary = run_pipeline(config)
                log_queue.put("Workflow finished successfully.")
                log_queue.put(json.dumps(summary, indent=2, ensure_ascii=False))
                root.after(0, lambda: add_figure_records_from_summary(summary))
                root.after(0, lambda: set_status("Finished."))
                root.after(0, lambda: messagebox.showinfo("Finished", f"Workflow finished.\n\nOutput directory:\n{summary.get('output_dir', '')}"))
            except Exception as e:
                log_queue.put("ERROR: " + str(e))
                log_queue.put(traceback.format_exc())
                root.after(0, lambda: set_status("Error."))
                root.after(0, lambda: messagebox.showerror("Run error", str(e)))
            finally:
                root.after(0, lambda: [button.configure(state="normal") for button in run_buttons])

        threading.Thread(target=worker, daemon=True).start()

    # Header buttons must be created after command functions exist.
    run_button = make_button(header_actions, "Run selected", run_selected, primary=True, width=120)
    run_button.pack(side="left", padx=(0, 8))
    run_buttons.append(run_button)
    make_button(header_actions, "Preview columns", open_preview_source_dialog, width=125).pack(side="left", padx=8)
    make_button(header_actions, "Save config", save_config_file, width=105).pack(side="left", padx=8)
    make_button(header_actions, "Load config", load_config_file, width=105).pack(side="left", padx=8)
    make_button(header_actions, "Template", save_template_file, width=95).pack(side="left", padx=(8, 0))

    poll_log_queue()
    render_column_list()
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
