#!/usr/bin/env python3
"""GUI application for plant bioactivity plotting.

Examples
--------
python p_00_01_plant_bioactivity_gui.py
python p_00_01_plant_bioactivity_gui.py --input data/plant_extract_fluorescence_graph.xlsx
"""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import sys
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText

import pandas as pd
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
import matplotlib.pyplot as plt


BG = "#262a30"
PANEL = "#31363d"
PANEL_ALT = "#3a4048"
ENTRY_BG = "#20242a"
FG = "#eef3f8"
MUTED = "#b5bfcb"
BLUE = "#4c86ff"
BLUE_DARK = "#2f69e1"
BORDER = "#4d5562"
SUCCESS = "#2ca96b"
WARNING = "#d7a53f"


def load_core_module(core_path: Path):
    spec = importlib.util.spec_from_file_location("plant_bioactivity_core", core_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load core module from: {core_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BioactivityGUI:
    def __init__(self, root: tk.Tk, core_module, initial_input: str | None = None) -> None:
        self.root = root
        self.core = core_module
        self.current_figure = None
        self.canvas_widget = None
        self.toolbar = None
        self.input_columns: list[str] = []
        self.order_columns: list[str] = []
        self.script_dir = Path(__file__).resolve().parent
        self.default_output_prefix = self.script_dir / "output" / "plant_bioactivity"
        self.state_file = self.script_dir / ".p_00_01_plant_bioactivity_gui_state.json"
        self.state = self._load_state()

        self.root.title("Plant bioactivity plotter")
        self.root.geometry("1480x920")
        self.root.minsize(1260, 780)
        self.root.configure(bg=BG)

        self._create_style()
        self._build_variables(initial_input)
        self._build_layout()
        self._restore_state_defaults()
        if self.input_path_var.get().strip():
            self.load_input_columns(auto_log=False)

    def _create_style(self) -> None:
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        style.configure("Dark.TNotebook", background=BG, borderwidth=0)
        style.configure(
            "Dark.TNotebook.Tab",
            background=PANEL_ALT,
            foreground=FG,
            padding=(16, 8),
            borderwidth=0,
        )
        style.map(
            "Dark.TNotebook.Tab",
            background=[("selected", BLUE_DARK), ("active", PANEL)],
            foreground=[("selected", "white")],
        )

        style.configure(
            "Dark.TCombobox",
            fieldbackground=ENTRY_BG,
            background=PANEL_ALT,
            foreground=FG,
            arrowcolor=FG,
            bordercolor=BORDER,
            lightcolor=BORDER,
            darkcolor=BORDER,
            padding=4,
        )
        style.map(
            "Dark.TCombobox",
            fieldbackground=[("readonly", ENTRY_BG)],
            background=[("readonly", ENTRY_BG)],
            foreground=[("readonly", FG)],
            selectbackground=[("readonly", ENTRY_BG)],
            selectforeground=[("readonly", FG)],
        )

    def _build_variables(self, initial_input: str | None) -> None:
        last_input = initial_input or self.state.get("input_path", "")
        self.input_path_var = tk.StringVar(value=last_input)
        self.order_path_var = tk.StringVar(value=self.state.get("order_path", ""))
        self.sample_column_var = tk.StringVar(value=self.state.get("sample_column", ""))
        self.order_column_var = tk.StringVar(value=self.state.get("order_column", ""))

        self.control_mode_var = tk.StringVar(value=self.state.get("control_mode", "None"))
        self.control_entry_1_var = tk.StringVar(value=self.state.get("control_entry_1", ""))
        self.control_entry_2_var = tk.StringVar(value=self.state.get("control_entry_2", ""))

        self.use_threshold_var = tk.BooleanVar(value=bool(self.state.get("use_threshold", False)))
        self.threshold_var = tk.StringVar(value=self.state.get("threshold", ""))
        self.threshold_mode_var = tk.StringVar(value=self.state.get("threshold_mode", "ge"))
        self.exclude_control_var = tk.BooleanVar(value=bool(self.state.get("exclude_control", True)))

        self.export_table_var = tk.BooleanVar(value=bool(self.state.get("export_table", True)))
        self.export_png_var = tk.BooleanVar(value=bool(self.state.get("export_png", True)))
        self.export_svg_var = tk.BooleanVar(value=bool(self.state.get("export_svg", True)))

        self.output_prefix_var = tk.StringVar(
            value=self.state.get("output_prefix", str(self.default_output_prefix))
        )
        self.title_var = tk.StringVar(value=self.state.get("title", "Sample activity"))
        self.ylabel_var = tk.StringVar(value=self.state.get("ylabel", ""))
        self.figure_width_var = tk.StringVar(value=self.state.get("figure_width", "16"))
        self.figure_height_var = tk.StringVar(value=self.state.get("figure_height", "6"))
        self.rotate_var = tk.StringVar(value=self.state.get("rotate", "45"))
        self.font_family_var = tk.StringVar(value=self.state.get("font_family", "Arial"))
        self.title_size_var = tk.StringVar(value=self.state.get("title_size", "16"))
        self.axis_label_size_var = tk.StringVar(value=self.state.get("axis_label_size", "20"))
        self.xtick_label_size_var = tk.StringVar(value=self.state.get("xtick_label_size", "8"))
        self.ytick_label_size_var = tk.StringVar(value=self.state.get("ytick_label_size", "16"))
        self.legend_size_var = tk.StringVar(value=self.state.get("legend_size", "12"))
        self.status_var = tk.StringVar(value="Ready.")
        self.preview_summary_var = tk.StringVar(
            value="Load a table, choose columns, and preview the figure here."
        )

        self.replicate_listbox = None
        self.sample_combo = None
        self.order_combo = None
        self.control_label_1 = None
        self.control_label_2 = None
        self.control_entry_1 = None
        self.control_entry_2 = None
        self.threshold_entry = None
        self.threshold_mode_combo = None
        self.log_text = None
        self.preview_frame = None
        self.preview_hint = None

    def _build_layout(self) -> None:
        outer = tk.Frame(self.root, bg=BG)
        outer.pack(fill="both", expand=True, padx=12, pady=12)

        self._build_header(outer)
        self._build_toolbar(outer)

        body = tk.PanedWindow(outer, orient="horizontal", bg=BG, sashwidth=8, bd=0, relief="flat")
        body.pack(fill="both", expand=True)

        left = tk.Frame(body, bg=BG, width=560)
        right = tk.Frame(body, bg=BG)
        body.add(left, minsize=520)
        body.add(right, minsize=520)

        self._build_left_panel(left)
        self._build_right_panel(right)

        footer = tk.Frame(outer, bg=BG)
        footer.pack(fill="x", pady=(10, 0))
        tk.Label(footer, textvariable=self.status_var, bg=BG, fg=MUTED, anchor="w").pack(side="left")

    def _build_header(self, parent: tk.Widget) -> None:
        header = tk.Frame(parent, bg=BG)
        header.pack(fill="x", pady=(0, 10))

        tk.Label(
            header,
            text="Plant bioactivity plotter",
            bg=BG,
            fg=FG,
            font=("Segoe UI", 16, "bold"),
            anchor="w",
        ).pack(anchor="w")
        tk.Label(
            header,
            text=(
                "GUI-first application for end-users. Pick a table, choose columns, preview the plot, "
                "then export only the outputs you want."
            ),
            bg=BG,
            fg=MUTED,
            anchor="w",
            justify="left",
        ).pack(anchor="w", pady=(2, 0))

    def _build_toolbar(self, parent: tk.Widget) -> None:
        bar = tk.Frame(parent, bg=BG)
        bar.pack(fill="x", pady=(0, 10))

        self._make_button(bar, "Preview figure", self.preview_figure).pack(side="left", padx=(0, 8))
        self._make_button(bar, "Run + export", self.run_and_export, primary=True).pack(side="left", padx=(0, 8))
        self._make_button(bar, "Load columns", self.load_input_columns, secondary=True).pack(side="left", padx=(0, 8))
        self._make_button(bar, "Reset", self.reset_form, secondary=True).pack(side="left", padx=(0, 8))
        self._make_button(bar, "Close", self.root.destroy, secondary=True).pack(side="right")

    def _build_left_panel(self, parent: tk.Widget) -> None:
        notebook = ttk.Notebook(parent, style="Dark.TNotebook")
        notebook.pack(fill="both", expand=True)

        self.tab_data = tk.Frame(notebook, bg=BG)
        self.tab_controls = tk.Frame(notebook, bg=BG)
        self.tab_plot = tk.Frame(notebook, bg=BG)

        notebook.add(self.tab_data, text="Data")
        notebook.add(self.tab_controls, text="Controls")
        notebook.add(self.tab_plot, text="Plot & Export")

        self._build_data_tab(self.tab_data)
        self._build_controls_tab(self.tab_controls)
        self._build_plot_tab(self.tab_plot)

    def _build_right_panel(self, parent: tk.Widget) -> None:
        split = tk.PanedWindow(parent, orient="vertical", bg=BG, sashwidth=8, bd=0, relief="flat")
        split.pack(fill="both", expand=True)

        preview_panel = tk.Frame(split, bg=PANEL, highlightbackground=BORDER, highlightthickness=1)
        info_panel = tk.Frame(split, bg=PANEL, highlightbackground=BORDER, highlightthickness=1)
        split.add(preview_panel, minsize=420)
        split.add(info_panel, minsize=220)

        tk.Label(
            preview_panel,
            text="Figure preview",
            bg=BLUE_DARK,
            fg="white",
            anchor="w",
            font=("Segoe UI", 11, "bold"),
            padx=12,
            pady=8,
        ).pack(fill="x")

        summary = tk.Label(
            preview_panel,
            textvariable=self.preview_summary_var,
            bg=PANEL,
            fg=MUTED,
            anchor="w",
            justify="left",
            wraplength=700,
            padx=12,
            pady=10,
        )
        summary.pack(fill="x")

        self.preview_frame = tk.Frame(preview_panel, bg="white")
        self.preview_frame.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        self.preview_hint = tk.Label(
            self.preview_frame,
            text="No preview yet.",
            bg="white",
            fg="#404040",
            font=("Segoe UI", 13),
        )
        self.preview_hint.pack(expand=True)

        tk.Label(
            info_panel,
            text="Run log",
            bg=BLUE_DARK,
            fg="white",
            anchor="w",
            font=("Segoe UI", 11, "bold"),
            padx=12,
            pady=8,
        ).pack(fill="x")

        self.log_text = ScrolledText(
            info_panel,
            height=10,
            bg=ENTRY_BG,
            fg=FG,
            insertbackground=FG,
            relief="flat",
            highlightbackground=BORDER,
            highlightthickness=1,
            font=("Consolas", 10),
        )
        self.log_text.pack(fill="both", expand=True, padx=12, pady=12)
        self.log("Ready.")

    def _section(self, parent: tk.Widget, title: str, subtitle: str | None = None) -> tk.Frame:
        frame = tk.Frame(parent, bg=PANEL, highlightbackground=BORDER, highlightthickness=1)
        frame.pack(fill="x", pady=(0, 10), padx=2)

        tk.Label(
            frame,
            text=title,
            bg=BLUE_DARK,
            fg="white",
            anchor="w",
            font=("Segoe UI", 11, "bold"),
            padx=12,
            pady=8,
        ).pack(fill="x")

        if subtitle:
            tk.Label(
                frame,
                text=subtitle,
                bg=PANEL,
                fg=MUTED,
                anchor="w",
                justify="left",
                wraplength=500,
                padx=12,
                pady=8,
            ).pack(fill="x")

        inner = tk.Frame(frame, bg=PANEL, padx=12, pady=12)
        inner.pack(fill="both", expand=True)
        return inner

    def _build_data_tab(self, parent: tk.Widget) -> None:
        sec1 = self._section(
            parent,
            "Input table",
            "Choose the main Excel/CSV file. The GUI can then read the headers and let the user select columns.",
        )
        for col in range(4):
            sec1.grid_columnconfigure(col, weight=1 if col in {0, 1, 2} else 0)

        self._make_label(sec1, "Input file").grid(row=0, column=0, sticky="w")
        self._make_entry(sec1, self.input_path_var).grid(row=1, column=0, columnspan=2, sticky="ew", padx=(0, 8), pady=(4, 10), ipady=4)
        self._make_button(sec1, "Browse", self.browse_input, secondary=True).grid(row=1, column=2, sticky="ew", padx=(0, 8), pady=(4, 10))
        self._make_button(sec1, "Load columns", self.load_input_columns, primary=True).grid(row=1, column=3, sticky="ew", pady=(4, 10))

        self._make_label(sec1, "Sample / species column").grid(row=2, column=0, sticky="w")
        self.sample_combo = ttk.Combobox(sec1, textvariable=self.sample_column_var, state="readonly", style="Dark.TCombobox")
        self.sample_combo.grid(row=3, column=0, columnspan=4, sticky="ew", pady=(4, 10))

        self._make_label(sec1, "Replicate columns").grid(row=4, column=0, sticky="w")
        tk.Label(
            sec1,
            text="Use Ctrl/Shift to select multiple replicate columns.",
            bg=PANEL,
            fg=MUTED,
            anchor="w",
        ).grid(row=5, column=0, columnspan=4, sticky="w")

        listbox_wrap = tk.Frame(sec1, bg=PANEL)
        listbox_wrap.grid(row=6, column=0, columnspan=4, sticky="ew", pady=(6, 10))
        listbox_wrap.grid_columnconfigure(0, weight=1)

        self.replicate_listbox = tk.Listbox(
            listbox_wrap,
            selectmode="extended",
            height=8,
            bg=ENTRY_BG,
            fg=FG,
            selectbackground=BLUE,
            selectforeground="white",
            highlightbackground=BORDER,
            highlightthickness=1,
            relief="flat",
            exportselection=False,
            font=("Consolas", 10),
        )
        scrollbar = tk.Scrollbar(listbox_wrap, orient="vertical", command=self.replicate_listbox.yview)
        self.replicate_listbox.configure(yscrollcommand=scrollbar.set)
        self.replicate_listbox.grid(row=0, column=0, sticky="ew")
        scrollbar.grid(row=0, column=1, sticky="ns")

        btns = tk.Frame(sec1, bg=PANEL)
        btns.grid(row=7, column=0, columnspan=4, sticky="w")
        self._make_button(btns, "Select numeric", self.select_numeric_columns, secondary=True).pack(side="left", padx=(0, 8))
        self._make_button(btns, "Clear selection", self.clear_replicate_selection, secondary=True).pack(side="left")

        sec2 = self._section(
            parent,
            "Optional plotting order",
            "Only use this if the user wants a custom sample order from another table.",
        )
        for col in range(4):
            sec2.grid_columnconfigure(col, weight=1 if col in {0, 1, 2} else 0)

        self._make_label(sec2, "Order file").grid(row=0, column=0, sticky="w")
        self._make_entry(sec2, self.order_path_var).grid(row=1, column=0, columnspan=2, sticky="ew", padx=(0, 8), pady=(4, 10), ipady=4)
        self._make_button(sec2, "Browse", self.browse_order, secondary=True).grid(row=1, column=2, sticky="ew", padx=(0, 8), pady=(4, 10))
        self._make_button(sec2, "Load order columns", self.load_order_columns, secondary=True).grid(row=1, column=3, sticky="ew", pady=(4, 10))

        self._make_label(sec2, "Order column").grid(row=2, column=0, sticky="w")
        self.order_combo = ttk.Combobox(sec2, textvariable=self.order_column_var, state="readonly", style="Dark.TCombobox")
        self.order_combo.grid(row=3, column=0, columnspan=4, sticky="ew", pady=(4, 0))

    def _build_controls_tab(self, parent: tk.Widget) -> None:
        sec1 = self._section(
            parent,
            "Threshold-based classification",
            "Optional. Useful when the user first wants to preview the values and only later decide what is active/inactive.",
        )
        for col in range(3):
            sec1.grid_columnconfigure(col, weight=1)

        tk.Checkbutton(
            sec1,
            text="Use threshold to assign Active / Inactive classes",
            variable=self.use_threshold_var,
            command=self.update_threshold_state,
            bg=PANEL,
            fg=FG,
            activebackground=PANEL,
            activeforeground=FG,
            selectcolor=ENTRY_BG,
            highlightthickness=0,
        ).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 10))

        self._make_label(sec1, "Threshold").grid(row=1, column=0, sticky="w")
        self._make_label(sec1, "Rule").grid(row=1, column=1, sticky="w")
        tk.Label(sec1, text="ge = Active if value ≥ threshold\nle = Active if value ≤ threshold", bg=PANEL, fg=MUTED, justify="left", anchor="w").grid(row=1, column=2, sticky="w")

        self.threshold_entry = self._make_entry(sec1, self.threshold_var, width=14)
        self.threshold_entry.grid(row=2, column=0, sticky="ew", padx=(0, 12), pady=(4, 0), ipady=4)
        self.threshold_mode_combo = ttk.Combobox(
            sec1,
            textvariable=self.threshold_mode_var,
            state="readonly",
            style="Dark.TCombobox",
            values=["ge", "le"],
            width=8,
        )
        self.threshold_mode_combo.grid(row=2, column=1, sticky="ew", pady=(4, 0))

        sec2 = self._section(
            parent,
            "Control selection",
            "Controls are used for normalization. The same controls can optionally be excluded from the plot and export table.",
        )
        for col in range(2):
            sec2.grid_columnconfigure(col, weight=1)

        self._make_label(sec2, "Control mode").grid(row=0, column=0, sticky="w")
        mode_combo = ttk.Combobox(
            sec2,
            textvariable=self.control_mode_var,
            state="readonly",
            style="Dark.TCombobox",
            values=["None", "Row numbers", "Sample names", "Column = value", "Query"],
        )
        mode_combo.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(4, 10))
        mode_combo.bind("<<ComboboxSelected>>", lambda event: self.update_control_inputs())

        self.control_label_1 = self._make_label(sec2, "")
        self.control_label_1.grid(row=2, column=0, columnspan=2, sticky="w")
        self.control_entry_1 = self._make_entry(sec2, self.control_entry_1_var)
        self.control_entry_1.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(4, 10), ipady=4)

        self.control_label_2 = self._make_label(sec2, "")
        self.control_label_2.grid(row=4, column=0, columnspan=2, sticky="w")
        self.control_entry_2 = self._make_entry(sec2, self.control_entry_2_var)
        self.control_entry_2.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(4, 10), ipady=4)

        tk.Checkbutton(
            sec2,
            text="Exclude control rows from plot and exported table",
            variable=self.exclude_control_var,
            bg=PANEL,
            fg=FG,
            activebackground=PANEL,
            activeforeground=FG,
            selectcolor=ENTRY_BG,
            highlightthickness=0,
        ).grid(row=6, column=0, columnspan=2, sticky="w")

        tk.Label(
            sec2,
            text="Tip: row numbers are 1-based, exactly as a human reads the spreadsheet.",
            bg=PANEL,
            fg=MUTED,
            anchor="w",
        ).grid(row=7, column=0, columnspan=2, sticky="w", pady=(10, 0))

        self.update_threshold_state()
        self.update_control_inputs()

    def _build_plot_tab(self, parent: tk.Widget) -> None:
        sec1 = self._section(
            parent,
            "Plot appearance",
            "These settings affect preview and export. Leave Y label empty if the automatic label is fine.",
        )
        for col in range(4):
            sec1.grid_columnconfigure(col, weight=1)

        self._make_label(sec1, "Title").grid(row=0, column=0, sticky="w")
        self._make_entry(sec1, self.title_var).grid(row=1, column=0, columnspan=4, sticky="ew", pady=(4, 10), ipady=4)

        self._make_label(sec1, "Y label").grid(row=2, column=0, sticky="w")
        self._make_entry(sec1, self.ylabel_var).grid(row=3, column=0, columnspan=4, sticky="ew", pady=(4, 10), ipady=4)

        self._make_label(sec1, "Width").grid(row=4, column=0, sticky="w")
        self._make_label(sec1, "Height").grid(row=4, column=1, sticky="w")
        self._make_label(sec1, "Bottom label tilt (°)").grid(row=4, column=2, sticky="w")
        tk.Label(sec1, text="inches / degrees", bg=PANEL, fg=MUTED, anchor="w").grid(row=4, column=3, sticky="w")

        self._make_entry(sec1, self.figure_width_var, width=10).grid(row=5, column=0, sticky="ew", padx=(0, 10), pady=(4, 0), ipady=4)
        self._make_entry(sec1, self.figure_height_var, width=10).grid(row=5, column=1, sticky="ew", padx=(0, 10), pady=(4, 0), ipady=4)
        self._make_entry(sec1, self.rotate_var, width=10).grid(row=5, column=2, sticky="ew", pady=(4, 0), ipady=4)

        sec_style = self._section(
            parent,
            "Text style",
            "Control font family and the sizes of the main text elements in the figure.",
        )
        for col in range(4):
            sec_style.grid_columnconfigure(col, weight=1)

        self._make_label(sec_style, "Font family").grid(row=0, column=0, sticky="w")
        font_combo = ttk.Combobox(
            sec_style,
            textvariable=self.font_family_var,
            values=["Arial", "DejaVu Sans", "Liberation Sans", "Helvetica", "Calibri", "Times New Roman"],
            style="Dark.TCombobox",
        )
        font_combo.grid(row=1, column=0, columnspan=4, sticky="ew", pady=(4, 10))

        self._make_label(sec_style, "Title size").grid(row=2, column=0, sticky="w")
        self._make_label(sec_style, "Axis label size").grid(row=2, column=1, sticky="w")
        self._make_label(sec_style, "Bottom label size").grid(row=2, column=2, sticky="w")
        self._make_label(sec_style, "Y tick size").grid(row=2, column=3, sticky="w")

        self._make_entry(sec_style, self.title_size_var, width=10).grid(row=3, column=0, sticky="ew", padx=(0, 10), pady=(4, 10), ipady=4)
        self._make_entry(sec_style, self.axis_label_size_var, width=10).grid(row=3, column=1, sticky="ew", padx=(0, 10), pady=(4, 10), ipady=4)
        self._make_entry(sec_style, self.xtick_label_size_var, width=10).grid(row=3, column=2, sticky="ew", padx=(0, 10), pady=(4, 10), ipady=4)
        self._make_entry(sec_style, self.ytick_label_size_var, width=10).grid(row=3, column=3, sticky="ew", pady=(4, 10), ipady=4)

        self._make_label(sec_style, "Legend size").grid(row=4, column=0, sticky="w")
        self._make_entry(sec_style, self.legend_size_var, width=10).grid(row=5, column=0, sticky="ew", padx=(0, 10), pady=(4, 0), ipady=4)
        tk.Label(
            sec_style,
            text="These settings apply to both preview and exported figures.",
            bg=PANEL,
            fg=MUTED,
            anchor="w",
        ).grid(row=5, column=1, columnspan=3, sticky="w", pady=(4, 0))

        sec2 = self._section(
            parent,
            "Export settings",
            "Choose where the exported files should go and which formats should be written.",
        )
        for col in range(3):
            sec2.grid_columnconfigure(col, weight=1 if col in {0, 1} else 0)

        self._make_label(sec2, "Output prefix (no extension)").grid(row=0, column=0, sticky="w")
        self._make_entry(sec2, self.output_prefix_var).grid(row=1, column=0, columnspan=2, sticky="ew", padx=(0, 8), pady=(4, 10), ipady=4)
        self._make_button(sec2, "Save as…", self.choose_output_prefix, secondary=True).grid(row=1, column=2, sticky="ew", pady=(4, 10))

        tk.Checkbutton(sec2, text="Export table (.csv)", variable=self.export_table_var, bg=PANEL, fg=FG, activebackground=PANEL, activeforeground=FG, selectcolor=ENTRY_BG, highlightthickness=0).grid(row=2, column=0, sticky="w")
        tk.Checkbutton(sec2, text="Export PNG", variable=self.export_png_var, bg=PANEL, fg=FG, activebackground=PANEL, activeforeground=FG, selectcolor=ENTRY_BG, highlightthickness=0).grid(row=3, column=0, sticky="w")
        tk.Checkbutton(sec2, text="Export SVG", variable=self.export_svg_var, bg=PANEL, fg=FG, activebackground=PANEL, activeforeground=FG, selectcolor=ENTRY_BG, highlightthickness=0).grid(row=4, column=0, sticky="w")

        note = tk.Label(
            sec2,
            text="Preview does not write files. Run + export does.",
            bg=PANEL,
            fg=SUCCESS,
            anchor="w",
        )
        note.grid(row=5, column=0, columnspan=3, sticky="w", pady=(10, 0))

    def _make_label(self, parent: tk.Widget, text: str) -> tk.Label:
        return tk.Label(parent, text=text, bg=PANEL, fg=FG, anchor="w", font=("Segoe UI", 10))

    def _make_entry(self, parent: tk.Widget, textvariable: tk.Variable, width: int = 40) -> tk.Entry:
        return tk.Entry(
            parent,
            textvariable=textvariable,
            width=width,
            bg=ENTRY_BG,
            fg=FG,
            insertbackground=FG,
            highlightbackground=BORDER,
            highlightcolor=BLUE,
            highlightthickness=1,
            relief="flat",
            font=("Consolas", 10),
        )

    def _make_button(self, parent: tk.Widget, text: str, command, primary: bool = False, secondary: bool = False) -> tk.Button:
        bg = BLUE if primary else (PANEL_ALT if secondary else BLUE_DARK)
        active = BLUE_DARK if primary else (BORDER if secondary else BLUE)
        return tk.Button(
            parent,
            text=text,
            command=command,
            bg=bg,
            fg="white",
            activebackground=active,
            activeforeground="white",
            relief="flat",
            padx=12,
            pady=8,
            cursor="hand2",
            font=("Segoe UI", 10, "bold"),
            highlightthickness=0,
            bd=0,
        )

    def _load_state(self) -> dict:
        if not self.state_file.exists():
            return {}
        try:
            return json.loads(self.state_file.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_state(self) -> None:
        try:
            state = {
                "input_path": self.input_path_var.get().strip(),
                "order_path": self.order_path_var.get().strip(),
                "sample_column": self.sample_column_var.get().strip(),
                "order_column": self.order_column_var.get().strip(),
                "control_mode": self.control_mode_var.get().strip(),
                "control_entry_1": self.control_entry_1_var.get().strip(),
                "control_entry_2": self.control_entry_2_var.get().strip(),
                "use_threshold": self.use_threshold_var.get(),
                "threshold": self.threshold_var.get().strip(),
                "threshold_mode": self.threshold_mode_var.get().strip(),
                "exclude_control": self.exclude_control_var.get(),
                "export_table": self.export_table_var.get(),
                "export_png": self.export_png_var.get(),
                "export_svg": self.export_svg_var.get(),
                "output_prefix": self.output_prefix_var.get().strip(),
                "title": self.title_var.get().strip(),
                "ylabel": self.ylabel_var.get().strip(),
                "figure_width": self.figure_width_var.get().strip(),
                "figure_height": self.figure_height_var.get().strip(),
                "rotate": self.rotate_var.get().strip(),
                "font_family": self.font_family_var.get().strip(),
                "title_size": self.title_size_var.get().strip(),
                "axis_label_size": self.axis_label_size_var.get().strip(),
                "xtick_label_size": self.xtick_label_size_var.get().strip(),
                "ytick_label_size": self.ytick_label_size_var.get().strip(),
                "legend_size": self.legend_size_var.get().strip(),
            }
            self.state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")
        except Exception:
            pass

    def _restore_state_defaults(self) -> None:
        self.update_threshold_state()
        self.update_control_inputs()

    def _dialog_initial_dir(self, current_value: str = "") -> str:
        text = (current_value or "").strip()
        if text:
            path = Path(text).expanduser()
            if path.exists():
                return str(path if path.is_dir() else path.parent)
        return str(self.script_dir)

    def _default_output_dir(self) -> Path:
        output_dir = self.script_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def update_status(self, text: str) -> None:
        self.status_var.set(text)
        self.root.update_idletasks()

    def update_threshold_state(self) -> None:
        state = "normal" if self.use_threshold_var.get() else "disabled"
        self.threshold_entry.configure(state=state)
        self.threshold_mode_combo.configure(state="readonly" if self.use_threshold_var.get() else "disabled")

    def update_control_inputs(self) -> None:
        mode = self.control_mode_var.get()
        configs = {
            "None": ("", "", False),
            "Row numbers": ("Control row numbers (comma-separated, 1-based)", "", False),
            "Sample names": ("Control sample names (comma-separated)", "", False),
            "Column = value": ("Control column", "Control value", True),
            "Query": ("Control query / mask", "Example: bioactivity == 2", True),
        }
        label1, label2, show_second = configs.get(mode, ("", "", False))
        self.control_label_1.config(text=label1)
        self.control_label_2.config(text=label2)

        if show_second:
            self.control_label_2.grid()
            self.control_entry_2.grid()
        else:
            self.control_label_2.grid_remove()
            self.control_entry_2.grid_remove()

        if mode == "None":
            self.exclude_control_var.set(False)

    def browse_input(self) -> None:
        path = filedialog.askopenfilename(
            title="Choose input table",
            initialdir=self._dialog_initial_dir(self.input_path_var.get()),
            filetypes=[("Tables", "*.csv *.xlsx *.xls"), ("All files", "*.*")],
        )
        if path:
            self.input_path_var.set(path)
            self._suggest_output_prefix(Path(path))
            self.load_input_columns(auto_log=False)

    def browse_order(self) -> None:
        path = filedialog.askopenfilename(
            title="Choose order table",
            initialdir=self._dialog_initial_dir(self.order_path_var.get()),
            filetypes=[("Tables", "*.csv *.xlsx *.xls"), ("All files", "*.*")],
        )
        if path:
            self.order_path_var.set(path)
            self.load_order_columns(auto_log=False)

    def choose_output_prefix(self) -> None:
        current_prefix = self.output_prefix_var.get().strip()
        current_path = Path(current_prefix).expanduser() if current_prefix else self.default_output_prefix
        path = filedialog.asksaveasfilename(
            title="Choose output prefix",
            initialdir=str(current_path.parent if current_path.parent.exists() else self._default_output_dir()),
            initialfile=current_path.name,
            defaultextension="",
            filetypes=[("All files", "*.*")],
        )
        if path:
            output_path = Path(path)
            if output_path.suffix:
                output_path = output_path.with_suffix("")
            self.output_prefix_var.set(str(output_path))

    def _suggest_output_prefix(self, input_path: Path) -> None:
        current = self.output_prefix_var.get().strip()
        starter = str(self.default_output_prefix)
        if current and current != starter:
            return
        output_dir = self._default_output_dir()
        self.output_prefix_var.set(str(output_dir / input_path.stem))

    def load_input_columns(self, auto_log: bool = True) -> None:
        path_text = self.input_path_var.get().strip()
        if not path_text:
            if auto_log:
                messagebox.showwarning("No input file", "Choose an input file first.")
            return

        try:
            df = self.core.read_table(Path(path_text))
        except Exception as exc:
            messagebox.showerror("Could not read input file", str(exc))
            return

        self.input_df = df
        self.input_columns = list(df.columns)
        self.sample_combo["values"] = self.input_columns
        self.replicate_listbox.delete(0, tk.END)
        for col in self.input_columns:
            self.replicate_listbox.insert(tk.END, col)

        if self.input_columns:
            self.sample_column_var.set(self._guess_sample_column(self.input_columns, current=self.sample_column_var.get()))

        self.select_numeric_columns()
        self._suggest_output_prefix(Path(path_text))
        self._save_state()

        if auto_log:
            self.log(f"Loaded input columns from: {path_text}")
        else:
            self.log(f"Loaded input file: {path_text}")

    def load_order_columns(self, auto_log: bool = True) -> None:
        path_text = self.order_path_var.get().strip()
        if not path_text:
            if auto_log:
                messagebox.showwarning("No order file", "Choose an order file first.")
            return

        try:
            df = self.core.read_table(Path(path_text))
        except Exception as exc:
            messagebox.showerror("Could not read order file", str(exc))
            return

        self.order_columns = list(df.columns)
        self.order_combo["values"] = self.order_columns
        if self.order_columns:
            self.order_column_var.set(self._guess_sample_column(self.order_columns, current=self.order_column_var.get()))

        self._save_state()
        if auto_log:
            self.log(f"Loaded order columns from: {path_text}")
        else:
            self.log(f"Loaded order file: {path_text}")

    def _guess_sample_column(self, columns: list[str], current: str = "") -> str:
        if current in columns:
            return current
        priorities = ["species", "sample", "name", "plant"]
        lowered = {col.lower(): col for col in columns}
        for needle in priorities:
            for low, original in lowered.items():
                if needle in low:
                    return original
        return columns[0] if columns else ""

    def select_numeric_columns(self) -> None:
        self.clear_replicate_selection()
        if not hasattr(self, "input_df"):
            return

        df = self.input_df
        candidates = []
        numeric_cols = set(df.select_dtypes(include="number").columns.tolist())
        for idx, col in enumerate(self.input_columns):
            low = col.lower()
            if col in numeric_cols or any(tag in low for tag in ["rep", "plate", "fluor", "signal", "readout", "avg"]):
                if col != self.sample_column_var.get():
                    self.replicate_listbox.selection_set(idx)
                    candidates.append(col)
        if candidates:
            self.log(f"Selected replicate candidates: {', '.join(candidates)}")

    def clear_replicate_selection(self) -> None:
        if self.replicate_listbox is not None:
            self.replicate_listbox.selection_clear(0, tk.END)

    def _selected_replicate_columns(self) -> list[str]:
        indices = self.replicate_listbox.curselection()
        return [self.replicate_listbox.get(i) for i in indices]

    def _parse_row_numbers(self, text: str) -> list[int]:
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

    def collect_settings(self) -> dict:
        input_path = self.input_path_var.get().strip()
        if not input_path:
            raise ValueError("Choose an input file.")

        sample_column = self.sample_column_var.get().strip()
        if not sample_column:
            raise ValueError("Choose the sample/species column.")

        replicate_columns = self._selected_replicate_columns()
        if not replicate_columns:
            raise ValueError("Choose at least one replicate column.")

        try:
            figure_size = (float(self.figure_width_var.get()), float(self.figure_height_var.get()))
        except ValueError as exc:
            raise ValueError("Figure width and height must be numbers.") from exc

        try:
            rotate_labels = float(self.rotate_var.get())
        except ValueError as exc:
            raise ValueError("Label rotation must be a number.") from exc

        try:
            title_size = float(self.title_size_var.get())
            axis_label_size = float(self.axis_label_size_var.get())
            xtick_label_size = float(self.xtick_label_size_var.get())
            ytick_label_size = float(self.ytick_label_size_var.get())
            legend_size = float(self.legend_size_var.get())
        except ValueError as exc:
            raise ValueError("Text sizes must be numbers.") from exc

        font_family = self.font_family_var.get().strip() or "Arial"

        threshold = None
        if self.use_threshold_var.get():
            threshold_text = self.threshold_var.get().strip()
            if not threshold_text:
                raise ValueError("Threshold is enabled, but no threshold value was entered.")
            try:
                threshold = float(threshold_text)
            except ValueError as exc:
                raise ValueError("Threshold must be a number.") from exc

        control_mode = self.control_mode_var.get()
        control_row_indices = None
        control_sample_names = None
        control_column = None
        control_value = None
        control_query = None

        if control_mode == "Row numbers":
            text = self.control_entry_1_var.get().strip()
            if not text:
                raise ValueError("Enter at least one control row number.")
            try:
                control_row_indices = self._parse_row_numbers(text)
            except ValueError as exc:
                raise ValueError("Control row numbers must be integers or ranges like 1-3,5.") from exc
        elif control_mode == "Sample names":
            control_sample_names = [item.strip() for item in self.control_entry_1_var.get().split(",") if item.strip()]
            if not control_sample_names:
                raise ValueError("Enter at least one control sample name.")
        elif control_mode == "Column = value":
            control_column = self.control_entry_1_var.get().strip()
            control_value = self.control_entry_2_var.get().strip()
            if not control_column or not control_value:
                raise ValueError("Fill both control column and control value.")
        elif control_mode == "Query":
            control_query = self.control_entry_1_var.get().strip()
            if not control_query:
                raise ValueError("Enter a control query / mask.")

        order_path = self.order_path_var.get().strip() or None
        order_column = self.order_column_var.get().strip() or None
        output_prefix = self.output_prefix_var.get().strip() or None
        title = self.title_var.get().strip() or "Sample activity"
        ylabel = self.ylabel_var.get().strip() or None

        return {
            "input_path": Path(input_path),
            "sample_column": sample_column,
            "replicate_columns": replicate_columns,
            "order_file": Path(order_path) if order_path else None,
            "order_column": order_column,
            "threshold": threshold,
            "threshold_mode": self.threshold_mode_var.get(),
            "control_row_indices": control_row_indices,
            "control_sample_names": control_sample_names,
            "control_column": control_column,
            "control_value": control_value,
            "control_query": control_query,
            "exclude_control_from_plot": self.exclude_control_var.get(),
            "title": title,
            "ylabel": ylabel,
            "figure_size": figure_size,
            "rotate_labels": rotate_labels,
            "plot_style": {
                "font_family": font_family,
                "title_size": title_size,
                "axis_label_size": axis_label_size,
                "xtick_label_size": xtick_label_size,
                "ytick_label_size": ytick_label_size,
                "legend_size": legend_size,
            },
            "output_prefix": Path(output_prefix) if output_prefix else None,
            "export_table": self.export_table_var.get(),
            "export_png": self.export_png_var.get(),
            "export_svg": self.export_svg_var.get(),
        }

    def log(self, message: str) -> None:
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.root.update_idletasks()

    def clear_log(self) -> None:
        self.log_text.delete("1.0", "end")
        self.log("Log cleared.")

    def show_figure(self, fig) -> None:
        if self.preview_hint and self.preview_hint.winfo_exists():
            self.preview_hint.pack_forget()

        if self.canvas_widget is not None:
            self.canvas_widget.get_tk_widget().destroy()
            self.canvas_widget = None
        if self.toolbar is not None:
            self.toolbar.destroy()
            self.toolbar = None
        if self.current_figure is not None:
            plt.close(self.current_figure)

        self.current_figure = fig
        canvas = FigureCanvasTkAgg(fig, master=self.preview_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

        toolbar = NavigationToolbar2Tk(canvas, self.preview_frame, pack_toolbar=False)
        toolbar.update()
        toolbar.pack(fill="x")

        self.canvas_widget = canvas
        self.toolbar = toolbar

    def _default_output_prefix(self, input_path: Path) -> Path:
        if self.output_prefix_var.get().strip():
            return Path(self.output_prefix_var.get().strip())
        output_dir = self._default_output_dir()
        return output_dir / input_path.stem

    def _set_preview_summary(self, processed_df, value_column: str, control_mean: float | None) -> None:
        lines = [
            f"Rows plotted: {len(processed_df)}",
            f"Value column: {value_column}",
        ]
        if control_mean is not None:
            lines.append(f"Control mean: {control_mean:.4f}")
        if "activity_class" in processed_df.columns:
            counts = processed_df["activity_class"].value_counts().to_dict()
            lines.append(f"Classes: {counts}")
        self.preview_summary_var.set("\n".join(lines))

    def preview_figure(self) -> None:
        try:
            settings = self.collect_settings()
            self._save_state()
            self.update_status("Previewing figure...")
            self.log("Previewing figure...")

            processed_df, fig, value_column, control_mean = self.core.run_pipeline(
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
            )
            self.show_figure(fig)
            self._set_preview_summary(processed_df, value_column, control_mean)
            self.log(f"Rows plotted: {len(processed_df)}")
            self.log(f"Value column used: {value_column}")
            if control_mean is not None:
                self.log(f"Control mean: {control_mean:.4f}")
            self.update_status("Preview ready.")
        except Exception as exc:
            self.update_status("Preview failed.")
            messagebox.showerror("Preview failed", str(exc))
            self.log(f"ERROR: {exc}")

    def run_and_export(self) -> None:
        try:
            settings = self.collect_settings()
            if not any([settings["export_table"], settings["export_png"], settings["export_svg"]]):
                raise ValueError("Choose at least one export format.")

            self._save_state()
            self.update_status("Running analysis and exporting files...")
            self.log("Running analysis and exporting files...")

            processed_df, fig, value_column, control_mean = self.core.run_pipeline(
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
            )
            self.show_figure(fig)
            self._set_preview_summary(processed_df, value_column, control_mean)

            output_prefix = settings["output_prefix"] or self._default_output_prefix(settings["input_path"])
            saved_paths = self.core.save_outputs(
                fig=fig,
                df=processed_df,
                output_prefix=output_prefix,
                export_table=settings["export_table"],
                export_png=settings["export_png"],
                export_svg=settings["export_svg"],
            )

            for path in saved_paths:
                self.log(f"Saved: {path}")

            self.update_status("Export finished.")
            messagebox.showinfo("Done", "Export finished successfully.")
        except Exception as exc:
            self.update_status("Export failed.")
            messagebox.showerror("Run failed", str(exc))
            self.log(f"ERROR: {exc}")

    def reset_form(self) -> None:
        if not messagebox.askyesno("Reset form", "Clear the current GUI settings?"):
            return
        self.input_path_var.set("")
        self.order_path_var.set("")
        self.sample_column_var.set("")
        self.order_column_var.set("")
        self.control_mode_var.set("None")
        self.control_entry_1_var.set("")
        self.control_entry_2_var.set("")
        self.use_threshold_var.set(False)
        self.threshold_var.set("")
        self.threshold_mode_var.set("ge")
        self.exclude_control_var.set(True)
        self.export_table_var.set(True)
        self.export_png_var.set(True)
        self.export_svg_var.set(True)
        self.output_prefix_var.set(str(self.default_output_prefix))
        self.title_var.set("Sample activity")
        self.ylabel_var.set("")
        self.figure_width_var.set("16")
        self.figure_height_var.set("6")
        self.rotate_var.set("45")
        self.font_family_var.set("Arial")
        self.title_size_var.set("16")
        self.axis_label_size_var.set("20")
        self.xtick_label_size_var.set("8")
        self.ytick_label_size_var.set("16")
        self.legend_size_var.set("12")
        self.input_columns = []
        self.order_columns = []
        self.sample_combo["values"] = []
        self.order_combo["values"] = []
        self.clear_replicate_selection()
        self.replicate_listbox.delete(0, tk.END)
        self.update_threshold_state()
        self.update_control_inputs()
        self.preview_summary_var.set("Load a table, choose columns, and preview the figure here.")
        self.clear_log()
        self._save_state()
        self.update_status("Form reset.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch the GUI for plant bioactivity plotting.")
    parser.add_argument("--input", help="Optional input CSV/XLSX file to pre-load.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    script_dir = Path(__file__).resolve().parent
    core_path = script_dir / "p_00_00_plant_bioactivity_core.py"

    if not core_path.exists():
        raise FileNotFoundError(
            "Could not find p_00_00_plant_bioactivity_core.py in the same folder as the GUI script."
        )

    core_module = load_core_module(core_path)
    root = tk.Tk()
    app = BioactivityGUI(root, core_module, initial_input=args.input)
    try:
        root.mainloop()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
