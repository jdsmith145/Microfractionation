from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tkinter as tk
import webbrowser
from pathlib import Path
from tkinter import filedialog, messagebox

try:
    import customtkinter as ctk
except ImportError as exc:  # pragma: no cover - user-facing dependency guard
    raise SystemExit(
        "customtkinter is required for the launcher. Install the project environment first."
    ) from exc

from support.gui_help_popover import HelpPopoverController


APP_TITLE = "Microfractionation workflow"
CONFIG_NAME = "microfractionation_project_config.json"
MZMINE_RELEASE_URL = "https://github.com/mzmine/mzmine/releases/tag/v4.7.8"
MZMINE_PORTABLE_DIR = "mzmine_Windows_portable_4.7.8"
MZMINE_CONSOLE_RELATIVE = f"{MZMINE_PORTABLE_DIR}/mzmine_console.exe"

COLORS = {
    "bg": "#17191d",
    "surface": "#20242a",
    "panel": "#20242a",
    "panel2": "#252a31",
    "card": "#252a31",
    "card_alt": "#2d333c",
    "entry": "#191d22",
    "line": "#3d4652",
    "border": "#3d4652",
    "text": "#f3f6fa",
    "muted": "#aab4c0",
    "blue": "#2563eb",
    "accent": "#2563eb",
    "accent_hover": "#3b82f6",
    "green": "#2f8f5b",
    "success": "#2f8f5b",
    "red": "#b54848",
    "danger": "#b54848",
    "amber": "#b7791f",
}

MODULES = [
    (
        "batch_setup",
        "Batch setup",
        "01_mzmine_pipeline/p_01_01_batch_setup_gui.py",
        "Create configured complete and fraction MZmine batch files.",
    ),
    (
        "mzmine_runner",
        "MZmine runner",
        "01_mzmine_pipeline/p_01_02_mzmine_runner_gui.py",
        "Run configured MZmine batches with mzmine_console.exe.",
    ),
    (
        "feature_filtering",
        "Feature filtering",
        "01_mzmine_pipeline/p_01_03_feature_filter_gui.py",
        "Match fraction CSVs to the complete feature table.",
    ),
    (
        "two_sided_plot",
        "Two-sided plot",
        "02_two_sided_plot/p_02_01_two_sided_plot_gui.py",
        "Chromatogram, filtered fraction features, and activity overlay.",
    ),
    (
        "wikidata",
        "Wikidata",
        "03_wikidata/p_03_01_wikidata_gui.py",
        "Formula/taxon search and molecule visualization.",
    ),
    (
        "fraction_predictor",
        "Fraction predictor",
        "04_fraction_predictor/p_04_01_fraction_predictor_gui.py",
        "Map HRMS features to HPLC fractions and activity.",
    ),
]

MODULE_HINTS = {
    "batch_setup": "First MZmine step. It writes HPLC sample injection files, HPLC blank injection files, export folders, and fraction export names into MZmine batch templates. It does not run MZmine yet.",
    "mzmine_runner": "Second MZmine step. It runs the configured .mzbatch files through mzmine_console.exe and checks that MZmine created the complete feature table and fraction CSV files.",
    "feature_filtering": "Third MZmine step. It matches fraction CSV features back to the complete HPLC feature table and writes the filtered HPLC feature table used by plotting and fraction prediction.",
    "two_sided_plot": "Use this after feature filtering to combine one chromatogram, filtered fraction features, and optional activity/intensity data into a two-sided publication-style figure.",
    "wikidata": "Use this for dereplication support. It searches formula and taxon combinations in Wikidata, then lets you inspect molecules and structures in the visualization tab.",
    "fraction_predictor": "Use this to connect HRMS/UPLC features to collected HPLC fractions, append annotations, combine fraction activity/intensity, and create the final prioritization table.",
}

SHARED_FIELDS = [
    ("main_feature_table", "Main feature table", "file", ["csv"]),
    ("sirius_annotation_table", "SIRIUS/CANOPUS annotations", "file", ["xlsx", "csv", "tsv"]),
    ("activity_root", "Activity folder", "folder", None),
    ("raw_mzml_root", "HPLC mzML folder", "folder", None),
]

DEFAULT_SHARED_INPUTS = {
    "activity_root": "data/activity",
    "raw_mzml_root": "data/hplc_mzml",
}

DEFAULT_BROWSE_STARTS = {
    "main_feature_table": "data",
    "sirius_annotation_table": "data/annotations",
    "activity_root": "data/activity",
    "raw_mzml_root": "data/hplc_mzml",
}

SHARED_EXAMPLES = {
    "main_feature_table": "",
    "sirius_annotation_table": "",
    "activity_root": "",
    "raw_mzml_root": "",
}

FRACTION_FIELDS = [
    ("start_time", "Fraction collection starts at (min)", "2.0"),
    ("end_time", "Fraction collection ends at (min)", "38.0"),
    ("n_fractions", "Number of collected fractions", "96"),
]

SHARED_FIELD_HINTS = {
    "main_feature_table": "The main HRMS/UPLC feature table. It should contain feature IDs, m/z, retention time, and sample peak-area columns.",
    "sirius_annotation_table": "The SIRIUS/CANOPUS result table. It should contain an ID column such as mappingFeatureId and annotation columns such as molecularFormula or NPC#class.",
    "activity_root": "Folder that contains activity/intensity data for individual samples. Each sample can point to its own subfolder.",
    "raw_mzml_root": "Folder that contains the raw HPLC-MS mzML files. In the Samples section, choose the exact HPLC sample injections and HPLC blank injections for each extract.",
}

FRACTION_FIELD_HINTS = {
    "start_time": "HPLC time, in minutes, when fraction collection begins. Features predicted before this time are not assigned to collected fractions.",
    "end_time": "HPLC time, in minutes, when fraction collection ends. Together with the start time and number of fractions, this defines the fraction windows.",
    "n_fractions": "Total number of collected fractions, for example 96 for one 96-well plate.",
}


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def default_config_path() -> Path:
    return project_root() / CONFIG_NAME


def resolve_project_path(value: str | Path) -> Path:
    path = Path(str(value))
    if path.is_absolute():
        return path
    return project_root() / path


def relative_to_project(path: str | Path) -> str:
    resolved = Path(str(path))
    try:
        return str(resolved.resolve().relative_to(project_root().resolve())).replace("\\", "/")
    except ValueError:
        return str(resolved)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def file_safe_name(text: str) -> str:
    """Return a readable name that is safe for folders and output filenames."""

    value = re.sub(r"[^A-Za-z0-9]+", "_", str(text).strip().lower()).strip("_")
    return value or "sample"


class Launcher(ctk.CTk):
    def __init__(self) -> None:
        super().__init__()
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        self.title(APP_TITLE)
        self.geometry("1500x900")
        self.minsize(1120, 720)
        self.protocol("WM_DELETE_WINDOW", self.destroy)

        self.config_path = default_config_path()
        self.config_data = self._load_or_create_config()
        self.entries: dict[str, ctk.CTkEntry] = {}
        self.sample_entries: dict[str, ctk.CTkEntry] = {}
        self.sample_box: ctk.CTkComboBox | None = None
        self.sample_summary: ctk.CTkLabel | None = None
        self.sample_list_frame: ctk.CTkFrame | None = None
        self.sample_notice: ctk.CTkLabel | None = None
        self._sample_notice_after_id: str | None = None
        self.module_buttons: dict[str, ctk.CTkButton] = {}
        self.module_processes: dict[str, subprocess.Popen] = {}
        self.status_text = ctk.StringVar(value="Ready.")
        self.help_popovers = HelpPopoverController(self, ctk, COLORS, ("Segoe UI", 11))

        self._build_ui()
        self.validate_project(silent=True)

    def _load_or_create_config(self) -> dict:
        if self.config_path.exists():
            config = read_json(self.config_path)
        else:
            config = {
                "schema_version": 1,
                "project_root": ".",
                "data_root": "data",
                "output_root": "output",
                "shared_inputs": {},
                "fraction_collection": {
                    "start_time": 2.0,
                    "end_time": 38.0,
                    "n_fractions": 96,
                    "first_fraction_number": 1,
                },
                "mzmine": {},
                "modules": {},
                "samples": [],
            }
        shared = config.setdefault("shared_inputs", {})
        for key, value in DEFAULT_SHARED_INPUTS.items():
            if not str(shared.get(key, "")).strip():
                shared[key] = value
        config.setdefault("fraction_collection", {}).setdefault("first_fraction_number", 1)
        mzmine = config.setdefault("mzmine", {})
        if not str(mzmine.get("portable_console", "")).strip():
            mzmine["portable_console"] = MZMINE_CONSOLE_RELATIVE
        config.setdefault("modules", {})
        config.setdefault("samples", [])
        return config

    def _build_ui(self) -> None:
        self.configure(fg_color=COLORS["bg"])
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(1, weight=1)

        header = ctk.CTkFrame(self, fg_color="transparent")
        header.grid(row=0, column=0, columnspan=2, sticky="ew", padx=24, pady=(20, 10))
        header.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(
            header,
            text="Microfractionation workflow",
            font=ctk.CTkFont(family="Segoe UI", size=30, weight="bold"),
            text_color=COLORS["text"],
        ).grid(row=0, column=0, sticky="w")
        ctk.CTkLabel(
            header,
            text="Load shared files once, validate the project structure, then open the focused workflow step.",
            text_color=COLORS["muted"],
            font=ctk.CTkFont(family="Segoe UI", size=14),
        ).grid(row=1, column=0, sticky="w", pady=(6, 0))
        for index, (label, command) in enumerate(
            [("Validate", self.validate_project), ("Save config", self.save_config), ("Load config", self.load_config_dialog)]
        ):
            self._button(header, label, command, width=130, primary=(label == "Validate")).grid(
                row=0, column=index + 1, rowspan=2, padx=(10, 0), sticky="e"
            )

        sidebar = ctk.CTkFrame(self, fg_color=COLORS["surface"], corner_radius=14)
        sidebar.grid(row=1, column=0, sticky="nsw", padx=(24, 10), pady=(0, 16))
        ctk.CTkLabel(
            sidebar,
            text="Workflow steps",
            font=ctk.CTkFont(family="Segoe UI", size=18, weight="bold"),
            text_color=COLORS["text"],
        ).pack(anchor="w", padx=18, pady=(18, 10))
        for key, label, _script, _note in MODULES:
            row = ctk.CTkFrame(sidebar, fg_color="transparent")
            row.pack(fill="x", padx=16, pady=5)
            row.grid_columnconfigure(0, weight=1)
            button = self._button(
                row,
                label,
                lambda module_key=key: self.launch_module(module_key),
                width=206,
                height=46,
                anchor="w",
            )
            button.grid(row=0, column=0, sticky="ew")
            self._help(row, MODULE_HINTS.get(key, _note)).grid(row=0, column=1, sticky="e", padx=(8, 0))
            self.module_buttons[key] = button
        ctk.CTkLabel(
            sidebar,
            text="MZmine steps are split so each screen has one job: prepare batches, run batches, then filter features.",
            wraplength=220,
            justify="left",
            text_color=COLORS["muted"],
        ).pack(anchor="w", padx=18, pady=(18, 16))

        body = ctk.CTkScrollableFrame(self, fg_color=COLORS["surface"], corner_radius=14)
        body.grid(row=1, column=1, sticky="nsew", padx=(0, 24), pady=(0, 16))
        body.grid_columnconfigure(0, weight=1)

        self._build_shared_inputs(body)
        self._build_sample_panel(body)
        self._build_validation_panel(body)
        self._build_workflow_map(body)

        footer = ctk.CTkFrame(self, fg_color=COLORS["surface"], corner_radius=0)
        footer.grid(row=2, column=0, columnspan=2, sticky="ew")
        ctk.CTkLabel(footer, textvariable=self.status_text, text_color=COLORS["muted"], anchor="w").pack(
            fill="x", padx=22, pady=8
        )

    def _button(self, parent, text: str, command, *, width: int = 110, height: int = 36, primary: bool = False, danger: bool = False, anchor: str = "center"):
        color = COLORS["danger"] if danger else COLORS["accent"] if primary else COLORS["card_alt"]
        hover = "#9e3b3b" if danger else COLORS["accent_hover"] if primary else "#39414c"
        return ctk.CTkButton(
            parent,
            text=text,
            command=command,
            width=width,
            height=height,
            corner_radius=9,
            fg_color=color,
            hover_color=hover,
            text_color=COLORS["text"],
            anchor=anchor,
        )

    def _entry(self, parent, value: str = "") -> ctk.CTkEntry:
        entry = ctk.CTkEntry(
            parent,
            fg_color=COLORS["entry"],
            border_color=COLORS["border"],
            text_color=COLORS["text"],
            height=35,
            corner_radius=8,
        )
        entry.insert(0, value)
        return entry

    def _help(self, parent, text: str):
        return self.help_popovers.create_bubble(parent, text)

    def _card(self, parent, title: str, subtitle: str = ""):
        card = ctk.CTkFrame(
            parent,
            fg_color=COLORS["card"],
            border_width=1,
            border_color=COLORS["border"],
            corner_radius=16,
        )
        card.grid(sticky="ew", padx=18, pady=10)
        card.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(
            card,
            text=title,
            font=ctk.CTkFont(family="Segoe UI", size=17, weight="bold"),
            text_color=COLORS["text"],
        ).grid(row=0, column=0, columnspan=4, sticky="w", padx=18, pady=(14, 4))
        if subtitle:
            ctk.CTkLabel(
                card,
                text=subtitle,
                wraplength=1200,
                justify="left",
                text_color=COLORS["muted"],
            ).grid(row=1, column=0, columnspan=4, sticky="w", padx=18, pady=(0, 12))
        return card

    def _build_shared_inputs(self, parent) -> None:
        card = self._card(
            parent,
            "Shared project inputs",
            "Choose files and folders that are reused across the workflow. The focused tools inherit these paths so the same file is not selected repeatedly.",
        )
        shared = self.config_data.setdefault("shared_inputs", {})
        row = 2
        mzmine_setup = ctk.CTkFrame(card, fg_color=COLORS["card_alt"], corner_radius=12)
        mzmine_setup.grid(row=row, column=0, columnspan=4, sticky="ew", padx=18, pady=(4, 12))
        mzmine_setup.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(
            mzmine_setup,
            text="MZmine setup",
            font=ctk.CTkFont(family="Segoe UI", size=15, weight="bold"),
            text_color=COLORS["text"],
            anchor="w",
        ).grid(row=0, column=0, sticky="ew", padx=14, pady=(12, 2))
        ctk.CTkLabel(
            mzmine_setup,
            text=(
                "Before running the MZmine steps, download the MZmine 4.7.8 portable release from GitHub, "
                f"unzip it into the microfractionation folder, and keep the folder name {MZMINE_PORTABLE_DIR}. "
                f"The launcher expects {MZMINE_CONSOLE_RELATIVE}."
            ),
            text_color=COLORS["muted"],
            wraplength=950,
            justify="left",
            anchor="w",
        ).grid(row=1, column=0, sticky="ew", padx=14, pady=(0, 10))
        self._button(
            mzmine_setup,
            "Open MZmine 4.7.8 download page",
            self.open_mzmine_download_page,
            width=240,
        ).grid(row=0, column=1, rowspan=2, sticky="e", padx=(8, 8), pady=12)
        self._button(
            mzmine_setup,
            "Check MZmine",
            self.check_mzmine_setup,
            primary=True,
            width=130,
        ).grid(row=0, column=2, rowspan=2, sticky="e", padx=(0, 14), pady=12)
        row += 1

        for key, label, kind, file_types in SHARED_FIELDS:
            label_frame = ctk.CTkFrame(card, fg_color="transparent")
            label_frame.grid(row=row, column=0, sticky="ew", padx=18, pady=6)
            ctk.CTkLabel(label_frame, text=label, text_color=COLORS["muted"]).pack(side="left")
            self._help(label_frame, SHARED_FIELD_HINTS[key]).pack(side="left", padx=(8, 0))
            entry = self._entry(card, str(shared.get(key, "")))
            entry.grid(row=row, column=1, sticky="ew", padx=(0, 10), pady=6)
            self.entries[key] = entry
            self._button(
                card,
                "Browse",
                lambda field_key=key, field_kind=kind, types=file_types: self.browse_path(
                    field_key, field_kind, types
                ),
                width=100,
            ).grid(row=row, column=2, sticky="e", padx=(0, 8), pady=6)
            self._button(
                card,
                "Example",
                lambda field_key=key: self.open_shared_example(field_key),
                width=82,
            ).grid(row=row, column=3, sticky="e", padx=(0, 18), pady=6)
            row += 1

        label_frame = ctk.CTkFrame(card, fg_color="transparent")
        label_frame.grid(row=row, column=0, sticky="ew", padx=18, pady=6)
        ctk.CTkLabel(label_frame, text="MZmine console", text_color=COLORS["muted"]).pack(side="left")
        self._help(
            label_frame,
            "Path to mzmine_console.exe. The recommended setup is the MZmine 4.7.8 portable folder unzipped directly inside the microfractionation folder.",
        ).pack(side="left", padx=(8, 0))
        mz_entry = self._entry(card, str(self.config_data.setdefault("mzmine", {}).get("portable_console", MZMINE_CONSOLE_RELATIVE)))
        mz_entry.grid(row=row, column=1, sticky="ew", padx=(0, 10), pady=6)
        self.entries["mzmine_console"] = mz_entry
        self._button(
            card,
            "Browse",
            lambda: self.browse_path("mzmine_console", "file", ["exe"]),
            width=100,
        ).grid(row=row, column=2, sticky="e", padx=(0, 18), pady=6)

        row += 1
        ctk.CTkLabel(
            card,
            text="Fraction collection parameters",
            font=ctk.CTkFont(family="Segoe UI", size=15, weight="bold"),
            text_color=COLORS["text"],
        ).grid(row=row, column=0, columnspan=3, sticky="w", padx=18, pady=(16, 4))
        self._help(
            card,
            "These values describe the HPLC fraction collection window once. Batch setup, two-sided plotting, and fraction prediction use the same timing.",
        ).grid(row=row, column=2, sticky="e", padx=(0, 18), pady=(16, 4))
        row += 1
        fraction_settings = self.config_data.setdefault("fraction_collection", {})
        for key, label, default in FRACTION_FIELDS:
            label_frame = ctk.CTkFrame(card, fg_color="transparent")
            label_frame.grid(row=row, column=0, sticky="ew", padx=18, pady=6)
            ctk.CTkLabel(label_frame, text=label, text_color=COLORS["muted"]).pack(side="left")
            self._help(label_frame, FRACTION_FIELD_HINTS[key]).pack(side="left", padx=(8, 0))
            entry = self._entry(card, str(fraction_settings.get(key, default)))
            entry.grid(row=row, column=1, sticky="ew", padx=(0, 10), pady=6)
            self.entries[f"fraction_{key}"] = entry
            row += 1

    def _build_workflow_map(self, parent) -> None:
        card = self._card(
            parent,
            "Workflow map",
            "This static map explains what each workflow step consumes and produces. It is a guide, not a workflow engine.",
        )
        flow = ctk.CTkFrame(card, fg_color="transparent")
        flow.grid(row=2, column=0, columnspan=4, sticky="ew", padx=18, pady=(4, 12))
        for column in range(9):
            flow.grid_columnconfigure(column, weight=1 if column % 2 == 0 else 0)

        self._workflow_node(
            flow,
            row=0,
            column=0,
            title="Raw HPLC-MS mzML",
            body="sample and blank files\nper extract",
            color="#253142",
        )
        self._workflow_connector(flow, row=0, column=1)
        self._workflow_node(
            flow,
            row=0,
            column=2,
            title="Batch setup",
            body="configured complete and\nfraction .mzbatch files",
            color="#27364a",
        )
        self._workflow_connector(flow, row=0, column=3)
        self._workflow_node(
            flow,
            row=0,
            column=4,
            title="MZmine runner",
            body="complete feature table\nand fraction CSV files",
            color="#283b50",
        )
        self._workflow_connector(flow, row=0, column=5)
        self._workflow_node(
            flow,
            row=0,
            column=6,
            title="Feature filtering",
            body="filtered HPLC\nfeature table",
            color="#2a4052",
        )
        self._workflow_connector(flow, row=0, column=7)
        self._workflow_node(
            flow,
            row=0,
            column=8,
            title="Two-sided plot",
            body="chromatogram plus\nfraction activity figure",
            color="#2b4655",
        )

        self._workflow_node(
            flow,
            row=1,
            column=0,
            title="Main feature table",
            body="row ID, m/z, RT,\nsample peak areas",
            color="#2b303d",
        )
        self._workflow_connector(flow, row=1, column=1)
        self._workflow_node(
            flow,
            row=1,
            column=2,
            title="Annotations",
            body="SIRIUS/CANOPUS\nand optional tables",
            color="#2b303d",
        )
        self._workflow_connector(flow, row=1, column=3)
        self._workflow_node(
            flow,
            row=1,
            column=4,
            title="Fraction predictor",
            body="feature table with\nfractions and activity",
            color="#31404c",
        )
        self._workflow_connector(flow, row=1, column=5)
        self._workflow_node(
            flow,
            row=1,
            column=6,
            title="Wikidata",
            body="formula/taxon search\nand structure browser",
            color="#31404c",
        )

        table = ctk.CTkFrame(card, fg_color="#1c222b", border_width=1, border_color=COLORS["line"], corner_radius=8)
        table.grid(row=3, column=0, columnspan=4, sticky="ew", padx=18, pady=(0, 18))
        table.grid_columnconfigure(1, weight=1)
        table.grid_columnconfigure(2, weight=1)
        table.grid_columnconfigure(3, weight=1)
        for column, label in enumerate(["Step", "Main input", "Main output", "Used by"]):
            ctk.CTkLabel(
                table,
                text=label,
                text_color=COLORS["text"],
                font=ctk.CTkFont(size=13, weight="bold"),
                anchor="w",
            ).grid(row=0, column=column, sticky="ew", padx=12, pady=(10, 6))

        rows = [
            (
                "Batch setup",
                "data/hplc_mzml/<sample>/",
                "output/01_mzmine_pipeline/<sample>/configured_batches/",
                "MZmine runner",
            ),
            (
                "MZmine runner",
                "configured .mzbatch files",
                "complete_feature_table/ and fraction_feature_tables/",
                "Feature filtering",
            ),
            (
                "Feature filtering",
                "complete feature table + frac_*.csv",
                "<sample>_filtered_feature_table.csv",
                "Two-sided plot, fraction predictor",
            ),
            (
                "Two-sided plot",
                "filtered HPLC table + mzML + activity files",
                "publication figure SVG/PNG and plotted data CSV",
                "manual review/publication",
            ),
            (
                "Fraction predictor",
                "main feature table + annotations + activity files",
                "final feature table with fraction/activity columns",
                "prioritization",
            ),
            (
                "Wikidata",
                "formula and taxon list",
                "species/genus/family/anywhere hit tables",
                "dereplication support",
            ),
        ]
        for row_index, row_values in enumerate(rows, start=1):
            self._workflow_io_row(table, row_index, row_values)

    def _workflow_node(self, parent, row: int, column: int, title: str, body: str, color: str) -> None:
        node = ctk.CTkFrame(parent, fg_color=color, border_width=1, border_color=COLORS["line"], corner_radius=8)
        node.grid(row=row, column=column, sticky="nsew", padx=4, pady=5)
        ctk.CTkLabel(
            node,
            text=title,
            text_color=COLORS["text"],
            font=ctk.CTkFont(size=13, weight="bold"),
            wraplength=145,
            justify="center",
        ).pack(fill="x", padx=8, pady=(10, 3))
        ctk.CTkLabel(
            node,
            text=body,
            text_color=COLORS["muted"],
            font=ctk.CTkFont(size=11),
            wraplength=145,
            justify="center",
        ).pack(fill="x", padx=8, pady=(0, 10))

    def _workflow_connector(self, parent, row: int, column: int) -> None:
        ctk.CTkLabel(
            parent,
            text="->",
            text_color=COLORS["muted"],
            font=ctk.CTkFont(size=18, weight="bold"),
        ).grid(row=row, column=column, sticky="nsew", padx=2, pady=5)

    def _workflow_io_row(self, parent, row: int, values: tuple[str, str, str, str]) -> None:
        fill = "#202832" if row % 2 else "#1b222b"
        for column, value in enumerate(values):
            cell = ctk.CTkFrame(parent, fg_color=fill, corner_radius=0)
            cell.grid(row=row, column=column, sticky="nsew", padx=0, pady=0)
            ctk.CTkLabel(
                cell,
                text=value,
                text_color=COLORS["muted"] if column else COLORS["text"],
                wraplength=340 if column else 180,
                justify="left",
                anchor="w",
            ).pack(fill="x", padx=12, pady=7)

    def _build_sample_panel(self, parent) -> None:
        card = self._card(
            parent,
            "Samples",
            "Add one record for each plant/extract/run. Downstream tools inherit these sample-specific files, so users do not select the same mzML or activity folder repeatedly.",
        )
        names = self.sample_names()
        fields = [
            ("display_name", "Sample display name", "Name shown in the GUI, for example Ruta corsica 107.", "Ruta corsica 107"),
            ("file_safe_name", "File-safe sample name", "Short lowercase name used for output folders and filenames, for example ruta_corsica_107. It updates automatically from the display name.", "ruta_corsica_107"),
            ("sample_mzml_files", "HPLC sample injection files", "Raw HPLC-MS mzML files measured for this extract. Select all replicate/sample injections that should be processed together.", ""),
            ("blank_mzml_files", "HPLC blank injection files", "Raw HPLC-MS mzML blank files for this extract or sequence. These are used by MZmine in the same batch as the sample injections.", ""),
            ("activity_subfolder", "Activity/intensity folder", "Folder with this sample's fraction activity or plate-reader files. Leave empty if no activity data exists yet.", ""),
            ("plot_mzml_file", "Representative chromatogram for plots", "One HPLC mzML file used when a figure needs a single chromatogram trace. Usually choose one of the HPLC sample injection files above.", ""),
            ("sample_peak_area_column", "Peak-area column", "Column in the main feature table that belongs to this sample. Script 04 uses it for feature filtering and plant cards.", ""),
        ]
        row = 2
        for key, label, hint, placeholder in fields:
            label_frame = ctk.CTkFrame(card, fg_color="transparent")
            label_frame.grid(row=row, column=0, sticky="ew", padx=18, pady=5)
            ctk.CTkLabel(label_frame, text=label, text_color=COLORS["muted"]).pack(side="left")
            self._help(label_frame, hint).pack(side="left", padx=(8, 0))
            entry = self._entry(card, "")
            entry.configure(placeholder_text=placeholder)
            entry.grid(row=row, column=1, sticky="ew", padx=(0, 10), pady=5)
            self.sample_entries[key] = entry
            if key == "display_name":
                entry.bind("<KeyRelease>", lambda _event: self.update_file_safe_name_from_display())
            elif key == "file_safe_name":
                entry.configure(state="disabled")
            if key in {"sample_mzml_files", "blank_mzml_files"}:
                self._button(
                    card,
                    "Browse",
                    lambda field_key=key: self.browse_sample_files(field_key),
                    width=100,
                ).grid(row=row, column=2, sticky="e", padx=(0, 8), pady=5)
            elif key in {"activity_subfolder"}:
                self._button(
                    card,
                    "Browse",
                    lambda field_key=key: self.browse_sample_folder(field_key),
                    width=100,
                ).grid(row=row, column=2, sticky="e", padx=(0, 8), pady=5)
            elif key in {"plot_mzml_file"}:
                self._button(
                    card,
                    "Choose from HPLC files",
                    self.choose_plot_file_from_sample_files,
                    width=165,
                ).grid(row=row, column=2, sticky="e", padx=(0, 8), pady=5)
                self._button(
                    card,
                    "Browse",
                    lambda field_key=key: self.browse_sample_single_file(field_key),
                    width=82,
                ).grid(row=row, column=3, sticky="e", padx=(0, 18), pady=5)
            elif key == "sample_peak_area_column":
                self._button(
                    card,
                    "Select column",
                    self.choose_peak_area_column,
                    width=120,
                ).grid(row=row, column=2, sticky="e", padx=(0, 8), pady=5)
            row += 1

        self._button(card, "Add / update sample", self.add_or_update_sample, primary=True, width=160).grid(
            row=row, column=1, sticky="w", padx=(0, 10), pady=(10, 10)
        )
        self._button(card, "New blank form", self.clear_sample_form, width=135).grid(row=row, column=2, sticky="e", padx=(0, 8), pady=(10, 10))
        self.sample_notice = ctk.CTkLabel(card, text="", text_color=COLORS["green"], anchor="w")
        self.sample_notice.grid(row=row + 1, column=0, columnspan=4, sticky="ew", padx=18, pady=(0, 4))
        self.sample_summary = ctk.CTkLabel(card, text="", justify="left", anchor="w", text_color=COLORS["muted"])
        self.sample_summary.grid(row=row + 2, column=0, columnspan=4, sticky="ew", padx=18, pady=(0, 16))

        edit_label = ctk.CTkFrame(card, fg_color="transparent")
        edit_label.grid(row=row + 3, column=0, sticky="ew", padx=18, pady=(4, 6))
        ctk.CTkLabel(edit_label, text="Edit an already-added sample", text_color=COLORS["muted"]).pack(side="left")
        self._help(edit_label, "Optional. Use this only when you want to load an existing sample record back into the form for editing. To add a new sample, fill the fields above and click Add / update sample.").pack(side="left", padx=(8, 0))
        self.sample_box = ctk.CTkComboBox(
            card,
            values=names or ["No samples configured"],
            fg_color=COLORS["entry"],
            border_color=COLORS["border"],
            button_color=COLORS["accent"],
            button_hover_color=COLORS["accent_hover"],
            dropdown_fg_color=COLORS["card"],
            dropdown_hover_color=COLORS["card_alt"],
            text_color=COLORS["text"],
            dropdown_text_color=COLORS["text"],
            command=lambda _: self.load_selected_sample_into_form(),
        )
        self.sample_box.grid(row=row + 3, column=1, sticky="ew", padx=(0, 10), pady=(4, 6))
        self._button(card, "Remove sample", self.remove_selected_sample, width=120, danger=True).grid(row=row + 3, column=2, sticky="e", padx=(0, 8), pady=(4, 6))

        ctk.CTkLabel(
            card,
            text="Configured samples",
            text_color=COLORS["text"],
            font=ctk.CTkFont(family="Segoe UI", size=14, weight="bold"),
            anchor="w",
        ).grid(row=row + 4, column=0, columnspan=4, sticky="ew", padx=18, pady=(8, 6))
        self.sample_list_frame = ctk.CTkFrame(card, fg_color="transparent")
        self.sample_list_frame.grid(row=row + 5, column=0, columnspan=4, sticky="ew", padx=18, pady=(0, 16))
        self.sample_list_frame.grid_columnconfigure(0, weight=1)
        if names:
            self.sample_box.set(names[0])
            self.load_selected_sample_into_form()
        self.refresh_sample_list()

    def _build_validation_panel(self, parent) -> None:
        card = self._card(
            parent,
            "Project checks",
            "Click Validate after editing paths. Missing required files should be fixed before opening a module.",
        )
        self.validation_rows_frame = ctk.CTkFrame(card, fg_color="transparent")
        self.validation_rows_frame.grid(row=2, column=0, columnspan=4, sticky="ew", padx=18, pady=(4, 16))
        self.validation_rows_frame.grid_columnconfigure(2, weight=1)

    def browse_path(self, key: str, kind: str, file_types: list[str] | None) -> None:
        current = self.entries.get(key).get().strip() if key in self.entries else ""
        default = current or DEFAULT_BROWSE_STARTS.get(key, "")
        default_path = resolve_project_path(default) if default else project_root()
        initialdir = default_path if default_path.is_dir() else default_path.parent if default_path.parent.exists() else project_root()
        if kind == "folder":
            value = filedialog.askdirectory(initialdir=str(initialdir))
        else:
            patterns = [("All files", "*.*")]
            if file_types:
                patterns = [
                    ("Supported files", " ".join(f"*.{extension}" for extension in file_types)),
                    ("All files", "*.*"),
                ]
            value = filedialog.askopenfilename(initialdir=str(initialdir), filetypes=patterns)
        if not value:
            return
        self.entries[key].delete(0, "end")
        self.entries[key].insert(0, relative_to_project(value))
        self.update_config_from_ui()
        self.validate_project(silent=True)

    def open_shared_example(self, key: str) -> None:
        example_rel = SHARED_EXAMPLES.get(key)
        if not example_rel:
            messagebox.showinfo(APP_TITLE, "No example file is configured for this input.")
            return
        path = resolve_project_path(example_rel)
        if not path.exists():
            messagebox.showwarning(APP_TITLE, f"Example path was not found:\n{path}")
            return
        try:
            if sys.platform.startswith("win"):
                os.startfile(path)  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(path)])
            else:
                subprocess.Popen(["xdg-open", str(path)])
        except Exception as exc:
            messagebox.showerror(APP_TITLE, f"Could not open example:\n{path}\n\n{exc}")
            return
        self.status_text.set(f"Opened example: {relative_to_project(path)}")

    def update_config_from_ui(self) -> None:
        shared = self.config_data.setdefault("shared_inputs", {})
        for key, _label, _kind, _types in SHARED_FIELDS:
            if key in self.entries:
                shared[key] = self.entries[key].get().strip()
        fraction_settings = self.config_data.setdefault("fraction_collection", {})
        for key, _label, _default in FRACTION_FIELDS:
            entry_key = f"fraction_{key}"
            if entry_key in self.entries:
                text = self.entries[entry_key].get().strip()
                try:
                    fraction_settings[key] = int(float(text)) if key in {"n_fractions", "first_fraction_number"} else float(text)
                except ValueError:
                    fraction_settings[key] = text
        self.config_data.setdefault("mzmine", {})["portable_console"] = self.entries["mzmine_console"].get().strip()

    def refresh_ui_from_config(self) -> None:
        shared = self.config_data.setdefault("shared_inputs", {})
        for key, _label, _kind, _types in SHARED_FIELDS:
            self.entries[key].delete(0, "end")
            self.entries[key].insert(0, str(shared.get(key, "")))
        self.entries["mzmine_console"].delete(0, "end")
        self.entries["mzmine_console"].insert(0, str(self.config_data.setdefault("mzmine", {}).get("portable_console", MZMINE_CONSOLE_RELATIVE)))
        fraction_settings = self.config_data.setdefault("fraction_collection", {})
        for key, _label, default in FRACTION_FIELDS:
            entry_key = f"fraction_{key}"
            if entry_key in self.entries:
                self.entries[entry_key].delete(0, "end")
                self.entries[entry_key].insert(0, str(fraction_settings.get(key, default)))
        if self.sample_box:
            names = self.sample_names()
            self.sample_box.configure(values=names or ["No samples configured"])
            self.sample_box.set(names[0] if names else "No samples configured")
            self.load_selected_sample_into_form()
        self.refresh_sample_list()

    def sample_names(self) -> list[str]:
        return [
            str(sample.get("display_name") or sample.get("slug") or sample.get("file_safe_name") or "sample")
            for sample in self.config_data.get("samples", [])
        ]

    @staticmethod
    def _join_paths(values: list[str]) -> str:
        return "; ".join(str(value) for value in values if str(value).strip())

    @staticmethod
    def _split_paths(value: str) -> list[str]:
        text = str(value).replace("\n", ";")
        return [part.strip().strip('"') for part in text.split(";") if part.strip().strip('"')]

    def sample_file_safe_name(self, sample: dict | None) -> str:
        if not sample:
            return "sample"
        return file_safe_name(sample.get("file_safe_name") or sample.get("slug") or sample.get("display_name") or "sample")

    def sample_display_name(self, sample: dict | None) -> str:
        if not sample:
            return "No sample selected"
        return str(sample.get("display_name") or sample.get("file_safe_name") or sample.get("slug") or "sample")

    def sample_pipeline_root(self, sample: dict | None) -> Path:
        slug = self.sample_file_safe_name(sample)
        return resolve_project_path(sample.get("output_folder") if sample else f"output/01_mzmine_pipeline/{slug}")

    def expected_complete_batch(self, sample: dict | None) -> Path:
        slug = self.sample_file_safe_name(sample)
        return self.sample_pipeline_root(sample) / "configured_batches" / f"{slug}_configured.mzbatch"

    def expected_fraction_batch(self, sample: dict | None) -> Path:
        slug = self.sample_file_safe_name(sample)
        return self.sample_pipeline_root(sample) / "configured_batches" / f"{slug}_fraction_configured.mzbatch"

    def expected_complete_feature_table(self, sample: dict | None) -> Path:
        slug = self.sample_file_safe_name(sample)
        return self.sample_pipeline_root(sample) / "complete_feature_table" / f"{slug}_complete_feature_table.csv"

    def expected_fraction_feature_dir(self, sample: dict | None) -> Path:
        return self.sample_pipeline_root(sample) / "fraction_feature_tables"

    def expected_filtered_feature_table(self, sample: dict | None) -> Path:
        slug = self.sample_file_safe_name(sample)
        return self.sample_pipeline_root(sample) / "filtered_feature_table" / f"{slug}_filtered_feature_table.csv"

    def _show_sample_notice(self, message: str, *, kind: str = "success") -> None:
        if self.sample_notice is None:
            self.status_text.set(message)
            return
        color = COLORS["green"] if kind == "success" else COLORS["amber"] if kind == "warning" else COLORS["red"]
        self.sample_notice.configure(text=message, text_color=color)
        self.status_text.set(message)
        if self._sample_notice_after_id is not None:
            try:
                self.after_cancel(self._sample_notice_after_id)
            except Exception:
                pass
        self._sample_notice_after_id = self.after(4500, self._clear_sample_notice)

    def _clear_sample_notice(self) -> None:
        self._sample_notice_after_id = None
        if self.sample_notice is not None:
            self.sample_notice.configure(text="")

    def selected_sample(self) -> dict | None:
        if not self.sample_box:
            return None
        selected = self.sample_box.get()
        return next(
            (
                sample
                for sample in self.config_data.get("samples", [])
                if str(sample.get("display_name") or sample.get("slug") or sample.get("file_safe_name")) == selected
            ),
            None,
        )

    def select_sample_by_name(self, name: str) -> None:
        if self.sample_box:
            self.sample_box.set(name)
        self.load_selected_sample_into_form()

    def remove_sample_by_name(self, name: str) -> None:
        sample = next(
            (
                item
                for item in self.config_data.get("samples", [])
                if str(item.get("display_name") or item.get("slug") or item.get("file_safe_name")) == name
            ),
            None,
        )
        if sample is None:
            return
        label = sample.get("display_name", sample.get("file_safe_name", "sample"))
        if not messagebox.askyesno(APP_TITLE, f"Remove sample setup for {label}?"):
            return
        self.config_data["samples"] = [item for item in self.config_data.get("samples", []) if item is not sample]
        self.refresh_sample_selector()
        self.validate_project(silent=True)
        self._show_sample_notice(f"Sample {label} removed from the list.", kind="warning")

    def refresh_sample_list(self) -> None:
        if self.sample_list_frame is None:
            return
        for child in self.sample_list_frame.winfo_children():
            child.destroy()
        samples = self.config_data.get("samples", [])
        if not samples:
            ctk.CTkLabel(
                self.sample_list_frame,
                text="No samples configured yet. Fill the fields above and click Add / update sample.",
                text_color=COLORS["muted"],
                anchor="w",
                justify="left",
            ).grid(row=0, column=0, sticky="ew", pady=4)
            return
        for row_index, sample in enumerate(samples):
            display = str(sample.get("display_name") or sample.get("slug") or "sample")
            safe_name = str(sample.get("file_safe_name") or sample.get("slug") or "")
            mzml_count = len(sample.get("sample_mzml_files", []) or [])
            blank_count = len(sample.get("blank_mzml_files", []) or [])
            has_activity = "yes" if sample.get("activity_subfolder") else "no"
            item = ctk.CTkFrame(self.sample_list_frame, fg_color=COLORS["card_alt"], corner_radius=10)
            item.grid(row=row_index, column=0, sticky="ew", pady=(0, 6))
            item.grid_columnconfigure(0, weight=1)
            ctk.CTkLabel(
                item,
                text=f"{display}  |  file-safe name: {safe_name}  |  HPLC sample injections: {mzml_count}  |  HPLC blank injections: {blank_count}  |  activity folder: {has_activity}",
                text_color=COLORS["text"],
                anchor="w",
                justify="left",
                wraplength=880,
            ).grid(row=0, column=0, sticky="ew", padx=12, pady=8)
            self._button(item, "Edit", lambda name=display: self.select_sample_by_name(name), width=70).grid(row=0, column=1, padx=(8, 6), pady=6)
            self._button(item, "Remove", lambda name=display: self.remove_sample_by_name(name), width=82, danger=True).grid(row=0, column=2, padx=(0, 8), pady=6)

    def clear_sample_form(self) -> None:
        for entry in self.sample_entries.values():
            self._set_entry_text(entry, "")
        self.sample_summary.configure(text="Enter a sample record, then click Add / update sample.")

    def _set_entry_text(self, entry: ctk.CTkEntry, value: str) -> None:
        state = entry.cget("state")
        if state == "disabled":
            entry.configure(state="normal")
        entry.delete(0, "end")
        entry.insert(0, value)
        if state == "disabled":
            entry.configure(state="disabled")

    def update_file_safe_name_from_display(self) -> None:
        name = self.sample_entries.get("display_name").get().strip() if "display_name" in self.sample_entries else ""
        safe = file_safe_name(name)
        entry = self.sample_entries.get("file_safe_name")
        if entry:
            self._set_entry_text(entry, safe)

    def load_selected_sample_into_form(self) -> None:
        sample = self.selected_sample()
        if not sample:
            self.clear_sample_form()
            return
        values = {
            "display_name": sample.get("display_name", ""),
            "file_safe_name": sample.get("file_safe_name", sample.get("slug", "")),
            "sample_mzml_files": self._join_paths(sample.get("sample_mzml_files", []) or []),
            "blank_mzml_files": self._join_paths(sample.get("blank_mzml_files", []) or []),
            "activity_subfolder": sample.get("activity_subfolder", ""),
            "plot_mzml_file": sample.get("plot_mzml_file", sample.get("representative_mzml_file", "")),
            "sample_peak_area_column": sample.get("sample_peak_area_column", ""),
        }
        for key, value in values.items():
            entry = self.sample_entries.get(key)
            if entry:
                self._set_entry_text(entry, str(value))
        self.update_file_safe_name_from_display()
        self.refresh_sample_summary()

    def browse_sample_files(self, field_key: str) -> None:
        raw_root = resolve_project_path(self.config_data.setdefault("shared_inputs", {}).get("raw_mzml_root", "data/hplc_mzml"))
        paths = filedialog.askopenfilenames(
            initialdir=str(raw_root if raw_root.exists() else project_root()),
            filetypes=[("mzML files", "*.mzML *.mzml"), ("All files", "*.*")],
        )
        if paths:
            self.sample_entries[field_key].delete(0, "end")
            self.sample_entries[field_key].insert(0, self._join_paths([relative_to_project(path) for path in paths]))

    def browse_sample_single_file(self, field_key: str) -> None:
        raw_root = resolve_project_path(self.config_data.setdefault("shared_inputs", {}).get("raw_mzml_root", "data/hplc_mzml"))
        path = filedialog.askopenfilename(
            initialdir=str(raw_root if raw_root.exists() else project_root()),
            filetypes=[("mzML files", "*.mzML *.mzml"), ("All files", "*.*")],
        )
        if path:
            self.sample_entries[field_key].delete(0, "end")
            self.sample_entries[field_key].insert(0, relative_to_project(path))

    def choose_plot_file_from_sample_files(self) -> None:
        files = self._split_paths(self.sample_entries.get("sample_mzml_files").get() if "sample_mzml_files" in self.sample_entries else "")
        if not files:
            messagebox.showinfo(APP_TITLE, "First choose the HPLC sample injection files for this sample.")
            return
        if len(files) == 1:
            self._set_entry_text(self.sample_entries["plot_mzml_file"], files[0])
            self._show_sample_notice("Representative chromatogram set from the HPLC sample injection file.")
            return

        dialog = ctk.CTkToplevel(self)
        dialog.title("Choose representative chromatogram")
        dialog.geometry("760x420")
        dialog.minsize(620, 340)
        dialog.configure(fg_color=COLORS["bg"])
        dialog.transient(self)
        dialog.grab_set()
        dialog.grid_columnconfigure(0, weight=1)
        dialog.grid_rowconfigure(1, weight=1)
        ctk.CTkLabel(
            dialog,
            text="Choose one HPLC sample injection for figure chromatograms.",
            text_color=COLORS["text"],
            font=ctk.CTkFont(family="Segoe UI", size=16, weight="bold"),
            anchor="w",
        ).grid(row=0, column=0, sticky="ew", padx=18, pady=(18, 8))
        frame = ctk.CTkFrame(dialog, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, corner_radius=10)
        frame.grid(row=1, column=0, sticky="nsew", padx=18, pady=(0, 12))
        frame.grid_columnconfigure(0, weight=1)
        frame.grid_rowconfigure(0, weight=1)
        listbox = tk.Listbox(
            frame,
            bg=COLORS["entry"],
            fg=COLORS["text"],
            selectbackground=COLORS["accent"],
            selectforeground="white",
            relief="flat",
            highlightthickness=0,
            exportselection=False,
            font=("Consolas", 10),
        )
        scrollbar = tk.Scrollbar(frame, orient="vertical", command=listbox.yview)
        listbox.configure(yscrollcommand=scrollbar.set)
        listbox.grid(row=0, column=0, sticky="nsew", padx=(10, 0), pady=10)
        scrollbar.grid(row=0, column=1, sticky="ns", padx=(0, 10), pady=10)
        for value in files:
            listbox.insert("end", value)
        current = self.sample_entries.get("plot_mzml_file").get().strip() if "plot_mzml_file" in self.sample_entries else ""
        if current in files:
            listbox.selection_set(files.index(current))
        else:
            listbox.selection_set(0)

        def on_mousewheel(event: object) -> str:
            delta = getattr(event, "delta", 0)
            number = getattr(event, "num", None)
            direction = 1 if delta < 0 or number == 5 else -1
            listbox.yview_scroll(direction, "units")
            return "break"

        def apply_selection(*_args: object) -> None:
            selection = listbox.curselection()
            if not selection:
                return
            value = files[int(selection[0])]
            self._set_entry_text(self.sample_entries["plot_mzml_file"], value)
            self.refresh_sample_summary()
            self._show_sample_notice("Representative chromatogram set from HPLC sample injection files.")
            dialog.destroy()

        listbox.bind("<MouseWheel>", on_mousewheel)
        listbox.bind("<Button-4>", on_mousewheel)
        listbox.bind("<Button-5>", on_mousewheel)
        listbox.bind("<Double-Button-1>", apply_selection)
        listbox.bind("<Return>", apply_selection)
        buttons = ctk.CTkFrame(dialog, fg_color="transparent")
        buttons.grid(row=2, column=0, sticky="ew", padx=18, pady=(0, 18))
        buttons.grid_columnconfigure(0, weight=1)
        self._button(buttons, "Use selected file", apply_selection, primary=True, width=150).grid(row=0, column=1, padx=(0, 8))
        self._button(buttons, "Cancel", dialog.destroy, width=100).grid(row=0, column=2)

    def browse_sample_folder(self, field_key: str) -> None:
        root = resolve_project_path(self.config_data.setdefault("shared_inputs", {}).get("activity_root", "data/activity"))
        path = filedialog.askdirectory(initialdir=str(root if root.exists() else project_root()))
        if path:
            self.sample_entries[field_key].delete(0, "end")
            self.sample_entries[field_key].insert(0, relative_to_project(path))

    def main_feature_table_columns(self) -> list[str]:
        path_text = self.entries.get("main_feature_table").get().strip() if "main_feature_table" in self.entries else ""
        path = resolve_project_path(path_text)
        if not path.exists():
            messagebox.showwarning(
                APP_TITLE,
                "Choose an existing Main feature table before selecting a peak-area column.",
            )
            return []
        try:
            import pandas as pd

            if path.suffix.lower() in {".xlsx", ".xls"}:
                columns = list(pd.read_excel(path, nrows=0).columns)
            else:
                columns = list(pd.read_csv(path, nrows=0, sep=None, engine="python").columns)
        except Exception as exc:
            messagebox.showerror(APP_TITLE, f"Could not read columns from:\n{path}\n\n{exc}")
            return []
        return [str(column) for column in columns]

    def choose_peak_area_column(self) -> None:
        columns = self.main_feature_table_columns()
        if not columns:
            return

        dialog = ctk.CTkToplevel(self)
        dialog.title("Select peak-area column")
        dialog.geometry("760x560")
        dialog.minsize(620, 420)
        dialog.configure(fg_color=COLORS["bg"])
        dialog.transient(self)
        dialog.grab_set()
        dialog.grid_columnconfigure(0, weight=1)
        dialog.grid_rowconfigure(2, weight=1)

        ctk.CTkLabel(
            dialog,
            text="Select the main-table peak-area column for this sample.",
            text_color=COLORS["text"],
            font=ctk.CTkFont(family="Segoe UI", size=16, weight="bold"),
            anchor="w",
        ).grid(row=0, column=0, sticky="ew", padx=18, pady=(18, 6))
        search_var = ctk.StringVar(value="")
        search_entry = ctk.CTkEntry(
            dialog,
            textvariable=search_var,
            placeholder_text="Filter columns, for example s107 or Peak area",
            fg_color=COLORS["entry"],
            border_color=COLORS["border"],
            text_color=COLORS["text"],
            height=36,
            corner_radius=8,
        )
        search_entry.grid(row=1, column=0, sticky="ew", padx=18, pady=(4, 10))

        list_frame = ctk.CTkFrame(dialog, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, corner_radius=10)
        list_frame.grid(row=2, column=0, sticky="nsew", padx=18, pady=(0, 12))
        list_frame.grid_columnconfigure(0, weight=1)
        list_frame.grid_rowconfigure(0, weight=1)
        listbox = tk.Listbox(
            list_frame,
            bg=COLORS["entry"],
            fg=COLORS["text"],
            selectbackground=COLORS["accent"],
            selectforeground="white",
            relief="flat",
            highlightthickness=0,
            exportselection=False,
            font=("Consolas", 10),
        )
        scrollbar = tk.Scrollbar(list_frame, orient="vertical", command=listbox.yview)
        listbox.configure(yscrollcommand=scrollbar.set)
        listbox.grid(row=0, column=0, sticky="nsew", padx=(10, 0), pady=10)
        scrollbar.grid(row=0, column=1, sticky="ns", padx=(0, 10), pady=10)

        current_columns: list[str] = []

        def filtered_columns() -> list[str]:
            query = search_var.get().strip().lower()
            if not query:
                return columns
            return [column for column in columns if query in column.lower()]

        def refresh_list(*_args: object) -> None:
            nonlocal current_columns
            current_columns = filtered_columns()
            listbox.delete(0, "end")
            for column in current_columns:
                listbox.insert("end", column)
            current = self.sample_entries.get("sample_peak_area_column").get().strip()
            if current in current_columns:
                index = current_columns.index(current)
                listbox.selection_set(index)
                listbox.see(index)
            elif current_columns:
                listbox.selection_set(0)

        def apply_selection(*_args: object) -> None:
            selection = listbox.curselection()
            if not selection:
                return
            value = current_columns[int(selection[0])]
            entry = self.sample_entries.get("sample_peak_area_column")
            if entry:
                self._set_entry_text(entry, value)
            self.refresh_sample_summary()
            self._show_sample_notice(f"Peak-area column selected: {value}", kind="success")
            dialog.destroy()

        def on_mousewheel(event: object) -> str:
            delta = getattr(event, "delta", 0)
            number = getattr(event, "num", None)
            direction = 1 if delta < 0 or number == 5 else -1
            listbox.yview_scroll(direction, "units")
            return "break"

        search_var.trace_add("write", refresh_list)
        listbox.bind("<MouseWheel>", on_mousewheel)
        listbox.bind("<Button-4>", on_mousewheel)
        listbox.bind("<Button-5>", on_mousewheel)
        listbox.bind("<Double-Button-1>", apply_selection)
        listbox.bind("<Return>", apply_selection)

        buttons = ctk.CTkFrame(dialog, fg_color="transparent")
        buttons.grid(row=3, column=0, sticky="ew", padx=18, pady=(0, 18))
        buttons.grid_columnconfigure(0, weight=1)
        self._button(buttons, "Use selected column", apply_selection, primary=True, width=160).grid(row=0, column=1, padx=(0, 8))
        self._button(buttons, "Cancel", dialog.destroy, width=100).grid(row=0, column=2)

        refresh_list()
        search_entry.focus_set()

    def expected_mzmine_console_path(self) -> Path:
        configured = self.config_data.setdefault("mzmine", {}).get("portable_console") or MZMINE_CONSOLE_RELATIVE
        return resolve_project_path(configured)

    def open_mzmine_download_page(self) -> None:
        webbrowser.open(MZMINE_RELEASE_URL)
        self.status_text.set("Opened the official MZmine 4.7.8 release page. Download the portable package and unzip it into the microfractionation folder.")

    def check_mzmine_setup(self) -> None:
        expected = project_root() / MZMINE_CONSOLE_RELATIVE
        if expected.exists():
            self._set_entry_text(self.entries["mzmine_console"], MZMINE_CONSOLE_RELATIVE)
            self.config_data.setdefault("mzmine", {})["portable_console"] = MZMINE_CONSOLE_RELATIVE
            self.validate_project(silent=True)
            messagebox.showinfo(APP_TITLE, f"MZmine console found:\n{expected}")
            self.status_text.set("MZmine 4.7.8 portable console found.")
            return
        self.status_text.set("MZmine 4.7.8 portable console was not found in the expected folder.")
        messagebox.showwarning(
            APP_TITLE,
            "MZmine was not found yet. Download MZmine 4.7.8 portable from the official GitHub release page, "
            f"unzip it into:\n{project_root()}\n\nExpected file:\n{expected}",
        )
        self.validate_project(silent=True)

    def collect_sample_form(self) -> dict:
        display_name = self.sample_entries["display_name"].get().strip()
        if not display_name:
            raise ValueError("Sample display name is required.")
        safe_name = file_safe_name(display_name)
        output_folder = f"output/01_mzmine_pipeline/{safe_name}"
        return {
            "display_name": display_name,
            "slug": safe_name,
            "file_safe_name": safe_name,
            "sample_mzml_files": self._split_paths(self.sample_entries["sample_mzml_files"].get()),
            "blank_mzml_files": self._split_paths(self.sample_entries["blank_mzml_files"].get()),
            "activity_subfolder": self.sample_entries["activity_subfolder"].get().strip(),
            "plot_mzml_file": self.sample_entries["plot_mzml_file"].get().strip(),
            "representative_mzml_file": self.sample_entries["plot_mzml_file"].get().strip(),
            "sample_peak_area_column": self.sample_entries["sample_peak_area_column"].get().strip(),
            "output_folder": output_folder,
        }

    def add_or_update_sample(self) -> None:
        try:
            sample = self.collect_sample_form()
        except Exception as exc:
            messagebox.showerror(APP_TITLE, str(exc))
            return
        samples = self.config_data.setdefault("samples", [])
        safe_name = sample["file_safe_name"]
        existing_index = next(
            (
                index
                for index, item in enumerate(samples)
                if item.get("file_safe_name") == safe_name or item.get("slug") == safe_name or item.get("display_name") == sample["display_name"]
            ),
            None,
        )
        action = "added" if existing_index is None else "updated"
        if existing_index is None:
            samples.append(sample)
        else:
            samples[existing_index] = sample
        self.refresh_sample_selector(select=sample["display_name"])
        self.refresh_sample_list()
        self.validate_project(silent=True)
        self._show_sample_notice(f"Sample {sample['display_name']} {action} to the list.")

    def remove_selected_sample(self) -> None:
        sample = self.selected_sample()
        if not sample:
            return
        label = sample.get("display_name", sample.get("file_safe_name", "sample"))
        if not messagebox.askyesno(APP_TITLE, f"Remove sample setup for {label}?"):
            return
        self.config_data["samples"] = [
            item
            for item in self.config_data.get("samples", [])
            if item is not sample
        ]
        self.refresh_sample_selector()
        self.refresh_sample_list()
        self.validate_project(silent=True)
        self._show_sample_notice(f"Sample {label} removed from the list.", kind="warning")

    def refresh_sample_selector(self, *, select: str | None = None) -> None:
        if not self.sample_box:
            return
        names = self.sample_names()
        self.sample_box.configure(values=names or ["No samples configured"])
        choice = select if select in names else names[0] if names else "No samples configured"
        self.sample_box.set(choice)
        if names:
            self.load_selected_sample_into_form()
        else:
            self.clear_sample_form()
        self.refresh_sample_list()

    def refresh_sample_summary(self) -> None:
        if not self.sample_box or self.sample_summary is None:
            return
        sample = self.selected_sample()
        if not sample:
            self.sample_summary.configure(text="No sample selected.")
            return
        activity = sample.get("activity_subfolder") or "No activity folder configured"
        text = (
            f"File-safe sample name: {sample.get('file_safe_name', sample.get('slug', ''))}\n"
            f"HPLC sample injection files: {len(sample.get('sample_mzml_files', []))}; "
            f"HPLC blank injection files: {len(sample.get('blank_mzml_files', []))}\n"
            f"Peak-area column: {sample.get('sample_peak_area_column', '') or 'not configured'}\n"
            f"Representative chromatogram for plots: {sample.get('plot_mzml_file', sample.get('representative_mzml_file', '')) or 'not configured'}\n"
            f"Activity folder: {activity}\n"
            f"Output folder: {sample.get('output_folder', '')}"
        )
        self.sample_summary.configure(text=text)

    def _path_check(self, level_if_missing: str, label: str, path: Path, *, found: str | None = None, missing: str | None = None) -> tuple[str, str, str]:
        # Empty config values resolve to the project root, which would otherwise
        # look like a valid existing path. For readiness checks that always means
        # the user has not configured the expected file/folder.
        try:
            if path.resolve() == project_root().resolve():
                return (level_if_missing, label, missing or "Path is empty.")
        except Exception:
            pass
        if path.exists():
            return ("ok", label, found or f"Found: {relative_to_project(path)}")
        return (level_if_missing, label, missing or f"Missing: {relative_to_project(path)}")

    def _sample_file_checks(self, sample: dict | None) -> list[tuple[str, str, str]]:
        if not sample:
            return [("error", "Selected sample", "Choose or add a sample before opening this module.")]
        label = self.sample_display_name(sample)
        checks: list[tuple[str, str, str]] = []
        sample_files = sample.get("sample_mzml_files", []) or []
        blank_files = sample.get("blank_mzml_files", []) or []
        missing_sample = [path for path in sample_files if not resolve_project_path(path).exists()]
        missing_blank = [path for path in blank_files if not resolve_project_path(path).exists()]
        if not sample_files:
            checks.append(("error", f"{label} HPLC sample injection files", "No HPLC sample injection files are configured for this sample."))
        elif missing_sample:
            checks.append(("error", f"{label} HPLC sample injection files", f"Missing {len(missing_sample)} configured HPLC sample injection file(s)."))
        else:
            checks.append(("ok", f"{label} HPLC sample injection files", f"Found {len(sample_files)} HPLC sample injection file(s)."))
        if not blank_files:
            checks.append(("warning", f"{label} HPLC blank injection files", "No HPLC blank injection files are configured. Continue only if this run intentionally has no blanks."))
        elif missing_blank:
            checks.append(("error", f"{label} HPLC blank injection files", f"Missing {len(missing_blank)} configured HPLC blank injection file(s)."))
        else:
            checks.append(("ok", f"{label} HPLC blank injection files", f"Found {len(blank_files)} HPLC blank injection file(s)."))
        return checks

    def module_readiness_items(self, module_key: str) -> list[tuple[str, str, str]]:
        self.update_config_from_ui()
        sample = self.selected_sample()
        sample_label = self.sample_display_name(sample)
        shared = self.config_data.setdefault("shared_inputs", {})
        mzmine = self.config_data.setdefault("mzmine", {})
        output_root = resolve_project_path(self.config_data.get("output_root", "output"))
        items: list[tuple[str, str, str]] = []


        if module_key in {"batch_setup", "mzmine_runner", "feature_filtering", "two_sided_plot", "fraction_predictor"}:
            items.extend(self._sample_file_checks(sample) if module_key == "batch_setup" else ([] if sample else [("error", "Selected sample", "Choose or add a sample before opening this module.")]))

        if module_key == "batch_setup":
            complete_template = str(mzmine.get("complete_template", "") or "").strip()
            fraction_template = str(mzmine.get("fraction_template", "") or "").strip()
            if complete_template and resolve_project_path(complete_template).exists():
                items.append(("ok", "Complete batch template", f"Configured: {complete_template}"))
            else:
                items.append(("warning", "Complete batch template", "Can be selected inside Batch setup. No launcher-level template is required to open the module."))
            if fraction_template and resolve_project_path(fraction_template).exists():
                items.append(("ok", "Fraction batch template", f"Configured: {fraction_template}"))
            else:
                items.append(("warning", "Fraction batch template", "Can be selected inside Batch setup. No launcher-level template is required to open the module."))
            batch_dir = self.sample_pipeline_root(sample) / "configured_batches"
            items.append(("ok", "Configured batch folder", f"Will use: {relative_to_project(batch_dir)}"))
            return items

        if module_key == "mzmine_runner":
            items.append(self._path_check("error", "MZmine console", resolve_project_path(mzmine.get("portable_console", ""))))
            items.append(self._path_check("error", f"{sample_label} complete batch", self.expected_complete_batch(sample), missing="Run Batch setup first; the configured complete .mzbatch is missing."))
            items.append(self._path_check("error", f"{sample_label} fraction batch", self.expected_fraction_batch(sample), missing="Run Batch setup first; the configured fraction .mzbatch is missing."))
            return items

        if module_key == "feature_filtering":
            items.append(self._path_check("warning", f"{sample_label} complete feature table", self.expected_complete_feature_table(sample), missing="MZmine runner should create this complete feature-table CSV. It is needed by Feature filtering."))
            fraction_dir = self.expected_fraction_feature_dir(sample)
            frac_files = sorted(fraction_dir.glob("frac_*.csv")) if fraction_dir.exists() else []
            if frac_files:
                items.append(("ok", f"{sample_label} fraction feature tables", f"Found {len(frac_files)} frac_*.csv file(s) in {relative_to_project(fraction_dir)}."))
            else:
                items.append(("warning", f"{sample_label} fraction feature tables", f"MZmine runner should create frac_*.csv files here: {relative_to_project(fraction_dir)}. They are needed by Feature filtering."))
            items.append(("ok", "Filtered feature-table folder", f"Will write to: {relative_to_project(self.expected_filtered_feature_table(sample).parent)}"))
            return items

        if module_key == "two_sided_plot":
            items.append(self._path_check("warning", f"{sample_label} filtered HPLC feature table", self.expected_filtered_feature_table(sample), missing="Feature filtering should create this filtered HPLC feature table. It is needed for the full two-sided plot."))
            plot_file = (sample or {}).get("plot_mzml_file") or (sample or {}).get("representative_mzml_file") or ""
            if plot_file:
                items.append(self._path_check("error", f"{sample_label} chromatogram mzML", resolve_project_path(plot_file), missing="The representative chromatogram mzML configured for plotting is missing."))
            else:
                sample_files = (sample or {}).get("sample_mzml_files", []) or []
                if sample_files and resolve_project_path(sample_files[0]).exists():
                    items.append(("warning", f"{sample_label} chromatogram mzML", "No representative chromatogram is configured; the module may use the first sample mzML or require manual selection."))
                else:
                    items.append(("error", f"{sample_label} chromatogram mzML", "No representative chromatogram mzML is configured and no usable sample mzML file was found."))
            activity = (sample or {}).get("activity_subfolder", "")
            if activity:
                items.append(self._path_check("warning", f"{sample_label} activity/intensity folder", resolve_project_path(activity), missing="Configured activity/intensity folder is missing; the figure can still be made without the overlay or after manual selection."))
            else:
                items.append(("warning", f"{sample_label} activity/intensity folder", "No activity/intensity folder is configured; the overlay will need manual input or can be omitted."))
            return items

        if module_key == "fraction_predictor":
            items.append(self._path_check("error", "Main feature table", resolve_project_path(shared.get("main_feature_table", ""))))
            items.append(self._path_check("error", "SIRIUS/CANOPUS annotation table", resolve_project_path(shared.get("sirius_annotation_table", ""))))
            peak_col = (sample or {}).get("sample_peak_area_column", "")
            if peak_col:
                items.append(("ok", f"{sample_label} peak-area column", f"Configured: {peak_col}"))
            else:
                items.append(("warning", f"{sample_label} peak-area column", "No sample peak-area column is configured; Script 04 will require manual column selection."))
            items.append(self._path_check("warning", f"{sample_label} filtered HPLC feature table", self.expected_filtered_feature_table(sample), missing="Script 04 can open, but feature-order anchor mode needs the Script 01 filtered HPLC feature table."))
            activity = (sample or {}).get("activity_subfolder", "")
            if activity:
                items.append(self._path_check("warning", f"{sample_label} activity/intensity folder", resolve_project_path(activity), missing="Configured activity/intensity folder is missing; plant response data will need manual selection or can be skipped."))
            else:
                items.append(("warning", f"{sample_label} activity/intensity folder", "No activity/intensity folder is configured for this sample."))
            return items


        if module_key == "wikidata":
            out = resolve_project_path(self.config_data.get("modules", {}).get("wikidata", {}).get("output_folder", output_root / "03_wikidata"))
            items.append(("ok", "Wikidata output folder", f"Will use: {relative_to_project(out)}"))
            return items

        return items

    def render_check_items(self, items: list[tuple[str, str, str]]) -> None:
        for child in self.validation_rows_frame.winfo_children():
            child.destroy()
        colors = {"ok": COLORS["green"], "warning": COLORS["amber"], "error": COLORS["red"]}
        for row, (level, label, message) in enumerate(items):
            ctk.CTkLabel(
                self.validation_rows_frame,
                text=level.upper(),
                width=72,
                text_color=colors.get(level, COLORS["muted"]),
                font=ctk.CTkFont(weight="bold"),
            ).grid(row=row, column=0, sticky="w", pady=2)
            ctk.CTkLabel(
                self.validation_rows_frame,
                text=label,
                width=230,
                anchor="w",
                text_color=COLORS["text"],
            ).grid(row=row, column=1, sticky="w", pady=2, padx=(8, 8))
            ctk.CTkLabel(
                self.validation_rows_frame,
                text=message,
                anchor="w",
                text_color=COLORS["muted"],
                wraplength=900,
                justify="left",
            ).grid(row=row, column=2, sticky="ew", pady=2)

    def validation_items(self) -> list[tuple[str, str, str]]:
        self.update_config_from_ui()
        shared = self.config_data.setdefault("shared_inputs", {})
        mzmine = self.config_data.setdefault("mzmine", {})
        items: list[tuple[str, str, str]] = []

        required_files = [
            ("Main feature table", shared.get("main_feature_table", "")),
            ("SIRIUS/CANOPUS annotations", shared.get("sirius_annotation_table", "")),
        ]
        for label, value in required_files:
            if not value:
                items.append(("error", label, "Path is empty."))
            elif resolve_project_path(value).exists():
                items.append(("ok", label, f"Found: {value}"))
            else:
                items.append(("error", label, f"Missing: {value}"))

        mzmine_console = mzmine.get("portable_console") or MZMINE_CONSOLE_RELATIVE
        mzmine_path = resolve_project_path(mzmine_console)
        if mzmine_path.exists():
            items.append(("ok", "MZmine 4.7.8 portable", f"Found: {relative_to_project(mzmine_path)}"))
        else:
            items.append(("warning", "MZmine 4.7.8 portable", f"Download the portable release from GitHub and unzip it so this file exists: {MZMINE_CONSOLE_RELATIVE}"))

        for label, value in [
            ("Activity root", shared.get("activity_root", "")),
            ("HPLC mzML root", shared.get("raw_mzml_root", "")),
            ("Output root", self.config_data.get("output_root", "output")),
        ]:
            path = resolve_project_path(value or "")
            if path.exists() and path.is_dir():
                items.append(("ok", label, f"Found: {value}"))
            else:
                items.append(("warning", label, f"Will create folder: {value}"))

        fraction_settings = self.config_data.setdefault("fraction_collection", {})
        for key, label, _default in FRACTION_FIELDS:
            value = fraction_settings.get(key, "")
            try:
                numeric = float(value)
                if numeric <= 0 and key != "first_fraction_number":
                    raise ValueError
                items.append(("ok", label, f"Configured: {value}"))
            except Exception:
                items.append(("error", label, f"Invalid numeric value: {value}"))

        for sample in self.config_data.get("samples", []):
            slug = sample.get("slug", "sample")
            missing = [
                path
                for path in sample.get("sample_mzml_files", []) + sample.get("blank_mzml_files", [])
                if not resolve_project_path(path).exists()
            ]
            if missing:
                items.append(("error", f"{slug} mzML files", f"Missing {len(missing)} configured raw file(s)."))
            else:
                items.append(("ok", f"{slug} mzML files", "All configured sample and blank files exist."))
            activity = sample.get("activity_subfolder", "")
            if activity and resolve_project_path(activity).exists():
                items.append(("ok", f"{slug} activity", f"Found: {activity}"))
            elif activity:
                items.append(("warning", f"{slug} activity", f"Configured folder is missing: {activity}"))
        return items

    def ensure_output_folders(self) -> None:
        output_root = resolve_project_path(self.config_data.get("output_root", "output"))
        output_root.mkdir(parents=True, exist_ok=True)
        for module in self.config_data.get("modules", {}).values():
            folder = module.get("output_folder")
            if folder:
                resolve_project_path(folder).mkdir(parents=True, exist_ok=True)
        for sample in self.config_data.get("samples", []):
            slug = sample.get("slug", "sample")
            for subfolder in [
                "configured_batches",
                "complete_feature_table",
                "fraction_feature_tables",
                "filtered_feature_table",
                "mzmine_temp",
            ]:
                (output_root / "01_mzmine_pipeline" / slug / subfolder).mkdir(parents=True, exist_ok=True)
            (output_root / "02_two_sided_plot" / slug).mkdir(parents=True, exist_ok=True)
            (output_root / "04_fraction_predictor" / slug).mkdir(parents=True, exist_ok=True)

    def validate_project(self, silent: bool = False) -> bool:
        self.update_config_from_ui()
        self.ensure_output_folders()
        items = self.validation_items()
        self.render_check_items(items)
        ok = not any(level == "error" for level, _label, _message in items)
        self.status_text.set("Project checks passed." if ok else "Project checks found missing required files.")
        if not silent and ok:
            messagebox.showinfo(APP_TITLE, "Project checks passed and output folders were created.")
        elif not silent and not ok:
            messagebox.showwarning(
                APP_TITLE,
                "Some required files are missing. Review the Project checks panel before launching a module.",
            )
        return ok

    def save_config(self) -> None:
        self.update_config_from_ui()
        write_json(self.config_path, self.config_data)
        self.status_text.set(f"Saved config: {self.config_path}")

    def load_config_dialog(self) -> None:
        path = filedialog.askopenfilename(
            initialdir=str(project_root()),
            filetypes=[("JSON config", "*.json"), ("All files", "*.*")],
        )
        if not path:
            return
        self.config_path = Path(path)
        self.config_data = read_json(self.config_path)
        self.refresh_ui_from_config()
        self.validate_project(silent=True)
        self.status_text.set(f"Loaded config: {self.config_path}")

    def _reenable_module_button(self, module_key: str) -> None:
        button = self.module_buttons.get(module_key)
        if button is not None:
            button.configure(state="normal")

    def launch_module(self, module_key: str) -> None:
        self.update_config_from_ui()
        self.validate_project(silent=True)
        module = next((item for item in MODULES if item[0] == module_key), None)
        if module is None:
            return
        _key, label, script_rel, _note = module
        readiness = self.module_readiness_items(module_key)
        if readiness:
            module_rows = [("ok", f"Module readiness: {label}", "These checks are specific to the selected workflow step and sample.")] + readiness
            self.render_check_items(self.validation_items() + module_rows)
            errors = [(item_label, message) for level, item_label, message in readiness if level == "error"]
            warnings = [(item_label, message) for level, item_label, message in readiness if level == "warning"]
            if errors:
                self.status_text.set(f"{label} is not ready. Review the module readiness checks.")
                details = "\n".join(f"- {item_label}: {message}" for item_label, message in errors[:5])
                if len(errors) > 5:
                    details += f"\n- ...and {len(errors) - 5} more issue(s)."
                if not messagebox.askyesno(APP_TITLE, f"{label} is missing required inputs. Open anyway?\n\n{details}"):
                    return
            elif warnings:
                self.status_text.set(f"{label} has readiness warnings. Review the module readiness checks.")
        active_process = self.module_processes.get(module_key)
        if active_process is not None:
            if active_process.poll() is None:
                self.status_text.set(f"{label} is already open or still starting.")
                return
            self.module_processes.pop(module_key, None)
        script_path = project_root() / "scripts" / script_rel
        if not script_path.exists():
            messagebox.showerror(APP_TITLE, f"Module script is missing:\n{script_path}")
            return
        handoff = project_root() / "configs" / "launcher_handoff_config.json"
        handoff.parent.mkdir(parents=True, exist_ok=True)
        write_json(handoff, self.config_data)
        env = os.environ.copy()
        env["MICROFRACTIONATION_PROJECT_CONFIG"] = str(handoff)
        button = self.module_buttons.get(module_key)
        if button is not None:
            button.configure(state="disabled")
        process = subprocess.Popen([sys.executable, str(script_path)], cwd=str(script_path.parent), env=env)
        self.module_processes[module_key] = process
        self.after(4000, lambda key=module_key: self._reenable_module_button(key))
        self.status_text.set(f"Opened {label}. Shared config path was exported for the module.")


if __name__ == "__main__":
    app = Launcher()
    app.mainloop()





