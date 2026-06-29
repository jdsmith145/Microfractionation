#!/usr/bin/env python3
"""Focused GUI for preparing complete and fraction MZmine batch files."""
from __future__ import annotations

import json
import os
import queue
import sys
import threading
import traceback
from pathlib import Path
from typing import Any


THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))
if str(THIS_DIR.parent) not in sys.path:
    sys.path.insert(0, str(THIS_DIR.parent))

from support.example_data_helper import open_example
from support.gui_help_popover import HelpPopoverController
import p_01_00_mzmine_pipeline_core as core


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
    "danger": "#b54848",
}


def main() -> int:
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox
    except Exception as exc:
        print(f"Tkinter is not available: {exc}", file=sys.stderr)
        return 2
    try:
        import customtkinter as ctk
    except Exception as exc:
        print(f"CustomTkinter is not installed: {exc}", file=sys.stderr)
        return 2

    ctk.set_appearance_mode("dark")
    ctk.set_default_color_theme("blue")

    root = ctk.CTk()
    root.title("MZmine batch setup")
    root.geometry("1320x860")
    root.minsize(1040, 720)
    root.configure(fg_color=COLORS["bg"])
    help_popovers = HelpPopoverController(root, ctk, COLORS, ("Segoe UI", 11))

    result_queue: queue.Queue[tuple[bool, Any]] = queue.Queue()
    log_queue: queue.Queue[str] = queue.Queue()
    worker_state = {"running": False}

    config = core.template_config()
    base_dir = Path(config.get("base_dir", THIS_DIR))


    project_config_path_text = os.environ.get("MICROFRACTIONATION_PROJECT_CONFIG", "")
    project_config_path = Path(project_config_path_text).expanduser() if project_config_path_text else None

    def load_project_config() -> dict[str, Any]:
        if not project_config_path or not project_config_path.exists():
            return {}
        try:
            return json.loads(project_config_path.read_text(encoding="utf-8-sig"))
        except Exception:
            return {}

    project_config = load_project_config()
    project_root = project_config_path.parent.parent if project_config_path else THIS_DIR.parent.parent
    project_samples = [item for item in project_config.get("samples", []) if isinstance(item, dict)]

    def resolve_project_path(value: Any) -> Path:
        text = str(value or "").strip()
        if not text:
            return Path("")
        candidate = Path(text).expanduser()
        return candidate if candidate.is_absolute() else project_root / candidate

    def sample_label(sample: dict[str, Any]) -> str:
        return str(sample.get("display_name") or sample.get("slug") or sample.get("file_safe_name") or "Unnamed sample")

    def sample_slug(sample: dict[str, Any]) -> str:
        return str(sample.get("file_safe_name") or sample.get("slug") or sample_label(sample).lower().replace(" ", "_")).strip()

    def project_sample_by_label(label: str) -> dict[str, Any] | None:
        return next((sample for sample in project_samples if sample_label(sample) == label), None)

    def sample_pipeline_root(sample: dict[str, Any]) -> Path:
        configured = str(sample.get("output_folder") or "").strip()
        if configured:
            return resolve_project_path(configured)
        return project_root / "output" / "01_mzmine_pipeline" / sample_slug(sample)

    project_sample_names = [sample_label(sample) for sample in project_samples]

    sample_name = tk.StringVar(value=str(config.get("sample_name", "")))
    sample_files_text: Any
    blank_files_text: Any
    complete_template = tk.StringVar(value=str(config["complete"]["template_path"]))
    complete_batch_dir = tk.StringVar(value=str(config["complete"]["out_dir"]))
    complete_feature_dir = tk.StringVar(value=str(config["complete"]["feature_dir"]))
    blank_pattern = tk.StringVar(value=str(config["complete"]["blank_pattern"]))
    fraction_template = tk.StringVar(value=str(config["fraction"]["template_path"]))
    fraction_batch_dir = tk.StringVar(value=str(config["fraction"]["out_dir"]))
    fraction_feature_dir = tk.StringVar(value=str(config["fraction"]["feature_dir"]))
    rt_start = tk.StringVar(value=str(config["fraction"]["rt_start"]))
    rt_end = tk.StringVar(value=str(config["fraction"]["rt_end"]))
    rt_width = tk.StringVar(value=str(config["fraction"]["width"]))
    status = tk.StringVar(value="Ready.")
    fraction_summary = tk.StringVar(value="Fraction collection settings will be inherited from the launcher or loaded config.")

    sample_prompt = "Select a sample..."
    var_project_sample = tk.StringVar(value=sample_prompt)
    project_status = tk.StringVar(value="Choose the sample you added in the main launcher. This fills HPLC files and output folders automatically." if project_samples else "No launcher samples were found. Add samples in the main launcher for the guided workflow, or enter paths directly here.")
    setup_cards_visible = not bool(project_samples)
    direct_entry_enabled = not bool(project_samples)
    setup_cards: list[Any] = []

    def make_button(parent: Any, text: str, command: Any, *, primary: bool = False, danger: bool = False, width: int = 118) -> Any:
        color = COLORS["danger"] if danger else COLORS["accent"] if primary else COLORS["card_alt"]
        hover = "#9e3b3b" if danger else COLORS["accent_hover"] if primary else "#39414c"
        return ctk.CTkButton(parent, text=text, command=command, width=width, height=38, corner_radius=10, fg_color=color, hover_color=hover)

    def example_button(parent: Any, relative_path: str, *, width: int = 82) -> Any:
        return make_button(parent, "Example", lambda: open_example(__file__, relative_path, messagebox=messagebox), width=width)

    def make_entry(parent: Any, var: tk.StringVar, placeholder: str = "") -> Any:
        return ctk.CTkEntry(parent, textvariable=var, placeholder_text=placeholder, fg_color=COLORS["entry"], border_color=COLORS["border"], text_color=COLORS["text"], height=36, corner_radius=8)

    def make_help(parent: Any, text: str) -> Any:
        return help_popovers.create_bubble(parent, text)

    def card(parent: Any, title: str, subtitle: str = "", *, visible: bool = True) -> Any:
        frame = ctk.CTkFrame(parent, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, corner_radius=14)
        if visible:
            frame.pack(fill="x", padx=12, pady=(0, 12))
        frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(frame, text=title, font=("Segoe UI", 16, "bold"), text_color=COLORS["text"], anchor="w").grid(row=0, column=0, columnspan=5, sticky="ew", padx=18, pady=(16, 4))
        if subtitle:
            ctk.CTkLabel(frame, text=subtitle, font=("Segoe UI", 11), text_color=COLORS["muted"], anchor="w", justify="left", wraplength=1120).grid(row=1, column=0, columnspan=5, sticky="ew", padx=18, pady=(0, 12))
        return frame

    def labeled(parent: Any, label: str, var: tk.StringVar, row: int, help_text: str, *, browse: Any = None, placeholder: str = "") -> None:
        label_frame = ctk.CTkFrame(parent, fg_color="transparent")
        label_frame.grid(row=row, column=0, sticky="ew", padx=(18, 10), pady=7)
        ctk.CTkLabel(label_frame, text=label, font=("Segoe UI", 12), text_color=COLORS["muted"], anchor="w").pack(side="left")
        make_help(label_frame, help_text).pack(side="left", padx=(8, 0))
        make_entry(parent, var, placeholder).grid(row=row, column=1, columnspan=3, sticky="ew", padx=(0, 8), pady=7)
        if browse:
            make_button(parent, "Browse", browse, width=88).grid(row=row, column=4, sticky="e", padx=(0, 18), pady=7)

    def browse_file(var: tk.StringVar, title: str, patterns: list[tuple[str, str]]) -> None:
        path = filedialog.askopenfilename(title=title, initialdir=str(base_dir), filetypes=patterns)
        if path:
            var.set(path)

    def browse_dir(var: tk.StringVar, title: str) -> None:
        path = filedialog.askdirectory(title=title, initialdir=str(base_dir))
        if path:
            var.set(path)

    def text_lines(widget: Any) -> list[str]:
        return [line.strip() for line in widget.get("1.0", "end").splitlines() if line.strip()]

    def set_text_lines(widget: Any, values: list[str]) -> None:
        widget.delete("1.0", "end")
        widget.insert("1.0", "\n".join(values))

    def add_files(widget: Any, title: str) -> None:
        paths = filedialog.askopenfilenames(title=title, initialdir=str(base_dir), filetypes=[("mzML files", "*.mzML *.mzml"), ("All files", "*.*")])
        if paths:
            current = text_lines(widget)
            set_text_lines(widget, [*current, *paths])


    def refresh_fraction_summary() -> None:
        try:
            start = float(rt_start.get())
            end = float(rt_end.get())
            width = float(rt_width.get())
            count = int(round((end - start) / width)) if width else 0
            fraction_summary.set(
                f"Fraction collection inherited: starts at {start:g} min, ends at {end:g} min, "
                f"about {count} fractions, width {width:g} min. Edit these values in the launcher."
            )
        except Exception:
            fraction_summary.set("Fraction collection settings are incomplete. Check the launcher fraction collection panel.")

    def reveal_setup_cards() -> None:
        nonlocal setup_cards_visible
        if setup_cards_visible:
            return
        for frame in setup_cards:
            frame.pack(fill="x", padx=12, pady=(0, 12))
        setup_cards_visible = True

    def show_direct_entry_fields() -> None:
        nonlocal direct_entry_enabled
        direct_entry_enabled = True
        reveal_setup_cards()
        project_status.set("Direct path entry is visible. Use it only if this sample was not added in the launcher or if you need a temporary override.")
        status.set("Direct path entry is visible.")

    def apply_project_sample(*_args: Any, notify: bool = True) -> None:
        selected_label = var_project_sample.get()
        sample = project_sample_by_label(selected_label)
        if not sample:
            project_status.set("Select a launcher sample first, or choose Enter paths directly.")
            return
        slug = sample_slug(sample)
        pipeline_root = sample_pipeline_root(sample)
        configured_dir = pipeline_root / "configured_batches"
        sample_name.set(slug)
        set_text_lines(sample_files_text, [str(resolve_project_path(path)) for path in core.as_list(sample.get("sample_mzml_files"))])
        set_text_lines(blank_files_text, [str(resolve_project_path(path)) for path in core.as_list(sample.get("blank_mzml_files"))])
        mzmine_cfg = project_config.get("mzmine", {}) if isinstance(project_config.get("mzmine", {}), dict) else {}
        if mzmine_cfg.get("complete_template"):
            complete_template.set(str(resolve_project_path(mzmine_cfg.get("complete_template"))))
        if mzmine_cfg.get("fraction_template"):
            fraction_template.set(str(resolve_project_path(mzmine_cfg.get("fraction_template"))))
        complete_batch_dir.set(str(configured_dir))
        fraction_batch_dir.set(str(configured_dir))
        complete_feature_dir.set(str(pipeline_root / "complete_feature_table"))
        fraction_feature_dir.set(str(pipeline_root / "fraction_feature_tables"))
        fraction_cfg = project_config.get("fraction_collection", {}) if isinstance(project_config.get("fraction_collection", {}), dict) else {}
        start = fraction_cfg.get("start_time")
        end = fraction_cfg.get("end_time")
        n_fractions = fraction_cfg.get("n_fractions")
        if start not in (None, ""):
            rt_start.set(str(start))
        if end not in (None, ""):
            rt_end.set(str(end))
        try:
            if start not in (None, "") and end not in (None, "") and n_fractions not in (None, "", 0):
                rt_width.set(str((float(end) - float(start)) / float(n_fractions)))
        except Exception:
            pass
        refresh_fraction_summary()
        reveal_setup_cards()
        project_status.set(f"Loaded sample: {sample_label(sample)}. Review the generated batch paths, then run setup.")
        if notify:
            status.set(f"Loaded sample: {sample_label(sample)}. Review the generated batch paths, then run setup.")

    def build_config() -> dict[str, Any]:
        cfg = json.loads(json.dumps(config))
        cfg["base_dir"] = str(base_dir)
        cfg["sample_name"] = sample_name.get().strip()
        cfg["sample_files"] = text_lines(sample_files_text)
        cfg["blank_files"] = text_lines(blank_files_text)
        cfg["stages"] = ["prepare"]
        cfg["complete"].update({
            "template_path": complete_template.get().strip(),
            "out_dir": complete_batch_dir.get().strip(),
            "feature_dir": complete_feature_dir.get().strip(),
            "blank_pattern": blank_pattern.get().strip() or core.DEFAULT_BLANK_PATTERN,
        })
        cfg["fraction"].update({
            "template_path": fraction_template.get().strip(),
            "out_dir": fraction_batch_dir.get().strip(),
            "feature_dir": fraction_feature_dir.get().strip(),
            "rt_start": float(rt_start.get()),
            "rt_end": float(rt_end.get()),
            "width": float(rt_width.get()),
        })
        return cfg

    def apply_config(cfg: dict[str, Any]) -> None:
        nonlocal base_dir, config
        config = core._normalize_legacy_config(dict(cfg))
        base_dir = Path(config.get("base_dir", THIS_DIR)).expanduser()
        sample_name.set(str(config.get("sample_name", "")))
        set_text_lines(sample_files_text, core.as_list(config.get("sample_files")))
        set_text_lines(blank_files_text, core.as_list(config.get("blank_files")))
        complete = config.get("complete", {})
        fraction = config.get("fraction", {})
        complete_template.set(str(complete.get("template_path", "")))
        complete_batch_dir.set(str(complete.get("out_dir", core.DEFAULT_BATCH_DIR)))
        complete_feature_dir.set(str(complete.get("feature_dir", core.DEFAULT_COMPLETE_FEATURE_DIR)))
        blank_pattern.set(str(complete.get("blank_pattern", core.DEFAULT_BLANK_PATTERN)))
        fraction_template.set(str(fraction.get("template_path", "")))
        fraction_batch_dir.set(str(fraction.get("out_dir", core.DEFAULT_BATCH_DIR)))
        fraction_feature_dir.set(str(fraction.get("feature_dir", core.DEFAULT_FRACTION_FEATURE_DIR)))
        rt_start.set(str(fraction.get("rt_start", core.DEFAULT_RT_START)))
        rt_end.set(str(fraction.get("rt_end", core.DEFAULT_RT_END)))
        rt_width.set(str(fraction.get("width", core.DEFAULT_WIDTH)))
        refresh_fraction_summary()

    def save_config() -> None:
        path = filedialog.asksaveasfilename(title="Save MZmine config", initialdir=str(base_dir), defaultextension=".json", filetypes=[("JSON config", "*.json"), ("All files", "*.*")])
        if not path:
            return
        try:
            core.save_config(build_config(), path)
            status.set(f"Saved config: {path}")
        except Exception as exc:
            messagebox.showerror("Save config", str(exc))

    def load_config() -> None:
        path = filedialog.askopenfilename(title="Load MZmine config", initialdir=str(base_dir), filetypes=[("JSON config", "*.json"), ("All files", "*.*")])
        if not path:
            return
        try:
            apply_config(core.load_config(path))
            reveal_setup_cards()
            status.set(f"Loaded config: {path}")
        except Exception as exc:
            messagebox.showerror("Load config", str(exc))

    def append_log(message: str) -> None:
        log_box.configure(state="normal")
        log_box.insert("end", message + "\n")
        log_box.see("end")
        log_box.configure(state="disabled")

    def run_setup() -> None:
        if worker_state["running"]:
            return
        if project_samples and not setup_cards_visible and not direct_entry_enabled:
            project_status.set("Select a launcher sample first, or choose Enter paths directly.")
            status.set("Select a launcher sample first, or choose Enter paths directly.")
            return
        try:
            cfg = build_config()
            core.sample_name_from_config(cfg)
        except Exception as exc:
            messagebox.showerror("Batch setup", str(exc))
            return
        worker_state["running"] = True
        status.set("Preparing MZmine batches...")
        append_log("Preparing complete and fraction .mzbatch files...")

        def worker() -> None:
            try:
                result = core.run_pipeline(cfg, stages=["prepare"], log_callback=lambda msg: log_queue.put(str(msg)))
                result_queue.put((True, result))
            except Exception:
                result_queue.put((False, traceback.format_exc()))

        threading.Thread(target=worker, daemon=True).start()

    def poll() -> None:
        while True:
            try:
                append_log(log_queue.get_nowait())
            except queue.Empty:
                break
        try:
            ok, payload = result_queue.get_nowait()
        except queue.Empty:
            root.after(120, poll)
            return
        worker_state["running"] = False
        if ok:
            paths = payload.get("paths", {})
            append_log("Batch setup complete.")
            append_log(f"Complete batch: {paths.get('complete_batch', '')}")
            append_log(f"Fraction batch: {paths.get('fraction_batch', '')}")
            append_log(f"Complete feature-table folder: {paths.get('complete_csv', '')}")
            append_log(f"Fraction feature-table folder: {paths.get('fraction_dir', '')}")
            status.set("Batch setup complete.")
            messagebox.showinfo("Batch setup complete", "Configured complete and fraction .mzbatch files were created.")
        else:
            append_log(payload)
            status.set("Batch setup failed.")
            messagebox.showerror("Batch setup failed", str(payload).splitlines()[-1])
        root.after(120, poll)

    header = ctk.CTkFrame(root, fg_color="transparent")
    header.pack(fill="x", padx=24, pady=(18, 10))
    header.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(header, text="MZmine batch setup", font=("Segoe UI", 26, "bold"), text_color=COLORS["text"]).grid(row=0, column=0, sticky="w")
    ctk.CTkLabel(header, text="Create configured complete and fraction .mzbatch files. This step does not start MZmine.", font=("Segoe UI", 12), text_color=COLORS["muted"]).grid(row=1, column=0, sticky="w", pady=(4, 0))
    make_button(header, "Run setup", run_setup, primary=True, width=130).grid(row=0, column=1, rowspan=2, padx=(8, 0))
    make_button(header, "Save config", save_config, width=120).grid(row=0, column=2, rowspan=2, padx=(8, 0))
    make_button(header, "Load config", load_config, width=120).grid(row=0, column=3, rowspan=2, padx=(8, 0))
    make_button(header, "Close", root.destroy, danger=True, width=100).grid(row=0, column=4, rowspan=2, padx=(8, 0))

    main_frame = ctk.CTkScrollableFrame(root, fg_color="transparent")
    main_frame.pack(fill="both", expand=True, padx=18, pady=(0, 10))


    if project_samples:
        project_card = ctk.CTkFrame(main_frame, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, corner_radius=14)
        project_card.pack(fill="x", padx=12, pady=(0, 12))
        project_card.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(project_card, text="1. Select sample", font=("Segoe UI", 15, "bold"), text_color=COLORS["text"], anchor="w").grid(row=0, column=0, sticky="w", padx=(18, 12), pady=(14, 4))
        ctk.CTkLabel(project_card, text="Choose a launcher sample to prefill HPLC files and output folders.", font=("Segoe UI", 11), text_color=COLORS["muted"], anchor="w").grid(row=1, column=0, sticky="w", padx=(18, 12), pady=(0, 12))
        sample_menu = ctk.CTkOptionMenu(
            project_card,
            variable=var_project_sample,
            values=[sample_prompt, *project_sample_names],
            command=lambda _choice: apply_project_sample(),
            fg_color=COLORS["entry"],
            button_color=COLORS["accent"],
            button_hover_color=COLORS["accent_hover"],
            dropdown_fg_color=COLORS["card_alt"],
            text_color=COLORS["text"],
            height=36,
            corner_radius=8,
        )
        sample_menu.grid(row=0, column=1, rowspan=2, sticky="ew", padx=(0, 10), pady=14)
        make_button(project_card, "Enter paths directly", show_direct_entry_fields, width=150).grid(row=0, column=2, rowspan=2, sticky="e", padx=(0, 18), pady=14)
        ctk.CTkLabel(project_card, textvariable=project_status, font=("Segoe UI", 11), text_color=COLORS["success"], anchor="w", wraplength=1080).grid(row=2, column=0, columnspan=3, sticky="ew", padx=18, pady=(0, 12))
    else:
        warning_card = card(main_frame, "Direct path entry", "No launcher samples were found. Add samples in the main launcher for the guided workflow, or enter paths directly here.")
        ctk.CTkLabel(warning_card, textvariable=project_status, font=("Segoe UI", 11), text_color="#d9a441", anchor="w", wraplength=980).grid(row=2, column=0, columnspan=5, sticky="ew", padx=18, pady=(0, 14))

    sample_card = card(main_frame, "Batch setup fields", "These fields appear after choosing a launcher sample. They are prefilled from the launcher but can still be edited before running setup.", visible=not bool(project_samples))
    setup_cards.append(sample_card)
    labeled(sample_card, "File-safe sample name", sample_name, 2, "Used for configured batch filenames and the final filtered feature table. Usually inherited from the launcher sample.", placeholder="ruta_corsica_107")
    ctk.CTkLabel(sample_card, text="HPLC sample injection files", text_color=COLORS["muted"], anchor="w").grid(row=3, column=0, sticky="nw", padx=(18, 10), pady=7)
    make_help(sample_card, "Raw HPLC-MS mzML files measured for this extract. These are written into complete and fraction MZmine batches.").grid(row=3, column=0, sticky="e", padx=(0, 10), pady=7)
    sample_files_text = ctk.CTkTextbox(sample_card, height=86, fg_color=COLORS["entry"], border_color=COLORS["border"], border_width=1, text_color=COLORS["text"])
    sample_files_text.grid(row=3, column=1, columnspan=3, sticky="ew", padx=(0, 8), pady=7)
    sample_file_buttons = ctk.CTkFrame(sample_card, fg_color="transparent")
    sample_file_buttons.grid(row=3, column=4, sticky="n", padx=(0, 18), pady=7)
    make_button(sample_file_buttons, "Add files", lambda: add_files(sample_files_text, "Select HPLC sample injection files"), width=88).pack(side="left", padx=(0, 8))
    example_button(sample_file_buttons, "example_data/hplc_mzml/example_mzml_file_list.csv").pack(side="left")
    ctk.CTkLabel(sample_card, text="HPLC blank injection files", text_color=COLORS["muted"], anchor="w").grid(row=4, column=0, sticky="nw", padx=(18, 10), pady=7)
    make_help(sample_card, "Raw HPLC-MS mzML blank files used by complete-run blank filtering. Leave only true blanks here, not biological samples.").grid(row=4, column=0, sticky="e", padx=(0, 10), pady=7)
    blank_files_text = ctk.CTkTextbox(sample_card, height=66, fg_color=COLORS["entry"], border_color=COLORS["border"], border_width=1, text_color=COLORS["text"])
    blank_files_text.grid(row=4, column=1, columnspan=3, sticky="ew", padx=(0, 8), pady=7)
    blank_file_buttons = ctk.CTkFrame(sample_card, fg_color="transparent")
    blank_file_buttons.grid(row=4, column=4, sticky="n", padx=(0, 18), pady=7)
    make_button(blank_file_buttons, "Add files", lambda: add_files(blank_files_text, "Select HPLC blank injection files"), width=88).pack(side="left", padx=(0, 8))
    example_button(blank_file_buttons, "example_data/hplc_mzml/example_mzml_file_list.csv").pack(side="left")

    complete_card = card(main_frame, "Complete-run batch", "The complete batch processes the full sample chromatogram and exports the complete feature table.", visible=not bool(project_samples))
    setup_cards.append(complete_card)
    labeled(complete_card, "Complete template", complete_template, 2, "Optimized MZmine batch template for the complete sample run.", browse=lambda: browse_file(complete_template, "Select complete .mzbatch template", [("MZmine batch", "*.mzbatch"), ("All files", "*.*")]))
    labeled(complete_card, "Configured batch folder", complete_batch_dir, 3, "Folder where the configured complete .mzbatch will be saved.", browse=lambda: browse_dir(complete_batch_dir, "Select configured batch folder"))
    labeled(complete_card, "Complete feature-table folder", complete_feature_dir, 4, "Folder written into the complete batch CSV export step. MZmine should later create sample_complete_feature_table.csv here.", browse=lambda: browse_dir(complete_feature_dir, "Select complete feature-table folder"))
    labeled(complete_card, "Blank pattern", blank_pattern, 5, "Text pattern used by blank filtering modules inside the MZmine batch, for example *blank*.")

    fraction_card = card(main_frame, "Fraction batch", "The fraction template is replicated into one MZmine step block per collected HPLC fraction.", visible=not bool(project_samples))
    setup_cards.append(fraction_card)
    labeled(fraction_card, "Fraction template", fraction_template, 2, "MZmine template optimized for one representative fraction, usually frac_01.", browse=lambda: browse_file(fraction_template, "Select fraction .mzbatch template", [("MZmine batch", "*.mzbatch"), ("All files", "*.*")]))
    labeled(fraction_card, "Configured batch folder", fraction_batch_dir, 3, "Folder where the configured fraction .mzbatch will be saved.", browse=lambda: browse_dir(fraction_batch_dir, "Select configured batch folder"))
    labeled(fraction_card, "Fraction feature-table folder", fraction_feature_dir, 4, "Folder written into every fraction CSV export step. Export filenames are forced to frac_01.csv, frac_02.csv, and so on.", browse=lambda: browse_dir(fraction_feature_dir, "Select fraction feature-table folder"))
    summary_row = ctk.CTkFrame(fraction_card, fg_color=COLORS["card_alt"], corner_radius=10)
    summary_row.grid(row=5, column=0, columnspan=5, sticky="ew", padx=18, pady=(8, 16))
    ctk.CTkLabel(summary_row, textvariable=fraction_summary, font=("Segoe UI", 11), text_color=COLORS["muted"], anchor="w", justify="left", wraplength=1080).pack(fill="x", padx=12, pady=10)

    log_card = card(main_frame, "Log", visible=not bool(project_samples))
    setup_cards.append(log_card)
    log_box = ctk.CTkTextbox(log_card, height=180, fg_color=COLORS["entry"], border_color=COLORS["border"], border_width=1, text_color=COLORS["text"], font=("Consolas", 11))
    log_box.grid(row=2, column=0, columnspan=5, sticky="nsew", padx=18, pady=(0, 18))
    log_box.configure(state="disabled")

    footer = ctk.CTkFrame(root, fg_color=COLORS["surface"], corner_radius=0)
    footer.pack(fill="x")
    ctk.CTkLabel(footer, textvariable=status, text_color=COLORS["muted"], anchor="w").pack(fill="x", padx=18, pady=8)

    apply_config(config)
    root.after(120, poll)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
