#!/usr/bin/env python3
"""Focused GUI for running configured MZmine batches."""
from __future__ import annotations

import json
import os
import queue
import re
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
    root.title("MZmine runner")
    root.geometry("1320x860")
    root.minsize(1040, 720)
    root.configure(fg_color=COLORS["bg"])
    help_popovers = HelpPopoverController(root, ctk, COLORS, ("Segoe UI", 11))

    cfg = core.template_config()
    base_dir = Path(cfg.get("base_dir", THIS_DIR))


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
    sample_prompt = "Select a sample..."
    setup_cards: list[Any] = []
    setup_cards_visible = tk.BooleanVar(value=not bool(project_samples))
    direct_entry_enabled = tk.BooleanVar(value=not bool(project_samples))
    log_queue: queue.Queue[str] = queue.Queue()
    progress_queue: queue.Queue[tuple[str, str, int | None]] = queue.Queue()
    result_queue: queue.Queue[tuple[bool, Any]] = queue.Queue()
    worker_state = {"running": False}
    progress_state: dict[str, Any] = {"total": 0, "current": 0, "stage_totals": {}, "stage_offsets": {}}

    run_complete = tk.BooleanVar(value=True)
    run_fraction = tk.BooleanVar(value=True)
    complete_batch = tk.StringVar(value="")
    fraction_batch = tk.StringVar(value="")
    complete_csv = tk.StringVar(value="")
    fraction_dir = tk.StringVar(value="")
    mzmine_exe = tk.StringVar(value=str(cfg.get("mzmine", {}).get("executable") or cfg.get("mzmine", {}).get("portable_console", "mzmine_Windows_portable_4.7.8/mzmine_console.exe")))
    user_file = tk.StringVar(value=str(cfg.get("mzmine", {}).get("user_file", "")))
    temp_dir = tk.StringVar(value=str(cfg.get("mzmine", {}).get("temp_dir", "outputs/mzmine_temp")))
    memory = tk.StringVar(value=str(cfg.get("mzmine", {}).get("memory", core.DEFAULT_MEMORY_MODE)))
    threads = tk.StringVar(value=str(cfg.get("mzmine", {}).get("threads", core.DEFAULT_THREADS)))
    ignore_warnings = tk.BooleanVar(value=bool(cfg.get("mzmine", {}).get("ignore_parameter_warnings", core.DEFAULT_IGNORE_PARAMETER_WARNINGS)))
    status = tk.StringVar(value="Ready.")
    progress_text = tk.StringVar(value="Progress: waiting to run.")
    var_project_sample = tk.StringVar(value=sample_prompt)
    project_status = tk.StringVar(value="Select a launcher sample to fill configured batch paths automatically." if project_samples else "No launcher samples were provided. Enter the paths below.")

    def make_button(parent: Any, text: str, command: Any, *, primary: bool = False, danger: bool = False, width: int = 118) -> Any:
        color = COLORS["danger"] if danger else COLORS["accent"] if primary else COLORS["card_alt"]
        hover = "#9e3b3b" if danger else COLORS["accent_hover"] if primary else "#39414c"
        return ctk.CTkButton(parent, text=text, command=command, width=width, height=38, corner_radius=10, fg_color=color, hover_color=hover)

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

    def reveal_setup_cards() -> None:
        if setup_cards_visible.get():
            return
        setup_cards_visible.set(True)
        for frame in setup_cards:
            frame.pack(fill="x", padx=12, pady=(0, 12))

    def enter_paths_directly() -> None:
        direct_entry_enabled.set(True)
        reveal_setup_cards()
        project_status.set("Direct path entry is open. Use this only when recovering or testing a run outside the launcher sample setup.")
        status.set("Direct path entry is open.")

    def sample_menu_changed(_choice: str | None = None) -> None:
        if var_project_sample.get() == sample_prompt:
            project_status.set("Select a launcher sample to fill configured batch paths automatically.")
            return
        apply_project_sample()


    def apply_project_sample(*_args: Any, notify: bool = True) -> None:
        sample = project_sample_by_label(var_project_sample.get())
        if not sample:
            project_status.set("Select a launcher sample first, or choose Enter paths directly.")
            return
        slug = sample_slug(sample)
        pipeline_root = sample_pipeline_root(sample)
        configured_dir = pipeline_root / "configured_batches"
        complete_batch.set(str(configured_dir / f"{slug}_configured.mzbatch"))
        fraction_batch.set(str(configured_dir / f"{slug}_fraction_configured.mzbatch"))
        complete_csv.set(str(pipeline_root / "complete_feature_table" / f"{slug}_complete_feature_table.csv"))
        fraction_dir.set(str(pipeline_root / "fraction_feature_tables"))
        mzmine_cfg = project_config.get("mzmine", {}) if isinstance(project_config.get("mzmine", {}), dict) else {}
        console = mzmine_cfg.get("portable_console") or mzmine_cfg.get("executable")
        if console:
            mzmine_exe.set(str(resolve_project_path(console)))
        if mzmine_cfg.get("user_file"):
            user_file.set(str(resolve_project_path(mzmine_cfg.get("user_file"))))
        temp_dir.set(str(pipeline_root / "mzmine_temp"))
        memory.set(str(mzmine_cfg.get("memory", memory.get() or core.DEFAULT_MEMORY_MODE)))
        threads.set(str(mzmine_cfg.get("threads", threads.get() or core.DEFAULT_THREADS)))
        if "ignore_parameter_warnings" in mzmine_cfg:
            ignore_warnings.set(bool(mzmine_cfg.get("ignore_parameter_warnings")))
        reveal_setup_cards()
        direct_entry_enabled.set(False)
        project_status.set(f"Loaded sample: {sample_label(sample)}. Review the configured batch paths, then run MZmine.")
        if notify:
            status.set(f"Loaded sample: {sample_label(sample)}")

    def update_paths_from_config(config: dict[str, Any]) -> None:
        paths = core.resolved_pipeline_paths(config, base_dir=config.get("base_dir", base_dir))
        complete_batch.set(paths["complete_batch"])
        fraction_batch.set(paths["fraction_batch"])
        complete_csv.set(paths["complete_csv"])
        fraction_dir.set(paths["fraction_dir"])

    def apply_config(config: dict[str, Any]) -> None:
        nonlocal cfg, base_dir
        cfg = core._normalize_legacy_config(dict(config))
        base_dir = Path(cfg.get("base_dir", THIS_DIR)).expanduser()
        mzmine = cfg.get("mzmine", {})
        mzmine_exe.set(str(mzmine.get("executable") or mzmine.get("portable_console", "mzmine_Windows_portable_4.7.8/mzmine_console.exe")))
        user_file.set(str(mzmine.get("user_file", "")))
        temp_dir.set(str(mzmine.get("temp_dir", "outputs/mzmine_temp")))
        memory.set(str(mzmine.get("memory", core.DEFAULT_MEMORY_MODE)))
        threads.set(str(mzmine.get("threads", core.DEFAULT_THREADS)))
        ignore_warnings.set(bool(mzmine.get("ignore_parameter_warnings", core.DEFAULT_IGNORE_PARAMETER_WARNINGS)))
        update_paths_from_config(cfg)

    def collect_config() -> dict[str, Any]:
        out = dict(cfg)
        out.setdefault("mzmine", {})
        out["mzmine"].update({
            "executable": mzmine_exe.get().strip(),
            "user_file": user_file.get().strip(),
            "temp_dir": temp_dir.get().strip(),
            "memory": memory.get().strip() or core.DEFAULT_MEMORY_MODE,
            "threads": threads.get().strip() or core.DEFAULT_THREADS,
            "ignore_parameter_warnings": bool(ignore_warnings.get()),
        })
        return out

    def save_config() -> None:
        path = filedialog.asksaveasfilename(title="Save MZmine config", initialdir=str(base_dir), defaultextension=".json", filetypes=[("JSON config", "*.json"), ("All files", "*.*")])
        if path:
            try:
                core.save_config(collect_config(), path)
                status.set(f"Saved config: {path}")
            except Exception as exc:
                messagebox.showerror("Save config", str(exc))

    def load_config() -> None:
        path = filedialog.askopenfilename(title="Load MZmine config", initialdir=str(base_dir), filetypes=[("JSON config", "*.json"), ("All files", "*.*")])
        if path:
            try:
                apply_config(core.load_config(path))
                status.set(f"Loaded config: {path}")
            except Exception as exc:
                messagebox.showerror("Load config", str(exc))

    def append_log(message: str) -> None:
        log_box.configure(state="normal")
        log_box.insert("end", message + "\n")
        log_box.see("end")
        log_box.configure(state="disabled")

    step_re = re.compile(r"\bStarting\s+step\s*#\s*(\d+)\b", re.IGNORECASE)

    def stage_name(stage: str) -> str:
        return "complete batch" if stage == "run_complete" else "fraction batch" if stage == "run_fraction" else stage

    def count_batch_steps(path: Path, label: str) -> int:
        try:
            return core.count_mzmine_batch_steps(path)
        except Exception as exc:
            append_log(f"Progress warning: could not count {label} steps before run: {exc}")
            return 0

    def prepare_progress(stage_paths: dict[str, Path]) -> None:
        stage_totals: dict[str, int] = {}
        for stage, path in stage_paths.items():
            stage_totals[stage] = count_batch_steps(path, stage_name(stage))
        offset = 0
        stage_offsets: dict[str, int] = {}
        for stage in ("run_complete", "run_fraction"):
            if stage in stage_totals:
                stage_offsets[stage] = offset
                offset += stage_totals[stage]
        progress_state.update({"total": offset, "current": 0, "stage_totals": stage_totals, "stage_offsets": stage_offsets})
        progress_bar.set(0)
        if offset:
            progress_text.set(f"Progress: 0 / {offset} MZmine steps.")
        else:
            progress_text.set("Progress: waiting for MZmine step messages.")

    def set_progress(done: int, label: str) -> None:
        total = int(progress_state.get("total") or 0)
        done = max(0, min(int(done), total)) if total else max(0, int(done))
        progress_state["current"] = done
        if total:
            progress_bar.set(done / total)
            progress_text.set(f"{label} ({done} / {total} steps, {done / total:.0%})")
        else:
            progress_bar.set(0)
            progress_text.set(label)

    def handle_progress_event(event: tuple[str, str, int | None]) -> None:
        kind, stage, step = event
        total = int(progress_state.get("stage_totals", {}).get(stage, 0) or 0)
        offset = int(progress_state.get("stage_offsets", {}).get(stage, 0) or 0)
        if kind == "stage_start":
            set_progress(offset, f"MZmine {stage_name(stage)}: starting.")
        elif kind == "step" and step is not None:
            shown_step = max(1, int(step))
            stage_done = min(shown_step, total) if total else shown_step
            if total:
                set_progress(offset + stage_done, f"MZmine {stage_name(stage)}: step {stage_done} / {total}.")
            else:
                set_progress(progress_state.get("current", 0), f"MZmine {stage_name(stage)}: step {shown_step}.")
        elif kind == "stage_done":
            set_progress(offset + total, f"MZmine {stage_name(stage)}: finished.")

    def make_progress_log_callback(stage: str) -> Any:
        def callback(message: str) -> None:
            text = str(message)
            log_queue.put(text)
            match = step_re.search(text)
            if match:
                progress_queue.put(("step", stage, int(match.group(1))))

        return callback

    def run_selected() -> None:
        if worker_state["running"]:
            return
        if project_samples and not setup_cards_visible.get() and not direct_entry_enabled.get():
            project_status.set("Select a launcher sample first, or choose Enter paths directly.")
            status.set("Select a launcher sample first, or choose Enter paths directly.")
            return
        stages: list[str] = []
        if run_complete.get():
            stages.append("run_complete")
        if run_fraction.get():
            stages.append("run_fraction")
        if not stages:
            messagebox.showwarning("MZmine runner", "Select at least one batch to run.")
            return
        config = collect_config()
        complete_batch_path = core.resolve_path(base_dir, complete_batch.get().strip()) or Path(complete_batch.get().strip())
        fraction_batch_path = core.resolve_path(base_dir, fraction_batch.get().strip()) or Path(fraction_batch.get().strip())
        complete_csv_path = core.resolve_path(base_dir, complete_csv.get().strip()) or Path(complete_csv.get().strip())
        fraction_csv_dir = core.resolve_path(base_dir, fraction_dir.get().strip()) or Path(fraction_dir.get().strip())
        stage_paths = {}
        if "run_complete" in stages:
            stage_paths["run_complete"] = complete_batch_path
        if "run_fraction" in stages:
            stage_paths["run_fraction"] = fraction_batch_path
        prepare_progress(stage_paths)
        worker_state["running"] = True
        status.set("Running MZmine...")
        append_log(f"Running selected MZmine batch stage(s): {', '.join(stages)}")

        def worker() -> None:
            try:
                settings = core.build_mzmine_settings(config, base_dir=config.get("base_dir", base_dir))
                result: dict[str, Any] = {"steps": {}}
                if "run_complete" in stages:
                    progress_queue.put(("stage_start", "run_complete", None))
                    result["steps"]["run_complete"] = core.run_mzmine_batch(
                        settings,
                        complete_batch_path,
                        log_callback=make_progress_log_callback("run_complete"),
                    )
                    progress_queue.put(("stage_done", "run_complete", None))
                    result["steps"]["complete_output_check"] = core.verify_complete_csv(complete_csv_path)
                if "run_fraction" in stages:
                    progress_queue.put(("stage_start", "run_fraction", None))
                    result["steps"]["run_fraction"] = core.run_mzmine_batch(
                        settings,
                        fraction_batch_path,
                        log_callback=make_progress_log_callback("run_fraction"),
                    )
                    progress_queue.put(("stage_done", "run_fraction", None))
                    result["steps"]["fraction_output_check"] = core.verify_fraction_csvs(fraction_csv_dir)
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
        while True:
            try:
                handle_progress_event(progress_queue.get_nowait())
            except queue.Empty:
                break
        try:
            ok, payload = result_queue.get_nowait()
        except queue.Empty:
            root.after(120, poll)
            return
        worker_state["running"] = False
        if ok:
            append_log("MZmine run complete.")
            set_progress(int(progress_state.get("total", 0) or progress_state.get("current", 0)), "MZmine run complete.")
            try:
                if run_complete.get():
                    core.verify_complete_csv(complete_csv.get())
                    append_log(f"Verified complete feature table: {complete_csv.get()}")
                if run_fraction.get():
                    summary = core.verify_fraction_csvs(fraction_dir.get())
                    append_log(f"Verified {summary['file_count']} fraction CSV file(s) in {fraction_dir.get()}")
            except Exception as exc:
                append_log(f"Output check warning: {exc}")
            status.set("MZmine run complete.")
            messagebox.showinfo("MZmine runner", "Selected MZmine batch run finished.")
        else:
            append_log(payload)
            status.set("MZmine run failed.")
            total = int(progress_state.get("total") or 0)
            current = int(progress_state.get("current") or 0)
            if total:
                progress_text.set(f"MZmine run failed at {current} / {total} steps ({current / total:.0%}).")
            else:
                progress_text.set("MZmine run failed before progress could be measured.")
            messagebox.showerror("MZmine run failed", str(payload).splitlines()[-1])
        root.after(120, poll)

    header = ctk.CTkFrame(root, fg_color="transparent")
    header.pack(fill="x", padx=24, pady=(18, 10))
    header.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(header, text="MZmine runner", font=("Segoe UI", 26, "bold"), text_color=COLORS["text"]).grid(row=0, column=0, sticky="w")
    ctk.CTkLabel(header, text="Run configured .mzbatch files through mzmine_console.exe and verify exported CSV tables.", font=("Segoe UI", 12), text_color=COLORS["muted"]).grid(row=1, column=0, sticky="w", pady=(4, 0))
    make_button(header, "Run selected", run_selected, primary=True, width=130).grid(row=0, column=1, rowspan=2, padx=(8, 0))
    make_button(header, "Save config", save_config, width=120).grid(row=0, column=2, rowspan=2, padx=(8, 0))
    make_button(header, "Load config", load_config, width=120).grid(row=0, column=3, rowspan=2, padx=(8, 0))
    make_button(header, "Close", root.destroy, danger=True, width=100).grid(row=0, column=4, rowspan=2, padx=(8, 0))

    main_frame = ctk.CTkScrollableFrame(root, fg_color="transparent")
    main_frame.pack(fill="both", expand=True, padx=18, pady=(0, 10))

    freeze_note = ctk.CTkFrame(main_frame, fg_color="#2f2a1d", border_color="#8a6d2f", border_width=1, corner_radius=12)
    freeze_note.pack(fill="x", padx=12, pady=(0, 12))
    freeze_note.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(
        freeze_note,
        text="MZmine can make this window look frozen during long batch steps.",
        font=("Segoe UI", 13, "bold"),
        text_color="#f3d28b",
        anchor="w",
    ).grid(row=0, column=0, sticky="ew", padx=14, pady=(10, 2))
    ctk.CTkLabel(
        freeze_note,
        text="Usually the run is still working; please be patient. If this computer struggles, open Advanced MZmine resource settings and lower Threads from auto to a smaller number such as 2 or 4.",
        font=("Segoe UI", 11),
        text_color="#d9c79b",
        anchor="w",
        justify="left",
        wraplength=1120,
    ).grid(row=1, column=0, sticky="ew", padx=14, pady=(0, 10))

    if project_samples:
        project_card = card(main_frame, "1. Select sample", "Choose the sample you added in the main launcher. This fills the configured batch paths and output folders automatically.")
        project_card.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(project_card, text="Sample", font=("Segoe UI", 12), text_color=COLORS["muted"], anchor="w").grid(row=2, column=0, sticky="w", padx=(18, 10), pady=8)
        sample_menu = ctk.CTkOptionMenu(project_card, variable=var_project_sample, values=[sample_prompt, *project_sample_names], command=sample_menu_changed, fg_color=COLORS["entry"], button_color=COLORS["accent"], button_hover_color=COLORS["accent_hover"], dropdown_fg_color=COLORS["card_alt"], text_color=COLORS["text"], height=36, corner_radius=8)
        sample_menu.grid(row=2, column=1, sticky="ew", padx=(0, 8), pady=8)
        make_button(project_card, "Enter paths directly", enter_paths_directly, width=160).grid(row=2, column=2, sticky="w", padx=(0, 18), pady=8)
        ctk.CTkLabel(project_card, textvariable=project_status, font=("Segoe UI", 11), text_color=COLORS["muted"], anchor="w", wraplength=980).grid(row=3, column=0, columnspan=5, sticky="ew", padx=18, pady=(0, 14))

    run_card = card(main_frame, "Batches to run", "Select which configured MZmine batch files should be executed. Most users run both after Batch setup.", visible=not bool(project_samples))
    if project_samples:
        setup_cards.append(run_card)
    complete_check_row = ctk.CTkFrame(run_card, fg_color="transparent")
    complete_check_row.grid(row=2, column=0, columnspan=2, sticky="w", padx=18, pady=8)
    ctk.CTkCheckBox(complete_check_row, text="Run complete batch", variable=run_complete, font=("Segoe UI", 12), text_color=COLORS["text"], fg_color=COLORS["accent"], hover_color=COLORS["accent_hover"], border_color=COLORS["border"]).pack(side="left")
    make_help(complete_check_row, "Runs the complete-sample MZmine batch. This should create the complete HPLC feature table that the filtering step uses as the reference table.").pack(side="left", padx=(8, 0))
    fraction_check_row = ctk.CTkFrame(run_card, fg_color="transparent")
    fraction_check_row.grid(row=2, column=2, columnspan=2, sticky="w", padx=18, pady=8)
    ctk.CTkCheckBox(fraction_check_row, text="Run fraction batch", variable=run_fraction, font=("Segoe UI", 12), text_color=COLORS["text"], fg_color=COLORS["accent"], hover_color=COLORS["accent_hover"], border_color=COLORS["border"]).pack(side="left")
    make_help(fraction_check_row, "Runs the replicated fraction MZmine batch. This should create one standardized fraction feature CSV per collected fraction, for example frac_01.csv, frac_02.csv, and so on.").pack(side="left", padx=(8, 0))
    labeled(run_card, "Complete batch", complete_batch, 3, "Configured complete .mzbatch created by Batch setup.", browse=lambda: browse_file(complete_batch, "Select complete .mzbatch", [("MZmine batch", "*.mzbatch"), ("All files", "*.*")]))
    labeled(run_card, "Fraction batch", fraction_batch, 4, "Configured fraction .mzbatch created by Batch setup.", browse=lambda: browse_file(fraction_batch, "Select fraction .mzbatch", [("MZmine batch", "*.mzbatch"), ("All files", "*.*")]))
    labeled(run_card, "Standard complete-table output", complete_csv, 5, "Standardized CSV path where MZmine should write the complete HPLC feature table. The next filtering step reads this path automatically, so keep it consistent unless you are recovering a manual run.", browse=lambda: browse_file(complete_csv, "Select complete feature table CSV", [("CSV file", "*.csv"), ("All files", "*.*")]))
    labeled(run_card, "Standard fraction-output folder", fraction_dir, 6, "Standardized folder where MZmine should write fraction CSV files. The filtering step reads frac_*.csv files from this folder automatically.", browse=lambda: browse_dir(fraction_dir, "Select fraction CSV folder"))

    mzmine_card = card(main_frame, "MZmine console settings", "Use mzmine_console.exe, not the graphical mzmine.exe. Recommended setup: download MZmine 4.7.8 portable from GitHub and unzip it directly into the Microfractionation folder.", visible=not bool(project_samples))
    if project_samples:
        setup_cards.append(mzmine_card)
    labeled(mzmine_card, "MZmine console", mzmine_exe, 2, "Path to mzmine_console.exe. The launcher expects the MZmine 4.7.8 portable release at mzmine_Windows_portable_4.7.8/mzmine_console.exe unless you choose another location.", browse=lambda: browse_file(mzmine_exe, "Select mzmine_console.exe", [("MZmine console", "mzmine_console.exe"), ("Executable", "*.exe"), ("All files", "*.*")]))
    labeled(mzmine_card, "MZmine user file", user_file, 3, "Optional .mzuser login file, usually under C:/Users/<name>/.mzmine/users/. Leave empty if MZmine already has a valid login.", browse=lambda: browse_file(user_file, "Select .mzuser file", [("MZmine user", "*.mzuser"), ("All files", "*.*")]))
    labeled(mzmine_card, "Temp folder", temp_dir, 4, "Folder where MZmine writes temporary processing files. Use a local folder with enough free disk space.", browse=lambda: browse_dir(temp_dir, "Select MZmine temp folder"))
    ctk.CTkCheckBox(mzmine_card, text="Continue after batch-version warnings", variable=ignore_warnings, font=("Segoe UI", 12), text_color=COLORS["text"], fg_color=COLORS["accent"], hover_color=COLORS["accent_hover"], border_color=COLORS["border"]).grid(row=5, column=1, columnspan=3, sticky="w", padx=(0, 8), pady=10)

    advanced_shell = ctk.CTkFrame(main_frame, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, corner_radius=14)
    if not project_samples:
        advanced_shell.pack(fill="x", padx=12, pady=(0, 12))
    else:
        setup_cards.append(advanced_shell)
    advanced_shell.grid_columnconfigure(0, weight=1)
    advanced_header = ctk.CTkFrame(advanced_shell, fg_color="transparent")
    advanced_header.grid(row=0, column=0, sticky="ew", padx=18, pady=(14, 14))
    advanced_header.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(advanced_header, text="Advanced MZmine resource settings", font=("Segoe UI", 16, "bold"), text_color=COLORS["text"], anchor="w").grid(row=0, column=0, sticky="ew")
    ctk.CTkLabel(advanced_header, text="Most users should keep these defaults. Open this only if MZmine needs less CPU or a different memory strategy on this computer.", font=("Segoe UI", 11), text_color=COLORS["muted"], anchor="w", justify="left", wraplength=980).grid(row=1, column=0, sticky="ew", pady=(4, 0))
    advanced_body = ctk.CTkFrame(advanced_shell, fg_color="transparent")
    advanced_body.grid_columnconfigure(1, weight=1)
    advanced_body.grid_columnconfigure(3, weight=1)

    def update_advanced_visibility() -> None:
        if advanced_visible.get():
            advanced_toggle.configure(text="Hide")
            advanced_body.grid(row=1, column=0, sticky="ew", padx=18, pady=(0, 18))
        else:
            advanced_toggle.configure(text="Show")
            advanced_body.grid_remove()

    def toggle_advanced_visibility() -> None:
        advanced_visible.set(not advanced_visible.get())
        update_advanced_visibility()

    advanced_visible = tk.BooleanVar(value=False)
    advanced_toggle = make_button(advanced_header, "Show", toggle_advanced_visibility, width=82)
    advanced_toggle.grid(row=0, column=1, rowspan=2, sticky="e", padx=(16, 0))
    labeled(advanced_body, "Memory mode", memory, 0, "MZmine memory strategy. Keep 'none' unless you know the machine has enough RAM for a more aggressive mode.")
    labeled(advanced_body, "Threads", threads, 1, "Use 'auto' for normal runs. Advanced users can type a number such as 2 or 4 to limit CPU usage.")
    update_advanced_visibility()

    progress_card = card(main_frame, "Run progress", "Step-based progress from the selected MZmine batch files. It shows completed batch steps, not predicted remaining time.", visible=not bool(project_samples))
    if project_samples:
        setup_cards.append(progress_card)
    progress_card.grid_columnconfigure(0, weight=1)
    progress_bar = ctk.CTkProgressBar(progress_card, height=16, progress_color=COLORS["accent"], fg_color=COLORS["entry"])
    progress_bar.grid(row=2, column=0, columnspan=5, sticky="ew", padx=18, pady=(0, 8))
    progress_bar.set(0)
    ctk.CTkLabel(progress_card, textvariable=progress_text, font=("Segoe UI", 11), text_color=COLORS["muted"], anchor="w").grid(row=3, column=0, columnspan=5, sticky="ew", padx=18, pady=(0, 14))

    log_card = card(main_frame, "MZmine log", visible=not bool(project_samples))
    if project_samples:
        setup_cards.append(log_card)
    log_box = ctk.CTkTextbox(log_card, height=300, fg_color=COLORS["entry"], border_color=COLORS["border"], border_width=1, text_color=COLORS["text"], font=("Consolas", 11))
    log_box.grid(row=2, column=0, columnspan=5, sticky="nsew", padx=18, pady=(0, 18))
    log_box.configure(state="disabled")

    footer = ctk.CTkFrame(root, fg_color=COLORS["surface"], corner_radius=0)
    footer.pack(fill="x")
    ctk.CTkLabel(footer, textvariable=status, text_color=COLORS["muted"], anchor="w").pack(fill="x", padx=18, pady=8)

    apply_config(cfg)
    root.after(120, poll)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

