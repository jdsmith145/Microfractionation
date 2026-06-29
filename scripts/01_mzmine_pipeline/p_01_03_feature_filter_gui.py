#!/usr/bin/env python3
"""Focused GUI for matching fraction feature tables to the complete feature table."""
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



def format_target_mz_for_gui(value: Any) -> str:
    """Format saved target m/z values for the GUI entry."""
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        return ", ".join(str(item) for item in value)
    return str(value)

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
    root.title("Feature filtering")
    root.geometry("1180x760")
    root.minsize(960, 660)
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
        return str(sample.get("display_name") or sample.get("file_safe_name") or sample.get("slug") or "Unnamed sample")

    def sample_file_safe_name(sample: dict[str, Any]) -> str:
        text = str(sample.get("file_safe_name") or sample.get("slug") or sample_label(sample).lower().replace(" ", "_")).strip()
        return text or "sample"

    def project_sample_by_label(label: str) -> dict[str, Any] | None:
        return next((sample for sample in project_samples if sample_label(sample) == label), None)

    def sample_pipeline_root(sample: dict[str, Any]) -> Path:
        configured = str(sample.get("output_folder") or "").strip()
        if configured:
            return resolve_project_path(configured)
        return project_root / "output" / "01_mzmine_pipeline" / sample_file_safe_name(sample)

    project_sample_names = [sample_label(sample) for sample in project_samples]
    sample_prompt = "Select a sample..."
    setup_cards: list[Any] = []
    setup_cards_visible = tk.BooleanVar(value=not bool(project_samples))
    direct_entry_enabled = tk.BooleanVar(value=not bool(project_samples))
    log_queue: queue.Queue[str] = queue.Queue()
    result_queue: queue.Queue[tuple[bool, Any]] = queue.Queue()
    worker_state = {"running": False}

    sample_name = tk.StringVar(value=str(cfg.get("sample_name", "")))
    complete_csv = tk.StringVar(value="")
    fraction_dir = tk.StringVar(value="")
    output_dir = tk.StringVar(value=str(cfg["matching"]["outdir"]))
    mz_tol = tk.StringVar(value=str(cfg["matching"]["mz_tol"]))
    rt_tol = tk.StringVar(value=str(cfg["matching"]["rt_tol"]))
    target_mz = tk.StringVar(value=format_target_mz_for_gui(cfg["matching"].get("target_mz", "")))
    target_mz_tolerance = tk.StringVar(value=str(cfg["matching"].get("target_mz_tolerance", 0.1)))
    output_preview = tk.StringVar(value="")
    status = tk.StringVar(value="Ready.")
    project_sample = tk.StringVar(value=sample_prompt)
    project_status = tk.StringVar(value="Select a launcher sample to fill filtering inputs automatically." if project_samples else "No launcher samples were provided. Enter the paths below.")

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
            ctk.CTkLabel(frame, text=subtitle, font=("Segoe UI", 11), text_color=COLORS["muted"], anchor="w", justify="left", wraplength=980).grid(row=1, column=0, columnspan=5, sticky="ew", padx=18, pady=(0, 12))
        return frame

    def example_button(parent: Any, relative_path: str, *, width: int = 82) -> Any:
        return make_button(parent, "Example", lambda: open_example(__file__, relative_path, messagebox=messagebox), width=width)

    def labeled(
        parent: Any,
        label: str,
        var: tk.StringVar,
        row: int,
        help_text: str,
        *,
        browse: Any = None,
        placeholder: str = "",
        example_path: str | None = None,
    ) -> None:
        label_frame = ctk.CTkFrame(parent, fg_color="transparent")
        label_frame.grid(row=row, column=0, sticky="ew", padx=(18, 10), pady=7)
        ctk.CTkLabel(label_frame, text=label, font=("Segoe UI", 12), text_color=COLORS["muted"], anchor="w").pack(side="left")
        make_help(label_frame, help_text).pack(side="left", padx=(8, 0))
        make_entry(parent, var, placeholder).grid(row=row, column=1, columnspan=2 if example_path else 3, sticky="ew", padx=(0, 8), pady=7)
        if browse:
            make_button(parent, "Browse", browse, width=88).grid(row=row, column=3 if example_path else 4, sticky="e", padx=(0, 8 if example_path else 18), pady=7)
        if example_path:
            example_button(parent, example_path).grid(row=row, column=4, sticky="e", padx=(0, 18), pady=7)

    def browse_file(var: tk.StringVar, title: str) -> None:
        path = filedialog.askopenfilename(title=title, initialdir=str(base_dir), filetypes=[("CSV file", "*.csv"), ("All files", "*.*")])
        if path:
            var.set(path)
            refresh_preview()

    def browse_dir(var: tk.StringVar, title: str) -> None:
        path = filedialog.askdirectory(title=title, initialdir=str(base_dir))
        if path:
            var.set(path)
            refresh_preview()

    def refresh_preview(*_args: Any) -> None:
        try:
            resolved_outdir = core.resolve_path(base_dir, output_dir.get().strip()) or base_dir
            output_preview.set(str(core.output_path_for(sample_name.get().strip(), resolved_outdir)))
        except Exception:
            output_preview.set("(set sample name and output folder)")

    def reveal_setup_cards() -> None:
        if setup_cards_visible.get():
            return
        setup_cards_visible.set(True)
        for frame in setup_cards:
            frame.pack(fill="x", padx=12, pady=(0, 12))

    def enter_paths_directly() -> None:
        direct_entry_enabled.set(True)
        reveal_setup_cards()
        project_status.set("Direct path entry is open. Use this only when recovering or testing outside the launcher sample setup.")
        status.set("Direct path entry is open.")

    def sample_menu_changed(_choice: str | None = None) -> None:
        if project_sample.get() == sample_prompt:
            project_status.set("Select a launcher sample to fill filtering inputs automatically.")
            return
        apply_project_sample()

    def apply_project_sample(*_args: Any, notify: bool = True) -> None:
        sample = project_sample_by_label(project_sample.get())
        if not sample:
            project_status.set("Select a launcher sample first, or choose Enter paths directly.")
            return
        label = sample_label(sample)
        file_safe_name = sample_file_safe_name(sample)
        pipeline_root = sample_pipeline_root(sample)
        sample_name.set(file_safe_name)
        complete_csv.set(str(pipeline_root / "complete_feature_table" / f"{file_safe_name}_complete_feature_table.csv"))
        fraction_dir.set(str(pipeline_root / "fraction_feature_tables"))
        output_dir.set(str(pipeline_root / "filtered_feature_table"))
        project_status.set(
            "\n".join(
                [
                    f"Loaded launcher sample: {label}",
                    f"Complete feature table: {complete_csv.get()}",
                    f"Fraction CSV folder: {fraction_dir.get()}",
                    f"Filtered output folder: {output_dir.get()}",
                ]
            )
        )
        reveal_setup_cards()
        direct_entry_enabled.set(False)
        refresh_preview()
        if notify:
            status.set(f"Loaded sample: {label}")

    def apply_config(config: dict[str, Any]) -> None:
        nonlocal cfg, base_dir
        cfg = core._normalize_legacy_config(dict(config))
        base_dir = Path(cfg.get("base_dir", THIS_DIR)).expanduser()
        sample_name.set(str(cfg.get("sample_name", "")))
        paths = core.resolved_pipeline_paths(cfg, base_dir=base_dir)
        complete_csv.set(paths["complete_csv"])
        fraction_dir.set(paths["fraction_dir"])
        matching = cfg.get("matching", {})
        output_dir.set(str(matching.get("outdir", core.DEFAULT_FILTERED_FEATURE_DIR)))
        mz_tol.set(str(matching.get("mz_tol", core.DEFAULT_MZ_TOL)))
        rt_tol.set(str(matching.get("rt_tol", core.DEFAULT_RT_TOL)))
        target_mz.set(format_target_mz_for_gui(matching.get("target_mz", "")))
        target_mz_tolerance.set(str(matching.get("target_mz_tolerance", 0.1)))
        refresh_preview()

    def collect_config() -> dict[str, Any]:
        out = dict(cfg)
        out["sample_name"] = sample_name.get().strip()
        out.setdefault("matching", {})
        out["matching"].update({
            "outdir": output_dir.get().strip(),
            "mz_tol": float(mz_tol.get()),
            "rt_tol": float(rt_tol.get()),
            "target_mz": target_mz.get().strip(),
            "target_mz_tolerance": float(target_mz_tolerance.get().strip() or 0.1),
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

    def run_filtering() -> None:
        if worker_state["running"]:
            return
        if project_samples and not setup_cards_visible.get() and not direct_entry_enabled.get():
            project_status.set("Select a launcher sample first, or choose Enter paths directly.")
            status.set("Select a launcher sample first, or choose Enter paths directly.")
            return
        try:
            settings = core.MatchSettings(
                fractions_dir=core.resolve_path(base_dir, fraction_dir.get().strip()) or Path(fraction_dir.get().strip()),
                big_csv=core.resolve_path(base_dir, complete_csv.get().strip()) or Path(complete_csv.get().strip()),
                sample_name=sample_name.get().strip(),
                outdir=core.resolve_path(base_dir, output_dir.get().strip()) or Path(output_dir.get().strip()),
                mz_tol=float(mz_tol.get()),
                rt_tol=float(rt_tol.get()),
                target_mz=core.parse_target_mz_values(target_mz.get()),
                target_mz_tolerance=float(target_mz_tolerance.get().strip() or 0.1),
            )
        except Exception as exc:
            messagebox.showerror("Feature filtering", str(exc))
            return
        worker_state["running"] = True
        status.set("Filtering fraction features...")
        append_log("Running fraction-feature matching...")

        def worker() -> None:
            try:
                result = core.run_match(settings)
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
            append_log(f"Saved filtered feature table: {payload['output_path']}")
            append_log(f"Saved purity estimate table: {payload['purity_output_path']}")
            append_log(f"Fraction CSV files: {payload['fraction_file_count']}")
            append_log(f"Matched rows: {payload['matched_rows']}")
            refresh_preview()
            status.set("Feature filtering complete.")
            messagebox.showinfo("Feature filtering complete", f"Filtered feature table saved:\n{payload['output_path']}")
        else:
            append_log(payload)
            status.set("Feature filtering failed.")
            messagebox.showerror("Feature filtering failed", str(payload).splitlines()[-1])
        root.after(120, poll)

    header = ctk.CTkFrame(root, fg_color="transparent")
    header.pack(fill="x", padx=24, pady=(18, 10))
    header.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(header, text="Feature filtering", font=("Segoe UI", 26, "bold"), text_color=COLORS["text"]).grid(row=0, column=0, sticky="w")
    ctk.CTkLabel(header, text="Match fraction feature tables against the complete feature table and export one filtered HPLC feature table.", font=("Segoe UI", 12), text_color=COLORS["muted"]).grid(row=1, column=0, sticky="w", pady=(4, 0))
    make_button(header, "Run filtering", run_filtering, primary=True, width=130).grid(row=0, column=1, rowspan=2, padx=(8, 0))
    make_button(header, "Save config", save_config, width=120).grid(row=0, column=2, rowspan=2, padx=(8, 0))
    make_button(header, "Load config", load_config, width=120).grid(row=0, column=3, rowspan=2, padx=(8, 0))
    make_button(header, "Close", root.destroy, danger=True, width=100).grid(row=0, column=4, rowspan=2, padx=(8, 0))

    main_frame = ctk.CTkScrollableFrame(root, fg_color="transparent")
    main_frame.pack(fill="both", expand=True, padx=18, pady=(0, 10))

    if project_samples:
        project_card = card(main_frame, "1. Select sample", "Choose the sample you added in the main launcher. This fills the complete table, fraction folder, and filtered output path automatically.")
        project_card.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(project_card, text="Sample", font=("Segoe UI", 12), text_color=COLORS["muted"], anchor="w").grid(row=2, column=0, sticky="w", padx=(18, 10), pady=8)
        sample_menu = ctk.CTkOptionMenu(project_card, variable=project_sample, values=[sample_prompt, *project_sample_names], command=sample_menu_changed, fg_color=COLORS["entry"], button_color=COLORS["accent"], button_hover_color=COLORS["accent_hover"], dropdown_fg_color=COLORS["card_alt"], text_color=COLORS["text"], height=36, corner_radius=8)
        sample_menu.grid(row=2, column=1, sticky="ew", padx=(0, 8), pady=8)
        make_button(project_card, "Enter paths directly", enter_paths_directly, width=160).grid(row=2, column=2, sticky="w", padx=(0, 18), pady=8)
        ctk.CTkLabel(project_card, textvariable=project_status, font=("Segoe UI", 11), text_color=COLORS["muted"], anchor="w", justify="left", wraplength=980).grid(row=3, column=0, columnspan=5, sticky="ew", padx=18, pady=(0, 14))

    settings_card = card(main_frame, "Input and output tables", "Run this after MZmine has created the complete feature table and the fraction CSV files.", visible=not bool(project_samples))
    if project_samples:
        setup_cards.append(settings_card)
    labeled(settings_card, "Complete feature table", complete_csv, 2, "Complete feature table exported by the complete MZmine batch.", browse=lambda: browse_file(complete_csv, "Select complete feature table CSV"), example_path="example_data/mzmine_outputs/complete_feature_table/example_complete_feature_table.csv")
    labeled(settings_card, "Fraction CSV folder", fraction_dir, 3, "Folder containing frac_01.csv, frac_02.csv, and so on from the fraction MZmine batch.", browse=lambda: browse_dir(fraction_dir, "Select fraction CSV folder"), example_path="example_data/mzmine_outputs/fraction_feature_tables")
    labeled(settings_card, "Filtered output folder", output_dir, 4, "Folder where the filtered HPLC feature table will be written.", browse=lambda: browse_dir(output_dir, "Select filtered output folder"))
    labeled(settings_card, "m/z tolerance", mz_tol, 5, "Maximum m/z difference allowed between fraction features and complete-table features.")
    labeled(settings_card, "RT tolerance", rt_tol, 6, "Maximum retention-time difference in minutes allowed between fraction features and complete-table features.")
    labeled(settings_card, "Target m/z values for proportion", target_mz, 7, "Optional. Enter one or more m/z values separated by commas, spaces, semicolons, or new lines. The output table estimates each target's peak-area proportion in every fraction. Leave empty to export only the dominant-feature proportion.")
    labeled(settings_card, "Target m/z tolerance", target_mz_tolerance, 8, "m/z window used for every target proportion calculation. Example: 0.1 means each target m/z plus or minus 0.1.")
    labeled(settings_card, "Filtered output", output_preview, 9, "Expected filtered HPLC feature table path. The filename uses the sample selected in the launcher. A matching fraction_purity_estimates CSV is written beside it.")

    log_card = card(main_frame, "Log", visible=not bool(project_samples))
    if project_samples:
        setup_cards.append(log_card)
    log_box = ctk.CTkTextbox(log_card, height=260, fg_color=COLORS["entry"], border_color=COLORS["border"], border_width=1, text_color=COLORS["text"], font=("Consolas", 11))
    log_box.grid(row=2, column=0, columnspan=5, sticky="nsew", padx=18, pady=(0, 18))
    log_box.configure(state="disabled")

    footer = ctk.CTkFrame(root, fg_color=COLORS["surface"], corner_radius=0)
    footer.pack(fill="x")
    ctk.CTkLabel(footer, textvariable=status, text_color=COLORS["muted"], anchor="w").pack(fill="x", padx=18, pady=8)

    sample_name.trace_add("write", refresh_preview)
    output_dir.trace_add("write", refresh_preview)
    apply_config(cfg)
    root.after(120, poll)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
