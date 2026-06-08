#!/usr/bin/env python3
"""CustomTkinter GUI for the integrated MZmine pipeline."""
from __future__ import annotations

import argparse
import json
import queue
import re
import sys
import threading
import traceback
from pathlib import Path
from typing import Any

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))
_SHARED_DIR = _THIS_DIR.parent
if str(_SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(_SHARED_DIR))

from shared.gui_help_popover import HelpPopoverController

try:
    import p_01_00_mzmine_pipeline_core as core
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "ERROR: Could not import p_01_00_mzmine_pipeline_core.py.\n"
        "Place this GUI script in the same folder as the core script.\n\n"
        f"Details: {exc}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Launch the integrated MZmine pipeline GUI.")
    parser.add_argument("--config", help="Optional JSON config to load at startup.")
    args = parser.parse_args()

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

    state_file = _THIS_DIR / ".p_01_01_mzmine_pipeline_gui_state.json"
    table_patterns = [("CSV files", "*.csv"), ("All files", "*.*")]
    config_patterns = [("JSON config", "*.json"), ("All files", "*.*")]
    batch_patterns = [("MZmine batch files", "*.mzbatch"), ("All files", "*.*")]
    mzml_patterns = [("mzML files", "*.mzML *.mzml"), ("All files", "*.*")]
    user_patterns = [("MZmine user files", "*.mzuser"), ("All files", "*.*")]
    exe_patterns = [("MZmine console", "*.exe"), ("All files", "*.*")]

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

    def points_to_legacy_batch_template(value: Any, template_name: str) -> bool:
        text = str(value or "").replace("\\", "/")
        return text.endswith(f"/{template_name}") and "batch_prep" in text

    def points_to_bundled_template(value: Any, template_name: str) -> bool:
        text = str(value or "").replace("\\", "/")
        return text.endswith(f"01_mzmine_pipeline/templates/{template_name}") or text == f"templates/{template_name}"

    def normalize_config_for_gui(config: dict[str, Any]) -> dict[str, Any]:
        config_version = int(config.get("config_version") or 1)
        default_config = core.template_config()
        complete = config.setdefault("complete", {})
        fraction = config.setdefault("fraction", {})
        mzmine = config.setdefault("mzmine", {})
        if points_to_legacy_batch_template(complete.get("template_path"), "big_empty_template.mzbatch"):
            complete["template_path"] = default_config["complete"]["template_path"]
        if points_to_legacy_batch_template(fraction.get("template_path"), "fraction_empty_template.mzbatch"):
            fraction["template_path"] = default_config["fraction"]["template_path"]
        if not mzmine.get("temp_dir"):
            mzmine["temp_dir"] = default_config["mzmine"]["temp_dir"]
        uses_bundled_templates = (
            points_to_bundled_template(complete.get("template_path"), "big_empty_template.mzbatch")
            and points_to_bundled_template(fraction.get("template_path"), "fraction_empty_template.mzbatch")
        )
        if "ignore_parameter_warnings" not in mzmine or (config_version < core.CONFIG_VERSION and uses_bundled_templates):
            mzmine["ignore_parameter_warnings"] = core.DEFAULT_IGNORE_PARAMETER_WARNINGS
        config["config_version"] = core.CONFIG_VERSION
        return config

    app_state = load_json_safe(state_file)
    if args.config:
        initial_config = normalize_config_for_gui(core.load_config(args.config))
        config_base_dir = Path(initial_config["base_dir"])
    else:
        initial_config = normalize_config_for_gui(app_state or core.template_config())
        config_base_dir = Path(initial_config.get("base_dir") or _THIS_DIR)

    root = ctk.CTk()
    root.title("Integrated MZmine pipeline")
    root.geometry("1240x860")
    root.minsize(1060, 740)
    root.configure(fg_color=colors["bg"])
    help_popovers = HelpPopoverController(root, ctk, colors, font_small)

    var_status = tk.StringVar(value="Ready.")
    var_sample_name = tk.StringVar()
    var_complete_template = tk.StringVar()
    var_complete_out_dir = tk.StringVar()
    var_complete_feature_dir = tk.StringVar()
    var_blank_pattern = tk.StringVar(value="*blank*")
    var_fraction_template = tk.StringVar()
    var_fraction_out_dir = tk.StringVar()
    var_fraction_feature_dir = tk.StringVar()
    var_rt_start = tk.StringVar(value="2.0")
    var_rt_end = tk.StringVar(value="38.0")
    var_rt_width = tk.StringVar(value="0.375")
    var_mzmine_exe = tk.StringVar()
    var_mzmine_user = tk.StringVar()
    var_mzmine_temp = tk.StringVar()
    var_memory = tk.StringVar(value=core.DEFAULT_MEMORY_MODE)
    var_threads = tk.StringVar(value=core.DEFAULT_THREADS)
    var_ignore_warnings = tk.BooleanVar(value=core.DEFAULT_IGNORE_PARAMETER_WARNINGS)
    var_match_outdir = tk.StringVar()
    var_mz_tol = tk.StringVar(value="0.1")
    var_rt_tol = tk.StringVar(value="1.0")
    var_complete_csv_preview = tk.StringVar(value="")
    var_fraction_dir_preview = tk.StringVar(value="")
    var_match_output_preview = tk.StringVar(value="")
    var_progress_detail = tk.StringVar(value="Progress: waiting.")
    var_progress_percent = tk.StringVar(value="0%")
    var_stage_prepare = tk.BooleanVar(value=True)
    var_stage_run_complete = tk.BooleanVar(value=True)
    var_stage_run_fraction = tk.BooleanVar(value=True)
    var_stage_match = tk.BooleanVar(value=True)

    log_queue: queue.Queue[str] = queue.Queue()
    result_queue: queue.Queue[tuple[bool, Any]] = queue.Queue()
    running = False
    stage_labels: dict[str, Any] = {}
    app_closing = {"value": False}
    scheduled_after_ids: set[str] = set()
    log_line_count = {"value": 0}
    log_filter_state = {"suppressed": 0}
    max_log_lines = 1800
    log_lines_per_tick = 60
    step_pattern = re.compile(r"starting step #\s*(\d+)", re.IGNORECASE)
    finished_batch_pattern = re.compile(r"finished a batch of\s*(\d+)\s*steps", re.IGNORECASE)
    progress_state: dict[str, Any] = {
        "selected_stages": [],
        "prepare_done": 0,
        "complete_total": 0,
        "complete_current": 0,
        "fraction_total": 0,
        "fraction_current": 0,
        "match_done": 0,
        "active_mzmine": None,
        "total_units": 1,
        "last_fraction_csv_count": 0,
    }

    def make_help(parent: Any, text: str) -> Any:
        return help_popovers.create_bubble(parent, text)

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

    def show_toast(message: str, *, kind: str = "info", timeout_ms: int = 4200) -> None:
        color = {
            "success": colors["success"],
            "error": colors["danger"],
            "warning": colors["warning"],
            "info": colors["accent"],
        }.get(kind, colors["accent"])
        toast = ctk.CTkFrame(root, fg_color=color, corner_radius=14)
        ctk.CTkLabel(
            toast,
            text=message,
            font=font_small,
            text_color="white",
            justify="left",
            wraplength=440,
            padx=16,
            pady=11,
        ).pack(fill="both", expand=True)
        toast.place(relx=1.0, rely=0.0, x=-26, y=86, anchor="ne")
        toast.lift()
        schedule_after(timeout_ms, lambda: toast.destroy() if toast.winfo_exists() else None)

    def make_button(parent: Any, text: str, command: Any, *, primary: bool = False, success: bool = False, width: int = 112) -> Any:
        fg = colors["accent"] if primary else colors["card_alt"]
        hover = colors["accent_hover"] if primary else colors["border"]
        if success:
            fg = colors["success"]
            hover = colors["success_hover"]
        return ctk.CTkButton(parent, text=text, command=command, width=width, height=38, corner_radius=10, fg_color=fg, hover_color=hover)

    def make_entry(parent: Any, var: tk.StringVar, placeholder: str = "") -> Any:
        return ctk.CTkEntry(
            parent,
            textvariable=var,
            placeholder_text=placeholder,
            height=36,
            corner_radius=8,
            fg_color=colors["entry"],
            border_color=colors["border"],
            text_color=colors["text"],
            font=font_label,
        )

    def make_combo(parent: Any, var: tk.StringVar, values: list[str]) -> Any:
        return ctk.CTkComboBox(
            parent,
            variable=var,
            values=values,
            height=36,
            corner_radius=8,
            fg_color=colors["entry"],
            border_color=colors["border"],
            button_color=colors["card_alt"],
            button_hover_color=colors["accent"],
            dropdown_fg_color=colors["surface"],
            text_color=colors["text"],
            font=font_label,
        )

    def make_checkbox(parent: Any, text: str, variable: tk.BooleanVar) -> Any:
        return ctk.CTkCheckBox(
            parent,
            text=text,
            variable=variable,
            fg_color=colors["accent"],
            hover_color=colors["accent_hover"],
            border_color=colors["border"],
            text_color=colors["text"],
            font=font_label,
        )

    def make_card(parent: Any, title: str, subtitle: str = "") -> Any:
        card = ctk.CTkFrame(
            parent,
            fg_color=colors["card"],
            border_color=colors["border"],
            border_width=1,
            corner_radius=14,
        )
        card.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(card, text=title, font=font_card_title, text_color=colors["text"], anchor="w").grid(
            row=0, column=0, columnspan=4, sticky="ew", padx=16, pady=(14, 2)
        )
        if subtitle:
            ctk.CTkLabel(
                card,
                text=subtitle,
                font=font_small,
                text_color=colors["muted"],
                anchor="w",
                justify="left",
                wraplength=920,
            ).grid(row=1, column=0, columnspan=4, sticky="ew", padx=16, pady=(0, 10))
        return card

    def labeled_entry(
        parent: Any,
        label: str,
        var: tk.StringVar,
        row: int,
        help_text: str,
        *,
        placeholder: str = "",
        browse: Any = None,
    ) -> None:
        label_frame = ctk.CTkFrame(parent, fg_color="transparent")
        label_frame.grid(row=row, column=0, sticky="ew", padx=(16, 8), pady=8)
        label_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(label_frame, text=label, font=font_label, text_color=colors["text"], anchor="w").grid(row=0, column=0, sticky="w")
        make_help(label_frame, help_text).grid(row=0, column=1, sticky="e", padx=(6, 0))
        make_entry(parent, var, placeholder).grid(row=row, column=1, sticky="ew", padx=(0, 10), pady=8)
        if browse:
            make_button(parent, "Browse", browse, width=86).grid(
                row=row, column=2, sticky="ew", padx=(0, 16), pady=8
            )

    def browse_file(var: tk.StringVar, title: str, patterns: list[tuple[str, str]]) -> None:
        path = filedialog.askopenfilename(title=title, filetypes=patterns)
        if path:
            var.set(path)

    def browse_dir(var: tk.StringVar, title: str) -> None:
        path = filedialog.askdirectory(title=title)
        if path:
            var.set(path)

    def text_lines(widget: Any) -> list[str]:
        return [line.strip() for line in widget.get("1.0", "end").splitlines() if line.strip()]

    def set_text_lines(widget: Any, values: list[str]) -> None:
        widget.delete("1.0", "end")
        if values:
            widget.insert("1.0", "\n".join(values))

    def choose_many(widget: Any, title: str, patterns: list[tuple[str, str]]) -> None:
        paths = filedialog.askopenfilenames(title=title, filetypes=patterns)
        if paths:
            current = text_lines(widget)
            set_text_lines(widget, current + list(paths))
            update_previews()

    def append_log(message: str) -> None:
        log_box.configure(state="normal")
        log_box.insert("end", message.rstrip() + "\n")
        log_line_count["value"] += 1
        if log_line_count["value"] > max_log_lines + 200:
            log_box.delete("1.0", "201.0")
            log_line_count["value"] -= 200
        log_box.see("end")
        log_box.configure(state="disabled")

    def flush_suppressed_log_summary() -> None:
        count = log_filter_state["suppressed"]
        if count:
            log_queue.put(f"MZmine detail log compacted: {count} verbose line(s) hidden to keep the GUI responsive.")
            log_filter_state["suppressed"] = 0

    def should_show_mzmine_log(message: str) -> bool:
        lowered = message.lower()
        important_markers = (
            "running mzmine:",
            "dry run:",
            "warning",
            "severe",
            "error",
            "finished a batch",
            "whole batch",
            "timing:",
            "exiting because",
            "exporting feature list",
            "batch file was created",
        )
        if any(marker in lowered for marker in important_markers):
            return True
        if "starting step #" in lowered:
            return True
        if not any(marker in lowered for marker in ("mzmine", "io.github", "processing of task", "starting processing of task")):
            return True
        return False

    def count_batch_steps(path: str | Path) -> int:
        target = Path(path).expanduser()
        if not target.exists():
            return 0
        try:
            return target.read_text(encoding="utf-8", errors="ignore").lower().count("<batchstep")
        except Exception:
            return 0

    def count_fraction_csvs() -> int:
        try:
            config = collect_config()
            paths = core.resolved_pipeline_paths(config, base_dir=config_base_dir)
            directory = Path(paths["fraction_dir"]).expanduser()
            if directory.exists():
                return len(list(directory.glob("frac_*.csv")))
        except Exception:
            return 0
        return 0

    def refresh_progress_totals() -> None:
        try:
            config = collect_config()
            paths = core.resolved_pipeline_paths(config, base_dir=config_base_dir)
        except Exception:
            paths = {}
        stages = set(progress_state["selected_stages"])
        if "run_complete" in stages:
            progress_state["complete_total"] = count_batch_steps(paths.get("complete_batch", "")) or progress_state["complete_total"] or 1
        else:
            progress_state["complete_total"] = 0
        if "run_fraction" in stages:
            progress_state["fraction_total"] = count_batch_steps(paths.get("fraction_batch", "")) or progress_state["fraction_total"] or 1
        else:
            progress_state["fraction_total"] = 0
        prepare_units = 2 if "prepare" in stages else 0
        match_units = 1 if "match" in stages else 0
        progress_state["total_units"] = max(
            1,
            prepare_units + progress_state["complete_total"] + progress_state["fraction_total"] + match_units,
        )

    def progress_completed_units() -> int:
        stages = set(progress_state["selected_stages"])
        completed = 0
        if "prepare" in stages:
            completed += min(2, int(progress_state["prepare_done"]))
        if "run_complete" in stages:
            completed += min(int(progress_state["complete_current"]), int(progress_state["complete_total"]))
        if "run_fraction" in stages:
            completed += min(int(progress_state["fraction_current"]), int(progress_state["fraction_total"]))
        if "match" in stages:
            completed += min(1, int(progress_state["match_done"]))
        return completed

    def set_progress_detail(text: str) -> None:
        var_progress_detail.set(text)

    def update_progress_display(detail: str | None = None) -> None:
        total = max(1, int(progress_state["total_units"]))
        completed = max(0, min(progress_completed_units(), total))
        fraction = completed / total
        progress_bar.set(fraction)
        var_progress_percent.set(f"{round(fraction * 100):d}%")
        if detail:
            set_progress_detail(detail)

    def reset_progress(stages: list[str]) -> None:
        progress_state.update(
            {
                "selected_stages": list(stages),
                "prepare_done": 0,
                "complete_total": 0,
                "complete_current": 0,
                "fraction_total": 0,
                "fraction_current": 0,
                "match_done": 0,
                "active_mzmine": None,
                "last_fraction_csv_count": count_fraction_csvs(),
            }
        )
        refresh_progress_totals()
        update_progress_display("Progress: starting.")

    def mark_progress_complete(detail: str) -> None:
        stages = set(progress_state["selected_stages"])
        if "prepare" in stages:
            progress_state["prepare_done"] = 2
        if "run_complete" in stages:
            progress_state["complete_current"] = progress_state["complete_total"]
        if "run_fraction" in stages:
            progress_state["fraction_current"] = progress_state["fraction_total"]
        if "match" in stages:
            progress_state["match_done"] = 1
        update_progress_display(detail)

    def mark_progress_failed() -> None:
        percent = var_progress_percent.get()
        set_progress_detail(f"Pipeline failed at {percent}.")

    def update_progress_from_log(message: str) -> None:
        lower = message.lower()
        stages = set(progress_state["selected_stages"])
        if "preparing complete mzmine batch" in lower:
            update_progress_display("Preparing complete MZmine batch...")
            return
        if "preparing fraction mzmine batch" in lower:
            if "prepare" in stages:
                progress_state["prepare_done"] = max(progress_state["prepare_done"], 1)
            update_progress_display("Preparing fraction MZmine batch...")
            return
        if "running mzmine" in lower and "fraction_configured.mzbatch" in lower:
            if "prepare" in stages:
                progress_state["prepare_done"] = 2
            refresh_progress_totals()
            progress_state["active_mzmine"] = "run_fraction"
            update_progress_display(
                f"MZmine fraction batch: step 0 / {progress_state['fraction_total']}"
            )
            return
        if "running mzmine" in lower and "configured.mzbatch" in lower:
            if "prepare" in stages:
                progress_state["prepare_done"] = 2
            refresh_progress_totals()
            progress_state["active_mzmine"] = "run_complete"
            update_progress_display(
                f"MZmine complete batch: step 0 / {progress_state['complete_total']}"
            )
            return
        step_match = step_pattern.search(message)
        if step_match and progress_state["active_mzmine"] == "run_complete":
            step = int(step_match.group(1))
            progress_state["complete_current"] = min(step, int(progress_state["complete_total"]))
            update_progress_display(f"MZmine complete batch: step {step} / {progress_state['complete_total']}")
            return
        if step_match and progress_state["active_mzmine"] == "run_fraction":
            step = int(step_match.group(1))
            progress_state["fraction_current"] = min(step, int(progress_state["fraction_total"]))
            update_progress_display(f"MZmine fraction batch: step {step} / {progress_state['fraction_total']}")
            return
        finished_match = finished_batch_pattern.search(message)
        if finished_match and progress_state["active_mzmine"] == "run_complete":
            progress_state["complete_total"] = max(progress_state["complete_total"], int(finished_match.group(1)))
            progress_state["complete_current"] = progress_state["complete_total"]
            refresh_progress_totals()
            update_progress_display("Complete MZmine batch finished.")
            return
        if finished_match and progress_state["active_mzmine"] == "run_fraction":
            progress_state["fraction_total"] = max(progress_state["fraction_total"], int(finished_match.group(1)))
            progress_state["fraction_current"] = progress_state["fraction_total"]
            refresh_progress_totals()
            update_progress_display("Fraction MZmine batch finished.")
            return
        if "running fraction-feature matching" in lower:
            if "run_fraction" in stages:
                progress_state["fraction_current"] = progress_state["fraction_total"]
            update_progress_display("Matching fraction features...")
            return

    def queue_log_message(message: str) -> None:
        if should_show_mzmine_log(message):
            flush_suppressed_log_summary()
            log_queue.put(message)
            return
        log_filter_state["suppressed"] += 1
        if log_filter_state["suppressed"] >= 500:
            flush_suppressed_log_summary()

    def selected_stages() -> list[str]:
        stages: list[str] = []
        if var_stage_prepare.get():
            stages.append("prepare")
        if var_stage_run_complete.get():
            stages.append("run_complete")
        if var_stage_run_fraction.get():
            stages.append("run_fraction")
        if var_stage_match.get():
            stages.append("match")
        return stages

    def collect_config() -> dict[str, Any]:
        return {
            "config_version": core.CONFIG_VERSION,
            "base_dir": str(config_base_dir),
            "sample_name": var_sample_name.get().strip(),
            "sample_files": text_lines(sample_text),
            "blank_files": text_lines(blank_text),
            "stages": selected_stages(),
            "complete": {
                "template_path": var_complete_template.get().strip(),
                "out_dir": var_complete_out_dir.get().strip(),
                "feature_dir": var_complete_feature_dir.get().strip(),
                "blank_pattern": var_blank_pattern.get().strip() or "*blank*",
            },
            "fraction": {
                "template_path": var_fraction_template.get().strip(),
                "out_dir": var_fraction_out_dir.get().strip(),
                "feature_dir": var_fraction_feature_dir.get().strip(),
                "rt_start": float(var_rt_start.get().strip()),
                "rt_end": float(var_rt_end.get().strip()),
                "width": float(var_rt_width.get().strip()),
            },
            "mzmine": {
                "executable": var_mzmine_exe.get().strip(),
                "user_file": var_mzmine_user.get().strip(),
                "temp_dir": var_mzmine_temp.get().strip(),
                "memory": var_memory.get().strip() or core.DEFAULT_MEMORY_MODE,
                "threads": var_threads.get().strip() or core.DEFAULT_THREADS,
                "ignore_parameter_warnings": bool(var_ignore_warnings.get()),
            },
            "matching": {
                "outdir": var_match_outdir.get().strip(),
                "mz_tol": float(var_mz_tol.get().strip()),
                "rt_tol": float(var_rt_tol.get().strip()),
            },
        }

    def apply_config(config: dict[str, Any]) -> None:
        var_sample_name.set(str(config.get("sample_name", "")))
        set_text_lines(sample_text, list(config.get("sample_files", [])))
        set_text_lines(blank_text, list(config.get("blank_files", [])))
        complete = config.get("complete", {})
        fraction = config.get("fraction", {})
        mzmine = config.get("mzmine", {})
        matching = config.get("matching", {})
        var_complete_template.set(str(complete.get("template_path", "")))
        var_complete_out_dir.set(str(complete.get("out_dir", "")))
        var_complete_feature_dir.set(str(complete.get("feature_dir", "")))
        var_blank_pattern.set(str(complete.get("blank_pattern", "*blank*")))
        var_fraction_template.set(str(fraction.get("template_path", "")))
        var_fraction_out_dir.set(str(fraction.get("out_dir", "")))
        var_fraction_feature_dir.set(str(fraction.get("feature_dir", "")))
        var_rt_start.set(str(fraction.get("rt_start", "2.0")))
        var_rt_end.set(str(fraction.get("rt_end", "38.0")))
        var_rt_width.set(str(fraction.get("width", "0.375")))
        var_mzmine_exe.set(str(mzmine.get("executable", "")))
        var_mzmine_user.set(str(mzmine.get("user_file", "")))
        var_mzmine_temp.set(str(mzmine.get("temp_dir", "")))
        var_memory.set(str(mzmine.get("memory", core.DEFAULT_MEMORY_MODE)))
        var_threads.set(str(mzmine.get("threads", core.DEFAULT_THREADS)))
        var_ignore_warnings.set(bool(mzmine.get("ignore_parameter_warnings", core.DEFAULT_IGNORE_PARAMETER_WARNINGS)))
        var_match_outdir.set(str(matching.get("outdir", "")))
        var_mz_tol.set(str(matching.get("mz_tol", "0.1")))
        var_rt_tol.set(str(matching.get("rt_tol", "1.0")))
        stages = set(config.get("stages", core.DEFAULT_STAGES))
        var_stage_prepare.set("prepare" in stages)
        var_stage_run_complete.set("run_complete" in stages)
        var_stage_run_fraction.set("run_fraction" in stages)
        var_stage_match.set("match" in stages)
        update_previews()

    def update_previews(*_args: Any) -> None:
        try:
            config = collect_config()
            paths = core.resolved_pipeline_paths(config, base_dir=config_base_dir)
            var_complete_csv_preview.set(paths["complete_csv"])
            var_fraction_dir_preview.set(paths["fraction_dir"])
            var_match_output_preview.set(paths["match_output"])
        except Exception:
            var_complete_csv_preview.set("(set sample name and output folders)")
            var_fraction_dir_preview.set("(set fraction output folder)")
            var_match_output_preview.set("(set sample name and matching output folder)")

    def set_stage(stage: str, status: str) -> None:
        label = stage_labels.get(stage)
        if not label:
            return
        color = {
            "waiting": colors["card_alt"],
            "running": colors["warning"],
            "success": colors["success"],
            "failed": colors["danger"],
        }.get(status, colors["card_alt"])
        label.configure(fg_color=color, text=status.upper())

    def reset_stages() -> None:
        for stage in ["prepare", "run_complete", "run_fraction", "match"]:
            set_stage(stage, "waiting")

    def stage_from_log(message: str) -> None:
        lower = message.lower()
        if "preparing complete" in lower or "preparing fraction" in lower:
            set_stage("prepare", "running")
        elif "running mzmine" in lower and "fraction_configured.mzbatch" in lower:
            set_stage("run_fraction", "running")
        elif "running mzmine" in lower and "configured.mzbatch" in lower:
            set_stage("run_complete", "running")
        elif "running fraction-feature matching" in lower:
            set_stage("match", "running")

    def save_config_dialog() -> None:
        try:
            config = collect_config()
        except Exception as exc:
            messagebox.showerror("Cannot save config", str(exc))
            return
        path = filedialog.asksaveasfilename(
            title="Save pipeline config",
            defaultextension=".json",
            filetypes=config_patterns,
        )
        if path:
            core.save_config(config, path)
            var_status.set(f"Saved config: {path}")

    def load_config_dialog() -> None:
        nonlocal config_base_dir
        path = filedialog.askopenfilename(title="Load pipeline config", filetypes=config_patterns)
        if path:
            config = normalize_config_for_gui(core.load_config(path))
            config_base_dir = Path(config["base_dir"])
            apply_config(config)
            var_status.set(f"Loaded config: {path}")

    def run_pipeline(dry_run: bool = False) -> None:
        nonlocal running
        if running:
            return
        try:
            config = collect_config()
            config["base_dir"] = str(config_base_dir)
            stages = selected_stages()
            if not stages:
                raise ValueError("Select at least one workflow stage.")
        except Exception as exc:
            messagebox.showerror("Invalid settings", str(exc))
            return
        running = True
        reset_stages()
        reset_progress(stages)
        log_line_count["value"] = 0
        var_status.set("Running dry run..." if dry_run else "Running pipeline...")
        append_log("")
        append_log("New integrated MZmine pipeline run started.")

        def worker() -> None:
            try:
                result = core.run_pipeline(
                    config,
                    stages=stages,
                    dry_run=dry_run,
                    log_callback=lambda text: queue_log_message(str(text)),
                )
                flush_suppressed_log_summary()
                result_queue.put((True, result))
            except Exception as exc:
                flush_suppressed_log_summary()
                result_queue.put((False, (exc, traceback.format_exc())))

        threading.Thread(target=worker, daemon=True).start()

    def poll_queues() -> None:
        nonlocal running
        if app_closing["value"]:
            return
        processed_logs = 0
        while not log_queue.empty() and processed_logs < log_lines_per_tick:
            message = log_queue.get_nowait()
            stage_from_log(message)
            update_progress_from_log(message)
            append_log(message)
            processed_logs += 1
        while not result_queue.empty():
            ok, payload = result_queue.get_nowait()
            running = False
            if ok:
                result = payload
                for stage in selected_stages():
                    set_stage(stage, "success")
                report_box.configure(state="normal")
                report_box.delete("1.0", "end")
                report_box.insert("1.0", json.dumps(result, indent=2, ensure_ascii=False))
                report_box.configure(state="disabled")
                var_status.set("Dry run complete." if result.get("dry_run") else "Pipeline complete.")
                if result.get("dry_run"):
                    mark_progress_complete("Dry run complete.")
                    show_toast("Dry run complete. Check the command preview and expected outputs.", kind="info")
                else:
                    mark_progress_complete("Pipeline complete.")
                    show_toast("Pipeline finished successfully.", kind="success")
                save_json_safe(state_file, collect_config())
            else:
                exc, tb = payload
                for stage in selected_stages():
                    current = stage_labels.get(stage)
                    if current and current.cget("text") == "RUNNING":
                        set_stage(stage, "failed")
                append_log(str(exc))
                append_log(tb)
                var_status.set("Pipeline failed.")
                mark_progress_failed()
                show_toast("Pipeline failed. Check the Log / Report tab for details.", kind="error", timeout_ms=6500)
                messagebox.showerror("Pipeline failed", str(exc))
        schedule_after(150, poll_queues)

    def on_close() -> None:
        if app_closing["value"]:
            return
        app_closing["value"] = True
        try:
            save_json_safe(state_file, collect_config())
        except Exception:
            pass
        try:
            help_popovers.close()
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

    header = ctk.CTkFrame(root, fg_color="transparent")
    header.pack(fill="x", padx=20, pady=(18, 10))
    header.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(
        header,
        text="Integrated MZmine pipeline",
        font=font_header,
        text_color=colors["text"],
        anchor="w",
    ).grid(row=0, column=0, sticky="w")
    ctk.CTkLabel(
        header,
        text="Prepare MZmine batches, run them headlessly, verify CSV outputs, and match fraction features in one guided workflow.",
        font=font_subtitle,
        text_color=colors["muted"],
        anchor="w",
    ).grid(row=1, column=0, sticky="w", pady=(2, 0))
    actions = ctk.CTkFrame(header, fg_color="transparent")
    actions.grid(row=0, column=1, rowspan=2, sticky="e")
    make_button(actions, "Load config", load_config_dialog, width=104).pack(side="left", padx=(0, 8))
    make_button(actions, "Save config", save_config_dialog, width=104).pack(side="left", padx=(0, 8))
    make_button(actions, "Dry run", lambda: run_pipeline(True), primary=True, width=96).pack(side="left", padx=(0, 8))
    make_button(actions, "Run pipeline", lambda: run_pipeline(False), success=True, width=124).pack(side="left")

    status_bar = ctk.CTkFrame(root, fg_color=colors["surface"], corner_radius=12)
    status_bar.pack(fill="x", padx=20, pady=(0, 10))
    status_bar.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(
        status_bar,
        textvariable=var_status,
        font=font_small,
        text_color=colors["muted"],
        anchor="w",
    ).grid(row=0, column=0, sticky="ew", padx=14, pady=(8, 2))
    progress_row = ctk.CTkFrame(status_bar, fg_color="transparent")
    progress_row.grid(row=1, column=0, sticky="ew", padx=14, pady=(0, 8))
    progress_row.grid_columnconfigure(1, weight=1)
    ctk.CTkLabel(
        progress_row,
        textvariable=var_progress_detail,
        font=font_small,
        text_color=colors["muted"],
        anchor="w",
    ).grid(row=0, column=0, sticky="w", padx=(0, 10))
    progress_bar = ctk.CTkProgressBar(
        progress_row,
        height=10,
        corner_radius=6,
        fg_color=colors["card_alt"],
        progress_color=colors["accent"],
    )
    progress_bar.grid(row=0, column=1, sticky="ew", padx=(0, 10))
    progress_bar.set(0)
    ctk.CTkLabel(
        progress_row,
        textvariable=var_progress_percent,
        font=font_small,
        text_color=colors["text"],
        anchor="e",
        width=42,
    ).grid(row=0, column=2, sticky="e")

    tabs = ctk.CTkTabview(
        root,
        fg_color=colors["surface"],
        corner_radius=14,
        border_width=0,
        segmented_button_fg_color=colors["card"],
        segmented_button_selected_color=colors["accent"],
        segmented_button_selected_hover_color=colors["accent_hover"],
        segmented_button_unselected_color=colors["card_alt"],
        segmented_button_unselected_hover_color="#39414c",
        text_color=colors["text"],
    )
    tabs.pack(fill="both", expand=True, padx=20, pady=(0, 20))
    workflow_tab = tabs.add("Workflow")
    runner_tab = tabs.add("MZmine Runner")
    matching_tab = tabs.add("Matching")
    log_tab = tabs.add("Log / Report")

    workflow_scroll = ctk.CTkScrollableFrame(workflow_tab, fg_color="transparent")
    workflow_scroll.pack(fill="both", expand=True, padx=8, pady=8)
    workflow_scroll.grid_columnconfigure(0, weight=1)

    stage_card = make_card(
        workflow_scroll,
        "Workflow stages",
        "Dry run checks settings, prints the MZmine commands, and shows expected output paths without starting MZmine or writing result files. Use it first; then run the full pipeline when the paths look correct.",
    )
    stage_card.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 12))
    for idx, (stage, label, var) in enumerate(
        [
            ("prepare", "Prepare complete and fraction batches", var_stage_prepare),
            ("run_complete", "Run complete MZmine batch", var_stage_run_complete),
            ("run_fraction", "Run fraction MZmine batch", var_stage_run_fraction),
            ("match", "Match fraction features", var_stage_match),
        ],
        start=2,
    ):
        row = ctk.CTkFrame(stage_card, fg_color="transparent")
        row.grid(row=idx, column=0, columnspan=4, sticky="ew", padx=16, pady=5)
        row.grid_columnconfigure(1, weight=1)
        make_checkbox(row, label, var).grid(row=0, column=0, sticky="w")
        bubble = ctk.CTkLabel(
            row,
            text="WAITING",
            width=86,
            height=26,
            corner_radius=13,
            fg_color=colors["card_alt"],
            text_color=colors["text"],
            font=("Segoe UI", 10, "bold"),
        )
        bubble.grid(row=0, column=2, sticky="e")
        stage_labels[stage] = bubble

    sample_card = make_card(
        workflow_scroll,
        "Sample and raw-data files",
        "These raw files are written into the generated MZmine batches. The batch templates still control the processing method.",
    )
    sample_card.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 12))
    labeled_entry(
        sample_card,
        "Sample name",
        var_sample_name,
        2,
        "Used for generated batch names and final filtered CSV names.",
    )
    ctk.CTkLabel(
        sample_card,
        text="Sample mzML files",
        font=font_label,
        text_color=colors["text"],
    ).grid(row=3, column=0, sticky="nw", padx=(16, 8), pady=8)
    sample_text = ctk.CTkTextbox(
        sample_card,
        height=82,
        fg_color=colors["entry"],
        border_color=colors["border"],
        border_width=1,
        text_color=colors["text"],
        font=font_mono,
    )
    sample_text.grid(row=3, column=1, sticky="ew", padx=(0, 10), pady=8)
    make_button(
        sample_card,
        "Add",
        lambda: choose_many(sample_text, "Select sample mzML files", mzml_patterns),
        width=86,
    ).grid(row=3, column=2, sticky="n", padx=(0, 16), pady=8)
    ctk.CTkLabel(
        sample_card,
        text="Blank mzML files",
        font=font_label,
        text_color=colors["text"],
    ).grid(row=4, column=0, sticky="nw", padx=(16, 8), pady=8)
    blank_text = ctk.CTkTextbox(
        sample_card,
        height=70,
        fg_color=colors["entry"],
        border_color=colors["border"],
        border_width=1,
        text_color=colors["text"],
        font=font_mono,
    )
    blank_text.grid(row=4, column=1, sticky="ew", padx=(0, 10), pady=8)
    make_button(
        sample_card,
        "Add",
        lambda: choose_many(blank_text, "Select blank/control mzML files", mzml_patterns),
        width=86,
    ).grid(row=4, column=2, sticky="n", padx=(0, 16), pady=8)

    batch_card = make_card(
        workflow_scroll,
        "Batch preparation",
        "Select the optimized complete and fraction templates, then choose where generated batches and CSV exports should be written.",
    )
    batch_card.grid(row=2, column=0, sticky="ew", padx=8, pady=(0, 12))
    labeled_entry(
        batch_card,
        "Complete template",
        var_complete_template,
        2,
        "Optimized MZmine batch for the full sample run. The default template is stored in this folder under templates/.",
        browse=lambda: browse_file(var_complete_template, "Select complete .mzbatch", batch_patterns),
    )
    labeled_entry(
        batch_card,
        "Complete batch folder",
        var_complete_out_dir,
        3,
        "Folder where the configured complete .mzbatch will be saved.",
        browse=lambda: browse_dir(var_complete_out_dir, "Select complete batch output folder"),
    )
    labeled_entry(
        batch_card,
        "Complete CSV folder",
        var_complete_feature_dir,
        4,
        "Folder written into the complete-batch CSV export step. Expected output: sample_name_complete_feature_table.csv.",
        browse=lambda: browse_dir(var_complete_feature_dir, "Select complete CSV folder"),
    )
    labeled_entry(
        batch_card,
        "Blank filename pattern",
        var_blank_pattern,
        5,
        "Pattern used by MZmine blank/control filtering modules, for example *blank*.",
    )
    labeled_entry(
        batch_card,
        "Fraction template",
        var_fraction_template,
        6,
        "MZmine batch optimized for one representative fraction, usually frac_01. The default template is stored in this script folder under templates/.",
        browse=lambda: browse_file(var_fraction_template, "Select fraction .mzbatch", batch_patterns),
    )
    labeled_entry(
        batch_card,
        "Fraction batch folder",
        var_fraction_out_dir,
        7,
        "Folder where the replicated fraction .mzbatch will be saved.",
        browse=lambda: browse_dir(var_fraction_out_dir, "Select fraction batch output folder"),
    )
    labeled_entry(
        batch_card,
        "Fraction CSV folder",
        var_fraction_feature_dir,
        8,
        "Folder written into every fraction CSV export step. It should contain frac_01.csv, frac_02.csv, and so on.",
        browse=lambda: browse_dir(var_fraction_feature_dir, "Select fraction CSV folder"),
    )
    timing = ctk.CTkFrame(batch_card, fg_color="transparent")
    timing.grid(row=9, column=1, columnspan=2, sticky="ew", padx=(0, 16), pady=8)
    timing.grid_columnconfigure((0, 1, 2), weight=1)
    make_entry(timing, var_rt_start, "2.0").grid(row=0, column=0, sticky="ew", padx=(0, 8))
    make_entry(timing, var_rt_end, "38.0").grid(row=0, column=1, sticky="ew", padx=(0, 8))
    make_entry(timing, var_rt_width, "0.375").grid(row=0, column=2, sticky="ew")
    ctk.CTkLabel(
        batch_card,
        text="RT start / end / width",
        font=font_label,
        text_color=colors["text"],
    ).grid(row=9, column=0, sticky="w", padx=(16, 8), pady=8)

    runner_scroll = ctk.CTkScrollableFrame(runner_tab, fg_color="transparent")
    runner_scroll.pack(fill="both", expand=True, padx=8, pady=8)
    runner_scroll.grid_columnconfigure(0, weight=1)
    runner_card = make_card(
        runner_scroll,
        "MZmine command-line runner",
        "Use the console executable, not the graphical launcher. Start with the defaults unless your MZmine installation or computer requires different runner settings.",
    )
    runner_card.grid(row=0, column=0, sticky="ew", padx=8, pady=8)
    labeled_entry(
        runner_card,
        "MZmine console",
        var_mzmine_exe,
        2,
        "Path to mzmine_console.exe, not mzmine.exe. The GUI calls this program with --batch so MZmine runs in the background from the generated .mzbatch files.",
        browse=lambda: browse_file(var_mzmine_exe, "Select MZmine console executable", exe_patterns),
    )
    labeled_entry(
        runner_card,
        "User file",
        var_mzmine_user,
        3,
        "Optional .mzuser login file. MZmine creates this after a successful login and stores it under your Windows user profile, typically C:/Users/<name>/.mzmine/users/. Use it when headless runs cannot find your current MZmine login. Leave empty if MZmine already runs with the current user.",
        browse=lambda: browse_file(var_mzmine_user, "Select .mzuser file", user_patterns),
    )
    labeled_entry(
        runner_card,
        "Temp folder",
        var_mzmine_temp,
        4,
        "Folder, not a file. MZmine uses it for temporary processing data and memory-mapped files. Keep the default script-7 outputs/mzmine_temp folder, or choose a fast local SSD folder with enough free space for large mzML runs.",
        browse=lambda: browse_dir(var_mzmine_temp, "Select MZmine temp folder"),
    )
    memory_combo = make_combo(
        runner_card,
        var_memory,
        ["none", "all", "features", "centroids", "raw", "masses_features"],
    )
    memory_label = ctk.CTkFrame(runner_card, fg_color="transparent")
    memory_label.grid(row=5, column=0, sticky="ew", padx=(16, 8), pady=8)
    memory_label.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(memory_label, text="Memory mode", font=font_label, text_color=colors["text"], anchor="w").grid(row=0, column=0, sticky="w")
    make_help(
        memory_label,
        "MZmine option --memory. none is the safest default and uses less RAM, but can be slower. all keeps more raw data/features in memory and can be faster if the computer has enough RAM. features, centroids, raw, and masses_features keep only selected object types in memory. If unsure, keep none.",
    ).grid(row=0, column=1, sticky="e", padx=(6, 0))
    memory_combo.grid(row=5, column=1, sticky="ew", padx=(0, 10), pady=8)
    labeled_entry(
        runner_card,
        "Threads",
        var_threads,
        6,
        "MZmine option --threads. auto lets MZmine choose based on available CPU resources and is the recommended default. Advanced users can type a number such as 4 or 8 to limit CPU use while keeping the computer responsive.",
        placeholder="auto",
    )
    warnings_label = ctk.CTkFrame(runner_card, fg_color="transparent")
    warnings_label.grid(row=7, column=0, sticky="ew", padx=(16, 8), pady=8)
    warnings_label.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(
        warnings_label,
        text="Batch-version warnings",
        font=font_label,
        text_color=colors["text"],
        anchor="w",
    ).grid(row=0, column=0, sticky="w")
    make_help(
        warnings_label,
        "MZmine stops by default when a .mzbatch was created in an older MZmine version or when a module parameter changed. The bundled templates can trigger this in newer MZmine versions, so this option is enabled by default. Disable it only when you want MZmine to stop until you manually open, review, and re-save the batch in the installed MZmine GUI.",
    ).grid(row=0, column=1, sticky="e", padx=(6, 0))
    make_checkbox(runner_card, "Continue after batch-version warnings", var_ignore_warnings).grid(
        row=7, column=1, sticky="w", padx=(0, 10), pady=8
    )

    matching_scroll = ctk.CTkScrollableFrame(matching_tab, fg_color="transparent")
    matching_scroll.pack(fill="both", expand=True, padx=8, pady=8)
    matching_scroll.grid_columnconfigure(0, weight=1)
    match_card = make_card(
        matching_scroll,
        "Matching and expected outputs",
        "These paths are inferred from the batch-preparation settings. Run matching only after the MZmine CSV exports exist.",
    )
    match_card.grid(row=0, column=0, sticky="ew", padx=8, pady=8)
    labeled_entry(
        match_card,
        "Complete CSV",
        var_complete_csv_preview,
        2,
        "Expected complete feature table exported by the complete MZmine batch.",
    )
    labeled_entry(
        match_card,
        "Fraction CSV folder",
        var_fraction_dir_preview,
        3,
        "Expected folder containing frac_*.csv exports from the fraction MZmine batch.",
    )
    labeled_entry(
        match_card,
        "Matching output folder",
        var_match_outdir,
        4,
        "Folder where the final sample_filtered.csv will be written.",
        browse=lambda: browse_dir(var_match_outdir, "Select matching output folder"),
    )
    labeled_entry(
        match_card,
        "m/z tolerance",
        var_mz_tol,
        5,
        "Maximum m/z difference allowed between a fraction feature and the complete feature table.",
    )
    labeled_entry(
        match_card,
        "RT tolerance",
        var_rt_tol,
        6,
        "Maximum retention-time difference in minutes allowed for matching.",
    )
    labeled_entry(match_card, "Filtered output", var_match_output_preview, 7, "Expected final output from the matching stage.")

    log_tab.grid_columnconfigure(0, weight=1)
    log_tab.grid_columnconfigure(1, weight=1)
    log_tab.grid_rowconfigure(0, weight=1)
    log_box = ctk.CTkTextbox(
        log_tab,
        fg_color=colors["entry"],
        text_color=colors["text"],
        font=font_mono,
        border_color=colors["border"],
        border_width=1,
    )
    log_box.grid(row=0, column=0, sticky="nsew", padx=(8, 4), pady=8)
    log_box.configure(state="disabled")
    report_box = ctk.CTkTextbox(
        log_tab,
        fg_color=colors["entry"],
        text_color=colors["text"],
        font=font_mono,
        border_color=colors["border"],
        border_width=1,
    )
    report_box.grid(row=0, column=1, sticky="nsew", padx=(4, 8), pady=8)
    report_box.configure(state="disabled")

    for variable in [
        var_sample_name,
        var_complete_feature_dir,
        var_fraction_feature_dir,
        var_match_outdir,
        var_mz_tol,
        var_rt_tol,
    ]:
        variable.trace_add("write", update_previews)

    apply_config(initial_config)
    reset_stages()
    poll_queues()
    root.protocol("WM_DELETE_WINDOW", on_close)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
