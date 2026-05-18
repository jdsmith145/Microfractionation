#!/usr/bin/env python3
"""CustomTkinter GUI for MZmine .mzbatch setup and fraction replication."""
from __future__ import annotations

import argparse
import json
import logging
import queue
import sys
import threading
import traceback
from pathlib import Path
from typing import Any, Callable

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

try:
    import p_01_00_mzmine_batch_core as core
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "ERROR: Could not import p_01_00_mzmine_batch_core.py.\n"
        "Place this GUI script in the same folder as the core script.\n\n"
        f"Details: {exc}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Launch the GUI for MZmine batch setup.")
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
        "danger_hover": "#9e3b3b",
    }

    FONT_HEADER = ("Segoe UI", 23, "bold")
    FONT_SUBTITLE = ("Segoe UI", 12)
    FONT_CARD_TITLE = ("Segoe UI", 15, "bold")
    FONT_LABEL = ("Segoe UI", 12)
    FONT_SMALL = ("Segoe UI", 11)
    FONT_MONO = ("Consolas", 11)

    MZBATCH_PATTERNS = [("MZmine batch files", "*.mzbatch"), ("All files", "*.*")]
    MZML_PATTERNS = [("mzML files", "*.mzML *.mzml"), ("All files", "*.*")]
    CONFIG_PATTERNS = [("JSON config", "*.json"), ("All files", "*.*")]
    STATE_FILE = _THIS_DIR / ".p_01_01_mzmine_batch_gui_state.json"

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

    root = ctk.CTk()
    root.title("MZmine batch helper")
    root.geometry("1180x850")
    root.minsize(1020, 720)
    root.configure(fg_color=COLORS["bg"])

    # ---------------------------
    # Variables / state
    # ---------------------------
    app_state = load_json_safe(STATE_FILE)
    initial_config = load_json_safe(Path(args.config).expanduser()) if args.config else {}

    def cfg_get(*keys: str, default: Any = "") -> Any:
        data: Any = initial_config or app_state
        for key in keys:
            if not isinstance(data, dict) or key not in data:
                return default
            data = data[key]
        return data

    var_status = tk.StringVar(value="Ready.")
    var_sample_name = tk.StringVar(value=str(cfg_get("sample_name", default="")))
    var_sample_summary = tk.StringVar(value="No sample mzML files selected")
    var_blank_summary = tk.StringVar(value="No blank mzML files selected")
    sample_files: list[str] = list(cfg_get("sample_files", default=[]))
    blank_files: list[str] = list(cfg_get("complete", "blank_files", default=[]))

    var_complete_template = tk.StringVar(value=str(cfg_get("complete", "template_path", default="")))
    var_complete_out_dir = tk.StringVar(value=str(cfg_get("complete", "out_dir", default="")))
    var_complete_out_preview = tk.StringVar(value="")
    var_blank_pattern = tk.StringVar(value=str(cfg_get("complete", "blank_pattern", default=core.DEFAULT_BLANK_PATTERN)))
    var_complete_feature_dir = tk.StringVar(value=str(cfg_get("complete", "feature_dir", default="")))

    var_fraction_template = tk.StringVar(value=str(cfg_get("fraction", "template_path", default="")))
    var_fraction_out_dir = tk.StringVar(value=str(cfg_get("fraction", "out_dir", default="")))
    var_fraction_out_preview = tk.StringVar(value="")
    var_fraction_feature_dir = tk.StringVar(value=str(cfg_get("fraction", "feature_dir", default="")))
    var_rt_start = tk.StringVar(value=str(cfg_get("fraction", "rt_start", default=core.DEFAULT_RT_START)))
    var_rt_end = tk.StringVar(value=str(cfg_get("fraction", "rt_end", default=core.DEFAULT_RT_END)))
    var_rt_width = tk.StringVar(value=str(cfg_get("fraction", "width", default=core.DEFAULT_WIDTH)))
    var_fraction_count = tk.StringVar(value="")

    # ---------------------------
    # Logging bridge
    # ---------------------------
    log_queue: queue.Queue[str] = queue.Queue()
    result_queue: queue.Queue[tuple[str, bool, Any]] = queue.Queue()

    class QueueLogHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                log_queue.put(self.format(record))
            except Exception:
                pass

    logger = logging.getLogger("mzmine_batch_core")
    logger.setLevel(logging.INFO)
    handler = QueueLogHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(handler)

    # ---------------------------
    # Helpers
    # ---------------------------
    class ToolTip:
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
            self.frame.place(x=min(max(12, int(x)), int(max_x)), y=min(max(12, int(y)), int(max_y)))
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

    def make_button(parent: Any, text: str, command: Callable[[], None], *, primary: bool = False, success: bool = False, danger: bool = False, width: int | None = None) -> Any:
        color = COLORS["success"] if success else COLORS["danger"] if danger else COLORS["accent"] if primary else COLORS["card_alt"]
        hover = COLORS["success_hover"] if success else COLORS["danger_hover"] if danger else COLORS["accent_hover"] if primary else "#39414c"
        return ctk.CTkButton(parent, text=text, command=command, width=width or 112, height=38, corner_radius=10, fg_color=color, hover_color=hover)

    def labeled_entry(parent: Any, label: str, var: tk.StringVar, row: int, *, help_text: str = "", browse_command: Callable[[], None] | None = None, placeholder: str = "") -> None:
        label_frame = ctk.CTkFrame(parent, fg_color="transparent")
        label_frame.grid(row=row, column=0, sticky="ew", padx=(0, 10), pady=8)
        label_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(label_frame, text=label, font=FONT_LABEL, text_color=COLORS["muted"], anchor="w").grid(row=0, column=0, sticky="w")
        if help_text:
            make_help(label_frame, help_text).grid(row=0, column=1, sticky="e", padx=(6, 0))
        make_entry(parent, var, placeholder).grid(row=row, column=1, sticky="ew", padx=(0, 10), pady=8)
        if browse_command:
            make_button(parent, "Browse", browse_command, width=90).grid(row=row, column=2, sticky="ew", pady=8)
        else:
            ctk.CTkLabel(parent, text="", fg_color="transparent", width=90).grid(row=row, column=2, sticky="ew", pady=8)

    def section_note(parent: Any, title: str, text: str, row: int, *, columnspan: int = 3) -> None:
        frame = ctk.CTkFrame(parent, fg_color=COLORS["card_alt"], corner_radius=12)
        frame.grid(row=row, column=0, columnspan=columnspan, sticky="ew", padx=(0, 0), pady=(0, 14))
        frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(frame, text=title, font=("Segoe UI", 13, "bold"), text_color=COLORS["text"], anchor="w").grid(row=0, column=0, sticky="ew", padx=14, pady=(10, 2))
        ctk.CTkLabel(frame, text=text, font=FONT_SMALL, text_color=COLORS["muted"], anchor="w", justify="left", wraplength=920).grid(row=1, column=0, sticky="ew", padx=14, pady=(0, 10))

    def set_status(text: str) -> None:
        var_status.set(text)
        root.update_idletasks()

    def append_log(message: str, *, notify: bool = False) -> None:
        log_box.configure(state="normal")
        log_box.insert("end", message + "\n")
        log_box.see("end")
        log_box.configure(state="disabled")
        if notify:
            show_toast(message, kind="info")

    def show_toast(message: str, *, kind: str = "info") -> None:
        color = COLORS["success"] if kind == "success" else COLORS["danger"] if kind == "error" else COLORS["accent"]
        toast = ctk.CTkFrame(root, fg_color=color, corner_radius=18)
        ctk.CTkLabel(toast, text=message, font=FONT_SMALL, text_color="white", wraplength=360, justify="left", padx=18, pady=10).pack(fill="both", expand=True)
        toast.place(relx=1.0, rely=0.0, x=-22, y=20, anchor="ne")
        toast.lift()
        root.after(3200, lambda: toast.destroy() if toast.winfo_exists() else None)

    def summarize_files(files: list[str], label: str) -> str:
        if not files:
            return f"No {label} mzML files selected"
        if len(files) == 1:
            return files[0]
        return f"{len(files)} {label} mzML files selected — first: {files[0]}"

    def update_file_text(textbox: Any, files: list[str]) -> None:
        textbox.configure(state="normal")
        textbox.delete("1.0", "end")
        if files:
            textbox.insert("end", "\n".join(files))
        else:
            textbox.insert("end", "No files selected.")
        textbox.configure(state="disabled")

    def current_config() -> dict[str, Any]:
        name = var_sample_name.get().strip()
        return {
            "sample_name": name,
            "sample_files": list(sample_files),
            "complete": {
                "template_path": var_complete_template.get().strip(),
                "out_dir": var_complete_out_dir.get().strip(),
                "sample_files": list(sample_files),
                "blank_files": list(blank_files),
                "blank_pattern": var_blank_pattern.get().strip() or core.DEFAULT_BLANK_PATTERN,
                "feature_dir": var_complete_feature_dir.get().strip() or None,
                "sample_name": name,
            },
            "fraction": {
                "template_path": var_fraction_template.get().strip(),
                "out_dir": var_fraction_out_dir.get().strip(),
                "sample_files": list(sample_files),
                "feature_dir": var_fraction_feature_dir.get().strip(),
                "rt_start": var_rt_start.get().strip(),
                "rt_end": var_rt_end.get().strip(),
                "width": var_rt_width.get().strip(),
                "sample_name": name,
            },
        }

    def apply_config(config: dict[str, Any]) -> None:
        nonlocal sample_files, blank_files
        var_sample_name.set(str(config.get("sample_name") or config.get("complete", {}).get("sample_name") or config.get("fraction", {}).get("sample_name") or ""))
        sample_files = list(config.get("sample_files") or config.get("complete", {}).get("sample_files") or config.get("fraction", {}).get("sample_files") or [])
        comp = config.get("complete", {}) or {}
        frac = config.get("fraction", {}) or {}
        blank_files = list(comp.get("blank_files") or [])
        var_complete_template.set(str(comp.get("template_path") or ""))
        var_complete_out_dir.set(str(comp.get("out_dir") or ""))
        var_blank_pattern.set(str(comp.get("blank_pattern") or core.DEFAULT_BLANK_PATTERN))
        var_complete_feature_dir.set(str(comp.get("feature_dir") or ""))
        var_fraction_template.set(str(frac.get("template_path") or ""))
        var_fraction_out_dir.set(str(frac.get("out_dir") or ""))
        var_fraction_feature_dir.set(str(frac.get("feature_dir") or ""))
        var_rt_start.set(str(frac.get("rt_start", core.DEFAULT_RT_START)))
        var_rt_end.set(str(frac.get("rt_end", core.DEFAULT_RT_END)))
        var_rt_width.set(str(frac.get("width", core.DEFAULT_WIDTH)))
        refresh_all_summaries()
        update_previews()
        update_fraction_count()

    def save_state() -> None:
        save_json_safe(STATE_FILE, current_config())

    def refresh_all_summaries() -> None:
        var_sample_summary.set(summarize_files(sample_files, "sample"))
        var_blank_summary.set(summarize_files(blank_files, "blank"))
        update_file_text(sample_files_box, sample_files)
        update_file_text(blank_files_box, blank_files)

    def update_previews(*_args: Any) -> None:
        name = var_sample_name.get().strip()
        try:
            if name and var_complete_out_dir.get().strip():
                var_complete_out_preview.set(str(core.complete_output_path(var_complete_out_dir.get().strip(), name)))
            else:
                var_complete_out_preview.set("(set sample name + output directory)")
        except Exception as exc:
            var_complete_out_preview.set(f"Invalid sample name: {exc}")
        try:
            if name and var_fraction_out_dir.get().strip():
                var_fraction_out_preview.set(str(core.fraction_output_path(var_fraction_out_dir.get().strip(), name)))
            else:
                var_fraction_out_preview.set("(set sample name + output directory)")
        except Exception as exc:
            var_fraction_out_preview.set(f"Invalid sample name: {exc}")
        save_state()

    def update_fraction_count(*_args: Any) -> None:
        try:
            n = core._fraction_count(float(var_rt_start.get()), float(var_rt_end.get()), float(var_rt_width.get()))
            var_fraction_count.set(f"This will create {n} fractions.")
        except Exception:
            var_fraction_count.set("Enter valid RT start/end/width to calculate the number of fractions.")
        save_state()

    def browse_file(var: tk.StringVar, title: str, patterns: list[tuple[str, str]]) -> None:
        p = filedialog.askopenfilename(title=title, initialdir=str(_THIS_DIR), filetypes=patterns)
        if p:
            var.set(p)
            save_state()

    def browse_dir(var: tk.StringVar, title: str) -> None:
        d = filedialog.askdirectory(title=title, initialdir=str(_THIS_DIR))
        if d:
            var.set(d)
            save_state()

    def pick_mzml_files(title: str) -> list[str]:
        files = filedialog.askopenfilenames(title=title, initialdir=str(_THIS_DIR), filetypes=MZML_PATTERNS)
        return list(files) if files else []

    def set_sample_files() -> None:
        nonlocal sample_files
        files = pick_mzml_files("Select SAMPLE .mzML file(s)")
        if not files:
            return
        sample_files = files
        refresh_all_summaries()
        append_log(f"Selected {len(sample_files)} sample mzML file(s).")
        show_toast("Sample files selected.", kind="success")
        save_state()

    def set_blank_files() -> None:
        nonlocal blank_files
        files = pick_mzml_files("Select BLANK .mzML file(s)")
        if not files:
            return
        blank_files = files
        refresh_all_summaries()
        append_log(f"Selected {len(blank_files)} blank mzML file(s).")
        show_toast("Blank files selected.", kind="success")
        save_state()

    def clear_sample_files() -> None:
        nonlocal sample_files
        sample_files = []
        refresh_all_summaries()
        save_state()

    def clear_blank_files() -> None:
        nonlocal blank_files
        blank_files = []
        refresh_all_summaries()
        save_state()

    def save_config_dialog() -> None:
        p = filedialog.asksaveasfilename(
            title="Save GUI config",
            defaultextension=".json",
            initialdir=str(_THIS_DIR),
            filetypes=CONFIG_PATTERNS,
        )
        if not p:
            return
        core.save_config(current_config(), p)
        append_log(f"Saved config: {p}", notify=True)
        show_toast("Config saved.", kind="success")

    def load_config_dialog() -> None:
        p = filedialog.askopenfilename(title="Load GUI config", initialdir=str(_THIS_DIR), filetypes=CONFIG_PATTERNS)
        if not p:
            return
        try:
            config = core.load_config(p)
            apply_config(config)
            append_log(f"Loaded config: {p}", notify=True)
            show_toast("Config loaded.", kind="success")
        except Exception as exc:
            append_log(f"ERROR loading config: {exc}")
            messagebox.showerror("Load config error", str(exc))

    def build_complete_settings() -> core.CompleteBatchSettings:
        return core.CompleteBatchSettings(
            template_path=var_complete_template.get().strip(),
            out_dir=var_complete_out_dir.get().strip(),
            sample_files=list(sample_files),
            blank_files=list(blank_files),
            blank_pattern=var_blank_pattern.get().strip() or core.DEFAULT_BLANK_PATTERN,
            feature_dir=var_complete_feature_dir.get().strip() or None,
            sample_name=var_sample_name.get().strip(),
        )

    def build_fraction_settings() -> core.FractionBatchSettings:
        try:
            rt_start = float(var_rt_start.get().strip())
            rt_end = float(var_rt_end.get().strip())
            width = float(var_rt_width.get().strip())
        except Exception as exc:
            raise ValueError("RT start/end/width must be numbers in minutes.") from exc
        return core.FractionBatchSettings(
            template_path=var_fraction_template.get().strip(),
            out_dir=var_fraction_out_dir.get().strip(),
            sample_files=list(sample_files),
            feature_dir=var_fraction_feature_dir.get().strip(),
            rt_start=rt_start,
            rt_end=rt_end,
            width=width,
            sample_name=var_sample_name.get().strip(),
        )

    def run_in_thread(label: str, fn: Callable[[], Any]) -> None:
        set_status(f"{label} running…")
        append_log(f"Starting: {label}")
        show_toast(f"{label} started.", kind="info")

        def worker() -> None:
            try:
                result_queue.put((label, True, fn()))
            except Exception as exc:
                result_queue.put((label, False, (exc, traceback.format_exc())))

        threading.Thread(target=worker, daemon=True).start()

    def run_complete() -> None:
        save_state()
        try:
            settings = build_complete_settings()
        except Exception as exc:
            messagebox.showerror("Complete setup error", str(exc))
            return
        run_in_thread("Complete batch setup", lambda: core.run_complete_from_settings(settings))

    def run_fraction() -> None:
        save_state()
        try:
            settings = build_fraction_settings()
        except Exception as exc:
            messagebox.showerror("Fraction setup error", str(exc))
            return
        run_in_thread("Fraction batch replication", lambda: core.run_fraction_from_settings(settings))

    def poll_queues() -> None:
        try:
            while True:
                append_log(log_queue.get_nowait())
        except queue.Empty:
            pass
        try:
            while True:
                label, ok, payload = result_queue.get_nowait()
                if ok:
                    summary = payload
                    append_log(json.dumps(summary, indent=2, ensure_ascii=False))
                    set_status(f"{label} finished.")
                    show_toast(f"{label} finished.", kind="success")
                    messagebox.showinfo(label, f"Saved:\n{summary.get('output_path')}\n\nDetails are in the Log tab.")
                else:
                    exc, tb = payload
                    append_log(f"ERROR in {label}: {exc}\n{tb}")
                    set_status(f"{label} failed.")
                    show_toast(f"{label} failed.", kind="error")
                    messagebox.showerror(label, str(exc))
        except queue.Empty:
            pass
        root.after(120, poll_queues)

    # ---------------------------
    # Layout
    # ---------------------------
    root.grid_columnconfigure(0, weight=1)
    root.grid_rowconfigure(1, weight=1)

    header = ctk.CTkFrame(root, fg_color="transparent")
    header.grid(row=0, column=0, sticky="ew", padx=24, pady=(20, 12))
    header.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(header, text="MZmine batch helper", font=FONT_HEADER, text_color=COLORS["text"], anchor="w").grid(row=0, column=0, sticky="w")
    ctk.CTkLabel(
        header,
        text="Configure a complete-feature-table batch and replicate a fraction batch without manually editing dozens of MZmine steps.",
        font=FONT_SUBTITLE,
        text_color=COLORS["muted"],
        anchor="w",
    ).grid(row=1, column=0, sticky="w", pady=(4, 0))
    header_actions = ctk.CTkFrame(header, fg_color="transparent")
    header_actions.grid(row=0, column=1, rowspan=2, sticky="e")
    make_button(header_actions, "Save config", save_config_dialog, width=120).pack(side="left", padx=(0, 8))
    make_button(header_actions, "Load config", load_config_dialog, width=120).pack(side="left", padx=(0, 8))
    make_button(header_actions, "Close", root.destroy, width=90).pack(side="left")

    body = ctk.CTkFrame(root, fg_color="transparent")
    body.grid(row=1, column=0, sticky="nsew", padx=24, pady=(0, 12))
    body.grid_columnconfigure(0, weight=1)
    body.grid_rowconfigure(1, weight=1)

    shared = ctk.CTkFrame(body, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, corner_radius=16)
    shared.grid(row=0, column=0, sticky="ew", pady=(0, 12))
    for col in range(4):
        shared.grid_columnconfigure(col, weight=1 if col == 1 else 0)
    ctk.CTkLabel(shared, text="Shared sample settings", font=FONT_CARD_TITLE, text_color=COLORS["text"], anchor="w").grid(row=0, column=0, columnspan=4, sticky="ew", padx=18, pady=(16, 4))
    ctk.CTkLabel(
        shared,
        text="The sample name and sample mzML files are used by both modes. Select only the actual sample files here; blanks are selected separately in the complete-batch tab.",
        font=FONT_SMALL,
        text_color=COLORS["muted"],
        justify="left",
        wraplength=980,
        anchor="w",
    ).grid(row=1, column=0, columnspan=4, sticky="ew", padx=18, pady=(0, 10))

    sample_label = ctk.CTkFrame(shared, fg_color="transparent")
    sample_label.grid(row=2, column=0, sticky="ew", padx=(18, 10), pady=8)
    ctk.CTkLabel(sample_label, text="Sample name", text_color=COLORS["muted"], font=FONT_LABEL).pack(side="left")
    make_help(sample_label, "Used for output filenames: {sample_name}_configured.mzbatch and {sample_name}_fraction_configured.mzbatch. Avoid Windows-forbidden filename characters such as /, \\, :, *, ?, and quotes.").pack(side="left", padx=(6, 0))
    make_entry(shared, var_sample_name, "e.g. Ruta_corsica_84").grid(row=2, column=1, sticky="ew", padx=(0, 10), pady=8)
    make_button(shared, "Select sample mzML", set_sample_files, primary=True, width=150).grid(row=2, column=2, sticky="ew", padx=(0, 8), pady=8)
    make_button(shared, "Clear", clear_sample_files, width=80).grid(row=2, column=3, sticky="ew", padx=(0, 18), pady=8)

    ctk.CTkLabel(shared, textvariable=var_sample_summary, text_color=COLORS["muted"], font=FONT_SMALL, anchor="w", wraplength=980).grid(row=3, column=0, columnspan=4, sticky="ew", padx=18, pady=(0, 14))

    tabs = ctk.CTkTabview(body, fg_color=COLORS["surface"], segmented_button_fg_color=COLORS["card"], segmented_button_selected_color=COLORS["accent"], segmented_button_selected_hover_color=COLORS["accent_hover"], segmented_button_unselected_color=COLORS["card_alt"], segmented_button_unselected_hover_color="#39414c", text_color=COLORS["text"], corner_radius=16)
    tabs.grid(row=1, column=0, sticky="nsew")
    complete_tab = tabs.add("Complete batch")
    fraction_tab = tabs.add("Fraction batches")
    info_tab = tabs.add("Info")
    log_tab = tabs.add("Log")

    # Complete tab
    complete_frame = ctk.CTkScrollableFrame(complete_tab, fg_color="transparent")
    complete_frame.pack(fill="both", expand=True, padx=8, pady=8)
    for col in range(3):
        complete_frame.grid_columnconfigure(col, weight=1 if col == 1 else 0)
    section_note(
        complete_frame,
        "Complete feature table batch setup",
        "This mode creates one configured MZmine batch for the full sample run. Experienced MZmine users could make this manually, so the value here is reproducibility and avoiding path mistakes. The important part is still the template: optimize the complete-feature-table parameters carefully for your instrument and data so the batch captures real features with low false positives and does not miss important compounds.",
        0,
    )
    labeled_entry(complete_frame, "Complete template .mzbatch", var_complete_template, 1, browse_command=lambda: browse_file(var_complete_template, "Select COMPLETE template .mzbatch", MZBATCH_PATTERNS), help_text="Template batch for the complete feature table workflow. This should be a real MZmine batch optimized for your instrument, chromatographic method, noise level, and expected peak shapes.")
    labeled_entry(complete_frame, "Save configured batch to", var_complete_out_dir, 2, browse_command=lambda: browse_dir(var_complete_out_dir, "Select output directory for configured COMPLETE batch"), help_text="Folder where the configured .mzbatch will be saved. The file name is generated from the shared sample name.")
    labeled_entry(complete_frame, "Output .mzbatch path", var_complete_out_preview, 3, help_text="Preview of the generated complete batch path. This is not an input; it updates from the sample name and output directory.")

    ctk.CTkFrame(complete_frame, fg_color=COLORS["border"], height=1).grid(row=4, column=0, columnspan=3, sticky="ew", pady=12)

    blank_label = ctk.CTkFrame(complete_frame, fg_color="transparent")
    blank_label.grid(row=5, column=0, sticky="ew", padx=(0, 10), pady=8)
    ctk.CTkLabel(blank_label, text="Blank mzML files", font=FONT_LABEL, text_color=COLORS["muted"]).pack(side="left")
    make_help(blank_label, "Blank files are included in the import list and are also used by MZmine modules that apply a blank/control raw-data filter. Select only real blank files here.").pack(side="left", padx=(6, 0))
    blank_summary_entry = make_entry(complete_frame, var_blank_summary)
    blank_summary_entry.grid(row=5, column=1, sticky="ew", padx=(0, 10), pady=8)
    make_button(complete_frame, "Select blanks", set_blank_files, primary=True, width=120).grid(row=5, column=2, sticky="ew", pady=8)
    blank_files_box = ctk.CTkTextbox(complete_frame, height=82, fg_color=COLORS["entry"], border_color=COLORS["border"], border_width=1, text_color=COLORS["muted"], font=FONT_MONO, corner_radius=10, wrap="none")
    blank_files_box.grid(row=6, column=1, columnspan=2, sticky="ew", padx=(0, 0), pady=(0, 8))
    make_button(complete_frame, "Clear blanks", clear_blank_files, width=120).grid(row=6, column=0, sticky="ne", padx=(0, 10), pady=(0, 8))

    labeled_entry(complete_frame, "Blank pattern", var_blank_pattern, 7, help_text="Text pattern MZmine uses to recognize blank/control raw data files. Example: *blank* matches file names containing 'blank'. Keep this consistent with your actual blank mzML names.", placeholder="*blank*")
    labeled_entry(complete_frame, "Feature-table CSV directory", var_complete_feature_dir, 8, browse_command=lambda: browse_dir(var_complete_feature_dir, "Select output directory for complete feature-table CSV"), help_text="Folder written into the CSV export step inside the batch. The current_file is set to {sample_name}_complete_feature_table.csv.")
    make_button(complete_frame, "Run COMPLETE", run_complete, success=True, width=150).grid(row=9, column=2, sticky="ew", pady=(18, 16))

    # Fraction tab
    fraction_frame = ctk.CTkScrollableFrame(fraction_tab, fg_color="transparent")
    fraction_frame.pack(fill="both", expand=True, padx=8, pady=8)
    for col in range(3):
        fraction_frame.grid_columnconfigure(col, weight=1 if col == 1 else 0)
    section_note(
        fraction_frame,
        "Fraction feature table batch replication",
        "This mode takes a template prepared for one fraction, usually frac_01, and replicates the processing steps across the whole retention-time range. Doing this by hand is tedious and error-prone. The fraction template does not need to be as perfectly tuned as the complete batch, but it is still worth checking that peak detection, noise thresholds, mass accuracy, and export settings are reasonable for your instrument and the observed run conditions.",
        0,
    )
    labeled_entry(fraction_frame, "Fraction template .mzbatch", var_fraction_template, 1, browse_command=lambda: browse_file(var_fraction_template, "Select FRACTION template .mzbatch", MZBATCH_PATTERNS), help_text="Template batch configured for one fraction. It should contain fraction-specific labels such as frac_01 so the script can detect the template index and replicate it.")
    labeled_entry(fraction_frame, "Save replicated batch to", var_fraction_out_dir, 2, browse_command=lambda: browse_dir(var_fraction_out_dir, "Select output directory for replicated FRACTION batch"), help_text="Folder where the replicated fraction .mzbatch will be saved. The generated name is {sample_name}_fraction_configured.mzbatch.")
    labeled_entry(fraction_frame, "Output .mzbatch path", var_fraction_out_preview, 3, help_text="Preview of the generated fraction batch path. It updates from the sample name and output directory.")
    labeled_entry(fraction_frame, "Per-fraction CSV directory", var_fraction_feature_dir, 4, browse_command=lambda: browse_dir(var_fraction_feature_dir, "Select directory for per-fraction CSV outputs"), help_text="Folder written into every CSV export step. The exported files keep the fraction numbering, e.g. frac_01.csv, frac_02.csv, and so on.")

    ctk.CTkFrame(fraction_frame, fg_color=COLORS["border"], height=1).grid(row=5, column=0, columnspan=3, sticky="ew", pady=12)
    labeled_entry(fraction_frame, "Fractionation RT start (min)", var_rt_start, 6, help_text="Start time of the first collected fraction in minutes. Example: 2.0 if the first fraction starts after the solvent/front delay.")
    labeled_entry(fraction_frame, "Fractionation RT end (min)", var_rt_end, 7, help_text="End time of the fractionation window in minutes. Example: 38.0 for fractions collected from 2.0 to 38.0 min.")
    labeled_entry(fraction_frame, "Time per fraction (min)", var_rt_width, 8, help_text="Width of each collected fraction in minutes. Example: 0.375 gives 96 fractions from 2.0 to 38.0 min.")
    ctk.CTkLabel(fraction_frame, textvariable=var_fraction_count, font=FONT_SMALL, text_color=COLORS["muted"], anchor="w").grid(row=9, column=1, columnspan=2, sticky="ew", pady=(0, 10))
    make_button(fraction_frame, "Run FRACTION", run_fraction, success=True, width=150).grid(row=10, column=2, sticky="ew", pady=(18, 16))

    # Info tab
    info_frame = ctk.CTkFrame(info_tab, fg_color="transparent")
    info_frame.pack(fill="both", expand=True, padx=8, pady=8)
    info_frame.grid_columnconfigure(0, weight=1)
    info_frame.grid_rowconfigure(0, weight=1)
    info_text = ctk.CTkTextbox(info_frame, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, text_color=COLORS["text"], font=("Segoe UI", 12), corner_radius=16, wrap="word")
    info_text.grid(row=0, column=0, sticky="nsew")
    info_text.insert("end", """
How to use this script

This tool does not replace method optimization in MZmine. It automates the boring and error-prone part: inserting raw-data paths, output paths, blank patterns, and repeating fraction-specific processing blocks.

1) Start from the provided templates
Use the complete template for the full sample feature table and the fraction template for fraction-specific feature tables. The templates are only starting points. Before using them for publication data, open them in MZmine and check that the parameters match your instrument, chromatography, ionization mode, and data quality.

2) Tune the complete feature table carefully
The complete feature table is the reference table that later fraction features are matched against. This batch should be stricter and higher quality: catch real peaks, suppress false positives, and avoid missing important features. Parameters such as mass tolerance, chromatogram building, peak resolving, deisotoping, gap filling, and blank filtering should be appropriate for your data.

3) Tune differently for different instruments
A single-quadrupole instrument, triple-quadrupole instrument, Q-TOF, Orbitrap, and FT-ICR do not produce equivalent data. Mass accuracy, resolving power, isotope pattern quality, dynamic range, and noise behavior are different, so MZmine tolerances and filters should not be blindly copied between instruments. Even within the same instrument class, LC conditions and sample complexity can change the best settings.

4) Use the fraction batch to catch many candidate features
The fraction workflow is mainly there to export feature tables for many narrow time windows. The goal is broad capture: each fraction table should contain enough features to match back to the complete feature table. It does not need to be as perfectly optimized as the final complete table, but unreasonable thresholds can still lose real fraction signals or create unnecessary noise.

5) Practical workflow
First optimize the complete-feature-table template in MZmine. Then optimize/check the fraction template for one representative fraction, usually fraction 01. After that, use this GUI: select sample mzML files, choose the complete/fraction template, set output folders and RT fractionation parameters, then run the relevant mode.

6) What the two modes write
Complete mode writes one configured batch named {sample_name}_configured.mzbatch and points the complete CSV export to {sample_name}_complete_feature_table.csv.

Fraction mode writes one replicated batch named {sample_name}_fraction_configured.mzbatch and creates repeated processing steps for each fraction window, for example 2.000–2.375 min, 2.375–2.750 min, and so on.
""".strip())
    info_text.configure(state="disabled")

    # Log tab
    log_tab.grid_columnconfigure(0, weight=1)
    log_tab.grid_rowconfigure(0, weight=1)
    log_box = ctk.CTkTextbox(log_tab, fg_color=COLORS["card"], border_color=COLORS["border"], border_width=1, text_color=COLORS["text"], font=FONT_MONO, corner_radius=16, wrap="word")
    log_box.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
    log_box.insert("end", "Ready. Configure a mode and click Run.\n")
    log_box.configure(state="disabled")

    # Sample files text box must be created after tab layout for helper references.
    sample_files_box = ctk.CTkTextbox(shared, height=1, fg_color=COLORS["bg"], border_width=0, text_color=COLORS["muted"], font=FONT_MONO, corner_radius=0, wrap="none")
    sample_files_box.grid(row=4, column=0, columnspan=4, sticky="ew", padx=18, pady=(0, 0))
    sample_files_box.grid_remove()

    # Status bar
    status_bar = ctk.CTkFrame(root, fg_color=COLORS["surface"], corner_radius=0)
    status_bar.grid(row=2, column=0, sticky="ew")
    status_bar.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(status_bar, textvariable=var_status, font=FONT_SMALL, text_color=COLORS["muted"], anchor="w").grid(row=0, column=0, sticky="ew", padx=18, pady=8)

    # Traces and startup
    for var in [var_sample_name, var_complete_out_dir, var_fraction_out_dir]:
        var.trace_add("write", update_previews)
    for var in [var_rt_start, var_rt_end, var_rt_width]:
        var.trace_add("write", update_fraction_count)

    refresh_all_summaries()
    update_previews()
    update_fraction_count()
    poll_queues()

    try:
        root.mainloop()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
