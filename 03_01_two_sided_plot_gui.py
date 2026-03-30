#!/usr/bin/env python3
"""
Standalone GUI wrapper for two_sided_plot_core.py (two-sided microfractionation plot).

This file only provides a sleek Tkinter UI (gray/blue theme) and calls:
    from two_sided_plot_core import make_two_sided_plot

Inputs:
- chromatogram mzML (sample raw file)
- <sample>_filtered.csv (output of matching step)
- optional bioactivity/UV Excel (fraction windows + optional values)
- if Excel is NOT provided: rt_start + fraction_width (+ optional n_fractions)

Outputs:
- SVG (default) and optional PNG saved into the selected output directory.
"""

from __future__ import annotations

import sys
import threading
from pathlib import Path

# Ensure we can import two_sided_plot_core.py when this GUI is placed next to it
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

try:
    from two_sided_plot_core import make_two_sided_plot, slugify  # type: ignore
except Exception as e:  # pragma: no cover
    raise SystemExit(
        "ERROR: Could not import 'two_sided_plot_core.py'.\n"
        "Place this GUI script in the same folder as two_sided_plot_core.py, "
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
            "Install a Python build that includes Tk (e.g., python.org installer on Windows/macOS,\n"
            "or your distro's python3-tk package on Linux).",
            file=sys.stderr,
        )
        print(f"Details: {e}", file=sys.stderr)
        return 2

    # --- theme (match your other GUIs) ---
    BG = "#2f2f2f"          # primary gray
    PANEL = "#353535"       # slightly lighter gray
    ENTRY_BG = "#3b3b3b"
    FG = "#f2f2f2"
    MUTED = "#c9c9c9"
    BLUE = "#2d7ff9"        # secondary blue
    BLUE_DARK = "#2367cc"

    def _button(parent: tk.Widget, text: str, command, primary: bool = False, width: int | None = None) -> tk.Button:
        bg = BLUE if primary else PANEL
        abg = BLUE_DARK if primary else "#424242"
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
        )
        if width is not None:
            btn.configure(width=width)
        return btn

    def _entry(parent: tk.Widget, textvariable: tk.StringVar | None = None) -> tk.Entry:
        return tk.Entry(
            parent,
            textvariable=textvariable,
            bg=ENTRY_BG,
            fg=FG,
            insertbackground=FG,
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground="#4a4a4a",
            highlightcolor=BLUE,
        )

    def _label(parent: tk.Widget, text: str, *, bold: bool = False, muted: bool = False) -> tk.Label:
        return tk.Label(
            parent,
            text=text,
            bg=PANEL,
            fg=(MUTED if muted else FG),
            font=("Segoe UI", 10, "bold" if bold else "normal"),
            anchor="w",
        )

    root = tk.Tk()
    root.title("Two-sided plot (microfractionation)")
    root.configure(bg=BG)
    root.minsize(880, 540)

    # --- state ---
    var_sample = tk.StringVar()
    var_mzml = tk.StringVar()
    var_filtered = tk.StringVar()
    var_excel = tk.StringVar()
    var_outdir = tk.StringVar()

    var_rt_start = tk.StringVar()
    var_width = tk.StringVar()
    var_n_frac = tk.StringVar()
    var_xmax = tk.StringVar()

    var_log = tk.BooleanVar(value=False)
    var_svg = tk.BooleanVar(value=True)
    var_png = tk.BooleanVar(value=False)

    var_preview_svg = tk.StringVar(value="")
    var_preview_png = tk.StringVar(value="")
    var_status = tk.StringVar(value="Ready.")

    def _compute_previews(*_):
        name = var_sample.get().strip()
        base = slugify(name) if name else "sample"
        suffix = "_log" if var_log.get() else ""
        outdir = var_outdir.get().strip() or "."
        var_preview_svg.set(str(Path(outdir) / f"{base}{suffix}.svg"))
        var_preview_png.set(str(Path(outdir) / f"{base}{suffix}.png"))

    var_sample.trace_add("write", _compute_previews)
    var_outdir.trace_add("write", _compute_previews)
    var_log.trace_add("write", _compute_previews)

    def _set_status(msg: str):
        var_status.set(msg)
        root.update_idletasks()

    def _browse_mzml():
        p = filedialog.askopenfilename(
            title="Select chromatogram mzML",
            filetypes=[("mzML files", "*.mzML *.mzml"), ("All files", "*.*")],
        )
        if p:
            var_mzml.set(p)

    def _browse_filtered():
        p = filedialog.askopenfilename(
            title="Select <sample>_filtered.csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if p:
            var_filtered.set(p)

    def _browse_excel():
        p = filedialog.askopenfilename(
            title="Select optional bioactivity Excel",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if p:
            var_excel.set(p)
            _sync_excel_state()

    def _clear_excel():
        var_excel.set("")
        _sync_excel_state()

    def _browse_outdir():
        p = filedialog.askdirectory(title="Select output directory")
        if p:
            var_outdir.set(p)

    def _sync_excel_state():
        has_excel = bool(var_excel.get().strip())
        state = "disabled" if has_excel else "normal"
        ent_rt_start.configure(state=state)
        ent_width.configure(state=state)
        ent_n_frac.configure(state=state)
        if has_excel:
            hint_frac.configure(text="Excel provided → RT start/width are ignored.", fg=MUTED)
        else:
            hint_frac.configure(text="No Excel → RT start + width required (n_fractions optional).", fg=MUTED)

    def _parse_float(label: str, s: str, *, required: bool = False) -> float | None:
        s = (s or "").strip()
        if not s:
            if required:
                raise ValueError(f"Missing required value: {label}")
            return None
        try:
            return float(s)
        except Exception:
            raise ValueError(f"Invalid number for {label}: '{s}'")

    def _parse_int(label: str, s: str, *, required: bool = False) -> int | None:
        s = (s or "").strip()
        if not s:
            if required:
                raise ValueError(f"Missing required value: {label}")
            return None
        try:
            return int(s)
        except Exception:
            raise ValueError(f"Invalid integer for {label}: '{s}'")

    def _validate_inputs():
        sample = var_sample.get().strip()
        if not sample:
            raise ValueError("Sample name is required.")

        mzml = Path(var_mzml.get().strip().strip('"'))
        if not mzml.exists():
            raise ValueError("Please select an existing mzML file.")

        filtered = Path(var_filtered.get().strip().strip('"'))
        if not filtered.exists():
            raise ValueError("Please select an existing *_filtered.csv file.")

        outdir = Path(var_outdir.get().strip().strip('"') or ".").expanduser()
        # create if missing? (more user-friendly)
        outdir.mkdir(parents=True, exist_ok=True)

        excel_s = var_excel.get().strip().strip('"')
        excel = Path(excel_s) if excel_s else None
        if excel is not None and not excel.exists():
            raise ValueError("Bioactivity Excel path was provided but file does not exist.")

        # If no Excel, need rt_start + width
        if excel is None:
            rt_start = _parse_float("RT start (min)", var_rt_start.get(), required=True)
            width = _parse_float("Fraction width (min)", var_width.get(), required=True)
            n_frac = _parse_int("Number of fractions", var_n_frac.get(), required=False)
        else:
            rt_start = None
            width = None
            n_frac = None

        x_max = _parse_float("X max (min)", var_xmax.get(), required=False)

        return {
            "sample_name": sample,
            "mzml": mzml,
            "filtered": filtered,
            "outdir": outdir,
            "excel": excel,
            "rt_start": rt_start,
            "width": width,
            "n_frac": n_frac,
            "x_max": x_max,
            "log_total": bool(var_log.get()),
            "save_svg": bool(var_svg.get()),
            "save_png": bool(var_png.get()),
        }

    def _run():
        try:
            cfg = _validate_inputs()
        except Exception as e:
            messagebox.showerror("Input error", str(e))
            return

        # Disable run button while running
        btn_run.configure(state="disabled")
        _set_status("Running... reading mzML and building plot (may take a bit).")

        def worker():
            try:
                fig, _, saved = make_two_sided_plot(
                    chrom_mzml=cfg["mzml"],
                    filtered_csv=cfg["filtered"],
                    sample_name=cfg["sample_name"],
                    out_dir=cfg["outdir"],
                    bioactivity_excel=cfg["excel"],
                    rt_start=cfg["rt_start"],
                    fraction_width=cfg["width"],
                    n_fractions=cfg["n_frac"],
                    x_max=cfg["x_max"],
                    log_total_area=cfg["log_total"],
                    save_svg=cfg["save_svg"],
                    save_png=cfg["save_png"],
                )
                # release memory (especially helpful when running many times)
                try:
                    import matplotlib.pyplot as plt
                    plt.close(fig)
                except Exception:
                    pass
                return ("ok", saved)
            except Exception as e:
                return ("err", e)

        def done(result):
            kind, payload = result
            btn_run.configure(state="normal")
            if kind == "ok":
                saved = payload
                if saved:
                    msg = "Saved:\n" + "\n".join(str(p) for p in saved)
                    _set_status("Done.")
                    messagebox.showinfo("Complete", msg)
                else:
                    _set_status("Done (nothing saved).")
                    messagebox.showinfo("Complete", "Plot created (no files saved).")
            else:
                _set_status("Error.")
                messagebox.showerror("Plotting error", str(payload))

        def runner():
            res = worker()
            root.after(0, lambda: done(res))

        threading.Thread(target=runner, daemon=True).start()

    # --- layout ---
    outer = tk.Frame(root, bg=BG)
    outer.pack(fill="both", expand=True, padx=16, pady=16)

    title = tk.Label(
        outer,
        text="Two-sided plot (chromatogram + fraction features)",
        bg=BG,
        fg=FG,
        font=("Segoe UI", 14, "bold"),
        anchor="w",
    )
    title.grid(row=0, column=0, sticky="ew", pady=(0, 10))

    # --- Inputs panel ---
    panel_inputs = tk.Frame(outer, bg=PANEL)
    panel_inputs.grid(row=1, column=0, sticky="ew", pady=(0, 10))
    panel_inputs.grid_columnconfigure(1, weight=1)

    _label(panel_inputs, "Sample name", bold=True).grid(row=0, column=0, sticky="w", padx=12, pady=(12, 6))
    ent_sample = _entry(panel_inputs, var_sample)
    ent_sample.grid(row=0, column=1, sticky="ew", padx=12, pady=(12, 6), ipady=4)

    # mzML
    _label(panel_inputs, "Chromatogram mzML", bold=True).grid(row=1, column=0, sticky="w", padx=12, pady=6)
    ent_mzml = _entry(panel_inputs, var_mzml)
    ent_mzml.grid(row=1, column=1, sticky="ew", padx=(12, 6), pady=6, ipady=4)
    _button(panel_inputs, "Browse", _browse_mzml, primary=False, width=10).grid(row=1, column=2, sticky="e", padx=(6, 12), pady=6)

    # filtered csv
    _label(panel_inputs, "Matched table (*_filtered.csv)", bold=True).grid(row=2, column=0, sticky="w", padx=12, pady=6)
    ent_filtered = _entry(panel_inputs, var_filtered)
    ent_filtered.grid(row=2, column=1, sticky="ew", padx=(12, 6), pady=6, ipady=4)
    _button(panel_inputs, "Browse", _browse_filtered, primary=False, width=10).grid(row=2, column=2, sticky="e", padx=(6, 12), pady=6)

    # excel optional
    _label(panel_inputs, "Bioactivity Excel (optional)", bold=True).grid(row=3, column=0, sticky="w", padx=12, pady=6)
    ent_excel = _entry(panel_inputs, var_excel)
    ent_excel.grid(row=3, column=1, sticky="ew", padx=(12, 6), pady=6, ipady=4)
    btn_excel = _button(panel_inputs, "Browse", _browse_excel, primary=False, width=10)
    btn_excel.grid(row=3, column=2, sticky="e", padx=(6, 12), pady=6)

    # clear excel
    btn_clear = _button(panel_inputs, "Clear", _clear_excel, primary=False, width=10)
    btn_clear.grid(row=3, column=3, sticky="e", padx=(0, 12), pady=6)

    # output dir
    _label(panel_inputs, "Output directory", bold=True).grid(row=4, column=0, sticky="w", padx=12, pady=6)
    ent_outdir = _entry(panel_inputs, var_outdir)
    ent_outdir.grid(row=4, column=1, sticky="ew", padx=(12, 6), pady=6, ipady=4)
    _button(panel_inputs, "Browse", _browse_outdir, primary=False, width=10).grid(row=4, column=2, sticky="e", padx=(6, 12), pady=6)

    # output preview
    hint = tk.Label(panel_inputs, text="Output preview", bg=PANEL, fg=MUTED, font=("Segoe UI", 9), anchor="w")
    hint.grid(row=5, column=0, sticky="w", padx=12, pady=(2, 0))
    prev = tk.Label(panel_inputs, textvariable=var_preview_svg, bg=PANEL, fg=FG, font=("Consolas", 9), anchor="w")
    prev.grid(row=5, column=1, columnspan=3, sticky="ew", padx=12, pady=(2, 0))
    prev2 = tk.Label(panel_inputs, textvariable=var_preview_png, bg=PANEL, fg=MUTED, font=("Consolas", 9), anchor="w")
    prev2.grid(row=6, column=1, columnspan=3, sticky="ew", padx=12, pady=(2, 10))

    # --- Fraction settings panel ---
    panel_frac = tk.Frame(outer, bg=PANEL)
    panel_frac.grid(row=2, column=0, sticky="ew", pady=(0, 10))
    panel_frac.grid_columnconfigure(1, weight=1)
    panel_frac.grid_columnconfigure(3, weight=1)

    hdr = tk.Label(panel_frac, text="Fraction settings (used only if no Excel)", bg=PANEL, fg=FG, font=("Segoe UI", 11, "bold"), anchor="w")
    hdr.grid(row=0, column=0, columnspan=4, sticky="ew", padx=12, pady=(12, 6))

    _label(panel_frac, "RT start (min)", muted=False).grid(row=1, column=0, sticky="w", padx=12, pady=6)
    ent_rt_start = _entry(panel_frac, var_rt_start)
    ent_rt_start.grid(row=1, column=1, sticky="ew", padx=(12, 12), pady=6, ipady=4)

    _label(panel_frac, "Fraction width (min)", muted=False).grid(row=1, column=2, sticky="w", padx=12, pady=6)
    ent_width = _entry(panel_frac, var_width)
    ent_width.grid(row=1, column=3, sticky="ew", padx=(12, 12), pady=6, ipady=4)

    _label(panel_frac, "Number of fractions (optional)", muted=True).grid(row=2, column=0, sticky="w", padx=12, pady=6)
    ent_n_frac = _entry(panel_frac, var_n_frac)
    ent_n_frac.grid(row=2, column=1, sticky="ew", padx=(12, 12), pady=6, ipady=4)

    hint_frac = tk.Label(panel_frac, text="", bg=PANEL, fg=MUTED, font=("Segoe UI", 9), anchor="w")
    hint_frac.grid(row=3, column=0, columnspan=4, sticky="ew", padx=12, pady=(2, 12))

    # --- Options panel ---
    panel_opts = tk.Frame(outer, bg=PANEL)
    panel_opts.grid(row=3, column=0, sticky="ew", pady=(0, 10))
    panel_opts.grid_columnconfigure(1, weight=1)
    panel_opts.grid_columnconfigure(3, weight=1)

    hdr2 = tk.Label(panel_opts, text="Options", bg=PANEL, fg=FG, font=("Segoe UI", 11, "bold"), anchor="w")
    hdr2.grid(row=0, column=0, columnspan=4, sticky="ew", padx=12, pady=(12, 6))

    chk_log = tk.Checkbutton(panel_opts, text="Log10(total area)", variable=var_log, bg=PANEL, fg=FG,
                             activebackground=PANEL, activeforeground=FG, selectcolor=ENTRY_BG)
    chk_log.grid(row=1, column=0, sticky="w", padx=12, pady=6)

    chk_svg = tk.Checkbutton(panel_opts, text="Save SVG", variable=var_svg, bg=PANEL, fg=FG,
                             activebackground=PANEL, activeforeground=FG, selectcolor=ENTRY_BG)
    chk_svg.grid(row=1, column=1, sticky="w", padx=12, pady=6)

    chk_png = tk.Checkbutton(panel_opts, text="Save PNG", variable=var_png, bg=PANEL, fg=FG,
                             activebackground=PANEL, activeforeground=FG, selectcolor=ENTRY_BG)
    chk_png.grid(row=1, column=2, sticky="w", padx=12, pady=6)

    _label(panel_opts, "X max (min, optional)", muted=True).grid(row=2, column=0, sticky="w", padx=12, pady=6)
    ent_xmax = _entry(panel_opts, var_xmax)
    ent_xmax.grid(row=2, column=1, sticky="ew", padx=(12, 12), pady=6, ipady=4)

    tip = tk.Label(
        panel_opts,
        text="Tip: If you converted mzML without Numpress, this runs with pymzML. "
             "If you provide Excel, RT start/width are ignored.",
        bg=PANEL,
        fg=MUTED,
        font=("Segoe UI", 9),
        anchor="w",
        justify="left",
        wraplength=820,
    )
    tip.grid(row=3, column=0, columnspan=4, sticky="ew", padx=12, pady=(2, 12))

    # --- Buttons + status ---
    panel_actions = tk.Frame(outer, bg=BG)
    panel_actions.grid(row=4, column=0, sticky="ew")
    panel_actions.grid_columnconfigure(2, weight=1)

    btn_run = _button(panel_actions, "Run plot", _run, primary=True, width=14)
    btn_run.grid(row=0, column=0, padx=(0, 10), pady=(0, 6), sticky="w")

    btn_close = _button(panel_actions, "Close", root.destroy, primary=False, width=12)
    btn_close.grid(row=0, column=1, padx=(0, 10), pady=(0, 6), sticky="w")

    status = tk.Label(panel_actions, textvariable=var_status, bg=BG, fg=MUTED, font=("Segoe UI", 9), anchor="w")
    status.grid(row=1, column=0, columnspan=3, sticky="ew")

    outer.grid_columnconfigure(0, weight=1)

    _compute_previews()
    _sync_excel_state()

    root.mainloop()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
