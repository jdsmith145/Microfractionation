#!/usr/bin/env python3
"""
GUI-first: Match (filter) fraction feature tables against a "big" (whole-run) feature table.

Inputs (via pop-ups):
- Directory with fraction feature tables: expected pattern 'frac_*.csv'
- Big/complete feature table CSV
- Sample name (used for output filename slug)
- Output directory (optional; if you cancel, current working directory is used)
- m/z tolerance (default 0.1)
- RT tolerance in minutes (default 1.0)

Output:
- One CSV named: <slug>_filtered.csv
  Columns:
    fraction_file, fraction_index, mz, rt, area, matched_target_mz, matched_target_rt
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd


DEFAULT_MZ_TOL = 0.1
DEFAULT_RT_TOL = 1.0


def slugify(name: str) -> str:
    s = (name or "").strip().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]+", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "sample"


def find_col(df: pd.DataFrame, preferred: str, fallbacks: Tuple[str, ...]) -> str:
    cols = list(df.columns)
    if preferred in cols:
        return preferred
    for c in fallbacks:
        if c in cols:
            return c
    raise ValueError(f"Missing required column '{preferred}'. Available columns: {cols}")


def parse_fraction_index(filename: str, fallback: int) -> int:
    m = re.search(r"frac_(\d{1,4})", filename)
    if m:
        return int(m.group(1))
    return fallback


def list_fraction_files(fractions_dir: Path) -> list[Path]:
    files = sorted(fractions_dir.glob("frac_*.csv"))
    if not files:
        raise FileNotFoundError(f"No fraction files found in {fractions_dir} (expected 'frac_*.csv').")
    return files


def build_big_index(big_df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    mz = big_df["mz"].to_numpy(dtype=float)
    rt = big_df["rt"].to_numpy(dtype=float)
    order = np.argsort(mz)
    return mz[order], rt[order]


def match_rows_to_big(
    frac_mz: np.ndarray,
    frac_rt: np.ndarray,
    big_mz_sorted: np.ndarray,
    big_rt_sorted: np.ndarray,
    mz_tol: float,
    rt_tol: float,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    n = frac_mz.size
    keep = np.zeros(n, dtype=bool)
    mmz = np.full(n, np.nan, dtype=float)
    mrt = np.full(n, np.nan, dtype=float)

    for i in range(n):
        mz = float(frac_mz[i])
        rt = float(frac_rt[i])

        lo = np.searchsorted(big_mz_sorted, mz - mz_tol, side="left")
        hi = np.searchsorted(big_mz_sorted, mz + mz_tol, side="right")
        if hi <= lo:
            continue

        cand_mz = big_mz_sorted[lo:hi]
        cand_rt = big_rt_sorted[lo:hi]

        rt_ok = np.abs(cand_rt - rt) <= rt_tol
        if not np.any(rt_ok):
            continue

        cand_mz = cand_mz[rt_ok]
        cand_rt = cand_rt[rt_ok]

        d = (mz - cand_mz) ** 2 + (rt - cand_rt) ** 2
        j = int(np.argmin(d))

        keep[i] = True
        mmz[i] = cand_mz[j]
        mrt[i] = cand_rt[j]

    return keep, mmz, mrt


def run_match(
    fractions_dir: Path,
    big_csv: Path,
    sample_name: str,
    outdir: Path,
    mz_tol: float,
    rt_tol: float,
) -> Path:
    frac_files = list_fraction_files(fractions_dir)

    big = pd.read_csv(big_csv)
    mz_col = find_col(big, "mz", ("m/z", "mzmed", "row m/z", "row_mz"))
    rt_col = find_col(big, "rt", ("retention_time", "rtmed", "row retention time", "row_rt", "row retention time (min)"))

    big = big.rename(columns={mz_col: "mz", rt_col: "rt"})
    big = big.dropna(subset=["mz", "rt"])
    if big.empty:
        raise ValueError("Big feature table has no valid rows after reading mz/rt.")

    big_mz_sorted, big_rt_sorted = build_big_index(big)

    matched_records = []
    for k, f in enumerate(frac_files, start=1):
        try:
            sub = pd.read_csv(f)
        except pd.errors.EmptyDataError:
            continue
        if sub.empty:
            continue

        mz_col_f = find_col(sub, "mz", ("m/z", "row m/z", "row_mz"))
        rt_col_f = find_col(sub, "rt", ("retention_time", "row retention time", "row_rt", "row retention time (min)"))
        area_col_f = find_col(sub, "area", ("Area", "peak_area", "Peak area", "row area", "row_area"))

        sub = sub.rename(columns={mz_col_f: "mz", rt_col_f: "rt", area_col_f: "area"})
        sub = sub.dropna(subset=["mz", "rt", "area"])
        if sub.empty:
            continue

        sub = sub[sub["area"] > 0]
        if sub.empty:
            continue

        frac_mz = sub["mz"].to_numpy(dtype=float)
        frac_rt = sub["rt"].to_numpy(dtype=float)

        keep, mmz, mrt = match_rows_to_big(frac_mz, frac_rt, big_mz_sorted, big_rt_sorted, mz_tol, rt_tol)
        if not np.any(keep):
            continue

        kept = sub.loc[keep, ["mz", "rt", "area"]].copy()
        kept["matched_target_mz"] = mmz[keep]
        kept["matched_target_rt"] = mrt[keep]

        frac_file = f.name
        frac_index = parse_fraction_index(frac_file, fallback=k)
        kept.insert(0, "fraction_index", frac_index)
        kept.insert(0, "fraction_file", frac_file)

        matched_records.append(kept)

    outdir.mkdir(parents=True, exist_ok=True)
    out_name = f"{slugify(sample_name)}_filtered.csv"
    out_path = outdir / out_name

    if matched_records:
        out_df = pd.concat(matched_records, ignore_index=True)
    else:
        out_df = pd.DataFrame(
            columns=[
                "fraction_file",
                "fraction_index",
                "mz",
                "rt",
                "area",
                "matched_target_mz",
                "matched_target_rt",
            ]
        )

    out_df.to_csv(out_path, index=False)
    return out_path


def main() -> int:
    """
    Styled, single-window GUI for matching fraction feature tables to a big/complete feature table.
    (Core matching logic is unchanged; only the UI is modernized.)
    """
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

    # --- theme ---
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
        ent = tk.Entry(
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
        return ent

    root = tk.Tk()
    root.title("Match fraction features")
    root.configure(bg=BG)
    root.minsize(760, 420)

    # --- state ---
    var_frac_dir = tk.StringVar()
    var_big_csv = tk.StringVar()
    var_sample = tk.StringVar()
    var_outdir = tk.StringVar()  # empty -> cwd
    var_mz_tol = tk.StringVar(value=str(DEFAULT_MZ_TOL))
    var_rt_tol = tk.StringVar(value=str(DEFAULT_RT_TOL))
    var_outfile = tk.StringVar(value=f"{slugify('sample')}_filtered.csv")
    var_status = tk.StringVar(value="Ready.")

    def _update_outfile(*_):
        var_outfile.set(f"{slugify(var_sample.get())}_filtered.csv")

    var_sample.trace_add("write", _update_outfile)

    # --- layout ---
    outer = tk.Frame(root, bg=BG)
    outer.pack(fill="both", expand=True, padx=16, pady=16)

    title = tk.Label(
        outer,
        text="Match fraction feature tables → big feature table",
        bg=BG,
        fg=FG,
        font=("Segoe UI", 14, "bold"),
        anchor="w",
    )
    title.grid(row=0, column=0, sticky="ew", pady=(0, 10))

    subtitle = tk.Label(
        outer,
        text="Inputs: directory with frac_*.csv + complete feature table. Output: <sample>_filtered.csv",
        bg=BG,
        fg=MUTED,
        font=("Segoe UI", 10),
        anchor="w",
    )
    subtitle.grid(row=1, column=0, sticky="ew", pady=(0, 14))

    panel = tk.Frame(outer, bg=PANEL)
    panel.grid(row=2, column=0, sticky="nsew")
    outer.grid_rowconfigure(2, weight=1)
    outer.grid_columnconfigure(0, weight=1)

    # grid config inside panel
    panel.grid_columnconfigure(1, weight=1)

    r = 0

    # Fractions dir
    tk.Label(panel, text="Fractions dir", bg=PANEL, fg=FG, anchor="w").grid(row=r, column=0, sticky="w", padx=12, pady=(12, 6))
    ent_frac = _entry(panel, var_frac_dir)
    ent_frac.grid(row=r, column=1, sticky="ew", padx=12, pady=(12, 6))

    def pick_frac_dir():
        d = filedialog.askdirectory(title="Select directory with fraction tables (frac_*.csv)")
        if d:
            var_frac_dir.set(str(Path(d).expanduser().resolve()))

    _button(panel, "Browse…", pick_frac_dir).grid(row=r, column=2, sticky="e", padx=12, pady=(12, 6))
    r += 1

    tk.Label(panel, text="Tip: files must match frac_*.csv", bg=PANEL, fg=MUTED, anchor="w").grid(row=r, column=1, sticky="w", padx=12, pady=(0, 10))
    r += 1

    # Big CSV
    tk.Label(panel, text="Big table CSV", bg=PANEL, fg=FG, anchor="w").grid(row=r, column=0, sticky="w", padx=12, pady=6)
    ent_big = _entry(panel, var_big_csv)
    ent_big.grid(row=r, column=1, sticky="ew", padx=12, pady=6)

    def pick_big_csv():
        f = filedialog.askopenfilename(
            title="Select big/complete feature table CSV",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if f:
            var_big_csv.set(str(Path(f).expanduser().resolve()))

    _button(panel, "Browse…", pick_big_csv).grid(row=r, column=2, sticky="e", padx=12, pady=6)
    r += 1

    # Sample name
    tk.Label(panel, text="Sample name", bg=PANEL, fg=FG, anchor="w").grid(row=r, column=0, sticky="w", padx=12, pady=6)
    ent_sample = _entry(panel, var_sample)
    ent_sample.grid(row=r, column=1, sticky="ew", padx=12, pady=6)

    out_preview = tk.Label(panel, textvariable=var_outfile, bg=PANEL, fg=BLUE, anchor="w")
    out_preview.grid(row=r, column=2, sticky="w", padx=12, pady=6)
    r += 1

    # Output dir
    tk.Label(panel, text="Output dir (optional)", bg=PANEL, fg=FG, anchor="w").grid(row=r, column=0, sticky="w", padx=12, pady=6)
    ent_outdir = _entry(panel, var_outdir)
    ent_outdir.grid(row=r, column=1, sticky="ew", padx=12, pady=6)

    def pick_outdir():
        d = filedialog.askdirectory(title="Select output directory (optional)")
        if d:
            var_outdir.set(str(Path(d).expanduser().resolve()))

    _button(panel, "Browse…", pick_outdir).grid(row=r, column=2, sticky="e", padx=12, pady=6)
    r += 1

    tk.Label(panel, text="Leave empty → current working directory", bg=PANEL, fg=MUTED, anchor="w").grid(row=r, column=1, sticky="w", padx=12, pady=(0, 10))
    r += 1

    # Tolerances row
    tol_frame = tk.Frame(panel, bg=PANEL)
    tol_frame.grid(row=r, column=0, columnspan=3, sticky="ew", padx=12, pady=6)
    tol_frame.grid_columnconfigure(1, weight=1)
    tol_frame.grid_columnconfigure(3, weight=1)

    tk.Label(tol_frame, text="m/z tol", bg=PANEL, fg=FG, anchor="w").grid(row=0, column=0, sticky="w")
    ent_mz = _entry(tol_frame, var_mz_tol)
    ent_mz.grid(row=0, column=1, sticky="ew", padx=(8, 18))

    tk.Label(tol_frame, text="RT tol (min)", bg=PANEL, fg=FG, anchor="w").grid(row=0, column=2, sticky="w")
    ent_rt = _entry(tol_frame, var_rt_tol)
    ent_rt.grid(row=0, column=3, sticky="ew", padx=(8, 0))
    r += 1

    # Status + buttons
    bottom = tk.Frame(panel, bg=PANEL)
    bottom.grid(row=r, column=0, columnspan=3, sticky="ew", padx=12, pady=(14, 12))
    bottom.grid_columnconfigure(0, weight=1)

    lbl_status = tk.Label(bottom, textvariable=var_status, bg=PANEL, fg=MUTED, anchor="w")
    lbl_status.grid(row=0, column=0, sticky="ew")

    def _validate_float(s: str, label: str) -> float:
        try:
            x = float(s)
        except Exception:
            raise ValueError(f"{label} must be a number.")
        if x < 0:
            raise ValueError(f"{label} must be ≥ 0.")
        return x

    def run_clicked():
        try:
            fractions_dir_s = var_frac_dir.get().strip()
            big_csv_s = var_big_csv.get().strip()
            sample_s = var_sample.get().strip()
            outdir_s = var_outdir.get().strip()

            if not fractions_dir_s:
                raise ValueError("Fractions dir is required.")
            if not big_csv_s:
                raise ValueError("Big table CSV is required.")
            if not sample_s:
                raise ValueError("Sample name is required.")

            fractions_dir = Path(fractions_dir_s).expanduser().resolve()
            big_csv = Path(big_csv_s).expanduser().resolve()

            if not fractions_dir.exists() or not fractions_dir.is_dir():
                raise FileNotFoundError(f"Fractions dir not found: {fractions_dir}")
            if not big_csv.exists() or not big_csv.is_file():
                raise FileNotFoundError(f"Big CSV not found: {big_csv}")

            outdir = Path(outdir_s).expanduser().resolve() if outdir_s else Path.cwd().resolve()

            mz_tol = _validate_float(var_mz_tol.get().strip(), "m/z tolerance")
            rt_tol = _validate_float(var_rt_tol.get().strip(), "RT tolerance")

            var_status.set("Running…")
            root.update_idletasks()

            out_path = run_match(
                fractions_dir=fractions_dir,
                big_csv=big_csv,
                sample_name=sample_s,
                outdir=outdir,
                mz_tol=mz_tol,
                rt_tol=rt_tol,
            )

            var_status.set(f"Done. Saved: {out_path}")
            messagebox.showinfo("Done", f"Saved:\n{out_path}")

        except Exception as e:
            var_status.set("Error.")
            messagebox.showerror("Error", str(e))

    btn_run = _button(bottom, "Run", run_clicked, primary=True)
    btn_run.grid(row=0, column=1, sticky="e", padx=(12, 0))

    btn_quit = _button(bottom, "Close", root.destroy, primary=False)
    btn_quit.grid(row=0, column=2, sticky="e", padx=(12, 0))

    # Enter triggers Run when focus is on an input
    def _enter_to_run(event):
        run_clicked()

    for w in (ent_frac, ent_big, ent_sample, ent_outdir, ent_mz, ent_rt):
        w.bind("<Return>", _enter_to_run)

    # Make panel stretch nicely
    panel.grid_rowconfigure(999, weight=1)  # harmless trick to allow some stretch

    root.mainloop()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
