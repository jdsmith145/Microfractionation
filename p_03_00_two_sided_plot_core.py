#!/usr/bin/env python3
"""two_sided_plot_core.py

Core plotting for the microfractionation pipeline.

Inputs
------
1) chromatogram mzML (one raw file)
2) matched/filtered features CSV (output of 02_match_fraction_features_gui.py)
   Expected columns (case-sensitive):
     - fraction_index (int-like; typically 1..N)
     - rt (min)
     - area
   Other columns are ignored.
3) optional bioactivity/UV Excel file with fraction windows and values.
   Typical columns used in your notebook: start, end, average, pos_avg

What it plots
-------------
Top panel:
  - MS chromatogram (BPC) from mzML
  - optional fluorescence/bioactivity bars on a secondary y-axis

Bottom panel:
  - per-fraction stacked bars:
      dominant feature area (max) + remaining area (sum-max)
    computed from the filtered/matched CSV.

Design goals
------------
- This file contains the *real* logic.
- GUI / notebook / CLI front-ends should only collect inputs and call
  `make_two_sided_plot(...)`.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple

import numpy as np
import pandas as pd


# Matplotlib import is intentionally inside plotting functions so this module can be
# imported in non-plot contexts without pulling GUI backends.


def slugify(name: str) -> str:
    s = (name or "").strip().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]+", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "sample"


def _find_col(df: pd.DataFrame, preferred: str, fallbacks: Tuple[str, ...]) -> str:
    cols = list(df.columns)
    if preferred in cols:
        return preferred
    for c in fallbacks:
        if c in cols:
            return c
    raise ValueError(f"Missing required column '{preferred}'. Available columns: {cols}")


def read_filtered_features(filtered_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(filtered_csv)
    frac_col = _find_col(df, "fraction_index", ("fraction", "frac", "fraction_id"))
    rt_col = _find_col(df, "rt", ("retention_time", "row retention time", "row_rt", "row retention time (min)"))
    area_col = _find_col(df, "area", ("Area", "peak_area", "Peak area", "row area", "row_area"))

    out = df.rename(columns={frac_col: "fraction_index", rt_col: "rt", area_col: "area"}).copy()
    out = out.dropna(subset=["fraction_index", "rt", "area"])
    if out.empty:
        raise ValueError("Filtered feature table is empty after reading required columns.")

    # Normalize types
    out["fraction_index"] = out["fraction_index"].astype(int)
    out["rt"] = out["rt"].astype(float)
    out["area"] = out["area"].astype(float)
    out = out[out["area"] > 0]
    return out


@dataclass
class FractionWindows:
    """Fraction window geometry used for plotting."""

    fraction_index: np.ndarray  # 1..N
    start: np.ndarray           # minutes
    end: np.ndarray             # minutes
    mid: np.ndarray             # minutes
    width: np.ndarray           # minutes


@dataclass
class Bioactivity:
    """Optional bioactivity series aligned to FractionWindows."""

    fraction_index: np.ndarray
    value: np.ndarray
    value_pct: np.ndarray
    label: str = "Normalized bioactivity (%)"


def read_bioactivity_excel(excel_path: Path) -> Tuple[FractionWindows, Optional[Bioactivity]]:
    """Read fraction windows (+ optional bioactivity) from an Excel file.

    Expected (flexible) columns:
      - start, end
      - average (bioactivity value)
      - pos_avg (optional positive control scalar; used like in your notebook)

    If 'average' is missing, windows are still returned but bioactivity is None.
    """
    df = pd.read_excel(excel_path)
    if df.empty:
        raise ValueError(f"Bioactivity Excel appears empty: {excel_path}")

    start_col = _find_col(df, "start", ("Start", "rt_start", "from", "begin"))
    end_col = _find_col(df, "end", ("End", "rt_end", "to", "finish"))

    windows = df.rename(columns={start_col: "start", end_col: "end"}).copy()
    windows = windows.dropna(subset=["start", "end"])
    if windows.empty:
        raise ValueError("Bioactivity Excel has no valid start/end rows.")
    windows["start"] = windows["start"].astype(float)
    windows["end"] = windows["end"].astype(float)
    windows["mid"] = (windows["start"] + windows["end"]) / 2.0
    windows["width"] = windows["end"] - windows["start"]

    # Align fraction indices: use an explicit column if present; else row order (1..N)
    frac_idx = None
    for c in ("fraction_index", "fraction", "frac", "well", "id"):
        if c in df.columns:
            frac_idx = c
            break
    if frac_idx:
        fi = pd.to_numeric(df[frac_idx], errors="coerce").astype("Int64")
        fi = fi[~fi.isna()].astype(int).to_numpy()
        if fi.size == len(windows):
            fraction_index = fi
        else:
            # fall back to order if mismatch
            fraction_index = np.arange(1, len(windows) + 1, dtype=int)
    else:
        fraction_index = np.arange(1, len(windows) + 1, dtype=int)

    fw = FractionWindows(
        fraction_index=fraction_index,
        start=windows["start"].to_numpy(dtype=float),
        end=windows["end"].to_numpy(dtype=float),
        mid=windows["mid"].to_numpy(dtype=float),
        width=windows["width"].to_numpy(dtype=float),
    )

    bio = None
    if "average" in df.columns or "Average" in df.columns:
        avg_col = "average" if "average" in df.columns else "Average"
        avg = pd.to_numeric(df[avg_col], errors="coerce").to_numpy(dtype=float)

        # Optional positive control scaling (your notebook used df['pos_avg'].iloc[0])
        pos = None
        for c in ("pos_avg", "Pos_avg", "positive", "pos", "pos_control"):
            if c in df.columns:
                x = pd.to_numeric(df[c], errors="coerce")
                x = x[~x.isna()]
                if len(x) > 0:
                    pos = float(x.iloc[0])
                    break

        if pos and pos != 0:
            norm = avg / pos
        else:
            norm = avg

        maxv = np.nanmax(norm) if np.isfinite(norm).any() else np.nan
        if maxv and maxv > 0:
            pct = norm / maxv * 100.0
        else:
            pct = np.zeros_like(norm)

        bio = Bioactivity(
            fraction_index=fraction_index,
            value=norm,
            value_pct=pct,
            label="Normalized bioactivity (%)",
        )

    return fw, bio


def build_uniform_fraction_windows(
    n_fractions: int,
    rt_start: float,
    width: float,
) -> FractionWindows:
    if n_fractions <= 0:
        raise ValueError("n_fractions must be > 0")
    if width <= 0:
        raise ValueError("width must be > 0")

    fraction_index = np.arange(1, n_fractions + 1, dtype=int)
    start = rt_start + (fraction_index - 1) * width
    end = start + width
    mid = (start + end) / 2.0
    w = np.full_like(mid, width, dtype=float)
    return FractionWindows(fraction_index=fraction_index, start=start, end=end, mid=mid, width=w)


def compute_fraction_stats(filtered_df: pd.DataFrame) -> pd.DataFrame:
    """Return per-fraction sum and max area from filtered/matched feature rows."""
    g = filtered_df.groupby("fraction_index", as_index=False)["area"].agg(total_area="sum", max_area="max")
    g["total_area"] = g["total_area"].astype(float)
    g["max_area"] = g["max_area"].astype(float)
    return g


def read_bpc_from_mzml(mzml_path: Path) -> Tuple[np.ndarray, np.ndarray]:
    """Read a simple BPC (base peak chromatogram) from mzML.

    Requires pymzml.
    """
    try:
        import pymzml  # type: ignore
    except Exception as e:
        raise ImportError(
            "pymzml is required to read mzML for the chromatogram. "
            "Install it (e.g., `pip install pymzml`)."
        ) from e

    times: list[float] = []
    intensities: list[float] = []

    for spec in pymzml.run.Reader(str(mzml_path)):
        # scan_time can be a scalar or tuple
        t = spec.scan_time[0] if isinstance(spec.scan_time, tuple) else spec.scan_time
        peaks = spec.peaks("raw")
        if isinstance(t, (int, float)) and hasattr(peaks, "__len__") and len(peaks) > 0:
            times.append(float(t))
            intensities.append(float(max(p[1] for p in peaks)))

    if not times:
        raise ValueError(f"No usable scans found in mzML (could not build BPC): {mzml_path}")

    return np.asarray(times, dtype=float), np.asarray(intensities, dtype=float)


def make_two_sided_plot(
    chrom_mzml: Path,
    filtered_csv: Path,
    sample_name: str,
    out_dir: Path,
    bioactivity_excel: Optional[Path] = None,
    *,
    rt_start: Optional[float] = None,
    fraction_width: Optional[float] = None,
    n_fractions: Optional[int] = None,
    x_max: Optional[float] = None,
    log_total_area: bool = False,
    save_svg: bool = True,
    save_png: bool = False,
    figsize: Tuple[float, float] = (14.0, 8.0),
):
    """Create the two-sided plot and save it.

    If `bioactivity_excel` is provided, fraction windows come from that file.
    Otherwise, you must provide enough info to build uniform windows:
      - rt_start
      - fraction_width
      - and either n_fractions OR it will be inferred from max fraction_index in filtered_csv.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- Load data ---
    feats = read_filtered_features(Path(filtered_csv))
    stats = compute_fraction_stats(feats)

    times, ints = read_bpc_from_mzml(Path(chrom_mzml))

    if bioactivity_excel:
        fw, bio = read_bioactivity_excel(Path(bioactivity_excel))
    else:
        if rt_start is None or fraction_width is None:
            raise ValueError(
                "When bioactivity_excel is not provided, you must supply rt_start and fraction_width "
                "to define fraction windows."
            )
        if n_fractions is None:
            n_fractions = int(stats["fraction_index"].max()) if not stats.empty else int(feats["fraction_index"].max())
        fw = build_uniform_fraction_windows(int(n_fractions), float(rt_start), float(fraction_width))
        bio = None

    # --- Align stats to windows ---
    win_df = pd.DataFrame({
        "fraction_index": fw.fraction_index,
        "mid": fw.mid,
        "width": fw.width,
        "start": fw.start,
        "end": fw.end,
    })
    plot_df = win_df.merge(stats, on="fraction_index", how="left")
    plot_df[["total_area", "max_area"]] = plot_df[["total_area", "max_area"]].fillna(0.0)

    # Optional log scaling (your notebook used log10(total) and split by max/total proportion)
    if log_total_area:
        total = plot_df["total_area"].to_numpy(dtype=float)
        with np.errstate(divide="ignore", invalid="ignore"):
            total_log = np.where(total > 0, np.log10(total), 0.0)
        plot_df["plot_total"] = total_log
        with np.errstate(divide="ignore", invalid="ignore"):
            prop = np.where(total > 0, plot_df["max_area"].to_numpy(dtype=float) / total, 0.0)
        plot_df["plot_max"] = total_log * prop
    else:
        plot_df["plot_total"] = plot_df["total_area"]
        plot_df["plot_max"] = plot_df["max_area"]

    # --- Plot ---
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator, StrMethodFormatter

    dominant_color = "#000080"         # navy
    background_bar_color = "#D9D2C7"   # warm gray
    chrom_color = "#000080"            # navy
    bio_color = "#32CD32"              # green

    fig, (ax_top, ax_bot) = plt.subplots(
        2, 1, sharex=True, figsize=figsize,
        gridspec_kw={"height_ratios": [1, 1], "hspace": 0},
    )
    fig.subplots_adjust(hspace=-0.05, right=0.95)

    # Top: chromatogram
    ax_top.plot(times, ints, color=chrom_color, lw=1.5)
    ax_top.set_ylabel("MS intensity (BPC)")
    ax_top.set_ylim(0, float(ints.max()) * 1.05 if ints.size else 1.0)
    ax_top.spines["bottom"].set_visible(False)
    ax_top.xaxis.set_ticks_position("none")
    title_suffix = " (log10 total area proportion)" if log_total_area else ""
    ax_top.set_title(f"{sample_name}{title_suffix}")

    # Optional bioactivity
    if bio is not None:
        ax_bio = ax_top.twinx()
        ax_bio.bar(
            x=fw.mid,
            height=bio.value_pct,
            width=fw.width,
            alpha=0.3,
            edgecolor="black",
            color=bio_color,
        )
        ax_bio.set_ylabel(bio.label)
        ax_bio.set_ylim(0, 100)
        ax_bio.yaxis.set_major_locator(MaxNLocator(integer=True))
        ax_bio.yaxis.set_major_formatter(StrMethodFormatter("{x:.0f}"))

    # Bottom: stacked bars (dominant + remainder)
    for _, r in plot_df.iterrows():
        mid = float(r["mid"])
        width = float(r["width"])
        red_h = float(r["plot_max"])
        total_h = float(r["plot_total"])
        grey_h = max(total_h - red_h, 0.0)

        if red_h > 0:
            ax_bot.bar(mid, red_h, width=width, color=dominant_color, edgecolor=background_bar_color)
        if grey_h > 0:
            ax_bot.bar(mid, grey_h, bottom=red_h, width=width, color=background_bar_color, edgecolor="none")

    y_label = "log10(total area)" if log_total_area else "Feature area"
    ax_bot.set_ylabel(y_label)
    ax_bot.set_xlabel("Retention time [min]")
    ax_bot.spines["top"].set_visible(False)

    # Invert like your notebook (bars pointing down)
    ymax = float(plot_df["plot_total"].max()) if not plot_df.empty else 1.0
    ax_bot.set_ylim(ymax * 1.05 if ymax > 0 else 1.0, 0)

    # X axis limits and ticks
    if x_max is None:
        x_max = float(max(times.max(), fw.end.max()))
    ax_top.set_xlim(0, x_max)
    ax_bot.set_xlim(0, x_max)

    ticks = np.arange(0, int(np.floor(x_max / 5) * 5) + 1, 5)
    ax_bot.set_xticks(ticks)
    ax_bot.set_xticklabels([str(int(t)) for t in ticks], rotation=90, fontsize=8)

    # Save
    base = slugify(sample_name)
    suffix = "_log" if log_total_area else ""
    saved = []

    if save_svg:
        svg_path = out_dir / f"{base}{suffix}.svg"
        fig.savefig(svg_path, dpi=300, bbox_inches="tight")
        saved.append(svg_path)
    if save_png:
        png_path = out_dir / f"{base}{suffix}.png"
        fig.savefig(png_path, dpi=300, bbox_inches="tight")
        saved.append(png_path)

    return fig, (ax_top, ax_bot), saved


def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Create a two-sided chromatogram + fraction-feature plot.")
    p.add_argument("--mzml", required=True, help="Path to mzML for chromatogram (sample raw file).")
    p.add_argument("--filtered", required=True, help="Path to <sample>_filtered.csv from matching step.")
    p.add_argument("--sample-name", required=True, help="Sample name for title and output naming.")
    p.add_argument("--outdir", default=".", help="Output directory for plots.")
    p.add_argument("--bio", default=None, help="Optional Excel with fraction start/end (+bioactivity).")
    p.add_argument("--rt-start", type=float, default=None, help="Fractionation start (min) if --bio not given.")
    p.add_argument("--width", type=float, default=None, help="Fraction width (min) if --bio not given.")
    p.add_argument("--n-fractions", type=int, default=None, help="Number of fractions if --bio not given.")
    p.add_argument("--x-max", type=float, default=None, help="Max x limit (min). Default: inferred.")
    p.add_argument("--log-total-area", action="store_true", help="Use log10(total area) scaling.")
    p.add_argument("--png", action="store_true", help="Also save PNG.")
    p.add_argument("--no-svg", action="store_true", help="Do not save SVG.")
    return p


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _build_argparser().parse_args(list(argv) if argv is not None else None)
    fig, _, saved = make_two_sided_plot(
        chrom_mzml=Path(args.mzml).expanduser().resolve(),
        filtered_csv=Path(args.filtered).expanduser().resolve(),
        sample_name=args.sample_name,
        out_dir=Path(args.outdir).expanduser().resolve(),
        bioactivity_excel=Path(args.bio).expanduser().resolve() if args.bio else None,
        rt_start=args.rt_start,
        fraction_width=args.width,
        n_fractions=args.n_fractions,
        x_max=args.x_max,
        log_total_area=bool(args.log_total_area),
        save_svg=not bool(args.no_svg),
        save_png=bool(args.png),
    )
    # Avoid GUI backend issues when used purely as CLI.
    import matplotlib.pyplot as plt
    plt.close(fig)
    if saved:
        print("Saved:")
        for p in saved:
            print(f"  - {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
