#!/usr/bin/env python3
"""Core plotting logic for chromatogram, fraction features, and activity overlays."""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


LOGGER = logging.getLogger("two_sided_plot_core")

_REPO_DIR = Path(__file__).resolve().parent.parent
if str(_REPO_DIR) not in sys.path:
    sys.path.insert(0, str(_REPO_DIR))

try:
    from shared import bioassay_plate_reader as plate_reader
except Exception:  # pragma: no cover - only needed for plate-reader activity input
    plate_reader = None  # type: ignore[assignment]


def slugify(name: str) -> str:
    value = (name or "").strip().lower()
    value = value.replace("&", " and ")
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^a-z0-9_]+", "", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "sample"


def _find_col(df: pd.DataFrame, preferred: str, fallbacks: Tuple[str, ...]) -> str:
    columns = list(df.columns)
    if preferred in columns:
        return preferred
    for candidate in fallbacks:
        if candidate in columns:
            return candidate
    raise ValueError(f"Missing required column '{preferred}'. Available columns: {columns}")


def read_table(path: Path | str, *, sheet_name: str | int | None = None) -> pd.DataFrame:
    path = Path(path).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Table not found: {path}")
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".tsv", ".txt"}:
        return pd.read_csv(path, sep="\t")
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, sheet_name=sheet_name if sheet_name not in {"", None} else 0)
    raise ValueError(f"Unsupported table type: {path.suffix}. Use CSV, TSV, TXT, XLSX, or XLS.")


def excel_sheet_names(path: Path | str) -> list[str]:
    path = Path(path).expanduser()
    if path.suffix.lower() not in {".xlsx", ".xls"}:
        return []
    return list(pd.ExcelFile(path).sheet_names)


def read_filtered_features(filtered_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(filtered_csv)
    frac_col = _find_col(df, "fraction_index", ("fraction", "frac", "fraction_id"))
    rt_col = _find_col(df, "rt", ("retention_time", "row retention time", "row_rt", "row retention time (min)"))
    area_col = _find_col(df, "area", ("Area", "peak_area", "Peak area", "row area", "row_area"))
    out = df.rename(columns={frac_col: "fraction_index", rt_col: "rt", area_col: "area"}).copy()
    out = out.dropna(subset=["fraction_index", "rt", "area"])
    if out.empty:
        raise ValueError("Filtered feature table is empty after reading required columns.")
    out["fraction_index"] = pd.to_numeric(out["fraction_index"], errors="coerce")
    out["rt"] = pd.to_numeric(out["rt"], errors="coerce")
    out["area"] = pd.to_numeric(out["area"], errors="coerce")
    out = out.dropna(subset=["fraction_index", "rt", "area"])
    out["fraction_index"] = out["fraction_index"].astype(int)
    out = out[out["area"] > 0].copy()
    if out.empty:
        raise ValueError("Filtered feature table has no positive-area rows.")
    return out


@dataclass
class FractionWindows:
    fraction_index: np.ndarray
    start: np.ndarray
    end: np.ndarray
    mid: np.ndarray
    width: np.ndarray


@dataclass
class ActivitySeries:
    fraction_index: np.ndarray
    raw_value: np.ndarray
    display_value: np.ndarray
    label: str = "Activity"
    display_mode: str = "raw"


@dataclass
class ActivityTableSettings:
    path: str
    input_type: str = "table"
    sheet_name: str | None = None
    fraction_column: str | None = None
    start_column: str | None = None
    end_column: str | None = None
    value_column: str | None = None
    replicate_columns: tuple[str, ...] = ()
    control_mode: str = "none"
    control_row_indices: tuple[int, ...] = ()
    control_column: str | None = None
    control_value: str | None = None
    control_query: str | None = None
    control_scalar_column: str | None = None
    explicit_control_value: float | None = None
    exclude_control_rows: bool = True
    normalization_mode: str = "none"
    display_mode: str = "percent_of_max"
    label: str = ""
    plate_files: tuple[str, ...] = ()
    plate_rows: int = 8
    plate_columns: int = 12
    plate_orientation: str = "auto"
    plate_positive_control_wells: str = ""
    plate_positive_control_wells_by_file: Any = None
    plate_scale_mode: str = "none"
    plate_exclude_control_wells: bool = True


@dataclass
class PlotStyle:
    title: str | None = None
    top_y_label: str = "MS intensity (BPC)"
    bottom_y_label: str | None = None
    x_label: str = "Retention time [min]"
    activity_y_label: str | None = None
    chrom_color: str = "#101b73"
    dominant_feature_color: str = "#101b73"
    remainder_feature_color: str = "#ddd6cc"
    activity_color: str = "#2f9e44"
    activity_alpha: float = 0.28
    activity_edge_color: str = "#101010"
    font_family: str = "Arial"
    title_size: float = 16.0
    axis_label_size: float = 12.0
    tick_label_size: float = 9.0
    line_width: float = 1.5
    figsize: tuple[float, float] = (14.0, 8.0)


def build_uniform_fraction_windows(n_fractions: int, rt_start: float, width: float) -> FractionWindows:
    if n_fractions <= 0:
        raise ValueError("n_fractions must be greater than zero.")
    if width <= 0:
        raise ValueError("fraction_width must be greater than zero.")
    fraction_index = np.arange(1, n_fractions + 1, dtype=int)
    start = rt_start + (fraction_index - 1) * width
    end = start + width
    mid = (start + end) / 2.0
    widths = np.full_like(mid, width, dtype=float)
    return FractionWindows(fraction_index=fraction_index, start=start, end=end, mid=mid, width=widths)


def compute_fraction_stats(filtered_df: pd.DataFrame) -> pd.DataFrame:
    grouped = filtered_df.groupby("fraction_index", as_index=False)["area"].agg(total_area="sum", max_area="max")
    grouped["total_area"] = grouped["total_area"].astype(float)
    grouped["max_area"] = grouped["max_area"].astype(float)
    return grouped


def read_bpc_from_mzml(mzml_path: Path) -> Tuple[np.ndarray, np.ndarray]:
    try:
        import pymzml  # type: ignore
    except Exception as exc:
        raise ImportError("pymzml is required to read mzML files. Install it with: pip install pymzml") from exc

    times: list[float] = []
    intensities: list[float] = []
    for spectrum in pymzml.run.Reader(str(mzml_path)):
        scan_time = spectrum.scan_time[0] if isinstance(spectrum.scan_time, tuple) else spectrum.scan_time
        peaks = spectrum.peaks("raw")
        if isinstance(scan_time, (int, float)) and hasattr(peaks, "__len__") and len(peaks) > 0:
            times.append(float(scan_time))
            intensities.append(float(max(peak[1] for peak in peaks)))
    if not times:
        raise ValueError(f"No usable scans found in mzML: {mzml_path}")
    return np.asarray(times, dtype=float), np.asarray(intensities, dtype=float)


def parse_row_numbers(text: str | Sequence[int] | None) -> list[int]:
    if text is None:
        return []
    if not isinstance(text, str):
        return [int(value) for value in text]
    values: list[int] = []
    for part in [piece.strip() for piece in text.split(",") if piece.strip()]:
        if "-" in part:
            left, right = [piece.strip() for piece in part.split("-", 1)]
            start, end = int(left), int(right)
            if end < start:
                raise ValueError(f"Invalid row range: {part}")
            values.extend(range(start, end + 1))
        else:
            values.append(int(part))
    return values


def _row_mask_from_indices(index: pd.Index, row_numbers: Sequence[int]) -> pd.Series:
    mask = pd.Series(False, index=index, dtype=bool)
    for row_number in row_numbers:
        if row_number < 1 or row_number > len(index):
            raise ValueError(f"Control row {row_number} is outside the loaded activity table.")
        mask.iloc[row_number - 1] = True
    return mask


def select_control_mask(df: pd.DataFrame, settings: ActivityTableSettings) -> pd.Series:
    mode = settings.control_mode.lower().strip()
    mask = pd.Series(False, index=df.index, dtype=bool)
    if mode in {"", "none"}:
        return mask
    if mode == "row_numbers":
        mask = _row_mask_from_indices(df.index, settings.control_row_indices)
    elif mode == "column_value":
        if not settings.control_column:
            raise ValueError("Choose a control-identification column.")
        if settings.control_column not in df.columns:
            raise ValueError(f"Control column not found: {settings.control_column}")
        mask = df[settings.control_column].astype("string") == str(settings.control_value)
    elif mode == "query":
        if not settings.control_query:
            raise ValueError("Enter a control query.")
        try:
            evaluated = df.eval(settings.control_query)
        except Exception:
            try:
                evaluated = df.index.isin(df.query(settings.control_query).index)
            except Exception as exc:
                raise ValueError(f"Invalid control query: {exc}") from exc
        mask = pd.Series(evaluated, index=df.index, dtype=bool)
    else:
        raise ValueError(f"Unknown control mode: {settings.control_mode}")
    if not mask.any():
        raise ValueError("No activity-table rows matched the selected control rule.")
    return mask.fillna(False)


def _compute_activity_signal(df: pd.DataFrame, settings: ActivityTableSettings) -> pd.Series:
    replicate_columns = [column for column in settings.replicate_columns if column]
    if replicate_columns:
        missing = [column for column in replicate_columns if column not in df.columns]
        if missing:
            raise ValueError(f"Replicate columns not found in activity table: {missing}")
        numeric = df[replicate_columns].apply(pd.to_numeric, errors="coerce")
        return numeric.mean(axis=1, skipna=True)
    if settings.value_column:
        if settings.value_column not in df.columns:
            raise ValueError(f"Activity value column not found: {settings.value_column}")
        return pd.to_numeric(df[settings.value_column], errors="coerce")
    raise ValueError("Choose either one activity value column or one or more replicate columns.")


def _resolve_control_reference(df: pd.DataFrame, signal: pd.Series, control_mask: pd.Series, settings: ActivityTableSettings) -> float | None:
    if settings.explicit_control_value is not None:
        return float(settings.explicit_control_value)
    if settings.control_scalar_column:
        if settings.control_scalar_column not in df.columns:
            raise ValueError(f"Shared control column not found: {settings.control_scalar_column}")
        values = pd.to_numeric(df[settings.control_scalar_column], errors="coerce").dropna()
        if values.empty:
            raise ValueError(f"Shared control column '{settings.control_scalar_column}' contains no numeric values.")
        return float(values.iloc[0])
    if control_mask.any():
        values = pd.to_numeric(signal[control_mask], errors="coerce").dropna()
        if values.empty:
            raise ValueError("Selected control rows contain no usable activity values.")
        return float(values.mean())
    return None


def _display_values(signal: pd.Series, control_reference: float | None, settings: ActivityTableSettings) -> pd.Series:
    normalization_mode = settings.normalization_mode.lower().strip()
    if normalization_mode == "none":
        normalized = signal.astype(float)
    elif normalization_mode == "control":
        if control_reference is None or control_reference == 0:
            raise ValueError("Control normalization requires a non-zero control reference.")
        normalized = signal.astype(float) / float(control_reference)
    else:
        raise ValueError("normalization_mode must be 'none' or 'control'.")

    display_mode = settings.display_mode.lower().strip()
    if display_mode == "raw":
        return normalized

    max_value = float(pd.to_numeric(normalized, errors="coerce").max())
    if not np.isfinite(max_value) or max_value == 0:
        raise ValueError("Cannot scale activity values because the maximum value is zero or missing.")
    scaled = normalized / max_value * 100.0
    if display_mode == "percent_of_max":
        return scaled
    if display_mode == "inhibition_from_max":
        return 100.0 - scaled
    raise ValueError("display_mode must be 'raw', 'percent_of_max', or 'inhibition_from_max'.")


def infer_activity_label(settings: ActivityTableSettings) -> str:
    display_mode = settings.display_mode.lower().strip()
    if display_mode == "inhibition_from_max":
        return "Inhibition from signal (%)"
    if display_mode == "percent_of_max":
        return "Relative signal (% of maximum)"
    if settings.input_type == "plate_reader":
        scale_mode = settings.plate_scale_mode.lower().strip()
        if scale_mode == "positive_control_pct":
            return "Fluorescence (% of positive control)"
        if scale_mode in {"positive_control_then_minmax_0_100", "minmax_0_100", "control_then_minmax_0_100"}:
            return "Normalized fluorescence (0-100)"
        return "Raw fluorescence"
    if settings.normalization_mode.lower().strip() == "control":
        return "Signal relative to control"
    return "Signal value"


def _fraction_index_for_activity(df: pd.DataFrame, settings: ActivityTableSettings) -> pd.Series:
    if settings.fraction_column:
        if settings.fraction_column not in df.columns:
            raise ValueError(f"Fraction column not found: {settings.fraction_column}")
        values = pd.to_numeric(df[settings.fraction_column], errors="coerce")
    else:
        values = pd.Series(np.arange(1, len(df) + 1, dtype=int), index=df.index, dtype=float)
    return values


def prepare_activity_overlay(
    settings: ActivityTableSettings,
    *,
    fallback_windows: FractionWindows | None = None,
) -> tuple[FractionWindows | None, ActivitySeries, pd.DataFrame]:
    activity_label = settings.label or infer_activity_label(settings)
    if str(settings.input_type or "table").strip().lower() == "plate_reader":
        if plate_reader is None:
            raise ImportError("Plate-reader activity input requires shared/bioassay_plate_reader.py in the workflow repository.")
        if not settings.plate_files:
            raise ValueError("Plate-reader activity input requires at least one replicate plate file.")
        df = plate_reader.build_fraction_activity_table(
            settings.plate_files,
            rows=int(settings.plate_rows),
            columns=int(settings.plate_columns),
            # Raw plate mode searches every sheet for the plate grid. A sheet
            # chosen for prepared-table mode must never constrain raw files.
            sheet_name=None,
            orientation=settings.plate_orientation,
            control_wells=settings.plate_positive_control_wells,
            control_wells_by_plate=settings.plate_positive_control_wells_by_file,
            scale_mode=settings.plate_scale_mode,
            exclude_control_wells=bool(settings.plate_exclude_control_wells),
        )
        settings = ActivityTableSettings(
            path="",
            input_type="table",
            fraction_column="fraction",
            value_column="average",
            control_scalar_column=None,
            normalization_mode="none",
            display_mode=settings.display_mode,
            label=activity_label,
        )
    else:
        df = read_table(settings.path, sheet_name=settings.sheet_name).copy()
    if df.empty:
        raise ValueError("Activity table is empty.")

    control_mask = select_control_mask(df, settings)
    signal = _compute_activity_signal(df, settings)
    control_reference = _resolve_control_reference(df, signal, control_mask, settings)
    display = _display_values(signal, control_reference, settings)
    fraction_index = _fraction_index_for_activity(df, settings)

    prepared = df.copy()
    prepared["__fraction_index__"] = fraction_index
    prepared["__activity_signal__"] = signal
    prepared["__activity_display__"] = display
    prepared["__is_control__"] = control_mask

    plot_rows = prepared.copy()
    if settings.exclude_control_rows and control_mask.any():
        plot_rows = plot_rows.loc[~control_mask].copy()
    plot_rows = plot_rows.dropna(subset=["__fraction_index__", "__activity_display__"]).copy()
    if plot_rows.empty:
        raise ValueError("No activity rows remain after control filtering and numeric cleanup.")
    plot_rows["__fraction_index__"] = plot_rows["__fraction_index__"].astype(int)

    windows: FractionWindows | None = None
    if settings.start_column or settings.end_column:
        if not settings.start_column or not settings.end_column:
            raise ValueError("Provide both start and end columns, or leave both blank.")
        for column in [settings.start_column, settings.end_column]:
            if column not in plot_rows.columns:
                raise ValueError(f"Window column not found: {column}")
        start = pd.to_numeric(plot_rows[settings.start_column], errors="coerce")
        end = pd.to_numeric(plot_rows[settings.end_column], errors="coerce")
        valid = start.notna() & end.notna()
        plot_rows = plot_rows.loc[valid].copy()
        if plot_rows.empty:
            raise ValueError("Activity table has no rows with valid explicit start/end times.")
        start = start.loc[valid].to_numpy(dtype=float)
        end = end.loc[valid].to_numpy(dtype=float)
        windows = FractionWindows(
            fraction_index=plot_rows["__fraction_index__"].to_numpy(dtype=int),
            start=start,
            end=end,
            mid=(start + end) / 2.0,
            width=end - start,
        )
    elif fallback_windows is None:
        raise ValueError("Activity table does not define start/end columns, so uniform fraction windows are required.")

    activity = ActivitySeries(
        fraction_index=plot_rows["__fraction_index__"].to_numpy(dtype=int),
        raw_value=plot_rows["__activity_signal__"].to_numpy(dtype=float),
        display_value=plot_rows["__activity_display__"].to_numpy(dtype=float),
        label=activity_label,
        display_mode=settings.display_mode.lower().strip(),
    )
    return windows, activity, prepared


def legacy_activity_settings_from_excel(path: Path) -> ActivityTableSettings:
    """Best-effort bridge for older workbooks passed through ``--bio``."""
    df = read_table(path)
    columns = set(df.columns)
    start_column = "start" if "start" in columns else None
    end_column = "end" if "end" in columns else None
    return ActivityTableSettings(
        path=str(path),
        fraction_column="fraction" if "fraction" in columns else None,
        start_column=start_column,
        end_column=end_column,
        value_column="average" if "average" in columns else None,
        control_scalar_column="pos_avg" if "pos_avg" in columns else None,
        normalization_mode="control" if "pos_avg" in columns else "none",
        display_mode="percent_of_max",
        label="Relative signal (% of maximum)",
    )


def activity_settings_from_dict(data: dict[str, Any]) -> ActivityTableSettings:
    payload = dict(data)
    payload.setdefault("path", "")
    payload["replicate_columns"] = tuple(payload.get("replicate_columns", ()) or ())
    payload["control_row_indices"] = tuple(int(value) for value in payload.get("control_row_indices", ()) or ())
    payload["plate_files"] = tuple(payload.get("plate_files", ()) or ())
    return ActivityTableSettings(**payload)


def normalize_plot_style(plot_style: PlotStyle | dict[str, Any] | None) -> PlotStyle:
    if plot_style is None:
        return PlotStyle()
    if isinstance(plot_style, PlotStyle):
        return plot_style
    return PlotStyle(**plot_style)


def _fraction_windows_from_activity_or_uniform(
    stats: pd.DataFrame,
    *,
    activity_settings: ActivityTableSettings | None,
    bioactivity_excel: Path | None,
    rt_start: float | None,
    fraction_width: float | None,
    n_fractions: int | None,
) -> tuple[FractionWindows, ActivitySeries | None, pd.DataFrame | None]:
    if activity_settings is None and bioactivity_excel is not None:
        activity_settings = legacy_activity_settings_from_excel(bioactivity_excel)

    fallback_windows: FractionWindows | None = None
    if rt_start is not None and fraction_width is not None:
        inferred_n = n_fractions or (int(stats["fraction_index"].max()) if not stats.empty else None)
        if inferred_n is not None:
            fallback_windows = build_uniform_fraction_windows(int(inferred_n), float(rt_start), float(fraction_width))

    if activity_settings is not None:
        explicit_windows, activity, prepared = prepare_activity_overlay(activity_settings, fallback_windows=fallback_windows)
        windows = explicit_windows or fallback_windows
        if windows is None:
            raise ValueError("Could not determine fraction windows for the activity overlay.")
        return windows, activity, prepared

    if fallback_windows is None:
        raise ValueError("Provide activity data with explicit windows, or supply RT start and fraction width.")
    return fallback_windows, None, None


def _plot_total_columns(plot_df: pd.DataFrame, log_total_area: bool) -> pd.DataFrame:
    out = plot_df.copy()
    if log_total_area:
        total = out["total_area"].to_numpy(dtype=float)
        with np.errstate(divide="ignore", invalid="ignore"):
            total_log = np.where(total > 0, np.log10(total), 0.0)
            proportion = np.where(total > 0, out["max_area"].to_numpy(dtype=float) / total, 0.0)
        out["plot_total"] = total_log
        out["plot_max"] = total_log * proportion
    else:
        out["plot_total"] = out["total_area"]
        out["plot_max"] = out["max_area"]
    return out


def make_two_sided_plot(
    chrom_mzml: Path,
    filtered_csv: Path,
    sample_name: str,
    out_dir: Path,
    bioactivity_excel: Optional[Path] = None,
    *,
    activity_settings: ActivityTableSettings | dict[str, Any] | None = None,
    rt_start: Optional[float] = None,
    fraction_width: Optional[float] = None,
    n_fractions: Optional[int] = None,
    x_max: Optional[float] = None,
    log_total_area: bool = False,
    save_svg: bool = True,
    save_png: bool = False,
    plot_style: PlotStyle | dict[str, Any] | None = None,
) -> tuple[Any, tuple[Any, Any], list[Path], dict[str, Any]]:
    """Create and optionally save the chromatogram / feature / activity figure."""
    out_dir = Path(out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    style = normalize_plot_style(plot_style)
    settings = activity_settings_from_dict(activity_settings) if isinstance(activity_settings, dict) else activity_settings

    features = read_filtered_features(Path(filtered_csv))
    stats = compute_fraction_stats(features)
    times, intensities = read_bpc_from_mzml(Path(chrom_mzml))
    windows, activity, prepared_activity = _fraction_windows_from_activity_or_uniform(
        stats,
        activity_settings=settings,
        bioactivity_excel=bioactivity_excel,
        rt_start=rt_start,
        fraction_width=fraction_width,
        n_fractions=n_fractions,
    )

    window_df = pd.DataFrame(
        {
            "fraction_index": windows.fraction_index,
            "mid": windows.mid,
            "width": windows.width,
            "start": windows.start,
            "end": windows.end,
        }
    )
    plot_df = window_df.merge(stats, on="fraction_index", how="left")
    plot_df[["total_area", "max_area"]] = plot_df[["total_area", "max_area"]].fillna(0.0)
    plot_df = _plot_total_columns(plot_df, log_total_area=log_total_area)

    import matplotlib as mpl
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator, StrMethodFormatter

    mpl.rcParams["font.family"] = style.font_family
    mpl.rcParams["svg.fonttype"] = "none"

    fig, (ax_top, ax_bottom) = plt.subplots(
        2,
        1,
        sharex=True,
        figsize=style.figsize,
        gridspec_kw={"height_ratios": [1, 1], "hspace": 0},
    )
    fig.subplots_adjust(hspace=-0.05, right=0.95)

    ax_top.plot(times, intensities, color=style.chrom_color, lw=style.line_width)
    ax_top.set_ylabel(style.top_y_label, fontsize=style.axis_label_size)
    ax_top.set_ylim(0, float(intensities.max()) * 1.05 if intensities.size else 1.0)
    ax_top.spines["bottom"].set_visible(False)
    ax_top.xaxis.set_ticks_position("none")
    title = style.title or sample_name
    if log_total_area:
        title += " (log10 total area proportion)"
    ax_top.set_title(title, fontsize=style.title_size)

    if activity is not None:
        ax_activity = ax_top.twinx()
        activity_df = pd.DataFrame({"fraction_index": activity.fraction_index, "value": activity.display_value})
        activity_plot = window_df.merge(activity_df, on="fraction_index", how="left")
        ax_activity.bar(
            x=activity_plot["mid"],
            height=activity_plot["value"].fillna(0.0),
            width=activity_plot["width"],
            alpha=style.activity_alpha,
            edgecolor=style.activity_edge_color,
            color=style.activity_color,
        )
        ax_activity.set_ylabel(style.activity_y_label or activity.label, fontsize=style.axis_label_size)
        finite_activity = pd.to_numeric(activity_plot["value"], errors="coerce").dropna()
        if activity.display_mode in {"percent_of_max", "inhibition_from_max"}:
            ax_activity.set_ylim(0, 100.0)
        else:
            ymax = max(100.0, float(finite_activity.max()) * 1.05) if not finite_activity.empty else 100.0
            ax_activity.set_ylim(0, ymax)
        ax_activity.yaxis.set_major_locator(MaxNLocator(integer=True))
        ax_activity.yaxis.set_major_formatter(StrMethodFormatter("{x:.0f}"))

    for _, row in plot_df.iterrows():
        dominant_height = float(row["plot_max"])
        total_height = float(row["plot_total"])
        remainder_height = max(total_height - dominant_height, 0.0)
        if dominant_height > 0:
            ax_bottom.bar(
                float(row["mid"]),
                dominant_height,
                width=float(row["width"]),
                color=style.dominant_feature_color,
                edgecolor=style.remainder_feature_color,
            )
        if remainder_height > 0:
            ax_bottom.bar(
                float(row["mid"]),
                remainder_height,
                bottom=dominant_height,
                width=float(row["width"]),
                color=style.remainder_feature_color,
                edgecolor="none",
            )

    bottom_label = style.bottom_y_label or ("log10(total area)" if log_total_area else "Feature area")
    ax_bottom.set_ylabel(bottom_label, fontsize=style.axis_label_size)
    ax_bottom.set_xlabel(style.x_label, fontsize=style.axis_label_size)
    ax_bottom.spines["top"].set_visible(False)
    ymax_bottom = float(plot_df["plot_total"].max()) if not plot_df.empty else 1.0
    ax_bottom.set_ylim(ymax_bottom * 1.05 if ymax_bottom > 0 else 1.0, 0)

    if x_max is None:
        x_max = float(max(times.max(), windows.end.max()))
    ax_top.set_xlim(0, x_max)
    ax_bottom.set_xlim(0, x_max)
    ticks = np.arange(0, int(np.floor(x_max / 5) * 5) + 1, 5)
    ax_bottom.set_xticks(ticks)
    ax_bottom.set_xticklabels([str(int(tick)) for tick in ticks], rotation=90, fontsize=style.tick_label_size)
    ax_top.tick_params(axis="y", labelsize=style.tick_label_size)
    ax_bottom.tick_params(axis="y", labelsize=style.tick_label_size)
    fig.tight_layout()

    base_name = slugify(sample_name)
    suffix = "_log" if log_total_area else ""
    saved_paths: list[Path] = []
    if save_svg:
        svg_path = out_dir / f"{base_name}{suffix}.svg"
        fig.savefig(svg_path, dpi=300, bbox_inches="tight")
        saved_paths.append(svg_path)
    if save_png:
        png_path = out_dir / f"{base_name}{suffix}.png"
        fig.savefig(png_path, dpi=300, bbox_inches="tight")
        saved_paths.append(png_path)

    summary = {
        "feature_rows": int(len(features)),
        "fraction_count": int(len(window_df)),
        "activity_rows": int(len(activity.fraction_index)) if activity is not None else 0,
        "saved_paths": [str(path) for path in saved_paths],
        "plot_style": asdict(style),
        "activity_settings": asdict(settings) if settings is not None else None,
        "activity_preview_rows": int(len(prepared_activity)) if prepared_activity is not None else 0,
    }
    LOGGER.info("Prepared plot with %s feature row(s) across %s fraction(s).", len(features), len(window_df))
    return fig, (ax_top, ax_bottom), saved_paths, summary


def template_config() -> dict[str, Any]:
    return {
        "sample_name": "sample_name_here",
        "chrom_mzml": "",
        "filtered_csv": "",
        "out_dir": "outputs",
        "fraction_windows": {"rt_start": 2.0, "fraction_width": 0.375, "n_fractions": 96},
        "activity_table": {
            "path": "",
            "sheet_name": "",
            "fraction_column": "fraction",
            "start_column": "",
            "end_column": "",
            "value_column": "average",
            "replicate_columns": [],
            "control_mode": "none",
            "control_row_indices": [],
            "control_column": "",
            "control_value": "",
            "control_query": "",
            "control_scalar_column": "pos_avg",
            "explicit_control_value": None,
            "exclude_control_rows": True,
            "normalization_mode": "control",
            "display_mode": "percent_of_max",
            "label": "",
        },
        "plot": {
            "x_max": None,
            "log_total_area": False,
            "save_svg": True,
            "save_png": False,
            "style": asdict(PlotStyle()),
        },
    }


def _build_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create a chromatogram + fraction-feature + activity plot.")
    parser.add_argument("--config", help="Optional JSON config generated by the GUI.")
    parser.add_argument("--mzml", help="Path to mzML for the chromatogram.")
    parser.add_argument("--filtered", help="Path to <sample>_filtered.csv from script 01.")
    parser.add_argument("--sample-name", help="Sample name for title and output naming.")
    parser.add_argument("--outdir", default=".", help="Output directory.")
    parser.add_argument("--bio", default=None, help="Legacy Excel file with start/end/average/pos_avg.")
    parser.add_argument("--rt-start", type=float, default=None, help="Uniform fractionation start in minutes.")
    parser.add_argument("--width", type=float, default=None, help="Uniform fraction width in minutes.")
    parser.add_argument("--n-fractions", type=int, default=None, help="Uniform number of fractions.")
    parser.add_argument("--x-max", type=float, default=None, help="Optional maximum retention time.")
    parser.add_argument("--log-total-area", action="store_true", help="Use log10(total area) bars.")
    parser.add_argument("--png", action="store_true", help="Also save PNG.")
    parser.add_argument("--no-svg", action="store_true", help="Do not save SVG.")
    return parser


def _run_from_config(path: Path) -> int:
    config = json.loads(path.read_text(encoding="utf-8"))
    activity_cfg = config.get("activity_table") or None
    windows_cfg = config.get("fraction_windows", {})
    plot_cfg = config.get("plot", {})
    fig, _, saved, summary = make_two_sided_plot(
        chrom_mzml=Path(config["chrom_mzml"]).expanduser(),
        filtered_csv=Path(config["filtered_csv"]).expanduser(),
        sample_name=config["sample_name"],
        out_dir=Path(config.get("out_dir", ".")).expanduser(),
        activity_settings=activity_cfg,
        rt_start=windows_cfg.get("rt_start"),
        fraction_width=windows_cfg.get("fraction_width"),
        n_fractions=windows_cfg.get("n_fractions"),
        x_max=plot_cfg.get("x_max"),
        log_total_area=bool(plot_cfg.get("log_total_area", False)),
        save_svg=bool(plot_cfg.get("save_svg", True)),
        save_png=bool(plot_cfg.get("save_png", False)),
        plot_style=plot_cfg.get("style"),
    )
    import matplotlib.pyplot as plt

    plt.close(fig)
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    for saved_path in saved:
        print(f"Saved: {saved_path}")
    return 0


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _build_argparser().parse_args(list(argv) if argv is not None else None)
    if args.config:
        return _run_from_config(Path(args.config).expanduser())
    if not args.mzml or not args.filtered or not args.sample_name:
        raise SystemExit("Provide --config, or provide --mzml, --filtered, and --sample-name.")
    fig, _, saved, _ = make_two_sided_plot(
        chrom_mzml=Path(args.mzml).expanduser(),
        filtered_csv=Path(args.filtered).expanduser(),
        sample_name=args.sample_name,
        out_dir=Path(args.outdir).expanduser(),
        bioactivity_excel=Path(args.bio).expanduser() if args.bio else None,
        rt_start=args.rt_start,
        fraction_width=args.width,
        n_fractions=args.n_fractions,
        x_max=args.x_max,
        log_total_area=bool(args.log_total_area),
        save_svg=not bool(args.no_svg),
        save_png=bool(args.png),
    )
    import matplotlib.pyplot as plt

    plt.close(fig)
    for path in saved:
        print(f"Saved: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
