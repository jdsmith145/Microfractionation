#!/usr/bin/env python3
"""Core UPLC-to-HPLC fraction prediction and bioactivity mapping workflow."""
from __future__ import annotations

import argparse
import json
import logging
import math
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd


LOGGER = logging.getLogger("fraction_predictor_core")

_REPO_DIR = Path(__file__).resolve().parent.parent
if str(_REPO_DIR) not in sys.path:
    sys.path.insert(0, str(_REPO_DIR))

try:
    from shared import bioassay_plate_reader as plate_reader
except Exception:  # pragma: no cover - handled when plate-reader input is requested
    plate_reader = None  # type: ignore[assignment]


# ---------------------------
# Helpers
# ---------------------------

def slugify(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "_", str(value).strip())
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "item"


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def resolve_path(base_dir: str | Path, path_value: str | Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return Path(base_dir) / path


def read_table(path: str | Path, **kwargs: Any) -> pd.DataFrame:
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix in {".csv", ".txt"}:
        return pd.read_csv(path, **kwargs)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, **kwargs)
    raise ValueError(f"Unsupported file type for {path}. Use CSV or Excel.")


def read_table_columns(path: str | Path, **kwargs: Any) -> list[str]:
    """Return column names from a supported table."""
    return [str(col) for col in read_table(path, **kwargs).columns]


def write_table(df: pd.DataFrame, path: str | Path) -> Path:
    path = Path(path)
    ensure_directory(path.parent)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df.to_csv(path, index=False)
    elif suffix in {".xlsx", ".xls"}:
        df.to_excel(path, index=False)
    else:
        raise ValueError(f"Unsupported output file type for {path}. Use CSV or Excel.")
    return path


def require_columns(df: pd.DataFrame, columns: Iterable[str], table_name: str = "table") -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns in {table_name}: {missing}")


def first_notna(series: pd.Series) -> float:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        raise ValueError(f"Column '{series.name}' does not contain any numeric values.")
    return float(clean.iloc[0])


def normalize_id_column(df: pd.DataFrame, id_column: Optional[str]) -> tuple[pd.DataFrame, str, bool]:
    out = df.copy()
    if id_column and id_column in out.columns:
        return out, id_column, False

    generated_col = "generated_row_id"
    if generated_col in out.columns:
        base = generated_col
        i = 2
        while generated_col in out.columns:
            generated_col = f"{base}_{i}"
            i += 1
    out[generated_col] = np.arange(1, len(out) + 1)
    return out, generated_col, True


def find_column_case_insensitive(df: pd.DataFrame, target: str) -> Optional[str]:
    target_lower = target.lower()
    for col in df.columns:
        if str(col).lower() == target_lower:
            return col
    return None


# ---------------------------
# Calibration
# ---------------------------

@dataclass
class CalibrationModel:
    slope: float
    intercept: float = 0.0
    method: str = "equation"
    r_squared: Optional[float] = None
    n_points: Optional[int] = None
    source: Optional[str] = None
    x_label: str = "UPLC retention time"
    y_label: str = "HPLC retention time"

    def predict(self, values: pd.Series | np.ndarray | list[float] | float) -> Any:
        if isinstance(values, (int, float, np.integer, np.floating)):
            return self.slope * float(values) + self.intercept
        arr = np.asarray(values, dtype=float)
        return self.slope * arr + self.intercept

    def to_dict(self) -> dict[str, Any]:
        return {
            "slope": self.slope,
            "intercept": self.intercept,
            "method": self.method,
            "r_squared": self.r_squared,
            "n_points": self.n_points,
            "source": self.source,
            "x_label": self.x_label,
            "y_label": self.y_label,
        }



def fit_linear_calibration_from_pairs(
    pairs_df: pd.DataFrame,
    x_col: str,
    y_col: str,
    *,
    minimum_points: int = 2,
    recommended_points: int = 5,
    source: Optional[str] = None,
) -> CalibrationModel:
    require_columns(pairs_df, [x_col, y_col], "calibration pairs table")
    work = pairs_df[[x_col, y_col]].copy()
    work[x_col] = pd.to_numeric(work[x_col], errors="coerce")
    work[y_col] = pd.to_numeric(work[y_col], errors="coerce")
    work = work.dropna().reset_index(drop=True)

    n_points = len(work)
    if n_points < minimum_points:
        raise ValueError(
            f"At least {minimum_points} valid calibration points are required, found {n_points}."
        )
    if n_points < recommended_points:
        LOGGER.warning(
            "Only %s calibration point(s) supplied. This is allowed but at least %s is recommended.",
            n_points,
            recommended_points,
        )

    x = work[x_col].to_numpy(dtype=float)
    y = work[y_col].to_numpy(dtype=float)
    x_mean = float(np.mean(x))
    y_mean = float(np.mean(y))
    denominator = float(np.sum((x - x_mean) ** 2))
    if denominator == 0:
        raise ValueError("Calibration pairs must contain at least two different UPLC retention times.")
    slope = float(np.sum((x - x_mean) * (y - y_mean)) / denominator)
    intercept = float(y_mean - slope * x_mean)
    y_pred = slope * x + intercept

    ss_res = float(np.sum((y - y_pred) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r_squared = None if ss_tot == 0 else 1 - ss_res / ss_tot

    return CalibrationModel(
        slope=float(slope),
        intercept=float(intercept),
        method="pairs",
        r_squared=r_squared,
        n_points=n_points,
        source=source,
        x_label=x_col,
        y_label=y_col,
    )



def calibration_from_equation(slope: float, intercept: float = 0.0) -> CalibrationModel:
    return CalibrationModel(
        slope=float(slope),
        intercept=float(intercept),
        method="equation",
        source="user equation",
    )



def calibration_from_runtime_scaling(uplc_total_runtime: float, hplc_total_runtime: float) -> CalibrationModel:
    if uplc_total_runtime <= 0 or hplc_total_runtime <= 0:
        raise ValueError("Both runtimes must be positive numbers.")
    slope = float(hplc_total_runtime) / float(uplc_total_runtime)
    return CalibrationModel(
        slope=slope,
        intercept=0.0,
        method="runtime_scale",
        source="runtime ratio",
    )


FEATURE_ORDER_METHODS = {"feature_order_alignment", "feature-order-alignment", "feature_order", "order"}
OUTSIDE_ANCHOR_LABEL = "Outside anchors"


def normalize_calibration_method(method: Any) -> str:
    text = str(method or "pairs").strip().lower().replace(" ", "_").replace("-", "_")
    if text in {"matched_rt_pairs", "matched_pairs"}:
        return "pairs"
    if text in {"runtime_scaling", "runtime_scale"}:
        return "runtime_scale"
    if text in FEATURE_ORDER_METHODS:
        return "feature_order_alignment"
    return text


def _first_existing_column(df: pd.DataFrame, candidates: Iterable[str], table_name: str) -> str:
    for candidate in candidates:
        found = find_column_case_insensitive(df, candidate)
        if found is not None:
            return found
    raise KeyError(f"Could not find any of these columns in {table_name}: {list(candidates)}")


def _format_fraction_interval(start: int | None, end: int | None) -> tuple[str, str, int]:
    if start is None or end is None:
        return OUTSIDE_ANCHOR_LABEL, "", 0
    lo = int(min(start, end))
    hi = int(max(start, end))
    fractions = list(range(lo, hi + 1))
    label = str(lo) if lo == hi else f"{lo}-{hi}"
    return label, ";".join(map(str, fractions)), len(fractions)


def load_feature_order_landmarks(config: dict[str, Any], *, base_dir: str | Path) -> pd.DataFrame:
    calibration_cfg = config.get("calibration", {}) or {}
    rows = calibration_cfg.get("landmarks", []) or []
    landmarks_file = str(calibration_cfg.get("landmarks_file", "") or "").strip()

    if landmarks_file:
        file_df = read_table(resolve_path(base_dir, landmarks_file))
        if rows:
            table_df = pd.concat([file_df, pd.DataFrame(rows)], ignore_index=True)
        else:
            table_df = file_df
    else:
        table_df = pd.DataFrame(rows)

    if table_df.empty:
        raise ValueError("Feature-order alignment requires at least two landmark rows.")

    anchor_col = _first_existing_column(table_df, ["anchor_id", "landmark_id", "anchor", "id"], "feature-order landmark table")
    hrms_rt_col = _first_existing_column(table_df, ["hrms_rt", "HRMS RT", "uplc_rt", "UPLC RT"], "feature-order landmark table")
    hplc_fraction_col = _first_existing_column(table_df, ["hplc_fraction", "fraction_index", "fraction", "matched_fraction"], "feature-order landmark table")

    optional_cols = {
        "hrms_feature_id": ["hrms_feature_id", "hrms_id", "feature_id", "row ID"],
        "hplc_feature_id": ["hplc_feature_id", "hplc_id", "matched_feature_id", "row ID"],
    }

    out = pd.DataFrame(
        {
            "anchor_id": table_df[anchor_col].astype(str).str.strip(),
            "hrms_rt": pd.to_numeric(table_df[hrms_rt_col], errors="coerce"),
            "hplc_fraction": pd.to_numeric(table_df[hplc_fraction_col], errors="coerce"),
        }
    )
    for output_col, candidates in optional_cols.items():
        source_col = next((find_column_case_insensitive(table_df, candidate) for candidate in candidates if find_column_case_insensitive(table_df, candidate) is not None), None)
        out[output_col] = table_df[source_col].astype(str).str.strip() if source_col else ""

    out = out.dropna(subset=["hrms_rt", "hplc_fraction"]).copy()
    out["hplc_fraction"] = out["hplc_fraction"].astype(int)
    out = out[out["anchor_id"] != ""].copy()
    out = out.sort_values(["hrms_rt", "hplc_fraction", "anchor_id"]).reset_index(drop=True)

    if len(out) < 2:
        raise ValueError("Feature-order alignment requires at least two valid landmarks with anchor ID, HRMS RT, and HPLC fraction.")
    if out["hrms_rt"].duplicated().any():
        duplicate = out.loc[out["hrms_rt"].duplicated(), "hrms_rt"].iloc[0]
        raise ValueError(f"Feature-order landmarks must have unique HRMS RT values. Duplicate RT: {duplicate}")
    if (out["hplc_fraction"].diff().dropna() < 0).any():
        raise ValueError(
            "Feature-order landmark conflict: HPLC fraction numbers must increase with HRMS retention time. "
            "Remove or correct anchors whose order crosses."
        )
    return out


def apply_feature_order_alignment(
    df: pd.DataFrame,
    *,
    rt_column: str,
    landmarks_df: pd.DataFrame,
    predicted_rt_column: str = "predicted_hplc_rt",
    near_anchor_rt_tolerance: float = 0.02,
) -> pd.DataFrame:
    require_columns(df, [rt_column], "feature table before feature-order alignment")
    require_columns(landmarks_df, ["anchor_id", "hrms_rt", "hplc_fraction"], "feature-order landmark table")

    anchors = landmarks_df.sort_values("hrms_rt").reset_index(drop=True)
    anchor_rts = anchors["hrms_rt"].to_numpy(dtype=float)

    out = df.copy()
    out[rt_column] = pd.to_numeric(out[rt_column], errors="coerce")
    out[predicted_rt_column] = np.nan

    alignment_mode: list[str] = []
    alignment_status: list[str] = []
    left_anchor_ids: list[str] = []
    right_anchor_ids: list[str] = []
    start_values: list[float] = []
    end_values: list[float] = []
    count_values: list[int] = []
    labels: list[str] = []
    parsed_values: list[str] = []

    for raw_rt in out[rt_column]:
        alignment_mode.append("feature_order_alignment")
        if pd.isna(raw_rt):
            alignment_status.append("no_hrms_rt")
            left_anchor_ids.append("")
            right_anchor_ids.append("")
            start_values.append(np.nan)
            end_values.append(np.nan)
            count_values.append(0)
            labels.append("No predicted RT")
            parsed_values.append("")
            continue

        rt = float(raw_rt)
        nearest_idx = int(np.argmin(np.abs(anchor_rts - rt)))
        near_anchor = abs(anchor_rts[nearest_idx] - rt) <= float(near_anchor_rt_tolerance)

        if rt < anchor_rts[0]:
            alignment_status.append("before_first_anchor")
            left_anchor_ids.append("")
            right_anchor_ids.append(str(anchors.loc[0, "anchor_id"]))
            start_values.append(np.nan)
            end_values.append(np.nan)
            count_values.append(0)
            labels.append(OUTSIDE_ANCHOR_LABEL)
            parsed_values.append("")
            continue
        if rt > anchor_rts[-1]:
            alignment_status.append("after_last_anchor")
            left_anchor_ids.append(str(anchors.loc[len(anchors) - 1, "anchor_id"]))
            right_anchor_ids.append("")
            start_values.append(np.nan)
            end_values.append(np.nan)
            count_values.append(0)
            labels.append(OUTSIDE_ANCHOR_LABEL)
            parsed_values.append("")
            continue

        right_idx = int(np.searchsorted(anchor_rts, rt, side="right"))
        if right_idx <= 0:
            left_idx, right_idx = 0, 1
        elif right_idx >= len(anchors):
            left_idx, right_idx = len(anchors) - 2, len(anchors) - 1
        else:
            left_idx = right_idx - 1

        left = anchors.loc[left_idx]
        right = anchors.loc[right_idx]
        start_fraction = int(left["hplc_fraction"])
        end_fraction = int(right["hplc_fraction"])
        label, parsed, count = _format_fraction_interval(start_fraction, end_fraction)

        alignment_status.append("near_anchor" if near_anchor else "between_anchors")
        left_anchor_ids.append(str(left["anchor_id"]))
        right_anchor_ids.append(str(right["anchor_id"]))
        start_values.append(float(min(start_fraction, end_fraction)))
        end_values.append(float(max(start_fraction, end_fraction)))
        count_values.append(count)
        labels.append(label)
        parsed_values.append(parsed)

    out["alignment_mode"] = alignment_mode
    out["alignment_status"] = alignment_status
    out["left_anchor_id"] = left_anchor_ids
    out["right_anchor_id"] = right_anchor_ids
    out["candidate_fraction_start"] = start_values
    out["candidate_fraction_end"] = end_values
    out["candidate_fraction_count"] = count_values
    out["matched_fraction"] = labels
    out["parsed_fraction_numbers"] = parsed_values
    return out


# ---------------------------
# Feature table
# ---------------------------


def filter_feature_table(
    feature_df: pd.DataFrame,
    *,
    sample_columns: list[str],
    mz_column: str,
    rt_column: str,
    id_column: Optional[str] = None,
    area_threshold: float = 10000,
) -> tuple[pd.DataFrame, str, bool]:
    df = feature_df.copy()
    require_columns(df, [mz_column, rt_column] + sample_columns, "feature table")
    df, resolved_id_column, generated_id = normalize_id_column(df, id_column)

    sample_block = df[sample_columns].apply(pd.to_numeric, errors="coerce").fillna(0)
    keep_mask = (sample_block >= float(area_threshold)).any(axis=1)
    filtered = df.loc[keep_mask].copy().reset_index(drop=True)

    LOGGER.info(
        "Filtered feature table from %s to %s rows using threshold %.4g across %s sample column(s).",
        len(df),
        len(filtered),
        area_threshold,
        len(sample_columns),
    )
    return filtered, resolved_id_column, generated_id



def apply_rt_calibration(
    df: pd.DataFrame,
    *,
    rt_column: str,
    calibration: CalibrationModel,
    output_column: str = "predicted_hplc_rt",
) -> pd.DataFrame:
    require_columns(df, [rt_column], "feature table before RT calibration")
    out = df.copy()
    out[rt_column] = pd.to_numeric(out[rt_column], errors="coerce")
    out[output_column] = calibration.predict(out[rt_column].to_numpy(dtype=float))
    return out


# ---------------------------
# Fractions
# ---------------------------


def make_fraction_table(
    start_time: float,
    end_time: float,
    n_fractions: int,
    *,
    first_fraction_number: int = 1,
    fraction_column: str = "fraction",
    start_column: str = "start_time",
    end_column: str = "end_time",
) -> pd.DataFrame:
    if n_fractions <= 0:
        raise ValueError("Number of fractions must be > 0.")
    if end_time <= start_time:
        raise ValueError("Fraction end time must be greater than fraction start time.")

    edges = np.linspace(float(start_time), float(end_time), int(n_fractions) + 1)
    df = pd.DataFrame(
        {
            fraction_column: np.arange(first_fraction_number, first_fraction_number + n_fractions),
            start_column: edges[:-1],
            end_column: edges[1:],
        }
    )
    return df



def _match_single_time_to_fraction(
    time_value: float,
    fractions_df: pd.DataFrame,
    *,
    fraction_column: str,
    start_column: str,
    end_column: str,
) -> tuple[str, list[int]]:
    starts = fractions_df[start_column].to_numpy(dtype=float)
    ends = fractions_df[end_column].to_numpy(dtype=float)
    fracs = fractions_df[fraction_column].tolist()

    mask = (starts <= time_value) & (time_value <= ends)
    if mask.any():
        matched = [int(fracs[i]) for i, flag in enumerate(mask) if flag]
        if len(matched) == 1:
            return str(matched[0]), matched
        return " & ".join(map(str, matched)), matched

    if time_value < starts[0] or time_value > ends[-1]:
        return "Fraction not collected", []

    for i in range(len(fractions_df) - 1):
        if ends[i] < time_value < starts[i + 1]:
            matched = [int(fracs[i]), int(fracs[i + 1])]
            return f"{matched[0]} & {matched[1]}", matched

    return "Fraction not collected", []



def match_features_to_fractions(
    df: pd.DataFrame,
    fractions_df: pd.DataFrame,
    *,
    time_column: str = "predicted_hplc_rt",
    fraction_column: str = "fraction",
    start_column: str = "start_time",
    end_column: str = "end_time",
    matched_label_column: str = "matched_fraction",
    parsed_column: str = "parsed_fraction_numbers",
) -> pd.DataFrame:
    require_columns(df, [time_column], "feature table before fraction matching")
    require_columns(fractions_df, [fraction_column, start_column, end_column], "fraction table")

    fr = fractions_df.copy()
    fr[start_column] = pd.to_numeric(fr[start_column], errors="coerce")
    fr[end_column] = pd.to_numeric(fr[end_column], errors="coerce")
    fr = fr.dropna(subset=[start_column, end_column]).sort_values(start_column).reset_index(drop=True)

    if fr.empty:
        raise ValueError("Fraction table is empty after removing rows with non-numeric start/end times.")

    matched_labels: list[str] = []
    parsed_values: list[str] = []

    for raw in pd.to_numeric(df[time_column], errors="coerce"):
        if pd.isna(raw):
            matched_labels.append("No predicted RT")
            parsed_values.append("")
            continue
        label, parsed = _match_single_time_to_fraction(
            float(raw),
            fr,
            fraction_column=fraction_column,
            start_column=start_column,
            end_column=end_column,
        )
        matched_labels.append(label)
        parsed_values.append(";".join(map(str, parsed)))

    out = df.copy()
    out[matched_label_column] = matched_labels
    out[parsed_column] = parsed_values
    return out


# ---------------------------
# Fluorescence and bioactivity
# ---------------------------


def prepare_fluorescence_table(
    df: pd.DataFrame,
    *,
    fraction_column: str,
    average_column: str,
    positive_control_column: str,
    normalized_column: str = "fluorescence_normalized",
    percent_column: str = "fluorescence_percent",
    bioactivity_column: str = "bioactivity",
) -> pd.DataFrame:
    require_columns(df, [fraction_column, average_column, positive_control_column], "fluorescence table")
    out = df.copy()
    out[fraction_column] = pd.to_numeric(out[fraction_column], errors="coerce")
    out[average_column] = pd.to_numeric(out[average_column], errors="coerce")
    out[positive_control_column] = pd.to_numeric(out[positive_control_column], errors="coerce")
    out = out.dropna(subset=[fraction_column, average_column]).copy()

    if out.empty:
        raise ValueError("Fluorescence table does not contain any valid fraction/average rows.")

    positive = out[positive_control_column].replace(0, np.nan)

    # Common real-world layout: one shared control value is stored only once,
    # then all other rows are empty. In that case broadcast the single control
    # value to all fractions.
    usable_positive = positive.dropna()
    if len(usable_positive) == 1:
        shared_control = float(usable_positive.iloc[0])
        positive = pd.Series(shared_control, index=out.index, dtype=float)
        LOGGER.info(
            "Using shared positive control %.6g from column '%s' for all %s fraction rows.",
            shared_control,
            positive_control_column,
            len(out),
        )
    elif usable_positive.empty:
        LOGGER.warning(
            "Positive control column '%s' does not contain usable non-zero values. Falling back to raw averages.",
            positive_control_column,
        )
        positive = None

    if positive is None:
        normalized = out[average_column].astype(float)
    else:
        normalized = out[average_column].astype(float) / positive.astype(float)

    max_norm = float(pd.to_numeric(normalized, errors="coerce").max())
    if max_norm == 0 or math.isnan(max_norm):
        raise ValueError("Cannot normalize fluorescence because the maximum normalized signal is zero or NaN.")

    out[normalized_column] = normalized
    out[percent_column] = out[normalized_column] / max_norm * 100.0
    out[bioactivity_column] = 100.0 - out[percent_column]
    return out


def fluorescence_table_from_config(plant_cfg: dict[str, Any], base_dir: str | Path) -> tuple[pd.DataFrame, dict[str, str]]:
    """Load fraction bioactivity data from a conventional table or raw plate files."""

    input_type = str(plant_cfg.get("fluorescence_input_type", "table") or "table").strip().lower()
    if input_type in {"table", "spreadsheet", "csv", "excel"}:
        fluoro_path = resolve_path(base_dir, plant_cfg["fluorescence_file"])
        fluoro_df = read_table(fluoro_path)
        return fluoro_df, {
            "fraction_column": plant_cfg.get("fluorescence_fraction_column", "fraction"),
            "average_column": plant_cfg.get("fluorescence_average_column", "average"),
            "positive_control_column": plant_cfg.get("fluorescence_positive_control_column", "pos_avg"),
        }

    if input_type not in {"plate_reader", "plate", "well_plate"}:
        raise ValueError(f"Unknown fluorescence input type: {plant_cfg.get('fluorescence_input_type')!r}")
    if plate_reader is None:
        raise ImportError("Plate-reader input requires shared/bioassay_plate_reader.py in the workflow repository.")

    files = plant_cfg.get("plate_files") or plant_cfg.get("fluorescence_plate_files") or []
    if isinstance(files, str):
        files = plate_reader.split_list(files)
    plate_files = [str(resolve_path(base_dir, path)) for path in files]
    if not plate_files:
        raise ValueError("Plate-reader fluorescence input requires at least one file in 'plate_files'.")

    table = plate_reader.build_fraction_activity_table(
        plate_files,
        rows=int(plant_cfg.get("plate_rows", 8)),
        columns=int(plant_cfg.get("plate_columns", 12)),
        sheet_name=plant_cfg.get("plate_sheet_name") or None,
        orientation=str(plant_cfg.get("plate_orientation", "auto") or "auto"),
        control_wells=plant_cfg.get("plate_positive_control_wells", plant_cfg.get("control_wells", "")),
        control_wells_by_plate=plant_cfg.get("plate_positive_control_wells_by_file")
        or plant_cfg.get("control_wells_by_plate"),
        scale_mode=str(plant_cfg.get("plate_scale_mode", "positive_control_then_minmax_0_100") or "positive_control_then_minmax_0_100"),
        exclude_control_wells=bool(plant_cfg.get("plate_exclude_control_wells", True)),
    )
    return table, {
        "fraction_column": "fraction",
        "average_column": "average",
        "positive_control_column": "positive_control_avg",
    }



def make_group_labels(cutoffs: list[float]) -> list[str]:
    if not cutoffs:
        return ["group_all"]
    values = [float(c) for c in sorted(cutoffs)]
    labels: list[str] = [f"group_below_{values[0]:g}"]
    for low, high in zip(values[:-1], values[1:]):
        labels.append(f"group_{low:g}_to_{high:g}")
    labels.append(f"group_above_{values[-1]:g}")
    return labels


def grouping_metric_from_config(config: dict[str, Any]) -> str:
    bio_cfg = config.get("bioactivity", {}) or {}
    metric = str(
        bio_cfg.get("grouping_value")
        or bio_cfg.get("grouping_metric")
        or bio_cfg.get("cutoff_basis")
        or "bioactivity"
    ).strip().lower()
    aliases = {
        "activity": "bioactivity",
        "derived_activity": "bioactivity",
        "derived bioactivity": "bioactivity",
        "bioactivity": "bioactivity",
        "fluorescence": "fluorescence_percent",
        "relative_fluorescence": "fluorescence_percent",
        "relative fluorescence": "fluorescence_percent",
        "signal_percent": "fluorescence_percent",
        "fluorescence_percent": "fluorescence_percent",
        "raw": "average",
        "raw_signal": "average",
        "signal": "average",
        "average": "average",
        "raw_or_processed_signal": "average",
    }
    return aliases.get(metric, metric)


def grouping_metric_label(metric: str) -> str:
    metric = grouping_metric_from_config({"bioactivity": {"grouping_value": metric}})
    if metric == "bioactivity":
        return "derived activity (100 - relative signal)"
    if metric == "fluorescence_percent":
        return "relative fluorescence/signal (% of maximum)"
    if metric == "average":
        return "raw or plate-processed signal average"
    return metric


def grouping_value_column(metric: str) -> str:
    metric = grouping_metric_from_config({"bioactivity": {"grouping_value": metric}})
    if metric == "bioactivity":
        return "bioactivity"
    if metric == "fluorescence_percent":
        return "fluorescence_percent"
    if metric == "average":
        return "average"
    return metric



def assign_bioactivity_groups(
    df: pd.DataFrame,
    *,
    bioactivity_column: str = "bioactivity",
    cutoffs: list[float],
    output_column: str = "bioactivity_group",
    rank_column: str = "bioactivity_group_rank",
) -> pd.DataFrame:
    require_columns(df, [bioactivity_column], "bioactivity table")
    out = df.copy()
    values = sorted(float(c) for c in cutoffs)
    labels = make_group_labels(values)

    def classify(v: float) -> tuple[str, int]:
        if not values:
            return labels[0], 1
        if v < values[0]:
            return labels[0], 1
        for i in range(len(values) - 1):
            if values[i] <= v < values[i + 1]:
                return labels[i + 1], i + 2
        return labels[-1], len(labels)

    groups = out[bioactivity_column].apply(lambda x: classify(float(x)) if pd.notna(x) else ("No data", 0))
    out[output_column] = groups.apply(lambda x: x[0])
    out[rank_column] = groups.apply(lambda x: x[1])
    return out


def _numeric_summary(values: pd.Series) -> dict[str, float | int | None]:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return {"count": 0, "min": None, "q10": None, "q30": None, "median": None, "q70": None, "q90": None, "max": None}
    return {
        "count": int(len(numeric)),
        "min": float(numeric.min()),
        "q10": float(numeric.quantile(0.10)),
        "q30": float(numeric.quantile(0.30)),
        "median": float(numeric.quantile(0.50)),
        "q70": float(numeric.quantile(0.70)),
        "q90": float(numeric.quantile(0.90)),
        "max": float(numeric.max()),
    }


def _group_counts_for_cutoffs(values: pd.Series, cutoffs: list[float]) -> dict[str, int]:
    grouped = assign_bioactivity_groups(pd.DataFrame({"bioactivity": values}), cutoffs=cutoffs)
    return {str(key): int(value) for key, value in grouped["bioactivity_group"].value_counts(sort=False).items()}


def recommend_bioactivity_cutoffs(values: pd.Series) -> list[float]:
    """Suggest cutoffs that isolate roughly the top 30% and top 10% highest response values."""

    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return []
    lower = float(numeric.quantile(0.70))
    upper = float(numeric.quantile(0.90))
    if lower == upper:
        unique_values = sorted(float(value) for value in numeric.unique())
        if len(unique_values) >= 3:
            lower = unique_values[max(0, int(len(unique_values) * 0.70) - 1)]
            upper = unique_values[max(0, int(len(unique_values) * 0.90) - 1)]
        elif len(unique_values) == 2:
            lower, upper = unique_values
        else:
            return [unique_values[0]]
    return [round(lower, 3), round(upper, 3)]


def preview_bioactivity_cutoffs(config: dict[str, Any]) -> dict[str, Any]:
    """Read plant bioactivity inputs and summarize cutoff behavior without writing outputs."""

    base_dir = Path(config.get("base_dir", Path(__file__).resolve().parent))
    current_cutoffs = [float(x) for x in config.get("bioactivity", {}).get("cutoffs", [16.5, 22.5])]
    metric = grouping_metric_from_config(config)
    metric_column = grouping_value_column(metric)
    plant_previews: list[dict[str, Any]] = []
    pooled_values: list[float] = []

    for plant_cfg in config.get("plants", []) or []:
        plant_name = str(plant_cfg.get("name", "plant")).strip() or "plant"
        fluoro_df, fluoro_columns = fluorescence_table_from_config(plant_cfg, base_dir)
        prepared = prepare_fluorescence_table(
            fluoro_df,
            fraction_column=fluoro_columns["fraction_column"],
            average_column=fluoro_columns["average_column"],
            positive_control_column=fluoro_columns["positive_control_column"],
        )
        require_columns(prepared, [metric_column], "prepared fluorescence preview table")
        values = pd.to_numeric(prepared[metric_column], errors="coerce").dropna()
        pooled_values.extend(float(value) for value in values)
        recommended = recommend_bioactivity_cutoffs(values)
        plant_previews.append({
            "plant": plant_name,
            "input_type": plant_cfg.get("fluorescence_input_type", "table"),
            "grouping_metric": metric,
            "grouping_metric_label": grouping_metric_label(metric),
            "fraction_count": int(len(values)),
            "summary": _numeric_summary(values),
            "current_cutoffs": current_cutoffs,
            "recommended_cutoffs": recommended,
            "current_group_counts": _group_counts_for_cutoffs(values, current_cutoffs),
            "recommended_group_counts": _group_counts_for_cutoffs(values, recommended) if recommended else {},
            "response_values": [float(value) for value in values],
            "bioactivity_values": [float(value) for value in values],
        })

    pooled_series = pd.Series(pooled_values, dtype=float)
    pooled_recommended = recommend_bioactivity_cutoffs(pooled_series)
    return {
        "current_cutoffs": current_cutoffs,
        "recommended_cutoffs": pooled_recommended,
        "grouping_metric": metric,
        "grouping_metric_label": grouping_metric_label(metric),
        "summary": _numeric_summary(pooled_series),
        "current_group_counts": _group_counts_for_cutoffs(pooled_series, current_cutoffs) if pooled_values else {},
        "recommended_group_counts": _group_counts_for_cutoffs(pooled_series, pooled_recommended) if pooled_recommended else {},
        "plants": plant_previews,
    }



def _parse_fraction_numbers(value: Any) -> list[int]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return []
    text = str(value).strip()
    if not text:
        return []
    nums = re.findall(r"\d+", text)
    return sorted({int(n) for n in nums})



def map_fraction_groups_to_features(
    feature_df: pd.DataFrame,
    fraction_bio_df: pd.DataFrame,
    *,
    parsed_fractions_column: str = "parsed_fraction_numbers",
    fraction_column: str = "fraction",
    bioactivity_column: str = "bioactivity",
    group_column: str = "bioactivity_group",
    plant_name: str,
) -> pd.DataFrame:
    require_columns(feature_df, [parsed_fractions_column], "feature table before bioactivity mapping")
    require_columns(fraction_bio_df, [fraction_column, bioactivity_column, group_column], "plant bioactivity table")

    slug = slugify(plant_name)
    out = feature_df.copy()
    map_df = fraction_bio_df.copy()
    map_df[fraction_column] = pd.to_numeric(map_df[fraction_column], errors="coerce")
    map_df = map_df.dropna(subset=[fraction_column]).copy()
    map_df[fraction_column] = map_df[fraction_column].astype(int)

    group_lookup = map_df.set_index(fraction_column)[group_column].to_dict()
    bio_lookup = map_df.set_index(fraction_column)[bioactivity_column].to_dict()

    group_values: list[str] = []
    bio_values: list[float | None] = []

    for raw in out[parsed_fractions_column]:
        fractions = _parse_fraction_numbers(raw)
        if not fractions:
            group_values.append("No data")
            bio_values.append(np.nan)
            continue

        matched_groups = [(group_lookup.get(f), bio_lookup.get(f)) for f in fractions if f in group_lookup]
        if not matched_groups:
            group_values.append("No data")
            bio_values.append(np.nan)
            continue

        # Pick the fraction with the highest bioactivity value.
        best_group, best_bio = max(
            matched_groups,
            key=lambda item: (-np.inf if pd.isna(item[1]) else float(item[1])),
        )
        group_values.append(best_group)
        bio_values.append(float(best_bio) if pd.notna(best_bio) else np.nan)

    out[f"bioactivity_group_{slug}"] = group_values
    out[f"bioactivity_value_{slug}"] = bio_values
    out[f"response_group_{slug}"] = group_values
    out[f"response_value_{slug}"] = bio_values
    return out


# ---------------------------
# Merge helper
# ---------------------------


def append_columns_by_id(
    base_df: pd.DataFrame,
    annotation_df: pd.DataFrame,
    *,
    id_column: str,
    columns_to_add: list[str],
    missing_fill_text: Optional[str] = None,
    status_column: Optional[str] = "target_plant_status",
) -> pd.DataFrame:
    require_columns(base_df, [id_column], "base table for merge")
    require_columns(annotation_df, [id_column] + columns_to_add, "annotation table for merge")

    left = base_df.copy()
    right = annotation_df[[id_column] + columns_to_add].drop_duplicates(subset=[id_column]).copy()

    left["__merge_id__"] = pd.to_numeric(left[id_column], errors="coerce").astype("Int64")
    right["__merge_id__"] = pd.to_numeric(right[id_column], errors="coerce").astype("Int64")

    merged = left.merge(right.drop(columns=[id_column]), on="__merge_id__", how="left")

    present_mask = merged[columns_to_add].notna().any(axis=1)
    if status_column:
        merged[status_column] = np.where(
            present_mask,
            "Present in target plants",
            "Not present in target plants",
        )

    if missing_fill_text is not None:
        for col in columns_to_add:
            merged[col] = merged[col].astype(object)
            merged.loc[merged[col].isna(), col] = missing_fill_text

    return merged.drop(columns=["__merge_id__"])


def _clean_bioactivity_group(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "No data"
    text = str(value).strip()
    if not text:
        return "No data"
    if text.startswith("group_below_"):
        return f"below {text.removeprefix('group_below_')}"
    if text.startswith("group_above_"):
        return f"above {text.removeprefix('group_above_')}"
    if text.startswith("group_") and "_to_" in text:
        body = text.removeprefix("group_").replace("_to_", " to ")
        return body
    return text.replace("_", " ")


def _format_float(value: Any, digits: int = 2) -> str:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return ""
    return f"{float(number):.{digits}f}"


def _join_human_list(values: list[str]) -> str:
    cleaned = [str(value).strip() for value in values if str(value).strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    return f"{', '.join(cleaned[:-1])} and {cleaned[-1]}"


def _coerce_bool(value: Any) -> Optional[bool]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "present"}:
        return True
    if text in {"false", "0", "no", "absent"}:
        return False
    return None


def _plant_result_columns(plants_cfg: list[dict[str, Any]]) -> list[dict[str, str]]:
    columns = []
    for plant_cfg in plants_cfg:
        name = str(plant_cfg["name"])
        slug = slugify(name)
        columns.append(
            {
                "name": name,
                "slug": slug,
                "present_col": f"present_in_{slug}",
                "group_col": f"bioactivity_group_{slug}",
                "value_col": f"bioactivity_value_{slug}",
            }
        )
    return columns


def add_target_report_columns(
    df: pd.DataFrame,
    plants_cfg: list[dict[str, Any]],
    *,
    predicted_rt_column: str = "predicted_hplc_rt",
    matched_fraction_column: str = "matched_fraction",
) -> pd.DataFrame:
    out = df.copy()
    plant_columns = _plant_result_columns(plants_cfg)

    for spec in plant_columns:
        for col in [spec["present_col"], spec["group_col"], spec["value_col"]]:
            if col not in out.columns:
                out[col] = np.nan

    present_values: list[str] = []
    absent_values: list[str] = []
    interpretation_values: list[str] = []

    for _, row in out.iterrows():
        present_names = []
        absent_names = []
        activity_parts: list[tuple[int, str]] = []
        any_evaluated = False

        for spec in plant_columns:
            present_state = _coerce_bool(row.get(spec["present_col"]))
            if present_state is True:
                present_names.append(spec["name"])
                any_evaluated = True
            elif present_state is False:
                absent_names.append(spec["name"])
                any_evaluated = True

            group = _clean_bioactivity_group(row.get(spec["group_col"]))
            value = _format_float(row.get(spec["value_col"]))
            if present_state is not None and group != "No data":
                suffix = f" ({value})" if value else ""
                order = 0 if present_state is True else 1 if present_state is False else 2
                activity_parts.append((order, f"{spec['name']}: {group}{suffix}"))

        if not any_evaluated:
            present_text = ""
            absent_text = ""
            interpretation = "Not detected above the peak-area threshold in the selected target plant columns."
        else:
            present_text = "; ".join(present_names)
            absent_text = "; ".join(absent_names)
            activity_text = " | ".join(text for _, text in sorted(activity_parts, key=lambda item: item[0]))
            rt_text = _format_float(row.get(predicted_rt_column))
            fraction_value = row.get(matched_fraction_column, "")
            fraction_text = "" if pd.isna(fraction_value) else str(fraction_value).strip()
            rt_part = f"Predicted HPLC RT {rt_text}" if rt_text else "Predicted HPLC RT unavailable"
            if not fraction_text or fraction_text.lower() == "no predicted rt":
                fraction_part = "no fraction assignment"
            elif fraction_text.lower() == "fraction not collected":
                fraction_part = "outside the collected fraction range"
            else:
                fraction_part = f"fraction {fraction_text}"
            presence_part = f"Present in {_join_human_list(present_names) or 'no selected plants'}"
            absent_part = f"; absent in {_join_human_list(absent_names)}" if absent_names else ""
            activity_part = f". Response group: {activity_text}" if activity_text else ""
            interpretation = f"{rt_part} -> {fraction_part}. {presence_part}{absent_part}{activity_part}."

        present_values.append(present_text)
        absent_values.append(absent_text)
        interpretation_values.append(interpretation)

    out["target_plants_present"] = present_values
    out["target_plants_absent"] = absent_values
    out["target_interpretation"] = interpretation_values
    return out


def merge_human_readable_report_by_id(
    base_df: pd.DataFrame,
    annotation_df: pd.DataFrame,
    *,
    id_column: str | None = None,
    base_id_column: str | None = None,
    annotation_id_column: str | None = None,
    columns_to_add: list[str],
    plants_cfg: list[dict[str, Any]],
    predicted_rt_column: str = "predicted_hplc_rt",
) -> pd.DataFrame:
    base_id = base_id_column or id_column
    annotation_id = annotation_id_column or id_column
    if not base_id or not annotation_id:
        raise ValueError("Both annotation-table and prediction-table match columns are required.")

    require_columns(base_df, [base_id], "base annotation table for final report")
    require_columns(annotation_df, [annotation_id] + columns_to_add, "annotation source table for final report")

    def normalize_id(value: Any) -> str:
        if pd.isna(value):
            return ""
        if isinstance(value, (int, np.integer)):
            return str(int(value))
        if isinstance(value, (float, np.floating)) and float(value).is_integer():
            return str(int(value))
        text = str(value).strip()
        if text.endswith(".0"):
            numeric = pd.to_numeric(text, errors="coerce")
            if pd.notna(numeric) and float(numeric).is_integer():
                return str(int(numeric))
        return text

    left = base_df.copy()
    right = annotation_df[[annotation_id] + columns_to_add].drop_duplicates(subset=[annotation_id]).copy()
    left["__merge_id__"] = left[base_id].map(normalize_id)
    right["__merge_id__"] = right[annotation_id].map(normalize_id)
    merged = left.merge(right.drop(columns=[annotation_id]), on="__merge_id__", how="left").drop(columns=["__merge_id__"])
    merged = add_target_report_columns(
        merged,
        plants_cfg,
        predicted_rt_column=predicted_rt_column,
    )

    plant_cols = []
    internal_cols = []
    for spec in _plant_result_columns(plants_cfg):
        plant_cols.extend([spec["group_col"], spec["value_col"]])
        internal_cols.append(spec["present_col"])
    leading_cols = [
        base_id,
        predicted_rt_column,
        "matched_fraction",
        "parsed_fraction_numbers",
        "alignment_mode",
        "alignment_status",
        "left_anchor_id",
        "right_anchor_id",
        "candidate_fraction_start",
        "candidate_fraction_end",
        "candidate_fraction_count",
        "target_plants_present",
        "target_plants_absent",
        *plant_cols,
    ]
    existing_leading = [col for col in leading_cols if col in merged.columns]
    final_cols = ["target_interpretation"] if "target_interpretation" in merged.columns else []
    excluded = set(existing_leading + final_cols + internal_cols)
    remaining = [col for col in merged.columns if col not in excluded]
    return merged[existing_leading + remaining + final_cols]


def write_post_run_analysis(
    combined_df: pd.DataFrame,
    output_dir: Path,
    *,
    plants_cfg: list[dict[str, Any]],
    predicted_rt_column: str,
    total_input_count: int,
    filtered_count: int,
    cutoffs: list[float],
) -> dict[str, str]:
    analysis_dir = ensure_directory(output_dir / "Post_run_analysis")
    figure_dir = ensure_directory(analysis_dir / "Figures")

    plant_specs = _plant_result_columns(plants_cfg)
    total_input_count = int(total_input_count)
    filtered_count = int(filtered_count)
    if "matched_fraction" in combined_df.columns:
        collected_mask = ~combined_df["matched_fraction"].astype(str).isin([
            "Fraction not collected",
            "No predicted RT",
            OUTSIDE_ANCHOR_LABEL,
        ])
    else:
        collected_mask = pd.Series(False, index=combined_df.index)

    present_masks = []
    for spec in plant_specs:
        if spec["present_col"] in combined_df.columns:
            present_masks.append(combined_df[spec["present_col"]].apply(_coerce_bool) == True)  # noqa: E712
    present_any_mask = pd.concat(present_masks, axis=1).any(axis=1) if present_masks else pd.Series(False, index=combined_df.index)
    collected_present_any_mask = collected_mask & present_any_mask

    high_group = make_group_labels([float(x) for x in sorted(cutoffs)])[-1]
    high_masks = []
    for spec in plant_specs:
        if spec["present_col"] in combined_df.columns and spec["group_col"] in combined_df.columns:
            present = combined_df[spec["present_col"]].apply(_coerce_bool) == True  # noqa: E712
            high = combined_df[spec["group_col"]].astype(str) == high_group
            high_masks.append(present & high)
    high_any_mask = pd.concat(high_masks, axis=1).any(axis=1) if high_masks else pd.Series(False, index=combined_df.index)
    collected_high_any_mask = collected_mask & high_any_mask

    funnel_steps = [
        ("Total input features", total_input_count),
        ("Above peak-area threshold", filtered_count),
        ("Assigned to collected fractions", int(collected_mask.sum())),
        ("Collected and present in at least one target plant", int(collected_present_any_mask.sum())),
        ("Collected and present in highest response group", int(collected_high_any_mask.sum())),
    ]

    overview_rows = []
    previous_value: int | None = None
    for metric, value in funnel_steps:
        overview_rows.append(
            {
                "metric": metric,
                "value": int(value),
                "percent_of_total_input": round(value / total_input_count * 100, 2) if total_input_count else 0,
                "percent_of_previous_step": round(value / previous_value * 100, 2) if previous_value else 100.0,
            }
        )
        previous_value = int(value)

    for spec in plant_specs:
        present = combined_df[spec["present_col"]].apply(_coerce_bool) if spec["present_col"] in combined_df.columns else pd.Series([], dtype=object)
        overview_rows.append(
            {
                "metric": f"{spec['slug']}_features_present",
                "value": int((present == True).sum()),  # noqa: E712
                "percent_of_total_input": round(int((present == True).sum()) / total_input_count * 100, 2) if total_input_count else 0,  # noqa: E712
                "percent_of_previous_step": "",
            }
        )
        overview_rows.append(
            {
                "metric": f"{spec['slug']}_features_absent",
                "value": int((present == False).sum()),  # noqa: E712
                "percent_of_total_input": round(int((present == False).sum()) / total_input_count * 100, 2) if total_input_count else 0,  # noqa: E712
                "percent_of_previous_step": "",
            }
        )
    overview_df = pd.DataFrame(overview_rows)
    overview_path = write_table(overview_df, analysis_dir / "06_run_overview.csv")

    plant_rows = []
    for spec in plant_specs:
        if spec["group_col"] not in combined_df.columns:
            continue
        work = combined_df[[spec["present_col"], spec["group_col"]]].copy()
        work["presence"] = work[spec["present_col"]].apply(lambda x: "present" if _coerce_bool(x) is True else "absent")
        work["bioactivity_group_clean"] = work[spec["group_col"]].apply(_clean_bioactivity_group)
        counts = work.groupby(["presence", "bioactivity_group_clean"], dropna=False).size().reset_index(name="feature_count")
        counts.insert(0, "plant", spec["name"])
        plant_rows.extend(counts.to_dict("records"))
    plant_summary_df = pd.DataFrame(plant_rows)
    plant_summary_path = write_table(plant_summary_df, analysis_dir / "07_plant_presence_activity_summary.csv")

    try:
        from PIL import Image, ImageDraw, ImageFont

        pastel = {
            "blue": "#9ecae1",
            "green": "#a1d99b",
            "pink": "#f4b6c2",
            "purple": "#c7b9ff",
            "yellow": "#fddc8a",
            "red": "#f2a7a7",
            "gray": "#d8dee9",
            "text": "#263238",
        }

        def _svg_escape(value: Any) -> str:
            return (
                str(value)
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
            )

        def _hex_to_rgb(value: str) -> tuple[int, int, int]:
            text = value.lstrip("#")
            return int(text[0:2], 16), int(text[2:4], 16), int(text[4:6], 16)

        def save_horizontal_bars(
            rows: list[tuple[str, int, str]],
            stem: str,
            title: str,
            *,
            width: int = 1180,
        ) -> None:
            rows = [(str(label), int(value), color) for label, value, color in rows if int(value) >= 0]
            if not rows:
                return
            row_h = 30
            left = 330
            right = 130
            top = 72
            bottom = 45
            height = max(260, top + bottom + row_h * len(rows))
            max_value = max([value for _, value, _ in rows] + [1])
            bar_w = max(120, width - left - right)

            image = Image.new("RGB", (width, height), "white")
            draw = ImageDraw.Draw(image)
            font_title = ImageFont.load_default()
            font = ImageFont.load_default()
            draw.text((24, 24), title, fill=_hex_to_rgb(pastel["text"]), font=font_title)
            for idx, (label, value, color) in enumerate(rows):
                y = top + idx * row_h
                draw.text((24, y + 5), label[:58], fill=_hex_to_rgb(pastel["text"]), font=font)
                length = int((value / max_value) * bar_w) if max_value else 0
                draw.rectangle([left, y + 3, left + length, y + row_h - 6], fill=_hex_to_rgb(color))
                draw.text((left + length + 8, y + 5), f"{value:,}", fill=_hex_to_rgb(pastel["text"]), font=font)
            image.save(figure_dir / f"{stem}.png")

            svg_rows = [
                f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
                '<rect width="100%" height="100%" fill="white"/>',
                f'<text x="24" y="34" font-family="Arial, sans-serif" font-size="20" font-weight="700" fill="{pastel["text"]}">{_svg_escape(title)}</text>',
            ]
            for idx, (label, value, color) in enumerate(rows):
                y = top + idx * row_h
                length = int((value / max_value) * bar_w) if max_value else 0
                svg_rows.append(f'<text x="24" y="{y + 18}" font-family="Arial, sans-serif" font-size="12" fill="{pastel["text"]}">{_svg_escape(label[:58])}</text>')
                svg_rows.append(f'<rect x="{left}" y="{y + 3}" width="{length}" height="{row_h - 9}" rx="3" fill="{color}"/>')
                svg_rows.append(f'<text x="{left + length + 8}" y="{y + 18}" font-family="Arial, sans-serif" font-size="12" fill="{pastel["text"]}">{value:,}</text>')
            svg_rows.append("</svg>")
            (figure_dir / f"{stem}.svg").write_text("\n".join(svg_rows), encoding="utf-8")

        funnel_rows = []
        funnel_colors = [pastel["blue"], pastel["green"], pastel["yellow"], pastel["purple"], pastel["pink"]]
        for idx, (label, value) in enumerate(funnel_steps):
            pct_total = value / total_input_count * 100 if total_input_count else 0
            funnel_rows.append((f"{label} ({pct_total:.1f}%)", int(value), funnel_colors[idx % len(funnel_colors)]))
        save_horizontal_bars(funnel_rows, "01_feature_prioritization_funnel", "Feature prioritization funnel")

        if "parsed_fraction_numbers" in combined_df.columns:
            exploded = []
            for value in combined_df["parsed_fraction_numbers"]:
                exploded.extend(_parse_fraction_numbers(value))
            if exploded:
                fraction_counts = pd.Series(exploded).value_counts().sort_index()
                fraction_rows = [(f"Fraction {idx}", int(value), pastel["blue"]) for idx, value in fraction_counts.items()]
                save_horizontal_bars(fraction_rows, "02_feature_counts_per_fraction", "Features assigned to candidate/predicted fractions", width=1040)

        if plant_specs:
            presence_rows = []
            high_rows = []
            for spec in plant_specs:
                present = combined_df[spec["present_col"]].apply(_coerce_bool) if spec["present_col"] in combined_df.columns else pd.Series([], dtype=object)
                presence_rows.append(
                    {
                        "plant": spec["name"],
                        "present": int((present == True).sum()),  # noqa: E712
                        "absent": int((present == False).sum()),  # noqa: E712
                    }
                )
                if spec["present_col"] in combined_df.columns and spec["group_col"] in combined_df.columns:
                    high_count = int(((present == True) & (combined_df[spec["group_col"]].astype(str) == high_group)).sum())  # noqa: E712
                else:
                    high_count = 0
                high_rows.append({"plant": spec["name"], "highest_activity": high_count})

            overview_bars: list[tuple[str, int, str]] = []
            for row in presence_rows:
                overview_bars.append((f"{row['plant']} present", int(row["present"]), pastel["green"]))
                overview_bars.append((f"{row['plant']} absent", int(row["absent"]), pastel["red"]))
            for row in high_rows:
                overview_bars.append((f"{row['plant']} highest response", int(row["highest_activity"]), pastel["pink"]))
            save_horizontal_bars(overview_bars, "03_plant_presence_and_high_activity", "Target plant presence and highest response")

        for spec in plant_specs:
            subset = plant_summary_df[plant_summary_df["plant"] == spec["name"]] if not plant_summary_df.empty else pd.DataFrame()
            if subset.empty:
                continue
            group_rows = []
            for _, row in subset.iterrows():
                color = pastel["green"] if row.get("presence") == "present" else pastel["red"]
                group_rows.append((f"{row.get('bioactivity_group_clean')} | {row.get('presence')}", int(row.get("feature_count", 0)), color))
            save_horizontal_bars(group_rows, f"04_activity_presence_{spec['slug']}", f"{spec['name']}: features by response group", width=1040)
    except Exception as exc:
        LOGGER.warning("Post-run chart creation skipped: %s", exc)

    return {
        "run_overview": str(overview_path),
        "plant_presence_activity_summary": str(plant_summary_path),
        "figures_dir": str(figure_dir),
    }


# ---------------------------
# Pipeline
# ---------------------------


def load_calibration_model_from_config(config: dict[str, Any], *, base_dir: str | Path) -> CalibrationModel:
    calibration_cfg = config["calibration"]
    method = normalize_calibration_method(calibration_cfg.get("method", "pairs"))

    if method == "pairs":
        pairs_path = resolve_path(base_dir, calibration_cfg["pairs_file"])
        pairs_df = read_table(pairs_path)
        return fit_linear_calibration_from_pairs(
            pairs_df,
            x_col=calibration_cfg["uplc_rt_column"],
            y_col=calibration_cfg["hplc_rt_column"],
            minimum_points=int(calibration_cfg.get("minimum_points", 2)),
            recommended_points=int(calibration_cfg.get("recommended_points", 5)),
            source=str(pairs_path),
        )

    if method == "equation":
        return calibration_from_equation(
            slope=float(calibration_cfg["slope"]),
            intercept=float(calibration_cfg.get("intercept", 0.0)),
        )

    if method == "runtime_scale":
        return calibration_from_runtime_scaling(
            uplc_total_runtime=float(calibration_cfg["uplc_total_runtime"]),
            hplc_total_runtime=float(calibration_cfg["hplc_total_runtime"]),
        )

    raise ValueError(
        f"Unknown calibration method '{method}'. Use 'pairs', 'equation', 'runtime_scale', or 'feature_order_alignment'."
    )



def run_pipeline(config: dict[str, Any]) -> dict[str, Any]:
    base_dir = Path(config.get("base_dir", Path(__file__).resolve().parent))
    output_value = Path(config.get("output_dir", "Outputs"))
    output_dir = ensure_directory(output_value if output_value.is_absolute() else base_dir / output_value)

    feature_cfg = config["feature_table"]
    feature_path = resolve_path(base_dir, feature_cfg["path"])
    feature_df = read_table(feature_path)

    filtered_df, resolved_id_column, generated_id = filter_feature_table(
        feature_df,
        sample_columns=list(feature_cfg["sample_columns"]),
        mz_column=feature_cfg.get("mz_column", "row m/z"),
        rt_column=feature_cfg.get("rt_column", "row retention time"),
        id_column=feature_cfg.get("id_column") or None,
        area_threshold=float(feature_cfg.get("area_threshold", 10000)),
    )

    fractions_cfg = config["fractions"]
    fraction_df = make_fraction_table(
        start_time=float(fractions_cfg.get("start_time", 2.0)),
        end_time=float(fractions_cfg.get("end_time", 38.0)),
        n_fractions=int(fractions_cfg.get("n_fractions", 96)),
        first_fraction_number=int(fractions_cfg.get("first_fraction_number", 1)),
    )

    calibration_cfg = config.get("calibration", {}) or {}
    calibration_method = normalize_calibration_method(calibration_cfg.get("method", "pairs"))
    predicted_rt_column = config.get("predicted_rt_column", "predicted_hplc_rt")
    if calibration_method == "feature_order_alignment":
        landmarks_df = load_feature_order_landmarks(config, base_dir=base_dir)
        predicted_df = apply_feature_order_alignment(
            filtered_df,
            rt_column=feature_cfg.get("rt_column", "row retention time"),
            landmarks_df=landmarks_df,
            predicted_rt_column=predicted_rt_column,
            near_anchor_rt_tolerance=float(calibration_cfg.get("near_anchor_rt_tolerance", 0.02)),
        )
        calibration_summary: dict[str, Any] = {
            "method": "feature_order_alignment",
            "source": str(calibration_cfg.get("landmarks_file", "config landmarks") or "config landmarks"),
            "n_points": int(len(landmarks_df)),
            "near_anchor_rt_tolerance": float(calibration_cfg.get("near_anchor_rt_tolerance", 0.02)),
            "landmarks": landmarks_df.to_dict(orient="records"),
        }
    else:
        calibration = load_calibration_model_from_config(config, base_dir=base_dir)
        predicted_df = apply_rt_calibration(
            filtered_df,
            rt_column=feature_cfg.get("rt_column", "row retention time"),
            calibration=calibration,
            output_column=predicted_rt_column,
        )
        predicted_df = match_features_to_fractions(
            predicted_df,
            fraction_df,
            time_column=predicted_rt_column,
        )
        calibration_summary = calibration.to_dict()

    plants_cfg = config.get("plants", [])
    if not plants_cfg:
        raise ValueError("Config must contain at least one plant entry in 'plants'.")

    plant_outputs: list[dict[str, Any]] = []
    combined_df = predicted_df.copy()

    for plant_cfg in plants_cfg:
        plant_name = plant_cfg["name"]
        grouping_metric = grouping_metric_from_config(config)
        grouping_column = grouping_value_column(grouping_metric)
        fluoro_df, fluoro_columns = fluorescence_table_from_config(plant_cfg, base_dir)
        fluoro_prepared = prepare_fluorescence_table(
            fluoro_df,
            fraction_column=fluoro_columns["fraction_column"],
            average_column=fluoro_columns["average_column"],
            positive_control_column=fluoro_columns["positive_control_column"],
        )
        fluoro_grouped = assign_bioactivity_groups(
            fluoro_prepared,
            bioactivity_column=grouping_column,
            cutoffs=[float(x) for x in config.get("bioactivity", {}).get("cutoffs", [16.5, 22.5])],
        )
        fluoro_grouped["grouping_value_column"] = grouping_column
        fluoro_grouped["grouping_value_label"] = grouping_metric_label(grouping_metric)

        combined_df = map_fraction_groups_to_features(
            combined_df,
            fluoro_grouped,
            parsed_fractions_column="parsed_fraction_numbers",
            fraction_column=fluoro_columns["fraction_column"],
            bioactivity_column=grouping_column,
            group_column="bioactivity_group",
            plant_name=plant_name,
        )

        plant_slug = slugify(plant_name)
        sample_column = plant_cfg.get("sample_column")
        if sample_column:
            require_columns(combined_df, [sample_column], f"feature table for plant '{plant_name}'")
            combined_df[f"present_in_{plant_slug}"] = (
                pd.to_numeric(combined_df[sample_column], errors="coerce").fillna(0) > 0
            )
        plant_output_path = output_dir / f"{plant_slug}_bioactivity_by_fraction.csv"
        write_table(fluoro_grouped, plant_output_path)
        plant_outputs.append(
            {
                "name": plant_name,
                "slug": plant_slug,
                "file": str(plant_output_path),
            }
        )

    filtered_path = write_table(filtered_df, output_dir / "01_filtered_feature_table.csv")
    fraction_path = write_table(fraction_df, output_dir / "02_fraction_windows.csv")
    predicted_path = write_table(predicted_df, output_dir / "03_features_with_fraction_predictions.csv")
    combined_path = write_table(combined_df, output_dir / "04_features_with_bioactivity.csv")

    extra_outputs: dict[str, Any] = {}
    append_cfg = config.get("append_to_feature_table")
    if append_cfg:
        append_path = resolve_path(base_dir, append_cfg["path"])
        append_df = read_table(append_path)
        annotation_match_column = (
            append_cfg.get("annotation_match_column")
            or append_cfg.get("id_column")
            or resolved_id_column
        )
        feature_match_column = (
            append_cfg.get("feature_match_column")
            or append_cfg.get("feature_id_column")
            or resolved_id_column
        )
        selected_annotation_cols = [
            str(col).strip()
            for col in append_cfg.get("columns_to_add", append_cfg.get("annotation_columns", [])) or []
            if str(col).strip()
        ]
        if selected_annotation_cols:
            keep_cols = [annotation_match_column]
            for col in selected_annotation_cols:
                if col not in keep_cols:
                    keep_cols.append(col)
            require_columns(append_df, keep_cols, "annotation table for final report")
            append_df = append_df[keep_cols].copy()
        plant_result_cols = []
        for spec in _plant_result_columns(plants_cfg):
            plant_result_cols.extend([spec["present_col"], spec["group_col"], spec["value_col"]])
        merge_columns = [
            predicted_rt_column,
            "matched_fraction",
            "parsed_fraction_numbers",
            "alignment_mode",
            "alignment_status",
            "left_anchor_id",
            "right_anchor_id",
            "candidate_fraction_start",
            "candidate_fraction_end",
            "candidate_fraction_count",
            *plant_result_cols,
        ]
        merge_columns = [col for col in merge_columns if col in combined_df.columns]
        merged_df = merge_human_readable_report_by_id(
            append_df,
            combined_df,
            id_column=append_cfg.get("id_column"),
            base_id_column=annotation_match_column,
            annotation_id_column=feature_match_column,
            columns_to_add=merge_columns,
            plants_cfg=plants_cfg,
            predicted_rt_column=predicted_rt_column,
        )
        merged_path = write_table(merged_df, output_dir / "05_appended_feature_table_with_bioactivity.csv")
        extra_outputs["human_readable_feature_report"] = str(merged_path)
        extra_outputs["appended_feature_table"] = str(merged_path)

    analysis_outputs = write_post_run_analysis(
        combined_df,
        output_dir,
        plants_cfg=plants_cfg,
        predicted_rt_column=predicted_rt_column,
        total_input_count=int(len(feature_df)),
        filtered_count=int(len(filtered_df)),
        cutoffs=[float(x) for x in config.get("bioactivity", {}).get("cutoffs", [16.5, 22.5])],
    )
    extra_outputs.update(analysis_outputs)

    summary = {
        "input_feature_table": str(feature_path),
        "resolved_id_column": resolved_id_column,
        "generated_id_column": generated_id,
        "calibration": calibration_summary,
        "n_rows_input": int(len(feature_df)),
        "n_rows_filtered": int(len(filtered_df)),
        "output_dir": str(output_dir),
        "files": {
            "filtered_feature_table": str(filtered_path),
            "fraction_windows": str(fraction_path),
            "features_with_fraction_predictions": str(predicted_path),
            "features_with_bioactivity": str(combined_path),
            **extra_outputs,
        },
        "plants": plant_outputs,
    }

    with open(output_dir / "run_summary.json", "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)

    return summary


# ---------------------------
# CLI
# ---------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Predict HPLC fractions and map fluorescence-derived bioactivity to LC-MS features. "
            "Use a JSON config file so the same core can be reused from CLI, GUI, or Jupyter."
        )
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to JSON config file.",
    )
    parser.add_argument(
        "--base-dir",
        default=None,
        help="Optional base directory for relative paths in the config. Defaults to the config file folder.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level.",
    )
    return parser


def load_config(path: str | Path, *, base_dir: str | Path | None = None) -> dict[str, Any]:
    config_path = Path(path).resolve()
    with open(config_path, "r", encoding="utf-8-sig") as handle:
        config = json.load(handle)
    config["base_dir"] = str(base_dir or config_path.parent)
    return config



def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(levelname)s: %(message)s",
    )

    config = load_config(args.config, base_dir=args.base_dir)

    summary = run_pipeline(config)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
