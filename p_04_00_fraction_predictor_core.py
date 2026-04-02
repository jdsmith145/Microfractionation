from __future__ import annotations

import argparse
import json
import logging
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd


LOGGER = logging.getLogger("fraction_core")


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
    slope, intercept = np.polyfit(x, y, deg=1)
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



def make_group_labels(cutoffs: list[float]) -> list[str]:
    if not cutoffs:
        return ["group_all"]
    values = [float(c) for c in sorted(cutoffs)]
    labels: list[str] = [f"group_below_{values[0]:g}"]
    for low, high in zip(values[:-1], values[1:]):
        labels.append(f"group_{low:g}_to_{high:g}")
    labels.append(f"group_above_{values[-1]:g}")
    return labels



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


# ---------------------------
# Pipeline
# ---------------------------


def load_calibration_model_from_config(config: dict[str, Any], *, base_dir: str | Path) -> CalibrationModel:
    calibration_cfg = config["calibration"]
    method = str(calibration_cfg.get("method", "pairs")).lower()

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

    if method in {"runtime_scale", "runtime-scaling", "scale"}:
        return calibration_from_runtime_scaling(
            uplc_total_runtime=float(calibration_cfg["uplc_total_runtime"]),
            hplc_total_runtime=float(calibration_cfg["hplc_total_runtime"]),
        )

    raise ValueError(
        f"Unknown calibration method '{method}'. Use 'pairs', 'equation', or 'runtime_scale'."
    )



def run_pipeline(config: dict[str, Any]) -> dict[str, Any]:
    base_dir = Path(config.get("base_dir", Path(__file__).resolve().parent))
    output_dir = ensure_directory(base_dir / config.get("output_dir", "Outputs"))

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

    calibration = load_calibration_model_from_config(config, base_dir=base_dir)
    predicted_df = apply_rt_calibration(
        filtered_df,
        rt_column=feature_cfg.get("rt_column", "row retention time"),
        calibration=calibration,
        output_column=config.get("predicted_rt_column", "predicted_hplc_rt"),
    )

    fractions_cfg = config["fractions"]
    fraction_df = make_fraction_table(
        start_time=float(fractions_cfg.get("start_time", 2.0)),
        end_time=float(fractions_cfg.get("end_time", 38.0)),
        n_fractions=int(fractions_cfg.get("n_fractions", 96)),
        first_fraction_number=int(fractions_cfg.get("first_fraction_number", 1)),
    )

    predicted_df = match_features_to_fractions(
        predicted_df,
        fraction_df,
        time_column=config.get("predicted_rt_column", "predicted_hplc_rt"),
    )

    plants_cfg = config.get("plants", [])
    if not plants_cfg:
        raise ValueError("Config must contain at least one plant entry in 'plants'.")

    plant_outputs: list[dict[str, Any]] = []
    combined_df = predicted_df.copy()

    for plant_cfg in plants_cfg:
        plant_name = plant_cfg["name"]
        fluoro_path = resolve_path(base_dir, plant_cfg["fluorescence_file"])
        fluoro_df = read_table(fluoro_path)
        fluoro_prepared = prepare_fluorescence_table(
            fluoro_df,
            fraction_column=plant_cfg.get("fluorescence_fraction_column", "fraction"),
            average_column=plant_cfg.get("fluorescence_average_column", "average"),
            positive_control_column=plant_cfg.get("fluorescence_positive_control_column", "pos_avg"),
        )
        fluoro_grouped = assign_bioactivity_groups(
            fluoro_prepared,
            bioactivity_column="bioactivity",
            cutoffs=[float(x) for x in config.get("bioactivity", {}).get("cutoffs", [16.5, 22.5])],
        )

        combined_df = map_fraction_groups_to_features(
            combined_df,
            fluoro_grouped,
            parsed_fractions_column="parsed_fraction_numbers",
            fraction_column=plant_cfg.get("fluorescence_fraction_column", "fraction"),
            bioactivity_column="bioactivity",
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
        bioactivity_columns = [
            c for c in combined_df.columns if c.startswith("bioactivity_group_") or c.startswith("bioactivity_value_")
        ]
        merge_columns = [
            config.get("predicted_rt_column", "predicted_hplc_rt"),
            "matched_fraction",
            "parsed_fraction_numbers",
            *bioactivity_columns,
        ]
        merged_df = append_columns_by_id(
            append_df,
            combined_df,
            id_column=append_cfg.get("id_column", resolved_id_column),
            columns_to_add=merge_columns,
            missing_fill_text=append_cfg.get("missing_fill_text", "Not present in target plants"),
            status_column=append_cfg.get("status_column", "target_plant_status"),
        )
        merged_path = write_table(merged_df, output_dir / "05_appended_feature_table_with_bioactivity.csv")
        extra_outputs["appended_feature_table"] = str(merged_path)

    summary = {
        "input_feature_table": str(feature_path),
        "resolved_id_column": resolved_id_column,
        "generated_id_column": generated_id,
        "calibration": calibration.to_dict(),
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



def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(levelname)s: %(message)s",
    )

    config_path = Path(args.config).resolve()
    with open(config_path, "r", encoding="utf-8") as handle:
        config = json.load(handle)

    if args.base_dir is not None:
        config["base_dir"] = args.base_dir
    else:
        config.setdefault("base_dir", str(config_path.parent))

    summary = run_pipeline(config)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
