#!/usr/bin/env python3
"""Utilities for importing direct well-plate reader output.

The functions in this module convert raw 24/96/384-well plate exports into the
long and summary tables used by the publication workflow. They intentionally do
not edit the original plate-reader files.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd


class PlateReaderError(ValueError):
    """Raised when a plate-reader file cannot be parsed safely."""


@dataclass(frozen=True)
class PlateGeometry:
    """Physical plate dimensions in row-major biological order."""

    rows: int = 8
    columns: int = 12

    def __post_init__(self) -> None:
        if self.rows <= 0 or self.columns <= 0:
            raise PlateReaderError("Plate rows and columns must be positive integers.")
        if self.rows > 26:
            raise PlateReaderError("Only row labels A-Z are currently supported.")

    @property
    def row_labels(self) -> list[str]:
        return [chr(ord("A") + index) for index in range(self.rows)]

    @property
    def well_count(self) -> int:
        return self.rows * self.columns


@dataclass(frozen=True)
class DetectedPlateBlock:
    """Location of a detected numeric plate block inside a raw sheet."""

    sheet_name: str
    start_row: int
    start_column: int
    rows: int
    columns: int
    transposed: bool
    numeric_ratio: float
    score: float


def split_list(value: str | Sequence[str] | None) -> list[str]:
    """Parse comma/semicolon/newline separated text into non-empty strings."""

    if value is None:
        return []
    if isinstance(value, str):
        text = value.replace(";", ",").replace("\n", ",")
        return [part.strip() for part in text.split(",") if part.strip()]
    return [str(part).strip() for part in value if str(part).strip()]


def normalize_well_name(value: Any) -> str:
    """Return a normalized well id such as ``A1`` or raise on invalid input."""

    text = str(value).strip().upper().replace(" ", "")
    if not text:
        raise PlateReaderError("Empty well name.")
    row = "".join(ch for ch in text if ch.isalpha())
    column = "".join(ch for ch in text if ch.isdigit())
    if len(row) != 1 or not column:
        raise PlateReaderError(f"Invalid well name: {value!r}. Use labels such as A1 or H12.")
    return f"{row}{int(column)}"


def parse_well_list(value: str | Sequence[str] | None) -> list[str]:
    return [normalize_well_name(item) for item in split_list(value)]


def parse_control_wells_by_plate(value: str | dict[str, Any] | Sequence[Any] | None) -> dict[str, list[str]] | Sequence[Any] | None:
    """Parse optional per-file/per-replicate control wells.

    Text format uses semicolon-separated entries such as
    ``1:H11,H12; plate_2.xlsx:H10,H11``. Keys may be replicate numbers, file
    names, file stems, or full file paths.
    """

    if value is None or isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return None
    parsed: dict[str, list[str]] = {}
    for entry in [part.strip() for part in text.split(";") if part.strip()]:
        if ":" in entry:
            key, wells = entry.split(":", 1)
        elif "=" in entry:
            key, wells = entry.split("=", 1)
        else:
            raise PlateReaderError(
                "File-specific controls must use entries like '1:H11,H12' or 'plate_2.xlsx:H10,H11'."
            )
        parsed[key.strip()] = parse_well_list(wells)
    return parsed


def _coerce_numeric_frame(df: pd.DataFrame) -> pd.DataFrame:
    text = df.astype("string").replace({"": pd.NA})
    text = text.apply(lambda col: col.str.replace(",", ".", regex=False))
    return text.apply(pd.to_numeric, errors="coerce")


def _label_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.upper()


def _label_bonus(raw: pd.DataFrame, start_row: int, start_col: int, geometry: PlateGeometry, *, transposed: bool) -> float:
    if transposed:
        expected_row_labels = [str(i) for i in range(1, geometry.columns + 1)]
        expected_col_labels = geometry.row_labels
    else:
        expected_row_labels = geometry.row_labels
        expected_col_labels = [str(i) for i in range(1, geometry.columns + 1)]

    bonus = 0.0
    if start_col > 0:
        labels = [_label_text(raw.iat[start_row + i, start_col - 1]) for i in range(len(expected_row_labels))]
        matches = sum(1 for observed, expected in zip(labels, expected_row_labels) if observed == expected)
        bonus += 25.0 * matches / max(len(expected_row_labels), 1)
    if start_row > 0:
        labels = [_label_text(raw.iat[start_row - 1, start_col + i]) for i in range(len(expected_col_labels))]
        matches = sum(1 for observed, expected in zip(labels, expected_col_labels) if observed == expected)
        bonus += 25.0 * matches / max(len(expected_col_labels), 1)
    return bonus


def _iter_candidate_shapes(geometry: PlateGeometry, orientation: str) -> Iterable[tuple[int, int, bool]]:
    mode = (orientation or "auto").strip().lower()
    if mode not in {"auto", "standard", "transposed"}:
        raise PlateReaderError("Plate orientation must be 'auto', 'standard', or 'transposed'.")
    if mode in {"auto", "standard"}:
        yield geometry.rows, geometry.columns, False
    if mode in {"auto", "transposed"}:
        yield geometry.columns, geometry.rows, True


def read_raw_sheets(path: str | Path, *, sheet_name: str | int | None = None) -> dict[str, pd.DataFrame]:
    """Read one or all sheets from a plate-reader table with no inferred header."""

    path = Path(path).expanduser()
    if not path.exists():
        raise PlateReaderError(f"Plate-reader file not found: {path}")
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return {path.stem: pd.read_csv(path, header=None)}
    if suffix in {".tsv", ".txt"}:
        return {path.stem: pd.read_csv(path, sep="\t", header=None)}
    if suffix in {".xlsx", ".xls"}:
        if sheet_name not in {None, ""}:
            df = pd.read_excel(path, sheet_name=sheet_name, header=None)
            return {str(sheet_name): df}
        sheets = pd.read_excel(path, sheet_name=None, header=None)
        return {str(name): df for name, df in sheets.items()}
    raise PlateReaderError(f"Unsupported plate-reader file type: {path.suffix}. Use CSV, TSV, TXT, XLSX, or XLS.")


def detect_plate_block(
    path: str | Path,
    *,
    rows: int = 8,
    columns: int = 12,
    sheet_name: str | int | None = None,
    orientation: str = "auto",
    min_numeric_ratio: float = 0.75,
) -> tuple[pd.DataFrame, DetectedPlateBlock]:
    """Find the best numeric plate block in a raw plate-reader export."""

    geometry = PlateGeometry(rows=rows, columns=columns)
    best: tuple[float, pd.DataFrame, DetectedPlateBlock] | None = None

    for sheet, raw in read_raw_sheets(path, sheet_name=sheet_name).items():
        if raw.empty:
            continue
        for block_rows, block_cols, transposed in _iter_candidate_shapes(geometry, orientation):
            if raw.shape[0] < block_rows or raw.shape[1] < block_cols:
                continue
            for start_row in range(0, raw.shape[0] - block_rows + 1):
                for start_col in range(0, raw.shape[1] - block_cols + 1):
                    block_raw = raw.iloc[start_row : start_row + block_rows, start_col : start_col + block_cols]
                    block_num = _coerce_numeric_frame(block_raw)
                    numeric_ratio = float(block_num.notna().to_numpy().mean())
                    if numeric_ratio < min_numeric_ratio:
                        continue
                    score = numeric_ratio * 100.0 + _label_bonus(raw, start_row, start_col, geometry, transposed=transposed)
                    detected = DetectedPlateBlock(
                        sheet_name=sheet,
                        start_row=start_row,
                        start_column=start_col,
                        rows=block_rows,
                        columns=block_cols,
                        transposed=transposed,
                        numeric_ratio=numeric_ratio,
                        score=score,
                    )
                    if best is None or score > best[0]:
                        best = (score, block_num, detected)

    if best is None:
        raise PlateReaderError(
            f"Could not find a {rows} x {columns} numeric plate block in {Path(path).name}. "
            "Check the selected plate dimensions or export the plate as a simple table."
        )
    _, matrix, detected = best
    return matrix.reset_index(drop=True), detected


def plate_file_to_long_table(
    path: str | Path,
    *,
    plate_index: int = 1,
    rows: int = 8,
    columns: int = 12,
    sheet_name: str | int | None = None,
    orientation: str = "auto",
) -> tuple[pd.DataFrame, DetectedPlateBlock]:
    """Convert one plate-reader file into one row per well."""

    geometry = PlateGeometry(rows=rows, columns=columns)
    matrix, detected = detect_plate_block(
        path,
        rows=rows,
        columns=columns,
        sheet_name=sheet_name,
        orientation=orientation,
    )
    values = matrix.to_numpy(dtype=float)
    if detected.transposed:
        values = values.T

    rows_out: list[dict[str, Any]] = []
    for row_idx, row_label in enumerate(geometry.row_labels, start=1):
        for column_idx in range(1, geometry.columns + 1):
            well = f"{row_label}{column_idx}"
            rows_out.append(
                {
                    "well": well,
                    "plate_row": row_label,
                    "plate_column": column_idx,
                    "row_index": row_idx,
                    "column_index": column_idx,
                    "row_major_index": (row_idx - 1) * geometry.columns + column_idx,
                    "raw_signal": float(values[row_idx - 1, column_idx - 1]),
                    "plate_index": plate_index,
                    "source_file": str(Path(path).expanduser()),
                    "source_name": Path(path).name,
                    "sheet_name": detected.sheet_name,
                }
            )
    return pd.DataFrame(rows_out), detected


def _control_wells_for_plate(
    plate_index: int,
    path: str | Path,
    default_control_wells: Sequence[str],
    control_wells_by_plate: dict[str, Any] | Sequence[Any] | None,
) -> list[str]:
    control_wells_by_plate = parse_control_wells_by_plate(control_wells_by_plate)
    if control_wells_by_plate:
        if isinstance(control_wells_by_plate, dict):
            keys = [
                str(plate_index),
                Path(path).name,
                Path(path).stem,
                str(Path(path).expanduser()),
            ]
            for key in keys:
                if key in control_wells_by_plate:
                    return parse_well_list(control_wells_by_plate[key])
        else:
            values = list(control_wells_by_plate)
            if 0 <= plate_index - 1 < len(values):
                return parse_well_list(values[plate_index - 1])
    return list(default_control_wells)


def _scaled_plate_values(
    df: pd.DataFrame,
    *,
    control_wells: Sequence[str],
    scale_mode: str,
    excluded_wells: Sequence[str] | None = None,
) -> pd.DataFrame:
    mode = (scale_mode or "positive_control_then_minmax_0_100").strip().lower()
    out = df.copy()
    controls = {normalize_well_name(well) for well in control_wells}
    excluded = {normalize_well_name(well) for well in (excluded_wells or [])}
    out["is_positive_control"] = out["well"].isin(controls)
    out["is_excluded_well"] = out["well"].isin(excluded)

    if controls and not out["is_positive_control"].any():
        raise PlateReaderError(f"None of the selected positive-control wells were found: {sorted(controls)}")

    control_mean = np.nan
    if out["is_positive_control"].any():
        control_mean = float(pd.to_numeric(out.loc[out["is_positive_control"], "raw_signal"], errors="coerce").mean())
        if not np.isfinite(control_mean) or control_mean == 0:
            raise PlateReaderError("Positive-control wells have no usable non-zero numeric mean.")
    out["positive_control_mean"] = control_mean

    if mode in {"none", "raw"}:
        out["normalized_signal"] = out["raw_signal"].astype(float)
        out["scaled_signal_0_100"] = out["normalized_signal"]
        return out

    if mode in {"positive_control_pct", "control_pct", "positive_control"}:
        if not np.isfinite(control_mean) or control_mean == 0:
            raise PlateReaderError("Positive-control scaling requires at least one positive-control well.")
        out["normalized_signal"] = out["raw_signal"].astype(float) / control_mean * 100.0
        out["scaled_signal_0_100"] = out["normalized_signal"]
        return out

    if mode in {"minmax_0_100", "positive_control_then_minmax_0_100", "control_then_minmax_0_100"}:
        if mode == "minmax_0_100":
            base = out["raw_signal"].astype(float)
        else:
            if not np.isfinite(control_mean) or control_mean == 0:
                raise PlateReaderError("Positive-control scaling requires at least one positive-control well.")
            base = out["raw_signal"].astype(float) / control_mean * 100.0
        out["normalized_signal"] = base
        sample_mask = ~(out["is_positive_control"] | out["is_excluded_well"])
        sample_values = pd.to_numeric(base.loc[sample_mask], errors="coerce").dropna()
        if sample_values.empty:
            raise PlateReaderError("No non-control numeric wells are available for 0-100 scaling.")
        min_value = float(sample_values.min())
        max_value = float(sample_values.max())
        if max_value == min_value:
            out["scaled_signal_0_100"] = 0.0
        else:
            out["scaled_signal_0_100"] = (base - min_value) / (max_value - min_value) * 100.0
        out["scale_min"] = min_value
        out["scale_max"] = max_value
        return out

    raise PlateReaderError(
        "Unknown scale mode. Use none, positive_control_pct, minmax_0_100, "
        "or positive_control_then_minmax_0_100."
    )


def read_plate_replicates(
    plate_files: Sequence[str | Path],
    *,
    rows: int = 8,
    columns: int = 12,
    sheet_name: str | int | None = None,
    orientation: str = "auto",
    control_wells: str | Sequence[str] | None = None,
    control_wells_by_plate: dict[str, Any] | Sequence[Any] | None = None,
    scale_mode: str = "positive_control_then_minmax_0_100",
    excluded_wells: str | Sequence[str] | None = None,
) -> pd.DataFrame:
    """Read and scale one or more replicate plate files."""

    files = [Path(path).expanduser() for path in plate_files]
    if not files:
        raise PlateReaderError("Choose at least one plate-reader file.")
    default_controls = parse_well_list(control_wells)
    default_excluded = parse_well_list(excluded_wells)
    plate_tables: list[pd.DataFrame] = []
    for index, file_path in enumerate(files, start=1):
        if not file_path.exists():
            raise PlateReaderError(f"Plate-reader file not found: {file_path}")
        long_df, _detected = plate_file_to_long_table(
            file_path,
            plate_index=index,
            rows=rows,
            columns=columns,
            sheet_name=sheet_name,
            orientation=orientation,
        )
        plate_controls = _control_wells_for_plate(index, file_path, default_controls, control_wells_by_plate)
        scaled = _scaled_plate_values(long_df, control_wells=plate_controls, scale_mode=scale_mode, excluded_wells=default_excluded)
        scaled["control_wells"] = ", ".join(plate_controls)
        plate_tables.append(scaled)
    return pd.concat(plate_tables, ignore_index=True)


def _wide_replicate_table(
    long_df: pd.DataFrame,
    *,
    index_columns: Sequence[str],
    value_column: str = "scaled_signal_0_100",
) -> pd.DataFrame:
    work = long_df.copy()
    work["replicate"] = "replicate_" + work["plate_index"].astype(int).astype(str)
    grouped = work.groupby([*index_columns, "replicate"], as_index=False)[value_column].mean()
    wide = grouped.pivot_table(index=list(index_columns), columns="replicate", values=value_column, aggfunc="mean")
    wide = wide.reset_index()
    wide.columns.name = None
    replicate_cols = sorted([col for col in wide.columns if str(col).startswith("replicate_")], key=lambda x: int(str(x).split("_")[-1]))
    wide["average"] = wide[replicate_cols].mean(axis=1, skipna=True)
    wide["n_replicates_used"] = wide[replicate_cols].notna().sum(axis=1)
    return wide


def build_fraction_activity_table(
    plate_files: Sequence[str | Path],
    *,
    rows: int = 8,
    columns: int = 12,
    sheet_name: str | int | None = None,
    orientation: str = "auto",
    control_wells: str | Sequence[str] | None = None,
    control_wells_by_plate: dict[str, Any] | Sequence[Any] | None = None,
    scale_mode: str = "positive_control_then_minmax_0_100",
    excluded_wells: str | Sequence[str] | None = None,
    exclude_control_wells: bool = True,
) -> pd.DataFrame:
    """Build a fraction table where A1=1, A2=2, ..., H12=96 by default."""

    long_df = read_plate_replicates(
        plate_files,
        rows=rows,
        columns=columns,
        sheet_name=sheet_name,
        orientation=orientation,
        control_wells=control_wells,
        control_wells_by_plate=control_wells_by_plate,
        scale_mode=scale_mode,
        excluded_wells=excluded_wells,
    )
    if exclude_control_wells:
        remove_mask = long_df["is_positive_control"]
        if "is_excluded_well" in long_df.columns:
            remove_mask = remove_mask | long_df["is_excluded_well"]
        long_df = long_df.loc[~remove_mask].copy()
    wide = _wide_replicate_table(
        long_df,
        index_columns=["row_major_index", "well", "plate_row", "plate_column"],
        value_column="scaled_signal_0_100",
    )
    wide = wide.rename(columns={"row_major_index": "fraction"})
    wide["positive_control_avg"] = 100.0
    ordered = ["fraction", "well", "plate_row", "plate_column"]
    replicate_cols = [col for col in wide.columns if str(col).startswith("replicate_")]
    ordered.extend(replicate_cols)
    ordered.extend(["average", "positive_control_avg", "n_replicates_used"])
    return wide[ordered].sort_values("fraction").reset_index(drop=True)


def read_well_mapping(
    mapping_file: str | Path,
    *,
    well_column: str = "well",
    sample_column: str | None = None,
) -> pd.DataFrame:
    """Read a user well-to-sample mapping file for plant/extract plates."""

    path = Path(mapping_file).expanduser()
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path)
    elif suffix in {".tsv", ".txt"}:
        df = pd.read_csv(path, sep="\t")
    elif suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    else:
        raise PlateReaderError(f"Unsupported mapping file type: {path.suffix}")

    if well_column not in df.columns:
        raise PlateReaderError(f"Mapping file must contain a well column named '{well_column}'.")
    if sample_column is None:
        candidates = [col for col in ["sample", "plant", "species", "name", "sample_name"] if col in df.columns]
        if not candidates:
            raise PlateReaderError("Mapping file must contain a sample column such as sample, plant, species, or name.")
        sample_column = candidates[0]
    if sample_column not in df.columns:
        raise PlateReaderError(f"Mapping file must contain sample column '{sample_column}'.")

    out = df[[well_column, sample_column]].copy()
    out = out.rename(columns={well_column: "well", sample_column: "sample"})
    out["well"] = out["well"].apply(normalize_well_name)
    out["sample"] = out["sample"].astype("string").str.strip()
    out = out.dropna(subset=["sample"])
    out = out[out["sample"] != ""].drop_duplicates(subset=["well"], keep="first")
    return out


def build_sample_activity_table(
    plate_files: Sequence[str | Path],
    *,
    rows: int = 8,
    columns: int = 12,
    sheet_name: str | int | None = None,
    orientation: str = "auto",
    control_wells: str | Sequence[str] | None = None,
    control_wells_by_plate: dict[str, Any] | Sequence[Any] | None = None,
    scale_mode: str = "positive_control_then_minmax_0_100",
    mapping_file: str | Path | None = None,
    mapping_well_column: str = "well",
    mapping_sample_column: str | None = None,
    exclude_control_wells: bool = True,
) -> pd.DataFrame:
    """Build a plant/extract activity table from replicate plate-reader files."""

    long_df = read_plate_replicates(
        plate_files,
        rows=rows,
        columns=columns,
        sheet_name=sheet_name,
        orientation=orientation,
        control_wells=control_wells,
        control_wells_by_plate=control_wells_by_plate,
        scale_mode=scale_mode,
    )
    if mapping_file:
        mapping = read_well_mapping(
            mapping_file,
            well_column=mapping_well_column,
            sample_column=mapping_sample_column,
        )
        long_df = long_df.merge(mapping, on="well", how="left")
    else:
        long_df["sample"] = long_df["well"]

    if exclude_control_wells:
        long_df = long_df.loc[~long_df["is_positive_control"]].copy()
    long_df = long_df.dropna(subset=["sample"]).copy()
    if long_df.empty:
        raise PlateReaderError("No mapped non-control wells remain after plate import.")

    sample_order = long_df.groupby("sample", as_index=False)["row_major_index"].min().rename(columns={"row_major_index": "_sample_order"})
    wide = _wide_replicate_table(
        long_df,
        index_columns=["sample"],
        value_column="scaled_signal_0_100",
    )
    wide = wide.merge(sample_order, on="sample", how="left").sort_values("_sample_order").drop(columns=["_sample_order"])
    wide["positive_control_avg"] = 100.0
    replicate_cols = [col for col in wide.columns if str(col).startswith("replicate_")]
    return wide[["sample", *replicate_cols, "average", "positive_control_avg", "n_replicates_used"]].reset_index(drop=True)
