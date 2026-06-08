#!/usr/bin/env python3
"""Integrated MZmine batch execution and fraction-feature matching workflow."""
from __future__ import annotations

import argparse
import copy
import json
import logging
import re
import shutil
import subprocess
import xml.etree.ElementTree as ET
from collections import deque
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath
from typing import Any, Callable, Iterable

import numpy as np
import pandas as pd


LOGGER = logging.getLogger("mzmine_pipeline_core")
SCRIPT_DIR = Path(__file__).resolve().parent

DEFAULT_STAGES = ("prepare", "run_complete", "run_fraction", "match")
ALLOWED_STAGES = set(DEFAULT_STAGES)
DEFAULT_MEMORY_MODE = "none"
DEFAULT_THREADS = "auto"
DEFAULT_IGNORE_PARAMETER_WARNINGS = True
DEFAULT_BLANK_PATTERN = "*blank*"
DEFAULT_RT_START = 2.0
DEFAULT_RT_END = 38.0
DEFAULT_WIDTH = 0.375
DEFAULT_MZ_TOL = 0.1
DEFAULT_RT_TOL = 1.0
OUTPUT_COLUMNS = [
    "fraction_file",
    "fraction_index",
    "mz",
    "rt",
    "area",
    "matched_target_mz",
    "matched_target_rt",
]
CONFIG_VERSION = 2
PARAMETER_WARNING_FAILURE = "Exiting because some parameter sets have been updated"
WINDOWS_FORBIDDEN_CHARS = set('<>:"/\\|?*')


class PipelineError(RuntimeError):
    """Raised for user-facing pipeline configuration and execution errors."""


@dataclass
class CompleteBatchSettings:
    template_path: str
    out_dir: str
    sample_files: list[str]
    blank_files: list[str]
    blank_pattern: str = DEFAULT_BLANK_PATTERN
    feature_dir: str | None = None
    sample_name: str = "sample"


@dataclass
class FractionBatchSettings:
    template_path: str
    out_dir: str
    sample_files: list[str]
    feature_dir: str
    rt_start: float = DEFAULT_RT_START
    rt_end: float = DEFAULT_RT_END
    width: float = DEFAULT_WIDTH
    sample_name: str = "sample"


@dataclass(frozen=True)
class MatchSettings:
    fractions_dir: Path
    big_csv: Path
    sample_name: str
    outdir: Path
    mz_tol: float = DEFAULT_MZ_TOL
    rt_tol: float = DEFAULT_RT_TOL


@dataclass(frozen=True)
class MZmineSettings:
    executable: str
    user_file: str = ""
    temp_dir: str = ""
    memory: str = DEFAULT_MEMORY_MODE
    threads: str = DEFAULT_THREADS
    ignore_parameter_warnings: bool = DEFAULT_IGNORE_PARAMETER_WARNINGS


def resolve_path(base_dir: str | Path, path_value: str | Path | None) -> Path | None:
    if path_value is None or str(path_value).strip() == "":
        return None
    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path
    return Path(base_dir).expanduser() / path


def stringify_path(path: Path | None) -> str:
    return "" if path is None else str(path)


def as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.splitlines() if item.strip()]
    return [str(item).strip() for item in value if str(item).strip()]


def _looks_like_windows_path(value: str) -> bool:
    text = (value or "").strip()
    return ("\\" in text) or (len(text) >= 2 and text[1] == ":") or (":/" in text)


def _normalize_path_for_batch(path: str) -> str:
    text = (path or "").strip()
    if not text:
        return text
    if _looks_like_windows_path(text):
        return str(PureWindowsPath(text))
    return text


def _basename_from_path(path: str) -> str:
    text = (path or "").strip()
    if not text:
        return ""
    if _looks_like_windows_path(text):
        return PureWindowsPath(text).name
    return Path(text).name


def _join_dir_and_filename(directory: str, filename: str) -> str:
    text = (directory or "").strip()
    if not text:
        return filename
    if _looks_like_windows_path(text):
        return str(PureWindowsPath(text) / filename)
    return str(Path(text) / filename)


def _validate_sample_name(sample_name: str) -> str:
    name = (sample_name or "").strip()
    if not name:
        raise ValueError("Enter a sample name used for output filenames.")
    if any(char in WINDOWS_FORBIDDEN_CHARS for char in name):
        bad = "".join(sorted({char for char in name if char in WINDOWS_FORBIDDEN_CHARS}))
        raise ValueError(f"Sample name contains forbidden filename characters: {bad}")
    if name.endswith("."):
        raise ValueError("Sample name cannot end with a dot.")
    return name


def _validate_mzml_files(files: list[str], label: str) -> None:
    if not files:
        raise ValueError(f"Select at least one {label} .mzML file.")
    bad_ext = [file for file in files if not file.lower().endswith(".mzml")]
    if bad_ext:
        raise ValueError(f"{label}: these are not .mzML files:\n  " + "\n  ".join(bad_ext))


def _indent_tree(tree: ET.ElementTree) -> None:
    try:
        ET.indent(tree, space="    ", level=0)
    except AttributeError:
        pass


def _update_blank_pattern_anywhere(elem: ET.Element, blank_pattern: str) -> int:
    updated = 0
    for param in elem.iter("parameter"):
        if param.get("name") != "Blank/Control raw data files":
            continue
        name_pattern = param.find("name_pattern")
        if name_pattern is not None:
            name_pattern.text = blank_pattern
            updated += 1
    return updated


def _find_import_batchsteps(root: ET.Element) -> list[ET.Element]:
    steps = root.findall("batchstep")
    out: list[ET.Element] = []
    for step in steps:
        method = step.get("method") or ""
        if "RawDataImportModule" in method or method.endswith("RawDataImportModule"):
            out.append(step)
    if not out and steps:
        out = [steps[0]]
    return out


def _set_import_file_names_in_step(step: ET.Element, files: list[str]) -> bool:
    param = step.find("parameter[@name='File names']")
    if param is None:
        return False
    for file_elem in list(param.findall("file")):
        param.remove(file_elem)
    for path in files:
        file_elem = ET.SubElement(param, "file")
        file_elem.text = _normalize_path_for_batch(path)
    return True


def _update_feature_table_path_complete(root: ET.Element, feature_dir: str | None, sample_name: str) -> int:
    if not feature_dir:
        return 0

    target_name = f"{sample_name}_complete_feature_table.csv"
    new_path = _join_dir_and_filename(_normalize_path_for_batch(feature_dir), target_name)

    updated = 0
    for param in root.iter("parameter"):
        if param.get("name") != "Filename":
            continue
        current_file = param.find("current_file")
        if current_file is None:
            continue
        current_file.text = new_path

        last_files = list(param.findall("last_file"))
        if last_files:
            last_files[0].text = new_path
            for extra in last_files[1:]:
                param.remove(extra)
        else:
            last_file = ET.SubElement(param, "last_file")
            last_file.text = new_path
        updated += 1
    return updated


def configure_complete_mzbatch(
    template_path: Path,
    out_path: Path,
    sample_files: list[str],
    blank_files: list[str],
    blank_pattern: str,
    feature_dir: str | None,
    sample_name: str,
) -> tuple[int, int, int]:
    try:
        tree = ET.parse(template_path)
    except ET.ParseError as exc:
        raise RuntimeError(f"Failed to parse XML from '{template_path}': {exc}") from exc

    root = tree.getroot()
    all_files = [*sample_files, *blank_files]
    n_import = 0
    did_any = False

    for step in _find_import_batchsteps(root):
        if _set_import_file_names_in_step(step, all_files):
            n_import += 1
            did_any = True

    if not did_any:
        for param in root.iter("parameter"):
            if param.get("name") != "File names":
                continue
            for child in list(param.findall("file")):
                param.remove(child)
            for path in all_files:
                file_elem = ET.SubElement(param, "file")
                file_elem.text = _normalize_path_for_batch(path)
            n_import += 1

    n_blank = _update_blank_pattern_anywhere(root, blank_pattern)
    n_filename = _update_feature_table_path_complete(root, feature_dir, sample_name)

    _indent_tree(tree)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tree.write(out_path, encoding="UTF-8", xml_declaration=True)
    return n_import, n_blank, n_filename


def _fraction_count(rt_start: float, rt_end: float, width: float) -> int:
    if width <= 0:
        raise ValueError("Time per fraction must be greater than 0.")
    if rt_end <= rt_start:
        raise ValueError("Fractionation end must be greater than start.")
    count = int(((rt_end - rt_start) / width) + 1e-9)
    if count <= 0:
        raise ValueError("Computed zero fractions. Check start/end/width.")
    return count


def _detect_template_fraction_index(root: ET.Element) -> tuple[str, int]:
    xml_text = ET.tostring(root, encoding="unicode")
    match = re.search(r"\bfrac_(\d{1,3})\b", xml_text)
    if not match:
        match = re.search(r"_[0-9]{1,3}\b", xml_text)
        if match:
            index = match.group(0)[1:]
            return index, len(index)
        raise ValueError("Could not detect a template fraction index such as frac_01 in the template batch.")
    index = match.group(1)
    return index, len(index)


def _replace_index_everywhere(elem: ET.Element, template_idx: str, new_idx: str) -> None:
    for key, value in list(elem.attrib.items()):
        if value:
            elem.attrib[key] = re.sub(rf"_{re.escape(template_idx)}\b", f"_{new_idx}", value)
    if elem.text:
        elem.text = re.sub(rf"_{re.escape(template_idx)}\b", f"_{new_idx}", elem.text)
    if elem.tail:
        elem.tail = re.sub(rf"_{re.escape(template_idx)}\b", f"_{new_idx}", elem.tail)
    for child in list(elem):
        _replace_index_everywhere(child, template_idx, new_idx)


def _set_crop_input_files(crop_step: ET.Element, mzml_paths: list[str]) -> None:
    param = crop_step.find("parameter[@name='Raw data files']")
    if param is None:
        raise ValueError("CropFilter step missing <parameter name='Raw data files'>.")
    for specific_file in list(param.findall("specific_file")):
        param.remove(specific_file)
    for path in mzml_paths:
        specific_file = ET.SubElement(param, "specific_file")
        specific_file.text = _basename_from_path(path)


def _set_crop_retention_time(crop_step: ET.Element, rt_min: float, rt_max: float) -> None:
    scan_filters = crop_step.find("parameter[@name='Scan filters']")
    if scan_filters is None:
        raise ValueError("CropFilter step missing <parameter name='Scan filters'>.")
    rt_param = scan_filters.find("parameter[@name='Retention time']")
    if rt_param is None:
        raise ValueError("CropFilter step missing retention time parameter.")
    min_elem = rt_param.find("min")
    max_elem = rt_param.find("max")
    if min_elem is None or max_elem is None:
        raise ValueError("Retention time parameter missing <min>/<max>.")
    min_elem.text = f"{rt_min:.3f}"
    max_elem.text = f"{rt_max:.3f}"


def _set_csv_export_path(export_step: ET.Element, feature_dir: str, template_idx: str, new_idx: str) -> None:
    filename_param = export_step.find("parameter[@name='Filename']")
    if filename_param is None:
        return
    current_file = filename_param.find("current_file")
    if current_file is None:
        return

    old_path = (current_file.text or "").strip()
    old_name = _basename_from_path(old_path) or f"frac_{new_idx}.csv"
    new_name = re.sub(rf"_{re.escape(template_idx)}\b", f"_{new_idx}", old_name)

    if new_name == old_name and template_idx != new_idx and not re.search(rf"_{re.escape(new_idx)}\b", old_name):
        stem = Path(old_name).stem
        suffix = Path(old_name).suffix or ".csv"
        new_name = f"{stem}_{new_idx}{suffix}"

    new_full = _join_dir_and_filename(feature_dir, new_name)
    current_file.text = new_full
    for last_file in list(filename_param.findall("last_file")):
        filename_param.remove(last_file)
    last_file = ET.SubElement(filename_param, "last_file")
    last_file.text = new_full


def replicate_fraction_mzbatch(
    template_path: Path,
    out_path: Path,
    mzml_paths: list[str],
    feature_dir: str,
    rt_start: float,
    rt_end: float,
    width: float,
) -> tuple[int, str]:
    tree = ET.parse(template_path)
    root = tree.getroot()
    steps = root.findall("batchstep")
    if len(steps) < 2:
        raise ValueError("Template batch must contain at least two batchsteps: import plus processing.")

    import_step = copy.deepcopy(steps[0])
    processing_templates = steps[1:]
    template_idx, idx_width = _detect_template_fraction_index(root)

    if not _set_import_file_names_in_step(import_step, mzml_paths):
        for param in import_step.iter("parameter"):
            if param.get("name") != "File names":
                continue
            for child in list(param.findall("file")):
                param.remove(child)
            for path in mzml_paths:
                file_elem = ET.SubElement(param, "file")
                file_elem.text = _normalize_path_for_batch(path)
            break

    new_root = ET.Element(root.tag, dict(root.attrib))
    for child in list(root):
        if child.tag != "batchstep":
            new_root.append(copy.deepcopy(child))
    new_root.append(import_step)

    n_fractions = _fraction_count(rt_start, rt_end, width)

    def is_method(step: ET.Element, endswith: str) -> bool:
        return step.get("method", "").endswith(endswith)

    for index in range(n_fractions):
        new_idx = str(index + 1).zfill(idx_width)
        rt_min = rt_start + index * width
        rt_max = rt_min + width
        for template_step in processing_templates:
            step = copy.deepcopy(template_step)
            _replace_index_everywhere(step, template_idx, new_idx)
            if is_method(step, "CropFilterModule"):
                _set_crop_input_files(step, mzml_paths)
                _set_crop_retention_time(step, rt_min, rt_max)
            if is_method(step, "CSVExportModularModule"):
                _set_csv_export_path(step, feature_dir, template_idx, new_idx)
            new_root.append(step)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_tree = ET.ElementTree(new_root)
    _indent_tree(out_tree)
    out_tree.write(out_path, encoding="UTF-8", xml_declaration=True)
    return n_fractions, template_idx


def complete_output_path(out_dir: str | Path, sample_name: str) -> Path:
    return Path(out_dir).expanduser() / f"{_validate_sample_name(sample_name)}_configured.mzbatch"


def fraction_output_path(out_dir: str | Path, sample_name: str) -> Path:
    return Path(out_dir).expanduser() / f"{_validate_sample_name(sample_name)}_fraction_configured.mzbatch"


def run_complete_from_settings(settings: CompleteBatchSettings | dict[str, Any]) -> dict[str, Any]:
    if isinstance(settings, dict):
        settings = CompleteBatchSettings(**settings)

    sample_name = _validate_sample_name(settings.sample_name)
    template_path = Path(settings.template_path).expanduser()
    if not template_path.exists():
        raise ValueError(f"COMPLETE template does not exist: {template_path}")

    _validate_mzml_files(settings.sample_files, "SAMPLE")
    _validate_mzml_files(settings.blank_files, "BLANK")

    out_path = complete_output_path(settings.out_dir, sample_name)
    blank_pattern = (settings.blank_pattern or "").strip() or DEFAULT_BLANK_PATTERN
    n_import, n_blank, n_filename = configure_complete_mzbatch(
        template_path=template_path,
        out_path=out_path,
        sample_files=settings.sample_files,
        blank_files=settings.blank_files,
        blank_pattern=blank_pattern,
        feature_dir=settings.feature_dir,
        sample_name=sample_name,
    )
    summary = {
        "mode": "complete",
        "output_path": str(out_path),
        "sample_name": sample_name,
        "sample_files": list(settings.sample_files),
        "blank_files": list(settings.blank_files),
        "blank_pattern": blank_pattern,
        "feature_dir": settings.feature_dir,
        "updated_import_file_name_blocks": n_import,
        "updated_blank_pattern_blocks": n_blank,
        "updated_filename_blocks": n_filename,
    }
    LOGGER.info("Complete batch saved: %s", out_path)
    return summary


def run_fraction_from_settings(settings: FractionBatchSettings | dict[str, Any]) -> dict[str, Any]:
    if isinstance(settings, dict):
        settings = FractionBatchSettings(**settings)

    sample_name = _validate_sample_name(settings.sample_name)
    template_path = Path(settings.template_path).expanduser()
    if not template_path.exists():
        raise ValueError(f"FRACTION template does not exist: {template_path}")
    _validate_mzml_files(settings.sample_files, "SAMPLE")
    if not str(settings.feature_dir).strip():
        raise ValueError("Select directory for per-fraction CSV feature tables.")

    out_path = fraction_output_path(settings.out_dir, sample_name)
    n_fractions, template_idx = replicate_fraction_mzbatch(
        template_path=template_path,
        out_path=out_path,
        mzml_paths=settings.sample_files,
        feature_dir=settings.feature_dir,
        rt_start=float(settings.rt_start),
        rt_end=float(settings.rt_end),
        width=float(settings.width),
    )
    summary = {
        "mode": "fraction",
        "output_path": str(out_path),
        "sample_name": sample_name,
        "sample_files": list(settings.sample_files),
        "feature_dir": settings.feature_dir,
        "rt_start": float(settings.rt_start),
        "rt_end": float(settings.rt_end),
        "width": float(settings.width),
        "fractions": n_fractions,
        "template_index_detected": template_idx,
    }
    LOGGER.info("Fraction batch saved: %s", out_path)
    return summary


def slugify(name: str) -> str:
    value = (name or "").strip().lower()
    value = value.replace("&", " and ")
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^a-z0-9_]+", "", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "sample"


def find_col(df: pd.DataFrame, preferred: str, fallbacks: tuple[str, ...]) -> str:
    columns = list(df.columns)
    if preferred in columns:
        return preferred
    for candidate in fallbacks:
        if candidate in columns:
            return candidate
    raise ValueError(f"Missing required column '{preferred}'. Available columns: {columns}")


def coerce_numeric_columns(df: pd.DataFrame, columns: tuple[str, ...], *, table_label: str) -> pd.DataFrame:
    converted = df.copy()
    for column in columns:
        converted[column] = pd.to_numeric(converted[column], errors="coerce")
    before = len(converted)
    converted = converted.dropna(subset=list(columns))
    dropped = before - len(converted)
    if dropped:
        LOGGER.info("Dropped %s row(s) with non-numeric %s values from %s.", dropped, ", ".join(columns), table_label)
    return converted


def parse_fraction_index(filename: str, fallback: int) -> int:
    match = re.search(r"frac_(\d{1,4})", filename)
    return int(match.group(1)) if match else fallback


def list_fraction_files(fractions_dir: Path) -> list[Path]:
    files = sorted(fractions_dir.glob("frac_*.csv"))
    if not files:
        raise FileNotFoundError(f"No fraction files found in {fractions_dir} (expected 'frac_*.csv').")
    return files


def build_big_index(big_df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    mz_values = big_df["mz"].to_numpy(dtype=float)
    rt_values = big_df["rt"].to_numpy(dtype=float)
    order = np.argsort(mz_values)
    return mz_values[order], rt_values[order]


def match_rows_to_big(
    frac_mz: np.ndarray,
    frac_rt: np.ndarray,
    big_mz_sorted: np.ndarray,
    big_rt_sorted: np.ndarray,
    mz_tol: float,
    rt_tol: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    keep = np.zeros(frac_mz.size, dtype=bool)
    matched_mz = np.full(frac_mz.size, np.nan, dtype=float)
    matched_rt = np.full(frac_mz.size, np.nan, dtype=float)

    for index in range(frac_mz.size):
        mz_value = float(frac_mz[index])
        rt_value = float(frac_rt[index])
        low = np.searchsorted(big_mz_sorted, mz_value - mz_tol, side="left")
        high = np.searchsorted(big_mz_sorted, mz_value + mz_tol, side="right")
        if high <= low:
            continue

        candidate_mz = big_mz_sorted[low:high]
        candidate_rt = big_rt_sorted[low:high]
        rt_ok = np.abs(candidate_rt - rt_value) <= rt_tol
        if not np.any(rt_ok):
            continue

        candidate_mz = candidate_mz[rt_ok]
        candidate_rt = candidate_rt[rt_ok]
        distances = (mz_value - candidate_mz) ** 2 + (rt_value - candidate_rt) ** 2
        best = int(np.argmin(distances))

        keep[index] = True
        matched_mz[index] = candidate_mz[best]
        matched_rt[index] = candidate_rt[best]

    return keep, matched_mz, matched_rt


def output_path_for(sample_name: str, outdir: Path) -> Path:
    return outdir / f"{slugify(sample_name)}_filtered.csv"


def run_match(settings: MatchSettings) -> dict[str, object]:
    fractions_dir = Path(settings.fractions_dir).expanduser().resolve()
    big_csv = Path(settings.big_csv).expanduser().resolve()
    outdir = Path(settings.outdir).expanduser().resolve()
    sample_name = settings.sample_name.strip()

    if not sample_name:
        raise ValueError("Sample name is required.")
    if settings.mz_tol < 0 or settings.rt_tol < 0:
        raise ValueError("Tolerances must be greater than or equal to zero.")
    if not fractions_dir.exists() or not fractions_dir.is_dir():
        raise FileNotFoundError(f"Fractions directory not found: {fractions_dir}")
    if not big_csv.exists() or not big_csv.is_file():
        raise FileNotFoundError(f"Big feature table CSV not found: {big_csv}")

    fraction_files = list_fraction_files(fractions_dir)
    LOGGER.info("Found %s fraction table(s).", len(fraction_files))

    big = pd.read_csv(big_csv)
    mz_col = find_col(big, "mz", ("m/z", "mzmed", "row m/z", "row_mz"))
    rt_col = find_col(big, "rt", ("retention_time", "rtmed", "row retention time", "row_rt", "row retention time (min)"))
    big = big.rename(columns={mz_col: "mz", rt_col: "rt"})
    big = coerce_numeric_columns(big, ("mz", "rt"), table_label=big_csv.name)
    if big.empty:
        raise ValueError("Big feature table has no valid rows after reading mz/rt.")
    big_mz_sorted, big_rt_sorted = build_big_index(big)
    LOGGER.info("Loaded big feature table with %s valid row(s).", len(big))

    matched_records: list[pd.DataFrame] = []
    files_with_rows = 0
    positive_rows = 0
    matched_rows = 0

    for fallback_index, path in enumerate(fraction_files, start=1):
        try:
            sub = pd.read_csv(path)
        except pd.errors.EmptyDataError:
            LOGGER.info("Skipped empty CSV: %s", path.name)
            continue
        if sub.empty:
            LOGGER.info("Skipped empty table: %s", path.name)
            continue

        mz_col_f = find_col(sub, "mz", ("m/z", "row m/z", "row_mz"))
        rt_col_f = find_col(sub, "rt", ("retention_time", "row retention time", "row_rt", "row retention time (min)"))
        area_col_f = find_col(sub, "area", ("Area", "peak_area", "Peak area", "row area", "row_area"))
        sub = sub.rename(columns={mz_col_f: "mz", rt_col_f: "rt", area_col_f: "area"})
        sub = coerce_numeric_columns(sub, ("mz", "rt", "area"), table_label=path.name)
        sub = sub[sub["area"] > 0]
        if sub.empty:
            LOGGER.info("Skipped table with no positive-area rows: %s", path.name)
            continue

        files_with_rows += 1
        positive_rows += len(sub)
        keep, matched_mz, matched_rt = match_rows_to_big(
            sub["mz"].to_numpy(dtype=float),
            sub["rt"].to_numpy(dtype=float),
            big_mz_sorted,
            big_rt_sorted,
            settings.mz_tol,
            settings.rt_tol,
        )
        if not np.any(keep):
            LOGGER.info("No matches in %s.", path.name)
            continue

        kept = sub.loc[keep, ["mz", "rt", "area"]].copy()
        kept["matched_target_mz"] = matched_mz[keep]
        kept["matched_target_rt"] = matched_rt[keep]
        kept.insert(0, "fraction_index", parse_fraction_index(path.name, fallback=fallback_index))
        kept.insert(0, "fraction_file", path.name)
        matched_rows += len(kept)
        matched_records.append(kept)
        LOGGER.info("Matched %s row(s) in %s.", len(kept), path.name)

    outdir.mkdir(parents=True, exist_ok=True)
    out_path = output_path_for(sample_name, outdir)
    out_df = pd.concat(matched_records, ignore_index=True) if matched_records else pd.DataFrame(columns=OUTPUT_COLUMNS)
    out_df.to_csv(out_path, index=False)
    LOGGER.info("Saved %s matched row(s) to %s.", len(out_df), out_path)

    return {
        "output_path": out_path,
        "fraction_file_count": len(fraction_files),
        "fraction_files_with_positive_rows": files_with_rows,
        "positive_fraction_rows": positive_rows,
        "matched_rows": matched_rows,
        "big_rows": len(big),
    }


def parse_stages(value: str | Iterable[str] | None) -> list[str]:
    if value is None:
        return list(DEFAULT_STAGES)
    if isinstance(value, str):
        stages = [item.strip() for item in value.split(",") if item.strip()]
    else:
        stages = [str(item).strip() for item in value if str(item).strip()]
    unknown = [stage for stage in stages if stage not in ALLOWED_STAGES]
    if unknown:
        raise PipelineError(f"Unknown stage(s): {unknown}. Allowed stages: {sorted(ALLOWED_STAGES)}")
    return stages


def template_config() -> dict[str, Any]:
    return {
        "config_version": CONFIG_VERSION,
        "base_dir": str(SCRIPT_DIR),
        "sample_name": "sample_name_here",
        "sample_files": [],
        "blank_files": [],
        "stages": list(DEFAULT_STAGES),
        "complete": {
            "template_path": "templates/big_empty_template.mzbatch",
            "out_dir": "outputs/configured_batches",
            "feature_dir": "outputs/complete_feature_table_csv",
            "blank_pattern": "*blank*",
        },
        "fraction": {
            "template_path": "templates/fraction_empty_template.mzbatch",
            "out_dir": "outputs/configured_batches",
            "feature_dir": "outputs/fraction_feature_tables_csv",
            "rt_start": 2.0,
            "rt_end": 38.0,
            "width": 0.375,
        },
        "mzmine": {
            "executable": "C:/Program Files/MZmine/mzmine_console.exe",
            "user_file": "",
            "temp_dir": "outputs/mzmine_temp",
            "memory": DEFAULT_MEMORY_MODE,
            "threads": DEFAULT_THREADS,
            "ignore_parameter_warnings": DEFAULT_IGNORE_PARAMETER_WARNINGS,
        },
        "matching": {
            "outdir": "outputs/matched_fraction_features",
            "mz_tol": 0.1,
            "rt_tol": 1.0,
        },
    }


def save_config(config: dict[str, Any], path: str | Path) -> Path:
    out = Path(path).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
    return out


def _points_to_legacy_batch_template(value: Any, template_name: str) -> bool:
    text = str(value or "").replace("\\", "/")
    return text.endswith(f"/{template_name}") and "batch_prep" in text


def _points_to_bundled_template(value: Any, template_name: str) -> bool:
    text = str(value or "").replace("\\", "/")
    return text.endswith(f"01_mzmine_pipeline/templates/{template_name}") or text == f"templates/{template_name}"


def _normalize_legacy_config(config: dict[str, Any]) -> dict[str, Any]:
    config_version = int(config.get("config_version") or 1)
    complete = config.setdefault("complete", {})
    fraction = config.setdefault("fraction", {})
    mzmine = config.setdefault("mzmine", {})
    if _points_to_legacy_batch_template(complete.get("template_path"), "big_empty_template.mzbatch"):
        complete["template_path"] = "templates/big_empty_template.mzbatch"
    if _points_to_legacy_batch_template(fraction.get("template_path"), "fraction_empty_template.mzbatch"):
        fraction["template_path"] = "templates/fraction_empty_template.mzbatch"
    if not mzmine.get("temp_dir"):
        mzmine["temp_dir"] = "outputs/mzmine_temp"
    uses_bundled_templates = (
        _points_to_bundled_template(complete.get("template_path"), "big_empty_template.mzbatch")
        and _points_to_bundled_template(fraction.get("template_path"), "fraction_empty_template.mzbatch")
    )
    if "ignore_parameter_warnings" not in mzmine or (config_version < CONFIG_VERSION and uses_bundled_templates):
        mzmine["ignore_parameter_warnings"] = DEFAULT_IGNORE_PARAMETER_WARNINGS
    config["config_version"] = CONFIG_VERSION
    return config


def load_config(path: str | Path) -> dict[str, Any]:
    config_path = Path(path).expanduser().resolve()
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    config.setdefault("base_dir", str(config_path.parent))
    return _normalize_legacy_config(config)


def sample_name_from_config(config: dict[str, Any]) -> str:
    sample_name = str(config.get("sample_name") or "").strip()
    if not sample_name:
        raise PipelineError("Config must contain a non-empty sample_name.")
    return sample_name


def resolved_pipeline_paths(config: dict[str, Any], *, base_dir: str | Path) -> dict[str, str]:
    sample_name = sample_name_from_config(config)
    complete_cfg = config.get("complete", {})
    fraction_cfg = config.get("fraction", {})
    matching_cfg = config.get("matching", {})

    complete_out_dir = resolve_path(base_dir, complete_cfg.get("out_dir")) or Path(base_dir)
    fraction_out_dir = resolve_path(base_dir, fraction_cfg.get("out_dir")) or Path(base_dir)
    complete_feature_dir = resolve_path(base_dir, complete_cfg.get("feature_dir"))
    fraction_feature_dir = resolve_path(base_dir, fraction_cfg.get("feature_dir"))
    match_outdir = resolve_path(base_dir, matching_cfg.get("outdir")) or Path(base_dir)

    complete_batch = complete_output_path(complete_out_dir, sample_name)
    fraction_batch = fraction_output_path(fraction_out_dir, sample_name)
    complete_csv = None if complete_feature_dir is None else complete_feature_dir / f"{sample_name}_complete_feature_table.csv"
    match_output = output_path_for(sample_name, match_outdir)

    return {
        "complete_batch": str(complete_batch),
        "fraction_batch": str(fraction_batch),
        "complete_csv": stringify_path(complete_csv),
        "fraction_dir": stringify_path(fraction_feature_dir),
        "match_output": str(match_output),
    }


def build_complete_settings(config: dict[str, Any], *, base_dir: str | Path) -> Any:
    complete_cfg = config.get("complete", {})
    return CompleteBatchSettings(
        template_path=stringify_path(resolve_path(base_dir, complete_cfg.get("template_path"))),
        out_dir=stringify_path(resolve_path(base_dir, complete_cfg.get("out_dir"))),
        sample_files=[stringify_path(resolve_path(base_dir, item)) for item in as_list(config.get("sample_files"))],
        blank_files=[stringify_path(resolve_path(base_dir, item)) for item in as_list(config.get("blank_files"))],
        blank_pattern=str(complete_cfg.get("blank_pattern") or "*blank*"),
        feature_dir=stringify_path(resolve_path(base_dir, complete_cfg.get("feature_dir"))),
        sample_name=sample_name_from_config(config),
    )


def build_fraction_settings(config: dict[str, Any], *, base_dir: str | Path) -> Any:
    fraction_cfg = config.get("fraction", {})
    return FractionBatchSettings(
        template_path=stringify_path(resolve_path(base_dir, fraction_cfg.get("template_path"))),
        out_dir=stringify_path(resolve_path(base_dir, fraction_cfg.get("out_dir"))),
        sample_files=[stringify_path(resolve_path(base_dir, item)) for item in as_list(config.get("sample_files"))],
        feature_dir=stringify_path(resolve_path(base_dir, fraction_cfg.get("feature_dir"))),
        rt_start=float(fraction_cfg.get("rt_start", 2.0)),
        rt_end=float(fraction_cfg.get("rt_end", 38.0)),
        width=float(fraction_cfg.get("width", 0.375)),
        sample_name=sample_name_from_config(config),
    )


def build_match_settings(config: dict[str, Any], *, base_dir: str | Path) -> Any:
    paths = resolved_pipeline_paths(config, base_dir=base_dir)
    matching_cfg = config.get("matching", {})
    complete_csv = Path(paths["complete_csv"])
    fraction_dir = Path(paths["fraction_dir"])
    outdir = resolve_path(base_dir, matching_cfg.get("outdir")) or Path(base_dir)
    return MatchSettings(
        fractions_dir=fraction_dir,
        big_csv=complete_csv,
        sample_name=sample_name_from_config(config),
        outdir=outdir,
        mz_tol=float(matching_cfg.get("mz_tol", DEFAULT_MZ_TOL)),
        rt_tol=float(matching_cfg.get("rt_tol", DEFAULT_RT_TOL)),
    )


def build_mzmine_settings(config: dict[str, Any], *, base_dir: str | Path) -> MZmineSettings:
    mzmine_cfg = config.get("mzmine", {})
    executable_value = str(mzmine_cfg.get("executable") or "").strip()
    if not executable_value:
        raise PipelineError("Select the MZmine console executable, for example mzmine_console.exe.")
    return MZmineSettings(
        executable=stringify_path(resolve_path(base_dir, executable_value))
        if any(mark in executable_value for mark in ("\\", "/", ":"))
        else executable_value,
        user_file=stringify_path(resolve_path(base_dir, mzmine_cfg.get("user_file"))),
        temp_dir=stringify_path(resolve_path(base_dir, mzmine_cfg.get("temp_dir"))),
        memory=str(mzmine_cfg.get("memory") or DEFAULT_MEMORY_MODE),
        threads=str(mzmine_cfg.get("threads") or DEFAULT_THREADS),
        ignore_parameter_warnings=bool(mzmine_cfg.get("ignore_parameter_warnings", DEFAULT_IGNORE_PARAMETER_WARNINGS)),
    )


def resolve_mzmine_executable(executable: str, *, dry_run: bool = False) -> str:
    value = executable.strip()
    if not value:
        raise PipelineError("MZmine executable is required.")
    if any(mark in value for mark in ("\\", "/", ":")):
        path = Path(value).expanduser()
        if not dry_run and not path.exists():
            raise FileNotFoundError(f"MZmine executable not found: {path}")
        return str(path)
    found = shutil.which(value)
    if found:
        return found
    if dry_run:
        return value
    raise FileNotFoundError(f"MZmine executable was not found on PATH: {value}")


def build_mzmine_command(settings: MZmineSettings, batch_path: str | Path, *, dry_run: bool = False) -> list[str]:
    command = [
        resolve_mzmine_executable(settings.executable, dry_run=dry_run),
    ]
    user_file = Path(settings.user_file).expanduser() if settings.user_file else None
    if user_file and (dry_run or user_file.exists()):
        command.extend(["--user", str(user_file)])
    command.extend(["--batch", str(Path(batch_path).expanduser())])
    if settings.temp_dir:
        command.extend(["--temp", str(Path(settings.temp_dir).expanduser())])
    if settings.memory:
        command.extend(["--memory", settings.memory])
    if settings.threads:
        command.extend(["--threads", settings.threads])
    if settings.ignore_parameter_warnings:
        command.append("--ignore-parameter-warnings")
    return command


def command_to_text(command: list[str]) -> str:
    return subprocess.list2cmdline(command)


def summarize_mzmine_failure(returncode: int, command_text: str, recent_lines: list[str]) -> str:
    joined_recent = "\n".join(recent_lines)
    important_lines = [
        line
        for line in recent_lines
        if any(marker in line for marker in (" SEVERE ", " ERROR ", " WARNING ", "Exiting because"))
    ]
    context = "\n".join((important_lines or recent_lines)[-8:])
    if PARAMETER_WARNING_FAILURE in joined_recent:
        return (
            "MZmine stopped because the batch was made with an older MZmine version and one or more "
            "parameter sets changed. Enable 'Continue after batch-version warnings' in the MZmine Runner "
            "tab, or open the generated .mzbatch in the MZmine GUI, review it, save it with the installed "
            "MZmine version, and run again.\n\n"
            f"Command: {command_text}\n\nLast MZmine messages:\n{context}"
        )
    return f"MZmine failed with exit code {returncode}: {command_text}\n\nLast MZmine messages:\n{context}"


def run_mzmine_batch(
    settings: MZmineSettings,
    batch_path: str | Path,
    *,
    dry_run: bool = False,
    log_callback: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    log = log_callback or LOGGER.info
    batch_path = Path(batch_path).expanduser()
    if not dry_run and not batch_path.exists():
        raise FileNotFoundError(f"MZmine batch file not found: {batch_path}")
    if settings.temp_dir and not dry_run:
        Path(settings.temp_dir).expanduser().mkdir(parents=True, exist_ok=True)
    if settings.user_file and not Path(settings.user_file).expanduser().exists():
        log(f"WARNING: MZmine user file not found; command will rely on the current MZmine login: {settings.user_file}")
    command = build_mzmine_command(settings, batch_path, dry_run=dry_run)
    command_text = command_to_text(command)
    if dry_run:
        log(f"DRY RUN: {command_text}")
        return {"command": command, "command_text": command_text, "returncode": None, "dry_run": True}

    log(f"Running MZmine: {command_text}")
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert process.stdout is not None
    recent_lines: deque[str] = deque(maxlen=120)
    for line in process.stdout:
        text = line.rstrip()
        if text:
            recent_lines.append(text)
            log(text)
    returncode = process.wait()
    if returncode != 0:
        raise PipelineError(summarize_mzmine_failure(returncode, command_text, list(recent_lines)))
    return {"command": command, "command_text": command_text, "returncode": returncode, "dry_run": False}


def verify_complete_csv(path: str | Path) -> dict[str, Any]:
    target = Path(path).expanduser()
    if not target.exists():
        raise FileNotFoundError(f"Complete feature table CSV was not found after MZmine run: {target}")
    return {"path": str(target), "exists": True, "size_bytes": target.stat().st_size}


def verify_fraction_csvs(directory: str | Path) -> dict[str, Any]:
    target = Path(directory).expanduser()
    if not target.exists() or not target.is_dir():
        raise FileNotFoundError(f"Fraction CSV directory was not found after MZmine run: {target}")
    files = sorted(target.glob("frac_*.csv"))
    if not files:
        raise FileNotFoundError(f"No frac_*.csv files were found after MZmine run in: {target}")
    return {"path": str(target), "file_count": len(files), "files": [str(path) for path in files]}


def run_pipeline(
    config: dict[str, Any],
    *,
    stages: str | Iterable[str] | None = None,
    dry_run: bool = False,
    log_callback: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    base_dir = Path(config.get("base_dir", ".")).expanduser()
    selected_stages = parse_stages(stages if stages is not None else config.get("stages", DEFAULT_STAGES))
    log = log_callback or LOGGER.info
    result: dict[str, Any] = {
        "sample_name": sample_name_from_config(config),
        "base_dir": str(base_dir),
        "stages": selected_stages,
        "dry_run": dry_run,
        "paths": resolved_pipeline_paths(config, base_dir=base_dir),
        "steps": {},
    }

    if "prepare" in selected_stages:
        if dry_run:
            result["steps"]["prepare"] = {
                "complete_settings": build_complete_settings(config, base_dir=base_dir).__dict__,
                "fraction_settings": build_fraction_settings(config, base_dir=base_dir).__dict__,
            }
            log("DRY RUN: batch preparation settings resolved.")
        else:
            log("Preparing complete MZmine batch...")
            complete_summary = run_complete_from_settings(build_complete_settings(config, base_dir=base_dir))
            log("Preparing fraction MZmine batch...")
            fraction_summary = run_fraction_from_settings(build_fraction_settings(config, base_dir=base_dir))
            result["steps"]["prepare"] = {
                "complete": complete_summary,
                "fraction": fraction_summary,
            }

    mzmine_settings = None
    if "run_complete" in selected_stages or "run_fraction" in selected_stages:
        mzmine_settings = build_mzmine_settings(config, base_dir=base_dir)
    if "run_complete" in selected_stages:
        assert mzmine_settings is not None
        complete_batch = result["paths"]["complete_batch"]
        run_summary = run_mzmine_batch(mzmine_settings, complete_batch, dry_run=dry_run, log_callback=log)
        result["steps"]["run_complete"] = run_summary
        if not dry_run:
            result["steps"]["complete_output_check"] = verify_complete_csv(result["paths"]["complete_csv"])

    if "run_fraction" in selected_stages:
        assert mzmine_settings is not None
        fraction_batch = result["paths"]["fraction_batch"]
        run_summary = run_mzmine_batch(mzmine_settings, fraction_batch, dry_run=dry_run, log_callback=log)
        result["steps"]["run_fraction"] = run_summary
        if not dry_run:
            result["steps"]["fraction_output_check"] = verify_fraction_csvs(result["paths"]["fraction_dir"])

    if "match" in selected_stages:
        if dry_run:
            match_settings = build_match_settings(config, base_dir=base_dir)
            result["steps"]["match"] = {
                "settings": {key: str(value) if isinstance(value, Path) else value for key, value in match_settings.__dict__.items()}
            }
            log("DRY RUN: matching settings resolved.")
        else:
            verify_complete_csv(result["paths"]["complete_csv"])
            verify_fraction_csvs(result["paths"]["fraction_dir"])
            log("Running fraction-feature matching...")
            match_summary = run_match(build_match_settings(config, base_dir=base_dir))
            result["steps"]["match"] = {
                key: str(value) if isinstance(value, Path) else value for key, value in match_summary.items()
            }

    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the integrated MZmine batch and fraction-feature matching pipeline.")
    parser.add_argument("--make-template-config", help="Write a template JSON config and exit.")
    parser.add_argument("--config", help="Run from a saved JSON config.")
    parser.add_argument("--stages", help="Comma-separated subset: prepare,run_complete,run_fraction,match.")
    parser.add_argument("--dry-run", action="store_true", help="Resolve settings and print MZmine commands without running MZmine.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    if args.make_template_config:
        out = save_config(template_config(), args.make_template_config)
        print(f"Template config written to: {out}")
        return 0

    if not args.config:
        raise SystemExit("Provide --config or --make-template-config.")

    config = load_config(args.config)
    result = run_pipeline(config, stages=args.stages, dry_run=bool(args.dry_run), log_callback=LOGGER.info)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
