#!/usr/bin/env python3
"""Core utilities for configuring MZmine .mzbatch files.

This module contains the XML-editing logic used by the GUI and CLI.
It preserves the behavior of the original combined script:

1) Complete feature table batch setup
   - writes sample + blank mzML files into the import step
   - updates the blank/control filename pattern
   - points CSV export Filename/current_file to
     {sample_name}_complete_feature_table.csv

2) Fraction feature table batch replication
   - imports selected sample mzML files once
   - replicates the fraction-processing block across RT windows
   - updates CropFilter raw data files and RT limits per fraction
   - updates per-fraction CSV export paths

The public functions configure_complete_mzbatch() and
replicate_fraction_mzbatch() intentionally keep the same signatures as the
original script, so older code can still call them directly.
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, asdict
from pathlib import Path, PureWindowsPath
from typing import Any


DEFAULT_BLANK_PATTERN = "*blank*"
DEFAULT_RT_START = 2.0
DEFAULT_RT_END = 38.0
DEFAULT_WIDTH = 0.375

WINDOWS_FORBIDDEN_CHARS = set('<>:"/\\|?*')

LOGGER = logging.getLogger("mzmine_batch_core")


# -------------------------
# Path helpers (Windows-friendly)
# -------------------------
def _looks_like_windows_path(s: str) -> bool:
    s = (s or "").strip()
    return ("\\" in s) or (len(s) >= 2 and s[1] == ":") or (":/" in s)


def _normalize_path_for_batch(p: str) -> str:
    """Return a path string formatted in a Windows-looking way if appropriate."""
    p = (p or "").strip()
    if not p:
        return p
    if _looks_like_windows_path(p):
        return str(PureWindowsPath(p))
    return p


def _basename_from_path(p: str) -> str:
    p = (p or "").strip()
    if not p:
        return ""
    if _looks_like_windows_path(p):
        return PureWindowsPath(p).name
    return Path(p).name


def _join_dir_and_filename(directory: str, filename: str) -> str:
    directory = (directory or "").strip()
    if not directory:
        return filename
    if _looks_like_windows_path(directory):
        return str(PureWindowsPath(directory) / filename)
    return str(Path(directory) / filename)


def _validate_sample_name(sample_name: str) -> str:
    name = (sample_name or "").strip()
    if not name:
        raise ValueError("Enter a sample name (used for output filenames).")
    if any(ch in WINDOWS_FORBIDDEN_CHARS for ch in name):
        bad = "".join(sorted(set(ch for ch in name if ch in WINDOWS_FORBIDDEN_CHARS)))
        raise ValueError(
            "Sample name contains forbidden filename characters: "
            f"{bad}\nAvoid: < > : \" / \\ | ? *"
        )
    if name.endswith("."):
        raise ValueError("Sample name cannot end with a dot (Windows filename rule).")
    return name


def _validate_mzml_files(files: list[str], label: str) -> None:
    if not files:
        raise ValueError(f"Select at least one {label} .mzML file.")
    bad_ext = [f for f in files if not f.lower().endswith(".mzml")]
    if bad_ext:
        raise ValueError(f"{label}: these are not .mzML files:\n  " + "\n  ".join(bad_ext))


# -------------------------
# XML helpers (shared)
# -------------------------
def _indent_tree(tree: ET.ElementTree) -> None:
    try:
        ET.indent(tree, space="    ", level=0)  # py3.9+
    except Exception:
        pass


def _update_blank_pattern_anywhere(elem: ET.Element, blank_pattern: str) -> int:
    """Update all <parameter name='Blank/Control raw data files'><name_pattern>...</name_pattern> under elem."""
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
    for s in steps:
        m = (s.get("method") or "")
        if ("RawDataImportModule" in m) or m.endswith("RawDataImportModule"):
            out.append(s)
    # If we couldn't detect, fall back to the first step (common in MZmine batches)
    if not out and steps:
        out = [steps[0]]
    return out


def _set_import_file_names_in_step(step: ET.Element, files: list[str]) -> bool:
    p = step.find("parameter[@name='File names']")
    if p is None:
        return False
    for f in list(p.findall("file")):
        p.remove(f)
    for fp in files:
        fe = ET.SubElement(p, "file")
        fe.text = _normalize_path_for_batch(fp)
    return True


# -------------------------
# 1) COMPLETE feature table batch
# -------------------------
def _update_feature_table_path_complete(root: ET.Element, feature_dir: str | None, sample_name: str) -> int:
    """Update <parameter name='Filename'> current_file/last_file to feature_dir + {sample}_complete_feature_table.csv."""
    if not feature_dir:
        return 0

    feature_dir = _normalize_path_for_batch(feature_dir)
    target_name = f"{sample_name}_complete_feature_table.csv"
    new_path = _join_dir_and_filename(feature_dir, target_name)

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
            lf = ET.SubElement(param, "last_file")
            lf.text = new_path

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
    """Returns (n_import_steps_updated, n_blank_params_updated, n_filename_params_updated)."""
    try:
        tree = ET.parse(template_path)
    except ET.ParseError as e:
        raise RuntimeError(f"Failed to parse XML from '{template_path}': {e}") from e

    root = tree.getroot()

    all_files = [*sample_files, *blank_files]

    # Update import step(s) file names (prefer RawDataImportModule)
    steps = _find_import_batchsteps(root)
    n_import = 0
    did_any = False
    for st in steps:
        if _set_import_file_names_in_step(st, all_files):
            n_import += 1
            did_any = True

    if not did_any:
        # fallback: try global replace (last resort)
        for param in root.iter("parameter"):
            if param.get("name") != "File names":
                continue
            for child in list(param.findall("file")):
                param.remove(child)
            for fp in all_files:
                fe = ET.SubElement(param, "file")
                fe.text = _normalize_path_for_batch(fp)
            n_import += 1

    n_blank = _update_blank_pattern_anywhere(root, blank_pattern)
    n_feat = _update_feature_table_path_complete(root, feature_dir, sample_name)

    _indent_tree(tree)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tree.write(out_path, encoding="UTF-8", xml_declaration=True)

    return n_import, n_blank, n_feat


# -------------------------
# 2) FRACTION feature tables replication batch
# -------------------------
def _fraction_count(rt_start: float, rt_end: float, width: float) -> int:
    if width <= 0:
        raise ValueError("Time per fraction must be > 0.")
    if rt_end <= rt_start:
        raise ValueError("Fractionation end must be greater than start.")
    x = (rt_end - rt_start) / width
    n = int(x + 1e-9)  # floor, guard float noise
    if n <= 0:
        raise ValueError("Computed zero fractions. Check start/end/width.")
    return n


def _detect_template_fraction_index(root: ET.Element) -> tuple[str, int]:
    """Detect template fraction index used in the batch (usually '01')."""
    xml_str = ET.tostring(root, encoding="unicode")
    m = re.search(r"\bfrac_(\d{1,3})\b", xml_str)
    if not m:
        m = re.search(r"_[0-9]{1,3}\b", xml_str)
        if m:
            idx = m.group(0)[1:]
            return idx, len(idx)
        raise ValueError("Could not detect a template fraction index (like frac_01) in the template batch.")
    idx = m.group(1)
    return idx, len(idx)


def _replace_index_everywhere(elem: ET.Element, template_idx: str, new_idx: str) -> None:
    for k, v in list(elem.attrib.items()):
        if v:
            elem.attrib[k] = re.sub(rf"_{re.escape(template_idx)}\b", f"_{new_idx}", v)

    if elem.text:
        elem.text = re.sub(rf"_{re.escape(template_idx)}\b", f"_{new_idx}", elem.text)
    if elem.tail:
        elem.tail = re.sub(rf"_{re.escape(template_idx)}\b", f"_{new_idx}", elem.tail)

    for child in list(elem):
        _replace_index_everywhere(child, template_idx, new_idx)


def _set_crop_input_files(crop_step: ET.Element, mzml_paths: list[str]) -> None:
    """Crop filter uses <parameter name='Raw data files'> with <specific_file> entries of basenames."""
    p = crop_step.find("parameter[@name='Raw data files']")
    if p is None:
        raise ValueError("CropFilter step missing <parameter name='Raw data files'>.")
    for sf in list(p.findall("specific_file")):
        p.remove(sf)
    for fp in mzml_paths:
        sf = ET.SubElement(p, "specific_file")
        sf.text = _basename_from_path(fp)


def _set_crop_retention_time(crop_step: ET.Element, rt_min: float, rt_max: float) -> None:
    scan_filters = crop_step.find("parameter[@name='Scan filters']")
    if scan_filters is None:
        raise ValueError("CropFilter step missing <parameter name='Scan filters'>.")
    rt = scan_filters.find("parameter[@name='Retention time']")
    if rt is None:
        raise ValueError("CropFilter step missing retention time parameter.")
    mn = rt.find("min")
    mx = rt.find("max")
    if mn is None or mx is None:
        raise ValueError("Retention time parameter missing <min>/<max>.")
    mn.text = f"{rt_min:.3f}"
    mx.text = f"{rt_max:.3f}"


def _set_csv_export_path(export_step: ET.Element, feature_dir: str, template_idx: str, new_idx: str) -> None:
    """Update CSV export current_file/last_file; keep exactly one <last_file>."""
    fn = export_step.find("parameter[@name='Filename']")
    if fn is None:
        return
    cf = fn.find("current_file")
    if cf is None:
        return

    old_path = (cf.text or "").strip()
    old_name = _basename_from_path(old_path) or f"frac_{new_idx}.csv"

    new_name = re.sub(rf"_{re.escape(template_idx)}\b", f"_{new_idx}", old_name)

    if new_name == old_name and template_idx != new_idx:
        if not re.search(rf"_{re.escape(new_idx)}\b", old_name):
            stem = Path(old_name).stem
            suffix = Path(old_name).suffix or ".csv"
            new_name = f"{stem}_{new_idx}{suffix}"

    new_full = _join_dir_and_filename(feature_dir, new_name)
    cf.text = new_full

    for lf in list(fn.findall("last_file")):
        fn.remove(lf)
    lf = ET.SubElement(fn, "last_file")
    lf.text = new_full


def replicate_fraction_mzbatch(
    template_path: Path,
    out_path: Path,
    mzml_paths: list[str],
    feature_dir: str,
    rt_start: float,
    rt_end: float,
    width: float,
) -> tuple[int, str]:
    """Returns (n_fractions, template_idx_detected)."""
    tree = ET.parse(template_path)
    root = tree.getroot()

    steps = root.findall("batchstep")
    if len(steps) < 2:
        raise ValueError("Template batch must contain at least 2 batchsteps (import + processing).")

    import_step = copy.deepcopy(steps[0])
    processing_templates = steps[1:]

    template_idx, idx_width = _detect_template_fraction_index(root)

    # Import list once (+ blank pattern if present there)
    if not _set_import_file_names_in_step(import_step, mzml_paths):
        # fallback: global search inside import_step
        for param in import_step.iter("parameter"):
            if param.get("name") == "File names":
                for child in list(param.findall("file")):
                    param.remove(child)
                for fp in mzml_paths:
                    fe = ET.SubElement(param, "file")
                    fe.text = _normalize_path_for_batch(fp)
                break

    # Build new root (copy non-batchstep children)
    new_root = ET.Element(root.tag, dict(root.attrib))
    for child in list(root):
        if child.tag != "batchstep":
            new_root.append(copy.deepcopy(child))

    new_root.append(import_step)

    n = _fraction_count(rt_start, rt_end, width)

    def _is_method(step: ET.Element, endswith: str) -> bool:
        m = step.get("method", "")
        return m.endswith(endswith)

    for i in range(n):
        new_idx = str(i + 1).zfill(idx_width)
        rt_min = rt_start + i * width
        rt_max = rt_min + width

        for tpl in processing_templates:
            step = copy.deepcopy(tpl)

            _replace_index_everywhere(step, template_idx, new_idx)

            # Keep blank pattern constant (update if module exists)

            if _is_method(step, "CropFilterModule"):
                _set_crop_input_files(step, mzml_paths)
                _set_crop_retention_time(step, rt_min, rt_max)

            if _is_method(step, "CSVExportModularModule"):
                _set_csv_export_path(step, feature_dir, template_idx, new_idx)

            new_root.append(step)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_tree = ET.ElementTree(new_root)
    _indent_tree(out_tree)
    out_tree.write(out_path, encoding="UTF-8", xml_declaration=True)

    return n, template_idx

# -------------------------
# Higher-level helpers / config objects
# -------------------------
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


def complete_output_path(out_dir: str | Path, sample_name: str) -> Path:
    """Return the configured complete-batch output path."""
    name = _validate_sample_name(sample_name)
    return Path(out_dir).expanduser() / f"{name}_configured.mzbatch"


def fraction_output_path(out_dir: str | Path, sample_name: str) -> Path:
    """Return the configured fraction-batch output path."""
    name = _validate_sample_name(sample_name)
    return Path(out_dir).expanduser() / f"{name}_fraction_configured.mzbatch"


def run_complete_from_settings(settings: CompleteBatchSettings | dict[str, Any]) -> dict[str, Any]:
    """Validate settings, run the complete-batch setup, and return a summary dict."""
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
    """Validate settings, run the fraction-batch replication, and return a summary dict."""
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


def template_config() -> dict[str, Any]:
    return {
        "sample_name": "sample_name_here",
        "sample_files": [],
        "complete": asdict(CompleteBatchSettings(
            template_path="big_empty_template.mzbatch",
            out_dir="configured_batches",
            sample_files=[],
            blank_files=[],
            blank_pattern=DEFAULT_BLANK_PATTERN,
            feature_dir="complete_feature_table_csv",
            sample_name="sample_name_here",
        )),
        "fraction": asdict(FractionBatchSettings(
            template_path="fraction_empty_template.mzbatch",
            out_dir="configured_batches",
            sample_files=[],
            feature_dir="fraction_feature_tables_csv",
            rt_start=DEFAULT_RT_START,
            rt_end=DEFAULT_RT_END,
            width=DEFAULT_WIDTH,
            sample_name="sample_name_here",
        )),
    }


def load_config(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).expanduser().read_text(encoding="utf-8"))


def save_config(config: dict[str, Any], path: str | Path) -> Path:
    out = Path(path).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
    return out


def _split_args_csv(values: list[str] | None) -> list[str]:
    if not values:
        return []
    out: list[str] = []
    for item in values:
        out.extend([x.strip() for x in str(item).split(",") if x.strip()])
    return out


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Configure or replicate MZmine .mzbatch files.")
    parser.add_argument("--make-template-config", help="Write a JSON config template and exit.")
    parser.add_argument("--config", help="Run from a JSON config saved by the GUI or made manually.")
    parser.add_argument("--mode", choices=["complete", "fraction", "both"], default="both", help="Mode to run when --config is used.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    sub = parser.add_subparsers(dest="command")

    c = sub.add_parser("complete", help="Create a configured complete-feature-table batch.")
    c.add_argument("--template", required=True, help="Complete .mzbatch template.")
    c.add_argument("--sample-name", required=True, help="Sample name used in output filenames.")
    c.add_argument("--sample-files", nargs="+", required=True, help="Sample mzML file(s).")
    c.add_argument("--blank-files", nargs="+", required=True, help="Blank mzML file(s).")
    c.add_argument("--out-dir", required=True, help="Directory where the configured .mzbatch is written.")
    c.add_argument("--feature-dir", default=None, help="Directory for the complete feature-table CSV output path inside the batch.")
    c.add_argument("--blank-pattern", default=DEFAULT_BLANK_PATTERN, help="Blank/control filename pattern, e.g. *blank*.")

    f = sub.add_parser("fraction", help="Replicate a fraction-template batch across RT windows.")
    f.add_argument("--template", required=True, help="Fraction .mzbatch template configured for fraction 01.")
    f.add_argument("--sample-name", required=True, help="Sample name used in output filenames.")
    f.add_argument("--sample-files", nargs="+", required=True, help="Sample mzML file(s).")
    f.add_argument("--out-dir", required=True, help="Directory where the replicated .mzbatch is written.")
    f.add_argument("--feature-dir", required=True, help="Directory for per-fraction CSV feature-table paths inside the batch.")
    f.add_argument("--rt-start", type=float, default=DEFAULT_RT_START, help="Fractionation start time in minutes.")
    f.add_argument("--rt-end", type=float, default=DEFAULT_RT_END, help="Fractionation end time in minutes.")
    f.add_argument("--width", type=float, default=DEFAULT_WIDTH, help="Time per fraction in minutes.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    if args.make_template_config:
        out = save_config(template_config(), args.make_template_config)
        print(f"Template config written to: {out}")
        return 0

    if args.config:
        config = load_config(args.config)
        summaries: list[dict[str, Any]] = []
        sample_name = config.get("sample_name") or config.get("complete", {}).get("sample_name") or config.get("fraction", {}).get("sample_name") or "sample"
        sample_files = config.get("sample_files") or []
        if args.mode in {"complete", "both"} and config.get("complete"):
            complete_cfg = dict(config["complete"])
            complete_cfg.setdefault("sample_name", sample_name)
            complete_cfg.setdefault("sample_files", sample_files)
            summaries.append(run_complete_from_settings(complete_cfg))
        if args.mode in {"fraction", "both"} and config.get("fraction"):
            fraction_cfg = dict(config["fraction"])
            fraction_cfg.setdefault("sample_name", sample_name)
            fraction_cfg.setdefault("sample_files", sample_files)
            summaries.append(run_fraction_from_settings(fraction_cfg))
        print(json.dumps(summaries, indent=2, ensure_ascii=False))
        return 0

    if args.command == "complete":
        summary = run_complete_from_settings(CompleteBatchSettings(
            template_path=args.template,
            out_dir=args.out_dir,
            sample_files=_split_args_csv(args.sample_files),
            blank_files=_split_args_csv(args.blank_files),
            blank_pattern=args.blank_pattern,
            feature_dir=args.feature_dir,
            sample_name=args.sample_name,
        ))
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return 0

    if args.command == "fraction":
        summary = run_fraction_from_settings(FractionBatchSettings(
            template_path=args.template,
            out_dir=args.out_dir,
            sample_files=_split_args_csv(args.sample_files),
            feature_dir=args.feature_dir,
            rt_start=args.rt_start,
            rt_end=args.rt_end,
            width=args.width,
            sample_name=args.sample_name,
        ))
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return 0

    parser.error("Provide --config, --make-template-config, or a subcommand: complete/fraction.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
