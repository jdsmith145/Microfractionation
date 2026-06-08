#!/usr/bin/env python3
"""
General LC-MS feature-table data exploration.

The workflow is intentionally table-driven:
1) Load a wide feature table (features in rows, sample/abundance values in columns).
2) Select abundance columns and optionally rename them using sample metadata.
3) Optionally append annotation columns from a second table by feature ID.
4) Optionally export generic count tables and publication-ready bar plots.

The same functions are used by the command-line interface and by the GUI.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd

LOGGER = logging.getLogger("hrms_exploration_core")


# ---------------------------
# General helpers
# ---------------------------

def slugify(value: Any) -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "_", str(value).strip())
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "item"


def ensure_directory(path: str | Path) -> Path:
    p = Path(path).expanduser()
    p.mkdir(parents=True, exist_ok=True)
    return p


def resolve_path(base_dir: str | Path, path_value: str | Path) -> Path:
    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path
    return Path(base_dir).expanduser() / path


def read_table(path: str | Path, *, sheet_name: str | int | None = None, sep: str | None = None) -> pd.DataFrame:
    """Read CSV/TSV/TXT/Excel into a DataFrame."""
    path = Path(path).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Table not found: {path}")

    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, sheet_name=0 if sheet_name in {None, ""} else sheet_name)
    if suffix in {".csv", ".txt", ".tsv"}:
        if sep is None:
            sep = "\t" if suffix == ".tsv" else ","
        return pd.read_csv(path, sep=sep)
    raise ValueError(f"Unsupported table type for {path}. Use CSV, TSV, TXT, XLSX, or XLS.")


def write_table(df: pd.DataFrame, path: str | Path) -> Path:
    path = Path(path).expanduser()
    ensure_directory(path.parent)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df.to_csv(path, index=False)
    elif suffix in {".xlsx", ".xls"}:
        df.to_excel(path, index=False)
    else:
        raise ValueError(f"Unsupported output table type for {path}. Use CSV or XLSX.")
    return path


def require_columns(df: pd.DataFrame, columns: Iterable[str], table_name: str = "table") -> None:
    missing = [c for c in columns if c and c not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns in {table_name}: {missing}")


def comma_list(value: str | Iterable[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [x.strip() for x in value.split(",") if x.strip()]
    return [str(x).strip() for x in value if str(x).strip()]


def clean_key(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def coerce_numeric_block(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    return df[columns].apply(pd.to_numeric, errors="coerce").fillna(0)


def uniqueify(names: list[str]) -> list[str]:
    counts: dict[str, int] = {}
    out: list[str] = []
    for name in names:
        base = str(name).strip() or "sample"
        counts[base] = counts.get(base, 0) + 1
        if counts[base] == 1:
            out.append(base)
        else:
            out.append(f"{base} (rep{counts[base]})")
    return out


def remove_rep_suffix(name: str) -> str:
    return re.sub(r"\s*\(rep\d+\)$", "", str(name))


# ---------------------------
# Sample-column selection and table preparation
# ---------------------------

def select_columns_by_rule(
    df: pd.DataFrame,
    *,
    method: str = "manual",
    columns: Optional[list[str]] = None,
    prefix: str | None = None,
    suffix: str | None = None,
    regex: str | None = None,
    exclude_columns: Optional[list[str]] = None,
    numeric_only: bool = False,
) -> list[str]:
    """Select sample/area columns from a wide feature table."""
    method = (method or "manual").strip().lower()
    exclude = set(exclude_columns or [])

    if method == "manual":
        selected = list(columns or [])
    elif method == "prefix":
        if not prefix:
            raise ValueError("Sample-column method 'prefix' requires a prefix value.")
        selected = [c for c in df.columns if str(c).startswith(prefix) and c not in exclude]
    elif method == "suffix":
        if not suffix:
            raise ValueError("Sample-column method 'suffix' requires a suffix value.")
        selected = [c for c in df.columns if str(c).endswith(suffix) and c not in exclude]
    elif method == "prefix_suffix":
        if not prefix and not suffix:
            raise ValueError("Sample-column method 'prefix_suffix' requires prefix and/or suffix.")
        selected = [
            c for c in df.columns
            if c not in exclude
            and (not prefix or str(c).startswith(prefix))
            and (not suffix or str(c).endswith(suffix))
        ]
    elif method == "regex":
        if not regex:
            raise ValueError("Sample-column method 'regex' requires a regex value.")
        pat = re.compile(regex)
        selected = [c for c in df.columns if c not in exclude and pat.search(str(c))]
    elif method in {"all_numeric", "numeric"}:
        selected = [c for c in df.columns if c not in exclude]
        numeric_only = True
    else:
        raise ValueError(
            "Unknown sample-column selection method. Use manual, prefix, suffix, "
            "prefix_suffix, regex, or all_numeric."
        )

    if numeric_only:
        selected = [c for c in selected if pd.to_numeric(df[c], errors="coerce").notna().any()]

    selected = [c for c in selected if c in df.columns]
    if not selected:
        raise ValueError("No sample columns were selected. Check prefix/suffix/regex/manual column names.")
    return selected


def make_sample_display_names(
    sample_columns: list[str],
    *,
    metadata_df: pd.DataFrame | None = None,
    metadata_key_column: str | None = None,
    metadata_value_column: str | None = None,
    strip_suffix_before_mapping: str | None = None,
    strip_prefix_before_mapping: str | None = None,
    fallback: str = "original",
) -> tuple[dict[str, str], list[str]]:
    """Build {old_sample_column -> display_name} and list unmapped sample columns."""
    mapping: dict[str, str] = {}
    if metadata_df is not None:
        if not metadata_key_column or not metadata_value_column:
            raise ValueError("Metadata mapping requires key and display columns.")
        require_columns(metadata_df, [metadata_key_column, metadata_value_column], "sample metadata table")
        meta = metadata_df[[metadata_key_column, metadata_value_column]].dropna().copy()
        meta[metadata_key_column] = meta[metadata_key_column].map(clean_key)
        meta[metadata_value_column] = meta[metadata_value_column].map(clean_key)
        mapping = dict(zip(meta[metadata_key_column], meta[metadata_value_column]))

    raw_names: list[str] = []
    unmapped: list[str] = []
    for col in sample_columns:
        key = str(col).strip()
        if strip_suffix_before_mapping and key.endswith(strip_suffix_before_mapping):
            key = key[: -len(strip_suffix_before_mapping)].strip()
        if strip_prefix_before_mapping and key.startswith(strip_prefix_before_mapping):
            key = key[len(strip_prefix_before_mapping) :].strip()

        if mapping:
            value = mapping.get(key)
            if value is None:
                unmapped.append(col)
                if fallback == "key":
                    value = key
                elif fallback == "empty":
                    value = ""
                else:
                    value = col
        else:
            value = key if (strip_suffix_before_mapping or strip_prefix_before_mapping) else col
        raw_names.append(str(value))

    unique_names = uniqueify(raw_names)
    rename_map = dict(zip(sample_columns, unique_names))
    return rename_map, unmapped


def aggregate_duplicate_sample_labels(
    df: pd.DataFrame,
    *,
    id_column: str,
    sample_columns: list[str],
    annotation_columns: Optional[list[str]] = None,
    aggregation: str = "sum",
) -> tuple[pd.DataFrame, list[str]]:
    """Aggregate sample columns with duplicated display labels."""
    annotation_columns = annotation_columns or []
    base_to_cols: dict[str, list[str]] = {}
    for col in sample_columns:
        base_to_cols.setdefault(remove_rep_suffix(col), []).append(col)

    fixed_cols = [id_column] + [c for c in annotation_columns if c in df.columns]
    parts = [df[fixed_cols].copy()]
    out_samples: list[str] = []

    for base, cols in base_to_cols.items():
        if len(cols) == 1:
            series = pd.to_numeric(df[cols[0]], errors="coerce").fillna(0)
        else:
            block = coerce_numeric_block(df, cols)
            if aggregation == "mean":
                series = block.mean(axis=1)
            elif aggregation == "max":
                series = block.max(axis=1)
            else:
                series = block.sum(axis=1)
        parts.append(series.to_frame(base))
        out_samples.append(base)

    return pd.concat(parts, axis=1), out_samples


def prepare_feature_sample_table(config: dict[str, Any], *, base_dir: str | Path) -> tuple[pd.DataFrame, list[str], dict[str, Any]]:
    """Load a wide feature table, select/rename sample columns, and keep selected annotation columns."""
    feature_cfg = config["feature_table"]
    feature_path = resolve_path(base_dir, feature_cfg["path"])
    df_raw = read_table(feature_path, sheet_name=feature_cfg.get("sheet_name"), sep=feature_cfg.get("sep"))
    df = df_raw.copy()
    id_column = feature_cfg.get("id_column", "row ID")
    require_columns(df, [id_column], "feature table")

    sample_select = feature_cfg.get("sample_selection", {})
    exclude_columns = comma_list(sample_select.get("exclude_columns"))
    if sample_select.get("method", "manual") != "manual" and id_column not in exclude_columns:
        exclude_columns.append(id_column)

    sample_columns = select_columns_by_rule(
        df,
        method=sample_select.get("method", "manual"),
        columns=comma_list(sample_select.get("columns")),
        prefix=sample_select.get("prefix"),
        suffix=sample_select.get("suffix"),
        regex=sample_select.get("regex"),
        exclude_columns=exclude_columns,
        numeric_only=bool(sample_select.get("numeric_only", False)),
    )

    keep_existing_columns = comma_list(feature_cfg.get("columns_to_keep"))
    analysis_annotation = (config.get("analysis") or {}).get("annotation_column")
    if analysis_annotation and analysis_annotation in df.columns and analysis_annotation not in keep_existing_columns:
        keep_existing_columns.append(analysis_annotation)
    keep_existing_columns = [
        c for c in keep_existing_columns
        if c in df.columns and c != id_column and c not in sample_columns
    ]

    metadata_cfg = config.get("sample_metadata") or {}
    metadata_df = None
    if metadata_cfg.get("enabled", True) and metadata_cfg.get("path"):
        metadata_df = read_table(
            resolve_path(base_dir, metadata_cfg["path"]),
            sheet_name=metadata_cfg.get("sheet_name"),
            sep=metadata_cfg.get("sep"),
        )

    rename_map, unmapped = make_sample_display_names(
        sample_columns,
        metadata_df=metadata_df,
        metadata_key_column=metadata_cfg.get("key_column"),
        metadata_value_column=metadata_cfg.get("value_column"),
        strip_suffix_before_mapping=metadata_cfg.get("strip_suffix_before_mapping"),
        strip_prefix_before_mapping=metadata_cfg.get("strip_prefix_before_mapping"),
        fallback=metadata_cfg.get("fallback", "original"),
    )

    keep = [id_column] + keep_existing_columns + sample_columns
    df_samples = df[keep].copy().rename(columns=rename_map)
    renamed_sample_columns = [rename_map[c] for c in sample_columns]

    if bool(metadata_cfg.get("aggregate_duplicate_labels", False)):
        df_samples, renamed_sample_columns = aggregate_duplicate_sample_labels(
            df_samples,
            id_column=id_column,
            sample_columns=renamed_sample_columns,
            annotation_columns=keep_existing_columns,
            aggregation=metadata_cfg.get("aggregation", "sum"),
        )

    for col in renamed_sample_columns:
        df_samples[col] = pd.to_numeric(df_samples[col], errors="coerce").fillna(0)

    info = {
        "input_feature_table": str(feature_path),
        "table_layout": "wide",
        "id_column": id_column,
        "n_rows_input": int(len(df_raw)),
        "n_rows_prepared": int(len(df_samples)),
        "sample_columns_original": sample_columns,
        "sample_columns_renamed": renamed_sample_columns,
        "existing_annotation_columns_kept": keep_existing_columns,
        "unmapped_sample_columns": unmapped,
    }
    LOGGER.info("Prepared feature/sample table in memory: %s rows, %s sample columns.", len(df_samples), len(renamed_sample_columns))
    return df_samples, renamed_sample_columns, info


# ---------------------------
# Annotation merging
# ---------------------------

def parse_sirius_v5_id(value: Any) -> Any:
    """Extract final integer from SIRIUS v5-style IDs such as x_out_y_123."""
    text = clean_key(value)
    if not text:
        return np.nan
    nums = re.findall(r"\d+", text)
    if not nums:
        return np.nan
    return int(nums[-1])


def normalize_merge_key(series: pd.Series, parser: str = "none") -> pd.Series:
    parser = (parser or "none").lower()
    if parser in {"none", "exact", "string"}:
        return series.map(clean_key)
    if parser in {"numeric", "integer", "int"}:
        return pd.to_numeric(series, errors="coerce").astype("Int64").astype(str).replace("<NA>", "")
    if parser in {"sirius_v5_id", "sirius_id_last_integer", "last_integer"}:
        return series.map(parse_sirius_v5_id).astype("Int64").astype(str).replace("<NA>", "")
    raise ValueError("Unknown merge-key parser. Use none, numeric, or sirius_v5_id.")


def append_annotation_columns(
    base_df: pd.DataFrame,
    annotation_df: pd.DataFrame,
    *,
    feature_id_column: str,
    annotation_id_column: str,
    columns_to_add: list[str],
    feature_id_parser: str = "numeric",
    annotation_id_parser: str = "numeric",
    missing_fill: str | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    require_columns(base_df, [feature_id_column], "prepared feature table")
    require_columns(annotation_df, [annotation_id_column] + columns_to_add, "annotation table")

    left = base_df.copy()
    right = annotation_df[[annotation_id_column] + columns_to_add].copy()
    right = right.drop_duplicates(subset=[annotation_id_column])

    left["__merge_key__"] = normalize_merge_key(left[feature_id_column], feature_id_parser)
    right["__merge_key__"] = normalize_merge_key(right[annotation_id_column], annotation_id_parser)

    merged = left.merge(right.drop(columns=[annotation_id_column]), on="__merge_key__", how="left")
    matched_mask = merged[columns_to_add].notna().any(axis=1)

    if missing_fill is not None:
        for col in columns_to_add:
            merged[col] = merged[col].astype(object)
            merged.loc[merged[col].isna(), col] = missing_fill

    merged = merged.drop(columns=["__merge_key__"])

    base_cols = [c for c in base_df.columns]
    insert_idx = base_cols.index(feature_id_column) + 1
    final_cols = base_cols[:insert_idx] + columns_to_add + base_cols[insert_idx:]
    final_cols = [c for c in final_cols if c in merged.columns]
    merged = merged[final_cols]

    stats = {
        "annotation_rows": int(len(annotation_df)),
        "features_before_merge": int(len(base_df)),
        "features_matched_to_annotation": int(matched_mask.sum()),
        "features_unmatched_to_annotation": int((~matched_mask).sum()),
        "columns_added": columns_to_add,
    }
    return merged, stats


def maybe_merge_annotations(
    sample_df: pd.DataFrame,
    config: dict[str, Any],
    *,
    base_dir: str | Path,
    id_column: str,
) -> tuple[pd.DataFrame, list[str], dict[str, Any]]:
    annotation_cfg = config.get("annotation_table") or {}
    if not annotation_cfg.get("path"):
        return sample_df.copy(), [], {"used": False}

    path = resolve_path(base_dir, annotation_cfg["path"])
    annot_df = read_table(path, sheet_name=annotation_cfg.get("sheet_name"), sep=annotation_cfg.get("sep"))
    columns_to_add = comma_list(annotation_cfg.get("columns_to_add"))
    if not columns_to_add:
        raise ValueError("Annotation table was provided, but no columns_to_add were selected.")

    merged, stats = append_annotation_columns(
        sample_df,
        annot_df,
        feature_id_column=annotation_cfg.get("feature_id_column", id_column),
        annotation_id_column=annotation_cfg.get("annotation_id_column", "mappingFeatureId"),
        columns_to_add=columns_to_add,
        feature_id_parser=annotation_cfg.get("feature_id_parser", "numeric"),
        annotation_id_parser=annotation_cfg.get("annotation_id_parser", "numeric"),
        missing_fill=annotation_cfg.get("missing_fill"),
    )
    stats.update({"used": True, "annotation_table": str(path)})
    LOGGER.info("Merged annotation columns in memory: %s/%s features matched.", stats["features_matched_to_annotation"], stats["features_before_merge"])
    return merged, columns_to_add, stats


# ---------------------------
# Counting and plotting
# ---------------------------

def set_matplotlib_style(font_family: str = "Arial", svg_text: bool = True) -> None:
    import matplotlib as mpl

    mpl.rcParams["font.family"] = font_family
    if svg_text:
        mpl.rcParams["svg.fonttype"] = "none"


def detect_sample_columns_from_final_table(
    df: pd.DataFrame,
    *,
    id_column: str,
    annotation_columns: Optional[list[str]] = None,
    explicit_samples: Optional[list[str]] = None,
) -> list[str]:
    if explicit_samples:
        require_columns(df, explicit_samples, "final feature table")
        return explicit_samples
    annotation_columns = annotation_columns or []
    exclude = {id_column, *annotation_columns}
    samples = [
        c for c in df.columns
        if c not in exclude and pd.to_numeric(df[c], errors="coerce").notna().any()
    ]
    if not samples:
        raise ValueError("Could not detect numeric sample columns in the final table.")
    return samples


def make_annotation_mask(
    df: pd.DataFrame,
    *,
    annotation_column: str,
    terms: list[str],
    match_mode: str = "exact",
    case_sensitive: bool = False,
) -> pd.Series:
    require_columns(df, [annotation_column], "final feature table")
    if not terms:
        return pd.Series(True, index=df.index)

    text = df[annotation_column].astype(str)
    mode = (match_mode or "exact").lower()
    if not case_sensitive:
        text = text.str.lower()
        terms_cmp = [t.lower() for t in terms]
    else:
        terms_cmp = terms

    mask = pd.Series(False, index=df.index)
    for term in terms_cmp:
        if mode == "exact":
            mask |= text == term
        elif mode == "contains":
            mask |= text.str.contains(re.escape(term), na=False)
        elif mode == "regex":
            mask |= text.str.contains(term, regex=True, na=False)
        else:
            raise ValueError("Unknown match_mode. Use exact, contains, or regex.")
    return mask


def counts_by_annotation_per_sample(
    df: pd.DataFrame,
    *,
    annotation_column: str,
    sample_columns: list[str],
    threshold: float = 100000,
    drop_missing_annotation: bool = True,
) -> pd.DataFrame:
    require_columns(df, [annotation_column] + sample_columns, "final feature table")
    work = df.copy()
    if drop_missing_annotation:
        work = work[work[annotation_column].notna() & (work[annotation_column].astype(str).str.strip() != "")]
    records: list[dict[str, Any]] = []
    for annotation_value, group in work.groupby(annotation_column, dropna=not drop_missing_annotation):
        row: dict[str, Any] = {annotation_column: annotation_value}
        for sample in sample_columns:
            row[sample] = int((pd.to_numeric(group[sample], errors="coerce").fillna(0) > threshold).sum())
        records.append(row)
    out = pd.DataFrame(records)
    if out.empty:
        return pd.DataFrame(columns=[annotation_column] + sample_columns)
    return out[[annotation_column] + sample_columns].sort_values(annotation_column).reset_index(drop=True)


def selected_terms_across_samples(
    df: pd.DataFrame,
    *,
    annotation_column: str,
    terms: list[str],
    sample_columns: list[str],
    threshold: float = 100000,
    match_mode: str = "exact",
    case_sensitive: bool = False,
    top_n: int | None = 15,
) -> pd.DataFrame:
    mask = make_annotation_mask(
        df,
        annotation_column=annotation_column,
        terms=terms,
        match_mode=match_mode,
        case_sensitive=case_sensitive,
    )
    work = df.loc[mask].copy()
    records = []
    label = " + ".join(terms) if terms else f"all_{annotation_column}"
    for sample in sample_columns:
        count = int((pd.to_numeric(work[sample], errors="coerce").fillna(0) > threshold).sum())
        records.append({"sample": sample, "count": count, "selected_terms": label})
    out = pd.DataFrame(records).sort_values("count", ascending=False).reset_index(drop=True)
    if top_n and top_n > 0:
        out = out.head(int(top_n))
    return out


def top_annotations_for_sample(
    df: pd.DataFrame,
    *,
    sample_column: str,
    annotation_column: str,
    threshold: float = 100000,
    top_n: int | None = 15,
    drop_missing_annotation: bool = True,
) -> pd.DataFrame:
    require_columns(df, [sample_column, annotation_column], "final feature table")
    mask = pd.to_numeric(df[sample_column], errors="coerce").fillna(0) > threshold
    work = df.loc[mask].copy()
    if drop_missing_annotation:
        work = work[work[annotation_column].notna() & (work[annotation_column].astype(str).str.strip() != "")]
    counts = work[annotation_column].value_counts(dropna=not drop_missing_annotation)
    if top_n and top_n > 0:
        counts = counts.head(int(top_n))
    return counts.rename_axis(annotation_column).reset_index(name="count")


def plot_bar(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    title: str,
    xlabel: str,
    ylabel: str,
    output_path: str | Path,
    figsize: tuple[float, float] = (10, 5),
    rotation: int = 45,
    preview_png_path: str | Path | None = None,
) -> Path:
    import matplotlib.pyplot as plt

    out = Path(output_path)
    ensure_directory(out.parent)
    fig, ax = plt.subplots(figsize=figsize)
    ax.bar(df[x].astype(str), df[y])
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.tick_params(axis="x", labelrotation=rotation)
    for label in ax.get_xticklabels():
        label.set_ha("right" if rotation else "center")
    fig.tight_layout()
    fig.savefig(out, bbox_inches="tight", dpi=300)
    if preview_png_path is not None:
        preview_out = Path(preview_png_path)
        ensure_directory(preview_out.parent)
        if preview_out.resolve() != out.resolve():
            fig.savefig(preview_out, bbox_inches="tight", dpi=160)
    plt.close(fig)
    return out


def run_analysis_outputs(
    df_final: pd.DataFrame,
    config: dict[str, Any],
    *,
    output_dir: Path,
    id_column: str,
    sample_columns: list[str],
    annotation_columns: list[str],
) -> dict[str, Any]:
    analysis_cfg = config.get("analysis") or {}
    if not analysis_cfg.get("enabled", True):
        return {"enabled": False}

    annotation_column = analysis_cfg.get("annotation_column")
    if not annotation_column:
        if annotation_columns:
            annotation_column = annotation_columns[-1]
        else:
            raise ValueError("No analysis.annotation_column was selected and no annotation columns are available.")
    require_columns(df_final, [annotation_column], "final feature table")

    threshold = float(analysis_cfg.get("threshold", 100000))
    top_n = int(analysis_cfg.get("top_n", 15))
    terms = comma_list(analysis_cfg.get("terms"))
    match_mode = analysis_cfg.get("match_mode", "exact")
    case_sensitive = bool(analysis_cfg.get("case_sensitive", False))
    selected_samples = comma_list(analysis_cfg.get("selected_samples"))
    plot_format = analysis_cfg.get("plot_format", "svg").lstrip(".") or "svg"

    actual_sample_columns = detect_sample_columns_from_final_table(
        df_final,
        id_column=id_column,
        annotation_columns=annotation_columns,
        explicit_samples=selected_samples or sample_columns,
    )

    font_family = analysis_cfg.get("font_family", "Arial")
    set_matplotlib_style(font_family=font_family, svg_text=bool(analysis_cfg.get("svg_text", True)))

    outputs: dict[str, Any] = {
        "enabled": True,
        "annotation_column": annotation_column,
        "threshold": threshold,
        "sample_columns": actual_sample_columns,
        "files": {},
    }

    if bool(analysis_cfg.get("make_counts_table", True)):
        counts = counts_by_annotation_per_sample(
            df_final,
            annotation_column=annotation_column,
            sample_columns=actual_sample_columns,
            threshold=threshold,
            drop_missing_annotation=bool(analysis_cfg.get("drop_missing_annotation", True)),
        )
        counts_path = write_table(counts, output_dir / f"03_counts_by_{slugify(annotation_column)}_threshold_{threshold:g}.csv")
        outputs["files"]["counts_by_annotation_per_sample"] = str(counts_path)
        LOGGER.info("Wrote counts table: %s", counts_path)

    if bool(analysis_cfg.get("make_selected_terms_plot", True)) and terms:
        term_counts = selected_terms_across_samples(
            df_final,
            annotation_column=annotation_column,
            terms=terms,
            sample_columns=actual_sample_columns,
            threshold=threshold,
            match_mode=match_mode,
            case_sensitive=case_sensitive,
            top_n=top_n,
        )
        term_slug = slugify("_".join(terms))
        term_csv = write_table(term_counts, output_dir / f"04_{term_slug}_across_samples_top{top_n}.csv")
        outputs["files"]["selected_terms_across_samples_table"] = str(term_csv)
        term_plot_path = output_dir / f"04_{term_slug}_across_samples_top{top_n}.{plot_format}"
        term_preview_path = term_plot_path if term_plot_path.suffix.lower() == ".png" else term_plot_path.with_suffix(".preview.png")
        term_plot = plot_bar(
            term_counts,
            x="sample",
            y="count",
            title=analysis_cfg.get("selected_terms_plot_title", f"Top {top_n} samples by selected feature count"),
            xlabel=analysis_cfg.get("sample_axis_label", "Sample"),
            ylabel=analysis_cfg.get("selected_terms_y_label", f"Feature count > {threshold:g}"),
            output_path=term_plot_path,
            figsize=tuple(analysis_cfg.get("selected_terms_figsize", [10, 5])),
            rotation=int(analysis_cfg.get("selected_terms_rotation", 45)),
            preview_png_path=term_preview_path,
        )
        outputs["files"]["selected_terms_across_samples_plot"] = str(term_plot)
        outputs["files"]["selected_terms_across_samples_preview_png"] = str(term_preview_path)
        LOGGER.info("Wrote selected-term plot: %s", term_plot)

    samples_for_top = comma_list(analysis_cfg.get("samples_for_top_annotations"))
    if bool(analysis_cfg.get("make_top_annotations_plot", True)) and samples_for_top:
        top_outputs: dict[str, dict[str, str]] = {}
        for sample in samples_for_top:
            require_columns(df_final, [sample], "final feature table")
            top_counts = top_annotations_for_sample(
                df_final,
                sample_column=sample,
                annotation_column=annotation_column,
                threshold=threshold,
                top_n=top_n,
                drop_missing_annotation=bool(analysis_cfg.get("drop_missing_annotation", True)),
            )
            sample_slug = slugify(sample)
            top_csv = write_table(top_counts, output_dir / f"05_top_{top_n}_{slugify(annotation_column)}_{sample_slug}.csv")
            top_plot_path = output_dir / f"05_top_{top_n}_{slugify(annotation_column)}_{sample_slug}.{plot_format}"
            top_preview_path = top_plot_path if top_plot_path.suffix.lower() == ".png" else top_plot_path.with_suffix(".preview.png")
            top_plot = plot_bar(
                top_counts,
                x=annotation_column,
                y="count",
                title=analysis_cfg.get("top_annotations_plot_title", f"Top {top_n} {annotation_column} categories for {sample}"),
                xlabel=annotation_column,
                ylabel=analysis_cfg.get("top_annotations_y_label", f"Feature count > {threshold:g}"),
                output_path=top_plot_path,
                figsize=tuple(analysis_cfg.get("top_annotations_figsize", [10, 6])),
                rotation=int(analysis_cfg.get("top_annotations_rotation", 90)),
                preview_png_path=top_preview_path,
            )
            top_outputs[sample] = {"table": str(top_csv), "plot": str(top_plot), "preview_png": str(top_preview_path)}
            LOGGER.info("Wrote top-annotation output for %s.", sample)
        outputs["files"]["top_annotations_for_samples"] = top_outputs

    return outputs


# ---------------------------
# Pipeline
# ---------------------------

def run_pipeline(config: dict[str, Any]) -> dict[str, Any]:
    base_dir = Path(config.get("base_dir", Path.cwd())).expanduser()
    output_dir = ensure_directory(resolve_path(base_dir, config.get("output_dir", "Outputs")))
    run_steps = config.get("run_steps") or {}
    write_prepared_table = bool(run_steps.get("write_prepared_table", True))
    write_annotation_table = bool(run_steps.get("write_annotation_table", True))
    run_analysis = bool(run_steps.get("run_analysis", True))

    analysis_cfg = config.get("analysis") or {}
    final_feature_table_path = analysis_cfg.get("final_feature_table_path") or config.get("final_feature_table_path")
    direct_analysis_mode = run_analysis and (not write_prepared_table) and (not write_annotation_table) and bool(final_feature_table_path)

    files: dict[str, Any] = {}

    if direct_analysis_mode:
        final_path = resolve_path(base_dir, final_feature_table_path)
        df_final = read_table(final_path)
        feature_cfg = config.get("feature_table") or {}
        id_column = feature_cfg.get("id_column", "row ID")
        if id_column not in df_final.columns:
            raise KeyError(
                f"The selected final feature table does not contain the ID column '{id_column}'. "
                "Adjust the Feature ID column or choose the correct final table."
            )
        sample_columns = comma_list(analysis_cfg.get("selected_samples"))
        annotation_columns = list(dict.fromkeys([
            *comma_list(feature_cfg.get("columns_to_keep")),
            *comma_list((config.get("annotation_table") or {}).get("columns_to_add")),
            analysis_cfg.get("annotation_column", ""),
        ]))
        annotation_columns = [c for c in annotation_columns if c and c in df_final.columns]
        prep_info = {
            "used_existing_final_feature_table": True,
            "input_final_feature_table": str(final_path),
            "table_layout": "wide",
            "id_column": id_column,
            "n_rows_prepared": int(len(df_final)),
            "sample_columns_renamed": sample_columns,
            "existing_annotation_columns_kept": annotation_columns,
        }
        annotation_info = {"used": False, "skipped_by_user": True, "used_existing_final_feature_table": True}
        files["final_feature_table_input"] = str(final_path)
        LOGGER.info("Using existing final feature table for analysis: %s", final_path)
    else:
        df_samples, sample_columns, prep_info = prepare_feature_sample_table(config, base_dir=base_dir)
        id_column = prep_info["id_column"]

        if write_prepared_table:
            samples_path = write_table(df_samples, output_dir / "01_feature_table_samples_prepared.csv")
            files["prepared_feature_table"] = str(samples_path)
            LOGGER.info("Wrote prepared feature table: %s", samples_path)

        if write_annotation_table:
            df_final, annotation_columns, annotation_info = maybe_merge_annotations(
                df_samples,
                config,
                base_dir=base_dir,
                id_column=id_column,
            )
            final_path = write_table(df_final, output_dir / "02_feature_table_with_annotations.csv")
            files["final_feature_table"] = str(final_path)
            LOGGER.info("Wrote final feature table: %s", final_path)
        else:
            df_final = df_samples
            annotation_columns = []
            annotation_info = {"used": False, "skipped_by_user": True}
            LOGGER.info("Annotation/final-table output step skipped by user selection.")

        existing_annotation_columns = prep_info.get("existing_annotation_columns_kept", [])
        annotation_columns = list(dict.fromkeys([*existing_annotation_columns, *annotation_columns]))

    if run_analysis:
        analysis_info = run_analysis_outputs(
            df_final,
            config,
            output_dir=output_dir,
            id_column=id_column,
            sample_columns=sample_columns,
            annotation_columns=annotation_columns,
        )
    else:
        analysis_info = {"enabled": False, "skipped_by_user": True}
        LOGGER.info("Analysis step skipped by user selection.")

    summary = {
        "output_dir": str(output_dir),
        "run_steps": {
            "write_prepared_table": write_prepared_table,
            "write_annotation_table": write_annotation_table,
            "run_analysis": run_analysis,
            "direct_analysis_mode": direct_analysis_mode,
        },
        "files": files,
        "preparation": prep_info,
        "annotation_merge": annotation_info,
        "analysis": analysis_info,
    }
    summary_path = output_dir / "hrms_exploration_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    summary["summary_json"] = str(summary_path)
    LOGGER.info("Workflow summary written to: %s", summary_path)
    return summary

# ---------------------------
# Config template and CLI
# ---------------------------

def template_config() -> dict[str, Any]:
    return {
        "base_dir": ".",
        "output_dir": "Outputs_hrms_exploration",
        "run_steps": {
            "write_prepared_table": True,
            "write_annotation_table": True,
            "run_analysis": True,
        },
        "feature_table": {
            "path": "data/feature_table.csv",
            "id_column": "row ID",
            "columns_to_keep": [],
            "sample_selection": {
                "method": "prefix_suffix",
                "prefix": "",
                "suffix": ".mzML Peak area",
                "columns": [],
                "exclude_columns": ["row ID"],
                "numeric_only": True,
            },
        },
        "sample_metadata": {
            "enabled": True,
            "path": "",
            "key_column": "Filename",
            "value_column": "Sample name",
            "strip_suffix_before_mapping": ".mzML Peak area",
            "strip_prefix_before_mapping": "",
            "fallback": "original",
            "aggregate_duplicate_labels": False,
            "aggregation": "sum",
        },
        "annotation_table": {
            "path": "",
            "feature_id_column": "row ID",
            "annotation_id_column": "mappingFeatureId",
            "feature_id_parser": "numeric",
            "annotation_id_parser": "numeric",
            "columns_to_add": ["molecularFormula", "NPC#pathway", "NPC#superclass", "NPC#class"],
            "missing_fill": None,
        },
        "analysis": {
            "enabled": True,
            "final_feature_table_path": "",
            "annotation_column": "NPC#class",
            "threshold": 100000,
            "top_n": 15,
            "terms": ["Isoquinoline alkaloids"],
            "match_mode": "exact",
            "case_sensitive": False,
            "selected_samples": [],
            "samples_for_top_annotations": [],
            "make_counts_table": True,
            "make_selected_terms_plot": True,
            "make_top_annotations_plot": True,
            "drop_missing_annotation": True,
            "plot_format": "svg",
            "font_family": "Arial",
            "svg_text": True,
            "selected_terms_figsize": [10, 5],
            "top_annotations_figsize": [10, 6],
            "selected_terms_rotation": 45,
            "top_annotations_rotation": 90,
        },
    }


def build_config_from_args(args: argparse.Namespace) -> dict[str, Any]:
    cfg = template_config()
    cfg["base_dir"] = args.base_dir or "."
    cfg["output_dir"] = args.output_dir or cfg["output_dir"]
    cfg["run_steps"] = {
        "write_prepared_table": not args.skip_prepared_table,
        "write_annotation_table": not args.skip_annotation_table,
        "run_analysis": not args.skip_analysis,
    }
    cfg["feature_table"]["path"] = args.feature_table
    cfg["feature_table"]["id_column"] = args.id_column

    if args.sample_columns:
        cfg["feature_table"]["sample_selection"] = {
            "method": "manual",
            "columns": comma_list(args.sample_columns),
            "exclude_columns": [args.id_column],
            "numeric_only": True,
        }
    else:
        cfg["feature_table"]["sample_selection"] = {
            "method": "prefix_suffix",
            "prefix": args.sample_prefix,
            "suffix": args.sample_suffix,
            "numeric_only": True,
            "exclude_columns": [args.id_column],
        }

    cfg["feature_table"]["columns_to_keep"] = comma_list(args.columns_to_keep)

    if args.metadata:
        cfg["sample_metadata"]["path"] = args.metadata
        cfg["sample_metadata"]["key_column"] = args.metadata_key_column
        cfg["sample_metadata"]["value_column"] = args.metadata_value_column
    else:
        cfg["sample_metadata"] = {}

    if args.annotation_table:
        cfg["annotation_table"]["path"] = args.annotation_table
        cfg["annotation_table"]["annotation_id_column"] = args.annotation_id_column
        cfg["annotation_table"]["columns_to_add"] = comma_list(args.columns_to_add)
    else:
        cfg["annotation_table"] = {}

    cfg["analysis"]["annotation_column"] = args.annotation_column
    cfg["analysis"]["terms"] = comma_list(args.terms)
    cfg["analysis"]["samples_for_top_annotations"] = comma_list(args.samples_for_top_annotations)
    cfg["analysis"]["threshold"] = args.threshold
    cfg["analysis"]["top_n"] = args.top_n
    cfg["analysis"]["match_mode"] = args.match_mode
    return cfg


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="General LC-MS feature-table data exploration.")
    parser.add_argument("--config", help="Path to JSON config. Recommended for reproducible runs.")
    parser.add_argument("--make-template", help="Write a template JSON config and exit.")

    parser.add_argument("--feature-table", help="Main wide feature table CSV/XLSX.")
    parser.add_argument("--base-dir", default=".", help="Base directory for relative paths.")
    parser.add_argument("--output-dir", default="Outputs_hrms_exploration", help="Output directory.")
    parser.add_argument("--id-column", default="row ID", help="Feature ID column in the feature table.")
    parser.add_argument("--columns-to-keep", default="", help="Comma-separated existing annotation columns to preserve from the feature table.")
    parser.add_argument("--sample-prefix", default="", help="Prefix for automatic sample-column detection.")
    parser.add_argument("--sample-suffix", default=".mzML Peak area", help="Suffix for automatic sample-column detection.")
    parser.add_argument("--sample-columns", help="Comma-separated sample columns. Overrides prefix/suffix detection.")

    parser.add_argument("--metadata", help="Optional sample metadata table for renaming sample columns.")
    parser.add_argument("--metadata-key-column", default="Filename", help="Metadata key column.")
    parser.add_argument("--metadata-value-column", default="Sample name", help="Metadata display-name column.")

    parser.add_argument("--annotation-table", help="Optional annotation table.")
    parser.add_argument("--annotation-id-column", default="mappingFeatureId", help="ID column in annotation table.")
    parser.add_argument(
        "--columns-to-add",
        default="molecularFormula,NPC#pathway,NPC#superclass,NPC#class",
        help="Comma-separated columns to append from annotation table.",
    )

    parser.add_argument("--annotation-column", default="NPC#class", help="Column used for counting/plotting categories.")
    parser.add_argument("--terms", default="Isoquinoline alkaloids", help="Comma-separated terms to plot across samples.")
    parser.add_argument("--match-mode", default="exact", choices=["exact", "contains", "regex"], help="How terms are matched.")
    parser.add_argument("--samples-for-top-annotations", default="", help="Comma-separated samples for top-category plots.")
    parser.add_argument("--threshold", type=float, default=100000, help="Area/intensity threshold.")
    parser.add_argument("--top-n", type=int, default=15, help="Number of bars/categories to keep.")
    parser.add_argument("--skip-prepared-table", action="store_true", help="Do not write 01_feature_table_samples_prepared.csv.")
    parser.add_argument("--skip-annotation-table", action="store_true", help="Do not write 02_feature_table_with_annotations.csv and do not merge annotation table.")
    parser.add_argument("--skip-analysis", action="store_true", help="Do not create analysis CSV/figure outputs.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    if args.make_template:
        out = Path(args.make_template)
        ensure_directory(out.parent if str(out.parent) else ".")
        out.write_text(json.dumps(template_config(), indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Template config written to: {out}")
        return 0

    if args.config:
        config_path = Path(args.config)
        config = json.loads(config_path.read_text(encoding="utf-8"))
        config.setdefault("base_dir", str(config_path.parent))
    else:
        if not args.feature_table:
            parser.error("Provide --config, --make-template, or --feature-table.")
        config = build_config_from_args(args)

    summary = run_pipeline(config)
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
