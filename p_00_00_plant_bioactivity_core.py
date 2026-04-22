#!/usr/bin/env python3
"""Plant bioactivity core script.

Reusable core for plotting replicate-based bioactivity data from a single CSV/XLSX
input table. It can normalize values to one or more control rows, optionally assign
activity classes using a user-defined threshold, export a result table, and save a
PNG/SVG plot.

Examples
--------
python p_00_00_plant_bioactivity_core.py \
    --input data/plant_extract_fluorescence_graph.xlsx \
    --sample-column Species \
    --replicate-columns fluorescence_plate1 fluorescence_plate2 \
    --control-row-indices 1 2 \
    --exclude-control-from-plot \
    --threshold 80 \
    --threshold-mode ge

python p_00_00_plant_bioactivity_core.py \
    --input data/measurements.csv \
    --sample-column sample_name \
    --replicate-columns rep1 rep2 rep3 \
    --control-sample-names control_A control_B \
    --order-file data/sample_order.xlsx \
    --order-column sample_name \
    --output-prefix output/my_bioactivity_plot \
    --export-table --export-png --export-svg
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Sequence

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Patch

mpl.rcParams["font.family"] = "Arial"
mpl.rcParams["svg.fonttype"] = "none"


DEFAULT_COLORS = {
    "active": "#00b25d",
    "inactive": "#7a7a7a",
    "control": "#4053d3",
    "default": "#7a7a7a",
}

DEFAULT_PLOT_STYLE = {
    "font_family": "Arial",
    "title_size": 16,
    "axis_label_size": 20,
    "xtick_label_size": 8,
    "ytick_label_size": 16,
    "legend_size": 12,
}


class BioactivityError(Exception):
    """Raised for user-facing data/configuration errors."""


def read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise BioactivityError(f"Unsupported file type: {path.suffix}. Use CSV or Excel.")


def clean_text_column(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip()


def ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> list[str]:
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise BioactivityError(f"Missing required columns: {missing}")
    return list(columns)


def resolve_path(path_str: str | None, base_dir: Path) -> Path | None:
    if not path_str:
        return None
    path = Path(path_str).expanduser()
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return path


def get_default_output_prefix(input_path: Path, base_dir: Path) -> Path:
    output_dir = base_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / input_path.stem


def parse_row_indices(values: Sequence[int] | None, n_rows: int) -> pd.Series:
    mask = pd.Series(False, index=range(n_rows), dtype=bool)
    if not values:
        return mask

    for idx in values:
        if idx < 1 or idx > n_rows:
            raise BioactivityError(
                f"Control row index {idx} is out of range. Use 1-based row numbers from 1 to {n_rows}."
            )
        mask.iloc[idx - 1] = True
    return mask


def split_csv_like_text(value: str | None) -> list[str]:
    if not value:
        return []
    items = [item.strip() for item in value.split(",")]
    return [item for item in items if item]


def select_control_mask(
    df: pd.DataFrame,
    sample_column: str,
    control_row_indices: Sequence[int] | None = None,
    control_sample_names: Sequence[str] | None = None,
    control_column: str | None = None,
    control_value: str | None = None,
    control_query: str | None = None,
) -> pd.Series:
    mask = pd.Series(False, index=df.index, dtype=bool)
    selection_modes_used = 0

    if control_row_indices:
        row_mask = parse_row_indices(control_row_indices, len(df))
        row_mask.index = df.index
        mask |= row_mask
        selection_modes_used += 1

    if control_sample_names:
        wanted = {item.strip() for item in control_sample_names if item.strip()}
        sample_mask = clean_text_column(df[sample_column]).isin(wanted)
        mask |= sample_mask.fillna(False)
        selection_modes_used += 1

    if control_query:
        try:
            query_mask = df.eval(control_query)
        except Exception:
            try:
                matched = df.query(control_query).index
                query_mask = df.index.isin(matched)
            except Exception as exc:  # pragma: no cover
                raise BioactivityError(f"Invalid control query: {exc}") from exc
        if not isinstance(query_mask, (pd.Series, np.ndarray, list)):
            raise BioactivityError("Control query did not produce a row-wise mask.")
        query_mask = pd.Series(query_mask, index=df.index, dtype=bool)
        mask |= query_mask.fillna(False)
        selection_modes_used += 1

    if control_column is not None and control_value is not None:
        ensure_columns(df, [control_column])
        column_mask = df[control_column].astype("string") == str(control_value)
        mask |= column_mask.fillna(False)
        selection_modes_used += 1

    if selection_modes_used == 0:
        return mask

    if not mask.any():
        raise BioactivityError("No control rows matched the provided control selection.")
    return mask


def compute_control_mean(df: pd.DataFrame, mean_column: str, control_mask: pd.Series) -> float | None:
    if not control_mask.any():
        return None
    control_mean = df.loc[control_mask, mean_column].dropna().mean()
    if pd.isna(control_mean) or float(control_mean) == 0:
        raise BioactivityError("Control mean is missing or zero; cannot normalize to control.")
    return float(control_mean)


def apply_order(
    df: pd.DataFrame,
    sample_column: str,
    order_file: Path | None,
    order_column: str | None,
) -> pd.DataFrame:
    if order_file is None:
        return df
    if order_column is None:
        raise BioactivityError("--order-column is required when --order-file is used.")

    order_df = read_table(order_file)
    ensure_columns(order_df, [order_column])
    order_values = clean_text_column(order_df[order_column]).dropna().drop_duplicates().tolist()
    order_index = {name: i for i, name in enumerate(order_values)}

    out = df.copy()
    out["_sort_order"] = clean_text_column(out[sample_column]).map(order_index)
    out = out[out["_sort_order"].notna()].copy()
    return out.sort_values("_sort_order").drop(columns=["_sort_order"])


def assign_threshold_classes(
    df: pd.DataFrame,
    value_column: str,
    threshold: float | None,
    threshold_mode: str,
    control_mask: pd.Series,
    active_label: str = "Active",
    inactive_label: str = "Inactive",
    control_label: str = "Control",
) -> pd.DataFrame:
    out = df.copy()
    if threshold is None:
        return out

    if threshold_mode not in {"ge", "le"}:
        raise BioactivityError("threshold_mode must be 'ge' or 'le'.")

    if threshold_mode == "ge":
        is_active = out[value_column] >= threshold
    else:
        is_active = out[value_column] <= threshold

    out["activity_class"] = np.where(is_active, active_label, inactive_label)
    if control_mask.any():
        out.loc[control_mask, "activity_class"] = control_label
    return out


def choose_bar_colors(df: pd.DataFrame) -> tuple[list[str], list[Patch]]:
    if "activity_class" in df.columns:
        color_map = {
            "Active": DEFAULT_COLORS["active"],
            "Inactive": DEFAULT_COLORS["inactive"],
            "Control": DEFAULT_COLORS["control"],
        }
        colors = [color_map.get(str(value), DEFAULT_COLORS["default"]) for value in df["activity_class"]]
        present = df["activity_class"].astype("string").dropna().unique().tolist()
        legend = [
            Patch(facecolor=color_map[name], label=name)
            for name in ["Active", "Inactive", "Control"]
            if name in present
        ]
        return colors, legend

    if "is_control" in df.columns and df["is_control"].any():
        colors = [DEFAULT_COLORS["control"] if is_ctrl else DEFAULT_COLORS["default"] for is_ctrl in df["is_control"]]
        legend = [
            Patch(facecolor=DEFAULT_COLORS["control"], label="Control"),
            Patch(facecolor=DEFAULT_COLORS["default"], label="Sample"),
        ]
        return colors, legend

    return [DEFAULT_COLORS["default"]] * len(df), []


def normalize_plot_style(plot_style: dict | None = None) -> dict:
    style = dict(DEFAULT_PLOT_STYLE)
    if plot_style:
        style.update({k: v for k, v in plot_style.items() if v is not None})
    return style


def make_plot(
    df: pd.DataFrame,
    sample_column: str,
    value_column: str,
    title: str,
    ylabel: str,
    figure_size: tuple[float, float] = (16, 6),
    rotate_labels: float = 45,
    plot_style: dict | None = None,
) -> plt.Figure:
    style = normalize_plot_style(plot_style)
    font_family = str(style["font_family"])
    title_size = float(style["title_size"])
    axis_label_size = float(style["axis_label_size"])
    xtick_label_size = float(style["xtick_label_size"])
    ytick_label_size = float(style["ytick_label_size"])
    legend_size = float(style["legend_size"])

    fig, ax = plt.subplots(figsize=figure_size)
    colors, legend_handles = choose_bar_colors(df)

    ax.bar(range(len(df)), df[value_column], color=colors)
    ax.set_xlabel(sample_column, fontsize=axis_label_size, fontfamily=font_family)
    ax.set_ylabel(ylabel, fontsize=axis_label_size, fontfamily=font_family)
    ax.set_title(title, fontsize=title_size, fontfamily=font_family)
    ax.set_xticks(range(len(df)))

    if rotate_labels > 0:
        horizontal_alignment = "right"
    elif rotate_labels < 0:
        horizontal_alignment = "left"
    else:
        horizontal_alignment = "center"

    ax.set_xticklabels(
        df[sample_column],
        fontsize=xtick_label_size,
        fontfamily=font_family,
        rotation=rotate_labels,
        ha=horizontal_alignment,
        rotation_mode="anchor",
    )
    ax.tick_params(axis="y", labelsize=ytick_label_size)
    for label in ax.get_yticklabels():
        label.set_fontfamily(font_family)

    max_value = float(df[value_column].max()) if len(df) else 0.0
    ax.set_ylim(0, max(max_value * 1.05, 1))

    if legend_handles:
        ax.legend(
            handles=legend_handles,
            loc="upper right",
            prop={"family": font_family, "size": legend_size},
        )

    fig.tight_layout()
    return fig


def process_bioactivity_table(
    input_path: Path,
    sample_column: str,
    replicate_columns: Sequence[str],
    order_file: Path | None = None,
    order_column: str | None = None,
    threshold: float | None = None,
    threshold_mode: str = "ge",
    control_row_indices: Sequence[int] | None = None,
    control_sample_names: Sequence[str] | None = None,
    control_column: str | None = None,
    control_value: str | None = None,
    control_query: str | None = None,
    exclude_control_from_plot: bool = False,
) -> tuple[pd.DataFrame, str, float | None]:
    df = read_table(input_path).copy()
    ensure_columns(df, [sample_column, *replicate_columns])

    df[sample_column] = clean_text_column(df[sample_column])
    df[replicate_columns] = df[replicate_columns].apply(pd.to_numeric, errors="coerce")
    df["mean_signal"] = df[replicate_columns].mean(axis=1, skipna=True)
    df["n_replicates_used"] = df[replicate_columns].notna().sum(axis=1)

    control_mask = select_control_mask(
        df=df,
        sample_column=sample_column,
        control_row_indices=control_row_indices,
        control_sample_names=control_sample_names,
        control_column=control_column,
        control_value=control_value,
        control_query=control_query,
    )
    df["is_control"] = control_mask

    control_mean = compute_control_mean(df, "mean_signal", control_mask)
    if control_mean is not None:
        df["relative_to_control_pct"] = (control_mean / df["mean_signal"]) * 100
        value_column = "relative_to_control_pct"
    else:
        value_column = "mean_signal"

    df = assign_threshold_classes(
        df=df,
        value_column=value_column,
        threshold=threshold,
        threshold_mode=threshold_mode,
        control_mask=control_mask,
    )

    if exclude_control_from_plot and control_mask.any():
        df = df.loc[~control_mask].copy()

    df = df[df[sample_column].notna()].copy()
    df = df[df[value_column].notna()].copy()
    df = apply_order(df, sample_column, order_file, order_column)

    if df.empty:
        raise BioactivityError("No rows remain after filtering/order application.")

    return df, value_column, control_mean


def save_outputs(
    fig: plt.Figure,
    df: pd.DataFrame,
    output_prefix: Path,
    export_table: bool = True,
    export_png: bool = True,
    export_svg: bool = True,
) -> list[Path]:
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    saved_paths: list[Path] = []

    if export_svg:
        path = output_prefix.with_suffix(".svg")
        fig.savefig(path, dpi=300, format="svg")
        saved_paths.append(path)
    if export_png:
        path = output_prefix.with_suffix(".png")
        fig.savefig(path, dpi=300)
        saved_paths.append(path)
    if export_table:
        path = output_prefix.with_suffix(".csv")
        df.to_csv(path, index=False)
        saved_paths.append(path)

    return saved_paths


def run_pipeline(
    input_path: Path,
    sample_column: str,
    replicate_columns: Sequence[str],
    order_file: Path | None = None,
    order_column: str | None = None,
    threshold: float | None = None,
    threshold_mode: str = "ge",
    control_row_indices: Sequence[int] | None = None,
    control_sample_names: Sequence[str] | None = None,
    control_column: str | None = None,
    control_value: str | None = None,
    control_query: str | None = None,
    exclude_control_from_plot: bool = False,
    title: str = "Sample activity",
    ylabel: str | None = None,
    figure_size: tuple[float, float] = (16, 6),
    rotate_labels: float = 45,
    plot_style: dict | None = None,
) -> tuple[pd.DataFrame, plt.Figure, str, float | None]:
    processed_df, value_column, control_mean = process_bioactivity_table(
        input_path=input_path,
        sample_column=sample_column,
        replicate_columns=replicate_columns,
        order_file=order_file,
        order_column=order_column,
        threshold=threshold,
        threshold_mode=threshold_mode,
        control_row_indices=control_row_indices,
        control_sample_names=control_sample_names,
        control_column=control_column,
        control_value=control_value,
        control_query=control_query,
        exclude_control_from_plot=exclude_control_from_plot,
    )

    if ylabel is None:
        ylabel = "Activity (% of control)" if control_mean is not None else "Mean signal"

    fig = make_plot(
        df=processed_df,
        sample_column=sample_column,
        value_column=value_column,
        title=title,
        ylabel=ylabel,
        figure_size=figure_size,
        rotate_labels=rotate_labels,
        plot_style=plot_style,
    )
    return processed_df, fig, value_column, control_mean


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plot replicate measurements from one CSV/XLSX file in a reusable way."
    )
    parser.add_argument("--input", required=True, help="Input CSV/XLSX file.")
    parser.add_argument("--sample-column", required=True, help="Column used for x-axis labels.")
    parser.add_argument(
        "--replicate-columns",
        nargs="+",
        required=True,
        help="One argument that accepts one or more replicate columns, e.g. --replicate-columns rep1 rep2 rep3",
    )
    parser.add_argument("--order-file", help="Optional CSV/XLSX file containing desired sample order.")
    parser.add_argument("--order-column", help="Column in --order-file containing sample names.")

    parser.add_argument(
        "--threshold",
        type=float,
        help="Optional threshold for assigning Active/Inactive classes. Leave empty to skip classification.",
    )
    parser.add_argument(
        "--threshold-mode",
        choices=["ge", "le"],
        default="ge",
        help="ge = Active if value >= threshold; le = Active if value <= threshold. Default: %(default)s",
    )

    parser.add_argument(
        "--control-row-indices",
        nargs="*",
        type=int,
        help="1-based row numbers in the input table to use as controls, e.g. --control-row-indices 1 2",
    )
    parser.add_argument(
        "--control-sample-names",
        nargs="*",
        help="Sample names from --sample-column to use as controls, e.g. --control-sample-names control_A control_B",
    )
    parser.add_argument("--control-column", help="Optional old-style control selection column.")
    parser.add_argument("--control-value", help="Optional old-style control selection value.")
    parser.add_argument(
        "--control-query",
        help="Optional pandas-style query/mask for selecting controls, e.g. 'bioactivity == 2'.",
    )
    parser.add_argument(
        "--exclude-control-from-plot",
        action="store_true",
        help="Remove control rows from the plotted/exported result table.",
    )

    parser.add_argument("--title", default="Sample activity", help="Plot title. Default: %(default)s")
    parser.add_argument(
        "--ylabel",
        default=None,
        help="Y-axis label. If omitted, it is chosen automatically based on normalization.",
    )
    parser.add_argument(
        "--figure-size",
        nargs=2,
        type=float,
        default=(16, 6),
        metavar=("WIDTH", "HEIGHT"),
        help="Figure size in inches. Default: 16 6",
    )
    parser.add_argument(
        "--rotate-labels",
        type=float,
        default=45,
        help="Rotation angle for x-axis labels. Default: %(default)s",
    )
    parser.add_argument("--font-family", default="Arial", help="Font family for figure text.")
    parser.add_argument("--title-size", type=float, default=16, help="Title font size.")
    parser.add_argument("--axis-label-size", type=float, default=20, help="X/Y axis label font size.")
    parser.add_argument("--xtick-label-size", type=float, default=8, help="Bottom label font size.")
    parser.add_argument("--ytick-label-size", type=float, default=16, help="Y-axis tick label font size.")
    parser.add_argument("--legend-size", type=float, default=12, help="Legend font size.")
    parser.add_argument(
        "--output-prefix",
        default=None,
        help="Output prefix without extension. Default: next to the script, under output/<input_stem>",
    )
    parser.add_argument("--export-table", action="store_true", help="Export the processed table as CSV.")
    parser.add_argument("--export-png", action="store_true", help="Export the plot as PNG.")
    parser.add_argument("--export-svg", action="store_true", help="Export the plot as SVG.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    input_path = resolve_path(args.input, script_dir)
    order_path = resolve_path(args.order_file, script_dir)

    if input_path is None:
        raise BioactivityError("Input path could not be resolved.")

    if args.output_prefix is None:
        output_prefix = get_default_output_prefix(input_path, script_dir)
    else:
        output_prefix = resolve_path(args.output_prefix, script_dir)
        if output_prefix is None:
            raise BioactivityError("Output prefix could not be resolved.")

    export_table = args.export_table
    export_png = args.export_png
    export_svg = args.export_svg
    if not any([export_table, export_png, export_svg]):
        export_table = export_png = export_svg = True

    try:
        processed_df, fig, _, _ = run_pipeline(
            input_path=input_path,
            sample_column=args.sample_column,
            replicate_columns=args.replicate_columns,
            order_file=order_path,
            order_column=args.order_column,
            threshold=args.threshold,
            threshold_mode=args.threshold_mode,
            control_row_indices=args.control_row_indices,
            control_sample_names=args.control_sample_names,
            control_column=args.control_column,
            control_value=args.control_value,
            control_query=args.control_query,
            exclude_control_from_plot=args.exclude_control_from_plot,
            title=args.title,
            ylabel=args.ylabel,
            figure_size=tuple(args.figure_size),
            rotate_labels=args.rotate_labels,
            plot_style={
                "font_family": args.font_family,
                "title_size": args.title_size,
                "axis_label_size": args.axis_label_size,
                "xtick_label_size": args.xtick_label_size,
                "ytick_label_size": args.ytick_label_size,
                "legend_size": args.legend_size,
            },
        )
        saved_paths = save_outputs(
            fig=fig,
            df=processed_df,
            output_prefix=output_prefix,
            export_table=export_table,
            export_png=export_png,
            export_svg=export_svg,
        )
    finally:
        plt.close("all")

    for path in saved_paths:
        print(f"Saved: {path}")


if __name__ == "__main__":
    main()
