# 05 - HRMS exploration

## Purpose

This step prepares and summarizes wide HRMS feature tables. It can preserve selected feature columns, rename sample columns through metadata, append annotation columns, count annotation categories across samples, and export publication-oriented summary plots.

## Inputs

Feature table requirements:

- feature ID column.
- sample abundance columns.
- optional existing annotation columns to keep.

Optional metadata table:

- key column matching sample column names after optional suffix removal.
- value column with display names.

Optional annotation table:

- feature ID column.
- one or more annotation columns to append.

Analysis inputs:

- annotation/category column for counting.
- terms to plot or top-N categories to summarize.
- abundance threshold.

## Outputs

Depending on the run settings:

- `01_feature_table_samples_prepared.csv`.
- `02_feature_table_with_annotations.csv`.
- category count CSV files.
- selected-term summary CSV files.
- SVG figures and PNG previews.
- `hrms_exploration_summary.json`.

## GUI

```powershell
python p_05_01_hrms_exploration_gui.py
```

## CLI examples

Create a template config:

```powershell
python p_05_00_hrms_exploration_core.py --make-template hrms_exploration_template.json
```

Run from a config:

```powershell
python p_05_00_hrms_exploration_core.py `
  --config ..\examples\05_HRMS_exploration\hrms_exploration_config.json
```

## Common issues

- Sample columns must be numeric abundance or peak-area columns.
- Annotation IDs must match feature-table IDs after reading the files.
- If automatic sample detection misses columns, provide `sample_columns` explicitly in the config or CLI.
