# 04 - Fraction predictor and response mapping

## Purpose

This step predicts HPLC retention time from UPLC retention time, assigns each LC-MS feature to an HPLC fraction, maps fraction response data back to features, and writes human-readable prioritization outputs.

## Inputs

Recommended input is a JSON config saved by the GUI.

Feature table requirements:

- feature ID column, for example `row ID`.
- m/z column, for example `row m/z`.
- UPLC retention-time column, for example `row retention time`.
- one or more sample peak-area columns.

Calibration options:

- matched UPLC/HPLC retention-time pairs.
- explicit linear equation.
- runtime scaling fallback.
- feature-order alignment for non-comparable gradients. This uses landmark features selected in the HRMS table and the script 01 filtered HPLC table, then assigns candidate fraction intervals between landmarks instead of predicting one exact HPLC retention time.

Feature-order landmark table requirements, when imported from CSV/Excel:

- `anchor_id`: readable landmark name such as `A1`.
- `hrms_rt`: HRMS retention time of the landmark feature.
- `hplc_fraction`: fraction number from the script 01 filtered HPLC table.
- optional `hrms_feature_id` and `hplc_feature_id` columns for traceability.

Plant or fraction response inputs:

- one plant card per biological sample or extract.
- fluorescence, activity, or other response table with fraction numbers, measured average values, and positive-control values when normalization is needed.
- alternatively, direct plate-reader files from replicate fraction bioassays. In the GUI plant card, choose `Plate reader`, select all replicate files, set plate dimensions, and enter positive-control wells.
- plate-reader fraction mapping is row-major by default: `A1 = 1`, `A2 = 2`, ..., `H12 = 96` for a 96-well plate.
- bins can be based on derived activity (`100 - relative signal`), relative fluorescence/signal, or raw/plate-processed signal average. Use the GUI preview to inspect distributions and suggested cutoffs before exporting a full run.

Annotation inputs:

- If the main feature table already contains annotations, mark it as annotated in the GUI and no second file is needed.
- If annotations are in a separate table, enable `Use a separate annotation table`, load that file, choose the matching column in the prediction table, choose the matching column in the annotation table, and select the annotation columns to keep.
- The matching columns may have different names, but their values must identify the same features, for example `row ID` in the prediction table and `feature_id` in the annotation table.
- If no annotation columns are selected, the final human-readable report keeps all columns from the annotation table.

## Outputs

Main CSV outputs:

- `01_filtered_feature_table.csv`.
- `02_fraction_windows.csv`.
- `03_features_with_fraction_predictions.csv`.
- `04_features_with_bioactivity.csv`. The filename is kept for compatibility, but the mapped values can represent derived activity, relative fluorescence/signal, or raw/plate-processed response depending on the selected grouping value.
- `05_appended_feature_table_with_bioactivity.csv` when an annotated table is supplied. The filename is kept for compatibility.
- one response-by-fraction CSV per plant. Existing filenames use `bioactivity_by_fraction` for compatibility.

Post-run analysis:

- compact summary CSV files.
- pastel PNG/SVG figures including a feature prioritization funnel and plant-specific summaries.
- `run_summary.json`.

## GUI

```powershell
python p_04_01_fraction_predictor_gui.py
```

For feature-order alignment, use the `Alignment` tab when the HRMS table is large. The tab lets users search and page through the HRMS and HPLC tables, filter HRMS features by sample-column intensity threshold, retention-time range, and m/z range, then add landmark pairs without loading the entire table into one visible list.

## CLI example

```powershell
python p_04_00_fraction_predictor_core.py `
  --config ..\examples\04_fraction_predictor\fraction_predictor_config.json
```

## Common issues

- Calibration pairs must have numeric UPLC and HPLC retention-time columns.
- Feature-order alignment requires at least two landmark features whose HRMS retention times and HPLC fraction numbers increase in the same order. It is intended for mixed gradients or isocratic sections where interpolation would be misleading.
- Features outside the first/last landmark are marked as outside anchors and are not assigned response summaries.
- Fraction numbers in fluorescence tables must match generated or configured fraction numbers.
- Sample columns used for filtering are usually the same samples that need plant cards.
- `05_appended_feature_table_with_bioactivity.csv` only contains molecule/class/formula annotations if the selected annotation table actually contains those columns and the matching columns point to the same feature IDs.
- Use the response preview before a full run when choosing cutoffs. The preview reads the configured response inputs, shows the current bins, and suggests percentile-based cutoffs.
