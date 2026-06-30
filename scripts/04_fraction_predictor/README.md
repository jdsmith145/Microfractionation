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

Visible GUI calibration options:

- gradient calibration with matched UPLC/HPLC retention-time pairs.
- feature-order anchor pairs for non-comparable gradients. This uses paired anchor features selected in the HRMS table and the script 01 filtered HPLC table, then assigns candidate fraction intervals between anchors instead of predicting one exact HPLC retention time.

### Known calibration feature RTs

When a matched-RT pairs table also contains the main feature-table ID, select its **Feature ID column** and enable **Use known HPLC RT for matching feature IDs**. For every filtered feature whose ID occurs in that table, the report records `Part of calibration = yes` and the entered HPLC RT in `Real RT`. Fraction assignment uses `Real RT` for those known features; all other features use the regression prediction in `predicted_hplc_rt`.

This is optional. It does not change the fitted regression, and `predicted_hplc_rt` remains in the report so the measured and predicted values can be compared. A feature ID must map to one unambiguous HPLC RT in the pairs table.


Feature-order anchor-pair table requirements, when imported from CSV/Excel:

- `anchor_id`: readable anchor pair name such as `A1`.
- `hrms_rt`: HRMS retention time of the anchor-pair feature.
- `hplc_fraction`: fraction number from the script 01 filtered HPLC table.
- optional `hrms_feature_id` and `hplc_feature_id` columns for traceability.

Plant or fraction response inputs:

- one plant card per biological sample or extract.
- intensity, activity, or other response table with fraction numbers, measured average values, and positive-control values when normalization is needed.
- alternatively, direct plate-reader files from replicate fraction bioassays. In the GUI plant card, choose `Plate reader`, select all replicate files, set plate dimensions, and enter positive-control wells.
- plate-reader fraction mapping is row-major by default: `A1 = 1`, `A2 = 2`, ..., `H12 = 96` for a 96-well plate.
- bins can be based on derived activity (`100 - relative signal`), relative intensity/signal, or raw/plate-processed signal average. The plant card also asks whether higher or lower signal means stronger response, so cutoffs always apply to the selected response direction. Use the GUI preview to inspect distributions and suggested cutoffs before exporting a full run.

Annotation inputs:

- In the normal launcher workflow, the shared SIRIUS/CANOPUS annotation table is loaded automatically.
- The main feature table supplies m/z, retention time, and sample intensity columns; the annotation table supplies interpretation columns such as `molecularFormula` and `NPC#class`.
- The default match is `row ID` in the main feature table to `mappingFeatureId` in the SIRIUS/CANOPUS table.
- Select only the annotation columns that should appear in the final report. If no annotation columns are selected, the final report keeps all columns from the annotation table.

## Outputs

Main CSV outputs:

- `final_feature_table_with_fraction_activity.csv`: the main user-facing table for features that passed the selected peak-area threshold. It preserves the filtered original feature-table columns, adds selected annotation columns near the feature identity/RT columns, and appends predicted fraction, candidate-fraction interval, activity/intensity group, and interpretation columns. When known calibration feature RTs are enabled, it also includes `Part of calibration` and `Real RT`; those real RTs determine the corresponding fraction assignments.
- `full_feature_table_with_fraction_activity.csv`: the complete original feature table with the same report columns appended. Rows below the peak-area threshold are retained, but prediction/activity fields are empty because they were not processed by the filtering step.
- one response-by-fraction CSV per plant/sample.
- `run_summary.json`.

Intermediate debug outputs:

- Diagnostic intermediate tables are written only when **Write debug tables** is enabled in the GUI. They are saved into `Debug_exports/`.
- In a JSON config, set `"debug_exports": true` for the same behavior.

Post-run analysis:

- compact summary CSV files.
- pastel PNG/SVG figures including a feature prioritization funnel and plant-specific summaries.
- `run_summary.json`.

## GUI

```powershell
python p_04_01_fraction_predictor_gui.py
```

For feature-order anchor-pair mapping, use the `Alignment` tab when the HRMS table is large. The tab lets users search and page through the HRMS and HPLC tables, filter HRMS features by sample-column intensity threshold, retention-time range, and m/z range, then add anchor pairs without loading the entire table into one visible list.

## CLI example

```powershell
python p_04_00_fraction_predictor_core.py `
  --config ..\examples\04_fraction_predictor\fraction_predictor_config.json
```

## Common issues

- Calibration pairs must have numeric UPLC and HPLC retention-time columns.
- Feature-order anchor-pair mode requires at least two anchor features whose HRMS retention times and HPLC fraction numbers increase in the same order. It is intended for mixed gradients or isocratic sections where interpolation would be misleading.
- Features outside the first/last anchor pair are marked as outside anchors and are not assigned response summaries.
- Fraction numbers in activity/intensity tables must match generated or configured fraction numbers.
- Sample columns used for filtering are usually the same samples that need plant cards.
- The final feature table only contains molecule/class/formula annotations if the selected annotation table actually contains those columns and the matching columns point to the same feature IDs.
- Use the response preview before a full run when choosing cutoffs. The preview reads the configured response inputs, shows the current bins, and suggests cutoffs intended to separate the top five response fractions and the strongest single response fraction.
