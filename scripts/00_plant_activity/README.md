# 00 - Plant activity plotting

## Purpose

This step summarizes crude extract or plant bioactivity measurements from replicate columns. It produces a publication-ready bar plot and, optionally, a processed table with mean, standard deviation, control normalization, and activity classification.

## Inputs

Required input table: CSV, TSV, TXT, XLSX, or XLS.

Required columns:

- sample column: names of plants, extracts, or samples used as x-axis labels.
- replicate columns: numeric activity or fluorescence measurements.

Optional inputs:

- order table with a column listing the desired sample order.
- control rows selected by row number, sample name, control column/value, or pandas query.
- direct plate-reader files from replicate well plates. For this mode, choose the plate dimensions, positive-control wells, and an optional well-to-sample mapping table.

Plate-reader input:

- this is an alternative to a summarized input table; select one input mode in the GUI or CLI.
- accepted file types are CSV, TSV, TXT, XLSX, or XLS.
- the importer searches each file for the numeric plate grid, such as an 8 x 12 block for a 96-well plate.
- positive-control wells are used as the control reference and are excluded from plotted samples.
- if no mapping table is supplied, well IDs such as `A1` and `B7` are used as sample names.
- a mapping table should contain a `well` column and a sample-name column such as `sample`, `plant`, `species`, or `name`.

## Outputs

Depending on selected options:

- processed CSV table with replicate statistics and activity calls.
- PNG figure.
- SVG figure.

The threshold can classify samples as active or inactive. Use `--threshold-mode ge` when higher values mean more activity and `--threshold-mode le` when lower values mean more activity.

## GUI

```powershell
python p_00_01_plant_bioactivity_gui.py
```

## CLI example

```powershell
python p_00_00_plant_bioactivity_core.py `
  --input ..\examples\00_plant_activity\plant_activity_input.csv `
  --sample-column sample `
  --replicate-columns rep1 rep2 rep3 `
  --threshold 50 `
  --output-prefix ..\examples\00_plant_activity\output\plant_activity_demo `
  --export-table --export-png --export-svg
```

Plate-reader CLI example:

```powershell
python p_00_00_plant_bioactivity_core.py `
  --input-type plate_reader `
  --plate-files plate_1.xlsx plate_2.xlsx `
  --plate-rows 8 --plate-columns 12 `
  --plate-positive-control-wells H11,H12 `
  --well-mapping-file well_to_plant_mapping.csv `
  --well-mapping-sample-column plant `
  --export-table --export-png
```

## Common issues

- Replicate columns must be numeric.
- If controls are used for normalization, the selected control rows must exist and contain numeric replicate values.
- If an order file is provided, names must match the sample-column values.
