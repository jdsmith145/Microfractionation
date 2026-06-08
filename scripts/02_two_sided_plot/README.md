# 02 - Chromatogram, fraction features, and response plot

## Purpose

This step creates a combined visualization: HPLC chromatogram on top, fraction-associated feature intensity in the middle, and an optional fraction response overlay below. The response overlay can represent raw fluorescence, normalized fluorescence, or derived activity depending on the settings chosen by the user.

## Inputs

Required:

- sample chromatogram `.mzML`.
- filtered feature CSV from script 01.
- sample name.

Filtered feature CSV must contain:

- `fraction_index` or alias such as `fraction`.
- `rt` or alias such as `retention_time`.
- `area` or alias such as `Peak area`.

Optional response/activity table:

- CSV, TSV, TXT, XLSX, or XLS.
- fraction number column.
- activity, fluorescence, or other response-value column, or replicate columns with normalization settings.
- optional start/end columns for non-uniform fraction windows.

Optional direct plate-reader input:

- this is an alternative to the optional prepared bioactivity table; select one input mode in the GUI, not both.
- select all replicate plate-reader files in the GUI activity tab.
- set plate rows and columns, for example 8 x 12 for a 96-well plate.
- enter positive-control wells such as `H11,H12`.
- the default fraction mapping is row-major: `A1 = 1`, `A2 = 2`, ..., `H12 = 96`.
- positive-control wells are excluded from the overlay; use the existing table input if a different fraction map is required.
- the GUI explains the available plate-scaling and display-transform options; the right y-axis label can be set manually or generated from the selected transform.

## Outputs

- SVG figure by default.
- PNG figure when requested.
- optional config-driven outputs from the GUI.

## GUI

```powershell
python p_02_01_two_sided_plot_gui.py
```

## CLI example

```powershell
python p_02_00_two_sided_plot_core.py `
  --mzml sample_chromatogram.mzML `
  --filtered ..\examples\02_two_sided_plot\filtered_features.csv `
  --sample-name demo_sample `
  --outdir ..\examples\02_two_sided_plot\output `
  --bio ..\examples\02_two_sided_plot\activity_table.csv `
  --png
```

## Common issues

- Reading mzML requires `pymzml`.
- The synthetic example includes table schemas but no mzML file; use a real chromatogram for a runnable plot.
- Fraction numbering must match the fraction labels used in the response/activity table.
