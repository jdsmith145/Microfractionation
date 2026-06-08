# Input schema reference

This page summarizes the minimum table columns expected by the public workflow. The per-script READMEs remain the authoritative place for full options, GUI guidance, and troubleshooting.

## 00 - Plant activity

Prepared table mode expects one row per plant, extract, or sample.

Required columns:

- sample/species name column, selected by the user;
- one or more numeric replicate measurement columns.

Optional order table:

- one column containing the desired display order of sample/species names.

Plate-reader mode accepts direct plate readout files. The user supplies plate dimensions, replicate files, positive-control well positions, and optional well-to-sample mapping.

## 01 - MZmine pipeline

Required inputs:

- sample mzML files;
- blank mzML files;
- complete-sample MZmine `.mzbatch` template;
- fraction MZmine `.mzbatch` template;
- path to `mzmine_console.exe` for real runs.

Expected MZmine outputs after running:

- complete feature table CSV named from the configured sample name;
- fraction feature table CSV files named like `frac_01.csv`, `frac_02.csv`, etc.

The matching stage expects MZmine-style feature tables with columns that contain feature m/z, retention time, and peak area information.

## 02 - Two-sided plot

Required inputs:

- one chromatogram mzML file;
- filtered/matched feature CSV from Script 01;
- fraction timing settings: start time, fraction width, and number of fractions.

The filtered feature CSV should contain usable fraction assignment plus feature m/z, retention time, and area columns. Typical columns are:

- `fraction_index`;
- `mz`;
- `rt`;
- `area`.

Optional response overlay:

- table mode: fraction column and measured value column, optionally positive-control values;
- plate-reader mode: replicate plate files, plate dimensions, and positive-control wells.

## 03 - Wikidata

Manual or file input requires molecular formulas and taxonomic context.

Common columns:

- formula column, for example `formula`;
- genus column, for example `genus`;
- species column, for example `species`.

The script writes result CSVs for exact species, genus, family, formula-anywhere searches, missing/problematic formulas, and a search summary.

## 04 - Fraction predictor

Main feature table requirements:

- feature ID column, for example `row ID`;
- m/z column, for example `row m/z`;
- HRMS/UPLC retention-time column, for example `row retention time`;
- one or more numeric sample peak-area columns.

Calibration by matched pairs requires a table with:

- UPLC/HRMS retention-time column;
- HPLC retention-time column.

Feature-order alignment requires landmark rows with:

- `anchor_id`;
- `hrms_rt`;
- `hplc_fraction`;
- optional `hrms_feature_id` and `hplc_feature_id` for traceability.

Response inputs can be table-based or direct plate-reader files. Table-based inputs normally need:

- fraction number column;
- measured average/value column;
- optional positive-control column when normalization is needed.

Separate annotation table mode requires:

- a match column in the prediction table;
- a match column in the annotation table;
- one or more annotation columns to append, unless the user intentionally keeps all annotation columns.

## 05 - HRMS exploration

Feature table requirements:

- feature ID column, usually `row ID`;
- numeric sample abundance columns selected manually or by prefix/suffix/regex.

Optional metadata table:

- key column matching sample column names after configured prefix/suffix cleanup;
- value/display column containing readable sample names.

Optional annotation table:

- annotation feature ID column matching the feature table ID after parser cleanup;
- annotation columns to append, such as molecular formula or chemical class columns.
