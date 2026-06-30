# Microfractionation example data

This folder contains tiny example inputs that show the expected shape of each data type. They are deliberately small and are meant for learning column names and table layout, not for real scientific analysis.

## Project-level tables

- `project_tables/main_feature_table.csv`: MZmine-style or HRMS-style feature table with `row ID`, `row m/z`, `row retention time`, and one or more sample `Peak area` columns.
- `annotations/sirius_canopus_annotations.csv` / `.xlsx`: SIRIUS/CANOPUS annotation table. The usual matching column is `mappingFeatureId`, matched to `row ID` in the main feature table.
- `annotations/optional_extra_annotations.csv`: example of an extra user annotation table matched by `row ID`.

## Activity/intensity tables

- `activity/plate_reader/ruta_corsica_107_rep_1_8x12_plate.csv` / `.xlsx`: direct 96-well plate-reader layout. Rows are A-H and columns are 1-12. In this example, H11 is a low positive-control well and H12 is intentionally high signal; users should select only the wells that are controls for their assay.
- `activity/fraction_table/macleaya_microcarpa_84_activity_table.csv` / `.xlsx`: fraction-by-row activity/intensity table. Each fraction has replicate columns; the shared positive-control mean can be entered once in the control column and left blank below.

## MZmine and fraction matching

- `hplc_mzml/example_mzml_file_list.csv`: naming guide for real `.mzML` files. This folder does not include fake mzML files because tiny placeholder mzML files would not be useful for real processing.
- `mzmine_outputs/complete_feature_table/example_complete_feature_table.csv`: complete sample feature table exported by MZmine.
- `mzmine_outputs/fraction_feature_tables/frac_001.csv`, `frac_002.csv`: per-fraction feature tables exported by MZmine. Script 01 looks for files named `frac_*.csv`.
- `mzmine_outputs/filtered_feature_table/example_filtered_feature_table.csv`: filtered table produced after matching fraction features to the complete feature table.

## Fraction predictor and Wikidata

- `calibration/manual_rt_pairs.csv` / `.xlsx`: manual UPLC-to-HPLC retention-time pairs for the same molecules. Script 04 uses `UPLC RT` and `HPLC RT`; `UPLC m/z` and `HPLC m/z` are optional evidence columns for the user. HPLC RT is shown to one decimal place because low-resolution HPLC timing is usually not more precise.
- `wikidata/wikidata_formula_queries.csv` / `.xlsx`: formula/taxon query table for Wikidata. Default columns are `genus`, `species`, and `formula`.

## Launcher sample record

- `launcher_sample_record_template.json`: example of how a launcher sample record connects sample name, files, activity folder, and peak-area column. Paths are illustrative and should be replaced by the user in the launcher GUI.
