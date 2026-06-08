# Workflow notebooks

The notebooks are optional review and visualization companions. They are not required for command-line reproducibility; the numbered core scripts remain the processing engines.

## Included notebooks

- `jn_04_02_fraction_visualization_notebook.ipynb`: review outputs from script 04, including calibration behavior, predicted fraction assignments, response groups, and prioritization summaries.
- `jn_05_02_sirius_to_graphs.ipynb`: review and visualize annotation/class summaries produced from HRMS exploration and SIRIUS/CANOPUS-style outputs.

## Expected inputs

Run the corresponding script first, then point the notebook to that script's output folder.

For script 04, expected inputs include the generated `run_summary.json`, `04_features_with_bioactivity.csv`, optional `05_appended_feature_table_with_bioactivity.csv`, per-plant response-by-fraction CSV files, and files under `Post_run_analysis/`.

For script 05, expected inputs include prepared HRMS feature tables, annotation-summary CSV files, and the summary JSON written by the HRMS exploration core.
