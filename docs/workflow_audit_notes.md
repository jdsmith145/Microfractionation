# Workflow audit notes

Date: 2026-06-05

These notes track the publication-ready workflow structure, current input/output expectations, and packaging decisions for the clean GitHub-ready folder.

## Workflow structure

Script 00 identifies active crude extracts or samples from replicate measurements or direct plate-reader exports. It outputs a processed activity/fluorescence table and publication-ready PNG/SVG bar plots.

Script 01 is the user-facing route for the MZmine-dependent section. It prepares the complete-sample and fraction `.mzbatch` files, runs MZmine from the command line, checks expected CSV outputs, and matches fraction features to the complete feature table. It now contains the former standalone batch-preparation and matching logic directly, so the former standalone batch-preparation and matching GUIs are not needed in the public GitHub package.

Script 02 visualizes a single sample by combining the mzML chromatogram, script 01 matched fraction features, and an optional fraction response overlay. The overlay can represent raw fluorescence, normalized fluorescence, or derived activity depending on the selected settings.

Script 03 searches Wikidata by molecular formula and taxonomic context. It writes species/genus/family/formula-only result CSV files, can merge duplicate compounds by URL and/or SMILES, and has a GUI visualization tab for RDKit structure previews, searching, copying, and exporting selected rows.

Script 04 predicts HPLC fraction assignment from UPLC retention time, maps fraction response values back to features, and writes human-readable prioritization tables plus post-run figures. Newer configurations may group fractions by derived activity, relative fluorescence/signal, or raw/plate-processed response average. Separate annotation tables can use different match-column names and selected annotation columns.

Script 05 summarizes wide HRMS feature tables. It can rename samples from metadata, append annotations, count annotation categories by sample, and generate publication-oriented summary tables and figures.

The notebooks are optional review companions. The script 04 notebook supports fraction-prediction review and prioritization figures. The script 05 notebook supports SIRIUS/CANOPUS-style annotation visualization.

## Documentation status

Root `README.md` describes installation, the numbered workflow, and script 01 as the single MZmine-processing bridge.

Each active script folder contains a `README.md` with purpose, inputs, outputs, GUI command, CLI example, and common issues. The script 02 and 04 READMEs now describe response/fluorescence inputs separately from derived bioactivity interpretation.

`docs/workflow.md` summarizes the analysis path at the project level. This audit note captures implementation and packaging decisions that are more detailed than the public-facing overview.

## Clean packaging decisions

Include in `python_final`:

- root documentation and metadata: `README.md`, `CHANGELOG.md`, `CITATION.cff`, `LICENSE`, `LICENSE-docs-data.md`, `requirements.txt`, `environment.yml`;
- `docs/` and `examples/`;
- shared helpers in `shared/`: `bioassay_plate_reader.py` and `gui_help_popover.py`;
- active public script folders `00_plant_activity`, `01_mzmine_pipeline`, `02_two_sided_plot`, `03_wikidata`, `04_fraction_predictor`, and `05_HRMS_exploration`;
- active `.py` files, per-script `README.md` files, and MZmine template `.mzbatch` files.

Exclude from `python_final`:

- `archived/` folders;
- `__pycache__/` folders and `.pyc` files;
- GUI state files such as `.p_*_state.json`;
- local output folders such as `output/`, `outputs/`, `Outputs/`, and `test_outputs/`;
- real project data folders and local configs used during development;
- former standalone batch-preparation and fraction-matching folders;
- notebooks, which are copied separately to `notebooks_final`.

Include in `notebooks_final`:

- `jn_04_02_fraction_visualization_notebook.ipynb`;
- `jn_05_02_sirius_to_graphs.ipynb`;
- a small `README.md` explaining which script output each notebook expects.

## Current quality checks

All active Python files compile with Python 3.11 in the `microfractionation` conda environment.

All core scripts expose command-line help.

The active dependency files cover the imported third-party packages used by the active scripts: `pandas`, `numpy`, `matplotlib`, `customtkinter`, `requests`, `pymzml`, `openpyxl`, `pillow`, and `rdkit` through conda.


## Publication cleanup performed

The clean folder now includes a root `.gitignore`, excludes generated Python caches and hidden GUI state files, and uses neutral output placeholders in MZmine template batch files instead of local Windows paths.

Additional public documentation was added:

- `docs/testing.md` for smoke checks and dry-run validation;
- `docs/input_schema_reference.md` for a compact cross-workflow table-column reference.
