# Microfractionation computational workflow

This repository contains the Python workflow used to connect plant or extract bioactivity, HPLC microfractionation, MZmine feature tables, Wikidata dereplication, fraction prediction, and HRMS feature-table exploration.

The scripts are organized as numbered steps. Each step has a core script for reproducible command-line or notebook use and, where useful, a CustomTkinter GUI for interactive work.

## Workflow steps

| Step | Folder | Purpose |
| --- | --- | --- |
| 00 | `00_plant_activity` | Plot crude extract or plant bioactivity from replicate measurements and classify active samples. |
| 01 | `01_mzmine_pipeline` | Prepare MZmine batches, run MZmine headlessly, and match fraction features in one guided workflow. This script contains the former standalone batch-preparation and matching stages. |
| 02 | `02_two_sided_plot` | Plot HPLC chromatogram, fraction feature intensity, and bioactivity in one figure. |
| 03 | `03_wikidata` | Search Wikidata for compounds by molecular formula and taxonomic scope. |
| 04 | `04_fraction_predictor` | Predict HPLC fractions from UPLC retention time and map fraction response values back to LC-MS features. |
| 05 | `05_HRMS_exploration` | Explore annotated HRMS feature tables and summarize molecular classes across samples. |

## Installation

Conda is recommended because RDKit is most reliable from conda-forge:

```powershell
conda env create -f environment.yml
conda activate microfractionation
```

The Conda environment uses Python 3.11 for reproducibility. It installs the scientific stack from conda-forge and installs `customtkinter` with pip inside the same environment, because `customtkinter` is not available as a standard conda-forge package on all platforms.

For a pip-only environment:

```powershell
python -m pip install -r requirements.txt
```

RDKit is optional for the core Wikidata search but required for structure previews in the Wikidata GUI:

```powershell
conda install -c conda-forge rdkit
```

## Running the workflow

Each folder contains its own `README.md` with exact input columns, outputs, GUI command, and CLI examples. In general:

```powershell
python 00_plant_activity/p_00_01_plant_bioactivity_gui.py
python 01_mzmine_pipeline/p_01_01_mzmine_pipeline_gui.py
python 02_two_sided_plot/p_02_01_two_sided_plot_gui.py
python 03_wikidata/p_03_01_wikidata_gui.py
python 04_fraction_predictor/p_04_01_fraction_predictor_gui.py
python 05_HRMS_exploration/p_05_01_hrms_exploration_gui.py
```

Script 01 is the only user-facing MZmine pipeline step. It includes batch preparation, headless MZmine execution, output checks, and fraction-feature matching so users do not need to choose between separate tools.

Core scripts can be run from the command line. For example:

```powershell
python 01_mzmine_pipeline/p_01_00_mzmine_pipeline_core.py `
  --config examples/01_mzmine_pipeline/mzmine_pipeline_config.json `
  --dry-run
```

## Inputs and outputs

The most important reproducibility rule is that every table must contain the expected columns. The per-step READMEs define the required columns explicitly. Most scripts accept CSV and Excel inputs; mzML inputs are used where chromatograms or MZmine batch files are involved.

Synthetic example files are provided under `examples/`. These are artificial and intended only to document table formats and test basic command-line behavior. They do not represent the research dataset.

Additional cross-workflow references:

- `docs/input_schema_reference.md` summarizes required columns across all steps.
- `docs/testing.md` lists smoke checks for syntax, command-line help, and Script 01 dry-run validation.

The repository intentionally does not include real research data. Files under `examples/` are synthetic schema examples. MZmine templates under `01_mzmine_pipeline/templates/` are starting points; Script 01 rewrites raw-data paths and export paths during batch preparation.

## Repository metadata

Code is licensed under MIT. Documentation and synthetic examples are licensed under CC-BY-4.0. See `LICENSE`, `LICENSE-docs-data.md`, and `CITATION.cff`.
