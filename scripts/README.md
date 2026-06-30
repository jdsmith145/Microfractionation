# Microfractionation computational workflow

This repository contains the active Python workflow used to connect HPLC microfractionation, MZmine feature tables, two-sided visualization, Wikidata dereplication, and fraction prediction.

## Workflow steps

| Step | Folder | Purpose |
| --- | --- | --- |
| 01 | `01_mzmine_pipeline` | Prepare MZmine batches, run MZmine headlessly, and filter fraction features through three focused tools. |
| 02 | `02_two_sided_plot` | Plot HPLC chromatogram, fraction feature intensity, and activity/intensity overlay in one figure. |
| 03 | `03_wikidata` | Search Wikidata for compounds by molecular formula and taxonomic scope. |
| 04 | `04_fraction_predictor` | Predict HPLC fractions from UPLC retention time or anchor pairs and map fraction response values back to LC-MS features. |

## Installation

Dependency files live in the repository root. Conda is recommended because RDKit is most reliable from conda-forge:

```powershell
conda env create -f ..\environment.yml
conda activate microfractionation
```

The Conda environment uses Python 3.11 for reproducibility. It installs the scientific stack from conda-forge and installs `customtkinter` with pip inside the same environment.

For a pip-only environment:

```powershell
python -m pip install -r ..\requirements.txt
```

RDKit is optional for the core Wikidata search but required for structure previews in the Wikidata GUI:

```powershell
conda install -c conda-forge rdkit
```

## Running the workflow

The normal entry point is the launcher in the parent `scripts` folder:

```powershell
python p_microfractionation_launcher.py
```

Focused GUIs can still be opened directly when needed:

```powershell
python 01_mzmine_pipeline/p_01_01_batch_setup_gui.py
python 01_mzmine_pipeline/p_01_02_mzmine_runner_gui.py
python 01_mzmine_pipeline/p_01_03_feature_filter_gui.py
python 02_two_sided_plot/p_02_01_two_sided_plot_gui.py
python 03_wikidata/p_03_01_wikidata_gui.py
python 04_fraction_predictor/p_04_01_fraction_predictor_gui.py
```

Script 01 is split into three focused MZmine screens because this part has three different jobs: batch setup, MZmine execution, and feature filtering. The screens share launcher/sample paths so users do not need to re-enter the same files.

Core scripts can be run from the command line once a JSON config has been created in the launcher or saved from a focused GUI. For example:

```powershell
python 01_mzmine_pipeline/p_01_00_mzmine_pipeline_core.py `
  --config path/to/mzmine_pipeline_config.json `
  --stages prepare
```

## Inputs and outputs

The most important reproducibility rule is that every table must contain the expected columns. The per-step READMEs define the required columns explicitly. Most scripts accept CSV and Excel inputs; mzML inputs are used where chromatograms or MZmine batch files are involved.

The public package does not include project-specific input data, saved configs, or generated outputs. It does include small synthetic example files under `../example_data/` and `examples/` so users can inspect expected file layouts. Per-step READMEs describe the expected table columns and file roles.

## Authors

Joshua David Smith1,&, Erik Bouchal1,2,&, Zdeněk Knejzlík1, Martin Dračínský1, Tito Damiani1, Roman Bushuiev1,3, Eva Tikalová1, Eva Tloušťová1, Marcela Pávová1, Jan Hodek1, Artur Jasanský1, Matouš Soldát1,2, Alžběta Kadlecová1, Vendula Tvrdoňová Stillerová1, Pavel Šácha1, Tomáš Pluskal1*, Téo Hebra1*

1 Institute of Organic Chemistry and Biochemistry of the Czech Academy of Sciences, Prague, Czechia  
2 Charles University, Faculty of Science, Prague, Czechia  
3 Robotics and Cybernetics, Czech Technical University, Prague, Czechia

& These authors contributed equally to this work.  
* To whom correspondence should be addressed.
