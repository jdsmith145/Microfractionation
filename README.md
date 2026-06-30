# Microfractionation Workflow Package

Open `scripts/p_microfractionation_launcher.py` to start the workflow launcher. The launcher creates `microfractionation_project_config.json` on first use, validates shared inputs, creates output folders, and opens the focused workflow tools.

The public package is distributed without project-specific configs, real project input data, or generated outputs. Small synthetic examples are included under `example_data/`. The MZmine batch templates are retained under `scripts/01_mzmine_pipeline/templates/`.


## Installation

The recommended setup uses Conda because RDKit is most reliable from conda-forge:

```powershell
conda env create -f environment.yml
conda activate microfractionation
```

For a pip-only setup, use:

```powershell
python -m pip install -r requirements.txt
```

RDKit is optional for the core Wikidata search, but required for molecule structure previews in the Wikidata GUI. If it is missing from a pip-only setup, install it with Conda when possible:

```powershell
conda install -c conda-forge rdkit
```

## Platform Note

The workflow GUIs are Python/CustomTkinter based, but the automated MZmine runner is currently documented for Windows because it expects the Windows portable MZmine folder and `mzmine_console.exe`. Linux/macOS users can still use the Python table/plotting steps, but the MZmine executable path and portable-folder instructions must be adapted to their local MZmine installation.

## First Setup: Install MZmine 4.7.8 Portable

MZmine is required for the batch runner step, but it is not bundled with this repository. Before running the MZmine pipeline:

1. Open the official MZmine 4.7.8 release page: https://github.com/mzmine/mzmine/releases/tag/v4.7.8
2. Download the Windows portable package.
3. Unzip it directly inside the `Microfractionation` folder.
4. Keep the folder name as `mzmine_Windows_portable_4.7.8`.
5. Confirm that this file exists: `mzmine_Windows_portable_4.7.8/mzmine_console.exe`.

The launcher has an `Open MZmine 4.7.8 download page` button and a `Check MZmine` button. Missing MZmine is shown as a setup warning because the other screens can still be configured, but the MZmine Runner cannot run until `mzmine_console.exe` is available.

## Workflow At A Glance

1. Raw HPLC-MS mzML files are used to create MZmine batch files.
2. MZmine writes a complete feature table and fraction feature tables.
3. Feature filtering combines the complete table with fraction CSV files and writes a filtered HPLC feature table.
4. The filtered HPLC feature table is used with activity/intensity data for the two-sided plot.
5. Based on two-sided plot prioritization, selected features from fractions can be dereplicated with Wikidata.
6. The fraction prediction workflow is used on prioritized samples to rank HRMS features of interest.

## Main Inputs And Outputs

| Workflow step | Main input | Main output |
| --- | --- | --- |
| Launcher | shared project files and sample records | `microfractionation_project_config.json` and module handoff config |
| Batch setup | HPLC mzML sample/blank injections and MZmine templates | configured `.mzbatch` files |
| MZmine runner | configured `.mzbatch` files | complete feature table and `frac_*.csv` files |
| Feature filtering | complete table plus fraction CSVs | `<sample>_filtered_feature_table.csv` and `<sample>_fraction_purity_estimates.csv` |
| Two-sided plot | filtered HPLC table, chromatogram mzML, activity files | SVG/PNG figure and plotted data CSV |
| Wikidata | formula/taxon list | Wikidata hit tables and structure visualization |
| Fraction predictor | HRMS feature table, filtered HPLC table, annotations, activity files | fully annotated HRMS feature table with fraction/activity columns |

## Clean State

- No real project input data are bundled in the public package; only small synthetic example files are included.
- No saved configs are bundled; `microfractionation_project_config.json` is created by the launcher on first use.
- No generated output files are bundled.
- MZmine batch templates are bundled in `scripts/01_mzmine_pipeline/templates/`.
- Synthetic example files are available under `example_data/`. Users should create local `data/`, `configs/`, and `output/` folders for their own analyses, or let the launcher create the needed project paths.

The workflow graph in the launcher is static. It explains data flow and file expectations; it is not a Snakemake-style execution engine yet.

## Authors

Joshua David Smith1,&, Erik Bouchal1,2,&, Zdeněk Knejzlík1, Martin Dračínský1, Tito Damiani1, Roman Bushuiev1,3, Eva Tikalová1, Eva Tloušťová1, Marcela Pávová1, Jan Hodek1, Artur Jasanský1, Matouš Soldát1,2, Alžběta Kadlecová1, Vendula Tvrdoňová Stillerová1, Pavel Šácha1, Tomáš Pluskal1\*, Téo Hebra1\*

1 Institute of Organic Chemistry and Biochemistry of the Czech Academy of Sciences, Prague, Czechia  
2 Charles University, Faculty of Science, Prague, Czechia  
3 Robotics and Cybernetics, Czech Technical University, Prague, Czechia

& These authors contributed equally to this work.  
\* To whom correspondence should be addressed.
