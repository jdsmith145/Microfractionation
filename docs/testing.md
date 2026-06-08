# Testing and smoke checks

This repository is designed so the core scripts can be checked without running the full research dataset. Full scientific runs may still require MZmine, mzML files, internet access for Wikidata, or local RDKit support for structure previews.

## Environment check

Create and activate the recommended environment:

```powershell
conda env create -f environment.yml
conda activate microfractionation
```

For an existing environment, verify imports with:

```powershell
python - <<'PY'
import customtkinter, matplotlib, numpy, openpyxl, pandas, PIL, pymzml, requests
print("Core imports OK")
PY
```

RDKit is optional for core Wikidata searches but required for molecule structure previews:

```powershell
python - <<'PY'
from rdkit import Chem
print("RDKit OK")
PY
```

## Syntax check

Run this from the repository root:

```powershell
python - <<'PY'
import ast
from pathlib import Path
for path in Path('.').rglob('*.py'):
    if '__pycache__' in path.parts:
        continue
    ast.parse(path.read_text(encoding='utf-8'))
    print(f'OK {path}')
PY
```

## Command-line help smoke test

Each core script should expose help without reading input files:

```powershell
python 00_plant_activity/p_00_00_plant_bioactivity_core.py --help
python 01_mzmine_pipeline/p_01_00_mzmine_pipeline_core.py --help
python 02_two_sided_plot/p_02_00_two_sided_plot_core.py --help
python 03_wikidata/p_03_00_wikidata_core.py --help
python 04_fraction_predictor/p_04_00_fraction_predictor_core.py --help
python 05_HRMS_exploration/p_05_00_hrms_exploration_core.py --help
```

## Script 01 dry run

Script 01 can validate command construction without launching MZmine:

```powershell
python 01_mzmine_pipeline/p_01_00_mzmine_pipeline_core.py `
  --config examples/01_mzmine_pipeline/mzmine_pipeline_config.json `
  --dry-run
```

A real Script 01 run requires a local `mzmine_console.exe`, valid mzML files, and MZmine batch templates suitable for the user's acquisition method. The bundled templates are starting points and are rewritten by Script 01 with the selected raw files and export folders.

## Example data

Files under `examples/` are synthetic schema examples. They are useful for checking column names and config shape, but they are not the research dataset and should not be interpreted scientifically.
