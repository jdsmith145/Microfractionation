# 01 - Integrated MZmine pipeline

## Purpose

This step wraps the MZmine-dependent part of the workflow into one guided run. It prepares the complete and fraction `.mzbatch` files, runs those batches through the MZmine command-line application, checks that the expected CSV files were produced, and then matches fraction features to the complete feature table.

This script contains the former standalone batch-preparation and fraction-matching stages so users do not need to run separate tools.

## Inputs

Required:

- sample name.
- sample `.mzML` files.
- blank/control `.mzML` files for the complete batch.
- complete-run MZmine `.mzbatch` template.
- fraction MZmine `.mzbatch` template configured for one representative fraction, usually `frac_01`.
- path to the MZmine command-line executable, for example `mzmine_console.exe`.
- output folders for generated batches, complete feature table CSV, fraction CSVs, and matched results.

Optional:

- MZmine `.mzuser` file. This is a login file created by MZmine after user login and usually stored under `C:/Users/<name>/.mzmine/users/`. Leave it empty if MZmine already has a valid current login on the computer.
- MZmine temp directory. This is a folder, not a file. MZmine uses it for temporary processing data and memory-mapped files; the default is `outputs/mzmine_temp` inside this script folder.
- MZmine memory mode. `none` is the safest default and uses less RAM; `all` can be faster on machines with enough RAM. Other valid values are `features`, `centroids`, `raw`, and `masses_features`.
- MZmine thread count. `auto` is recommended. Advanced users can type a number such as `4` or `8` to limit CPU usage.
- Continue after batch-version warnings. This passes `--ignore-parameter-warnings` to MZmine. It is enabled by default because bundled templates may have been saved by an older MZmine version than the one installed on a user's computer. Disable it only if you want MZmine to stop until the batch is opened, reviewed, and saved in the installed MZmine GUI.
- matching m/z and retention-time tolerances.

## Outputs

- configured complete batch: `{sample_name}_configured.mzbatch`.
- configured fraction batch: `{sample_name}_fraction_configured.mzbatch`.
- complete feature table expected from MZmine: `{sample_name}_complete_feature_table.csv`.
- fraction feature tables expected from MZmine: `frac_*.csv`.
- final matched table: `{sample_name}_filtered.csv`.

## GUI

```powershell
python p_01_01_mzmine_pipeline_gui.py
```

Recommended GUI workflow:

1. Fill the Workflow tab and save a config.
2. Fill the MZmine Runner tab and run **Dry run**.
3. Check the printed commands and expected output paths in the Log / Report tab. Dry run does not start MZmine and should not create result files.
4. Run the full pipeline.
5. Watch the stage bubbles and progress bar while MZmine runs. Progress is step-based: Python stages advance when they complete, and MZmine stages advance when MZmine reports each batch step.
6. Read the Log / Report tab for MZmine output and the final file summary.

## CLI examples

Create a template config:

```powershell
python p_01_00_mzmine_pipeline_core.py --make-template-config mzmine_pipeline_config.json
```

Preview the commands without running MZmine:

```powershell
python p_01_00_mzmine_pipeline_core.py --config mzmine_pipeline_config.json --dry-run
```

Run only batch preparation:

```powershell
python p_01_00_mzmine_pipeline_core.py --config mzmine_pipeline_config.json --stages prepare
```

Run only matching after the MZmine CSV files already exist:

```powershell
python p_01_00_mzmine_pipeline_core.py --config mzmine_pipeline_config.json --stages match
```

## Common issues

- MZmine batch templates must be optimized in MZmine before this script is used.
- The GUI expects `mzmine_console.exe` on Windows, not the normal graphical launcher `mzmine.exe`.
- If no `.mzuser` file is selected, MZmine must already have a valid current login. If login fails in headless mode, open MZmine once, login, then select the created `.mzuser` file from `C:/Users/<name>/.mzmine/users/`.
- If MZmine reports that parameter sets were updated since the batch was created, either enable **Continue after batch-version warnings** or open the generated `.mzbatch` in the installed MZmine GUI, review the changed parameters, save it, and run again.
- The matching step fails until the complete CSV and `frac_*.csv` files exist in the configured output folders.
- Use **Dry run** whenever moving the workflow to another computer or changing output folders.
