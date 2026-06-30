# 01 - MZmine batch setup, runner, and feature filtering

## MZmine 4.7.8 Portable Requirement

This step runs `mzmine_console.exe`, but MZmine is not bundled with the repository. Download MZmine 4.7.8 portable from https://github.com/mzmine/mzmine/releases/tag/v4.7.8, unzip it into the top-level `Microfractionation` folder, and keep the folder name `mzmine_Windows_portable_4.7.8`. The expected executable is `mzmine_Windows_portable_4.7.8/mzmine_console.exe`.

## Purpose

This step covers the MZmine-dependent part of the workflow in three focused tools:

- **Batch setup** creates the complete and fraction `.mzbatch` files.
- **MZmine runner** runs those batch files through `mzmine_console.exe` and checks that MZmine exported the expected CSV files.
- **Feature filtering** matches fraction features to the complete feature table and writes the filtered HPLC feature table.

The three tools use the same JSON config, so paths entered in Batch setup can be reused by the runner and filtering tools.

## Inputs

Required:

- sample name.
- sample `.mzML` files.
- blank/control `.mzML` files for the complete batch.
- complete-run MZmine `.mzbatch` template.
- fraction MZmine `.mzbatch` template configured for one representative fraction, usually `frac_01`.
- path to the MZmine command-line executable, for example `mzmine_console.exe`.
- output folders for generated batches, complete feature table CSV, fraction CSVs, and filtered results.

Optional:

- MZmine `.mzuser` file. This is a login file created by MZmine after user login and usually stored under `C:/Users/<name>/.mzmine/users/`. Leave it empty if MZmine already has a valid current login on the computer.
- MZmine temp directory. This is a folder, not a file. MZmine uses it for temporary processing data and memory-mapped files; the default is `outputs/mzmine_temp` inside this script folder.
- MZmine memory mode. `none` is the safest default and uses less RAM; `all` can be faster on machines with enough RAM. Other valid values are `features`, `centroids`, `raw`, and `masses_features`.
- MZmine thread count. `auto` is recommended. Advanced users can type a number such as `4` or `8` to limit CPU usage.
- Continue after batch-version warnings. This passes `--ignore-parameter-warnings` to MZmine. It is enabled by default because bundled templates may have been saved by an older MZmine version than the one installed on a user's computer. Disable it only if you want MZmine to stop until the batch is opened, reviewed, and saved in the installed MZmine GUI.
- matching m/z and retention-time tolerances.
- optional target m/z values and m/z tolerance for estimating one or more features' fraction-by-fraction peak-area proportions.

## Outputs

- configured complete batch: `{sample_name}_configured.mzbatch`.
- configured fraction batch: `{sample_name}_fraction_configured.mzbatch`.
- complete feature table expected from MZmine: `{sample_name}_complete_feature_table.csv`.
- fraction feature tables expected from MZmine: `frac_*.csv`.
- final filtered HPLC feature table: `{sample_slug}_filtered_feature_table.csv`.
- MS peak-area composition estimate: `{sample_slug}_fraction_purity_estimates.csv`. This table reports the dominant detected feature per fraction as `dominant_area_proportion_percent`. If one target m/z is entered, it also reports `target_area_proportion_percent` for that target. If multiple target m/z values are entered, it writes numbered target columns such as `target_1_mz_356p123_area_proportion_percent`, `target_2_mz_512p321_area_proportion_percent`, and so on. These are relative MS peak-area estimates, not detector-independent chemical purity measurements.

## GUI

```powershell
python p_01_01_batch_setup_gui.py
python p_01_02_mzmine_runner_gui.py
python p_01_03_feature_filter_gui.py
```

Recommended GUI workflow:

1. Open **Batch setup**, select the raw mzML files and templates, run setup, and save the JSON config.
2. Open **MZmine runner**, load the same config, choose the MZmine console settings, and run the complete and/or fraction batches.
3. Open **Feature filtering**, load the same config, confirm the complete CSV and `frac_*.csv` folder, and run filtering.

## CLI examples

Create a template config:

```powershell
python p_01_00_mzmine_pipeline_core.py --make-template-config mzmine_pipeline_config.json
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
- Target m/z values can be entered as a comma-, semicolon-, space-, or newline-separated list. The purity estimate table is based on feature-table peak areas. It is useful for prioritization but should not be interpreted as chemical purity because different compounds can ionize with different efficiency.
- If the fraction batch creates files named `OUTPUT_PLACEHOLDER.csv`, regenerate the fraction batch with the current Batch setup tool. The current tool forces fraction exports to `frac_01.csv`, `frac_02.csv`, and so on.

## Archived legacy GUI

The former all-in-one GUI is archived at `archived/p_01_01_mzmine_pipeline_gui.py`. The launcher and documentation now use the three focused GUIs above.
