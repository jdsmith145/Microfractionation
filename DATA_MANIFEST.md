# Data Manifest

This public package is intended for GitHub/publication distribution.

Included:

- `example_data/`: small synthetic input examples that show expected table layouts.
- `scripts/examples/`: script-specific synthetic examples, example configs, and expected-output notes.
- `scripts/01_mzmine_pipeline/templates/`: MZmine batch templates required by the batch setup step.

Not included:

- Real project input data.
- Saved user configs from local analyses.
- Generated output folders.
- The MZmine application itself.

Before running the MZmine steps, download MZmine 4.7.8 portable from https://github.com/mzmine/mzmine/releases/tag/v4.7.8, unzip it into the top-level `Microfractionation` folder, and keep the folder name `mzmine_Windows_portable_4.7.8`.

Users should create local `data/`, `configs/`, and `output/` folders for their own analyses, or let the launcher create the needed project paths.