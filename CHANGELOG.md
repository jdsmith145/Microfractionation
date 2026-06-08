# Changelog

## 0.3.0 - 2026-06-05

- Renumbered the public workflow to `00` through `05` after merging the former standalone MZmine batch-preparation and fraction-matching steps into the integrated MZmine pipeline.
- Made `01_mzmine_pipeline` the only user-facing MZmine-processing step for the GitHub/publication package.
- Shifted downstream tools to `02_two_sided_plot`, `03_wikidata`, `04_fraction_predictor`, and `05_HRMS_exploration`.
- Updated script filenames, example folders, README commands, notebooks, and packaging notes to match the new public numbering.

## 0.2.0 - 2026-06-05

- Added direct plate-reader input support for plant/extract plots and fraction-response mapping.
- Added shared GUI help popovers and tightened CustomTkinter GUI behavior across active scripts.
- Added integrated MZmine pipeline progress reporting and made it the guided route for MZmine batch execution plus fraction-feature matching.
- Expanded fraction-predictor response preview, cutoff suggestions, response-grouping options, and flexible annotation-table matching.
- Revised documentation to describe response/fluorescence inputs separately from derived bioactivity interpretation.
- Prepared clean final-folder packaging rules for publication and GitHub distribution.

## 0.1.0 - 2026-05-20

- Standardized active workflow folders during development.
- Split core and GUI scripts for the main workflow steps.
- Added publication-oriented documentation, requirements, environment metadata, citation metadata, and synthetic examples.
- Added command-line entrypoint for fraction-feature matching during development.
- Renamed HRMS exploration scripts to the core/GUI development pattern used by the workflow.
- Added the integrated MZmine pipeline wrapper around batch preparation, MZmine headless execution, and fraction-feature matching.
