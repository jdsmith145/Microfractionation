# Synthetic examples

These files are artificial and are intended only to document input formats and support lightweight command-line checks. They are not research data.

The examples prefer CSV files even when the scripts also support Excel workbooks, because CSV is easy to inspect in a text editor.

`01_mzmine_pipeline` contains a dry-run config and small matching tables. The example can test path resolution and matching without a real MZmine installation; full MZmine execution still requires local mzML files, optimized `.mzbatch` templates, and MZmine command-line access.

`04_fraction_predictor` includes both the standard matched-RT-pairs example and a feature-order alignment example. The feature-order example demonstrates the landmark table used when HRMS and HPLC gradients are not comparable.
