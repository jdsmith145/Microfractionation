# Support Modules

This folder contains shared helper modules used by the microfractionation GUI and core scripts. These files are not standalone workflow steps.

- `bioassay_plate_reader.py`: reads direct plate-reader workbooks and converts plate wells to fraction or sample response values.
- `column_selector_dialog.py`: provides the searchable column selector used when a table has many columns.
- `example_data_helper.py`: opens bundled synthetic example files from GUI buttons.
- `gui_help_popover.py`: provides anchored `?` help popovers used across the GUIs.

User-facing workflow scripts are one level above this folder or inside numbered workflow folders. Keep shared GUI/data-loading helpers here so the main `scripts/` folder stays focused on workflow entry points.