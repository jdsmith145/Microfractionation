# 03 - Wikidata compound search

## Purpose

This step searches Wikidata for compounds by molecular formula and taxonomic context. It can be used for dereplication, literature triage, and visual inspection of candidate molecules.

## Inputs

Input table: CSV, XLSX, or XLS.

Required columns:

- genus column.
- formula column.

Optional columns:

- species column.

The species value may be the epithet only or the full binomial name. The script normalizes genus/species combinations before searching.

## Outputs

The output folder contains CSV files summarizing:

- input rows and search status.
- exact species matches, genus matches, family matches, and formula-only matches anywhere in Wikidata.
- diagnostic rows with no match or incomplete search result.
- optional merged rows by compound URL and/or canonical SMILES.

The GUI visualization tab can load previous result CSV files and display molecule structures when RDKit is installed. Users can search loaded results by name, SMILES, formula, species, or taxon text, copy selected molecule details, and export selected rows to a new CSV.

## GUI

```powershell
python p_03_01_wikidata_gui.py
```

In the GUI, **Output file label** is only a run label used in filenames. It is not the compound, formula, or taxon being searched. **Parallel searches** controls how many input rows are queried at the same time. The default value of `4` is usually a good balance; use `1` or `2` if Wikidata returns throttling or retry messages.

## CLI examples

Create a template config:

```powershell
python p_03_00_wikidata_core.py --make-template-config wikidata_template.json
```

Run a small search:

```powershell
python p_03_00_wikidata_core.py `
  --input ..\examples\03_wikidata\wikidata_input.csv `
  --genus-column genus `
  --species-column species `
  --formula-column formula `
  --output-dir ..\examples\03_wikidata\output `
  --output-label example_run `
  --max-workers 4 `
  --merge-compounds --merge-smiles
```

## Common issues

- This script requires internet access for Wikidata SPARQL queries.
- Normal successful queries are not deliberately delayed. Retry/backoff handling is still used for timeouts, HTTP errors, and Wikidata throttling.
- RDKit is optional for searching but needed for structure previews in the GUI.
- Formula formatting should be plain text such as `C21H22NO4`, not subscripted formula text.
