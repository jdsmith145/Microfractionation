#!/usr/bin/env python3
"""Core Wikidata dereplication logic for taxon/formula searches."""
from __future__ import annotations

import os
import re
import time
import argparse
import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

URL = 'https://query.wikidata.org/sparql'
HEADERS = {
    'User-Agent': 'WikidataTaxonDereplicatorGUI/2.0',
    'Accept': 'application/sparql-results+json',
}

GENUS_RANK_QID = 'Q34740'
FAMILY_RANK_QID = 'Q35409'

MAX_RETRIES = 4
BASE_SLEEP = 2.0
REQUEST_TIMEOUT = 60
BETWEEN_QUERIES_SLEEP = 1.2


LOGGER = logging.getLogger("wikidata_dereplicator_core")


@dataclass
class FileInputSettings:
    path: str
    genus_column: str = "genus"
    species_column: str = "species"
    formula_column: str = "formula"
    sheet_name: str | None = None


@dataclass
class SearchSettings:
    output_dir: str
    search_name: str = ""
    merge_compounds: bool = False
    merge_smiles: bool = False


def safe_str(value) -> str:
    if value is None:
        return ''
    if pd.isna(value):
        return ''
    return str(value).strip()


def normalize_formula(formula: str) -> str:
    return re.sub(r'\s+', '', safe_str(formula))


def normalize_species_only(genus: str, species_value: str) -> str:
    genus = safe_str(genus)
    species_value = safe_str(species_value)
    if not species_value:
        return ''

    lowered = species_value.lower()
    genus_lower = genus.lower()
    if lowered == genus_lower:
        return ''
    if lowered.startswith(genus_lower + ' '):
        return species_value[len(genus):].strip()
    return species_value


def build_species_name(genus: str, species_only: str) -> str:
    genus = safe_str(genus)
    species_only = normalize_species_only(genus, species_only)
    return f"{genus} {species_only}".strip()


def to_subscript(formula: str) -> str:
    subscripts = str.maketrans({
        '0': '₀',
        '1': '₁',
        '2': '₂',
        '3': '₃',
        '4': '₄',
        '5': '₅',
        '6': '₆',
        '7': '₇',
        '8': '₈',
        '9': '₉',
    })
    return safe_str(formula).translate(subscripts)


def quote_string(value: str) -> str:
    return (
        str(value)
        .replace('\\', '\\\\')
        .replace('"', '\\"')
        .replace('\n', ' ')
        .strip()
    )


def slugify_suffix(text: str) -> str:
    cleaned = re.sub(r'[^a-zA-Z0-9]+', '_', safe_str(text).strip().lower()).strip('_')
    return f'_{cleaned}' if cleaned else ''


def extract_qid(entity_url: str) -> str:
    return entity_url.rstrip('/').split('/')[-1]


def get_binding_value(hit: dict, key: str, default: str = '') -> str:
    return hit.get(key, {}).get('value', default)


def run_query(session: Any, query: str) -> Tuple[Optional[List[dict]], Optional[str]]:
    response = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.post(
                URL,
                data={'format': 'json', 'query': query},
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                sleep_for = float(retry_after) if retry_after else BASE_SLEEP * attempt
                time.sleep(sleep_for)
                continue

            response.raise_for_status()
            data = response.json()
            return data['results']['bindings'], None

        except session.requests_module.exceptions.Timeout:
            error = f'timeout on attempt {attempt}/{MAX_RETRIES}'
        except session.requests_module.exceptions.HTTPError as exc:
            body_preview = response.text[:250].replace('\n', ' ') if response is not None else ''
            status_code = response.status_code if response is not None else 'unknown'
            error = f'HTTP {status_code} on attempt {attempt}/{MAX_RETRIES}: {body_preview or exc}'
        except Exception as exc:
            error = f'{type(exc).__name__} on attempt {attempt}/{MAX_RETRIES}: {exc}'

        if attempt < MAX_RETRIES:
            time.sleep(BASE_SLEEP * attempt)
        else:
            return None, error

    return None, 'unknown error'


def resolve_taxon_qid(session: Any, scientific_name: str, rank_qid: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    name_esc = quote_string(scientific_name)
    rank_filter = f'?taxon wdt:P105 wd:{rank_qid} .' if rank_qid else ''
    query = f'''
    SELECT DISTINCT ?taxon WHERE {{
      ?taxon wdt:P225 "{name_esc}" .
      {rank_filter}
    }}
    LIMIT 5
    '''
    hits, error = run_query(session, query)
    if hits is None:
        return None, error
    if not hits:
        return None, 'no taxon QID found'
    return extract_qid(hits[0]['taxon']['value']), None


def resolve_family_for_taxon(session: Any, taxon_qid: str) -> Tuple[Optional[Tuple[str, str]], Optional[str]]:
    query = f'''
    SELECT DISTINCT ?family ?familyName WHERE {{
      wd:{taxon_qid} wdt:P171* ?family .
      ?family wdt:P105 wd:{FAMILY_RANK_QID} ;
              wdt:P225 ?familyName .
    }}
    LIMIT 5
    '''
    hits, error = run_query(session, query)
    if hits is None:
        return None, error
    if not hits:
        return None, 'no family found'
    family_qid = extract_qid(hits[0]['family']['value'])
    family_name = get_binding_value(hits[0], 'familyName', '')
    return (family_qid, family_name), None


def build_exact_species_query(formula_sub: str, species_name: str) -> str:
    formula_esc = quote_string(formula_sub)
    species_esc = quote_string(species_name)
    return f'''
    SELECT DISTINCT ?compound ?compoundLabel ?taxon ?taxonName ?smiles ?inchi ?inchikey WHERE {{
      ?compound wdt:P274 "{formula_esc}" ;
                wdt:P703 ?taxon .
      ?taxon wdt:P225 "{species_esc}" ;
             wdt:P225 ?taxonName .

      OPTIONAL {{ ?compound wdt:P233 ?smiles . }}
      OPTIONAL {{ ?compound wdt:P234 ?inchi . }}
      OPTIONAL {{ ?compound wdt:P235 ?inchikey . }}

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 500
    '''


def build_taxon_scope_query(formula_sub: str, ancestor_qid: str) -> str:
    formula_esc = quote_string(formula_sub)
    return f'''
    SELECT DISTINCT ?compound ?compoundLabel ?taxon ?taxonName ?smiles ?inchi ?inchikey WHERE {{
      ?compound wdt:P274 "{formula_esc}" ;
                wdt:P703 ?taxon .
      ?taxon wdt:P171* wd:{ancestor_qid} ;
             wdt:P225 ?taxonName .

      OPTIONAL {{ ?compound wdt:P233 ?smiles . }}
      OPTIONAL {{ ?compound wdt:P234 ?inchi . }}
      OPTIONAL {{ ?compound wdt:P235 ?inchikey . }}

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 1200
    '''


def build_anywhere_query(formula_sub: str) -> str:
    formula_esc = quote_string(formula_sub)
    return f'''
    SELECT DISTINCT ?compound ?compoundLabel ?taxon ?taxonName ?smiles ?inchi ?inchikey WHERE {{
      ?compound wdt:P274 "{formula_esc}" .
      OPTIONAL {{
        ?compound wdt:P703 ?taxon .
        OPTIONAL {{ ?taxon wdt:P225 ?taxonName . }}
      }}
      OPTIONAL {{ ?compound wdt:P233 ?smiles . }}
      OPTIONAL {{ ?compound wdt:P234 ?inchi . }}
      OPTIONAL {{ ?compound wdt:P235 ?inchikey . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 300
    '''


def append_taxon_hits(output_list: List[dict], hits: List[dict], genus: str, species: str, family: str,
                      formula_plain: str, search_scope: str):
    for hit in hits:
        output_list.append({
            'Search Scope': search_scope,
            'Target Genus': genus,
            'Target Species': species,
            'Target Family': family,
            'Target Formula': formula_plain,
            'Compound URL': get_binding_value(hit, 'compound'),
            'Compound Name': get_binding_value(hit, 'compoundLabel', 'Unknown'),
            'Taxon URL': get_binding_value(hit, 'taxon'),
            'Taxon Name': get_binding_value(hit, 'taxonName', 'Unknown'),
            'Canonical SMILES': get_binding_value(hit, 'smiles', ''),
            'InChI': get_binding_value(hit, 'inchi', ''),
            'InChIKey': get_binding_value(hit, 'inchikey', ''),
        })


def append_anywhere_hits(output_list: List[dict], hits: List[dict], genus: str, species: str, family: str,
                         formula_plain: str):
    for hit in hits:
        output_list.append({
            'Search Scope': 'formula_exists_anywhere',
            'Target Genus': genus,
            'Target Species': species,
            'Target Family': family,
            'Target Formula': formula_plain,
            'Compound URL': get_binding_value(hit, 'compound'),
            'Compound Name': get_binding_value(hit, 'compoundLabel', 'Unknown'),
            'Taxon URL': get_binding_value(hit, 'taxon', ''),
            'Taxon Name': get_binding_value(hit, 'taxonName', ''),
            'Canonical SMILES': get_binding_value(hit, 'smiles', ''),
            'InChI': get_binding_value(hit, 'inchi', ''),
            'InChIKey': get_binding_value(hit, 'inchikey', ''),
        })


def deduplicate_rows(rows: List[dict], subset: List[str]) -> List[dict]:
    seen = set()
    output = []
    for row in rows:
        key = tuple(row.get(col, '') for col in subset)
        if key not in seen:
            seen.add(key)
            output.append(row)
    return output


def join_unique_values(values) -> str:
    seen = []
    for value in values:
        value = safe_str(value)
        if value and value not in seen:
            seen.append(value)
    return ', '.join(seen)


def merge_rows_by_group_cols(
    rows: List[dict],
    group_cols: List[str],
    *,
    require_nonempty_group_col: str | None = None,
) -> List[dict]:
    if not rows:
        return []
    if not group_cols:
        return rows

    df = pd.DataFrame(rows).fillna('')
    merged_rows = []
    for col in group_cols:
        if col not in df.columns:
            df[col] = ''

    if require_nonempty_group_col:
        if require_nonempty_group_col not in df.columns:
            return rows
        eligible_df = df[df[require_nonempty_group_col].astype(str).str.strip() != '']
        passthrough_df = df[df[require_nonempty_group_col].astype(str).str.strip() == '']
    else:
        eligible_df = df
        passthrough_df = df.iloc[0:0]

    for _, group in eligible_df.groupby(group_cols, dropna=False, sort=False):
        merged = {col: safe_str(group.iloc[0][col]) for col in group_cols}

        for col in df.columns:
            if col in group_cols:
                continue
            merged[col] = join_unique_values(group[col].tolist())

        merged_rows.append(merged)

    merged_rows.extend(passthrough_df.to_dict('records'))
    return merged_rows


def merge_rows_by_compound_url(rows: List[dict], group_cols: List[str]) -> List[dict]:
    return merge_rows_by_group_cols(rows, group_cols)


def resolve_column_name(df: pd.DataFrame, wanted_name: str, required: bool = True) -> Optional[str]:
    wanted_name = safe_str(wanted_name)
    if not wanted_name:
        return None if not required else None

    exact_map = {col: col for col in df.columns}
    if wanted_name in exact_map:
        return exact_map[wanted_name]

    normalized_map = {safe_str(col).strip().lower(): col for col in df.columns}
    match = normalized_map.get(wanted_name.strip().lower())
    if match:
        return match

    return None


def excel_sheet_names(path: str | Path) -> list[str]:
    path = Path(path).expanduser()
    if path.suffix.lower() not in {'.xlsx', '.xls'}:
        return []
    return list(pd.ExcelFile(path).sheet_names)


def load_input_table(
    input_path: str,
    genus_col: str,
    species_col: str,
    formula_col: str,
    sheet_name: str | int | None = None,
) -> pd.DataFrame:
    if not input_path:
        raise ValueError('Please choose an input file.')

    ext = os.path.splitext(input_path)[1].lower()
    if ext == '.csv':
        df = pd.read_csv(input_path)
    elif ext in {'.xlsx', '.xls'}:
        df = pd.read_excel(input_path, sheet_name=sheet_name if sheet_name not in {'', None} else 0)
    else:
        raise ValueError('Supported input files are .csv, .xlsx, and .xls.')

    genus_column = resolve_column_name(df, genus_col or 'genus', required=True)
    formula_column = resolve_column_name(df, formula_col or 'formula', required=True)
    species_column = resolve_column_name(df, species_col or 'species', required=False)

    missing_required = []
    if genus_column is None:
        missing_required.append(genus_col or 'genus')
    if formula_column is None:
        missing_required.append(formula_col or 'formula')
    if missing_required:
        raise ValueError(f'Missing required column(s): {missing_required}')

    output_rows = []
    for _, row in df.iterrows():
        genus = safe_str(row[genus_column])
        formula = normalize_formula(row[formula_column])
        species = safe_str(row[species_column]) if species_column else ''
        species = normalize_species_only(genus, species)

        if not genus or not formula:
            continue

        output_rows.append({
            'genus': genus,
            'species': species,
            'formula': formula,
        })

    if not output_rows:
        raise ValueError('No usable rows were found. Genus and formula must be filled in.')

    return pd.DataFrame(output_rows)


def load_input_table_from_settings(settings: FileInputSettings | dict) -> pd.DataFrame:
    if isinstance(settings, dict):
        settings = FileInputSettings(**settings)
    return load_input_table(
        settings.path,
        settings.genus_column,
        settings.species_column,
        settings.formula_column,
        sheet_name=settings.sheet_name,
    )


def save_csv_if_any(
    rows: List[dict],
    path: str,
    merge_compounds: bool = False,
    group_cols: Optional[List[str]] = None,
    merge_smiles: bool = False,
    smiles_group_cols: Optional[List[str]] = None,
):
    if not rows:
        return

    output_rows = rows
    if merge_compounds:
        output_rows = merge_rows_by_compound_url(output_rows, group_cols or [])
    if merge_smiles:
        output_rows = merge_rows_by_group_cols(
            output_rows,
            smiles_group_cols or [],
            require_nonempty_group_col='Canonical SMILES',
        )

    pd.DataFrame(output_rows).to_csv(path, index=False)


def run_search(data: pd.DataFrame, output_dir: str, search_suffix: str = '',
               log_callback=None, merge_compounds: bool = False, merge_smiles: bool = False) -> Dict[str, str]:
    os.makedirs(output_dir, exist_ok=True)

    def log(message: str):
        if log_callback:
            log_callback(message)

    exact_results: List[dict] = []
    genus_results: List[dict] = []
    family_results: List[dict] = []
    anywhere_results: List[dict] = []
    summary_rows: List[dict] = []
    diagnostic_rows: List[dict] = []

    genus_qid_cache: Dict[str, Optional[str]] = {}
    genus_qid_note_cache: Dict[str, str] = {}
    family_cache: Dict[str, Optional[Tuple[str, str]]] = {}
    family_note_cache: Dict[str, str] = {}

    output_paths = {
        'exact': os.path.join(output_dir, f'exact_species_matches{search_suffix}.csv'),
        'genus': os.path.join(output_dir, f'genus_matches{search_suffix}.csv'),
        'family': os.path.join(output_dir, f'family_matches{search_suffix}.csv'),
        'anywhere': os.path.join(output_dir, f'formula_exists_anywhere{search_suffix}.csv'),
        'diagnostic': os.path.join(output_dir, f'missing_or_problematic_formulas{search_suffix}.csv'),
        'summary': os.path.join(output_dir, f'search_summary{search_suffix}.csv'),
    }

    try:
        import requests  # type: ignore
    except Exception as exc:
        raise ImportError("The requests package is required for Wikidata queries. Install it with: pip install requests") from exc

    session = requests.Session()
    session.requests_module = requests  # type: ignore[attr-defined]
    total_rows = len(data)
    merge_note = 'ON' if merge_compounds else 'OFF'
    smiles_merge_note = 'ON' if merge_smiles else 'OFF'
    log(f'Starting Wikidata search for {total_rows} row(s)...')
    log(f'Merge duplicate compounds by URL: {merge_note}')
    log(f'Merge structural duplicates by canonical SMILES: {smiles_merge_note}')

    for idx, row in data.iterrows():
        genus = safe_str(row['genus'])
        species_only = normalize_species_only(genus, row.get('species', ''))
        species_name = build_species_name(genus, species_only)
        formula_plain = normalize_formula(row['formula'])
        formula_sub = to_subscript(formula_plain)

        log(f'[{idx + 1}/{total_rows}] {species_name or genus} | {formula_plain}')

        exact_count = 0
        genus_count = 0
        family_count = 0
        anywhere_count = 0

        exact_error = ''
        genus_error = ''
        family_error = ''
        anywhere_error = ''

        exact_note = ''
        genus_note = ''
        family_note = ''

        genus_qid = ''
        family_qid = ''
        family_name = ''

        if genus not in genus_qid_cache:
            qid, note = resolve_taxon_qid(session, genus, rank_qid=GENUS_RANK_QID)
            genus_qid_cache[genus] = qid
            genus_qid_note_cache[genus] = note or ''
            time.sleep(BETWEEN_QUERIES_SLEEP)

        genus_qid = genus_qid_cache.get(genus) or ''
        if not genus_qid:
            genus_note = genus_qid_note_cache.get(genus, '') or 'genus_qid_not_found'
            family_note = 'family_skipped_because_genus_qid_unavailable'
            log(f'  -> Could not resolve genus QID for {genus}: {genus_note}')
        else:
            if genus_qid not in family_cache:
                family_tuple, note = resolve_family_for_taxon(session, genus_qid)
                family_cache[genus_qid] = family_tuple
                family_note_cache[genus_qid] = note or ''
                time.sleep(BETWEEN_QUERIES_SLEEP)

            family_tuple = family_cache.get(genus_qid)
            if not family_tuple:
                family_note = family_note_cache.get(genus_qid, '') or 'family_not_found'
                log(f'  -> Family could not be resolved from {genus}: {family_note}')
            else:
                family_qid, family_name = family_tuple
                log(f'  -> Resolved family: {family_name} ({family_qid})')

        if species_only:
            exact_hits, error = run_query(session, build_exact_species_query(formula_sub, species_name))
            if exact_hits is None:
                exact_error = error or ''
                log(f'  -> Exact-species query failed: {exact_error}')
            elif exact_hits:
                exact_count = len(exact_hits)
                log(f'  -> Exact-species hits: {exact_count}')
                append_taxon_hits(exact_results, exact_hits, genus, species_name, family_name, formula_plain, 'exact_species')
            else:
                log('  -> Exact-species hits: 0')
            time.sleep(BETWEEN_QUERIES_SLEEP)
        else:
            exact_note = 'skipped_no_species_provided'
            log('  -> Exact-species search skipped (species blank).')

        if genus_qid:
            genus_hits, error = run_query(session, build_taxon_scope_query(formula_sub, genus_qid))
            if genus_hits is None:
                genus_error = error or ''
                log(f'  -> Genus query failed: {genus_error}')
            elif genus_hits:
                genus_count = len(genus_hits)
                log(f'  -> Genus hits: {genus_count}')
                append_taxon_hits(genus_results, genus_hits, genus, species_name, family_name, formula_plain, 'genus_or_descendant_taxon')
            else:
                log('  -> Genus hits: 0')
            time.sleep(BETWEEN_QUERIES_SLEEP)

            if family_qid:
                family_hits, error = run_query(session, build_taxon_scope_query(formula_sub, family_qid))
                if family_hits is None:
                    family_error = error or ''
                    log(f'  -> Family query failed: {family_error}')
                elif family_hits:
                    family_count = len(family_hits)
                    log(f'  -> Family hits: {family_count}')
                    append_taxon_hits(family_results, family_hits, genus, species_name, family_name, formula_plain, 'family_or_descendant_taxon')
                else:
                    log('  -> Family hits: 0')
                time.sleep(BETWEEN_QUERIES_SLEEP)

        anywhere_hits, error = run_query(session, build_anywhere_query(formula_sub))
        if anywhere_hits is None:
            anywhere_error = error or ''
            log(f'  -> Formula-anywhere query failed: {anywhere_error}')
        elif anywhere_hits:
            anywhere_count = len(anywhere_hits)
            log(f'  -> Formula exists somewhere in Wikidata: {anywhere_count} hit(s)')
            append_anywhere_hits(anywhere_results, anywhere_hits, genus, species_name, family_name, formula_plain)
        else:
            log('  -> Formula exists somewhere in Wikidata: 0 hits')
        time.sleep(BETWEEN_QUERIES_SLEEP)

        fatal_query_problem = any([exact_error, genus_error, family_error, anywhere_error])
        found_in_requested_scope = any([exact_count > 0, genus_count > 0, family_count > 0])

        if fatal_query_problem:
            interpretation = 'query_problem'
        elif found_in_requested_scope:
            interpretation = 'found_in_requested_taxon_scope'
        elif anywhere_count > 0:
            interpretation = 'absent_from_requested_taxon_scope_but_present_elsewhere'
        else:
            interpretation = 'possibly_absent_from_wikidata'

        summary_rows.append({
            'Target Genus': genus,
            'Target Species': species_name,
            'Target Family': family_name,
            'Target Formula': formula_plain,
            'Formula with Subscripts': formula_sub,
            'Genus QID': genus_qid,
            'Family QID': family_qid,
            'Exact Species Hits': exact_count,
            'Genus Hits': genus_count,
            'Family Hits': family_count,
            'Formula Exists Anywhere in Wikidata': anywhere_count,
            'Interpretation': interpretation,
        })

        diagnostic_rows.append({
            'Target Genus': genus,
            'Target Species': species_name,
            'Target Family': family_name,
            'Target Formula': formula_plain,
            'Formula with Subscripts': formula_sub,
            'Genus QID': genus_qid,
            'Family QID': family_qid,
            'Exact Species Hits': exact_count,
            'Genus Hits': genus_count,
            'Family Hits': family_count,
            'Formula Exists Anywhere in Wikidata': anywhere_count,
            'Exact Query Error': exact_error,
            'Genus Query Error': genus_error,
            'Family Query Error': family_error,
            'Anywhere Query Error': anywhere_error,
            'Exact Query Note': exact_note,
            'Genus Query Note': genus_note,
            'Family Query Note': family_note,
            'Interpretation': interpretation,
        })

    exact_results = deduplicate_rows(exact_results, ['Target Formula', 'Compound URL', 'Taxon URL'])
    genus_results = deduplicate_rows(genus_results, ['Target Formula', 'Compound URL', 'Taxon URL'])
    family_results = deduplicate_rows(family_results, ['Target Formula', 'Compound URL', 'Taxon URL'])
    anywhere_results = deduplicate_rows(anywhere_results, ['Target Formula', 'Compound URL', 'Taxon URL'])

    taxon_merge_cols = ['Search Scope', 'Target Genus', 'Target Species', 'Target Family', 'Target Formula', 'Compound URL']
    anywhere_merge_cols = ['Target Genus', 'Target Species', 'Target Family', 'Target Formula', 'Compound URL']
    taxon_smiles_merge_cols = ['Search Scope', 'Target Genus', 'Target Species', 'Target Family', 'Target Formula', 'Canonical SMILES']
    anywhere_smiles_merge_cols = ['Target Genus', 'Target Species', 'Target Family', 'Target Formula', 'Canonical SMILES']

    save_csv_if_any(exact_results, output_paths['exact'], merge_compounds=merge_compounds, group_cols=taxon_merge_cols, merge_smiles=merge_smiles, smiles_group_cols=taxon_smiles_merge_cols)
    save_csv_if_any(genus_results, output_paths['genus'], merge_compounds=merge_compounds, group_cols=taxon_merge_cols, merge_smiles=merge_smiles, smiles_group_cols=taxon_smiles_merge_cols)
    save_csv_if_any(family_results, output_paths['family'], merge_compounds=merge_compounds, group_cols=taxon_merge_cols, merge_smiles=merge_smiles, smiles_group_cols=taxon_smiles_merge_cols)
    save_csv_if_any(anywhere_results, output_paths['anywhere'], merge_compounds=merge_compounds, group_cols=anywhere_merge_cols, merge_smiles=merge_smiles, smiles_group_cols=anywhere_smiles_merge_cols)
    pd.DataFrame(diagnostic_rows).to_csv(output_paths['diagnostic'], index=False)
    pd.DataFrame(summary_rows).to_csv(output_paths['summary'], index=False)

    log('Finished. Files written:')
    for key in ['exact', 'genus', 'family', 'anywhere', 'diagnostic', 'summary']:
        if os.path.exists(output_paths[key]):
            log(f'  - {output_paths[key]}')
        else:
            log(f'  - {output_paths[key]} (not created because there were no rows)')

    return output_paths


def build_manual_dataframe(rows: list[dict]) -> pd.DataFrame:
    output_rows = []
    for row in rows:
        genus = safe_str(row.get('genus', ''))
        formula = normalize_formula(row.get('formula', ''))
        species = normalize_species_only(genus, row.get('species', ''))
        if genus and formula:
            output_rows.append({'genus': genus, 'species': species, 'formula': formula})
    if not output_rows:
        raise ValueError('No usable manual rows were found. Genus and formula are required.')
    return pd.DataFrame(output_rows)


def build_config_template() -> dict:
    return {
        'input_mode': 'file',
        'file_input': asdict(FileInputSettings(path='data/molecules.xlsx')),
        'manual_rows': [
            {'genus': 'Hypericum', 'species': 'olympicum', 'formula': 'C18H16O7'},
        ],
        'search': asdict(SearchSettings(output_dir='output', search_name='example', merge_compounds=False, merge_smiles=False)),
    }


def run_from_config(config: dict, *, log_callback=None) -> dict:
    search_cfg = config.get('search', {})
    search = SearchSettings(**search_cfg)
    input_mode = str(config.get('input_mode', 'file')).lower()
    if input_mode == 'manual':
        data = build_manual_dataframe(config.get('manual_rows', []))
    else:
        data = load_input_table_from_settings(config.get('file_input', {}))
    return run_search(
        data,
        output_dir=search.output_dir,
        search_suffix=slugify_suffix(search.search_name),
        log_callback=log_callback,
        merge_compounds=search.merge_compounds,
        merge_smiles=search.merge_smiles,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Search Wikidata for compounds by formula and taxonomic scope.')
    parser.add_argument('--config', help='JSON config generated by the GUI.')
    parser.add_argument('--make-template-config', help='Write a template JSON config and exit.')
    parser.add_argument('--input', help='Input CSV/XLSX file with genus, optional species, and formula columns.')
    parser.add_argument('--sheet-name', default=None, help='Excel sheet name, if --input is an Excel workbook.')
    parser.add_argument('--genus-column', default='genus')
    parser.add_argument('--species-column', default='species')
    parser.add_argument('--formula-column', default='formula')
    parser.add_argument('--output-dir', default='output')
    parser.add_argument('--search-name', default='')
    parser.add_argument('--merge-compounds', action='store_true')
    parser.add_argument('--merge-smiles', action='store_true')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format='%(levelname)s: %(message)s')
    if args.make_template_config:
        template_path = Path(args.make_template_config).expanduser()
        template_path.parent.mkdir(parents=True, exist_ok=True)
        template_path.write_text(json.dumps(build_config_template(), indent=2), encoding='utf-8')
        print(f'Template config written to: {template_path}')
        return 0
    if args.config:
        config = json.loads(Path(args.config).expanduser().read_text(encoding='utf-8'))
        paths = run_from_config(config, log_callback=LOGGER.info)
        print(json.dumps(paths, indent=2, ensure_ascii=False))
        return 0
    if not args.input:
        raise SystemExit('Provide --config, --make-template-config, or --input.')
    data = load_input_table(
        args.input,
        args.genus_column,
        args.species_column,
        args.formula_column,
        sheet_name=args.sheet_name,
    )
    paths = run_search(
        data,
        args.output_dir,
        search_suffix=slugify_suffix(args.search_name),
        log_callback=LOGGER.info,
        merge_compounds=bool(args.merge_compounds),
        merge_smiles=bool(args.merge_smiles),
    )
    print(json.dumps(paths, indent=2, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

