#!/usr/bin/env python3
"""CustomTkinter GUI for Wikidata taxon/formula dereplication."""
from __future__ import annotations

import json
import queue
import sys
import threading
import traceback
from pathlib import Path
from typing import Any

import pandas as pd

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))
_SHARED_DIR = _THIS_DIR.parent
if str(_SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(_SHARED_DIR))

from support.column_selector_dialog import choose_column
from support.example_data_helper import open_example
from support.gui_help_popover import HelpPopoverController

try:
    import p_03_00_wikidata_core as core
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "ERROR: Could not import p_03_00_wikidata_core.py.\n"
        "Place this GUI script in the same folder as the core script.\n\n"
        f"Details: {exc}"
    )


def main() -> int:
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox
    except Exception as exc:
        print("ERROR: Tkinter is not available in this Python environment.", file=sys.stderr)
        print(f"Details: {exc}", file=sys.stderr)
        return 2

    try:
        import customtkinter as ctk
    except Exception as exc:
        print("ERROR: CustomTkinter is not installed. Install it with: pip install customtkinter", file=sys.stderr)
        print(f"Details: {exc}", file=sys.stderr)
        return 2

    ctk.set_appearance_mode("dark")
    ctk.set_default_color_theme("blue")

    colors = {
        "bg": "#17191d",
        "surface": "#20242a",
        "card": "#252a31",
        "card_alt": "#2d333c",
        "entry": "#191d22",
        "border": "#3d4652",
        "text": "#f3f6fa",
        "muted": "#aab4c0",
        "accent": "#2563eb",
        "accent_hover": "#3b82f6",
        "success": "#2f8f5b",
        "success_hover": "#37a96a",
        "warning": "#b7791f",
        "danger": "#b54848",
        "danger_hover": "#9e3b3b",
    }
    font_header = ("Segoe UI", 23, "bold")
    font_subtitle = ("Segoe UI", 12)
    font_card_title = ("Segoe UI", 15, "bold")
    font_label = ("Segoe UI", 12)
    font_small = ("Segoe UI", 11)
    font_mono = ("Consolas", 11)

    state_file = _THIS_DIR / ".p_03_01_wikidata_gui_state.json"
    table_patterns = [("Tables", "*.csv *.xlsx *.xls"), ("All files", "*.*")]
    result_patterns = [("Wikidata result CSV", "*.csv"), ("All files", "*.*")]
    config_patterns = [("JSON configuration", "*.json"), ("All files", "*.*")]

    def load_json_safe(path: Path) -> dict[str, Any]:
        try:
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return {}

    def save_json_safe(path: Path, data: dict[str, Any]) -> None:
        try:
            path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    app_state = load_json_safe(state_file)
    root = ctk.CTk()
    root.title("Wikidata dereplicator")
    root.geometry("1320x900")
    root.minsize(1120, 760)
    root.configure(fg_color=colors["bg"])
    help_popovers = HelpPopoverController(root, ctk, colors, font_small)

    search_state = app_state.get("search", {}) or {}

    def limit_display(value: Any, default: int) -> str:
        normalized = core.normalize_query_limit(value, default)
        return "No limit" if normalized is None else str(normalized)

    file_state = app_state.get("file_input", {}) or {}
    manual_state = app_state.get("manual_rows", []) or []

    var_status = tk.StringVar(value="Ready.")
    var_input_mode = tk.StringVar(value=str(app_state.get("input_mode", "file")))
    var_search_name = tk.StringVar(value=str(search_state.get("output_label", search_state.get("search_name", ""))))
    var_output_dir = tk.StringVar(value=str(search_state.get("output_dir", str(_THIS_DIR / "output"))))
    var_merge = tk.BooleanVar(value=bool(search_state.get("merge_compounds", False)))
    var_merge_smiles = tk.BooleanVar(value=bool(search_state.get("merge_smiles", False)))
    var_max_workers = tk.StringVar(value=str(search_state.get("max_workers", core.DEFAULT_MAX_WORKERS)))
    limit_state = search_state.get("query_limits", {}) or {}
    var_limit_taxon = tk.StringVar(value=limit_display(limit_state.get("taxon_lookup"), core.DEFAULT_TAXON_LOOKUP_LIMIT))
    var_limit_exact = tk.StringVar(value=limit_display(limit_state.get("exact"), core.DEFAULT_EXACT_LIMIT))
    var_limit_rank = tk.StringVar(value=limit_display(limit_state.get("rank"), core.DEFAULT_RANK_LIMIT))
    var_limit_anywhere = tk.StringVar(value=limit_display(limit_state.get("anywhere"), core.DEFAULT_ANYWHERE_LIMIT))
    var_file_path = tk.StringVar(value=str(file_state.get("path", "")))
    var_sheet_name = tk.StringVar(value=str(file_state.get("sheet_name", "")))
    var_genus_col = tk.StringVar(value=str(file_state.get("genus_column", "genus")))
    var_species_col = tk.StringVar(value=str(file_state.get("species_column", "species")))
    var_formula_col = tk.StringVar(value=str(file_state.get("formula_column", "formula")))
    var_manual_genus = tk.StringVar(value="")
    var_manual_species = tk.StringVar(value="")
    var_manual_formula = tk.StringVar(value="")
    var_preview_summary = tk.StringVar(value="Load a file to preview its columns and first rows.")
    var_result_path = tk.StringVar(value=str((app_state.get("visualization", {}) or {}).get("result_path", "")))
    var_result_summary = tk.StringVar(value="Load a result CSV to browse molecule structures and annotations.")
    var_result_page_size = tk.StringVar(value=str((app_state.get("visualization", {}) or {}).get("page_size", "50")))
    var_result_page = tk.StringVar(value="Page 0 of 0")
    var_result_search = tk.StringVar(value="")
    var_result_selection = tk.StringVar(value="0 selected")

    manual_rows: list[dict[str, str]] = [
        {
            "genus": core.safe_str(row.get("genus", "")),
            "species": core.safe_str(row.get("species", "")),
            "formula": core.normalize_formula(row.get("formula", "")),
        }
        for row in manual_state
        if core.safe_str(row.get("genus", "")) and core.safe_str(row.get("formula", ""))
    ]
    table_df: pd.DataFrame | None = None
    table_columns: list[str] = []
    result_df: pd.DataFrame | None = None
    result_images: list[Any] = []
    result_selection_vars: list[Any] = []
    selected_result_indices: set[int] = set()
    log_queue: queue.Queue[str] = queue.Queue()
    result_queue: queue.Queue[tuple[bool, Any]] = queue.Queue()
    worker_state = {"running": False}

    def make_help(parent: Any, text: str) -> Any:
        return help_popovers.create_bubble(parent, text)

    def make_button(parent: Any, text: str, command: Any, *, primary: bool = False, success: bool = False, danger: bool = False, width: int | None = None) -> Any:
        color = colors["success"] if success else colors["danger"] if danger else colors["accent"] if primary else colors["card_alt"]
        hover = colors["success_hover"] if success else colors["danger_hover"] if danger else colors["accent_hover"] if primary else "#39414c"
        return ctk.CTkButton(parent, text=text, command=command, width=width or 112, height=38, corner_radius=10, fg_color=color, hover_color=hover)

    def example_button(parent: Any, relative_path: str, *, width: int = 82) -> Any:
        return make_button(parent, "Example", lambda: open_example(__file__, relative_path, messagebox=messagebox), width=width)

    def select_loaded_column(var: tk.StringVar, title: str) -> None:
        if not table_columns:
            show_toast("Load the input table first.", kind="warning")
            return
        choice = choose_column(root, ctk, colors, table_columns, title=title, current=var.get())
        if choice:
            var.set(choice)

    def make_entry(parent: Any, var: tk.StringVar, placeholder: str = "") -> Any:
        return ctk.CTkEntry(parent, textvariable=var, placeholder_text=placeholder, fg_color=colors["entry"], border_color=colors["border"], text_color=colors["text"], height=36, corner_radius=8)

    def make_combo(parent: Any, var: tk.StringVar, values: list[str]) -> Any:
        return ctk.CTkComboBox(parent, variable=var, values=values or [""], fg_color=colors["entry"], border_color=colors["border"], button_color=colors["accent"], button_hover_color=colors["accent_hover"], dropdown_fg_color=colors["card"], dropdown_hover_color=colors["card_alt"], text_color=colors["text"], dropdown_text_color=colors["text"], height=36, corner_radius=8)

    def make_checkbox(parent: Any, text: str, variable: tk.BooleanVar) -> Any:
        return ctk.CTkCheckBox(parent, text=text, variable=variable, font=font_small, text_color=colors["text"], fg_color=colors["accent"], hover_color=colors["accent_hover"], border_color=colors["border"], checkbox_width=20, checkbox_height=20)

    def parse_max_workers() -> int:
        try:
            return max(1, min(12, int(str(var_max_workers.get()).strip())))
        except (TypeError, ValueError):
            return core.DEFAULT_MAX_WORKERS

    def parse_query_limits() -> dict[str, int | None]:
        return {
            "taxon_lookup": core.normalize_query_limit(var_limit_taxon.get(), core.DEFAULT_TAXON_LOOKUP_LIMIT),
            "exact": core.normalize_query_limit(var_limit_exact.get(), core.DEFAULT_EXACT_LIMIT),
            "rank": core.normalize_query_limit(var_limit_rank.get(), core.DEFAULT_RANK_LIMIT),
            "anywhere": core.normalize_query_limit(var_limit_anywhere.get(), core.DEFAULT_ANYWHERE_LIMIT),
        }

    def set_limit_vars(limit_cfg: dict[str, Any]) -> None:
        var_limit_taxon.set(limit_display(limit_cfg.get("taxon_lookup"), core.DEFAULT_TAXON_LOOKUP_LIMIT))
        var_limit_exact.set(limit_display(limit_cfg.get("exact"), core.DEFAULT_EXACT_LIMIT))
        var_limit_rank.set(limit_display(limit_cfg.get("rank"), core.DEFAULT_RANK_LIMIT))
        var_limit_anywhere.set(limit_display(limit_cfg.get("anywhere"), core.DEFAULT_ANYWHERE_LIMIT))

    def labeled_widget(parent: Any, label: str, widget: Any, row: int, col: int, *, help_text: str, colspan: int = 1) -> None:
        label_frame = ctk.CTkFrame(parent, fg_color="transparent")
        label_frame.grid(row=row, column=col, sticky="ew", padx=(0, 10), pady=8)
        label_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(label_frame, text=label, font=font_label, text_color=colors["muted"], anchor="w").grid(row=0, column=0, sticky="w")
        make_help(label_frame, help_text).grid(row=0, column=1, sticky="e", padx=(6, 0))
        widget.grid(row=row, column=col + 1, columnspan=colspan, sticky="ew", padx=(0, 18), pady=8)

    def make_card(parent: Any, title: str) -> Any:
        card = ctk.CTkFrame(parent, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=16)
        card.pack(fill="x", padx=6, pady=(0, 12))
        card.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(card, text=title, font=font_card_title, text_color=colors["text"], anchor="w").grid(row=0, column=0, sticky="ew", padx=18, pady=(16, 10))
        body = ctk.CTkFrame(card, fg_color="transparent")
        body.grid(row=1, column=0, sticky="ew", padx=18, pady=(0, 18))
        for col in range(6):
            body.grid_columnconfigure(col, weight=1 if col % 2 == 1 else 0)
        return body

    def append_log(message: str) -> None:
        txt_log.configure(state="normal")
        txt_log.insert("end", message + "\n")
        txt_log.see("end")
        txt_log.configure(state="disabled")

    def show_toast(message: str, *, kind: str = "info") -> None:
        color = colors["success"] if kind == "success" else colors["danger"] if kind == "error" else colors["warning"] if kind == "warning" else colors["accent"]
        toast = ctk.CTkFrame(root, fg_color=color, corner_radius=14)
        ctk.CTkLabel(toast, text=message, font=font_small, text_color="white", justify="left", wraplength=420, padx=16, pady=11).pack(fill="both", expand=True)
        toast.place(relx=1.0, rely=0.0, x=-26, y=86, anchor="ne")
        toast.lift()
        root.after(3600, lambda: toast.destroy() if toast.winfo_exists() else None)

    def current_config() -> dict[str, Any]:
        return {
            "input_mode": var_input_mode.get(),
            "file_input": {
                "path": var_file_path.get().strip(),
                "genus_column": var_genus_col.get().strip() or "genus",
                "species_column": var_species_col.get().strip() or "species",
                "formula_column": var_formula_col.get().strip() or "formula",
                "sheet_name": var_sheet_name.get().strip() or None,
            },
            "manual_rows": list(manual_rows),
            "search": {
                "output_dir": var_output_dir.get().strip() or str(_THIS_DIR / "output"),
                "output_label": var_search_name.get().strip(),
                "search_name": var_search_name.get().strip(),
                "merge_compounds": bool(var_merge.get()),
                "merge_smiles": bool(var_merge_smiles.get()),
                "max_workers": parse_max_workers(),
                "query_limits": parse_query_limits(),
            },
            "visualization": {
                "result_path": var_result_path.get().strip(),
                "page_size": var_result_page_size.get(),
            },
        }

    def save_state() -> None:
        save_json_safe(state_file, current_config())

    def update_input_mode(*_args: Any) -> None:
        if var_input_mode.get() == "manual":
            manual_panel.grid(row=0, column=0, sticky="ew")
            file_panel.grid_remove()
        else:
            file_panel.grid(row=0, column=0, sticky="ew")
            manual_panel.grid_remove()
        save_state()

    def refresh_manual_list() -> None:
        manual_list.delete(0, "end")
        for row in manual_rows:
            species = f" {row['species']}" if row.get("species") else ""
            manual_list.insert("end", f"{row['genus']}{species} | {row['formula']}")

    def refresh_column_combos() -> None:
        values = ["", *table_columns]
        combo_genus.configure(values=values)
        combo_species.configure(values=values)
        combo_formula.configure(values=values)

    def load_preview_file() -> None:
        nonlocal table_df, table_columns
        path_text = var_file_path.get().strip()
        if not path_text:
            show_toast("Choose an input file first.", kind="warning")
            return
        path = Path(path_text).expanduser()
        if not path.exists():
            messagebox.showerror("Input file", f"File not found:\n{path}")
            return
        try:
            sheets = core.excel_sheet_names(path)
            if sheets:
                sheet_combo.configure(values=sheets)
                if var_sheet_name.get().strip() not in sheets:
                    var_sheet_name.set(sheets[0])
            else:
                sheet_combo.configure(values=[""])
                var_sheet_name.set("")
            table_df = core.load_input_table(
                str(path),
                var_genus_col.get() or "genus",
                var_species_col.get() or "species",
                var_formula_col.get() or "formula",
                sheet_name=var_sheet_name.get().strip() or None,
            )
            raw_df = pd.read_excel(path, sheet_name=var_sheet_name.get().strip() or 0) if path.suffix.lower() in {".xlsx", ".xls"} else pd.read_csv(path)
            table_columns = [str(col) for col in raw_df.columns]
            refresh_column_combos()
            var_preview_summary.set(f"Loaded {len(table_df)} usable row(s). Columns: {', '.join(table_columns[:8])}{'...' if len(table_columns) > 8 else ''}")
            with pd.option_context("display.max_columns", 12, "display.width", 180, "display.max_colwidth", 28):
                preview = raw_df.head(10).to_string(index=False)
            txt_preview.configure(state="normal")
            txt_preview.delete("1.0", "end")
            txt_preview.insert("end", preview)
            txt_preview.configure(state="disabled")
            append_log(f"Loaded input preview: {path} ({len(table_df)} usable row(s)).")
            show_toast("Input file loaded.", kind="success")
        except Exception as exc:
            messagebox.showerror("Input preview", str(exc))

    def browse_input_file() -> None:
        path = filedialog.askopenfilename(title="Choose input table", initialdir=str(_THIS_DIR), filetypes=table_patterns)
        if path:
            var_file_path.set(path)
            load_preview_file()

    def browse_output_dir() -> None:
        path = filedialog.askdirectory(title="Choose output folder", initialdir=str(_THIS_DIR))
        if path:
            var_output_dir.set(path)

    def first_nonempty(row: pd.Series, columns: list[str]) -> str:
        for col in columns:
            if col in row.index:
                value = core.safe_str(row.get(col, ""))
                if value:
                    return value
        return ""

    def split_joined_values(value: Any) -> list[str]:
        text = core.safe_str(value)
        if not text:
            return []
        values = [piece.strip() for piece in text.split(",") if piece.strip()]
        seen: list[str] = []
        for item in values:
            if item not in seen:
                seen.append(item)
        return seen

    def row_search_scope(row: pd.Series) -> str:
        return first_nonempty(row, ["Search Scope", "Interpretation"])

    def source_taxon_summary(row: pd.Series) -> str:
        taxon_values = split_joined_values(first_nonempty(row, ["Taxon Name", "taxonName", "organism"]))
        scope = row_search_scope(row)
        if taxon_values:
            label = "Found in taxon" if len(taxon_values) == 1 else "Found in taxa"
            return f"{label}: {', '.join(taxon_values)}"
        if scope == "formula_exists_anywhere":
            return "Formula exists in Wikidata; no found-in-taxon statement was exported for this row."
        target = first_nonempty(row, ["Target Species", "Target Genus", "Target Family"])
        return f"Target taxon: {target}" if target else "Taxon not listed"

    def source_taxon_copy_text(row: pd.Series) -> str:
        taxon_values = split_joined_values(first_nonempty(row, ["Taxon Name", "taxonName", "organism"]))
        if taxon_values:
            return ", ".join(taxon_values)
        if row_search_scope(row) == "formula_exists_anywhere":
            return "No found-in-taxon statement exported"
        return first_nonempty(row, ["Target Species", "Target Genus", "Target Family"]) or "Organism not listed"

    def first_smiles(value: str) -> str:
        value = core.safe_str(value)
        if not value:
            return ""
        for separator in [",", ";", "|"]:
            if separator in value:
                return core.safe_str(value.split(separator)[0])
        return value

    def make_structure_image(smiles: str) -> Any | None:
        smiles = first_smiles(smiles)
        if not smiles:
            return None
        try:
            from rdkit import Chem  # type: ignore
            from rdkit.Chem import Draw  # type: ignore
        except Exception:
            return None
        try:
            mol = Chem.MolFromSmiles(smiles)
            if mol is None:
                return None
            image = Draw.MolToImage(mol, size=(300, 210))
            ctk_image = ctk.CTkImage(light_image=image, dark_image=image, size=(300, 210))
            result_images.append(ctk_image)
            return ctk_image
        except Exception:
            return None

    result_page_state = {"page": 0}

    def result_page_size() -> int:
        try:
            return max(1, int(var_result_page_size.get()))
        except Exception:
            return 50

    def result_filtered_df() -> pd.DataFrame | None:
        if result_df is None:
            return None
        df = result_df.fillna("")
        query = core.safe_str(var_result_search.get()).lower()
        if not query:
            return df

        preferred_cols = [
            "Compound Name",
            "Taxon Name",
            "Target Species",
            "Target Genus",
            "Target Family",
            "Canonical SMILES",
            "SMILES",
            "Target Formula",
        ]
        cols = [col for col in preferred_cols if col in df.columns]
        if not cols:
            cols = list(df.columns)
        mask = pd.Series(False, index=df.index)
        for col in cols:
            mask = mask | df[col].astype(str).str.lower().str.contains(query, na=False, regex=False)
        return df[mask]

    def result_page_count() -> int:
        df = result_filtered_df()
        if df is None or df.empty:
            return 0
        size = result_page_size()
        return max(1, (len(df) + size - 1) // size)

    def clamp_result_page() -> None:
        pages = result_page_count()
        if pages == 0:
            result_page_state["page"] = 0
            return
        result_page_state["page"] = min(max(0, result_page_state["page"]), pages - 1)

    def update_result_page_controls() -> None:
        pages = result_page_count()
        if pages == 0:
            var_result_page.set("Page 0 of 0")
            try:
                prev_page_button.configure(state="disabled")
                next_page_button.configure(state="disabled")
            except Exception:
                pass
            return
        page = result_page_state["page"]
        var_result_page.set(f"Page {page + 1} of {pages}")
        try:
            prev_page_button.configure(state="normal" if page > 0 else "disabled")
            next_page_button.configure(state="normal" if page < pages - 1 else "disabled")
        except Exception:
            pass

    def reset_result_scroll_position() -> None:
        try:
            result_scroll._parent_canvas.yview_moveto(0)  # type: ignore[attr-defined]
        except Exception:
            pass

    def change_result_page(delta: int) -> None:
        result_page_state["page"] += delta
        clamp_result_page()
        render_result_cards()

    def change_result_page_size(_choice: str | None = None) -> None:
        result_page_state["page"] = 0
        render_result_cards()
        save_state()

    def apply_result_search(_event: Any = None) -> None:
        result_page_state["page"] = 0
        render_result_cards()

    def clear_result_search() -> None:
        var_result_search.set("")
        apply_result_search()

    def update_result_selection_summary() -> None:
        count = len(selected_result_indices)
        var_result_selection.set(f"{count} selected")
        try:
            export_selected_button.configure(state="normal" if count else "disabled")
        except Exception:
            pass

    def toggle_result_selection(row_index: int, selected: bool) -> None:
        if selected:
            selected_result_indices.add(int(row_index))
        else:
            selected_result_indices.discard(int(row_index))
        update_result_selection_summary()

    def copy_result_row(row: pd.Series) -> None:
        compound_name = first_nonempty(row, ["Compound Name", "compoundName", "compound", "name"]) or "Unnamed compound"
        taxon_name = source_taxon_copy_text(row)
        smiles = first_nonempty(row, ["Canonical SMILES", "SMILES", "smiles"])
        formula = first_nonempty(row, ["Target Formula", "Molecular Formula", "Formula", "formula"])
        lines = [f"Name: {compound_name}", f"Organism: {taxon_name}"]
        if formula:
            lines.append(f"Formula: {formula}")
        if smiles:
            lines.append(f"SMILES: {first_smiles(smiles)}")
        root.clipboard_clear()
        root.clipboard_append("\n".join(lines))
        show_toast("Molecule details copied.", kind="success")

    def export_selected_results() -> None:
        if result_df is None or not selected_result_indices:
            show_toast("Select at least one molecule first.", kind="warning")
            return
        selected_existing = [idx for idx in result_df.index if int(idx) in selected_result_indices]
        if not selected_existing:
            show_toast("Selected rows are no longer available.", kind="warning")
            return
        path = filedialog.asksaveasfilename(
            title="Export selected molecules",
            initialdir=str(Path(var_result_path.get()).expanduser().parent if var_result_path.get().strip() else _THIS_DIR),
            defaultextension=".csv",
            filetypes=[("CSV file", "*.csv"), ("All files", "*.*")],
            initialfile="selected_wikidata_molecules.csv",
        )
        if not path:
            return
        try:
            result_df.loc[selected_existing].to_csv(path, index=False)
            show_toast(f"Exported {len(selected_existing)} selected molecule(s).", kind="success")
        except Exception as exc:
            messagebox.showerror("Export selected", str(exc))

    def render_result_cards() -> None:
        nonlocal result_images
        nonlocal result_selection_vars
        result_images = []
        result_selection_vars = []
        for child in result_scroll.winfo_children():
            child.destroy()
        filtered_df = result_filtered_df()
        if filtered_df is None or filtered_df.empty:
            empty = ctk.CTkFrame(result_scroll, fg_color="#edf1f5", corner_radius=16)
            empty.pack(fill="x", padx=8, pady=8)
            title = "No matching rows." if result_df is not None and not result_df.empty else "No result rows loaded."
            detail = "Clear or change the search text." if result_df is not None and not result_df.empty else "Load one of the CSV files created by the search, such as exact_species_matches, genus_matches, family_matches, or formula_exists_anywhere."
            ctk.CTkLabel(empty, text=title, font=font_card_title, text_color="#1f2937").pack(anchor="w", padx=18, pady=(16, 4))
            ctk.CTkLabel(empty, text=detail, font=font_small, text_color="#4b5563", wraplength=760, justify="left").pack(anchor="w", padx=18, pady=(0, 16))
            update_result_page_controls()
            update_result_selection_summary()
            return

        clamp_result_page()
        page_size = result_page_size()
        start = result_page_state["page"] * page_size
        stop = min(start + page_size, len(filtered_df))
        page_df = filtered_df.iloc[start:stop]
        if var_result_search.get().strip():
            var_result_summary.set(f"Loaded {len(result_df)} row(s). {len(filtered_df)} match search. Showing matches {start + 1}-{stop}.")
        else:
            var_result_summary.set(f"Loaded {len(result_df)} row(s). Showing rows {start + 1}-{stop}.")

        for idx, row in page_df.iterrows():
            compound_name = first_nonempty(row, ["Compound Name", "compoundName", "compound", "name"]) or "Unnamed compound"
            taxon_name = source_taxon_summary(row)
            formula = first_nonempty(row, ["Target Formula", "Molecular Formula", "Formula", "formula"])
            scope = row_search_scope(row)
            smiles = first_nonempty(row, ["Canonical SMILES", "SMILES", "smiles"])
            compound_url = first_nonempty(row, ["Compound URL", "compound"])
            taxon_url = first_nonempty(row, ["Taxon URL", "taxon"])

            outer = ctk.CTkFrame(result_scroll, fg_color="#edf1f5", border_color="#d7dde5", border_width=1, corner_radius=18)
            outer.pack(fill="x", padx=8, pady=(0, 14))
            outer.grid_columnconfigure(1, weight=1)

            structure_box = ctk.CTkFrame(outer, fg_color="#ffffff", border_color="#dde3ea", border_width=1, corner_radius=16, width=330, height=242)
            structure_box.grid(row=0, column=0, rowspan=2, sticky="nw", padx=18, pady=18)
            structure_box.grid_propagate(False)
            structure_box.grid_columnconfigure(0, weight=1)
            structure_box.grid_rowconfigure(0, weight=1)
            image = make_structure_image(smiles)
            if image is not None:
                ctk.CTkLabel(structure_box, image=image, text="").grid(row=0, column=0, sticky="nsew", padx=14, pady=14)
            else:
                placeholder = "Structure preview unavailable"
                if smiles:
                    placeholder += f"\n\nSMILES:\n{first_smiles(smiles)}"
                else:
                    placeholder += "\n\nNo canonical SMILES was found in this row."
                ctk.CTkLabel(structure_box, text=placeholder, font=font_small, text_color="#4b5563", justify="center", wraplength=280).grid(row=0, column=0, sticky="nsew", padx=18, pady=18)

            text_box = ctk.CTkFrame(outer, fg_color="transparent")
            text_box.grid(row=0, column=1, sticky="nsew", padx=(0, 18), pady=(20, 8))
            text_box.grid_columnconfigure(0, weight=1)
            ctk.CTkLabel(text_box, text=compound_name, font=("Segoe UI", 16, "bold"), text_color="#111827", anchor="w", justify="left", wraplength=760).grid(row=0, column=0, sticky="ew")
            ctk.CTkLabel(text_box, text=taxon_name, font=font_label, text_color="#374151", anchor="w", justify="left", wraplength=760).grid(row=1, column=0, sticky="ew", pady=(4, 0))
            details = []
            if formula:
                details.append(f"Formula: {formula}")
            if scope:
                details.append(f"Source: {scope}")
            if smiles:
                details.append(f"Canonical SMILES: {first_smiles(smiles)}")
            ctk.CTkLabel(text_box, text="\n".join(details) if details else "No extra annotation columns were found.", font=font_small, text_color="#4b5563", anchor="w", justify="left", wraplength=760).grid(row=2, column=0, sticky="ew", pady=(12, 0))
            card_actions = ctk.CTkFrame(text_box, fg_color="transparent")
            card_actions.grid(row=3, column=0, sticky="w", pady=(16, 0))
            selected_var = tk.BooleanVar(value=int(idx) in selected_result_indices)
            result_selection_vars.append(selected_var)
            ctk.CTkCheckBox(card_actions, text="Select", variable=selected_var, command=lambda row_idx=int(idx), var=selected_var: toggle_result_selection(row_idx, bool(var.get())), font=font_small, text_color="#1f2937", fg_color=colors["accent"], hover_color=colors["accent_hover"], border_color="#9ca3af", checkbox_width=18, checkbox_height=18).pack(side="left", padx=(0, 12))
            make_button(card_actions, "Copy", lambda row=row.copy(): copy_result_row(row), width=72).pack(side="left")

            annotation_text = f"Row {idx + 1}"
            if compound_url:
                annotation_text += f" | Compound URL: {compound_url}"
            if taxon_url:
                annotation_text += f" | Taxon URL: {taxon_url}"
            ctk.CTkLabel(outer, text=annotation_text, font=font_small, text_color="#4b5563", fg_color="#ffffff", corner_radius=12, anchor="w", justify="left", wraplength=780, height=42).grid(row=1, column=1, sticky="ew", padx=(0, 18), pady=(0, 18))

        update_result_page_controls()
        update_result_selection_summary()
        reset_result_scroll_position()

    def load_result_file(path_text: str | None = None) -> None:
        nonlocal result_df
        path_text = path_text or var_result_path.get().strip()
        if not path_text:
            show_toast("Choose a result CSV first.", kind="warning")
            return
        path = Path(path_text).expanduser()
        if not path.exists():
            messagebox.showerror("Result file", f"File not found:\n{path}")
            return
        try:
            df = pd.read_csv(path).dropna(how="all")
            if "formula_exists_anywhere" in path.name and "Search Scope" not in df.columns:
                df["Search Scope"] = "formula_exists_anywhere"
            result_df = df.fillna("")
            selected_result_indices.clear()
            result_page_state["page"] = 0
            var_result_path.set(str(path))
            var_result_summary.set(f"Loaded {len(result_df)} row(s) from {path.name}.")
            render_result_cards()
            save_state()
            show_toast("Result visualization loaded.", kind="success")
        except Exception as exc:
            messagebox.showerror("Load result", str(exc))

    def browse_result_file() -> None:
        initial = Path(var_output_dir.get().strip() or _THIS_DIR)
        if not initial.exists():
            initial = _THIS_DIR
        path = filedialog.askopenfilename(title="Choose result CSV", initialdir=str(initial), filetypes=result_patterns)
        if path:
            var_result_path.set(path)
            load_result_file(path)

    def add_manual_row() -> None:
        genus = core.safe_str(var_manual_genus.get())
        species = core.normalize_species_only(genus, var_manual_species.get())
        formula = core.normalize_formula(var_manual_formula.get())
        if not genus or not formula:
            messagebox.showerror("Manual row", "Genus and formula are required. Species can be left blank.")
            return
        manual_rows.append({"genus": genus, "species": species, "formula": formula})
        var_manual_species.set("")
        var_manual_formula.set("")
        refresh_manual_list()
        save_state()

    def remove_manual_row() -> None:
        selection = list(manual_list.curselection())
        for index in sorted(selection, reverse=True):
            del manual_rows[index]
        refresh_manual_list()
        save_state()

    def clear_log() -> None:
        txt_log.configure(state="normal")
        txt_log.delete("1.0", "end")
        txt_log.configure(state="disabled")

    def build_input_dataframe() -> pd.DataFrame:
        if var_input_mode.get() == "manual":
            if not manual_rows and var_manual_genus.get().strip() and var_manual_formula.get().strip():
                add_manual_row()
            return core.build_manual_dataframe(manual_rows)
        return core.load_input_table(
            var_file_path.get().strip(),
            var_genus_col.get().strip() or "genus",
            var_species_col.get().strip() or "species",
            var_formula_col.get().strip() or "formula",
            sheet_name=var_sheet_name.get().strip() or None,
        )

    def start_run() -> None:
        if worker_state["running"]:
            return
        try:
            data = build_input_dataframe()
            outdir = Path(var_output_dir.get().strip() or (_THIS_DIR / "output")).expanduser()
            outdir.mkdir(parents=True, exist_ok=True)
            suffix = core.slugify_suffix(var_search_name.get())
            merge = bool(var_merge.get())
            merge_smiles = bool(var_merge_smiles.get())
            max_workers = parse_max_workers()
            query_limits = parse_query_limits()
        except Exception as exc:
            messagebox.showerror("Cannot start", str(exc))
            return
        save_state()
        clear_log()
        append_log("New Wikidata run started.")
        append_log(f"Input rows: {len(data)}")
        append_log(f"Output folder: {outdir}")
        append_log(f"Parallel searches: {max_workers}")
        worker_state["running"] = True
        run_button.configure(state="disabled")
        progress.start()
        var_status.set("Running Wikidata queries...")

        def log_callback(message: str) -> None:
            log_queue.put(message)

        def worker() -> None:
            try:
                result_queue.put((True, core.run_search(data, str(outdir), search_suffix=suffix, log_callback=log_callback, merge_compounds=merge, merge_smiles=merge_smiles, max_workers=max_workers, query_limits=query_limits)))
            except Exception as exc:
                result_queue.put((False, (exc, traceback.format_exc())))

        threading.Thread(target=worker, daemon=True).start()

    def poll_queues() -> None:
        try:
            while True:
                append_log(log_queue.get_nowait())
        except queue.Empty:
            pass
        try:
            while True:
                ok, payload = result_queue.get_nowait()
                worker_state["running"] = False
                run_button.configure(state="normal")
                progress.stop()
                if ok:
                    var_status.set("Finished.")
                    paths = payload
                    created = [path for path in paths.values() if Path(path).exists()]
                    append_log("")
                    append_log(f"Finished. Created {len(created)} output file(s).")
                    for key in ["exact", "genus", "family", "anywhere"]:
                        candidate = paths.get(key)
                        if candidate and Path(candidate).exists():
                            var_result_path.set(candidate)
                            load_result_file(candidate)
                            break
                    show_toast("Wikidata search finished.", kind="success")
                    messagebox.showinfo("Finished", "Search finished.\n\nCreated files:\n" + "\n".join(created) if created else "Search finished. No result CSVs were created.")
                else:
                    exc, tb = payload
                    var_status.set("Error.")
                    append_log(f"ERROR: {exc}\n{tb}")
                    show_toast("Wikidata search failed.", kind="error")
                    messagebox.showerror("Run failed", str(exc))
        except queue.Empty:
            pass
        root.after(150, poll_queues)

    def save_config_dialog() -> None:
        path = filedialog.asksaveasfilename(title="Save configuration", initialdir=str(_THIS_DIR), defaultextension=".json", filetypes=config_patterns, initialfile="wikidata_config.json")
        if path:
            Path(path).write_text(json.dumps(current_config(), indent=2, ensure_ascii=False), encoding="utf-8")
            show_toast("Configuration saved.", kind="success")

    def apply_config(config: dict[str, Any]) -> None:
        nonlocal manual_rows
        var_input_mode.set(str(config.get("input_mode", "file")))
        file_cfg = config.get("file_input", {}) or {}
        search_cfg = config.get("search", {}) or {}
        var_file_path.set(str(file_cfg.get("path", "")))
        var_sheet_name.set(str(file_cfg.get("sheet_name", "") or ""))
        var_genus_col.set(str(file_cfg.get("genus_column", "genus")))
        var_species_col.set(str(file_cfg.get("species_column", "species")))
        var_formula_col.set(str(file_cfg.get("formula_column", "formula")))
        var_output_dir.set(str(search_cfg.get("output_dir", str(_THIS_DIR / "output"))))
        var_search_name.set(str(search_cfg.get("output_label", search_cfg.get("search_name", ""))))
        var_merge.set(bool(search_cfg.get("merge_compounds", False)))
        var_merge_smiles.set(bool(search_cfg.get("merge_smiles", False)))
        var_max_workers.set(str(search_cfg.get("max_workers", core.DEFAULT_MAX_WORKERS)))
        set_limit_vars(search_cfg.get("query_limits", {}) or {})
        viz_cfg = config.get("visualization", {}) or {}
        var_result_path.set(str(viz_cfg.get("result_path", "")))
        var_result_page_size.set(str(viz_cfg.get("page_size", var_result_page_size.get())))
        manual_rows = list(config.get("manual_rows", []) or [])
        refresh_manual_list()
        update_input_mode()

    def load_config_dialog() -> None:
        path = filedialog.askopenfilename(title="Load configuration", initialdir=str(_THIS_DIR), filetypes=config_patterns)
        if not path:
            return
        try:
            apply_config(json.loads(Path(path).read_text(encoding="utf-8")))
            if var_file_path.get().strip():
                load_preview_file()
            save_state()
            show_toast("Configuration loaded.", kind="success")
        except Exception as exc:
            messagebox.showerror("Load config", str(exc))

    # Layout
    root.grid_columnconfigure(0, weight=1)
    root.grid_rowconfigure(1, weight=1)

    header = ctk.CTkFrame(root, fg_color="transparent")
    header.grid(row=0, column=0, sticky="ew", padx=24, pady=(20, 12))
    header.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(header, text="Wikidata dereplicator", font=font_header, text_color=colors["text"], anchor="w").grid(row=0, column=0, sticky="w")
    ctk.CTkLabel(header, text="Search formulas inside species, genus, family, and all of Wikidata to support compound prioritization.", font=font_subtitle, text_color=colors["muted"], anchor="w").grid(row=1, column=0, sticky="w", pady=(4, 0))
    actions = ctk.CTkFrame(header, fg_color="transparent")
    actions.grid(row=0, column=1, rowspan=2, sticky="e")
    run_button = make_button(actions, "Run search", start_run, primary=True, width=118)
    run_button.pack(side="left", padx=(0, 8))
    make_button(actions, "Save config", save_config_dialog, width=112).pack(side="left", padx=(0, 8))
    make_button(actions, "Load config", load_config_dialog, width=112).pack(side="left", padx=(0, 8))
    make_button(actions, "Clear log", clear_log, width=100).pack(side="left", padx=(0, 8))
    make_button(actions, "Close", root.destroy, danger=True, width=82).pack(side="left")

    tabs = ctk.CTkTabview(root, fg_color=colors["surface"], segmented_button_fg_color=colors["card_alt"], segmented_button_selected_color=colors["accent"], segmented_button_selected_hover_color=colors["accent_hover"], segmented_button_unselected_color=colors["card_alt"], segmented_button_unselected_hover_color="#39414c", text_color=colors["text"], corner_radius=14)
    tabs.grid(row=1, column=0, sticky="nsew", padx=24, pady=(0, 12))
    workflow_tab = tabs.add("Workflow")
    visualization_tab = tabs.add("Visualization")
    workflow_tab.grid_columnconfigure(0, weight=1)
    workflow_tab.grid_rowconfigure(0, weight=1)
    visualization_tab.grid_columnconfigure(0, weight=1)
    visualization_tab.grid_rowconfigure(1, weight=1)

    body = ctk.CTkFrame(workflow_tab, fg_color="transparent")
    body.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
    body.grid_columnconfigure(0, weight=3)
    body.grid_columnconfigure(1, weight=2)
    body.grid_rowconfigure(0, weight=1)

    workflow_scroll = ctk.CTkScrollableFrame(body, fg_color="transparent")
    workflow_scroll.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
    log_panel = ctk.CTkFrame(body, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=16)
    log_panel.grid(row=0, column=1, sticky="nsew", padx=(8, 0))
    log_panel.grid_columnconfigure(0, weight=1)
    log_panel.grid_rowconfigure(1, weight=1)
    ctk.CTkLabel(log_panel, text="Run log", font=font_card_title, text_color=colors["text"], anchor="w").grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
    txt_log = ctk.CTkTextbox(log_panel, fg_color=colors["entry"], border_color=colors["border"], border_width=1, text_color=colors["text"], font=font_mono, corner_radius=10, wrap="word")
    txt_log.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 16))
    txt_log.insert("end", "Ready. The log stays visible here while the search runs.\n")
    txt_log.configure(state="disabled")

    card = make_card(workflow_scroll, "Search settings")
    labeled_widget(card, "Output file label", make_entry(card, var_search_name, "e.g. s19_hookerianum"), 0, 0, help_text="Optional label added to output file names. This is not the search term; it is only a short sample or project label used in output file names so repeated runs do not overwrite each other.")
    out_frame = ctk.CTkFrame(card, fg_color="transparent")
    out_frame.grid(row=1, column=1, sticky="ew", padx=(0, 18), pady=8)
    out_frame.grid_columnconfigure(0, weight=1)
    make_entry(out_frame, var_output_dir).grid(row=0, column=0, sticky="ew", padx=(0, 8))
    make_button(out_frame, "Browse", browse_output_dir, width=82).grid(row=0, column=1)
    label_frame = ctk.CTkFrame(card, fg_color="transparent")
    label_frame.grid(row=1, column=0, sticky="ew", padx=(0, 10), pady=8)
    label_frame.grid_columnconfigure(0, weight=1)
    ctk.CTkLabel(label_frame, text="Output folder", font=font_label, text_color=colors["muted"], anchor="w").grid(row=0, column=0, sticky="w")
    make_help(label_frame, "Folder where the result CSV files will be written. The run always writes a summary and diagnostic CSV; match CSVs are created only when hits exist.").grid(row=0, column=1, sticky="e", padx=(6, 0))
    merge_frame = ctk.CTkFrame(card, fg_color="transparent")
    merge_frame.grid(row=2, column=1, columnspan=5, sticky="ew", pady=(8, 0))
    merge_frame.grid_columnconfigure(1, weight=1)
    merge_frame.grid_columnconfigure(4, weight=1)
    make_checkbox(merge_frame, "Merge duplicate compound URLs", var_merge).grid(row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
    make_help(merge_frame, "Use this when Wikidata returns the same compound page more than once, usually because multiple taxon statements point to the same molecule. The exported row keeps one compound URL and joins the unique names, taxa, and annotations from the duplicate rows.").grid(row=0, column=1, sticky="w", padx=(0, 22), pady=(0, 8))
    make_checkbox(merge_frame, "Merge same canonical SMILES", var_merge_smiles).grid(row=0, column=3, sticky="w", padx=(0, 8), pady=(0, 8))
    make_help(merge_frame, "Use this when different Wikidata compound pages describe the same molecular structure. Rows are grouped only when canonical SMILES is present; compound names, compound URLs, taxon names, and taxon URLs are joined into the same exported row. Blank-SMILES rows are left separate.").grid(row=0, column=4, sticky="w", pady=(0, 8))
    labeled_widget(card, "Parallel searches", make_combo(card, var_max_workers, ["1", "2", "4", "6", "8"]), 3, 0, help_text="Number of input rows searched at the same time. The default 4 is a good balance for typical runs. Use 1 or 2 if Wikidata starts throttling or your network is unstable; use 6 or 8 only for larger input tables when retry messages are rare.")

    limit_options = ["1", "5", "20", "50", "100", "300", "500", "1200", "3000", "No limit"]
    advanced_toggle_row = ctk.CTkFrame(card, fg_color="transparent")
    advanced_toggle_row.grid(row=4, column=0, columnspan=6, sticky="ew", pady=(8, 0))
    advanced_limits = ctk.CTkFrame(card, fg_color=colors["card_alt"], corner_radius=12)
    advanced_limits.grid(row=5, column=0, columnspan=6, sticky="ew", pady=(8, 0))
    for col in range(6):
        advanced_limits.grid_columnconfigure(col, weight=1 if col % 2 == 1 else 0)
    advanced_note = (
        "These settings cap how many rows Wikidata may return for each query type. "
        "Use No limit only for focused formulas; broad formulas can become slow or time out."
    )
    ctk.CTkLabel(advanced_limits, text=advanced_note, font=font_small, text_color=colors["muted"], justify="left", wraplength=720).grid(row=0, column=0, columnspan=6, sticky="ew", padx=12, pady=(12, 4))
    labeled_widget(advanced_limits, "Taxon lookup limit", make_combo(advanced_limits, var_limit_taxon, limit_options), 1, 0, help_text="Maximum rows used when resolving genus and family names to Wikidata taxon identifiers. The default is 5 because the script only needs the best matching taxon page.")
    labeled_widget(advanced_limits, "Exact species search limit", make_combo(advanced_limits, var_limit_exact, limit_options), 2, 0, help_text="Maximum compound rows returned for the exact species plus formula search. Default: 500. Choose No limit only if you expect many valid species-level hits.")
    labeled_widget(advanced_limits, "Genus/family search limit", make_combo(advanced_limits, var_limit_rank, limit_options), 3, 0, help_text="Maximum compound rows returned for each genus-level and family-level formula search. Default: 1200, because broader taxonomic searches can return many rows.")
    labeled_widget(advanced_limits, "Formula-anywhere limit", make_combo(advanced_limits, var_limit_anywhere, limit_options), 4, 0, help_text="Maximum compound rows returned when the formula is searched anywhere in Wikidata, without restricting the taxon. Default: 300. No limit may be slow for common formulas.")
    advanced_limits.grid_remove()

    def toggle_advanced_limits() -> None:
        if advanced_limits.winfo_ismapped():
            advanced_limits.grid_remove()
            advanced_button.configure(text="Show advanced query limits")
        else:
            advanced_limits.grid()
            advanced_button.configure(text="Hide advanced query limits")

    advanced_button = make_button(advanced_toggle_row, "Show advanced query limits", toggle_advanced_limits, width=190)
    advanced_button.pack(anchor="w")

    card = make_card(workflow_scroll, "Input source")
    mode_frame = ctk.CTkFrame(card, fg_color="transparent")
    mode_frame.grid(row=0, column=0, columnspan=6, sticky="ew", pady=(0, 8))
    ctk.CTkLabel(mode_frame, text="Use file input for batch runs, or manual input for a small one-off query.", font=font_small, text_color=colors["muted"], anchor="w").pack(anchor="w", pady=(0, 8))
    mode_switch = ctk.CTkSegmentedButton(mode_frame, values=["file", "manual"], variable=var_input_mode, command=lambda _v: update_input_mode(), fg_color=colors["card_alt"], selected_color=colors["accent"], selected_hover_color=colors["accent_hover"], unselected_color=colors["card_alt"], unselected_hover_color="#39414c", text_color=colors["text"])
    mode_switch.pack(fill="x")

    panel_host = ctk.CTkFrame(card, fg_color="transparent")
    panel_host.grid(row=1, column=0, columnspan=6, sticky="ew")
    panel_host.grid_columnconfigure(0, weight=1)
    file_panel = ctk.CTkFrame(panel_host, fg_color="transparent")
    file_panel.grid_columnconfigure(1, weight=1)
    manual_panel = ctk.CTkFrame(panel_host, fg_color="transparent")
    manual_panel.grid_columnconfigure(1, weight=1)

    file_panel.grid_columnconfigure(0, weight=0)
    file_panel.grid_columnconfigure(1, weight=1)

    def file_row_label(row: int, text: str, help_text: str) -> None:
        label = ctk.CTkFrame(file_panel, fg_color="transparent")
        label.grid(row=row, column=0, sticky="ew", padx=(0, 10), pady=8)
        label.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(label, text=text, font=font_label, text_color=colors["muted"], anchor="w").grid(row=0, column=0, sticky="w")
        make_help(label, help_text).grid(row=0, column=1, sticky="e", padx=(6, 0))

    file_row_label(
        0,
        "Input table",
        "CSV or Excel table. Each usable row should contain a plant genus, a molecular formula, and optionally a species. If species is blank, exact-species matching is skipped but genus, family, and formula-anywhere searches still run.",
    )
    file_picker = ctk.CTkFrame(file_panel, fg_color="transparent")
    file_picker.grid(row=0, column=1, sticky="ew", pady=8)
    file_picker.grid_columnconfigure(0, weight=1)
    make_entry(file_picker, var_file_path).grid(row=0, column=0, sticky="ew", padx=(0, 8))
    make_button(file_picker, "Browse", browse_input_file, width=82).grid(row=0, column=1, padx=(0, 8))
    make_button(file_picker, "Load", load_preview_file, primary=True, width=72).grid(row=0, column=2, padx=(0, 8))
    example_button(file_picker, "example_data/wikidata/wikidata_formula_queries.xlsx").grid(row=0, column=3)

    file_row_label(
        1,
        "Excel sheet",
        "For Excel files, choose the worksheet that contains the genus/species/formula table. CSV files ignore this field.",
    )
    sheet_combo = make_combo(file_panel, var_sheet_name, [""])
    sheet_combo.grid(row=1, column=1, sticky="ew", pady=8)
    sheet_combo.configure(command=lambda _choice: load_preview_file() if var_file_path.get().strip() else None)

    file_row_label(
        2,
        "Genus column",
        "Column containing the plant genus, for example Hypericum. This column is required because the script uses it to resolve the taxonomic scope.",
    )
    genus_row = ctk.CTkFrame(file_panel, fg_color="transparent")
    genus_row.grid(row=2, column=1, sticky="ew", pady=8)
    genus_row.grid_columnconfigure(0, weight=1)
    combo_genus = make_combo(genus_row, var_genus_col, ["genus"])
    combo_genus.grid(row=0, column=0, sticky="ew", padx=(0, 8))
    make_button(genus_row, "Select", lambda: select_loaded_column(var_genus_col, "Select genus column"), width=74).grid(row=0, column=1)

    file_row_label(
        3,
        "Species column",
        "Optional species column. Values may be only the epithet, such as olympicum, or the full name; the script normalizes it against the genus.",
    )
    species_row = ctk.CTkFrame(file_panel, fg_color="transparent")
    species_row.grid(row=3, column=1, sticky="ew", pady=8)
    species_row.grid_columnconfigure(0, weight=1)
    combo_species = make_combo(species_row, var_species_col, ["species"])
    combo_species.grid(row=0, column=0, sticky="ew", padx=(0, 8))
    make_button(species_row, "Select", lambda: select_loaded_column(var_species_col, "Select species column"), width=74).grid(row=0, column=1)

    file_row_label(
        4,
        "Formula column",
        "Column containing molecular formulas such as C18H16O7. Spaces are removed automatically before searching Wikidata.",
    )
    formula_row = ctk.CTkFrame(file_panel, fg_color="transparent")
    formula_row.grid(row=4, column=1, sticky="ew", pady=8)
    formula_row.grid_columnconfigure(0, weight=1)
    combo_formula = make_combo(formula_row, var_formula_col, ["formula"])
    combo_formula.grid(row=0, column=0, sticky="ew", padx=(0, 8))
    make_button(formula_row, "Select", lambda: select_loaded_column(var_formula_col, "Select formula column"), width=74).grid(row=0, column=1)

    ctk.CTkLabel(file_panel, textvariable=var_preview_summary, font=font_small, text_color=colors["muted"], anchor="w", justify="left", wraplength=760).grid(row=5, column=0, columnspan=2, sticky="ew", pady=(8, 4))
    txt_preview = ctk.CTkTextbox(file_panel, height=160, fg_color=colors["entry"], border_color=colors["border"], border_width=1, text_color=colors["text"], font=font_mono, corner_radius=10, wrap="none")
    txt_preview.grid(row=6, column=0, columnspan=2, sticky="ew")
    txt_preview.insert("end", "Load a table to preview the first rows.")
    txt_preview.configure(state="disabled")

    labeled_widget(manual_panel, "Genus", make_entry(manual_panel, var_manual_genus, "Hypericum"), 0, 0, help_text="Required plant genus. The script uses this to resolve a Wikidata taxon and then search compounds reported from that genus and its family.")
    labeled_widget(manual_panel, "Species", make_entry(manual_panel, var_manual_species, "olympicum"), 0, 2, help_text="Optional species name or species epithet. Leave blank if the species is unknown or if only genus/family context should be searched.")
    labeled_widget(manual_panel, "Formula", make_entry(manual_panel, var_manual_formula, "C18H16O7"), 1, 0, help_text="Required molecular formula. Use the normal plain-text formula; the script converts digits to Wikidata's subscript format internally.")
    manual_buttons = ctk.CTkFrame(manual_panel, fg_color="transparent")
    manual_buttons.grid(row=1, column=2, columnspan=2, sticky="ew", pady=8)
    make_button(manual_buttons, "Add row", add_manual_row, primary=True, width=90).pack(side="left", padx=(0, 8))
    make_button(manual_buttons, "Remove selected", remove_manual_row, danger=True, width=132).pack(side="left")
    manual_list = tk.Listbox(manual_panel, bg=colors["entry"], fg=colors["text"], selectbackground=colors["accent"], selectforeground="white", relief="flat", highlightthickness=1, highlightbackground=colors["border"], height=8, exportselection=False)
    manual_list.grid(row=2, column=0, columnspan=4, sticky="ew", pady=(8, 0))

    viz_top = ctk.CTkFrame(visualization_tab, fg_color=colors["card"], border_color=colors["border"], border_width=1, corner_radius=16)
    viz_top.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 12))
    viz_top.grid_columnconfigure(1, weight=1)
    ctk.CTkLabel(viz_top, text="Result file", font=font_label, text_color=colors["muted"], anchor="w").grid(row=0, column=0, sticky="w", padx=(16, 10), pady=(16, 8))
    make_entry(viz_top, var_result_path, "Choose a result CSV from this or an older run").grid(row=0, column=1, sticky="ew", pady=(16, 8))
    make_button(viz_top, "Browse", browse_result_file, width=82).grid(row=0, column=2, padx=(10, 8), pady=(16, 8))
    make_button(viz_top, "Load", load_result_file, primary=True, width=72).grid(row=0, column=3, padx=(0, 16), pady=(16, 8))
    ctk.CTkLabel(viz_top, text="Search", font=font_label, text_color=colors["muted"], anchor="w").grid(row=1, column=0, sticky="w", padx=(16, 10), pady=(4, 8))
    search_entry = make_entry(viz_top, var_result_search, "Name, species, genus, formula, or SMILES")
    search_entry.grid(row=1, column=1, sticky="ew", pady=(4, 8))
    search_entry.bind("<Return>", apply_result_search)
    make_button(viz_top, "Search", apply_result_search, primary=True, width=82).grid(row=1, column=2, padx=(10, 8), pady=(4, 8))
    make_button(viz_top, "Clear", clear_result_search, width=72).grid(row=1, column=3, padx=(0, 16), pady=(4, 8))
    ctk.CTkLabel(viz_top, textvariable=var_result_summary, font=font_small, text_color=colors["muted"], anchor="w", justify="left", wraplength=980).grid(row=2, column=0, columnspan=2, sticky="ew", padx=16, pady=(0, 16))
    page_controls = ctk.CTkFrame(viz_top, fg_color="transparent")
    page_controls.grid(row=2, column=2, columnspan=2, sticky="e", padx=16, pady=(0, 16))
    ctk.CTkLabel(page_controls, text="Rows per page", font=font_small, text_color=colors["muted"]).pack(side="left", padx=(0, 8))
    page_size_combo = ctk.CTkComboBox(page_controls, variable=var_result_page_size, values=["20", "50", "100"], width=82, height=32, command=change_result_page_size, fg_color=colors["entry"], border_color=colors["border"], button_color=colors["accent"], button_hover_color=colors["accent_hover"], dropdown_fg_color=colors["card"], dropdown_hover_color=colors["card_alt"], text_color=colors["text"], dropdown_text_color=colors["text"], corner_radius=8)
    page_size_combo.pack(side="left", padx=(0, 12))
    prev_page_button = make_button(page_controls, "Previous", lambda: change_result_page(-1), width=86)
    prev_page_button.pack(side="left", padx=(0, 8))
    ctk.CTkLabel(page_controls, textvariable=var_result_page, font=font_small, text_color=colors["muted"], width=88).pack(side="left", padx=(0, 8))
    next_page_button = make_button(page_controls, "Next", lambda: change_result_page(1), width=72)
    next_page_button.pack(side="left", padx=(0, 12))
    ctk.CTkLabel(page_controls, textvariable=var_result_selection, font=font_small, text_color=colors["muted"], width=76).pack(side="left", padx=(0, 8))
    export_selected_button = make_button(page_controls, "Export selected", export_selected_results, success=True, width=126)
    export_selected_button.pack(side="left")
    export_selected_button.configure(state="disabled")

    result_scroll = ctk.CTkScrollableFrame(visualization_tab, fg_color="#f6f8fb", corner_radius=18)
    result_scroll.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))
    render_result_cards()

    progress = ctk.CTkProgressBar(root, mode="indeterminate", height=10, progress_color=colors["accent"])
    progress.grid(row=2, column=0, sticky="ew", padx=24, pady=(0, 8))
    progress.stop()
    status_bar = ctk.CTkFrame(root, fg_color=colors["surface"], corner_radius=0)
    status_bar.grid(row=3, column=0, sticky="ew")
    ctk.CTkLabel(status_bar, textvariable=var_status, font=font_small, text_color=colors["muted"], anchor="w").pack(fill="x", padx=18, pady=8)

    refresh_manual_list()
    update_input_mode()
    if var_file_path.get().strip():
        try:
            load_preview_file()
        except Exception:
            pass
    if var_result_path.get().strip():
        try:
            load_result_file()
        except Exception:
            pass
    poll_queues()

    try:
        root.mainloop()
    finally:
        save_state()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
