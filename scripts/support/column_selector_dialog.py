"""Reusable searchable column selector for CustomTkinter GUIs."""
from __future__ import annotations

import tkinter as tk
from typing import Any, Iterable


def _normalize_columns(columns: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for column in columns:
        text = str(column).strip()
        if text and text not in seen:
            out.append(text)
            seen.add(text)
    return out


def choose_column(
    root: Any,
    ctk: Any,
    colors: dict[str, str],
    columns: Iterable[Any],
    *,
    title: str = "Select column",
    current: str = "",
) -> str | None:
    selected = choose_columns(root, ctk, colors, columns, title=title, current=[current] if current else [], multiple=False)
    return selected[0] if selected else None


def choose_columns(
    root: Any,
    ctk: Any,
    colors: dict[str, str],
    columns: Iterable[Any],
    *,
    title: str = "Select columns",
    current: Iterable[str] | None = None,
    multiple: bool = True,
) -> list[str] | None:
    all_columns = _normalize_columns(columns)
    if not all_columns:
        return None

    selected: list[str] | None = None
    current_set = {str(item).strip() for item in (current or []) if str(item).strip()}

    dialog = ctk.CTkToplevel(root)
    dialog.title(title)
    dialog.geometry("760x560")
    dialog.minsize(620, 420)
    dialog.configure(fg_color=colors.get("bg", "#17191d"))
    dialog.transient(root)
    dialog.grab_set()
    dialog.grid_columnconfigure(0, weight=1)
    dialog.grid_rowconfigure(2, weight=1)

    ctk.CTkLabel(
        dialog,
        text=title,
        text_color=colors.get("text", "white"),
        font=ctk.CTkFont(family="Segoe UI", size=16, weight="bold"),
        anchor="w",
    ).grid(row=0, column=0, sticky="ew", padx=18, pady=(18, 6))

    search_var = ctk.StringVar(value="")
    search_entry = ctk.CTkEntry(
        dialog,
        textvariable=search_var,
        placeholder_text="Filter visible columns",
        fg_color=colors.get("entry", "#191d22"),
        border_color=colors.get("border", "#3d4652"),
        text_color=colors.get("text", "white"),
        height=36,
        corner_radius=8,
    )
    search_entry.grid(row=1, column=0, sticky="ew", padx=18, pady=(4, 10))

    list_frame = ctk.CTkFrame(
        dialog,
        fg_color=colors.get("card", "#252a31"),
        border_color=colors.get("border", "#3d4652"),
        border_width=1,
        corner_radius=10,
    )
    list_frame.grid(row=2, column=0, sticky="nsew", padx=18, pady=(0, 12))
    list_frame.grid_columnconfigure(0, weight=1)
    list_frame.grid_rowconfigure(0, weight=1)

    select_mode = "extended" if multiple else "browse"
    listbox = tk.Listbox(
        list_frame,
        selectmode=select_mode,
        bg=colors.get("entry", "#191d22"),
        fg=colors.get("text", "white"),
        selectbackground=colors.get("accent", colors.get("blue", "#2563eb")),
        selectforeground="white",
        relief="flat",
        highlightthickness=0,
        exportselection=False,
        font=("Consolas", 10),
    )
    scrollbar = tk.Scrollbar(list_frame, orient="vertical", command=listbox.yview)
    listbox.configure(yscrollcommand=scrollbar.set)
    listbox.grid(row=0, column=0, sticky="nsew", padx=(10, 0), pady=10)
    scrollbar.grid(row=0, column=1, sticky="ns", padx=(0, 10), pady=10)

    visible_columns: list[str] = []

    def filtered_columns() -> list[str]:
        query = search_var.get().strip().lower()
        if not query:
            return all_columns
        return [column for column in all_columns if query in column.lower()]

    def refresh_list(*_args: object) -> None:
        nonlocal visible_columns
        visible_columns = filtered_columns()
        listbox.delete(0, "end")
        for column in visible_columns:
            listbox.insert("end", column)
        for index, column in enumerate(visible_columns):
            if column in current_set:
                listbox.selection_set(index)
                listbox.see(index)
        if not listbox.curselection() and visible_columns:
            listbox.selection_set(0)

    def apply_selection(*_args: object) -> None:
        nonlocal selected
        selection = listbox.curselection()
        if not selection:
            selected = []
        else:
            selected = [visible_columns[int(index)] for index in selection]
        dialog.destroy()

    def cancel() -> None:
        nonlocal selected
        selected = None
        dialog.destroy()

    def on_mousewheel(event: object) -> str:
        delta = getattr(event, "delta", 0)
        number = getattr(event, "num", None)
        direction = 1 if delta < 0 or number == 5 else -1
        listbox.yview_scroll(direction, "units")
        return "break"

    search_var.trace_add("write", refresh_list)
    listbox.bind("<MouseWheel>", on_mousewheel)
    listbox.bind("<Button-4>", on_mousewheel)
    listbox.bind("<Button-5>", on_mousewheel)
    listbox.bind("<Double-Button-1>", apply_selection)
    listbox.bind("<Return>", apply_selection)
    dialog.bind("<Escape>", lambda _event: cancel())

    buttons = ctk.CTkFrame(dialog, fg_color="transparent")
    buttons.grid(row=3, column=0, sticky="ew", padx=18, pady=(0, 18))
    buttons.grid_columnconfigure(0, weight=1)
    ctk.CTkButton(
        buttons,
        text="Use selected",
        command=apply_selection,
        width=130,
        height=36,
        corner_radius=9,
        fg_color=colors.get("accent", colors.get("blue", "#2563eb")),
        hover_color=colors.get("accent_hover", "#3b82f6"),
    ).grid(row=0, column=1, padx=(0, 8))
    ctk.CTkButton(
        buttons,
        text="Cancel",
        command=cancel,
        width=100,
        height=36,
        corner_radius=9,
        fg_color=colors.get("card_alt", "#2d333c"),
        hover_color="#39414c",
    ).grid(row=0, column=2)

    refresh_list()
    search_entry.focus_set()
    root.wait_window(dialog)
    return selected
