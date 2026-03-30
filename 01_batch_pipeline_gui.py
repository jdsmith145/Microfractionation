#!/usr/bin/env python3
"""
MZmine .mzbatch helper (single-window GUI)

This combines two workflows in one gray/blue Tkinter GUI:

1) Complete feature table batch setup:
   - template .mzbatch (complete-feature-table workflow)
   - sample name + output directory -> {sample_name}_configured.mzbatch
   - SAMPLE mzML files + BLANK mzML files -> written into RawDataImport "File names"
   - blank pattern -> "Blank/Control raw data files" name_pattern
   - feature-table CSV directory -> "Filename/current_file" set to
         {sample_name}_complete_feature_table.csv
     and keeps only one <last_file>

2) Fraction feature tables batch replication:
   - template .mzbatch (fraction workflow configured for fraction "_01")
   - sample name + output directory -> {sample_name}_fraction_configured.mzbatch
   - SAMPLE mzML files -> written into RawDataImport "File names" (once) and CropFilter "Raw data files"
   - per-fraction CSV directory -> CSVExport "Filename/current_file" updated per fraction
   - RT start/end/width -> replicates blocks across fractions

Run:
  python 02_batch_pipeline_gui.py
"""

from __future__ import annotations

import copy
import re
import xml.etree.ElementTree as ET
from pathlib import Path, PureWindowsPath

import tkinter as tk
from tkinter import ttk, filedialog, messagebox


DEFAULT_BLANK_PATTERN = "*blank*"
DEFAULT_RT_START = 2.0
DEFAULT_RT_END = 38.0
DEFAULT_WIDTH = 0.375

WINDOWS_FORBIDDEN_CHARS = set('<>:"/\\|?*')


# -------------------------
# Path helpers (Windows-friendly)
# -------------------------
def _looks_like_windows_path(s: str) -> bool:
    s = (s or "").strip()
    return ("\\" in s) or (len(s) >= 2 and s[1] == ":") or (":/" in s)


def _normalize_path_for_batch(p: str) -> str:
    """Return a path string formatted in a Windows-looking way if appropriate."""
    p = (p or "").strip()
    if not p:
        return p
    if _looks_like_windows_path(p):
        return str(PureWindowsPath(p))
    return p


def _basename_from_path(p: str) -> str:
    p = (p or "").strip()
    if not p:
        return ""
    if _looks_like_windows_path(p):
        return PureWindowsPath(p).name
    return Path(p).name


def _join_dir_and_filename(directory: str, filename: str) -> str:
    directory = (directory or "").strip()
    if not directory:
        return filename
    if _looks_like_windows_path(directory):
        return str(PureWindowsPath(directory) / filename)
    return str(Path(directory) / filename)


def _validate_sample_name(sample_name: str) -> str:
    name = (sample_name or "").strip()
    if not name:
        raise ValueError("Enter a sample name (used for output filenames).")
    if any(ch in WINDOWS_FORBIDDEN_CHARS for ch in name):
        bad = "".join(sorted(set(ch for ch in name if ch in WINDOWS_FORBIDDEN_CHARS)))
        raise ValueError(
            "Sample name contains forbidden filename characters: "
            f"{bad}\nAvoid: < > : \" / \\ | ? *"
        )
    if name.endswith("."):
        raise ValueError("Sample name cannot end with a dot (Windows filename rule).")
    return name


def _validate_mzml_files(files: list[str], label: str) -> None:
    if not files:
        raise ValueError(f"Select at least one {label} .mzML file.")
    bad_ext = [f for f in files if not f.lower().endswith(".mzml")]
    if bad_ext:
        raise ValueError(f"{label}: these are not .mzML files:\n  " + "\n  ".join(bad_ext))


# -------------------------
# XML helpers (shared)
# -------------------------
def _indent_tree(tree: ET.ElementTree) -> None:
    try:
        ET.indent(tree, space="    ", level=0)  # py3.9+
    except Exception:
        pass


def _update_blank_pattern_anywhere(elem: ET.Element, blank_pattern: str) -> int:
    """Update all <parameter name='Blank/Control raw data files'><name_pattern>...</name_pattern> under elem."""
    updated = 0
    for param in elem.iter("parameter"):
        if param.get("name") != "Blank/Control raw data files":
            continue
        name_pattern = param.find("name_pattern")
        if name_pattern is not None:
            name_pattern.text = blank_pattern
            updated += 1
    return updated


def _find_import_batchsteps(root: ET.Element) -> list[ET.Element]:
    steps = root.findall("batchstep")
    out: list[ET.Element] = []
    for s in steps:
        m = (s.get("method") or "")
        if ("RawDataImportModule" in m) or m.endswith("RawDataImportModule"):
            out.append(s)
    # If we couldn't detect, fall back to the first step (common in MZmine batches)
    if not out and steps:
        out = [steps[0]]
    return out


def _set_import_file_names_in_step(step: ET.Element, files: list[str]) -> bool:
    p = step.find("parameter[@name='File names']")
    if p is None:
        return False
    for f in list(p.findall("file")):
        p.remove(f)
    for fp in files:
        fe = ET.SubElement(p, "file")
        fe.text = _normalize_path_for_batch(fp)
    return True


# -------------------------
# 1) COMPLETE feature table batch
# -------------------------
def _update_feature_table_path_complete(root: ET.Element, feature_dir: str | None, sample_name: str) -> int:
    """Update <parameter name='Filename'> current_file/last_file to feature_dir + {sample}_complete_feature_table.csv."""
    if not feature_dir:
        return 0

    feature_dir = _normalize_path_for_batch(feature_dir)
    target_name = f"{sample_name}_complete_feature_table.csv"
    new_path = _join_dir_and_filename(feature_dir, target_name)

    updated = 0
    for param in root.iter("parameter"):
        if param.get("name") != "Filename":
            continue
        current_file = param.find("current_file")
        if current_file is None:
            continue

        current_file.text = new_path

        last_files = list(param.findall("last_file"))
        if last_files:
            last_files[0].text = new_path
            for extra in last_files[1:]:
                param.remove(extra)
        else:
            lf = ET.SubElement(param, "last_file")
            lf.text = new_path

        updated += 1

    return updated


def configure_complete_mzbatch(
    template_path: Path,
    out_path: Path,
    sample_files: list[str],
    blank_files: list[str],
    blank_pattern: str,
    feature_dir: str | None,
    sample_name: str,
) -> tuple[int, int, int]:
    """Returns (n_import_steps_updated, n_blank_params_updated, n_filename_params_updated)."""
    try:
        tree = ET.parse(template_path)
    except ET.ParseError as e:
        raise RuntimeError(f"Failed to parse XML from '{template_path}': {e}") from e

    root = tree.getroot()

    all_files = [*sample_files, *blank_files]

    # Update import step(s) file names (prefer RawDataImportModule)
    steps = _find_import_batchsteps(root)
    n_import = 0
    did_any = False
    for st in steps:
        if _set_import_file_names_in_step(st, all_files):
            n_import += 1
            did_any = True

    if not did_any:
        # fallback: try global replace (last resort)
        for param in root.iter("parameter"):
            if param.get("name") != "File names":
                continue
            for child in list(param.findall("file")):
                param.remove(child)
            for fp in all_files:
                fe = ET.SubElement(param, "file")
                fe.text = _normalize_path_for_batch(fp)
            n_import += 1

    n_blank = _update_blank_pattern_anywhere(root, blank_pattern)
    n_feat = _update_feature_table_path_complete(root, feature_dir, sample_name)

    _indent_tree(tree)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tree.write(out_path, encoding="UTF-8", xml_declaration=True)

    return n_import, n_blank, n_feat


# -------------------------
# 2) FRACTION feature tables replication batch
# -------------------------
def _fraction_count(rt_start: float, rt_end: float, width: float) -> int:
    if width <= 0:
        raise ValueError("Time per fraction must be > 0.")
    if rt_end <= rt_start:
        raise ValueError("Fractionation end must be greater than start.")
    x = (rt_end - rt_start) / width
    n = int(x + 1e-9)  # floor, guard float noise
    if n <= 0:
        raise ValueError("Computed zero fractions. Check start/end/width.")
    return n


def _detect_template_fraction_index(root: ET.Element) -> tuple[str, int]:
    """Detect template fraction index used in the batch (usually '01')."""
    xml_str = ET.tostring(root, encoding="unicode")
    m = re.search(r"\bfrac_(\d{1,3})\b", xml_str)
    if not m:
        m = re.search(r"_[0-9]{1,3}\b", xml_str)
        if m:
            idx = m.group(0)[1:]
            return idx, len(idx)
        raise ValueError("Could not detect a template fraction index (like frac_01) in the template batch.")
    idx = m.group(1)
    return idx, len(idx)


def _replace_index_everywhere(elem: ET.Element, template_idx: str, new_idx: str) -> None:
    for k, v in list(elem.attrib.items()):
        if v:
            elem.attrib[k] = re.sub(rf"_{re.escape(template_idx)}\b", f"_{new_idx}", v)

    if elem.text:
        elem.text = re.sub(rf"_{re.escape(template_idx)}\b", f"_{new_idx}", elem.text)
    if elem.tail:
        elem.tail = re.sub(rf"_{re.escape(template_idx)}\b", f"_{new_idx}", elem.tail)

    for child in list(elem):
        _replace_index_everywhere(child, template_idx, new_idx)


def _set_crop_input_files(crop_step: ET.Element, mzml_paths: list[str]) -> None:
    """Crop filter uses <parameter name='Raw data files'> with <specific_file> entries of basenames."""
    p = crop_step.find("parameter[@name='Raw data files']")
    if p is None:
        raise ValueError("CropFilter step missing <parameter name='Raw data files'>.")
    for sf in list(p.findall("specific_file")):
        p.remove(sf)
    for fp in mzml_paths:
        sf = ET.SubElement(p, "specific_file")
        sf.text = _basename_from_path(fp)


def _set_crop_retention_time(crop_step: ET.Element, rt_min: float, rt_max: float) -> None:
    scan_filters = crop_step.find("parameter[@name='Scan filters']")
    if scan_filters is None:
        raise ValueError("CropFilter step missing <parameter name='Scan filters'>.")
    rt = scan_filters.find("parameter[@name='Retention time']")
    if rt is None:
        raise ValueError("CropFilter step missing retention time parameter.")
    mn = rt.find("min")
    mx = rt.find("max")
    if mn is None or mx is None:
        raise ValueError("Retention time parameter missing <min>/<max>.")
    mn.text = f"{rt_min:.3f}"
    mx.text = f"{rt_max:.3f}"


def _set_csv_export_path(export_step: ET.Element, feature_dir: str, template_idx: str, new_idx: str) -> None:
    """Update CSV export current_file/last_file; keep exactly one <last_file>."""
    fn = export_step.find("parameter[@name='Filename']")
    if fn is None:
        return
    cf = fn.find("current_file")
    if cf is None:
        return

    old_path = (cf.text or "").strip()
    old_name = _basename_from_path(old_path) or f"frac_{new_idx}.csv"

    new_name = re.sub(rf"_{re.escape(template_idx)}\b", f"_{new_idx}", old_name)

    if new_name == old_name and template_idx != new_idx:
        if not re.search(rf"_{re.escape(new_idx)}\b", old_name):
            stem = Path(old_name).stem
            suffix = Path(old_name).suffix or ".csv"
            new_name = f"{stem}_{new_idx}{suffix}"

    new_full = _join_dir_and_filename(feature_dir, new_name)
    cf.text = new_full

    for lf in list(fn.findall("last_file")):
        fn.remove(lf)
    lf = ET.SubElement(fn, "last_file")
    lf.text = new_full


def replicate_fraction_mzbatch(
    template_path: Path,
    out_path: Path,
    mzml_paths: list[str],
    feature_dir: str,
    rt_start: float,
    rt_end: float,
    width: float,
) -> tuple[int, str]:
    """Returns (n_fractions, template_idx_detected)."""
    tree = ET.parse(template_path)
    root = tree.getroot()

    steps = root.findall("batchstep")
    if len(steps) < 2:
        raise ValueError("Template batch must contain at least 2 batchsteps (import + processing).")

    import_step = copy.deepcopy(steps[0])
    processing_templates = steps[1:]

    template_idx, idx_width = _detect_template_fraction_index(root)

    # Import list once (+ blank pattern if present there)
    if not _set_import_file_names_in_step(import_step, mzml_paths):
        # fallback: global search inside import_step
        for param in import_step.iter("parameter"):
            if param.get("name") == "File names":
                for child in list(param.findall("file")):
                    param.remove(child)
                for fp in mzml_paths:
                    fe = ET.SubElement(param, "file")
                    fe.text = _normalize_path_for_batch(fp)
                break

    # Build new root (copy non-batchstep children)
    new_root = ET.Element(root.tag, dict(root.attrib))
    for child in list(root):
        if child.tag != "batchstep":
            new_root.append(copy.deepcopy(child))

    new_root.append(import_step)

    n = _fraction_count(rt_start, rt_end, width)

    def _is_method(step: ET.Element, endswith: str) -> bool:
        m = step.get("method", "")
        return m.endswith(endswith)

    for i in range(n):
        new_idx = str(i + 1).zfill(idx_width)
        rt_min = rt_start + i * width
        rt_max = rt_min + width

        for tpl in processing_templates:
            step = copy.deepcopy(tpl)

            _replace_index_everywhere(step, template_idx, new_idx)

            # Keep blank pattern constant (update if module exists)

            if _is_method(step, "CropFilterModule"):
                _set_crop_input_files(step, mzml_paths)
                _set_crop_retention_time(step, rt_min, rt_max)

            if _is_method(step, "CSVExportModularModule"):
                _set_csv_export_path(step, feature_dir, template_idx, new_idx)

            new_root.append(step)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_tree = ET.ElementTree(new_root)
    _indent_tree(out_tree)
    out_tree.write(out_path, encoding="UTF-8", xml_declaration=True)

    return n, template_idx


# -------------------------
# GUI widgets
# -------------------------
class CollapsibleSection(ttk.Frame):
    def __init__(self, parent: tk.Widget, title: str, *, start_open: bool = True) -> None:
        super().__init__(parent)
        self._open = start_open

        self.header = ttk.Frame(self)
        self.header.grid(row=0, column=0, sticky="ew")
        self.header.columnconfigure(1, weight=1)

        self.btn = ttk.Button(self.header, text="▾" if self._open else "▸", width=3, command=self.toggle)
        self.btn.grid(row=0, column=0, sticky="w")

        self.lbl = ttk.Label(self.header, text=title, font=("Segoe UI", 11, "bold"))
        self.lbl.grid(row=0, column=1, sticky="w", padx=(6, 0))

        self.body = ttk.Frame(self)
        self.body.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        self.columnconfigure(0, weight=1)

        if not self._open:
            self.body.grid_remove()

    def toggle(self) -> None:
        self._open = not self._open
        self.btn.configure(text="▾" if self._open else "▸")
        if self._open:
            self.body.grid()
        else:
            self.body.grid_remove()


class ScrollableFrame(ttk.Frame):
    def __init__(self, parent: tk.Widget) -> None:
        super().__init__(parent)

        self.canvas = tk.Canvas(self, highlightthickness=0)
        self.vsb = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.vsb.set)

        self.vsb.grid(row=0, column=1, sticky="ns")
        self.canvas.grid(row=0, column=0, sticky="nsew")

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self.inner = ttk.Frame(self.canvas)
        self.inner_id = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")

        self.inner.bind("<Configure>", self._on_inner_configure)
        self.canvas.bind("<Configure>", self._on_canvas_configure)

        # Mousewheel
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)          # Windows
        self.canvas.bind_all("<Button-4>", self._on_mousewheel_linux)      # Linux up
        self.canvas.bind_all("<Button-5>", self._on_mousewheel_linux)      # Linux down

    def _on_inner_configure(self, _evt) -> None:
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _on_canvas_configure(self, evt) -> None:
        # Make inner frame width follow canvas width
        self.canvas.itemconfigure(self.inner_id, width=evt.width)

    def _on_mousewheel(self, evt) -> None:
        self.canvas.yview_scroll(int(-1 * (evt.delta / 120)), "units")

    def _on_mousewheel_linux(self, evt) -> None:
        self.canvas.yview_scroll(-1 if evt.num == 4 else 1, "units")


# -------------------------
# Main App
# -------------------------
class App:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("MZmine batch helper (complete + fraction)")

        self.sample_files: list[str] = []
        self.blank_files: list[str] = []

        self._build_style()
        self._build_ui()

    def _build_style(self) -> None:
        self.bg = "#2f2f2f"
        self.fg = "#eeeeee"
        self.blue = "#2d74da"

        self.root.configure(bg=self.bg)

        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure("TFrame", background=self.bg)
        style.configure("TLabel", background=self.bg, foreground=self.fg)
        style.configure("TEntry", fieldbackground="#3a3a3a", foreground=self.fg)

        style.configure("Accent.TButton", background=self.blue, foreground="white")
        style.map("Accent.TButton", background=[("active", "#3a86ff"), ("pressed", "#1e5bb8")])

        style.configure("TButton", background="#444444", foreground=self.fg)
        style.map("TButton", background=[("active", "#555555"), ("pressed", "#3b3b3b")])

        style.configure("TLabelframe", background=self.bg, foreground=self.fg)
        style.configure("TLabelframe.Label", background=self.bg, foreground=self.fg)

    def _build_ui(self) -> None:
        sf = ScrollableFrame(self.root)
        sf.grid(row=0, column=0, sticky="nsew")
        sf.canvas.configure(bg=self.bg)
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        frame = sf.inner
        frame.columnconfigure(1, weight=1)
        frame.columnconfigure(2, weight=0)

        # ---- Shared vars
        self.var_sample_name = tk.StringVar()
        self.var_sample_summary = tk.StringVar(value="No files selected")

        # ---- Complete vars
        self.var_complete_template = tk.StringVar()
        self.var_complete_out_dir = tk.StringVar()
        self.var_complete_out_preview = tk.StringVar(value="(set sample name + output directory)")
        self.var_complete_blank_pattern = tk.StringVar(value=DEFAULT_BLANK_PATTERN)
        self.var_complete_feature_dir = tk.StringVar()
        self.var_blank_summary = tk.StringVar(value="No files selected")
        self.var_complete_status = tk.StringVar(value="Ready.")

        # ---- Fraction vars
        self.var_frac_template = tk.StringVar()
        self.var_frac_out_dir = tk.StringVar()
        self.var_frac_out_preview = tk.StringVar(value="(set sample name + output directory)")
        self.var_frac_feature_dir = tk.StringVar()
        self.var_rt_start = tk.StringVar(value=str(DEFAULT_RT_START))
        self.var_rt_end = tk.StringVar(value=str(DEFAULT_RT_END))
        self.var_rt_width = tk.StringVar(value=str(DEFAULT_WIDTH))
        self.var_frac_status = tk.StringVar(value="Ready.")

        # Update previews when name/dirs change
        self.var_sample_name.trace_add("write", lambda *_: self._update_previews())
        self.var_complete_out_dir.trace_add("write", lambda *_: self._update_previews())
        self.var_frac_out_dir.trace_add("write", lambda *_: self._update_previews())

        r = 0
        ttk.Label(frame, text="Sample name (shared)").grid(row=r, column=0, sticky="w", pady=4, padx=(12, 0))
        ttk.Entry(frame, textvariable=self.var_sample_name).grid(row=r, column=1, sticky="ew", pady=4, padx=(10, 10))
        ttk.Label(frame, text="(used in output filenames)").grid(row=r, column=2, sticky="w", pady=4, padx=(0, 12))
        r += 1

        ttk.Label(frame, text="SAMPLE .mzML file(s) (shared)").grid(row=r, column=0, sticky="w", pady=4, padx=(12, 0))
        ttk.Entry(frame, textvariable=self.var_sample_summary, state="readonly").grid(row=r, column=1, sticky="ew", pady=4, padx=(10, 10))
        ttk.Button(frame, text="Select…", command=self.pick_sample_files).grid(row=r, column=2, sticky="ew", pady=4, padx=(0, 12))
        r += 1

        ttk.Separator(frame).grid(row=r, column=0, columnspan=3, sticky="ew", pady=12, padx=12)
        r += 1

        # ---- Sections
        sec_complete = CollapsibleSection(frame, "Complete feature table batch setup", start_open=True)
        sec_complete.grid(row=r, column=0, columnspan=3, sticky="ew", padx=12)
        sec_complete.body.columnconfigure(1, weight=1)
        self._build_complete_section(sec_complete.body)
        r += 1

        ttk.Separator(frame).grid(row=r, column=0, columnspan=3, sticky="ew", pady=12, padx=12)
        r += 1

        sec_frac = CollapsibleSection(frame, "Fraction feature tables batch replication", start_open=True)
        sec_frac.grid(row=r, column=0, columnspan=3, sticky="ew", padx=12)
        sec_frac.body.columnconfigure(1, weight=1)
        self._build_fraction_section(sec_frac.body)
        r += 1

        ttk.Separator(frame).grid(row=r, column=0, columnspan=3, sticky="ew", pady=12, padx=12)
        r += 1

        ttk.Label(
            frame,
            text=(
                "Tip: select only your actual SAMPLE/BLANK mzML files. "
                "The file picker is filtered to *.mzML."
            ),
        ).grid(row=r, column=0, columnspan=3, sticky="w", padx=12, pady=(0, 12))

    # -------------------------
    # Section builders
    # -------------------------
    def _build_complete_section(self, parent: ttk.Frame) -> None:
        r = 0
        ttk.Label(parent, text="Template .mzbatch (complete)").grid(row=r, column=0, sticky="w", pady=4)
        ttk.Entry(parent, textvariable=self.var_complete_template).grid(row=r, column=1, sticky="ew", padx=(10, 10))
        ttk.Button(parent, text="Browse…", command=self.pick_complete_template).grid(row=r, column=2, sticky="ew")
        r += 1

        ttk.Label(parent, text="Save configured .mzbatch to (directory)").grid(row=r, column=0, sticky="w", pady=4)
        ttk.Entry(parent, textvariable=self.var_complete_out_dir).grid(row=r, column=1, sticky="ew", padx=(10, 10))
        ttk.Button(parent, text="Browse…", command=self.pick_complete_out_dir).grid(row=r, column=2, sticky="ew")
        r += 1

        ttk.Label(parent, text="Output .mzbatch path").grid(row=r, column=0, sticky="w", pady=4)
        ttk.Entry(parent, textvariable=self.var_complete_out_preview, state="readonly").grid(row=r, column=1, columnspan=2, sticky="ew", padx=(10, 0))
        r += 1

        ttk.Separator(parent).grid(row=r, column=0, columnspan=3, sticky="ew", pady=10)
        r += 1

        ttk.Label(parent, text="BLANK .mzML file(s)").grid(row=r, column=0, sticky="w", pady=4)
        ttk.Entry(parent, textvariable=self.var_blank_summary, state="readonly").grid(row=r, column=1, sticky="ew", padx=(10, 10))
        ttk.Button(parent, text="Select…", command=self.pick_blank_files).grid(row=r, column=2, sticky="ew")
        r += 1

        ttk.Label(parent, text="Blank pattern").grid(row=r, column=0, sticky="w", pady=4)
        ttk.Entry(parent, textvariable=self.var_complete_blank_pattern).grid(row=r, column=1, sticky="ew", padx=(10, 10))
        ttk.Label(parent, text="(e.g. *blank*)").grid(row=r, column=2, sticky="w")
        r += 1

        ttk.Label(parent, text="Feature-table CSV directory").grid(row=r, column=0, sticky="w", pady=4)
        ttk.Entry(parent, textvariable=self.var_complete_feature_dir).grid(row=r, column=1, sticky="ew", padx=(10, 10))
        ttk.Button(parent, text="Browse…", command=self.pick_complete_feature_dir).grid(row=r, column=2, sticky="ew")
        r += 1

        ttk.Separator(parent).grid(row=r, column=0, columnspan=3, sticky="ew", pady=10)
        r += 1

        ttk.Label(parent, textvariable=self.var_complete_status).grid(row=r, column=0, columnspan=2, sticky="w")
        ttk.Button(parent, text="Run COMPLETE", style="Accent.TButton", command=self.run_complete).grid(row=r, column=2, sticky="ew")
        r += 1

    def _build_fraction_section(self, parent: ttk.Frame) -> None:
        r = 0
        ttk.Label(parent, text="Template .mzbatch (fraction)").grid(row=r, column=0, sticky="w", pady=4)
        ttk.Entry(parent, textvariable=self.var_frac_template).grid(row=r, column=1, sticky="ew", padx=(10, 10))
        ttk.Button(parent, text="Browse…", command=self.pick_frac_template).grid(row=r, column=2, sticky="ew")
        r += 1

        ttk.Label(parent, text="Save replicated .mzbatch to (directory)").grid(row=r, column=0, sticky="w", pady=4)
        ttk.Entry(parent, textvariable=self.var_frac_out_dir).grid(row=r, column=1, sticky="ew", padx=(10, 10))
        ttk.Button(parent, text="Browse…", command=self.pick_frac_out_dir).grid(row=r, column=2, sticky="ew")
        r += 1

        ttk.Label(parent, text="Output .mzbatch path").grid(row=r, column=0, sticky="w", pady=4)
        ttk.Entry(parent, textvariable=self.var_frac_out_preview, state="readonly").grid(row=r, column=1, columnspan=2, sticky="ew", padx=(10, 0))
        r += 1

        ttk.Separator(parent).grid(row=r, column=0, columnspan=3, sticky="ew", pady=10)
        r += 1

        ttk.Label(parent, text="Per-fraction CSV directory").grid(row=r, column=0, sticky="w", pady=4)
        ttk.Entry(parent, textvariable=self.var_frac_feature_dir).grid(row=r, column=1, sticky="ew", padx=(10, 10))
        ttk.Button(parent, text="Browse…", command=self.pick_frac_feature_dir).grid(row=r, column=2, sticky="ew")
        r += 1

        ttk.Separator(parent).grid(row=r, column=0, columnspan=3, sticky="ew", pady=10)
        r += 1

        ttk.Label(parent, text="Fractionation RT start (min)").grid(row=r, column=0, sticky="w", pady=4)
        ttk.Entry(parent, textvariable=self.var_rt_start).grid(row=r, column=1, sticky="ew", padx=(10, 10))
        ttk.Label(parent, text=f"(default {DEFAULT_RT_START})").grid(row=r, column=2, sticky="w")
        r += 1

        ttk.Label(parent, text="Fractionation RT end (min)").grid(row=r, column=0, sticky="w", pady=4)
        ttk.Entry(parent, textvariable=self.var_rt_end).grid(row=r, column=1, sticky="ew", padx=(10, 10))
        ttk.Label(parent, text=f"(default {DEFAULT_RT_END})").grid(row=r, column=2, sticky="w")
        r += 1

        ttk.Label(parent, text="Time per fraction (min)").grid(row=r, column=0, sticky="w", pady=4)
        ttk.Entry(parent, textvariable=self.var_rt_width).grid(row=r, column=1, sticky="ew", padx=(10, 10))
        ttk.Label(parent, text=f"(default {DEFAULT_WIDTH})").grid(row=r, column=2, sticky="w")
        r += 1

        ttk.Separator(parent).grid(row=r, column=0, columnspan=3, sticky="ew", pady=10)
        r += 1

        ttk.Label(parent, textvariable=self.var_frac_status).grid(row=r, column=0, columnspan=2, sticky="w")
        ttk.Button(parent, text="Run FRACTION", style="Accent.TButton", command=self.run_fraction).grid(row=r, column=2, sticky="ew")
        r += 1

    # -------------------------
    # Pickers
    # -------------------------
    def _pick_mzml_files(self, title: str) -> list[str]:
        files = filedialog.askopenfilenames(
            title=title,
            filetypes=[("mzML files", "*.mzML *.mzml"), ("All files", "*.*")],
        )
        return list(files) if files else []

    @staticmethod
    def _summarize_files(files: list[str]) -> str:
        if not files:
            return "No files selected"
        if len(files) == 1:
            return files[0]
        return f"{len(files)} files selected (first: {files[0]})"

    def pick_sample_files(self) -> None:
        files = self._pick_mzml_files("Select SAMPLE .mzML file(s)")
        if not files:
            return
        self.sample_files = files
        self.var_sample_summary.set(self._summarize_files(files))

    def pick_blank_files(self) -> None:
        files = self._pick_mzml_files("Select BLANK .mzML file(s)")
        if not files:
            return
        self.blank_files = files
        self.var_blank_summary.set(self._summarize_files(files))

    def pick_complete_template(self) -> None:
        p = filedialog.askopenfilename(
            title="Select COMPLETE template .mzbatch",
            filetypes=[("MZmine batch files", "*.mzbatch"), ("All files", "*.*")],
        )
        if p:
            self.var_complete_template.set(p)

    def pick_frac_template(self) -> None:
        p = filedialog.askopenfilename(
            title="Select FRACTION template .mzbatch",
            filetypes=[("MZmine batch files", "*.mzbatch"), ("All files", "*.*")],
        )
        if p:
            self.var_frac_template.set(p)

    def pick_complete_out_dir(self) -> None:
        d = filedialog.askdirectory(title="Select output directory for COMPLETE configured batch")
        if d:
            self.var_complete_out_dir.set(d)

    def pick_frac_out_dir(self) -> None:
        d = filedialog.askdirectory(title="Select output directory for FRACTION replicated batch")
        if d:
            self.var_frac_out_dir.set(d)

    def pick_complete_feature_dir(self) -> None:
        d = filedialog.askdirectory(title="Select output directory for COMPLETE feature table CSV")
        if d:
            self.var_complete_feature_dir.set(d)

    def pick_frac_feature_dir(self) -> None:
        d = filedialog.askdirectory(title="Select output directory for per-fraction CSV feature tables")
        if d:
            self.var_frac_feature_dir.set(d)

    # -------------------------
    # Previews
    # -------------------------
    def _update_previews(self) -> None:
        name = (self.var_sample_name.get() or "").strip()
        if name and (self.var_complete_out_dir.get() or "").strip():
            self.var_complete_out_preview.set(
                _normalize_path_for_batch(
                    _join_dir_and_filename(self.var_complete_out_dir.get(), f"{name}_configured.mzbatch")
                )
            )
        else:
            self.var_complete_out_preview.set("(set sample name + output directory)")

        if name and (self.var_frac_out_dir.get() or "").strip():
            self.var_frac_out_preview.set(
                _normalize_path_for_batch(
                    _join_dir_and_filename(self.var_frac_out_dir.get(), f"{name}_fraction_configured.mzbatch")
                )
            )
        else:
            self.var_frac_out_preview.set("(set sample name + output directory)")

    # -------------------------
    # Run actions
    # -------------------------
    def run_complete(self) -> None:
        try:
            sample_name = _validate_sample_name(self.var_sample_name.get())

            tmpl = (self.var_complete_template.get() or "").strip()
            if not tmpl:
                raise ValueError("Select the COMPLETE template .mzbatch.")
            template_path = Path(tmpl).expanduser()
            if not template_path.exists():
                raise ValueError(f"COMPLETE template does not exist:\n{template_path}")

            out_dir_s = (self.var_complete_out_dir.get() or "").strip()
            if not out_dir_s:
                raise ValueError("Select output directory for the COMPLETE configured batch.")
            out_dir = Path(out_dir_s).expanduser()
            out_path = out_dir / f"{sample_name}_configured.mzbatch"

            blank_pattern = (self.var_complete_blank_pattern.get() or "").strip() or DEFAULT_BLANK_PATTERN
            feature_dir = (self.var_complete_feature_dir.get() or "").strip() or None

            _validate_mzml_files(self.sample_files, "SAMPLE")
            _validate_mzml_files(self.blank_files, "BLANK")

            self.var_complete_status.set("Working…")
            self.root.update_idletasks()

            n_import, n_blank, n_feat = configure_complete_mzbatch(
                template_path=template_path,
                out_path=out_path,
                sample_files=self.sample_files,
                blank_files=self.blank_files,
                blank_pattern=blank_pattern,
                feature_dir=feature_dir,
                sample_name=sample_name,
            )

            self.var_complete_status.set("Done.")
            messagebox.showinfo(
                "Complete batch saved",
                (
                    f"Saved:\n{out_path}\n\n"
                    f"Updated:\n"
                    f"  Import 'File names' blocks: {n_import}\n"
                    f"  Blank pattern blocks: {n_blank}\n"
                    f"  Feature table 'Filename' blocks: {n_feat}"
                ),
            )

        except Exception as e:
            self.var_complete_status.set("Error.")
            messagebox.showerror("Complete batch error", str(e))

    def run_fraction(self) -> None:
        try:
            sample_name = _validate_sample_name(self.var_sample_name.get())

            tmpl = (self.var_frac_template.get() or "").strip()
            if not tmpl:
                raise ValueError("Select the FRACTION template .mzbatch.")
            template_path = Path(tmpl).expanduser()
            if not template_path.exists():
                raise ValueError(f"FRACTION template does not exist:\n{template_path}")

            out_dir_s = (self.var_frac_out_dir.get() or "").strip()
            if not out_dir_s:
                raise ValueError("Select output directory for the FRACTION replicated batch.")
            out_dir = Path(out_dir_s).expanduser()
            out_path = out_dir / f"{sample_name}_fraction_configured.mzbatch"

            feature_dir = (self.var_frac_feature_dir.get() or "").strip()
            if not feature_dir:
                raise ValueError("Select directory for per-fraction CSV feature tables.")

            _validate_mzml_files(self.sample_files, "SAMPLE")

            try:
                rt_start = float((self.var_rt_start.get() or "").strip())
                rt_end = float((self.var_rt_end.get() or "").strip())
                width = float((self.var_rt_width.get() or "").strip())
            except ValueError:
                raise ValueError("RT start/end/width must be numbers (minutes).")

            self.var_frac_status.set("Working…")
            self.root.update_idletasks()

            n_frac, template_idx = replicate_fraction_mzbatch(
                template_path=template_path,
                out_path=out_path,
                mzml_paths=self.sample_files,
                feature_dir=feature_dir,
                rt_start=rt_start,
                rt_end=rt_end,
                width=width,
            )

            self.var_frac_status.set("Done.")
            messagebox.showinfo(
                "Fraction batch saved",
                (
                    f"Saved:\n{out_path}\n\n"
                    f"Fractions: {n_frac}\n"
                    f"Template index detected: {template_idx}\n"
                    f"CSV output dir:\n{feature_dir}"
                ),
            )

        except Exception as e:
            self.var_frac_status.set("Error.")
            messagebox.showerror("Fraction batch error", str(e))


def main() -> int:
    root = tk.Tk()
    root.geometry("980x760")
    root.minsize(860, 640)
    App(root)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
