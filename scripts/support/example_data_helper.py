"""Helpers for optional example input files used by GUI scripts."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any


def project_root_for(script_file: str | Path) -> Path:
    """Return the nearest parent directory that contains optional examples, if present."""
    path = Path(script_file).resolve()
    for parent in path.parents:
        if (parent / "example_data").exists():
            return parent
    return path.parent.parent


def example_path(script_file: str | Path, relative_path: str | Path) -> Path:
    return project_root_for(script_file) / Path(relative_path)


def open_example(script_file: str | Path, relative_path: str | Path, *, messagebox: Any | None = None) -> bool:
    """Open an optional example file/folder with the operating-system default app."""
    path = example_path(script_file, relative_path)
    if not path.exists():
        if messagebox is not None:
            messagebox.showwarning(
                "Example file",
                f"Example path was not found:\n{path}\n\n"
                "If you are running a development copy, use the public package folder that contains example_data.",
            )
        return False
    try:
        if os.name == "nt":
            os.startfile(str(path))  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(path)])
        else:
            subprocess.Popen(["xdg-open", str(path)])
    except Exception as exc:
        if messagebox is not None:
            messagebox.showerror("Example file", f"Could not open example:\n{path}\n\n{exc}")
        return False
    return True
