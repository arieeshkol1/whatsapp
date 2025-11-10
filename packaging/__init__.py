from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType

_THIS_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _THIS_DIR.parent


def _load_upstream_packaging() -> ModuleType:
    name = __name__
    original_module = sys.modules.pop(name, None)
    original_sys_path = list(sys.path)
    try:
        sys.path = [
            entry
            for entry in original_sys_path
            if Path(entry).resolve() != _PROJECT_ROOT
        ]
        module = importlib.import_module(name)
    finally:
        sys.path = original_sys_path
        if original_module is not None:
            sys.modules[name] = original_module
    return module


_upstream = _load_upstream_packaging()

# Ensure submodule discovery also checks this directory for the licenses shim.
paths = list(getattr(_upstream, "__path__", []))
this_dir_str = str(_THIS_DIR)
if this_dir_str not in paths:
    paths.append(this_dir_str)
_upstream.__path__ = paths  # type: ignore[attr-defined]

# Replace this shim with the upstream module.
sys.modules[__name__] = _upstream

globals().update(_upstream.__dict__)
