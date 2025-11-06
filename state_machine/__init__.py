"""Compatibility shims for legacy imports."""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Iterable

os.environ.setdefault("STATE_MACHINE_IMPORT_MODE", "minimal")
os.environ.setdefault("LOG_LEVEL", "ERROR")

_ROOT = Path(__file__).resolve().parent.parent
_BACKEND_ROOT = _ROOT / "backend"

for path in (str(_BACKEND_ROOT), str(_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

_BACKEND_PACKAGE = "backend.state_machine"


def _load_backend_package() -> ModuleType:
    return importlib.import_module(_BACKEND_PACKAGE)


def _install_alias(module_names: Iterable[str]) -> None:
    for name in module_names:
        backend_module = importlib.import_module(f"{_BACKEND_PACKAGE}.{name}")
        alias_name = f"{__name__}.{name}"
        sys.modules[alias_name] = backend_module
        setattr(sys.modules[__name__], name, backend_module)


_backend = _load_backend_package()

for attr in getattr(_backend, "__all__", []):
    setattr(sys.modules[__name__], attr, getattr(_backend, attr))

_install_alias(
    [
        "processing",
        "utils",
        "integrations",
        "state_machine_handler",
        "base_step_function",
    ]
)

__all__ = getattr(_backend, "__all__", [])
