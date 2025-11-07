from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


def _load_sitecustomize():
    """Load the repository's sitecustomize module by file path."""
    module_name = "sitecustomize"
    module_path = Path(__file__).resolve().parents[3] / "backend" / "sitecustomize.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_sitecustomize_registers_backend_alias(monkeypatch):
    original_find_spec = importlib.util.find_spec

    def fake_find_spec(name: str, package: str | None = None):
        if name == "backend":
            return None
        return original_find_spec(name, package)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)

    existing_module = sys.modules.pop("backend", None)
    try:
        _load_sitecustomize()
        assert "backend" in sys.modules
        aliased = sys.modules["backend"]
        assert isinstance(aliased, types.ModuleType)
        assert getattr(aliased, "__path__", None)
    finally:
        if existing_module is not None:
            sys.modules["backend"] = existing_module
        else:
            sys.modules.pop("backend", None)
        monkeypatch.setattr(importlib.util, "find_spec", original_find_spec)
