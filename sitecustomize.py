"""Repository-level sitecustomize to supply vendored packaging.licenses.

Some tooling (e.g., Poetry 1.8.x) imports ``packaging.licenses`` which is
not available in packaging 24.1 pinned in the current lock file. On hosts
where packaging hasn't yet gained that submodule, we fall back to a vendored
copy bundled inside pip or poetry so commands like ``poetry run poe
black-check`` continue to work.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys


def _load_packaging_licenses(vendor_dir: pathlib.Path) -> bool:
    """Attempt to load packaging.licenses from the given vendor directory."""

    init_py = vendor_dir / "__init__.py"
    if not init_py.exists():
        return False

    spec = importlib.util.spec_from_file_location("packaging.licenses", init_py)
    if spec and spec.loader:
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[reportAny]
        sys.modules.setdefault("packaging.licenses", module)
        return True
    return False


if importlib.util.find_spec("packaging.licenses") is None:
    base = pathlib.Path(sys.prefix)
    py_dir = f"python{sys.version_info.major}.{sys.version_info.minor}"
    candidates = [
        base
        / "lib"
        / py_dir
        / "site-packages"
        / "pip"
        / "_vendor"
        / "packaging"
        / "licenses",
        base
        / "lib"
        / py_dir
        / "site-packages"
        / "poetry"
        / "core"
        / "_vendor"
        / "packaging"
        / "licenses",
    ]

    for candidate in candidates:
        if _load_packaging_licenses(candidate):
            break
