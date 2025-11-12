import importlib.util
from pathlib import Path


MODULE_PATH = Path("backend/state_machine/processing/assess_changes.py")


def _load_module():
    spec = importlib.util.spec_from_file_location("assess_changes", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_assess_changes_returns_event_when_disabled():
    payload = {"foo": "bar"}
    module = _load_module()
    processor = module.AssessChanges(payload)

    assert processor.assess_and_apply() == payload


def test_conversation_key_variants_match_history_schema():
    module = _load_module()

    variants = module._conversation_key_variants("+972502425777")

    assert "NUMBER#972502425777" in variants
    assert "NUMBER#+972502425777" in variants
    assert "+972502425777" in variants
