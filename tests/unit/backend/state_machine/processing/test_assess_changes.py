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


def test_conversation_partition_keys_include_multiple_numbers():
    module = _load_module()

    keys = module._conversation_partition_keys(
        "+972524347196",
        "972502649476",
        None,
        "   ",
    )

    assert "NUMBER#+972524347196" in keys
    assert "NUMBER#972524347196" in keys
    assert "NUMBER#972502649476" in keys
    assert "972502649476" in keys


def test_rules_partition_key_variants_cover_normalized_destination():
    module = _load_module()

    variants = module._rules_partition_key_variants("972502649476")

    assert "972502649476" in variants
    assert "+972502649476" in variants
    assert "RULESET#972502649476" in variants
    assert "RULESET#+972502649476" in variants


def test_rules_partition_key_variants_handle_trailing_newline():
    module = _load_module()

    variants = module._rules_partition_key_variants("972502649476\n")

    assert "972502649476" in variants
    assert "972502649476\n" in variants
    assert "RULESET#972502649476" in variants
    assert "RULESET#972502649476\n" in variants
