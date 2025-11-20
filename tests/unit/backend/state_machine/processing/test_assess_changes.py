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


def test_extract_user_name_prefers_attributes_map():
    module = _load_module()
    user_data = {
        "PhoneNumber": "+123",
        "Attributes": {"Name": "Jane", "FamilyName": "Doe"},
    }

    assert module._extract_user_name(user_data) == "Jane Doe"


def test_determine_user_type_accepts_attributes_map():
    module = _load_module()
    processor = module.AssessChanges({"features": {"assess_changes": True}})
    user_data = {"Attributes": {"Name": "Jane"}}

    assert processor._determine_user_type(user_data) == "existing_customer"


def test_normalize_user_type_defaults_to_consumer():
    module = _load_module()

    assert module._normalize_user_type(None) == "C"
    assert module._normalize_user_type(" ") == "C"
    assert module._normalize_user_type("b") == "B"


def test_json_safe_value_preserves_nested_attributes():
    module = _load_module()

    assert module._json_safe_value(
        {"Attributes": {"Name": " Jane ", "Address": {"City": "NY"}}}
    ) == {"Attributes": {"Name": "Jane", "Address": {"City": "NY"}}}


def test_unwrap_attribute_handles_dynamodb_attribute_maps():
    module = _load_module()

    raw_item = {
        "PhoneNumber": {"S": "+123"},
        "Attributes": {
            "M": {
                "Name": {"S": "Jane"},
                "FamilyName": {"S": "Doe"},
                "Nested": {"M": {"Flag": {"BOOL": True}}},
                "Tags": {"L": [{"S": "vip"}, {"NULL": True}]},
            }
        },
    }

    assert module._unwrap_attribute(raw_item) == {
        "PhoneNumber": "+123",
        "Attributes": {
            "Name": "Jane",
            "FamilyName": "Doe",
            "Nested": {"Flag": True},
            "Tags": ["vip", None],
        },
    }
