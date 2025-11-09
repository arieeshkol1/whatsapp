import json
import os
from typing import Dict

import pytest

from state_machine.processing import process_text as process_text_module

@pytest.fixture(autouse=True)
def patch_dependencies(monkeypatch):
    monkeypatch.setattr(process_text_module, "_touch_user_info_record", lambda *_: None)
    monkeypatch.setattr(
        process_text_module, "_fetch_conversation_history", lambda *_: []
    )
    monkeypatch.setattr(process_text_module, "load_customer_profile", lambda *_: None)
    monkeypatch.setattr(process_text_module, "format_customer_summary", lambda *_: None)
    monkeypatch.setattr(
        process_text_module, "extract_state_updates_from_message", lambda *_: {}
    )
    monkeypatch.setattr(
        process_text_module, "merge_conversation_state", lambda *_1, **_2: {}
    )
    monkeypatch.setattr(
        process_text_module, "format_order_progress_summary", lambda *_: None
    )
    monkeypatch.setattr(
        process_text_module, "_update_user_info_details", lambda *_: None
    )
    monkeypatch.setattr(
        process_text_module, "_update_user_info_profile", lambda *_: None
    )
    monkeypatch.setattr(process_text_module, "_load_user_info_details", lambda *_: {})
    monkeypatch.setattr(process_text_module, "get_rules_text", lambda *_: "")
    monkeypatch.setattr(process_text_module, "_history_helper", None)

class StubUsersInfoTable:
    def __init__(self) -> None:
        self.update_calls = []
        self.items: Dict[str, Dict[str, Any]] = {}

    def update_item(self, **kwargs) -> None:  # pragma: no cover - simple recorder
        self.update_calls.append(kwargs)

    def get_item(self, Key: Dict[str, Any]) -> Dict[str, Any]:  # pragma: no cover
        phone = Key.get("PhoneNumber")
        return {"Item": self.items.get(phone, {})}


@pytest.fixture(autouse=True)
def reset_users_info_table():
    original_table = process_text_module._users_info_table
    original_name = process_text_module.USER_INFO_TABLE_NAME
    process_text_module._users_info_table = None
    process_text_module.USER_INFO_TABLE_NAME = "UsersInfoTest"
    yield
    process_text_module._users_info_table = original_table
    process_text_module.USER_INFO_TABLE_NAME = original_name


def _base_event() -> Dict[str, Any]:
    return {
        "input": {
            "dynamodb": {
                "NewImage": {
                    "text": {"S": "שלום"},
                    "from_number": {"S": "972500000000"},
                    "whatsapp_id": {"S": "wamid.example"},
                    "last_seen_at": {"S": "1700000000"},
                }
            }
        },
        "text": "שלום",
        "conversation_id": 1,
    }


def test_process_text_persists_user_updates(monkeypatch):
    stub_table = StubUsersInfoTable()
    process_text_module._users_info_table = stub_table

    monkeypatch.setattr(process_text_module, "load_customer_profile", lambda *_: None)
    monkeypatch.setattr(process_text_module, "format_customer_summary", lambda *_: None)
    monkeypatch.setattr(process_text_module, "get_rules_text", lambda: None)
    monkeypatch.setattr(
        process_text_module, "_fetch_conversation_history", lambda *args, **kwargs: []
    )
    monkeypatch.setattr(
        process_text_module, "format_order_progress_summary", lambda *_: None
    )
    monkeypatch.setattr(
        process_text_module,
        "call_bedrock_agent",
        lambda **_: json.dumps(
            {
                "reply": "תודה",
                "user_updates": [
                    {"tag": "profile.first_name", "value": "דנה"},
                    {"tag": "conversation.date_of_event", "value": "2025-01-01"},
                ],
            }
        ),
    )

    assert result["response_message"] == "תודה רבה"
    assert "user_updates" not in result
    assert called is False


def test_process_text_includes_user_info_context(monkeypatch):
    captured: Dict[str, str] = {}

    def fake_call_bedrock_agent(**kwargs):
        captured["input_text"] = kwargs["input_text"]
        return ""

    monkeypatch.setattr(
        process_text_module,
        "_load_user_info_details",
        lambda *_: {"first_name": "Dana", "event_date": "2025-01-01"},
    )
    monkeypatch.setattr(
        process_text_module, "call_bedrock_agent", fake_call_bedrock_agent
    )

    event = {
        "input": {
            "dynamodb": {
                "NewImage": {
                    "text": {"S": "שלום"},
                    "from_number": {"S": "972542804535"},
                    "whatsapp_id": {"S": "wamid.123"},
                }
            }
        },
        "text": "שלום",
        "from_number": "972542804535",
        "whatsapp_id": "wamid.123",
        "conversation_id": 1,
    }

    ProcessText(event).process_text()

    assert "פרטי משתמש ידועים" in captured["input_text"]
    assert "first_name: Dana" in captured["input_text"]
