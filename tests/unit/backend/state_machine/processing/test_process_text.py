import json
from typing import Any, Dict

import pytest

from state_machine.processing import process_text as process_text_module


@pytest.fixture(autouse=True)
def patch_dependencies(monkeypatch):
    monkeypatch.setattr(
        process_text_module, "_fetch_conversation_history", lambda *_1, **_2: []
    )
    monkeypatch.setattr(process_text_module, "load_customer_profile", lambda *_: None)
    monkeypatch.setattr(process_text_module, "format_customer_summary", lambda *_: None)
    monkeypatch.setattr(
        process_text_module, "extract_state_updates_from_message", lambda *_: {}
    )
    monkeypatch.setattr(
        process_text_module,
        "merge_conversation_state",
        lambda state, updates: {
            **state,
            **updates,
        },
    )
    monkeypatch.setattr(
        process_text_module, "format_order_progress_summary", lambda *_: None
    )
    monkeypatch.setattr(process_text_module, "get_rules_text", lambda *_: "")
    monkeypatch.setattr(process_text_module, "_load_user_info_details", lambda *_: {})
    monkeypatch.setattr(process_text_module, "_history_helper", None)


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


def test_process_text_includes_assess_changes_details(monkeypatch):
    captured: Dict[str, str] = {}

    def fake_call_bedrock_agent(**kwargs):
        captured.update(kwargs)
        return ""

    monkeypatch.setattr(
        process_text_module, "call_bedrock_agent", fake_call_bedrock_agent
    )

    event = _base_event()
    event["from_number"] = "972500000000"
    event["whatsapp_id"] = "wamid.example"
    event["customer_info"] = {
        "details": {"first_name": "Dana", "event_date": "2025-01-01"}
    }

    process_text_module.ProcessText(event).process_text()

    input_text = captured.get("input_text", "")
    assert "פרטי משתמש ידועים" in input_text
    assert "first_name: Dana" in input_text
    assert "event_date: 2025-01-01" in input_text
    assert "הודעת הלקוח כעת:\nשלום" in input_text


def test_process_text_merges_customer_info_without_persisting(monkeypatch):
    monkeypatch.setattr(
        process_text_module,
        "call_bedrock_agent",
        lambda **_: json.dumps(
            {
                "reply": "תודה",
                "user_updates": [
                    {"tag": "conversation.delivery_eta", "value": "tomorrow"},
                    {"tag": "profile.first_name", "value": "Dana"},
                ],
            }
        ),
    )

    event = _base_event()
    event["customer_info"] = {"details": {"first_name": "Dana"}}

    result = process_text_module.ProcessText(event).process_text()

    assert result["response_message"].startswith("תודה")
    assert result["conversation_state"]["delivery_eta"] == "tomorrow"
    assert result["user_updates"] == [
        {"tag": "conversation.delivery_eta", "value": "tomorrow"},
        {"tag": "profile.first_name", "value": "Dana"},
    ]


def test_process_text_includes_stored_user_info_in_context(monkeypatch):
    captured: Dict[str, str] = {}

    def fake_call_bedrock_agent(**kwargs):
        captured.update(kwargs)
        return ""

    monkeypatch.setattr(
        process_text_module, "call_bedrock_agent", fake_call_bedrock_agent
    )
    monkeypatch.setattr(
        process_text_module, "_load_user_info_details", lambda *_: {"name": "Dana"}
    )

    event = _base_event()
    event["from_number"] = "972500000000"

    process_text_module.ProcessText(event).process_text()

    input_text = captured.get("input_text", "")
    assert "פרטי משתמש ידועים" in input_text
    assert "name: Dana" in input_text
    assert "phone_number: 972500000000" in input_text
