import json
import os

import pytest

os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from state_machine.processing import process_text as process_text_module
from state_machine.processing.process_text import ProcessText


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
    monkeypatch.setattr(process_text_module, "get_rules_text", lambda *_: "")
    monkeypatch.setattr(process_text_module, "_history_helper", None)


def test_process_text_parses_structured_json(monkeypatch):
    captured = {}

    def fake_profile_update(phone, updates, last_seen):
        captured["phone"] = phone
        captured["updates"] = updates
        captured["last_seen"] = last_seen

    monkeypatch.setattr(
        process_text_module, "_update_user_info_profile", fake_profile_update
    )
    monkeypatch.setattr(
        process_text_module,
        "call_bedrock_agent",
        lambda **_: json.dumps(
            {
                "reply": "תודה &quot;לך&quot;",
                "user_updates": {
                    "first_name": "דנה",
                    "email": "dana@example.com",
                },
            }
        ),
    )

    event = {
        "input": {
            "dynamodb": {
                "NewImage": {
                    "text": {"S": "חברה טובה"},
                    "from_number": {"S": "972542804535"},
                    "whatsapp_id": {"S": "wamid.123"},
                    "last_seen_at": {"S": "1762208436"},
                }
            }
        },
        "text": "חברה טובה",
        "from_number": "972542804535",
        "whatsapp_id": "wamid.123",
        "message_type": "text",
        "conversation_id": 2,
        "features": {"assess_changes": "off"},
        "last_seen_at": "1762208436",
    }

    result = ProcessText(event).process_text()

    assert result["response_message"] == 'תודה "לך"'
    assert result["user_updates"] == {
        "first_name": "דנה",
        "email": "dana@example.com",
    }
    assert captured["updates"] == {
        "first_name": "דנה",
        "email": "dana@example.com",
    }
    assert captured["last_seen"] == "1762208436"


def test_process_text_handles_non_json_response(monkeypatch):
    called = False

    def fake_profile_update(*_args, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(
        process_text_module, "_update_user_info_profile", fake_profile_update
    )
    monkeypatch.setattr(
        process_text_module,
        "call_bedrock_agent",
        lambda **_: "תודה רבה",
    )

    event = {
        "input": {
            "dynamodb": {
                "NewImage": {
                    "text": {"S": "חברה טובה"},
                    "from_number": {"S": "972542804535"},
                    "whatsapp_id": {"S": "wamid.123"},
                }
            }
        },
        "text": "חברה טובה",
        "from_number": "972542804535",
        "whatsapp_id": "wamid.123",
        "message_type": "text",
        "conversation_id": 2,
        "features": {"assess_changes": "off"},
    }

    result = ProcessText(event).process_text()

    assert result["response_message"] == "תודה רבה"
    assert "user_updates" not in result
    assert called is False
