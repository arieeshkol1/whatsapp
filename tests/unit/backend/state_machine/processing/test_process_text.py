import json
import os
from typing import Any, Dict

import pytest

os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from state_machine.processing import process_text as process_text_module


@pytest.fixture(autouse=True)
def reset_users_info_table():
    original_table = process_text_module._users_info_table
    original_name = process_text_module.USER_INFO_TABLE_NAME
    process_text_module._users_info_table = None
    process_text_module.USER_INFO_TABLE_NAME = None
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

    event = _base_event()
    result = process_text_module.ProcessText(event).process_text()

    assert result["response_message"].startswith("תודה")
    assert result["conversation_state"]["date_of_event"] == "2025-01-01"
    assert result["user_updates"] == [
        {"tag": "profile.first_name", "value": "דנה"},
        {"tag": "conversation.date_of_event", "value": "2025-01-01"},
    ]

    # With no UsersInfo table configured, no table handle should be initialised.
    assert process_text_module._users_info_table is None


def test_process_text_logs_interaction_history(monkeypatch):
    recorded: Dict[str, Any] = {}

    class FakeHistoryHelper:
        def put_item(self, item):
            recorded.update(item)
            return {"status": "ok"}

    monkeypatch.setattr(process_text_module, "_history_helper", FakeHistoryHelper())
    monkeypatch.setattr(
        process_text_module, "_history_partition_keys", lambda *_: ["972500000000"]
    )
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
        lambda **_: json.dumps({"reply": "היי"}),
    )

    event = _base_event()
    event["correlation_id"] = "corr-123"

    process_text_module.ProcessText(event).process_text()

    assert recorded["PK"] == "972500000000"
    assert recorded["conversation_id"] == 1
    assert recorded["correlation_id"] == "corr-123"
    assert recorded["system_response"]["text"] == "היי"
