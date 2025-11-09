import json
from typing import Any, Dict

import pytest

from state_machine.processing import process_text as process_text_module


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

    event = _base_event()
    result = process_text_module.ProcessText(event).process_text()

    assert result["response_message"].startswith("תודה")
    assert result["conversation_state"]["date_of_event"] == "2025-01-01"
    assert result["user_updates"] == [
        {"tag": "profile.first_name", "value": "דנה"},
        {"tag": "conversation.date_of_event", "value": "2025-01-01"},
    ]

    # Two calls are expected: one to touch the record and one to persist profile updates.
    assert len(stub_table.update_calls) >= 2
    update_expression = stub_table.update_calls[-1]["UpdateExpression"]
    assert "#info." in update_expression
    expression_values = stub_table.update_calls[-1]["ExpressionAttributeValues"]
    assert any(value == "דנה" for value in expression_values.values())
