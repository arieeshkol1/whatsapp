import json
import os
from typing import Any, Dict

import pytest

os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from state_machine.processing import process_text as process_text_module
from state_machine.processing.process_text import (
    USER_TYPE_BUSINESS,
    USER_TYPE_CONSUMER,
)


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
                    "to_number": {"S": "972500000111"},
                    "whatsapp_id": {"S": "wamid.example"},
                    "last_seen_at": {"S": "1700000000"},
                }
            }
        },
        "text": "שלום",
        "conversation_id": 1,
        "to_number": "972500000111",
    }


def _stub_routing_dependencies(monkeypatch):
    """Neutralise external dependencies so routing logic can be asserted."""

    monkeypatch.setattr(process_text_module, "load_customer_profile", lambda *_: None)
    monkeypatch.setattr(process_text_module, "format_customer_summary", lambda *_: None)
    monkeypatch.setattr(process_text_module, "get_rules_text", lambda: None)
    monkeypatch.setattr(
        process_text_module, "_fetch_conversation_history", lambda *args, **kwargs: []
    )
    monkeypatch.setattr(
        process_text_module, "_format_history_messages", lambda *args, **kwargs: ""
    )
    monkeypatch.setattr(
        process_text_module, "format_order_progress_summary", lambda *_: None
    )
    monkeypatch.setattr(
        process_text_module,
        "_save_interaction_to_history",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        process_text_module, "_touch_user_info_record", lambda *args, **kwargs: None
    )


def test_process_text_persists_user_updates(monkeypatch):
    _stub_routing_dependencies(monkeypatch)
    monkeypatch.setattr(
        process_text_module,
        "_save_interaction_to_history",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        process_text_module, "_touch_user_info_record", lambda *args, **kwargs: None
    )


def test_process_text_persists_user_updates(monkeypatch):
    _stub_routing_dependencies(monkeypatch)
    monkeypatch.setattr(
        process_text_module,
        "_save_interaction_to_history",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        process_text_module, "_touch_user_info_record", lambda *args, **kwargs: None
    )


def test_process_text_persists_user_updates(monkeypatch):
    _stub_routing_dependencies(monkeypatch)
    monkeypatch.setattr(
        process_text_module,
        "_save_interaction_to_history",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        process_text_module, "_touch_user_info_record", lambda *args, **kwargs: None
    )


def test_process_text_persists_user_updates(monkeypatch):
    _stub_routing_dependencies(monkeypatch)
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


def test_business_user_routes_to_business_agent(monkeypatch):
    _stub_routing_dependencies(monkeypatch)

    monkeypatch.setenv("BUSINESS_AGENT_ID", "business-agent")
    monkeypatch.setenv("BUSINESS_AGENT_ALIAS_ID", "business-alias")

    consumer_called = False

    def fake_consumer(**_):
        nonlocal consumer_called
        consumer_called = True
        return "consumer"

    captured: Dict[str, Any] = {}

    def fake_business(session_id: str, input_text: str) -> str:
        captured["session_id"] = session_id
        captured["input_text"] = input_text
        return "business-response"

    monkeypatch.setattr(process_text_module, "call_bedrock_agent", fake_consumer)
    monkeypatch.setattr(
        process_text_module, "_call_business_owner_agent", fake_business
    )

    event = _base_event()
    event["assess_changes"] = {
        "user_data": {"UserType": USER_TYPE_BUSINESS, "BusinessId": "972500000111"}
    }

    result = process_text_module.ProcessText(event).process_text()

    assert not consumer_called, "Business routing should bypass the consumer agent"
    assert result["response_message"].startswith("business-response")
    assert captured["session_id"] == "972500000000-1"
    assert "[user_type=B]" in captured["input_text"]


def test_consumer_user_routes_to_consumer_agent(monkeypatch):
    _stub_routing_dependencies(monkeypatch)

    monkeypatch.setenv("CONSUMER_AGENT_ID", "consumer-agent")
    monkeypatch.setenv("CONSUMER_AGENT_ALIAS_ID", "consumer-alias")

    business_called = False

    def fake_business(
        session_id: str, input_text: str
    ) -> str:  # pragma: no cover - safety
        nonlocal business_called
        business_called = True
        return "business"

    captured: Dict[str, Any] = {}

    def fake_consumer(**kwargs):
        captured.update(kwargs)
        return "consumer-response"

    monkeypatch.setattr(process_text_module, "call_bedrock_agent", fake_consumer)
    monkeypatch.setattr(
        process_text_module, "_call_business_owner_agent", fake_business
    )

    event = _base_event()
    event["assess_changes"] = {
        "user_data": {"UserType": USER_TYPE_CONSUMER, "BusinessId": ""}
    }

    result = process_text_module.ProcessText(event).process_text()

    assert not business_called, "Consumer routing should not call the business agent"
    assert result["response_message"].startswith("consumer-response")
    assert captured["agent_id"] == "consumer-agent"
    assert captured["agent_alias_id"] == "consumer-alias"


def test_default_user_routes_to_consumer_agent(monkeypatch):
    _stub_routing_dependencies(monkeypatch)

    monkeypatch.setenv("CONSUMER_AGENT_ID", "consumer-agent")
    monkeypatch.setenv("CONSUMER_AGENT_ALIAS_ID", "consumer-alias")

    business_called = False

    def fake_business(
        session_id: str, input_text: str
    ) -> str:  # pragma: no cover - safety
        nonlocal business_called
        business_called = True
        return "business"

    captured: Dict[str, Any] = {}

    def fake_consumer(**kwargs):
        captured.update(kwargs)
        return "consumer-response"

    monkeypatch.setattr(process_text_module, "call_bedrock_agent", fake_consumer)
    monkeypatch.setattr(
        process_text_module, "_call_business_owner_agent", fake_business
    )

    event = _base_event()
    result = process_text_module.ProcessText(event).process_text()

    assert (
        not business_called
    ), "Unspecified user type should default to consumer routing"
    assert result["response_message"].startswith("consumer-response")
    assert captured["agent_id"] == "consumer-agent"
    assert captured["agent_alias_id"] == "consumer-alias"
