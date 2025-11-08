import os

import pytest

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from state_machine.processing.validate_message import ValidateMessage


@pytest.fixture(autouse=True)
def reset_feature_flag(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("ASSESS_CHANGES_FEATURE", raising=False)
    yield
    monkeypatch.delenv("ASSESS_CHANGES_FEATURE", raising=False)


def test_validate_message_handles_dynamodb_image(monkeypatch):
    monkeypatch.setenv("ASSESS_CHANGES_FEATURE", "off")
    event = {
        "input": {
            "dynamodb": {
                "NewImage": {
                    "from_number": {"S": "+15551234567"},
                    "type": {"S": "text"},
                    "text": {"S": "hello"},
                    "whatsapp_id": {"S": "wamid.123"},
                    "correlation_id": {"S": "corr-123"},
                    "conversation_id": {"N": "7"},
                }
            }
        }
    }

    validator = ValidateMessage(event)
    result = validator.validate_input()

    assert result["validated"] is True
    assert result["message_type"] == "text"
    assert result["conversation_id"] == 7
    assert result["features"]["assess_changes"] == "off"


def test_validate_message_accepts_direct_payload(monkeypatch):
    monkeypatch.setenv("ASSESS_CHANGES_FEATURE", "on")
    event = {
        "from_number": "+15559876543",
        "text": "hi there",
        "whatsapp_id": "wamid.manual",
        "features": {},
        "conversation_id": 3,
    }

    validator = ValidateMessage(event)
    result = validator.validate_input()

    assert result["validated"] is True
    assert result["message_type"] == "text"
    assert result["from_number"] == "+15559876543"
    assert result["whatsapp_id"] == "wamid.manual"
    assert result["conversation_id"] == 3
    assert result["features"]["assess_changes"] == "on"


def test_validate_message_defaults_type_when_missing(monkeypatch):
    event = {
        "input": {
            "dynamodb": {
                "NewImage": {
                    "from_number": {"S": "+15551234567"},
                    "text": {"S": "Name Dana"},
                    "whatsapp_id": {"S": "wamid.abc"},
                }
            }
        },
        "text": "Name Dana",
    }

    validator = ValidateMessage(event)
    result = validator.validate_input()

    assert result["message_type"] == "text"
    assert result["text"] == "Name Dana"
