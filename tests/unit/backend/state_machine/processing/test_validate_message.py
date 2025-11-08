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
        "raw_event": {
            "from": "+15551234567",
            "message_type": "text",
            "message_body": "hello",
            "wa_id": "wamid.123",
        },
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
        },
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
        "from": "+15559876543",
        "message_body": "hi there",
        "wa_id": "wamid.manual",
        "message_type": "text",
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


def test_validate_message_hydrates_from_number_alias():
    event = {
        "raw_event": {
            "from_number": "+15557771234",
            "message_body": "שלום",
            "message_type": "text",
            "wa_id": "wamid.alias",
        },
        "input": {"dynamodb": {"NewImage": {"text": {"S": "שלום"}}}},
    }

    validator = ValidateMessage(event)
    result = validator.validate_input()

    assert result["from_number"] == "+15557771234"
    assert result["message_type"] == "text"


def test_validate_message_defaults_type_when_missing(monkeypatch):
    event = {
        "raw_event": {
            "from": "+15551234567",
            "message_body": "Name Dana",
            "wa_id": "wamid.abc",
        },
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


def test_validate_message_requires_type_specific_fields(monkeypatch):
    event = {
        "raw_event": {
            "from": "+15550000000",
            "message_type": "image",
            "wa_id": "wamid.999",
        },
        "input": {
            "dynamodb": {
                "NewImage": {
                    "from_number": {"S": "+15550000000"},
                    "type": {"S": "image"},
                    "whatsapp_id": {"S": "wamid.999"},
                }
            }
        },
    }

    validator = ValidateMessage(event)

    with pytest.raises(ValueError) as excinfo:
        validator.validate_input()

    assert "image_url" in str(excinfo.value)
