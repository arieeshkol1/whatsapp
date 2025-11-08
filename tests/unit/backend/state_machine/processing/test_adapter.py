import os

import pytest

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from state_machine.processing.adapter import Adapter


def test_adapter_builds_dynamodb_shape_for_text():
    payload = {
        "from": "+15551234567",
        "to": "+15550987654",
        "message_type": "text",
        "message_body": "שלום",  # Hebrew greeting to ensure unicode preserved
        "wa_id": "wamid.123",
        "last_seen_at": "2025-11-08T16:13:00Z",
        "message_id": "mid-001",
    }

    event = {"input": payload, "conversation_id": 9, "correlation_id": "abc"}

    result = Adapter(event).transform_input()

    new_image = result["input"]["dynamodb"]["NewImage"]
    assert new_image["type"]["S"] == "text"
    assert new_image["from_number"]["S"] == "+15551234567"
    assert new_image["text"]["S"] == "שלום"
    assert new_image["message_id"]["S"] == "mid-001"

    assert result["from_number"] == "+15551234567"
    assert result["to_number"] == "+15550987654"
    assert result["text"] == "שלום"
    assert result["raw_event"] == payload
    assert result["conversation_id"] == 9
    assert result["correlation_id"] == "abc"


def test_adapter_accepts_from_number_alias():
    payload = {
        "from_number": "+15551234567",
        "to_number": "+15550987654",
        "message_type": "text",
        "message_body": "שלום",
    }

    event = {"input": payload}

    result = Adapter(event).transform_input()

    new_image = result["input"]["dynamodb"]["NewImage"]
    assert new_image["from_number"]["S"] == "+15551234567"
    assert result["from_number"] == "+15551234567"
    assert result["to_number"] == "+15550987654"


@pytest.mark.parametrize(
    "message_type,field,value",
    [
        ("image", "image_url", "https://example.com/image.jpg"),
        ("video", "video_url", "https://example.com/video.mp4"),
        ("voice", "voice_url", "https://example.com/audio.ogg"),
        ("interactive", "interactive_payload", "{}"),
    ],
)
def test_adapter_type_specific_fields(message_type, field, value):
    payload = {
        "from": "+15551234567",
        "to": "+15550987654",
        "message_type": message_type,
        field: value,
        "wa_id": "wamid.123",
    }

    result = Adapter({"input": payload}).transform_input()

    new_image = result["input"]["dynamodb"]["NewImage"]
    assert new_image["type"]["S"] == message_type
    assert new_image[field]["S"] == value
    assert "text" not in new_image


def test_adapter_defaults_type_when_missing():
    payload = {
        "from": "+15551234567",
        "message_body": "שלום",
        "wa_id": "wamid.999",
    }

    result = Adapter({"input": payload}).transform_input()

    new_image = result["input"]["dynamodb"]["NewImage"]
    assert new_image["type"]["S"] == "text"
    assert new_image["text"]["S"] == "שלום"


def test_adapter_passthrough_for_dynamodb_payload():
    dynamodb_event = {
        "input": {
            "dynamodb": {
                "NewImage": {
                    "from_number": {"S": "+15551234567"},
                    "type": {"S": "text"},
                }
            }
        }
    }

    result = Adapter(dynamodb_event).transform_input()

    assert result["input"]["dynamodb"]["NewImage"]["type"]["S"] == "text"
