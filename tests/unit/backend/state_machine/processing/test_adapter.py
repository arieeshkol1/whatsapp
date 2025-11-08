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

    result = Adapter(payload).transform_input()

    new_image = result["input"]["dynamodb"]["NewImage"]
    assert new_image["type"]["S"] == "text"
    assert new_image["from_number"]["S"] == "+15551234567"
    assert new_image["text"]["S"] == "שלום"
    assert new_image["message_id"]["S"] == "mid-001"

    assert result["from_number"] == "+15551234567"
    assert result["to_number"] == "+15550987654"
    assert result["text"] == "שלום"
    assert result["raw_event"] == payload


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

    result = Adapter(payload).transform_input()

    new_image = result["input"]["dynamodb"]["NewImage"]
    assert new_image["type"]["S"] == message_type
    assert new_image[field]["S"] == value
    assert "text" not in new_image
