import os

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from state_machine.processing import bedrock_agent


def test_sanitize_session_id_replaces_disallowed_characters():
    raw = "972524347196|1"
    sanitized = bedrock_agent._sanitize_session_id(raw)
    assert "|" not in sanitized
    assert sanitized == "972524347196-1"


def test_sanitize_session_id_returns_default_when_empty():
    assert bedrock_agent._sanitize_session_id("@@@") == "default-session"
