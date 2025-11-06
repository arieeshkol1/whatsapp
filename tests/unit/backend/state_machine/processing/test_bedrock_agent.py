import os


os.environ.setdefault("STATE_MACHINE_IMPORT_MODE", "minimal")

from backend.state_machine.processing import bedrock_agent


def test_sanitize_session_id_replaces_invalid_characters():
    sanitized = bedrock_agent._sanitize_session_id("972524347196|1")
    assert sanitized == "972524347196-1"


def test_sanitize_session_id_falls_back_when_empty():
    sanitized = bedrock_agent._sanitize_session_id("@@@@")
    assert sanitized == "default-session"


def test_sanitize_session_id_truncates_long_values():
    long_value = "a" * 300
    sanitized = bedrock_agent._sanitize_session_id(long_value)
    assert len(sanitized) <= 128
    assert sanitized == "a" * 128
