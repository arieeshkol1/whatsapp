"""Unit tests for the Bedrock agent helper."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parents[5]
MODULE_PATH = ROOT_DIR / "backend" / "state_machine" / "processing" / "bedrock_agent.py"

spec = importlib.util.spec_from_file_location("bedrock_agent_for_tests", MODULE_PATH)
bedrock_agent = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = bedrock_agent
assert spec.loader is not None  # narrow type for mypy/linters
spec.loader.exec_module(bedrock_agent)  # type: ignore[attr-defined]


class _DummyBedrockRuntime:
    """Lightweight stub that mimics the Bedrock runtime client."""

    class exceptions:  # pylint: disable=too-few-public-methods
        """Namespace for exception types used by the helper."""

        class AccessDeniedException(Exception):
            """Placeholder exception to satisfy isinstance checks."""

    def __init__(self):
        self.calls: list[dict[str, str]] = []

    def invoke_agent(self, **kwargs):  # noqa: D401 - simple stub
        """Capture the invocation payload and mimic a streaming response."""

        self.calls.append(kwargs)
        return {"completion": [{"chunk": {"bytes": b"Hello"}}]}


@pytest.fixture(autouse=True)
def _configure_environment(monkeypatch):
    """Ensure environment-based configuration paths avoid real AWS calls."""

    monkeypatch.setenv("AGENT_ID", "test-agent")
    monkeypatch.setenv("AGENT_ALIAS_ID", "alias")
    monkeypatch.setenv("AWS_REGION", "us-east-1")


@pytest.fixture(autouse=True)
def _stub_runtime(monkeypatch):
    """Patch the module-level Bedrock runtime client with the dummy stub."""

    dummy_client = _DummyBedrockRuntime()
    monkeypatch.setattr(
        bedrock_agent, "bedrock_agent_runtime_client", dummy_client, raising=False
    )
    return dummy_client


def test_call_bedrock_agent_supports_positional_arguments(_stub_runtime):
    """Calling with positional arguments should succeed without errors."""

    response = bedrock_agent.call_bedrock_agent("Hi there", "session-123")

    assert response == "Hello"
    assert _stub_runtime.calls[0]["inputText"] == "Hi there"
    assert _stub_runtime.calls[0]["sessionId"] == "session-123"


def test_call_bedrock_agent_supports_keyword_arguments(_stub_runtime):
    """Calling with keyword arguments continues to work as expected."""

    response = bedrock_agent.call_bedrock_agent(
        input_text="Howdy", session_id="session-456"
    )

    assert response == "Hello"
    assert _stub_runtime.calls[0]["inputText"] == "Howdy"
    assert _stub_runtime.calls[0]["sessionId"] == "session-456"


def test_call_bedrock_agent_rejects_extra_positional_arguments():
    """More than two positional arguments should raise a ``TypeError``."""

    with pytest.raises(TypeError):
        bedrock_agent.call_bedrock_agent("hi", "session", "unexpected")


def test_call_bedrock_agent_requires_input_text():
    """Fail fast when no input text is provided by caller."""

    with pytest.raises(ValueError):
        bedrock_agent.call_bedrock_agent(session_id="session-789")

