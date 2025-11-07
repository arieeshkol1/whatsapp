"""State machine flow smoke tests.

These tests exercise the dynamic dispatch wiring that the Step Functions
Lambda relies on.  They intentionally avoid any external network calls by
targeting the lightweight ``ValidateMessage`` step which only performs
payload validation.
"""

from __future__ import annotations

from backend.state_machine import (
    ProcessText,
    ProcessVoice,
    SendMessage,
    ValidateMessage,
)
from backend.state_machine.state_machine_handler import lambda_handler


def test_expected_state_machine_classes_are_exposed() -> None:
    """The state machine package should expose the core handler classes."""

    for klass in (ValidateMessage, ProcessText, ProcessVoice, SendMessage):
        assert isinstance(klass, type), f"{klass!r} is not a class"


class _DummyLambdaContext:
    function_name = "test_fn"
    memory_limit_in_mb = 128
    invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test_fn"
    aws_request_id = "unit-test-request"


def test_validate_message_lambda_flow() -> None:
    """Invoking the Lambda handler should execute the ValidateMessage step."""

    event = {
        "params": {"class_name": "ValidateMessage", "method_name": "validate_input"},
        "event": {
            "input": {
                "dynamodb": {
                    "NewImage": {
                        "type": {"S": "text"},
                        "correlation_id": {"S": "corr-123"},
                    }
                }
            }
        },
    }

    result = lambda_handler(event, _DummyLambdaContext())

    assert result["message_type"] == "text"
    assert result["correlation_id"] == "corr-123"
    # The step should signal that processing can continue.
    assert result["ExceptionOcurred"] is False
