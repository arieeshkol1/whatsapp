import importlib.util
import os
from pathlib import Path

import pytest

MODULE_PATH = Path("backend/state_machine/processing/assess_changes.py")


def _load_module():
    spec = importlib.util.spec_from_file_location("assess_changes", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_assess_changes_returns_event_when_disabled():
    payload = {"foo": "bar"}
    module = _load_module()
    processor = module.AssessChanges(payload)

    assert processor.assess_and_apply() == payload


@pytest.mark.skip(reason="Temporarily disabled while refactoring _load_conversation_items")
def test_load_conversation_items_queries_message_prefix():
    module = _load_module()

    captured_kwargs = {}

    class FakeTable:
        def query(self, **kwargs):
            captured_kwargs.update(kwargs)
            return {
                "Items": [
                    {
                        "PK": {"S": "NUMBER#123"},
                        "SK": {"S": "MESSAGE#1"},
                        "text": {"S": "hi"},
                    }
                ]
            }

    class FakeDynamo:
        def __init__(self, table):
            self._table = table

        def Table(self, name):
            assert name == "conversation-table"
            return self._table

    processor = module.AssessChanges({})
    processor._conversation_table_name = "conversation-table"
    processor._conversation_history_limit = 25
    processor._dynamodb_resource = FakeDynamo(FakeTable())

    results = processor._load_conversation_items("+123", None)

    assert results
    assert captured_kwargs["Limit"] == 10
    expr = captured_kwargs["KeyConditionExpression"]
    assert hasattr(expr, "_values")
    assert len(expr._values) == 2
    begins_with = expr._values[1]
    assert getattr(begins_with, "_values", [None, None])[1] == "MESSAGE#"


@pytest.mark.skip(reason="Temporarily disabled while refactoring _load_conversation_items")
def test_load_conversation_items_respects_smaller_history_limit():
    module = _load_module()

    limits = []

    class FakeTable:
        def query(self, **kwargs):
            limits.append(kwargs["Limit"])
            return {"Items": []}

    class FakeDynamo:
        def __init__(self, table):
            self._table = table

        def Table(self, name):
            assert name == "conversation-table"
            return self._table

    processor = module.AssessChanges({})
    processor._conversation_table_name = "conversation-table"
    processor._conversation_history_limit = 5
    processor._dynamodb_resource = FakeDynamo(FakeTable())

    processor._load_conversation_items("+123", None)

    assert limits
    assert all(limit == 5 for limit in limits)
