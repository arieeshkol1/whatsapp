from __future__ import annotations

from typing import Any, Dict

import pytest

from common import rules_config


@pytest.fixture(autouse=True)
def reset_rules(monkeypatch):
    monkeypatch.delenv("RULES_TABLE_NAME", raising=False)
    monkeypatch.delenv("RULESET_ID", raising=False)
    monkeypatch.delenv("RULESET_VERSION", raising=False)
    rules_config.reset_rules_cache()
    yield
    rules_config.reset_rules_cache()


def test_get_rules_text_without_configuration_returns_none():
    assert rules_config.get_rules_text() is None


def test_get_rules_text_with_list_instructions(monkeypatch):
    monkeypatch.setenv("RULES_TABLE_NAME", "rules-table")
    monkeypatch.setenv("RULESET_ID", "default")
    monkeypatch.setenv("RULESET_VERSION", "CURRENT")

    class _Table:
        name = "rules-table"

        def get_item(self, Key: Dict[str, Any]):  # noqa: N802 - boto3 method
            assert Key == {"PK": "RULESET#default", "SK": "VERSION#CURRENT"}
            return {"Item": {"instructions": ["Rule A", "Rule B"]}}

    resource_stub = type(
        "_Resource", (), {"Table": staticmethod(lambda name: _Table())}
    )
    monkeypatch.setattr(
        rules_config,
        "boto3",
        type("_Boto", (), {"resource": staticmethod(lambda _: resource_stub)}),
    )

    text = rules_config.get_rules_text()
    assert text == "Rule A\nRule B"


def test_get_rules_text_prefers_instruction_text(monkeypatch):
    monkeypatch.setenv("RULES_TABLE_NAME", "rules-table")

    class _Table:
        name = "rules-table"

        def get_item(self, Key):  # noqa: N802 - boto3 method
            return {"Item": {"instruction_text": "Explicit block"}}

    resource_stub = type(
        "_Resource", (), {"Table": staticmethod(lambda name: _Table())}
    )
    monkeypatch.setattr(
        rules_config,
        "boto3",
        type("_Boto", (), {"resource": staticmethod(lambda _: resource_stub)}),
    )

    text = rules_config.get_rules_text()
    assert text == "Explicit block"
