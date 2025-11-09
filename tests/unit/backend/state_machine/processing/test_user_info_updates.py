import decimal
import os
from typing import Any, Dict

import pytest

os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from state_machine.processing import process_text as process_text_module


class _FakeUsersInfoTable:
    def __init__(self):
        self.calls = []

    def update_item(self, **kwargs):  # pragma: no cover - simple recorder
        self.calls.append(kwargs)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


@pytest.fixture
def fake_table(monkeypatch):
    table = _FakeUsersInfoTable()
    monkeypatch.setattr(process_text_module, "_users_info_table", table)
    monkeypatch.setattr(process_text_module, "_get_users_info_table", lambda: table)
    return table


def test_touch_user_info_record_initialises_profile(fake_table):
    process_text_module._touch_user_info_record("9725", 1700000000)

    assert fake_table.calls, "update_item should be invoked"
    call: Dict[str, Any] = fake_table.calls[0]

    assert call["Key"] == {"PhoneNumber": "+9725"}
    update_expression = call["UpdateExpression"]
    assert "#user_info = if_not_exists(#user_info, :empty)" in update_expression
    assert "#collected = if_not_exists(#collected, :empty)" in update_expression
    assert call["ExpressionAttributeNames"]["#user_info"] == "UserInfo"
    assert call["ExpressionAttributeNames"]["#collected"] == "CollectedFields"
    assert call["ExpressionAttributeValues"][":last_seen"] == decimal.Decimal(
        "1700000000"
    )


def test_update_user_info_profile_sets_profile_map(fake_table):
    process_text_module._update_user_info_profile(
        phone_number="972542804535",
        updates={"first_name": "Dana", "email": "dana@example.com"},
        last_seen_at="1762208436",
    )

    assert len(fake_table.calls) == 1
    call = fake_table.calls[0]

    assert call["Key"] == {"PhoneNumber": "+972542804535"}
    update_expression = call["UpdateExpression"]
    # Ensure both UserInfo and CollectedFields maps receive the values
    assert "#profile.#field0 = :value0" in update_expression
    assert "#collected.#field0 = :true" in update_expression
    assert call["ExpressionAttributeNames"]["#profile"] == "UserInfo"
    assert call["ExpressionAttributeNames"]["#collected"] == "CollectedFields"
    assert call["ExpressionAttributeValues"][":value1"] == "dana@example.com"
