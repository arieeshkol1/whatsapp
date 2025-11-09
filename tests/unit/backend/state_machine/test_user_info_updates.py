import os

import pytest

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from state_machine.processing import process_text as process_text_module  # noqa: E402


class _FakeUsersInfoTable:
    def __init__(self):
        self.calls = []

    def update_item(self, **kwargs):  # pragma: no cover - simple stub behaviour
        self.calls.append(kwargs)


@pytest.fixture(autouse=True)
def reset_users_info_table():
    original_table = process_text_module._users_info_table
    original_name = process_text_module.USER_INFO_TABLE_NAME
    process_text_module._users_info_table = None
    process_text_module.USER_INFO_TABLE_NAME = "UsersInfo"
    yield
    process_text_module._users_info_table = original_table
    process_text_module.USER_INFO_TABLE_NAME = original_name


@pytest.fixture
def fake_table():
    table = _FakeUsersInfoTable()
    process_text_module._users_info_table = table
    return table


def test_touch_user_info_record_initialises_profile(fake_table):
    process_text_module._touch_user_info_record("9725", 1700000000)

    assert fake_table.calls, "update_item should be invoked"
    call = fake_table.calls[0]

    assert call["Key"] == {"PhoneNumber": "+9725"}
    update_expression = call["UpdateExpression"]
    assert "SET #info = if_not_exists(#info, :empty)" in update_expression
    assert "#collected = if_not_exists(#collected, :empty)" in update_expression
    assert (
        call["ExpressionAttributeNames"]["#info"]
        == process_text_module.USER_INFO_ATTRIBUTE
    )
    assert call["ExpressionAttributeValues"][":last_seen"]


def test_update_user_info_profile_sets_profile_map(fake_table):
    process_text_module._update_user_info_profile(
        phone_number="972542804535",
        updates={"profile.first_name": "Dana", "profile.email": "dana@example.com"},
        last_seen_at="1762208436",
    )

    assert len(fake_table.calls) == 1
    call = fake_table.calls[0]

    assert call["Key"] == {"PhoneNumber": "+972542804535"}
    update_expression = call["UpdateExpression"]
    assert update_expression.startswith("SET ")
    assert "#info = if_not_exists(#info, :empty)" in update_expression
    assert "#collected = if_not_exists(#collected, :empty)" in update_expression
    assert any(
        fragment.startswith("#info.#field0_0")
        for fragment in update_expression.split(", ")
    )
    assert any(
        fragment.startswith("#collected.#field0_0")
        for fragment in update_expression.split(", ")
    )

    names = call["ExpressionAttributeNames"]
    assert names["#field0_0"] == "profile"
    assert names["#field0_1"] == "first_name"
    assert names["#field1_0"] == "profile"
    assert names["#field1_1"] == "email"

    values = call["ExpressionAttributeValues"]
    assert values[":value0"] == "Dana"
    assert values[":value1"] == "dana@example.com"
    assert values[":true"] is True
