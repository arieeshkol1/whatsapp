import os
from copy import deepcopy

import boto3
import pytest
from moto import mock_aws

from backend.state_machine.processing.assess_changes import (
    AssessChanges,
    _is_enabled,
    _normalize_phone,
)

@pytest.mark.parametrize(
    "flag,expected",
    [
        (None, False),
        ("off", False),
        ("0", False),
        ("On", True),
        ("true", True),
        ("ENABLED", True),
        ("yes", True),
        (1, True),
    ],
)
def test_is_enabled(flag, expected):
    assert _is_enabled(flag) is expected


@pytest.mark.parametrize(
    "raw,normalized",
    [
        (None, None),
        ("", None),
        ("  ", None),
        ("+15551234567", "+15551234567"),
        ("15551234567", "+15551234567"),
        (" 97252 ", "+97252"),
        ("foo", "foo"),
    ],
)
def test_normalize_phone(raw, normalized):
    assert _normalize_phone(raw) == normalized


def _base_event(phone="+15550123456"):
    return {
        "features": {"assess_changes": "on"},
        "input": {"from": phone, "message_type": "text", "message_body": "hi"},
    }


@mock_aws
def test_assess_changes_with_name_in_user_data():
    os.environ["USER_DATA_TABLE"] = "UserData"
    os.environ["DYNAMODB_TABLE"] = "Conversation"
    os.environ["ASSESS_CHANGES_FEATURE"] = "on"

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    dynamodb.create_table(
        TableName="UserData",
        KeySchema=[{"AttributeName": "PhoneNumber", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "PhoneNumber", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    user_table = dynamodb.Table("UserData")

    dynamodb.create_table(
        TableName="Conversation",
        KeySchema=[
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    convo_table = dynamodb.Table("Conversation")

    phone = "+15550123456"
    user_table.put_item(
        Item={"PhoneNumber": phone, "Name": "Test User", "SomeOtherField": "X"}
    )

    convo_table.put_item(Item={"PK": f"NUMBER#{phone}", "SK": "MSG#001", "body": "a"})
    convo_table.put_item(Item={"PK": f"NUMBER#{phone}", "SK": "MSG#002", "body": "b"})

    ac = AssessChanges(event=deepcopy(_base_event(phone)))
    out = ac.assess_and_apply()

    assert "assess_changes" in out
    payload = out["assess_changes"]
    assert payload["user_data"]["PhoneNumber"] == phone
    assert payload["user_data"]["Name"] == "Test User"
    assert payload["user_name"] == "Test User"
    assert len(payload["conversation_items"]) == 2


@mock_aws
def test_assess_changes_without_name():
    os.environ["USER_DATA_TABLE"] = "UserData"
    os.environ["DYNAMODB_TABLE"] = "Conversation"
    os.environ["ASSESS_CHANGES_FEATURE"] = "on"

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    dynamodb.create_table(
        TableName="UserData",
        KeySchema=[{"AttributeName": "PhoneNumber", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "PhoneNumber", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    user_table = dynamodb.Table("UserData")

    dynamodb.create_table(
        TableName="Conversation",
        KeySchema=[
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    phone = "+972524347196"
    user_table.put_item(Item={"PhoneNumber": phone})

    ac = AssessChanges(event={"features": {"assess_changes": "on"}, "from_number": phone})
    out = ac.assess_and_apply()

    assert "assess_changes" in out
    payload = out["assess_changes"]
    assert payload["user_data"]["PhoneNumber"] == phone
    assert payload["user_data"].get("Name") is None
    assert "user_name" not in payload


@mock_aws
def test_disabled_feature_returns_event_unchanged():
    os.environ.pop("ASSESS_CHANGES_FEATURE", None)
    ac = AssessChanges(event={"from_number": "+1"})
    out = ac.assess_and_apply()
    assert "assess_changes" not in out
