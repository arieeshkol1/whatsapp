import os
from typing import Any, Dict, List

import pytest

from common.helpers.dynamodb_helper import DynamoDBHelper


class _FakeTable:
    def __init__(
        self,
        items: List[Dict[str, Any]],
        expected_whatsapp_id: str,
        page_size: int = 1,
    ):
        self.items = items
        self.expected_whatsapp_id = expected_whatsapp_id
        self.page_size = page_size

    def query(self, **kwargs):
        limit = kwargs.get("Limit", self.page_size)
        exclusive_start_key = kwargs.get("ExclusiveStartKey") or {}
        start_index = 0
        if exclusive_start_key:
            for idx, item in enumerate(self.items):
                if item.get("PK") == exclusive_start_key.get("PK") and item.get(
                    "SK"
                ) == exclusive_start_key.get("SK"):
                    start_index = idx + 1
                    break

        evaluated_items = self.items[start_index : start_index + limit]
        items = [
            dict(item)
            for item in evaluated_items
            if item.get("whatsapp_id") == self.expected_whatsapp_id
        ]

        response: Dict[str, Any] = {"Items": items}

        if start_index + limit < len(self.items):
            last_item = evaluated_items[-1]
            response["LastEvaluatedKey"] = {
                "PK": last_item["PK"],
                "SK": last_item["SK"],
            }

        return response

    def update_item(self, **kwargs):
        update_expression: str = kwargs.get("UpdateExpression", "")
        expression_attribute_values: Dict[str, Any] = kwargs.get(
            "ExpressionAttributeValues", {}
        )
        expression_attribute_names: Dict[str, str] = kwargs.get(
            "ExpressionAttributeNames", {}
        )

        assignments = update_expression.replace("SET", "", 1).strip().split(",")
        key = kwargs.get("Key", {})
        pk = key.get("PK")
        sk = key.get("SK")

        for assignment in assignments:
            if not assignment.strip():
                continue
            left, placeholder = [part.strip() for part in assignment.split("=", 1)]
            attribute_name = expression_attribute_names.get(left, left)
            for item in self.items:
                if item.get("PK") == pk and item.get("SK") == sk:
                    item[attribute_name] = expression_attribute_values.get(placeholder)

    def get_item(self, **kwargs):
        return {"Item": self.items[0]}


def test_update_system_response_persists_full_response_when_missing(monkeypatch):
    item = {
        "PK": "972524347196",
        "SK": "MESSAGE#2025-11-20T10:24:52.172852+00:00",
        "whatsapp_id": "wamid.HBgMOTcyNTI0MzQ3MTk2FQIAEhgWM0VCMDIwQjNCMjQ2MkUyNDAxRUQ4NgA=",
        "conversation_id": 1,
    }

    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    helper = DynamoDBHelper(table_name="interaction-history", endpoint_url=None)
    helper.table = _FakeTable([item], expected_whatsapp_id=item["whatsapp_id"])  # type: ignore[attr-defined]

    system_response: Dict[str, str] = {"text": "שלום"}

    helper.update_system_response(
        partition_keys=["972524347196"],
        whatsapp_id=item["whatsapp_id"],
        system_response=system_response,
        full_response=None,
    )

    updated = helper.table.get_item(  # type: ignore[attr-defined]
        Key={"PK": item["PK"], "SK": item["SK"]}
    )["Item"]

    assert updated["system_response"] == system_response


def test_update_system_response_pages_until_match(monkeypatch):
    target_whatsapp_id = "wamid.target"
    items = [
        {
            "PK": "972524347196",
            "SK": "MESSAGE#2025-11-20T10:24:51.000000+00:00",
            "whatsapp_id": "wamid.other",
            "conversation_id": 1,
        },
        {
            "PK": "972524347196",
            "SK": "MESSAGE#2025-11-20T10:24:52.172852+00:00",
            "whatsapp_id": target_whatsapp_id,
            "conversation_id": 1,
        },
    ]

    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    helper = DynamoDBHelper(table_name="interaction-history", endpoint_url=None)
    helper.table = _FakeTable(items, expected_whatsapp_id=target_whatsapp_id, page_size=1)  # type: ignore[attr-defined]

    system_response: Dict[str, str] = {"text": "שלום"}

    helper.update_system_response(
        partition_keys=["972524347196"],
        whatsapp_id=target_whatsapp_id,
        system_response=system_response,
        full_response=None,
    )

    assert items[1]["system_response"] == system_response
