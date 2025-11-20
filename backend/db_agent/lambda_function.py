import json
import os
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

USER_DATA_TABLE = os.environ.get("USER_DATA_TABLE")
INTERACTION_TABLE = os.environ.get("INTERACTION_TABLE")

dynamodb = boto3.resource("dynamodb")


def _get_param(parameters: List[Dict[str, Any]], name: str) -> Optional[str]:
    for param in parameters:
        if param.get("name") == name:
            return param.get("value")
    return None


def _query_interaction_table(
    table_name: Optional[str], partition_key: str, sort_key_prefix: str
) -> List[Dict[str, Any]]:
    if not table_name:
        return []

    table = dynamodb.Table(table_name)
    key_condition = Key("PK").eq(partition_key)
    if sort_key_prefix:
        key_condition &= Key("SK").begins_with(sort_key_prefix)

    try:
        response = table.query(KeyConditionExpression=key_condition, Limit=50)
    except ClientError:
        return []

    items = response.get("Items", [])
    while response.get("LastEvaluatedKey"):
        response = table.query(
            KeyConditionExpression=key_condition,
            Limit=50,
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        items.extend(response.get("Items", []))
    return items


def _get_user_data(
    table_name: Optional[str], phone_number: str
) -> List[Dict[str, Any]]:
    if not table_name:
        return []

    table = dynamodb.Table(table_name)
    candidates = [phone_number]
    trimmed = phone_number.strip()
    if trimmed and trimmed not in candidates:
        candidates.append(trimmed)

    for candidate in candidates:
        try:
            response = table.get_item(Key={"PhoneNumber": candidate})
        except ClientError:
            continue

        item = response.get("Item") if isinstance(response, dict) else None
        if item:
            return [item]

    return []


def _stringify_items(items: List[Dict[str, Any]]) -> List[str]:
    results: List[str] = []
    for item in items:
        try:
            results.append(json.dumps(item, ensure_ascii=False))
        except TypeError:
            results.append(str(item))
    return results


def action_group_query_user_data(parameters: List[Dict[str, Any]]) -> List[str]:
    phone_number = _get_param(parameters, "phone_number")
    if not phone_number:
        return ["phone_number is required for QueryUserData"]

    items = _get_user_data(USER_DATA_TABLE, phone_number)
    if not items:
        return ["No matching user data found"]
    return _stringify_items(items)


def action_group_query_interaction_history(
    parameters: List[Dict[str, Any]]
) -> List[str]:
    partition_key = _get_param(parameters, "partition_key")
    sort_key_prefix = _get_param(parameters, "sort_key_prefix") or ""
    if not partition_key:
        return ["partition_key is required for QueryInteractionHistory"]

    items = _query_interaction_table(INTERACTION_TABLE, partition_key, sort_key_prefix)
    if not items:
        return ["No matching interaction history found"]
    return _stringify_items(items)


def lambda_handler(event, context):
    action_group = event.get("actionGroup")
    _function = event.get("function")
    parameters = event.get("parameters", [])

    if action_group == "QueryUserData":
        results = action_group_query_user_data(parameters)
    elif action_group == "QueryInteractionHistory":
        results = action_group_query_interaction_history(parameters)
    else:
        raise ValueError(f"Action Group <{action_group}> not supported.")

    response_body = {
        "TEXT": {"body": "\n".join(results)},
        "results": results,
    }

    action_response = {
        "actionGroup": action_group,
        "function": _function,
        "functionResponse": {"responseBody": response_body},
    }

    function_response = {
        "response": action_response,
        "messageVersion": event.get("messageVersion"),
    }

    return function_response
