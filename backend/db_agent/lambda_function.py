import json
import os
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# Environment variables
USER_DATA_TABLE = os.environ.get("USER_DATA_TABLE")
INTERACTION_TABLE = os.environ.get("INTERACTION_TABLE")

dynamodb = boto3.resource("dynamodb")


def _get_param(parameters: List[Dict[str, Any]], name: str) -> Optional[str]:
    """
    Helper to extract a parameter by name from the parameters list.

    Supports:
      { "name": "phone_number", "value": "+972..." }
    and defensively:
      { "name": "phone_number", "value": { "stringValue": "+972..." } }
    """
    for param in parameters:
        if param.get("name") != name:
            continue

        value = param.get("value")
        if isinstance(value, str):
            return value
        if isinstance(value, dict) and "stringValue" in value:
            return value["stringValue"]
        if value is not None:
            return str(value)

    return None


def _safe_json(data: Any) -> str:
    """
    JSON-dumps with handling for Decimal and other non-serializable types.
    """
    return json.dumps(data, ensure_ascii=False, default=str)


def _get_user_data(
    table_name: Optional[str],
    phone_number: str,
) -> List[Dict[str, Any]]:
    """
    Fetch user data from USER_DATA_TABLE by phone number.

    IMPORTANT: "PhoneNumber" must match your DynamoDB PK name exactly.
    """
    if not table_name:
        print("USER_DATA_TABLE env var not set")
        return []

    table = dynamodb.Table(table_name)

    candidates = [phone_number]
    trimmed = phone_number.strip()
    if trimmed and trimmed not in candidates:
        candidates.append(trimmed)

    for candidate in candidates:
        try:
            response = table.get_item(Key={"PhoneNumber": candidate})
        except ClientError as e:
            print(f"DynamoDB get_item failed for {candidate}: {e}")
            continue

        item = response.get("Item") if isinstance(response, dict) else None
        if item:
            return [item]

    return []


def _query_user_interactions(
    table_name: Optional[str],
    to_number: str,
    from_number: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
) -> List[Dict[str, Any]]:
    """
    Query interactions for a business (to_number) and optional customer (from_number).

    Uses the GSI_To_From index to isolate a single business and optionally a
    specific customer.
    """
    if not table_name:
        print("INTERACTION_TABLE env var not set")
        return []

    if not to_number:
        print("to_number is required to query interactions")
        return []

    table = dynamodb.Table(table_name)

    print(
        f"DEBUG: _query_user_interactions for to_number={to_number}, "
        f"from_number={from_number}, start_date={start_date}, end_date={end_date}"
    )

    key_condition = Key("to_number").eq(to_number)
    if from_number:
        key_condition = key_condition & Key("from_number").eq(from_number.lstrip("+"))

    query_kwargs: Dict[str, Any] = {
        "KeyConditionExpression": key_condition,
        "IndexName": "GSI_To_From",
    }

    # Build FilterExpression on timestamp if date filters are provided
    filter_expr = None
    if start_date and end_date:
        filter_expr = Attr("timestamp").between(
            start_date, end_date + "T99:99:99"
        )
    elif start_date:
        filter_expr = Attr("timestamp").gte(start_date)
    elif end_date:
        filter_expr = Attr("timestamp").lte(end_date + "T99:99:99")

    if filter_expr is not None:
        query_kwargs["FilterExpression"] = filter_expr

    items: List[Dict[str, Any]] = []

    try:
        response = table.query(Limit=50, **query_kwargs)
        print(f"DEBUG: first page item count={len(response.get('Items', []))}")
        items.extend(response.get("Items", []))

        while response.get("LastEvaluatedKey"):
            response = table.query(
                ExclusiveStartKey=response["LastEvaluatedKey"],
                Limit=50,
                **query_kwargs,
            )
            print(f"DEBUG: next page item count={len(response.get('Items', []))}")
            items.extend(response.get("Items", []))
    except ClientError as e:
        print(f"DynamoDB query failed in _query_user_interactions: {e}")
        return []

    return items


def _scan_interactions_global(
    table_name: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
) -> List[Dict[str, Any]]:
    """
    Scan interactions across ALL users, optionally filtered by date range.

    Uses Scan with FilterExpression on timestamp.
    """
    if not table_name:
        print("INTERACTION_TABLE env var not set")
        return []

    print(
        f"DEBUG: _scan_interactions_global start_date={start_date}, "
        f"end_date={end_date}"
    )

    table = dynamodb.Table(table_name)

    filter_expr = None
    if start_date and end_date:
        filter_expr = Attr("timestamp").between(start_date, end_date + "T99:99:99")
    elif start_date:
        filter_expr = Attr("timestamp").gte(start_date)
    elif end_date:
        filter_expr = Attr("timestamp").lte(end_date + "T99:99:99")

    scan_kwargs: Dict[str, Any] = {}
    if filter_expr is not None:
        scan_kwargs["FilterExpression"] = filter_expr

    items: List[Dict[str, Any]] = []

    try:
        response = table.scan(Limit=50, **scan_kwargs)
        print(f"DEBUG: first scan page item count={len(response.get('Items', []))}")
        items.extend(response.get("Items", []))

        while response.get("LastEvaluatedKey"):
            response = table.scan(
                ExclusiveStartKey=response["LastEvaluatedKey"],
                Limit=50,
                **scan_kwargs,
            )
            print(f"DEBUG: next scan page item count={len(response.get('Items', []))}")
            items.extend(response.get("Items", []))
    except ClientError as e:
        print(f"DynamoDB scan failed in _scan_interactions_global: {e}")
        return []

    return items


def _stringify_items(items: List[Dict[str, Any]]) -> List[str]:
    """
    Turn a list of dict items into a list of JSON strings.
    Handles Decimal via default=str.
    """
    results: List[str] = []
    for item in items:
        results.append(_safe_json(item))
    return results


# --------- ACTION IMPLEMENTATIONS --------- #


def action_group_query_user_data(parameters: List[Dict[str, Any]]) -> List[str]:
    """
    Read user data by phone_number.
    """
    phone_number = _get_param(parameters, "phone_number")
    if not phone_number:
        return ["phone_number is required for QueryUserData"]

    if not USER_DATA_TABLE:
        return ["USER_DATA_TABLE environment variable is not set"]

    items = _get_user_data(USER_DATA_TABLE, phone_number)
    if not items:
        return [
            _safe_json(
                {
                    "user": None,
                    "phone_number": phone_number,
                    "message": "No matching user data found",
                }
            )
        ]

    user = items[0]
    return [_safe_json({"user": user})]


def action_group_update_user_name(parameters: List[Dict[str, Any]]) -> List[str]:
    """
    Update the Name field for a user identified by phone_number.
    """
    phone_number = _get_param(parameters, "phone_number")
    new_name = _get_param(parameters, "name")

    if not phone_number:
        return ["phone_number is required for UpdateUserName"]
    if not new_name:
        return ["name is required for UpdateUserName"]

    if not USER_DATA_TABLE:
        return ["USER_DATA_TABLE environment variable is not set"]

    table = dynamodb.Table(USER_DATA_TABLE)

    try:
        response = table.update_item(
            Key={"PhoneNumber": phone_number},
            UpdateExpression="SET #N = :name",
            ExpressionAttributeNames={"#N": "Name"},
            ExpressionAttributeValues={":name": new_name},
            ReturnValues="ALL_NEW",
        )
        updated = response.get("Attributes")
        if not updated:
            return [
                _safe_json(
                    {
                        "user": None,
                        "phone_number": phone_number,
                        "message": "User not found to update",
                    }
                )
            ]

        return [
            _safe_json(
                {
                    "user": updated,
                    "message": "Name updated successfully",
                }
            )
        ]
    except ClientError as e:
        print(f"DynamoDB update_item failed: {e}")
        return [
            _safe_json(
                {
                    "error": "Failed to update user name",
                    "details": str(e),
                    "phone_number": phone_number,
                }
            )
        ]


def action_group_query_interaction_history(
    parameters: List[Dict[str, Any]],
) -> List[str]:
    """
    Unified QueryInteractionHistory:

    Parameters:
    - to_number (string, required): business/WhatsApp destination number (PK)
    - from_number | phone_number (string, optional):
        If provided -> limit results to this customer.
        If omitted -> return all customers for the business.
    - start_date (string, optional): YYYY-MM-DD
    - end_date (string, optional): YYYY-MM-DD

    Returns:
    - JSON string with:
      { "interactions": [...], "phone_number": ..., "start_date": ..., "end_date": ... }
    """
    to_number = _get_param(parameters, "to_number") or _get_param(
        parameters, "business_number"
    )
    from_number = _get_param(parameters, "from_number") or _get_param(
        parameters, "phone_number"
    )
    start_date = _get_param(parameters, "start_date")
    end_date = _get_param(parameters, "end_date")

    if not INTERACTION_TABLE:
        return ["INTERACTION_TABLE environment variable is not set"]

    if not to_number:
        return ["to_number (business number) is required"]

    # Decide whether to query per-user or scan globally
    if to_number:
        items = _query_user_interactions(
            INTERACTION_TABLE, to_number, from_number, start_date, end_date
        )
    else:
        items = _scan_interactions_global(INTERACTION_TABLE, start_date, end_date)

    if not items:
        return [
                    _safe_json(
                        {
                            "interactions": [],
                            "message": "No matching interaction history found",
                            "phone_number": from_number,
                            "to_number": to_number,
                            "start_date": start_date,
                            "end_date": end_date,
                        }
                    )
        ]

    return [
        _safe_json(
            {
                "interactions": items,
                "phone_number": from_number,
                "to_number": to_number,
                "start_date": start_date,
                "end_date": end_date,
            }
        )
    ]


# --------- MAIN LAMBDA HANDLER --------- #


def lambda_handler(event, context):
    """
    Main handler for Bedrock Agents action group.

    Response schema:

    {
      "messageVersion": "1.0",
      "response": {
        "actionGroup": "<ActionGroupName>",
        "function": "<FunctionName>",
        "functionResponse": {
          "responseBody": {
            "TEXT": {
              "body": "<string>"
            }
          }
        }
      }
    }
    """
    print("Received event:", _safe_json(event))

    action_group = event.get("actionGroup")
    function_name = event.get("function")
    parameters = event.get("parameters", []) or []

    # Route based on function name first
    if function_name == "QueryUserData":
        results = action_group_query_user_data(parameters)
    elif function_name == "UpdateUserName":
        results = action_group_update_user_name(parameters)
    elif function_name == "QueryInteractionHistory":
        results = action_group_query_interaction_history(parameters)
    else:
        # Fallback by actionGroup if function is missing/blank
        if action_group == "QueryUserData":
            results = action_group_query_user_data(parameters)
        elif action_group == "QueryInteractionHistory":
            results = action_group_query_interaction_history(parameters)
        else:
            results = [
                f"Action Group or function <{action_group}:{function_name}> not supported."
            ]

    body_text = "\n".join(results)
    message_version = event.get("messageVersion") or "1.0"

    response_body = {"TEXT": {"body": body_text}}

    action_response = {
        "actionGroup": action_group,
        "function": function_name,
        "functionResponse": {"responseBody": response_body},
    }

    return {"messageVersion": message_version, "response": action_response}
