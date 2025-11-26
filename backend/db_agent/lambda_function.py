import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# -------------------- LOGGING -------------------- #

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

# -------------------- DYNAMODB -------------------- #

dynamodb = boto3.resource("dynamodb")

# Existing rules table - use env overrides if provided (may or may not be used by this Lambda)
RULES_TABLE_NAME = os.getenv("RULES_TABLE_NAME") or os.getenv("RULES_TABLE")
RULES_TABLE_NAME = RULES_TABLE_NAME or "aws-whatsapp-rules-dev"
rules_table = dynamodb.Table(RULES_TABLE_NAME)

# UserData table (already exists in your account)
USERDATA_TABLE_NAME = os.getenv("USER_DATA_TABLE", "UserData")
USERDATA_TABLE = dynamodb.Table(USERDATA_TABLE_NAME)

# Interaction history table
HISTORY_TABLE_NAME = os.getenv("DYNAMODB_TABLE") or os.getenv("INTERACTION_TABLE")
HISTORY_TABLE_NAME = HISTORY_TABLE_NAME or "Interaction-history"
history_table = dynamodb.Table(HISTORY_TABLE_NAME)


# -------------------- HELPERS -------------------- #


def _normalize_ddb_types(obj):
    """
    Recursively convert DynamoDB/boto3 types (e.g. Decimal)
    to plain JSON-serializable Python types (int/float/str).
    """
    if isinstance(obj, list):
        return [_normalize_ddb_types(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _normalize_ddb_types(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        # Keep integers as int, non-integers as float
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj


def build_success_response(action_group, function, message_version, payload):
    """
    Bedrock Agent expected response format:
    {
      "response": {
        "actionGroup": ...,
        "function": ...,
        "functionResponse": {
          "responseBody": {
            "TEXT": {"body": "<json string>"}
          }
        }
      },
      "messageVersion": ...
    }
    """
    safe_payload = _normalize_ddb_types(payload)

    return {
        "response": {
            "actionGroup": action_group,
            "function": function,
            "functionResponse": {
                "responseBody": {
                    "TEXT": {"body": json.dumps(safe_payload, ensure_ascii=False)}
                }
            },
        },
        "messageVersion": message_version,
    }


def build_error_response(
    action_group, function, message_version, message, code="ERROR"
):
    payload = {
        "status": "error",
        "error_code": code,
        "message": message,
    }
    return build_success_response(action_group, function, message_version, payload)


# -------------------- BUSINESS RULES (OPTIONAL) -------------------- #


def get_business_rules(business_id: str) -> dict:
    """
    Read current rules for a business. This is optional in this Lambda,
    but useful if the DB Agent also exposes rules-related actions.
    """
    resp = rules_table.get_item(Key={"PK": business_id, "SK": "CURRENT"})
    item = resp.get("Item")
    if not item:
        raise KeyError(f"No rules found for business_id={business_id}")

    rules_json = item.get("rules_json")
    if not rules_json:
        raise KeyError(f"rules_json missing for business_id={business_id}")

    rules = json.loads(rules_json)
    return {
        "business_id": business_id,
        "version": item.get("version", "v1"),
        "rules": rules,
        "updated_at": item.get("updated_at"),
    }


def upsert_business_rules(business_id: str, version: str, rules: dict) -> dict:
    """
    Create or update the CURRENT rules for a business.
    """
    now = datetime.now(timezone.utc).isoformat()
    rules_table.put_item(
        Item={
            "PK": business_id,
            "SK": "CURRENT",
            "version": version,
            "rules_json": json.dumps(rules, ensure_ascii=False),
            "updated_at": now,
        }
    )
    return {
        "business_id": business_id,
        "version": version,
        "updated_at": now,
    }


# -------------------- USER DATA -------------------- #


def update_user_business_id(phone_number: str, business_id: str) -> dict:
    """
    Adds or updates the BusinessId field for the user in UserData table.
    Does NOT modify any other fields.
    """
    now = datetime.now(timezone.utc).isoformat()

    USERDATA_TABLE.update_item(
        Key={"PhoneNumber": phone_number},
        UpdateExpression="SET BusinessId = :b, updated_at = :u",
        ExpressionAttributeValues={":b": business_id, ":u": now},
    )

    return {
        "phone_number": phone_number,
        "business_id": business_id,
        "updated_at": now,
    }


# -------------------- INTERACTION HISTORY -------------------- #


def query_interaction_history(partition_key: str, sort_key_prefix: str) -> dict:
    """
    Query the Interaction-history table.

    Schema:
      PK = WhatsApp business "to" number  (business_id)
      SK = local-time ISO 8601 timestamp

    Called from Bedrock with:
      partition_key: used as PK (business_id / to_number)
      sort_key_prefix: date in 'YYYY-MM-DD' format (matched against SK prefix)
    """
    try:
        response = history_table.query(
            KeyConditionExpression=Key("PK").eq(partition_key)
            & Key("SK").begins_with(sort_key_prefix)
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        logger.exception("DynamoDB query failed: %s", code)
        if code == "ResourceNotFoundException":
            return {
                "status": "error",
                "error_code": "TABLE_NOT_FOUND",
                "message": f"History table '{HISTORY_TABLE_NAME}' not found",
            }
        return {
            "status": "error",
            "error_code": "DDB_QUERY_ERROR",
            "message": str(e),
        }

    items = response.get("Items", [])

    interactions = []
    for item in items:
        interactions.append(
            {
                "pk": item.get("PK"),
                "sk": item.get("SK"),
                "timestamp": item.get("timestamp") or item.get("SK"),
                "conversation_id": item.get("conversation_id"),
                "correlation_id": item.get("correlation_id"),
                "from_number": item.get("from_number"),
                "to_number": item.get("to_number") or item.get("PK"),
                "user_message": item.get("user_message") or item.get("text"),
                "system_response": item.get("system_response"),
                "raw_response": item.get("raw_response"),
                "type": item.get("type"),
            }
        )

    result = {
        "status": "ok",
        "table": HISTORY_TABLE_NAME,
        "partition_key": partition_key,
        "sort_key_prefix": sort_key_prefix,
        "count": len(interactions),
        "interactions": interactions,
    }

    # Normalize here too (harmless if already normalized)
    return _normalize_ddb_types(result)


# -------------------- LAMBDA HANDLER -------------------- #


def lambda_handler(event, context):
    """
    Entry point for the DB Agent Lambda.

    Expects events from Bedrock Agents in the format:
      {
        "actionGroup": "...",
        "function": "...",
        "messageVersion": "1.0",
        "parameters": [
          {"name": "...", "type": "string", "value": "..."},
          ...
        ]
      }
    """
    try:
        logger.info("Received event: %s", json.dumps(event, ensure_ascii=False))

        action_group = event.get("actionGroup", "unknown")
        function = event.get("function", "unknown")
        message_version = event.get("messageVersion", "1.0")

        # parameters: list of {name, type, value}
        raw_params = event.get("parameters", []) or []
        params = {p.get("name"): p.get("value") for p in raw_params if p.get("name")}

        # ---------------- BUSINESS RULES (OPTIONAL) ---------------- #

        if function == "GetBusinessRules":
            business_id = params.get("business_id")
            if not business_id:
                return build_error_response(
                    action_group,
                    function,
                    message_version,
                    "Missing 'business_id' parameter",
                    code="MISSING_PARAMETER",
                )

            try:
                result = get_business_rules(business_id)
                return build_success_response(
                    action_group, function, message_version, result
                )
            except KeyError as e:
                return build_error_response(
                    action_group,
                    function,
                    message_version,
                    str(e),
                    code="NOT_FOUND",
                )

        elif function == "UpsertBusinessRules":
            business_id = params.get("business_id")
            rules_raw = params.get("rules")
            version = params.get("version", "v1")

            if not business_id:
                return build_error_response(
                    action_group,
                    function,
                    message_version,
                    "Missing 'business_id' parameter",
                    code="MISSING_PARAMETER",
                )

            if rules_raw is None:
                return build_error_response(
                    action_group,
                    function,
                    message_version,
                    "Missing 'rules' parameter",
                    code="MISSING_PARAMETER",
                )

            # rules_raw may be JSON string or dict
            if isinstance(rules_raw, str):
                try:
                    rules = json.loads(rules_raw)
                except json.JSONDecodeError:
                    return build_error_response(
                        action_group,
                        function,
                        message_version,
                        "Invalid JSON in 'rules' parameter",
                        code="INVALID_RULES_JSON",
                    )
            else:
                rules = rules_raw

            result = upsert_business_rules(business_id, version, rules)
            return build_success_response(
                action_group, function, message_version, result
            )

        elif function == "UpdateUserBusinessId":
            phone = params.get("phone_number")
            business_id = params.get("business_id")

            if not phone:
                return build_error_response(
                    action_group,
                    function,
                    message_version,
                    "Missing 'phone_number' parameter",
                    code="MISSING_PARAMETER",
                )

            if not business_id:
                return build_error_response(
                    action_group,
                    function,
                    message_version,
                    "Missing 'business_id' parameter",
                    code="MISSING_PARAMETER",
                )

            result = update_user_business_id(phone, business_id)
            return build_success_response(
                action_group, function, message_version, result
            )

        # ---------------- INTERACTION HISTORY ---------------- #

        elif function == "QueryInteractionHistory":
            partition_key = params.get("partition_key")
            sort_key_prefix = params.get("sort_key_prefix")

            if not partition_key:
                return build_error_response(
                    action_group,
                    function,
                    message_version,
                    "Missing 'partition_key' parameter",
                    code="MISSING_PARAMETER",
                )

            if not sort_key_prefix:
                return build_error_response(
                    action_group,
                    function,
                    message_version,
                    "Missing 'sort_key_prefix' parameter",
                    code="MISSING_PARAMETER",
                )

            result = query_interaction_history(partition_key, sort_key_prefix)
            return build_success_response(
                action_group, function, message_version, result
            )

        # ---------------- UNSUPPORTED FUNCTION ---------------- #

        else:
            return build_error_response(
                action_group,
                function,
                message_version,
                f"Unsupported function: {function}",
                code="UNSUPPORTED_FUNCTION",
            )

    except Exception:
        logger.exception("Unexpected error")
        return build_error_response(
            event.get("actionGroup", "unknown"),
            event.get("function", "unknown"),
            event.get("messageVersion", "1.0"),
            "Internal error",
            code="INTERNAL_ERROR",
        )
