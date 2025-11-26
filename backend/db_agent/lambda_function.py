import json
import logging
import os
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

dynamodb = boto3.resource("dynamodb")

# -------------------- TABLES -------------------- #

# Existing rules table - use env overrides if provided
RULES_TABLE_NAME = os.getenv("RULES_TABLE_NAME") or os.getenv("RULES_TABLE")
RULES_TABLE_NAME = RULES_TABLE_NAME or "aws-whatsapp-rules-dev"
rules_table = dynamodb.Table(RULES_TABLE_NAME)

# UserData table (already exists in your account)
USERDATA_TABLE_NAME = os.getenv("USER_DATA_TABLE", "UserData")
USERDATA_TABLE = dynamodb.Table(USERDATA_TABLE_NAME)

# ✅ NEW: interaction history table (your real name)
HISTORY_TABLE_NAME = os.getenv("DYNAMODB_TABLE") or os.getenv("INTERACTION_TABLE")
HISTORY_TABLE_NAME = HISTORY_TABLE_NAME or "Interaction-history"
history_table = dynamodb.Table(HISTORY_TABLE_NAME)


# -------------------- HELPERS -------------------- #


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
    return {
        "response": {
            "actionGroup": action_group,
            "function": function,
            "functionResponse": {
                "responseBody": {
                    "TEXT": {"body": json.dumps(payload, ensure_ascii=False)}
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


# -------------------- BUSINESS RULES -------------------- #


def get_business_rules(business_id: str) -> dict:
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
    }


def upsert_business_rules(business_id: str, version: str, rules: dict) -> dict:
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

    Schema (updated):
      PK = WhatsApp business "to" number
      SK = local-time ISO 8601 timestamp

    We get from Bedrock:
      partition_key: used as PK
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
                "type": item.get("type"),
            }
        )

    return {
        "status": "ok",
        "table": HISTORY_TABLE_NAME,
        "partition_key": partition_key,
        "sort_key_prefix": sort_key_prefix,
        "count": len(interactions),
        "interactions": interactions,
    }


# -------------------- LAMBDA HANDLER -------------------- #


def lambda_handler(event, context):
    try:
        logger.info("Received event: %s", json.dumps(event, ensure_ascii=False))

        action_group = event.get("actionGroup", "unknown")
        function = event.get("function", "unknown")
        message_version = event.get("messageVersion", "1.0")

        # parameters: list of {name, type, value}
        raw_params = event.get("parameters", []) or []
        params = {p.get("name"): p.get("value") for p in raw_params if p.get("name")}

        # ---------------- EXISTING FUNCTIONS ---------------- #

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

        # ---------------- NEW: QueryInteractionHistory ---------------- #

        elif function == "QueryInteractionHistory":
            # Current action group schema (from traces):
            #   partition_key: string
            #   sort_key_prefix: string (YYYY-MM-DD)
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
