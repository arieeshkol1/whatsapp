import json
import logging
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


dynamodb = boto3.resource("dynamodb")
TABLE_NAME = "aws-whatsapp-rules-dev"
table = dynamodb.Table(TABLE_NAME)


def build_success_response(action_group, function, message_version, payload):
    """
    Bedrock Agent response format:
    {
      "response": {
        "actionGroup": ...,
        "function": ...,
        "functionResponse": {
          "responseBody": {
            "TEXT": { "body": "<json string>" }
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


# ---------- Core operations ----------


def get_business_rules(business_id: str) -> dict:
    """
    Read CURRENT rules for a business from aws-whatsapp-rules-dev.

    PK: business_id (string, no +)
    SK: "CURRENT"
    rules_json: JSON string with the full rules object
    """
    resp = table.get_item(Key={"PK": business_id, "SK": "CURRENT"})
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
    Create or update CURRENT rules for a business.
    """
    now = datetime.now(timezone.utc).isoformat()
    table.put_item(
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


# ---------- Lambda entrypoint ----------


def lambda_handler(event, context):
    try:
        logger.info("Received event: %s", json.dumps(event, ensure_ascii=False))

        action_group = event.get("actionGroup", "unknown")
        function = event.get("function", "unknown")
        message_version = event.get("messageVersion", "1.0")

        raw_params = event.get("parameters", []) or []
        params = {p.get("name"): p.get("value") for p in raw_params if p.get("name")}

        # ------- GetBusinessRules -------
        if function == "GetBusinessRules":
            business_id = params.get("business_id")  # <<-- exact name
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

        # ------- UpsertBusinessRules -------
        elif function == "UpsertBusinessRules":
            business_id = params.get("business_id")  # <<-- exact name
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

            # rules_raw may be a JSON string or a dict
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

        # ------- Unsupported function -------
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
