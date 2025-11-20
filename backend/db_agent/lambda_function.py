import json
import os
import textwrap
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

BUSINESS_RULES_BUCKET = os.environ.get("BUSINESS_RULES_BUCKET")
BUSINESS_RULES_PREFIX = os.environ.get("BUSINESS_RULES_PREFIX", "business-rules/")
DB_KNOWLEDGE_BASE_ID = os.environ.get("DB_KNOWLEDGE_BASE_ID")
DB_KB_DATA_SOURCE_ID = os.environ.get("DB_KB_DATA_SOURCE_ID")

USER_DATA_TABLE = os.environ.get("USER_DATA_TABLE")
INTERACTION_TABLE = os.environ.get("INTERACTION_TABLE")

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
bedrock_agent = boto3.client("bedrock-agent")


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


def _build_business_rules_document(phone_number: str, rules_text: str) -> str:
    templated_instructions = textwrap.dedent(
        """
        ## הנחיות לבעל העסק
        1) פרטי המוצרים או השירותים של העסק.
        2) אילו נתוני לקוח נדרשים לכל שירות (לדוגמה: שם, טלפון, עמדת שירות, תאריך ביקור).
        3) תיאור מפורט של תהליך העבודה עבור כל שירות, כולל שלבי טיפול והסלמה.

        שמרו על טון כתיבה ברור ובר השבה ללקוח. השתמשו בשפה העברית בלבד.
        """
    ).strip()

    trimmed_rules = rules_text.strip()

    return "\n".join(
        [
            f"# Business Rules for {phone_number}",
            templated_instructions,
            "",
            "## הנחיות שנמסרו על ידי בעל העסק",
            trimmed_rules,
        ]
    )


def _write_rules_to_s3(phone_number: str, rules_text: str) -> str:
    if not BUSINESS_RULES_BUCKET:
        return "Business rules bucket is not configured"

    body = _build_business_rules_document(phone_number, rules_text)
    key = f"{BUSINESS_RULES_PREFIX}{phone_number}.md"

    try:
        s3.put_object(Bucket=BUSINESS_RULES_BUCKET, Key=key, Body=body.encode("utf-8"))
    except ClientError:
        return "Failed to persist business rules to storage"

    return key


def _start_ingestion_job_if_configured() -> Optional[str]:
    if not DB_KNOWLEDGE_BASE_ID or not DB_KB_DATA_SOURCE_ID:
        return None

    try:
        job = bedrock_agent.start_ingestion_job(
            knowledgeBaseId=DB_KNOWLEDGE_BASE_ID,
            dataSourceId=DB_KB_DATA_SOURCE_ID,
        )
    except ClientError:
        return None

    return job.get("ingestionJob", {}).get("ingestionJobId")


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


def action_group_update_business_rules(parameters: List[Dict[str, Any]]) -> List[str]:
    phone_number = _get_param(parameters, "business_phone_number")
    rules_text = _get_param(parameters, "rules_markdown")

    if not phone_number:
        return ["business_phone_number is required for UpdateBusinessRules"]
    if not rules_text:
        return ["rules_markdown is required for UpdateBusinessRules"]

    key_or_error = _write_rules_to_s3(phone_number, rules_text)
    if key_or_error.startswith("Failed") or key_or_error.startswith("Business"):
        return [key_or_error]

    ingestion_job_id = _start_ingestion_job_if_configured()
    confirmation_parts = [
        f"Persisted business rules for {phone_number} at {key_or_error}",
    ]
    if ingestion_job_id:
        confirmation_parts.append(f"Triggered ingestion job {ingestion_job_id}")

    return confirmation_parts


def lambda_handler(event, context):
    action_group = event.get("actionGroup")
    _function = event.get("function")
    parameters = event.get("parameters", [])

    if action_group == "QueryUserData":
        results = action_group_query_user_data(parameters)
    elif action_group == "QueryInteractionHistory":
        results = action_group_query_interaction_history(parameters)
    elif action_group == "UpdateBusinessRules":
        results = action_group_update_business_rules(parameters)
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
