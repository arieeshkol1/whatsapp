import os
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# =====================================================================
# CONSTANTS
# =====================================================================

TABLE_NAME = os.environ.get("DYNAMODB_TABLE") or os.environ.get("TABLE_NAME")
SYSTEM_RESPONSE_ATTRIBUTE = "system_response"  # canonical unified field
RAW_BEDROCK_ATTRIBUTE = "bedrock_response"  # optional legacy (kept same)

dynamodb_resource = boto3.resource("dynamodb")
table = dynamodb_resource.Table(TABLE_NAME)


# =====================================================================
# QUERY: Fetch by PK + SK begins_with
# =====================================================================


def query_dynamodb_pk_sk(
    partition_key: str, sort_key_prefix: str
) -> List[Dict[str, Any]]:
    """
    Query DynamoDB items by PK and SK beginning with SK prefix.
    """
    print(
        f"[DDB] query_dynamodb_pk_sk: PK={partition_key}, "
        f"SK begins_with={sort_key_prefix}"
    )

    all_items: List[Dict[str, Any]] = []
    try:
        condition = Key("PK").eq(partition_key) & Key("SK").begins_with(sort_key_prefix)

        response = table.query(
            KeyConditionExpression=condition,
            Limit=50,
        )

        all_items.extend(response.get("Items", []))

        while "LastEvaluatedKey" in response:
            response = table.query(
                KeyConditionExpression=condition,
                Limit=50,
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            all_items.extend(response.get("Items", []))

        return all_items

    except ClientError as exc:
        print(f"[DDB] ERROR in query_dynamodb_pk_sk: {exc}")
        raise


# =====================================================================
# HELPERS: History State
# =====================================================================


def query_by_conversation(
    partition_key: str, conversation_id: int, limit: int = 50
) -> List[Dict[str, Any]]:
    """
    Query messages for a conversation ID using the new Interactions_History schema.

    Schema:
      PK = ToNumber (WhatsApp business number)
      SK = ISO 8601 timestamp (starts with YYYY-MM-DD)
    """
    try:
        condition = Key("PK").eq(partition_key)

        response = table.query(
            KeyConditionExpression=condition,
            Limit=limit,
            FilterExpression="conversation_id = :c",
            ExpressionAttributeValues={":c": conversation_id},
        )
        items = response.get("Items", [])

        while "LastEvaluatedKey" in response:
            response = table.query(
                KeyConditionExpression=condition,
                Limit=limit,
                FilterExpression="conversation_id = :c",
                ExpressionAttributeValues={":c": conversation_id},
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            items.extend(response.get("Items", []))

        return items

    except ClientError as exc:
        print(f"[DDB] ERROR: query_by_conversation failed: {exc}")
        return []


def get_conversation_state(
    partition_key: str, conversation_id: int
) -> Optional[Dict[str, Any]]:
    """
    Fetch STATE#<conversation_id> record.
    """
    sk = f"STATE#{conversation_id}"
    try:
        response = table.get_item(Key={"PK": partition_key, "SK": sk})
        return response.get("Item")
    except ClientError:
        return None


def put_conversation_state(
    partition_key: str, conversation_id: int, state: Dict[str, Any]
) -> None:
    """
    Store or overwrite conversation state.
    """
    sk = f"STATE#{conversation_id}"
    try:
        table.put_item(
            Item={
                "PK": partition_key,
                "SK": sk,
                "conversation_id": conversation_id,
                **state,
            }
        )
    except ClientError as exc:
        print(f"[DDB] ERROR: put_conversation_state failed: {exc}")
        raise


# =====================================================================
# UPDATE: Attach system_response to a specific WhatsApp message
# =====================================================================


def update_system_response(
    partition_keys: List[str],
    whatsapp_id: str,
    system_response: Dict[str, Any],
    legacy_raw_same_json: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Update the message item identified by whatsapp_id, under ANY valid PK.

    Writes ONLY ONE canonical attribute:
        system_response = { ... JSON ... }

    Also writes legacy field "bedrock_response" (same JSON) if provided.
    """
    if not partition_keys:
        print("[DDB] update_system_response: No partition keys provided")
        return

    for pk in partition_keys:
        try:
            # Query message item by PK + filter_expression for whatsapp_id
            result = table.query(
                KeyConditionExpression=Key("PK").eq(pk),
                FilterExpression="whatsapp_id = :w",
                ExpressionAttributeValues={":w": whatsapp_id},
                Limit=1,
            )

            items = result.get("Items", [])
            if not items:
                continue  # Try next PK

            item = items[0]
            sk = item["SK"]

            # Build UpdateExpression
            update_expr = "SET #sys = :s"
            expr_names = {"#sys": SYSTEM_RESPONSE_ATTRIBUTE}
            expr_values = {":s": system_response}

            if legacy_raw_same_json is not None:
                update_expr += ", #legacy = :l"
                expr_names["#legacy"] = RAW_BEDROCK_ATTRIBUTE
                expr_values[":l"] = legacy_raw_same_json

            table.update_item(
                Key={"PK": pk, "SK": sk},
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_names,
                ExpressionAttributeValues=expr_values,
            )

            print(f"[DDB] system_response updated for PK={pk}, SK={sk}")
            return

        except ClientError as exc:
            print(f"[DDB] ERROR updating system_response for PK={pk}: {exc}")

    print("[DDB] update_system_response: Message not found under any PK")
