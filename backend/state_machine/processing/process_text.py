# Own imports
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from state_machine.base_step_function import BaseStepFunction
from common.logger import custom_logger
from common.helpers.dynamodb_helper import DynamoDBHelper
from common.customer_profiles import (
    format_customer_summary,
    load_customer_profile,
)
from common.conversation_state import (
    extract_state_updates_from_message,
    format_order_progress_summary,
    merge_conversation_state,
)
from common.rules_config import get_rules_text

from state_machine.processing.bedrock_agent import call_bedrock_agent


logger = custom_logger()

DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE")
ENDPOINT_URL = os.environ.get("ENDPOINT_URL")
CONVERSATION_HISTORY_LIMIT = int(os.environ.get("CONVERSATION_HISTORY_LIMIT", "20"))
CUSTOMERS_TABLE_NAME = os.environ.get("CUSTOMERS_TABLE_NAME")

_history_helper = (
    DynamoDBHelper(table_name=DYNAMODB_TABLE, endpoint_url=ENDPOINT_URL)
    if DYNAMODB_TABLE
    else None
)

_customers_table = None


MAX_SESSION_ID_LENGTH = 256


def _normalize_phone(number: Optional[str]) -> Optional[str]:
    if not number:
        return None
    stripped = str(number).strip()
    if not stripped:
        return None
    if stripped.startswith("+"):
        return stripped
    if stripped[0].isdigit():
        return f"+{stripped}"
    return stripped


def _get_customers_table():
    global _customers_table

    if _customers_table is not None:
        return _customers_table

    if not CUSTOMERS_TABLE_NAME:
        return None

    try:
        resource = boto3.resource("dynamodb", endpoint_url=ENDPOINT_URL)
        _customers_table = resource.Table(CUSTOMERS_TABLE_NAME)
    except Exception:  # pragma: no cover - defensive guard
        logger.exception("Failed to initialise Customers DynamoDB table handle")
        _customers_table = None

    return _customers_table


def _touch_customer_record(phone_number: Optional[str]) -> None:
    table = _get_customers_table()
    normalized = _normalize_phone(phone_number)
    if not table or not normalized:
        return

    try:
        table.update_item(
            Key={"PK": normalized},
            UpdateExpression="SET attributes = if_not_exists(attributes, :empty), last_seen_at = :now",
            ExpressionAttributeValues={
                ":empty": {},
                ":now": datetime.utcnow().isoformat(),
            },
        )
    except (ClientError, BotoCoreError):  # pragma: no cover - runtime protection
        logger.exception("Failed to ensure customer record exists")


def _extract_customer_details(state: Dict[str, Any]) -> Dict[str, str]:
    details: Dict[str, str] = {}
    candidates = {
        "first_name": ["customer_first_name", "first_name"],
        "last_name": ["customer_last_name", "last_name"],
        "email_address": ["customer_email", "email", "email_address"],
    }

    for canonical, keys in candidates.items():
        for key in keys:
            value = state.get(key)
            if value:
                details[canonical] = value
                break

    return details


def _update_customer_details(
    phone_number: Optional[str],
    state: Dict[str, Any],
) -> None:
    if not state:
        return

    table = _get_customers_table()
    normalized = _normalize_phone(phone_number)
    if not table or not normalized:
        return

    attributes = _extract_customer_details(state)
    if not attributes:
        return

    expression_names = {"#attr": "attributes"}
    expression_values: Dict[str, Any] = {
        ":empty": {},
        ":now": datetime.utcnow().isoformat(),
    }
    set_fragments = [
        "#attr = if_not_exists(#attr, :empty)",
        "last_seen_at = :now",
    ]

    for index, (key, value) in enumerate(attributes.items()):
        name_token = f"#field{index}"
        value_token = f":value{index}"
        expression_names[name_token] = key
        expression_values[value_token] = value
        set_fragments.append(f"#attr.{name_token} = {value_token}")

    update_expression = "SET " + ", ".join(set_fragments)

    try:
        table.update_item(
            Key={"PK": normalized},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_names,
            ExpressionAttributeValues=expression_values,
        )
    except (ClientError, BotoCoreError):  # pragma: no cover - runtime protection
        logger.exception("Failed to update customer details")


def _build_session_id(
    from_number: Optional[str], conversation_id: Optional[int], fallback: str
) -> str:
    """Construct a stable session identifier for the Bedrock agent."""

    components: List[str] = []
    if from_number:
        components.append(str(from_number).strip())
    if conversation_id and conversation_id > 0:
        components.append(str(conversation_id))

    session_identifier = "-".join(filter(None, components))

    if "|" in session_identifier:
        session_identifier = session_identifier.replace("|", "-")
    if not session_identifier:
        session_identifier = fallback

    return session_identifier[:MAX_SESSION_ID_LENGTH]


def _fetch_conversation_history(from_number: str, conversation_id: int) -> List[dict]:
    if not _history_helper or not from_number or conversation_id < 1:
        return []

    partition_key = f"NUMBER#{from_number}"
    try:
        return _history_helper.query_by_conversation(
            partition_key=partition_key,
            conversation_id=conversation_id,
            limit=CONVERSATION_HISTORY_LIMIT,
        )
    except Exception:  # pragma: no cover - defensive logging
        logger.exception("Failed to fetch conversation history")
        return []


def _format_history_messages(items: List[dict], current_whatsapp_id: str) -> List[str]:
    history_lines: List[str] = []
    if not items:
        return history_lines

    sorted_items = sorted(items, key=lambda item: item.get("SK", ""))
    for item in sorted_items:
        text = item.get("text")
        whatsapp_id = item.get("whatsapp_id")
        if not text or whatsapp_id == current_whatsapp_id:
            continue
        created_at = item.get("created_at")
        history_lines.append(
            f"[{created_at}] לקוח: {text}" if created_at else f"לקוח: {text}"
        )

    return history_lines


class ProcessText(BaseStepFunction):
    """
    This class contains methods that serve as the "text processing" for the State Machine.
    """

    def __init__(self, event):
        super().__init__(event, logger=logger)

    def process_text(self):
        """
        Method to validate the input message and process the expected text response.
        """

        self.logger.info("Starting process_text for the chatbot")

        message_payload = (
            self.event.get("input", {}).get("dynamodb", {}).get("NewImage", {})
        )
        self.text = self.event.get("text") or message_payload.get("text", {}).get(
            "S", "DEFAULT_RESPONSE"
        )
        from_number = self.event.get("from_number") or message_payload.get(
            "from_number", {}
        ).get("S")
        conversation_id = self.event.get("conversation_id", 1)
        current_whatsapp_id = self.event.get("whatsapp_id") or message_payload.get(
            "whatsapp_id", {}
        ).get("S", "")

        _touch_customer_record(from_number)

        history_items = _fetch_conversation_history(from_number, conversation_id)
        history_lines = _format_history_messages(history_items, current_whatsapp_id)

        customer_profile = load_customer_profile(from_number)
        customer_summary: Optional[str] = (
            format_customer_summary(customer_profile) if customer_profile else None
        )

        partition_key = f"NUMBER#{from_number}" if from_number else None
        conversation_state: Dict[str, Any] = {}
        order_progress_summary: Optional[str] = None

        if partition_key and _history_helper:
            try:
                stored_state = _history_helper.get_conversation_state(
                    partition_key, conversation_id
                )
                if isinstance(stored_state, dict):
                    conversation_state = stored_state
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("Failed to fetch conversation state")

        inferred_updates = extract_state_updates_from_message(self.text)
        provided_updates = (
            self.event.get("conversation_state_updates")
            if isinstance(self.event.get("conversation_state_updates"), dict)
            else {}
        )

        combined_updates: Dict[str, Any] = {}
        combined_updates.update(inferred_updates)
        combined_updates.update(provided_updates)

        if combined_updates:
            conversation_state = merge_conversation_state(
                conversation_state, combined_updates
            )
            if partition_key and _history_helper:
                try:
                    _history_helper.put_conversation_state(
                        partition_key, conversation_id, conversation_state
                    )
                except Exception:  # pragma: no cover - defensive logging
                    logger.exception("Failed to persist conversation state")

        order_progress_summary = format_order_progress_summary(conversation_state)

        _update_customer_details(from_number, conversation_state)

        self.logger.info(
            "Prepared conversation context",
            extra={
                "conversation_id": conversation_id,
                "history_message_count": len(history_lines),
                "has_customer_profile": bool(customer_summary),
                "has_conversation_state": bool(conversation_state),
            },
        )

        context_sections: List[str] = []

        rules_text = get_rules_text()
        if rules_text:
            context_sections.append(rules_text)

        if customer_summary:
            context_sections.append(customer_summary)

        if order_progress_summary:
            context_sections.append(order_progress_summary)

        if history_lines:
            history_block = "\n".join(history_lines)
            context_sections.append(
                f"היסטוריית השיחה עבור הנושא הנוכחי:\n{history_block}"
            )

        context_sections.append(f"הודעת הלקוח כעת:\n{self.text}")

        input_text = "\n\n".join(context_sections)

        session_identifier = _build_session_id(
            from_number=from_number,
            conversation_id=conversation_id,
            fallback=self.correlation_id,
        )

        self.response_message = call_bedrock_agent(
            session_id=session_identifier,
            input_text=input_text,
        )

        self.logger.info(f"Generated response message: {self.response_message}")
        self.logger.info("Validation finished successfully")

        final_response = self.response_message
        for section in [customer_summary, order_progress_summary]:
            if section:
                if section not in final_response:
                    final_response = f"{final_response}\n\n{section}"

        self.event["response_message"] = final_response
        if customer_summary:
            self.event["customer_summary"] = customer_summary
        if order_progress_summary:
            self.event["order_progress_summary"] = order_progress_summary
        if conversation_state:
            self.event["conversation_state"] = conversation_state

        return self.event
