# Own imports
import os
from typing import List, Optional

from state_machine.base_step_function import BaseStepFunction
from common.logger import custom_logger
from common.helpers.dynamodb_helper import DynamoDBHelper
from common.customer_profiles import (
    format_customer_summary,
    load_customer_profile,
)

from state_machine.processing.bedrock_agent import call_bedrock_agent


logger = custom_logger()

DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE")
ENDPOINT_URL = os.environ.get("ENDPOINT_URL")
CONVERSATION_HISTORY_LIMIT = int(os.environ.get("CONVERSATION_HISTORY_LIMIT", "20"))

_history_helper = (
    DynamoDBHelper(table_name=DYNAMODB_TABLE, endpoint_url=ENDPOINT_URL)
    if DYNAMODB_TABLE
    else None
)


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

        history_items = _fetch_conversation_history(from_number, conversation_id)
        history_lines = _format_history_messages(history_items, current_whatsapp_id)

        customer_profile = load_customer_profile(from_number)
        customer_summary: Optional[str] = (
            format_customer_summary(customer_profile) if customer_profile else None
        )

        self.logger.info(
            "Prepared conversation context",
            extra={
                "conversation_id": conversation_id,
                "history_message_count": len(history_lines),
                "has_customer_profile": bool(customer_summary),
            },
        )
        from_number = self.event.get("from_number") or message_payload.get(
            "from_number", {}
        ).get("S")
        conversation_id = self.event.get("conversation_id", 1)
        current_whatsapp_id = self.event.get("whatsapp_id") or message_payload.get(
            "whatsapp_id", {}
        ).get("S", "")

        history_items = _fetch_conversation_history(from_number, conversation_id)
        history_lines = _format_history_messages(history_items, current_whatsapp_id)

        self.logger.info(
            "Prepared conversation context",
            extra={
                "conversation_id": conversation_id,
                "history_message_count": len(history_lines),
            },
        )

        if history_lines:
            history_block = "\n".join(history_lines)
            input_text = (
                "היסטוריית השיחה עבור הנושא הנוכחי:\n"
                f"{history_block}\n\nהודעת הלקוח כעת:\n{self.text}"
            )
        else:
            input_text = self.text

        context_sections: List[str] = []

        if customer_summary:
            context_sections.append(customer_summary)

        if history_lines:
            history_block = "\n".join(history_lines)
            context_sections.append(
                f"היסטוריית השיחה עבור הנושא הנוכחי:\n{history_block}"
            )

        context_sections.append(f"הודעת הלקוח כעת:\n{self.text}")

        input_text = "\n\n".join(context_sections)

        self.response_message = call_bedrock_agent(
            session_id=self.correlation_id,
            input_text=input_text,
        )

        self.logger.info(f"Generated response message: {self.response_message}")
        self.logger.info("Validation finished successfully")

        final_response = self.response_message
        if customer_summary:
            final_response = f"{self.response_message}\n\n{customer_summary}"

        self.event["response_message"] = final_response
        if customer_summary:
            self.event["customer_summary"] = customer_summary

        return self.event
