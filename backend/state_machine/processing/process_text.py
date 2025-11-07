import os
from typing import List, Optional

# Own imports
from ..base_step_function import BaseStepFunction
from common.enums import WhatsAppMessageTypes
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
from .customer_flow import ConversationFlow

from ..processing.bedrock_agent import call_bedrock_agent

DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE")
ENDPOINT_URL = os.environ.get("ENDPOINT_URL")
CONVERSATION_HISTORY_LIMIT = int(os.environ.get("CONVERSATION_HISTORY_LIMIT", "20"))

_history_helper = (
    DynamoDBHelper(table_name=DYNAMODB_TABLE, endpoint_url=ENDPOINT_URL)
    if DYNAMODB_TABLE
    else None
)

MAX_SESSION_ID_LENGTH = 256


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
    """Text processing logic for the State Machine."""

    def __init__(self, event):
        super().__init__(event, logger=logger)

    def process_text(self):
        """Validate the input message and produce a response."""
        self.logger.info("Starting process_text for the chatbot")

        record = self.event.get("input", {}).get("dynamodb", {}).get("NewImage", {})
        self.text = record.get("text", {}).get("S", "DEFAULT_RESPONSE")
        phone_number = record.get("from_number", {}).get("S", "unknown")

        conversation = ConversationFlow(phone_number)
        response_message = conversation.handle(self.text)

        self.logger.info(f"Generated response message: {response_message}")
        self.logger.info("Validation finished successfully")

        self.event["response_message"] = response_message
        return self.event
