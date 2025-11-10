import json
import os
import time
from datetime import datetime
from decimal import Decimal
from html import unescape
from typing import Any, Dict, List, Optional, Tuple

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

MAX_SESSION_ID_LENGTH = 256

__all__ = ["ProcessText"]

logger = custom_logger()

DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE")
ENDPOINT_URL = os.environ.get("ENDPOINT_URL")
CONVERSATION_HISTORY_LIMIT = int(os.environ.get("CONVERSATION_HISTORY_LIMIT", "20"))
USER_INFO_TABLE_NAME = os.environ.get("USER_INFO_TABLE")

_history_helper = (
    DynamoDBHelper(table_name=DYNAMODB_TABLE, endpoint_url=ENDPOINT_URL)
    if DYNAMODB_TABLE
    else None
)

_users_info_table = None


def _extract_customer_details(event: Dict[str, Any]) -> Dict[str, Any]:
    """Return customer details payload provided by the Assess Changes step."""

    customer_info = event.get("customer_info")
    if not isinstance(customer_info, dict):
        return {}

    raw_details = customer_info.get("details") or customer_info.get("user_info_details")

    return raw_details if isinstance(raw_details, dict) else {}

SENSITIVE_CONVERSATION_STATE_KEYS = {
    "customer_first_name",
    "customer_last_name",
    "customer_email",
    "customer_name",
    "email",
    "email_address",
    "first_name",
    "last_name",
    "full_name",
    "company",
    "company_name",
    # note: 'date_of_event' is intentionally NOT excluded — tests expect it to persist
}


def _as_epoch_decimal(raw: Optional[Any]) -> Decimal:
    if raw is None:
        return Decimal(str(int(time.time())))

    if isinstance(raw, (int, float)):
        return Decimal(str(int(raw)))

    try:
        trimmed = str(raw).strip()
        if not trimmed:
            raise ValueError
        try:
            return Decimal(str(int(trimmed)))
        except ValueError:
            return Decimal(str(int(float(trimmed))))
    except (ValueError, TypeError):
        return Decimal(str(int(time.time())))


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


def _get_users_info_table():
    global _users_info_table

    if _users_info_table is not None:
        return _users_info_table

    if not USER_INFO_TABLE_NAME:
        return None

    try:
        resource = boto3.resource("dynamodb", endpoint_url=ENDPOINT_URL)
        _users_info_table = resource.Table(USER_INFO_TABLE_NAME)
    except Exception:  # pragma: no cover - defensive guard
        logger.exception("Failed to initialise UsersInfo table handle")
        _users_info_table = None

    return _users_info_table


def _load_user_info_details(phone_number: Optional[str]) -> Dict[str, Any]:
    table = _get_users_info_table()
    normalized = _normalize_phone(phone_number)
    if not table or not normalized:
        return {}

    try:
        response = table.get_item(Key={"PhoneNumber": normalized})
    except (ClientError, BotoCoreError):  # pragma: no cover - runtime protection
        logger.exception("Failed to load UsersInfo record")
        return {}

    item = response.get("Item") if isinstance(response, dict) else None
    if not isinstance(item, dict):
        return {}

    name = item.get("Name")
    if isinstance(name, str):
        stripped = name.strip()
        if stripped:
            return {"name": stripped}

    return {}


def _normalise_user_update_entries(raw_updates: Any) -> List[Dict[str, Any]]:
    if raw_updates is None:
        return []

    entries: List[Dict[str, Any]] = []

    def _coerce_entry(tag: Optional[Any], value: Any):
        if tag is None:
            return
        if value in (None, "", []):
            return
        entries.append({"tag": str(tag), "value": value})

    if isinstance(raw_updates, dict):
        for key, value in raw_updates.items():
            _coerce_entry(key, value)
        return entries

    if isinstance(raw_updates, list):
        for item in raw_updates:
            if isinstance(item, dict):
                tag = (
                    item.get("tag")
                    or item.get("path")
                    or item.get("field")
                    or item.get("name")
                )
                value = item.get("value")
                if value in (None, "", []):
                    value = item.get("text")
                if tag is None and len(item) == 1:
                    [(tag, value)] = list(item.items())
                _coerce_entry(tag, value)
            elif isinstance(item, tuple) and len(item) == 2:
                tag, value = item
                _coerce_entry(tag, value)
        return entries

    return entries


def _partition_user_update_entries(
    entries: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    profile_updates: Dict[str, Any] = {}
    conversation_updates: Dict[str, Any] = {}
    passthrough: List[Dict[str, Any]] = []

    for entry in entries:
        tag = entry.get("tag")
        value = entry.get("value")
        if tag is None:
            continue

        normalized_tag = str(tag).strip()
        if not normalized_tag or value in (None, "", []):
            continue

        passthrough.append({"tag": normalized_tag, "value": value})

        if normalized_tag.startswith("conversation."):
            key_path = normalized_tag[len("conversation.") :]
            if key_path:
                conversation_updates[key_path] = value
        elif normalized_tag.startswith("profile."):
            key_path = normalized_tag[len("profile.") :]
            if key_path:
                profile_updates[key_path] = value
        else:
            profile_updates[normalized_tag] = value

    return profile_updates, conversation_updates, passthrough


def _conversation_state_updates_from_tags(
    tagged_updates: Dict[str, Any],
) -> Dict[str, Any]:
    if not tagged_updates:
        return {}

    updates = tagged_updates
    state_updates: Dict[str, Any] = {}

    first_name = updates.get("first_name")
    last_name = updates.get("last_name")
    full_name = updates.get("full_name")
    email = updates.get("email")
    company = updates.get("company")
    date_of_event = updates.get("date_of_event")
    event_address = updates.get("event_address")
    guest_count = updates.get("guest_count")

    if first_name:
        state_updates["customer_first_name"] = first_name
    if last_name:
        state_updates["customer_last_name"] = last_name
    if first_name or last_name:
        full_name_candidate = " ".join(filter(None, [first_name, last_name])).strip()
        if full_name_candidate:
            state_updates["customer_name"] = full_name_candidate
    elif full_name:
        state_updates["customer_name"] = str(full_name).strip()
    if email:
        state_updates["customer_email"] = email
    if company:
        state_updates["company_name"] = company
    if date_of_event:
        state_updates["date_of_event"] = date_of_event
    if event_address:
        state_updates["event_address"] = event_address
    if guest_count not in (None, ""):
        try:
            state_updates["guest_count"] = int(guest_count)
        except (ValueError, TypeError):
            state_updates["guest_count"] = guest_count

    return state_updates


def _sanitize_conversation_state(state: Dict[str, Any]) -> Dict[str, Any]:
    if not state:
        return {}
    return {
        key: value
        for key, value in state.items()
        if key not in SENSITIVE_CONVERSATION_STATE_KEYS
    }


def _format_user_info_for_context(profile: Dict[str, Any]) -> Optional[str]:
    if not profile:
        return None

    visible_items: List[str] = []
    for key, value in profile.items():
        if value in (None, "", []):
            continue
        visible_items.append(f"{key}: {value}")

    if not visible_items:
        return None

    joined = ", ".join(visible_items)
    return f"פרטי משתמש ידועים:\n{joined}"


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
        last_seen_at = self.event.get("last_seen_at")
        payload_last_seen = message_payload.get("last_seen_at")
        if not last_seen_at:
            if isinstance(payload_last_seen, dict):
                last_seen_at = payload_last_seen.get("S") or payload_last_seen.get("N")
            elif payload_last_seen:
                last_seen_at = payload_last_seen
        if not last_seen_at:
            last_seen_at = self.event.get("raw_event", {}).get("last_seen_at")

        assess_customer_details = _extract_customer_details(self.event)

        history_items = _fetch_conversation_history(from_number, conversation_id)
        history_lines = _format_history_messages(history_items, current_whatsapp_id)

        customer_profile = load_customer_profile(from_number)
        customer_summary: Optional[str] = (
            format_customer_summary(customer_profile) if customer_profile else None
        )

        stored_user_info = _load_user_info_details(from_number)
        combined_customer_info: Dict[str, Any] = {}
        if isinstance(stored_user_info, dict):
            combined_customer_info.update(stored_user_info)
        if assess_customer_details:
            combined_customer_info.update(assess_customer_details)
        if from_number:
            combined_customer_info.setdefault("phone_number", from_number)
        user_info_context = _format_user_info_for_context(combined_customer_info)

        partition_key = f"NUMBER#{from_number}" if from_number else None
        conversation_state: Dict[str, Any] = {}
        prompt_state: Dict[str, Any] = {}
        conversation_state_dirty = False

        if partition_key and _history_helper:
            try:
                stored_state = _history_helper.get_conversation_state(
                    partition_key, conversation_id
                )
                if isinstance(stored_state, dict):
                    conversation_state = stored_state
                    prompt_state = dict(stored_state)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("Failed to fetch conversation state")

        if combined_customer_info:
            for key, value in _conversation_state_updates_from_tags(
                combined_customer_info
            ).items():
                prompt_state.setdefault(key, value)

        sanitized_state = _sanitize_conversation_state(prompt_state)
        if sanitized_state != conversation_state:
            conversation_state = sanitized_state
            conversation_state_dirty = True

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
            prompt_state = merge_conversation_state(prompt_state, combined_updates)
            sanitized_state = _sanitize_conversation_state(prompt_state)
            if sanitized_state != conversation_state:
                conversation_state = sanitized_state
                conversation_state_dirty = True

        order_progress_summary_for_prompt = format_order_progress_summary(prompt_state)

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

        if order_progress_summary_for_prompt:
            context_sections.append(order_progress_summary_for_prompt)

        if user_info_context:
            context_sections.append(user_info_context)

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

        raw_response = call_bedrock_agent(
            session_id=session_identifier,
            input_text=input_text,
        )

        reply_text = raw_response or ""
        profile_updates: Dict[str, Any] = {}
        conversation_tag_updates: Dict[str, Any] = {}
        user_update_entries: List[Dict[str, Any]] = []

        if raw_response:
            try:
                parsed = json.loads(raw_response)
            except json.JSONDecodeError:
                self.logger.warning(
                    "Bedrock response was not valid JSON",
                    extra={"session_id": session_identifier},
                )
            else:
                if isinstance(parsed, dict):
                    candidate_reply = parsed.get("reply")
                    if isinstance(candidate_reply, str):
                        reply_text = candidate_reply
                    else:
                        self.logger.warning(
                            "Bedrock response missing string reply",
                            extra={"parsed_keys": list(parsed.keys())},
                        )

                    updates_candidate = parsed.get("user_updates")
                    entries = _normalise_user_update_entries(updates_candidate)
                    (
                        profile_updates,
                        conversation_tag_updates,
                        user_update_entries,
                    ) = _partition_user_update_entries(entries)
                else:
                    self.logger.warning(
                        "Bedrock response JSON was not an object",
                        extra={"type": type(parsed).__name__},
                    )

        self.response_message = unescape(reply_text)

        if profile_updates or conversation_tag_updates:
            agent_state_updates = _conversation_state_updates_from_tags(
                {**profile_updates, **conversation_tag_updates}
            )
            if agent_state_updates:
                prompt_state = merge_conversation_state(
                    prompt_state, agent_state_updates
                )
                sanitized_state = _sanitize_conversation_state(prompt_state)
                if sanitized_state != conversation_state:
                    conversation_state = sanitized_state
                    conversation_state_dirty = True

        final_order_progress_summary = format_order_progress_summary(prompt_state)

        self.logger.info(f"Generated response message: {self.response_message}")
        self.logger.info("Validation finished successfully")

        final_response = self.response_message
        for section in [customer_summary, final_order_progress_summary]:
            if section:
                if section not in final_response:
                    final_response = f"{final_response}\n\n{section}"

        self.event["response_message"] = final_response
        if customer_summary:
            self.event["customer_summary"] = customer_summary
        if final_order_progress_summary:
            self.event["order_progress_summary"] = final_order_progress_summary
        self.event["conversation_state"] = conversation_state
        if user_update_entries:
            self.event["user_updates"] = user_update_entries

        return self.event
