# Own imports
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


MAX_SESSION_ID_LENGTH = 256


USER_INFO_TABLE_NAME = os.environ.get("USER_INFO_TABLE")

_users_info_table = None


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


USER_INFO_ATTRIBUTE = "UserInfo"
COLLECTED_FIELDS_ATTRIBUTE = "CollectedFields"


def _load_user_info_profile(phone_number: Optional[str]) -> Dict[str, Any]:
    table = _get_users_info_table()
    normalized = _normalize_phone(phone_number)
    if not table or not normalized:
        return {}

    try:
        response = table.get_item(Key={"PhoneNumber": normalized})
    except (ClientError, BotoCoreError):  # pragma: no cover - runtime protection
        logger.exception("Failed to load user info profile")
        return {}

    item = response.get("Item") if isinstance(response, dict) else None
    if not isinstance(item, dict):
        return {}

    profile = item.get(USER_INFO_ATTRIBUTE)
    if isinstance(profile, dict):
        return profile

    return {}


def _touch_user_info_record(
    phone_number: Optional[str], last_seen_at: Optional[Any]
) -> None:
    table = _get_users_info_table()
    normalized = _normalize_phone(phone_number)
    if not table or not normalized:
        return

    try:
        table.update_item(
            Key={"PhoneNumber": normalized},
            UpdateExpression=(
                "SET #info = if_not_exists(#info, :empty), "
                "#collected = if_not_exists(#collected, :empty), "
                "updated_at = :updated_at, last_seen_at = :last_seen"
            ),
            ExpressionAttributeNames={
                "#info": USER_INFO_ATTRIBUTE,
                "#collected": COLLECTED_FIELDS_ATTRIBUTE,
            },
            ExpressionAttributeValues={
                ":empty": {},
                ":updated_at": datetime.utcnow().isoformat(),
                ":last_seen": _as_epoch_decimal(last_seen_at),
            },
        )
    except (ClientError, BotoCoreError):  # pragma: no cover - runtime protection
        logger.exception("Failed to touch UsersInfo record")


def _update_user_info_profile(
    phone_number: Optional[str],
    updates: Dict[str, Any],
    last_seen_at: Optional[Any],
) -> None:
    if not updates:
        return

    table = _get_users_info_table()
    normalized = _normalize_phone(phone_number)
    if not table or not normalized:
        return

    cleaned_updates = {
        key: value for key, value in updates.items() if value not in (None, "", [])
    }

    if not cleaned_updates:
        return

    expression_names = {
        "#info": USER_INFO_ATTRIBUTE,
        "#collected": COLLECTED_FIELDS_ATTRIBUTE,
    }
    expression_values: Dict[str, Any] = {
        ":empty": {},
        ":true": True,
        ":updated_at": datetime.utcnow().isoformat(),
        ":last_seen": _as_epoch_decimal(last_seen_at),
    }
    set_fragments = [
        "#info = if_not_exists(#info, :empty)",
        "#collected = if_not_exists(#collected, :empty)",
        "updated_at = :updated_at",
        "last_seen_at = :last_seen",
    ]

    for index, (path, value) in enumerate(cleaned_updates.items()):
        segments = [segment for segment in str(path).split(".") if segment]
        if not segments:
            continue

        value_token = f":value{index}"
        expression_values[value_token] = value

        profile_tokens: List[str] = []
        for segment_index, segment in enumerate(segments):
            token = f"#field{index}_{segment_index}"
            expression_names[token] = segment
            profile_tokens.append(token)

        profile_path = ".".join(profile_tokens)
        set_fragments.append(f"#info.{profile_path} = {value_token}")
        set_fragments.append(f"#collected.{profile_path} = :true")

    update_expression = "SET " + ", ".join(set_fragments)

    try:
        table.update_item(
            Key={"PhoneNumber": normalized},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_names,
            ExpressionAttributeValues=expression_values,
        )
    except (ClientError, BotoCoreError):  # pragma: no cover - runtime protection
        logger.exception("Failed to update UsersInfo profile")


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
    entries: List[Dict[str, Any]]
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
    tagged_updates: Dict[str, Any]
) -> Dict[str, Any]:
    if not tagged_updates:
        return {}

    state_updates: Dict[str, Any] = {}
    for key_path, value in tagged_updates.items():
        if value in (None, "", []):
            continue
        normalized_key = key_path.replace(" ", "_")
        normalized_key = normalized_key.replace(".", "_")
        state_updates[normalized_key] = value

    return state_updates


def _format_user_info_for_context(profile: Dict[str, Any]) -> Optional[str]:
    if not profile:
        return None

    visible_items = []
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

        _touch_user_info_record(
            from_number, last_seen_at=last_seen_at or datetime.utcnow()
        )

        history_items = _fetch_conversation_history(from_number, conversation_id)
        history_lines = _format_history_messages(history_items, current_whatsapp_id)

        customer_profile = load_customer_profile(from_number)
        customer_summary: Optional[str] = (
            format_customer_summary(customer_profile) if customer_profile else None
        )

        stored_user_profile = _load_user_info_profile(from_number)
        user_profile_context = _format_user_info_for_context(stored_user_profile)

        partition_key = f"NUMBER#{from_number}" if from_number else None
        conversation_state: Dict[str, Any] = {}
        conversation_state_dirty = False

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
            conversation_state_dirty = True

        order_progress_summary_for_prompt = format_order_progress_summary(
            conversation_state
        )

        _update_user_info_details(from_number, conversation_state, last_seen_at)

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

        if history_lines:
            history_block = "\n".join(history_lines)
            context_sections.append(
                f"היסטוריית השיחה עבור הנושא הנוכחי:\n{history_block}"
            )

        if user_profile_context:
            context_sections.append(user_profile_context)

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
        user_update_entries: List[Dict[str, Any]] = []
        profile_updates: Dict[str, Any] = {}
        conversation_tag_updates: Dict[str, Any] = {}

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

        if profile_updates:
            _update_user_info_profile(from_number, profile_updates, last_seen_at)

        state_updates_from_tags = _conversation_state_updates_from_tags(
            conversation_tag_updates
        )
        if state_updates_from_tags:
            conversation_state = merge_conversation_state(
                conversation_state, state_updates_from_tags
            )
            conversation_state_dirty = True

        if conversation_state_dirty and partition_key and _history_helper:
            try:
                _history_helper.put_conversation_state(
                    partition_key, conversation_id, conversation_state
                )
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("Failed to persist conversation state")

        final_order_progress_summary = format_order_progress_summary(conversation_state)

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
        if conversation_state:
            self.event["conversation_state"] = conversation_state
        if user_update_entries:
            self.event["user_updates"] = user_update_entries

        return self.event
