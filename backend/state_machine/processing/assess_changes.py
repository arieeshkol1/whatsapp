"""AssessChanges step - enrich events with persisted user context.

This step is feature-gated ("ASSESS_CHANGES_FEATURE") and, when enabled,
retrieves additional context for the current phone number from multiple sources:

* The "UserData" table (name provided by the "USER_DATA_TABLE" env var).
* The main WhatsApp conversation table ("DYNAMODB_TABLE" env var).
* The WhatsApp rules table ("RULES_TABLE_NAME"/"RULES_TABLE" env vars).

The resulting payload is appended to the event so the downstream
"ProcessText" step can use it without re-querying DynamoDB.

Change log:
- Include the new "Name" attribute from "UserData" when present.
- Make DynamoDB resource region-safe for tests/CI (default: us-east-1).
- Tolerate bad stored PKs (e.g., trailing newline / missing '+') when reading.
- Canonicalize returned item to strip whitespace from PhoneNumber and Name.
- Expose RULESET_VERSION, MIN_INTL_DIGITS, and history controls via env.
- Clearer flag evaluation + logging for easier ops.
- New: inline rules mirror under assess_changes["rules"].
- New: newest-first conversation query with summary.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Set

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import BotoCoreError, ClientError

from common.logger import custom_logger

logger = custom_logger()
_DYNAMODB_SCALAR_KEYS = ("S", "N", "B", "BOOL", "NULL")
_DEFAULT_HISTORY_LIMIT = 50
_DEV_HISTORY_LIMIT = 10
_MAX_RECENT_HISTORY = 10
_DEV_HISTORY_TABLES: Set[str] = {"aws-wpp-dev"}
try:
    _MIN_INTL_DIGITS = int(os.environ.get("MIN_INTL_DIGITS", "11"))
except (TypeError, ValueError):
    _MIN_INTL_DIGITS = 11
_DEFAULT_MODEL_ID = os.environ.get("ASSESS_LLM_MODEL_ID", "amazon.nova-lite-v1:0")
_DEFAULT_MAX_TOKENS = 1024


def _parse_float(value: Optional[str], fallback: float) -> float:
    if value is None:
        return fallback
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _parse_int(value: Optional[str], fallback: int) -> int:
    if value is None:
        return fallback
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _unwrap_attribute(value: Any) -> Any:
    """Return the underlying value for simple DynamoDB attribute maps."""
    if isinstance(value, dict):
        for key in _DYNAMODB_SCALAR_KEYS:
            if key in value:
                return value[key]
    return value


def _is_enabled(flag: Optional[str]) -> bool:
    """Return True when the supplied flag represents an enabled state."""
    if flag is None:
        return False
    normalized = str(flag).strip().lower()
    return normalized in {"1", "true", "on", "enabled", "yes"}


_ENABLE_TOLERANT_SCAN = _is_enabled(os.environ.get("ASSESS_TOLERANT_SCAN"))


def _coerce_int(value: Any) -> Optional[int]:
    """Attempt to coerce the provided value into an integer."""

    if value is None:
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return None

    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            return None
        try:
            return int(trimmed)
        except ValueError:
            return None

    return None


def _normalize_phone(number: Optional[str]) -> Optional[str]:
    """Normalize a phone number to E.164 conservatively.

    - If input already starts with '+', return it as-is (trimmed) to preserve stored keys.
    - If digits >= _MIN_INTL_DIGITS, assume international and prefix '+'.
    - Otherwise return the trimmed input.
    """
    if not number:
        return None
    trimmed = str(number).strip()
    if not trimmed:
        return None

    # IMPORTANT: do not rewrite already '+' numbers to '+digits' — it can break key matches
    if trimmed.startswith("+"):
        return trimmed

    digits = "".join(ch for ch in trimmed if ch.isdigit())

    digits = "".join(ch for ch in trimmed if ch.isdigit())
    min_intl_digits = globals().get("_MIN_INTL_DIGITS")
    if not isinstance(min_intl_digits, int) or min_intl_digits < 0:
        try:
            min_intl_digits = int(os.environ.get("MIN_INTL_DIGITS", "11"))
        except (TypeError, ValueError):
            min_intl_digits = 11
        globals()["_MIN_INTL_DIGITS"] = min_intl_digits

    if len(digits) >= min_intl_digits:
        return f"+{digits}"
    return trimmed


def _key_variants(e164: str) -> List[str]:
    """
    Generate robust variants for lookup to tolerate bad stored keys.
    Order matters; first successful match wins.
    """
    variants: List[str] = []
    base = e164.strip()
    if not base:
        return variants
    variants.append(base)
    # Common bad write: trailing newline
    variants.append(base + "\n")
    # If someone stored without '+'
    if base.startswith("+"):
        variants.append(base[1:])
        variants.append(base[1:] + "\n")
    return variants


def _conversation_key_variants(e164: str) -> List[str]:
    """Return candidate partition keys for the conversation history table."""
    prefixed: List[str] = []
    raw_candidates: List[str] = []

    base = e164.strip()
    if not base:
        return prefixed

    def _append_raw(candidate: str) -> None:
        if candidate and candidate not in raw_candidates:
            raw_candidates.append(candidate)

    _append_raw(base)
    if base.startswith("+"):
        _append_raw(base[1:])

    for candidate in raw_candidates:
        prefixed_key = f"NUMBER#{candidate}"
        if prefixed_key not in prefixed:
            prefixed.append(prefixed_key)

    for candidate in raw_candidates:
        if candidate not in prefixed:
            prefixed.append(candidate)

    return prefixed


def _conversation_partition_keys(*numbers: Optional[str]) -> List[str]:
    """Combine conversation key variants for the supplied phone numbers."""

    collected: List[str] = []

    for value in numbers:
        if not isinstance(value, str):
            continue
        trimmed = value.strip()
        if not trimmed:
            continue
        for candidate in _conversation_key_variants(trimmed):
            if candidate not in collected:
                collected.append(candidate)

    return collected


def _rules_partition_key_variants(
    number: Optional[str], explicit_ruleset_id: Optional[str] = None
) -> List[str]:
    """Return candidate PK values for the rules table based on the to-number."""
    variants: List[str] = []

    def _add(candidate: Optional[str]) -> None:
        if candidate and candidate not in variants:
            variants.append(candidate)

    if explicit_ruleset_id:
        raw_ruleset = str(explicit_ruleset_id).strip()
        if raw_ruleset:
            _add(raw_ruleset)
            _add(f"{raw_ruleset}\n")

    if number:
        raw = str(number).strip()
        if raw:
            _add(raw)
            _add(f"{raw}\n")

            normalized = _normalize_phone(raw)
            if normalized:
                _add(normalized)
                _add(f"{normalized}\n")
                if normalized.startswith("+"):
                    _add(normalized[1:])
                    _add(f"{normalized[1:]}\n")

            if raw.startswith("+"):
                _add(raw[1:])
                _add(f"{raw[1:]}\n")

    # Ensure RULESET# prefixed variants are included for compatibility with the
    # dedicated rules_config helper and the table schema defined in the CDK.
    for candidate in list(variants):
        if not candidate.startswith("RULESET#"):
            _add(f"RULESET#{candidate}")

    return variants


def _rules_sort_key_variants(version: Optional[str]) -> List[str]:
    """Return candidate SK values for the rules table."""

    variants: List[str] = []

    if version is not None:
        trimmed = str(version).strip()
    else:
        trimmed = ""

    def _add(candidate: Optional[str]) -> None:
        if candidate and candidate not in variants:
            variants.append(candidate)

    if trimmed:
        _add(trimmed)
        if not trimmed.startswith("VERSION#"):
            _add(f"VERSION#{trimmed}")

    _add("CURRENT")
    _add("VERSION#CURRENT")

    return variants


def _resolve_history_limit(table_name: Optional[str]) -> int:
    """Determine the maximum number of conversation records to return."""

    env_limit = _coerce_int(os.environ.get("ASSESS_CONVERSATION_HISTORY_LIMIT"))
    if env_limit is None:
        env_limit = _coerce_int(os.environ.get("CONVERSATION_HISTORY_LIMIT"))

    if env_limit is not None and env_limit > 0:
        return env_limit

    if table_name:
        normalized = str(table_name).strip().lower()
        if normalized in _DEV_HISTORY_TABLES:
            return _DEV_HISTORY_LIMIT

    return _DEFAULT_HISTORY_LIMIT


class AssessChanges:
    """Enriches the event with user context retrieved from DynamoDB tables."""

    def __init__(self, event: Optional[Dict[str, Any]] = None) -> None:
        self.event = event if isinstance(event, dict) else {}
        self.logger = logger
        self._endpoint_url = os.environ.get("ENDPOINT_URL")
        self._user_data_table_name = os.environ.get("USER_DATA_TABLE")
        self._conversation_table_name = os.environ.get(
            "DYNAMODB_TABLE"
        ) or os.environ.get("TABLE_NAME")
        self._rules_table_name = os.environ.get("RULES_TABLE_NAME") or os.environ.get(
            "RULES_TABLE"
        )
        self._rules_version = os.environ.get("RULESET_VERSION", "CURRENT")
        self._ruleset_id = os.environ.get("RULESET_ID")
        self._llm_model_id = os.environ.get("ASSESS_LLM_MODEL_ID", _DEFAULT_MODEL_ID)
        self._llm_max_tokens = _parse_int(
            os.environ.get("ASSESS_LLM_MAX_TOKENS"), _DEFAULT_MAX_TOKENS
        )
        self._llm_temperature = _parse_float(
            os.environ.get("ASSESS_LLM_TEMPERATURE"), 0.5
        )
        self._llm_top_p = _parse_float(os.environ.get("ASSESS_LLM_TOP_P"), 0.9)

        self._dynamodb_resource = None
        self._conversation_history_limit = _resolve_history_limit(
            self._conversation_table_name
        )

    # ------------------------------------------------------------------
    def assess_and_apply(self) -> Dict[str, Any]:
        """Fetch user context and append it to the event when the feature is on."""
        feature_flag = None
        if isinstance(self.event, dict):
            features = self.event.get("features")
            if isinstance(features, dict):
                feature_flag = features.get("assess_changes")
        if feature_flag is None:
            feature_flag = os.environ.get("ASSESS_CHANGES_FEATURE", "off")

        if not _is_enabled(feature_flag):
            self.logger.debug(
                "AssessChanges disabled",
                extra={
                    "event_flag": (
                        self.event.get("features", {}).get("assess_changes")
                        if isinstance(self.event, dict)
                        else None
                    ),
                    "env_flag": os.environ.get("ASSESS_CHANGES_FEATURE"),
                },
            )
            return self.event

        phone_number = self._extract_phone_number(self.event)
        normalized_phone = _normalize_phone(phone_number)
        if not normalized_phone:
            self.logger.warning("AssessChanges enabled but phone number missing")
            return self.event

        user_data_record = self._load_user_data(normalized_phone)
        destination_number = self._extract_to_number(self.event)
        normalized_destination = _normalize_phone(destination_number)
        if not normalized_destination and destination_number:
            normalized_destination = destination_number
        phone_number_id = self._extract_phone_number_id(self.event)
        conversation_id = self._extract_conversation_id(self.event)
        conversation_items = self._load_conversation_items(
            normalized_phone,
            conversation_id,
            phone_number,
            normalized_destination,
            destination_number,
        )
        business_rules = self._load_business_rules(
            normalized_destination,
            destination_number,
            phone_number_id,
        )

        if (
            user_data_record is not None
            or conversation_items
            or business_rules is not None
        ):
            payload = self.event.get("assess_changes")
            if not isinstance(payload, dict):
                payload = {}
                self.event["assess_changes"] = payload
            if user_data_record is not None:
                # Canonicalize and copy selected fields
                pn = user_data_record.get("PhoneNumber")
                if isinstance(pn, str):
                    user_data_record["PhoneNumber"] = pn.strip()
                name_value = user_data_record.get("Name")
                if isinstance(name_value, str):
                    name_value = name_value.strip()
                    if name_value:
                        user_data_record["Name"] = name_value
                else:
                    user_data_record.pop("Name", None)

                payload["user_data"] = user_data_record
                if (
                    isinstance(user_data_record.get("Name"), str)
                    and user_data_record["Name"]
                ):
                    payload["user_name"] = user_data_record["Name"]

                # Avoid reserved LogRecord keys (e.g., "name")
                self.logger.debug(
                    "AssessChanges user_data loaded",
                    extra={
                        "ctx_phone_last4": (
                            normalized_phone[-4:] if normalized_phone else None
                        ),
                        "has_name": bool(user_data_record.get("Name")),
                    },
                )
            history_items: List[Dict[str, Any]] = []
            if isinstance(conversation_items, list):
                history_items = conversation_items
            if (
                history_items
                or user_data_record is not None
                or business_rules is not None
            ):
                payload["conversation_items"] = history_items
                payload["conversation_history_count"] = len(history_items)

            if business_rules is not None:
                payload["business_rules"] = business_rules
            payload["business_rules_present"] = business_rules is not None

            llm_payload = self._build_llm_payload(
                normalized_phone,
                normalized_destination,
                phone_number_id,
                conversation_id,
                user_data_record,
                conversation_items,
                business_rules,
            )
            if llm_payload is not None:
                payload["llm_request"] = llm_payload

        return self.event

    # ------------------------------------------------------------------
    def _extract_conversation_id(self, event: Dict[str, Any]) -> Optional[int]:
        """Extract the current conversation identifier when available."""

        def _attempt(raw_value: Any) -> Optional[int]:
            coerced = _coerce_int(raw_value)
            if coerced is not None and coerced > 0:
                return coerced
            return None

        if not isinstance(event, dict):
            return None

        direct = _attempt(event.get("conversation_id"))
        if direct is not None:
            return direct

        raw_event = event.get("raw_event")
        if isinstance(raw_event, dict):
            candidate = _attempt(raw_event.get("conversation_id"))
            if candidate is not None:
                return candidate

        input_event = event.get("input")
        if isinstance(input_event, dict):
            candidate = _attempt(input_event.get("conversation_id"))
            if candidate is not None:
                return candidate

            dynamodb_payload = input_event.get("dynamodb")
            if isinstance(dynamodb_payload, dict):
                new_image = dynamodb_payload.get("NewImage")
                if isinstance(new_image, dict):
                    attr = new_image.get("conversation_id")
                    if attr is not None:
                        value = _coerce_int(_unwrap_attribute(attr))
                        if value is not None and value > 0:
                            return value

        original_event = event.get("original_event")
        if isinstance(original_event, dict):
            candidate = self._extract_conversation_id(original_event)
            if candidate is not None:
                return candidate

        return None

    # ------------------------------------------------------------------
    def _extract_phone_number(self, event: Dict[str, Any]) -> Optional[str]:
        """Attempt to locate a phone number across different event shapes."""
        if not isinstance(event, dict):
            return None

        from_number = event.get("from_number")
        if isinstance(from_number, str) and from_number.strip():
            return from_number

        input_event = event.get("input")
        if isinstance(input_event, dict):
            candidate = input_event.get("from") or input_event.get("from_number")
            if isinstance(candidate, str) and candidate.strip():
                return candidate

            dynamodb_payload = input_event.get("dynamodb")
            if isinstance(dynamodb_payload, dict):
                new_image = dynamodb_payload.get("NewImage")
                if isinstance(new_image, dict):
                    from_attr = new_image.get("from_number")
                    if isinstance(from_attr, dict):
                        if "S" in from_attr and from_attr["S"]:
                            return from_attr["S"]
                    elif isinstance(from_attr, str) and from_attr.strip():
                        return from_attr

        raw_event = event.get("raw_event")
        if isinstance(raw_event, dict):
            raw_from = raw_event.get("from") or raw_event.get("from_number")
            if isinstance(raw_from, str) and raw_from.strip():
                return raw_from

        return None

    # ------------------------------------------------------------------
    def _extract_to_number(self, event: Dict[str, Any]) -> Optional[str]:
        """Locate the destination/"to" number for the current event."""
        if not isinstance(event, dict):
            return None

        to_number = event.get("to_number")
        if isinstance(to_number, str) and to_number.strip():
            return to_number

        input_event = event.get("input")
        if isinstance(input_event, dict):
            candidate = input_event.get("to") or input_event.get("to_number")
            if isinstance(candidate, str) and candidate.strip():
                return candidate

            dynamodb_payload = input_event.get("dynamodb")
            if isinstance(dynamodb_payload, dict):
                new_image = dynamodb_payload.get("NewImage")
                if isinstance(new_image, dict):
                    to_attr = new_image.get("to_number")
                    if isinstance(to_attr, dict):
                        value = to_attr.get("S")
                        if isinstance(value, str) and value.strip():
                            return value
                    elif isinstance(to_attr, str) and to_attr.strip():
                        return to_attr

        raw_event = event.get("raw_event")
        if isinstance(raw_event, dict):
            raw_to = raw_event.get("to") or raw_event.get("to_number")
            if isinstance(raw_to, str) and raw_to.strip():
                return raw_to

        return None

    # ------------------------------------------------------------------
    def _extract_phone_number_id(self, event: Dict[str, Any]) -> Optional[str]:
        """Retrieve the WhatsApp phone_number_id when present."""
        if not isinstance(event, dict):
            return None

        direct = event.get("phone_number_id")
        if isinstance(direct, str) and direct.strip():
            return direct.strip()

        raw_event = event.get("raw_event")
        if isinstance(raw_event, dict):
            candidate = raw_event.get("phone_number_id")
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()

        input_event = event.get("input")
        if isinstance(input_event, dict):
            candidate = input_event.get("phone_number_id")
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()

        original_event = event.get("original_event")
        if isinstance(original_event, dict):
            return self._extract_phone_number_id(original_event)

        return None

    # ------------------------------------------------------------------
    def _extract_message_text(self, event: Dict[str, Any]) -> Optional[str]:
        if not isinstance(event, dict):
            return None

        message = event.get("text")
        if isinstance(message, str) and message.strip():
            return message

        raw_event = event.get("raw_event")
        if isinstance(raw_event, dict):
            body = raw_event.get("message_body") or raw_event.get("text")
            if isinstance(body, str) and body.strip():
                return body

        input_event = event.get("input")
        if isinstance(input_event, dict):
            body = input_event.get("message_body") or input_event.get("text")
            if isinstance(body, str) and body.strip():
                return body
            dynamodb_payload = input_event.get("dynamodb")
            if isinstance(dynamodb_payload, dict):
                new_image = dynamodb_payload.get("NewImage")
                if isinstance(new_image, dict):
                    text_attr = new_image.get("text")
                    if isinstance(text_attr, dict):
                        content = text_attr.get("S")
                        if isinstance(content, str) and content.strip():
                            return content
                    elif isinstance(text_attr, str) and text_attr.strip():
                        return text_attr

        return None

    # ------------------------------------------------------------------
    def _determine_user_type(self, user_data: Optional[Dict[str, Any]]) -> str:
        if isinstance(user_data, dict) and user_data.get("Name"):
            return "existing_customer"
        return "new_customer"

    # ------------------------------------------------------------------
    def _build_prior_context(
        self,
        normalized_phone: str,
        destination_number: Optional[str],
        phone_number_id: Optional[str],
        conversation_id: Optional[int],
        user_data: Optional[Dict[str, Any]],
        conversation_items: List[Dict[str, Any]],
        business_rules: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        context: Dict[str, Any] = {
            "phone_number": normalized_phone,
        }
        if destination_number:
            context["business_number"] = destination_number

        if phone_number_id:
            context["phone_number_id"] = phone_number_id

        if conversation_id and conversation_id > 0:
            context["conversation_id"] = conversation_id

        if isinstance(user_data, dict) and user_data:
            context["user_data"] = {
                key: value
                for key, value in user_data.items()
                if isinstance(value, (str, int, float, bool))
            }

        recent: List[Dict[str, Any]] = []
        if conversation_items:
            for item in conversation_items[:_MAX_RECENT_HISTORY]:
                if not isinstance(item, dict):
                    continue
                recent.append(
                    {
                        "from_number": str(item.get("from_number", "")),
                        "type": str(item.get("type", "")),
                        "text": str(item.get("text", "")),
                        "timestamp": str(
                            item.get("whatsapp_timestamp")
                            or item.get("created_at")
                            or ""
                        ),
                        "whatsapp_id": str(item.get("whatsapp_id", "")),
                    }
                )
        context["recent_history"] = recent
        context["recent_history_count"] = len(recent)

        has_rules = isinstance(business_rules, dict) and isinstance(
            business_rules.get("rules_json"), dict
        )
        context["has_business_rules"] = has_rules
        if has_rules:
            context["business_rules"] = business_rules["rules_json"]

        return context

    # ------------------------------------------------------------------
    def _build_system_prompt(
        self, user_type: str, prior_context: Dict[str, Any]
    ) -> str:
        lines = [
            "You are a smart agent for WhatsApp. Based on the user_type, respond accordingly.",
            "Extract structured fields and generate a friendly reply. Always return JSON with keys 'response', 'customer_info', and 'interaction_log'.",
            f"user_type: {user_type}",
        ]

        if prior_context:
            serialized_context = json.dumps(
                prior_context, ensure_ascii=False, separators=(",", ":")
            )
            lines.append("Context:")
            lines.append(serialized_context)

        return "\n".join(lines)

    # ------------------------------------------------------------------
    def _build_llm_payload(
        self,
        normalized_phone: str,
        destination_number: Optional[str],
        phone_number_id: Optional[str],
        conversation_id: Optional[int],
        user_data: Optional[Dict[str, Any]],
        conversation_items: List[Dict[str, Any]],
        business_rules: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        user_message = self._extract_message_text(self.event)
        if not user_message:
            return None

        user_type = self._determine_user_type(user_data)
        prior_context = self._build_prior_context(
            normalized_phone,
            destination_number,
            phone_number_id,
            conversation_id,
            user_data,
            conversation_items,
            business_rules,
        )
        system_prompt = self._build_system_prompt(user_type, prior_context)

        messages = [
            {"role": "system", "content": [{"text": system_prompt}]},
            {"role": "user", "content": [{"text": user_message}]},
        ]

        inference_config = {
            "maxTokens": self._llm_max_tokens,
            "temperature": self._llm_temperature,
            "topP": self._llm_top_p,
        }

        return {
            "model_id": self._llm_model_id,
            "api": "converse",
            "messages": messages,
            "inference_config": inference_config,
            "user_type": user_type,
            "prior_context": prior_context,
        }

    # ------------------------------------------------------------------
    def _get_dynamodb_resource(self):
        if self._dynamodb_resource is None:
            try:
                # Prefer Lambda/real env; fall back to a default for local tests/moto.
                region = (
                    os.environ.get("AWS_REGION")
                    or os.environ.get("AWS_DEFAULT_REGION")
                    or "us-east-1"
                )
                if self._endpoint_url:
                    self._dynamodb_resource = boto3.resource(
                        "dynamodb", endpoint_url=self._endpoint_url, region_name=region
                    )
                else:
                    self._dynamodb_resource = boto3.resource(
                        "dynamodb", region_name=region
                    )
            except Exception:  # pragma: no cover
                self.logger.exception(
                    "Failed to initialise DynamoDB resource for AssessChanges"
                )
                self._dynamodb_resource = None
        return self._dynamodb_resource

    # ------------------------------------------------------------------
    def _load_user_data(self, normalized_phone: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve user data by phone number from the UserData table.

        Since DynamoDB items are schemaless for non-key attributes, if the "Name"
        attribute exists for the item, it will be returned as part of the full item.
        This method also tolerates bad stored keys (trailing newline / missing '+').
        """
        if not self._user_data_table_name:
            return None

        dynamodb = self._get_dynamodb_resource()
        if dynamodb is None:
            return None

        try:
            table = dynamodb.Table(self._user_data_table_name)
            # Try a few variants to tolerate bad stored keys (e.g., trailing newline).
            response = None
            item = None
            for candidate in _key_variants(normalized_phone):
                try:
                    response = table.get_item(Key={"PhoneNumber": candidate})
                    item = response.get("Item") if isinstance(response, dict) else None
                    if isinstance(item, dict):
                        break
                except (ClientError, BotoCoreError):
                    # Will be caught by outer except
                    raise
        except (ClientError, BotoCoreError):
            self.logger.exception(
                "Failed to read user data",
                extra={"phone": normalized_phone},
            )
            return None
        except Exception:  # pragma: no cover
            self.logger.exception(
                "Unexpected error loading user data",
                extra={"phone": normalized_phone},
            )
            return None

        response = None
        item: Optional[Dict[str, Any]] = None
        for candidate in _key_variants(normalized_phone):
            try:
                response = table.get_item(Key={"PhoneNumber": candidate})
                item = response.get("Item") if isinstance(response, dict) else None
                if isinstance(item, dict):
                    break
            except (ClientError, BotoCoreError):
                self.logger.exception(
                    "UserData get_item failed", extra={"candidate": candidate}
                )
                continue
            except Exception:  # pragma: no cover
                self.logger.exception(
                    "Unexpected error reading user data", extra={"candidate": candidate}
                )
                continue

        # Optional tolerant scan to recover truly dirty keys (guarded)
        if item is None and _ENABLE_TOLERANT_SCAN:
            tail = "".join(ch for ch in normalized_phone if ch.isdigit())[-8:]
            scan_kwargs: Dict[str, Any] = {}
            if tail:
                scan_kwargs["FilterExpression"] = Attr("PhoneNumber").contains(tail)
            try:
                resp = table.scan(**scan_kwargs) if scan_kwargs else table.scan()
                for it in resp.get("Items", []) or []:
                    stored = _unwrap_attribute(it.get("PhoneNumber"))
                    if _normalize_phone(stored) == normalized_phone:
                        item = it
                        break
            except (ClientError, BotoCoreError):
                self.logger.exception("UserData tolerant scan failed")
            except Exception:  # pragma: no cover
                self.logger.exception("Unexpected error in tolerant scan")

        if not isinstance(item, dict):
            return None

        # Some environments store the raw DynamoDB attribute map instead of the
        # document-deserialised form. Detect that scenario and convert it to a
        # standard Python dictionary so downstream callers don't have to deal
        # with AttributeValue wrappers (e.g., {"S": "value"}).
        item = {key: _unwrap_attribute(value) for key, value in item.items()}

        # Canonicalise the returned item: strip whitespace from PK if present.
        pn = item.get("PhoneNumber")
        if isinstance(pn, str):
            item["PhoneNumber"] = pn.strip()
        name_val = item.get("Name")
        if isinstance(name_val, str):
            name_val = name_val.strip()
            if name_val:
                item["Name"] = name_val
            else:
                item.pop("Name", None)

        return item

    # ------------------------------------------------------------------
    def _load_conversation_items(
        self,
        normalized_phone: Optional[str],
        conversation_id: Optional[int],
        *additional_numbers: Optional[str],
    ) -> List[Dict[str, Any]]:
        if not self._conversation_table_name:
            return []

        dynamodb = self._get_dynamodb_resource()
        if dynamodb is None:
            return []

        partition_keys = _conversation_partition_keys(
            normalized_phone, *additional_numbers
        )
        if not partition_keys:
            return []

        # Clamp history limit to 1.._MAX_RECENT_HISTORY (10)
        history_limit = self._conversation_history_limit or 1
        if history_limit < 1:
            history_limit = 1
        if history_limit > _MAX_RECENT_HISTORY:
            history_limit = _MAX_RECENT_HISTORY

        try:
            table = dynamodb.Table(self._conversation_table_name)
        except Exception:  # pragma: no cover
            self.logger.exception(
                "Unexpected error preparing conversation query",
                extra={"table": self._conversation_table_name},
            )
            return []

        history_limit = self._conversation_history_limit
        if history_limit is None or history_limit <= 0:
            history_limit = 10
        history_limit = min(history_limit, 10)
        if history_limit <= 0:
            history_limit = 1

        for partition_key in partition_keys:
            collected: List[Dict[str, Any]] = []
            last_evaluated_key: Optional[Dict[str, Any]] = None

            while True:
                query_kwargs: Dict[str, Any] = {
                    "KeyConditionExpression": Key("PK").eq(partition_key)
                    & Key("SK").begins_with("MESSAGE#"),
                    "ScanIndexForward": False,
                    "Limit": history_limit,
                }
                if conversation_id is not None and conversation_id > 0:
                    query_kwargs["FilterExpression"] = Attr("conversation_id").eq(
                        conversation_id
                    )
                if last_evaluated_key:
                    query_kwargs["ExclusiveStartKey"] = last_evaluated_key

                while True:
                    query_kwargs: Dict[str, Any] = {
                        "KeyConditionExpression": Key("PK").eq(partition_key),
                        "ScanIndexForward": False,  # newest first
                        "Limit": page_limit,
                    }
                    if use_filter and prefer_filtered:
                        query_kwargs["FilterExpression"] = Attr("conversation_id").eq(
                            conversation_id
                        )
                    if last_evaluated_key:
                        query_kwargs["ExclusiveStartKey"] = last_evaluated_key

                    try:
                        response = table.query(**query_kwargs)
                    except (ClientError, BotoCoreError):
                        self.logger.exception(
                            "Failed to query conversation items",
                            extra={
                                "table": self._conversation_table_name,
                                "pk": partition_key,
                                "filtered": use_filter,
                            },
                        )
                        collected = []
                        break
                    except Exception:  # pragma: no cover
                        self.logger.exception(
                            "Unexpected error querying conversation items",
                            extra={
                                "table": self._conversation_table_name,
                                "pk": partition_key,
                                "filtered": use_filter,
                            },
                        )
                        collected = []
                        break

                    items = (
                        response.get("Items") if isinstance(response, dict) else None
                    )
                    if isinstance(items, list) and items:
                        for raw_item in items:
                            if not isinstance(raw_item, dict):
                                continue
                            collected.append(
                                {
                                    key: _unwrap_attribute(value)
                                    for key, value in raw_item.items()
                                }
                            )
                            if len(collected) >= history_limit:
                                break

                        if len(collected) >= history_limit:
                            break

                    last_evaluated_key = (
                        response.get("LastEvaluatedKey")
                        if isinstance(response, dict)
                        else None
                    )
                    collected = []
                    break

                items = response.get("Items") if isinstance(response, dict) else None
                if isinstance(items, list) and items:
                    collected.extend(items)
                    if len(collected) >= history_limit:
                        break

                last_evaluated_key = (
                    response.get("LastEvaluatedKey")
                    if isinstance(response, dict)
                    else None
                )
                if not last_evaluated_key:
                    break

            if collected:
                sliced = collected[:history_limit]
                normalized: List[Dict[str, Any]] = []
                for item in sliced:
                    if isinstance(item, dict):
                        normalized.append(
                            {
                                key: _unwrap_attribute(value)
                                for key, value in item.items()
                            }
                        )
                if normalized:
                    return normalized
                return sliced

        return []

    # ------------------------------------------------------------------
    def _load_business_rules(
        self, *identifiers: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        if not self._rules_table_name:
            return None

        dynamodb = self._get_dynamodb_resource()
        if dynamodb is None:
            return None

        try:
            table = dynamodb.Table(self._rules_table_name)
        except Exception:  # pragma: no cover
            self.logger.exception(
                "Unexpected error preparing rules table",
                extra={"table": self._rules_table_name},
            )
            return None

        key_variants: List[str] = []

        explicit_variants = _rules_partition_key_variants(None, self._ruleset_id)
        for candidate in explicit_variants:
            if candidate not in key_variants:
                key_variants.append(candidate)

        for identifier in identifiers:
            if not isinstance(identifier, str):
                continue
            trimmed = identifier.strip()
            if not trimmed:
                continue
            for candidate in _rules_partition_key_variants(trimmed, None):
                if candidate not in key_variants:
                    key_variants.append(candidate)

        if not key_variants:
            return None

        sort_key_variants = _rules_sort_key_variants(self._rules_version)

        item: Optional[Dict[str, Any]] = None
        for partition_key in key_variants:
            for sort_key in sort_key_variants:
                try:
                    response = table.get_item(Key={"PK": partition_key, "SK": sort_key})
                except (ClientError, BotoCoreError):
                    self.logger.exception(
                        "Failed to load business rules",
                        extra={
                            "table": self._rules_table_name,
                            "pk": partition_key,
                            "sk": sort_key,
                        },
                    )
                    continue
                except Exception:  # pragma: no cover
                    self.logger.exception(
                        "Unexpected error loading business rules",
                        extra={
                            "table": self._rules_table_name,
                            "pk": partition_key,
                            "sk": sort_key,
                        },
                    )
                    continue

                data = response.get("Item") if isinstance(response, dict) else None
                if isinstance(data, dict):
                    item = data
                    break
            if item is not None:
                break

        if not isinstance(item, dict):
            return None

        item = {key: _unwrap_attribute(value) for key, value in item.items()}

        rules_blob = item.get("rules_json")
        if isinstance(rules_blob, str):
            try:
                item["rules_json"] = json.loads(rules_blob)
            except json.JSONDecodeError:
                self.logger.warning(
                    "Failed to decode rules_json for business rules",
                    extra={"pk": item.get("PK")},
                )

        return item
