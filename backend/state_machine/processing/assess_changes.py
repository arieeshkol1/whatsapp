"""AssessChanges step - enrich events with persisted user context.

This step is feature-gated ("ASSESS_CHANGES_FEATURE") and, when enabled,
retrieves additional context for the current phone number from two sources:

* The "UserData" table (name provided by the "USER_DATA_TABLE" env var).
* The main WhatsApp conversation table ("DYNAMODB_TABLE" env var).

The resulting payload is appended to the event so the downstream
"ProcessText" step can use it without re-querying DynamoDB.

Change log:
- Include the new "Name" attribute from "UserData" when present.
- Make DynamoDB resource region-safe for tests/CI (default: us-east-1).
- Tolerate bad stored PKs (e.g., trailing newline / missing '+') when reading.
- Canonicalize returned item to strip whitespace from PhoneNumber.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import BotoCoreError, ClientError

from common.logger import custom_logger

logger = custom_logger()
_DYNAMODB_SCALAR_KEYS = ("S", "N", "B", "BOOL", "NULL")


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


def _normalize_phone(number: Optional[str]) -> Optional[str]:
    """Normalise a phone number to E.164 (adds '+' prefix when missing)."""
    if not number:
        return None
    trimmed = str(number).strip()
    if not trimmed:
        return None
    if trimmed.startswith("+"):
        return trimmed
    if trimmed[0].isdigit():
        return f"+{trimmed}"
    return trimmed


def _key_variants(e164: str) -> List[str]:
    """
    Generate robust variants for lookup to tolerate bad stored keys.
    Order matters; first successful match wins.
    """
    variants: List[str] = []
    base = e164.strip()
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
    variants: List[str] = []
    base = e164.strip()
    if not base:
        return variants

    def _append_variant(phone: str) -> None:
        key = f"NUMBER#{phone}"
        if key not in variants:
            variants.append(key)

    _append_variant(base)
    if base.startswith("+"):
        _append_variant(base[1:])

    return variants


class AssessChanges:
    """Enriches the event with user context retrieved from DynamoDB tables."""

    _CONVERSATION_QUERY_LIMIT = 200

    def __init__(self, event: Optional[Dict[str, Any]] = None) -> None:
        self.event = event if isinstance(event, dict) else {}
        self.logger = logger
        self._endpoint_url = os.environ.get("ENDPOINT_URL")
        self._user_data_table_name = os.environ.get("USER_DATA_TABLE")
        self._conversation_table_name = os.environ.get(
            "DYNAMODB_TABLE"
        ) or os.environ.get("TABLE_NAME")

        self._dynamodb_resource = None

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
            self.logger.debug("AssessChanges disabled; returning event unchanged")
            return self.event

        phone_number = self._extract_phone_number(self.event)
        normalized_phone = _normalize_phone(phone_number)
        if not normalized_phone:
            self.logger.warning("AssessChanges enabled but phone number missing")
            return self.event

        user_data_record = self._load_user_data(normalized_phone)
        conversation_items = self._load_conversation_items(normalized_phone)

        if user_data_record is not None or conversation_items:
            payload = self.event.get("assess_changes")
            if not isinstance(payload, dict):
                payload = {}
                self.event["assess_changes"] = payload
            if user_data_record is not None:
                payload["user_data"] = user_data_record
                # Provide a flat "user_name" for convenience in downstream steps.
                name_value = user_data_record.get("Name")
                if isinstance(name_value, str) and name_value.strip():
                    payload["user_name"] = name_value
                # Avoid reserved LogRecord keys (e.g., "name")
                self.logger.debug(
                    "AssessChanges user_data loaded",
                    extra={
                        "ctx_phone": normalized_phone,
                        "ctx_user_name": user_data_record.get("Name"),
                    },
                )
            if conversation_items:
                payload["conversation_items"] = conversation_items

        return self.event

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
                "Failed to read user data", extra={"phone": normalized_phone}
            )
            return None
        except Exception:  # pragma: no cover
            self.logger.exception(
                "Unexpected error loading user data", extra={"phone": normalized_phone}
            )
            return None

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
        if isinstance(name_val, str) and not name_val.strip():
            # Normalise empty strings to missing to avoid confusing downstream checks.
            item.pop("Name", None)
        return item

    # ------------------------------------------------------------------
    def _load_conversation_items(self, normalized_phone: str) -> List[Dict[str, Any]]:
        if not self._conversation_table_name:
            return []

        dynamodb = self._get_dynamodb_resource()
        if dynamodb is None:
            return []

        partition_keys = _conversation_key_variants(normalized_phone)
        if not partition_keys:
            return []

        try:
            table = dynamodb.Table(self._conversation_table_name)
        except Exception:  # pragma: no cover
            self.logger.exception(
                "Unexpected error preparing conversation query",
                extra={"table": self._conversation_table_name},
            )
            return []

        for partition_key in partition_keys:
            try:
                response = table.query(
                    KeyConditionExpression=Key("PK").eq(partition_key),
                    Limit=self._CONVERSATION_QUERY_LIMIT,
                )
            except (ClientError, BotoCoreError):
                self.logger.exception(
                    "Failed to query conversation items",
                    extra={
                        "table": self._conversation_table_name,
                        "pk": partition_key,
                    },
                )
                continue
            except Exception:  # pragma: no cover
                self.logger.exception(
                    "Unexpected error querying conversation items",
                    extra={
                        "table": self._conversation_table_name,
                        "pk": partition_key,
                    },
                )
                continue

            items = response.get("Items") if isinstance(response, dict) else None
            if isinstance(items, list) and items:
                return items

        return []
