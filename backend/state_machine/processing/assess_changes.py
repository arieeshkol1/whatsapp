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
- Canonicalize returned item to strip whitespace from PhoneNumber.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import BotoCoreError, ClientError

from common.logger import custom_logger

logger = custom_logger()
_DYNAMODB_SCALAR_KEYS = ("S", "N", "B", "BOOL", "NULL")
_HISTORY_LIMIT = 50


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


def _rules_partition_key_variants(number: Optional[str]) -> List[str]:
    """Return candidate PK values for the rules table based on the to-number."""
    variants: List[str] = []
    if not number:
        return variants

    raw = str(number).strip()
    if not raw:
        return variants

    def _add(candidate: str) -> None:
        if candidate and candidate not in variants:
            variants.append(candidate)

    _add(raw)

    normalized = _normalize_phone(raw)
    if normalized:
        _add(normalized)
        if normalized.startswith("+"):
            _add(normalized[1:])

    if raw.startswith("+"):
        _add(raw[1:])

    return variants


class AssessChanges:
    """Enriches the event with user context retrieved from DynamoDB tables."""

    _CONVERSATION_QUERY_LIMIT = _HISTORY_LIMIT

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
        business_rules = self._load_business_rules(self._extract_to_number(self.event))

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
            if business_rules is not None:
                payload["business_rules"] = business_rules

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
                # Some environments return raw AttributeValue maps. Normalize them so
                # downstream code always receives simple Python dictionaries.
                normalized: List[Dict[str, Any]] = []
                for item in items:
                    if isinstance(item, dict):
                        normalized.append(
                            {
                                key: _unwrap_attribute(value)
                                for key, value in item.items()
                            }
                        )
                if normalized:
                    return normalized
                return items

        return []

    # ------------------------------------------------------------------
    def _load_business_rules(
        self, to_number: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        if not self._rules_table_name or not to_number:
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

        key_variants = _rules_partition_key_variants(to_number)
        if not key_variants:
            return None

        item: Optional[Dict[str, Any]] = None
        for candidate in key_variants:
            try:
                response = table.get_item(
                    Key={"PK": candidate, "SK": self._rules_version}
                )
            except (ClientError, BotoCoreError):
                self.logger.exception(
                    "Failed to load business rules",
                    extra={"table": self._rules_table_name, "pk": candidate},
                )
                continue
            except Exception:  # pragma: no cover
                self.logger.exception(
                    "Unexpected error loading business rules",
                    extra={"table": self._rules_table_name, "pk": candidate},
                )
                continue

            data = response.get("Item") if isinstance(response, dict) else None
            if isinstance(data, dict):
                item = data
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
