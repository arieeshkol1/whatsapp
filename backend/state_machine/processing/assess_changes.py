"""AssessChanges step - enrich events with persisted user context.

This step is feature-gated ("ASSESS_CHANGES_FEATURE") and, when enabled,
retrieves additional context for the current phone number from multiple sources:

* The "UserData" table (name provided by the "USER_DATA_TABLE" env var).
* The main WhatsApp conversation table ("DYNAMODB_TABLE" env var).
* The WhatsApp rules table ("RULES_TABLE_NAME"/"RULES_TABLE" env vars; version via
  "RULESET_VERSION", default: "CURRENT").

The resulting payload is appended to the event so the downstream
"ProcessText" step can use it without re-querying DynamoDB.

Change log:
- Include the new "Name" attribute from "UserData" when present.
- Make DynamoDB resource region-safe for tests/CI (default: us-east-1).
- Tolerate bad stored PKs (e.g., trailing newline / missing '+') when reading.
- Canonicalize returned item to strip whitespace from PhoneNumber and Name.
- Expose RULESET_VERSION and MIN_INTL_DIGITS via env.
- Clearer flag evaluation + logging for easier ops.
- NEW: newest-first history (ScanIndexForward=False) and size limit via env.
- NEW: add a compact conversation_summary block.
- NEW: expose rules JSON under assess_changes["rules"] for easy consumption.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import BotoCoreError, ClientError

from common.logger import custom_logger

logger = custom_logger()
_DYNAMODB_SCALAR_KEYS = ("S", "N", "B", "BOOL", "NULL")

# --- Configurable knobs via env ---
_MIN_INTL_DIGITS = int(os.environ.get("MIN_INTL_DIGITS", "11"))
_ENABLE_TOLERANT_SCAN = (
    os.environ.get("ASSESS_TOLERANT_SCAN", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
_HISTORY_LIMIT = int(os.environ.get("ASSESS_HISTORY_LIMIT", "50"))
_INCLUDE_HISTORY_SUMMARY = (
    os.environ.get("ASSESS_INCLUDE_HISTORY_SUMMARY", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
_INCLUDE_RULES_INLINE = (
    os.environ.get("ASSESS_INCLUDE_RULES_INLINE", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)


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
    """Normalize a phone number to E.164 conservatively.

    - If already starts with '+', return as-is (stripped).
    - If digits >= _MIN_INTL_DIGITS, assume it's an international number and prefix '+'.
    - Otherwise return the trimmed input to avoid making a bad E.164.
    """
    if not number:
        return None
    trimmed = str(number).strip()
    if not trimmed:
        return None
    if trimmed.startswith("+"):
        return trimmed
    digits = "".join(ch for ch in trimmed if ch.isdigit())
    if len(digits) >= _MIN_INTL_DIGITS:
        return f"+{digits}"
    return trimmed


def _key_variants(e164: str) -> List[str]:
    """Generate robust variants for lookup to tolerate bad stored keys.
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
        self._conversation_table_name = (
            os.environ.get("DYNAMODB_TABLE") or os.environ.get("TABLE_NAME")
        )
        self._rules_table_name = os.environ.get("RULES_TABLE_NAME") or os.environ.get(
            "RULES_TABLE"
        )
        self._rules_version = os.environ.get("RULESET_VERSION", "CURRENT")

        self._dynamodb_resource = None

        # Helpful diagnostics if env is missing
        if not self._conversation_table_name:
            self.logger.warning(
                "AssessChanges: DYNAMODB_TABLE/TABLE_NAME missing; history disabled"
            )
        if not self._rules_table_name:
            self.logger.debug(
                "AssessChanges: RULES_TABLE_NAME/RULES_TABLE missing; rules disabled"
            )
        if not self._user_data_table_name:
            self.logger.debug(
                "AssessChanges: USER_DATA_TABLE missing; profile disabled"
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

            # --- user profile ---
            if user_data_record is not None:
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

                self.logger.debug(
                    "AssessChanges user_data loaded",
                    extra={
                        "ctx_phone_last4": (
                            normalized_phone[-4:] if normalized_phone else None
                        ),
                        "has_name": bool(user_data_record.get("Name")),
                    },
                )

            # --- conversation history ---
            if conversation_items:
                payload["conversation_items"] = conversation_items
                if _INCLUDE_HISTORY_SUMMARY:
                    payload["conversation_summary"] = self._summarize_history(
                        conversation_items
                    )

            # --- rules ---
            if business_rules is not None:
                payload["business_rules"] = business_rules
                rules = business_rules.get("rules_json")
                if _INCLUDE_RULES_INLINE and isinstance(rules, dict):
                    payload["rules"] = rules

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
    def _get_dynamodb_resource(self):  # -> Optional[boto3.resources.base.ServiceResource]
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
        """Retrieve user data by phone number from the UserData table.

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
        except Exception:  # pragma: no cover
            self.logger.exception(
                "Unexpected error preparing user data table",
                extra={"table": self._user_data_table_name},
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
        # document-deserialized form. Convert to plain dict of scalars.
        item = {key: _unwrap_attribute(value) for key, value in item.items()}

        # Canonicalize common fields
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
                    ScanIndexForward=False,  # newest first
                    ProjectionExpression="#pk, SK, created_at, text, whatsapp_id, from_number, #t",
                    ExpressionAttributeNames={"#pk": "PK", "#t": "type"},
                )
            except (ClientError, BotoCoreError):
                self.logger.exception(
                    "Failed to query conversation items",
                    extra={"table": self._conversation_table_name, "pk": partition_key},
                )
                continue
            except Exception:  # pragma: no cover
                self.logger.exception(
                    "Unexpected error querying conversation items",
                    extra={"table": self._conversation_table_name, "pk": partition_key},
                )
                continue

            items = response.get("Items") if isinstance(response, dict) else None
            if isinstance(items, list) and items:
                return items

        return []

    # ------------------------------------------------------------------
    def _load_business_rules(self, to_number: Optional[str]) -> Optional[Dict[str, Any]]:
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
                response = table.get_item(Key={"PK": candidate, "SK": self._rules_version})
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
        if isinstance(rules_blob, bytes):
            try:
                rules_blob = rules_blob.decode("utf-8", errors="replace")
            except Exception:  # pragma: no cover
                rules_blob = None
        if isinstance(rules_blob, str):
            try:
                item["rules_json"] = json.loads(rules_blob)
            except json.JSONDecodeError:
                self.logger.warning(
                    "Failed to decode rules_json for business rules",
                    extra={"pk": item.get("PK")},
                )
        # If dict, leave as-is

        return item

    # ------------------------------------------------------------------
    def _summarize_history(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not items:
            return {}
        # Query is newest-first, so index 0 is the most recent
        newest = items[0]
        oldest = items[-1]

        def _get(d: Dict[str, Any], k: str) -> Optional[str]:
            v = d.get(k)
            return v.strip() if isinstance(v, str) else v

        return {
            "count": len(items),
            "newest_at": _get(newest, "created_at") or _get(newest, "SK"),
            "newest_text": _get(newest, "text"),
            "newest_whatsapp_id": _get(newest, "whatsapp_id"),
            "oldest_at": _get(oldest, "created_at") or _get(oldest, "SK"),
            "oldest_text": _get(oldest, "text"),
        }
