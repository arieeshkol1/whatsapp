"""Utility functions for managing customer profiles shared with Bedrock."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from botocore.exceptions import BotoCoreError, ClientError

from common.helpers.dynamodb_helper import DynamoDBHelper
from common.logger import custom_logger

_CUSTOMER_DATA_PATH = Path(__file__).with_name("customer_profiles.json")
_CUSTOMER_PROFILE_TABLE = os.environ.get("CUSTOMER_PROFILE_TABLE") or os.environ.get(
    "DYNAMODB_TABLE"
)
_DYNAMODB_ENDPOINT = os.environ.get("ENDPOINT_URL")

logger = custom_logger()

_PROFILE_SORT_KEY = "PROFILE#0"

_dynamodb_helper: Optional[DynamoDBHelper] = None


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


def _load_all_profiles() -> List[Dict]:
    if not _CUSTOMER_DATA_PATH.exists():
        return []
    try:
        with _CUSTOMER_DATA_PATH.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):  # pragma: no cover - defensive guard
        logger.warning("Failed to read customer profile template", exc_info=True)
        return []

    if isinstance(data, list):
        return [entry for entry in data if isinstance(entry, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def _get_dynamodb_helper() -> Optional[DynamoDBHelper]:
    """Return a singleton DynamoDB helper when a table is configured."""

    global _dynamodb_helper

    if not _CUSTOMER_PROFILE_TABLE:
        return None
    if _dynamodb_helper:
        return _dynamodb_helper

    try:
        _dynamodb_helper = DynamoDBHelper(
            table_name=_CUSTOMER_PROFILE_TABLE, endpoint_url=_DYNAMODB_ENDPOINT
        )
    except Exception:  # pragma: no cover - defensive guard
        logger.exception("Failed to initialize DynamoDB helper for customer profiles")
        _dynamodb_helper = None

    return _dynamodb_helper


def _load_profile_from_dynamodb(normalized_phone: str) -> Optional[Dict]:
    helper = _get_dynamodb_helper()
    if not helper:
        return None

    try:
        record = helper.get_customer_profile(normalized_phone, _PROFILE_SORT_KEY)
    except (ClientError, BotoCoreError):  # pragma: no cover - runtime protection
        logger.exception("Failed to read customer profile from DynamoDB")
        return None

    if not record:
        return None

    stored_profile = record.get("profile") if isinstance(record, dict) else None
    if isinstance(stored_profile, dict):
        return stored_profile

    # If the record already follows the template shape just return it directly.
    if isinstance(record, dict):
        return record

    return None


def _persist_profile_to_dynamodb(normalized_phone: str, profile: Dict) -> None:
    helper = _get_dynamodb_helper()
    if not helper:
        return

    try:
        helper.put_customer_profile(normalized_phone, profile, _PROFILE_SORT_KEY)
    except (ClientError, BotoCoreError):  # pragma: no cover - runtime protection
        logger.exception("Failed to persist customer profile to DynamoDB")


def load_customer_profile(phone_number: Optional[str]) -> Optional[Dict]:
    """Return the profile entry that matches the supplied phone number."""

    normalized = _normalize_phone(phone_number)
    if not normalized:
        return None

    stored_profile = _load_profile_from_dynamodb(normalized)
    if stored_profile:
        return stored_profile

    for entry in _load_all_profiles():
        customer = entry.get("\u05dc\u05e7\u05d5\u05d7") or entry.get("customer")
        if not isinstance(customer, dict):
            continue
        phone = _normalize_phone(
            customer.get("\u05de\u05e1\u05e4\u05e8_\u05d8\u05dc\u05e4\u05d5\u05df")
            or customer.get("phone")
            or customer.get("מספר_טלפון")
        )
        if phone == normalized:
            _persist_profile_to_dynamodb(normalized, entry)
            return entry

    return None


def format_customer_summary(entry: Dict) -> str:
    """Build a Hebrew summary of the customer profile and their orders."""

    customer = entry.get("\u05dc\u05e7\u05d5\u05d7") or entry.get("customer") or {}
    orders: Iterable[Dict] = entry.get(
        "\u05d4\u05d6\u05de\u05e0\u05d5\u05ea"
    ) or entry.get("orders", [])

    lines: List[str] = []

    name = customer.get("\u05e9\u05dd") or customer.get("first_name")
    last_name = customer.get("\u05e9\u05dd_\u05de\u05e9\u05e4\u05d7") or customer.get(
        "last_name"
    )
    company = customer.get("\u05e9\u05dd_\u05d7\u05d1\u05e8\u05d4") or customer.get(
        "company_name"
    )
    address = customer.get("\u05db\u05ea\u05d5\u05d1\u05ea") or customer.get("address")
    over_18 = customer.get("\u05de\u05e2\u05dc_\u05d2\u05d9\u05dc_18") or customer.get(
        "over_18"
    )

    lines.append("פרטי הלקוח שנאספו:")
    if name or last_name:
        full_name = " ".join(part for part in [name, last_name] if part)
        lines.append(f"- שם מלא: {full_name.strip() if full_name else ''}".strip())
    if over_18:
        lines.append(f"- הצהרת גיל: {over_18}")
    if company:
        lines.append(f"- שם החברה: {company}")
    if address:
        lines.append(f"- כתובת: {address}")

    orders = [order for order in orders if isinstance(order, dict)]
    if orders:
        lines.append("- הזמנות קודמות:")
        for order in orders:
            order_id = order.get(
                "\u05de\u05e1\u05e4\u05e8_\u05d4\u05d6\u05de\u05e0\u05d4"
            ) or order.get("order_id")
            event_type = order.get(
                "\u05e1\u05d5\u05d2_\u05d0\u05d9\u05e8\u05d5\u05e2"
            ) or order.get("event_type")
            guest_count = order.get(
                "\u05de\u05e1\u05e4\u05e8_\u05d0\u05d5\u05e8\u05d7\u05d9\u05dd"
            ) or order.get("guest_count")
            event_date = order.get(
                "\u05ea\u05d0\u05e8\u05d9\u05da_\u05d0\u05d9\u05e8\u05d5\u05e2"
            ) or order.get("event_date")
            details = [
                f"מספר הזמנה: {order_id}" if order_id else None,
                f"סוג אירוע: {event_type}" if event_type else None,
                f"מספר אורחים: {guest_count}" if guest_count else None,
                f"תאריך: {event_date}" if event_date else None,
            ]
            details = [part for part in details if part]
            if details:
                lines.append("  • " + " | ".join(details))

    timestamp = datetime.now(timezone.utc).isoformat()
    lines.append(f"(נכון ל-{timestamp})")

    return "\n".join(lines)
