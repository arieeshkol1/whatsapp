"""Utilities for extracting and formatting structured conversation state."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, Optional

POSITIVE_HEBREW_RESPONSES = {
    "כן",
    "בהחלט",
    "בטח",
    "כמובן",
    "נכון",
    "ברור",
    "מסכים",
}

NEGATIVE_HEBREW_RESPONSES = {
    "לא",
    "ממש לא",
    "אין",
}

NAME_PATTERNS = (
    re.compile(r"(?:שמי|קוראים לי)\s+([\u0590-\u05FF\w'-]+)\s+([\u0590-\u05FF\w'-]+)"),
    re.compile(r"אני\s+([\u0590-\u05FF\w'-]+)\s+([\u0590-\u05FF\w'-]+)"),
)

COMPANY_PATTERN = re.compile(
    r"(?:שם החברה|שם החברה הוא|החברה היא|חברת)[:\-\s]+([\u0590-\u05FF\w'\s]+)",
    re.IGNORECASE,
)

ADDRESS_PATTERN = re.compile(
    r"(?:כתובת|רחוב|כתובת האירוע)[:\-\s]+([\u0590-\u05FF\w'\s,\d]+)",
    re.IGNORECASE,
)

GUEST_COUNT_PATTERN = re.compile(r"(\d{1,4})\s*(?:אורחים|משתתפים)")

EVENT_DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4})")

EVENT_TYPE_PATTERN = re.compile(
    r"(?:סוג האירוע|האירוע הוא|מדובר ב)[:\-\s]+([\u0590-\u05FF\w'\s]+)",
    re.IGNORECASE,
)


def _normalise_date(raw: str) -> str:
    if not raw:
        return raw
    if "/" in raw:
        day, month, year = raw.split("/")
        try:
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
        except ValueError:
            return raw
    return raw


def extract_state_updates_from_message(message: Optional[str]) -> Dict[str, Any]:
    """Derive structured updates from a free-form customer message."""

    if not message:
        return {}

    text = message.strip()
    if not text:
        return {}

    updates: Dict[str, Any] = {}

    if "18" in text or "גיל" in text:
        has_positive = any(word in text for word in POSITIVE_HEBREW_RESPONSES)
        has_negative = any(word in text for word in NEGATIVE_HEBREW_RESPONSES)
        mentions_all = "כל" in text or "כולם" in text
        mentions_above = "מעל" in text or "לפחות" in text
        if (
            has_positive
            and (mentions_all or mentions_above or "18" in text)
            and not has_negative
        ):
            updates["age_verified"] = True
        elif has_negative and ("18" in text or "גיל" in text):
            updates["age_verified"] = False

    for pattern in NAME_PATTERNS:
        match = pattern.search(text)
        if match:
            first, last = match.groups()
            updates["customer_name"] = f"{first} {last}".strip()
            break

    company_match = COMPANY_PATTERN.search(text)
    if company_match:
        updates["company_name"] = company_match.group(1).strip()

    address_match = ADDRESS_PATTERN.search(text)
    if address_match:
        updates["event_address"] = address_match.group(1).strip()

    guest_match = GUEST_COUNT_PATTERN.search(text)
    if guest_match:
        try:
            updates["guest_count"] = int(guest_match.group(1))
        except ValueError:
            pass

    date_match = EVENT_DATE_PATTERN.search(text)
    if date_match:
        updates["event_date"] = _normalise_date(date_match.group(1))

    event_type_match = EVENT_TYPE_PATTERN.search(text)
    if event_type_match:
        updates["event_type"] = event_type_match.group(1).strip()

    if "הזמנה" in text and "ORD" in text:
        order_id_match = re.search(r"ORD[\-\d]+", text)
        if order_id_match:
            updates["order_id"] = order_id_match.group(0)

    return updates


def merge_conversation_state(
    existing: Optional[Dict[str, Any]], updates: Dict[str, Any]
) -> Dict[str, Any]:
    """Merge updates into the existing conversation state."""

    if not existing:
        existing = {}

    merged = dict(existing)
    for key, value in updates.items():
        if value in (None, ""):
            continue
        merged[key] = value

    if updates:
        merged["last_updated_at"] = datetime.utcnow().isoformat()

    return merged


def format_order_progress_summary(state: Optional[Dict[str, Any]]) -> Optional[str]:
    """Render a Hebrew summary of the collected order details."""

    if not state:
        return None

    lines: list[str] = []

    if state.get("age_verified") is True:
        lines.append("• כל המשתתפים באירוע מעל גיל 18")

    if state.get("customer_name"):
        lines.append(f"• שם הלקוח: {state['customer_name']}")

    if state.get("company_name"):
        lines.append(f"• חברה: {state['company_name']}")

    if state.get("event_type"):
        lines.append(f"• סוג האירוע: {state['event_type']}")

    if state.get("event_date"):
        lines.append(f"• תאריך האירוע: {state['event_date']}")

    if state.get("guest_count"):
        lines.append(f"• מספר משתתפים משוער: {state['guest_count']}")

    if state.get("event_address"):
        lines.append(f"• כתובת האירוע: {state['event_address']}")

    if state.get("order_id"):
        lines.append(f"• מזהה הזמנה שזוהה: {state['order_id']}")

    if not lines:
        return None

    return "\n".join(lines)
