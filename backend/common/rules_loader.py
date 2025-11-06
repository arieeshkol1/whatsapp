"""Load configurable conversation rules from DynamoDB.

The WhatsApp chatbot uses a dedicated DynamoDB table to hold the
conversation flow definition. This module reads the ruleset (and seeds a
baseline definition when the table is empty) so the runtime behaviour can be
updated without redeploying Lambda code.
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

from common.logger import custom_logger

LOGGER = custom_logger()

_RULESET_PARTITION = "RULESET#HAVITUSH"
_RULESET_SORT = "CURRENT"


DEFAULT_RULESET: Dict[str, Any] = {
    "flow_name": "havitush",
    "initial_step_existing": "confirm_name",
    "initial_step_new": "collect_name",
    "supervisor": {
        "trigger_code": "חביתוש123",
        "greeting": "שלום חביתוש!",
        "menu_lines": [
            "כאן רשימת הפעולות הזמינות:",
            "1. קבלת מידע על לקוחות",
            "2. קבלת מידע על הזמנות",
            "3. סקירת שלבי התהליך הנוכחיים",
            "ענה במספר או תאר את הבקשה שלך ואכוון אותך לשם.",
        ],
    },
    "flow": [
        {
            "id": "confirm_name",
            "type": "confirm",
            "prompt": "confirm_name",
            "positive_next": "ask_new_order",
            "negative_next": "collect_name",
        },
        {
            "id": "collect_name",
            "type": "name",
            "prompt": "collect_name",
            "next": "ask_new_order",
        },
        {
            "id": "ask_new_order",
            "type": "confirm",
            "prompt": "ask_new_order",
            "positive_next": "collect_company",
            "negative_next": "completed_no_order",
        },
        {
            "id": "collect_company",
            "type": "text",
            "prompt": "collect_company",
            "next": "collect_address",
        },
        {
            "id": "collect_address",
            "type": "text",
            "prompt": "collect_address",
            "next": "collect_event_date",
        },
        {
            "id": "collect_event_date",
            "type": "text",
            "prompt": "collect_event_date",
            "next": "collect_guest_count",
        },
        {
            "id": "collect_guest_count",
            "type": "integer",
            "prompt": "collect_guest_count",
            "next": "confirm_age",
        },
        {
            "id": "confirm_age",
            "type": "confirm",
            "prompt": "confirm_age",
            "positive_next": "completed",
            "negative_next": "halted_underage",
        },
        {"id": "completed", "type": "terminal", "prompt": "completed_success"},
        {
            "id": "completed_no_order",
            "type": "terminal",
            "prompt": "completed_no_order",
        },
        {
            "id": "halted_underage",
            "type": "terminal",
            "prompt": "completed_underage",
        },
    ],
    "messages": {
        "summary": {
            "customer_header": "פרטי לקוח:",
            "order_header": "פרטי ההזמנה:",
            "name_known": "- שם: {full_name}",
            "name_missing": "- שם: טרם נמסר",
            "company_known": "- חברה: {company_name}",
            "company_missing": "- חברה: טרם נמסרה",
            "phone": "- מספר טלפון: {phone_number}",
            "address_known": "- כתובת אירוע: {event_address}",
            "address_missing": "- כתובת אירוע: טרם נמסרה",
            "event_date_known": "- תאריך אירוע: {event_date}",
            "event_date_missing": "- תאריך אירוע: טרם נבחר",
            "guest_count_known": "- מספר משתתפים: {guest_count}",
            "guest_count_missing": "- מספר משתתפים: טרם נמסר",
            "guest_reco_small": "- המלצה: להזמנה עד 60 משתתפים ניתן להזמין באתר https://www.havitush.co.il",
            "guest_reco_medium": "- המלצה: שירות עצמי (עלות משוערת: {price_self_service} ₪)",
            "guest_reco_large": "- המלצה: עמדה מאוישת (עלות משוערת: {price_staffed} ₪)",
            "guest_reco_unknown": "- המלצה: נדרש מספר משתתפים כדי להתאים הצעה",
            "age_pending": "- סטטוס גילאים: ממתין לאישור",
            "age_ok": "- סטטוס גילאים: כל המשתתפים מעל גיל 18",
            "age_failed": "- סטטוס גילאים: הזמנה נעצרה (מתחת לגיל 18)",
            "order_status_open": "- סטטוס הזמנה: בתהליך פתיחת הזמנה חדשה",
            "order_status_declined": "- סטטוס הזמנה: הלקוח לא ביקש לפתוח הזמנה חדשה",
            "order_status_unknown": "- סטטוס הזמנה: טרם הוחלט",
        },
        "prompts": {
            "confirm_name": "האם השם שלך הוא {full_name}?",
            "collect_name": "איך תרצה שנרשום את שמך המלא (שם פרטי ושם משפחה)?",
            "ask_new_order": "האם תרצה לבצע הזמנה חדשה של אירוע בחביתוש?",
            "collect_company": "מה שם החברה עבור ההזמנה?",
            "collect_address": "מהי הכתובת המלאה של האירוע?",
            "collect_event_date": "מהו תאריך האירוע (בפורמט YYYY-MM-DD)?",
            "collect_guest_count": "כמה משתתפים צפויים להגיע לאירוע?",
            "confirm_age": "האם כל המשתתפים באירוע מעל גיל 18?",
            "completed_success": "נחזור אליך עם הצעת מחיר מסודרת ביממה הקרובה. תודה שבחרת בחביתוש!",
            "completed_no_order": "נשמור את הפרטים שלך ונשמח לעזור כשתרצה להזמין.",
            "completed_underage": "מצטער, לא ניתן לבצע הזמנה אם אחד מהמשתתפים מתחת לגיל 18.",
            "completed_repeat": "הפרטים כבר נקלטו. אם תרצה לפתוח הזמנה חדשה, כתוב זאת ואעדכן בהתאם.",
        },
        "errors": {
            "confirm_name_retry": "אנא אשר/י אם זהו שמך או ציין/י את השם הנכון.",
            "name_retry": "אשמח לשם פרטי ושם משפחה (לדוגמה: 'דנה כהן').",
            "guest_count_retry": "לא הצלחתי לקלוט את מספר המשתתפים. אפשר לציין מספר?",
            "age_retry": "אנא אשר/י אם כל המשתתפים מעל גיל 18.",
            "new_order_retry": "לא הבנתי, האם ברצונך לבצע הזמנה חדשה בחביתוש?",
        },
        "fallback": "נשמח להמשיך לעזור, רק אמור/י לי מה הצעד הבא שתרצה לבצע.",
    },
}


def _should_use_stub_rules() -> bool:
    return os.environ.get("STATE_MACHINE_IMPORT_MODE") == "minimal"


def _get_rules_table_name() -> str:
    # Make RULES_TABLE_NAME optional: an empty string means "use defaults"
    table_name = os.environ.get("RULES_TABLE_NAME")
    return table_name or ""


def _get_rules_table():
    table_name = _get_rules_table_name()
    if not table_name:
        return None
    return boto3.resource("dynamodb").Table(table_name)


def _ruleset_key() -> Dict[str, str]:
    return {"PK": _RULESET_PARTITION, "SK": _RULESET_SORT}


def _seed_ruleset(table) -> Dict[str, Any]:
    LOGGER.info(
        "Seeding default conversation rules", extra={"table_name": table.table_name}
    )
    table.put_item(
        Item={
            **_ruleset_key(),
            "rules_json": json.dumps(DEFAULT_RULESET, ensure_ascii=False),
            "version": "v1",
        }
    )
    return DEFAULT_RULESET


def _deserialize_rules(item: Dict[str, Any]) -> Dict[str, Any]:
    data = item.get("rules_json") if item else None
    if isinstance(data, str):
        try:
            return json.loads(data)
        except json.JSONDecodeError as exc:
            LOGGER.error("Failed to decode ruleset JSON", extra={"error": str(exc)})
            return DEFAULT_RULESET
    if isinstance(data, dict):
        return data
    return DEFAULT_RULESET


@lru_cache(maxsize=1)
def load_ruleset() -> Dict[str, Any]:
    """Return the current ruleset, seeding defaults when the table is empty."""
    # If tests or local runs want to bypass DynamoDB entirely
    if _should_use_stub_rules():
        return DEFAULT_RULESET

    table = _get_rules_table()
    if table is None:
        # No RULES_TABLE_NAME provided → just use the built-in defaults
        return DEFAULT_RULESET

    try:
        response = table.get_item(Key=_ruleset_key())
    except ClientError as exc:
        LOGGER.error(
            "Unable to load ruleset from DynamoDB",
            extra={
                "table_name": table.table_name,
                "error": exc.response.get("Error", {}),
            },
        )
        raise

    item = response.get("Item")
    if not item:
        return _seed_ruleset(table)

    return _deserialize_rules(item)


__all__ = ["load_ruleset", "DEFAULT_RULESET"]
