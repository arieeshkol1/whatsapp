"""Conversation flow utilities for the WhatsApp state machine.

The real system previously relied on a Bedrock agent to orchestrate the
dialogue.  For the new strict workflow we keep the Lambda handler lean by
encapsulating the customer/order state machine in this module.  The
`ConversationFlow` class persists customer details in DynamoDB and returns the
next reply that the WhatsApp sender should deliver.

The conversation requirements are:

* Always greet as the Havitush digital agent and speak Hebrew.
* Each reply must include the customer details followed by the current order
  details so the guest can see the accumulated information.
* Support the ``חביתוש123`` supervisor code which diverts the conversation to a
  dedicated management menu.
* Enforce the mandatory ordering steps (name confirmation, company, address,
  date, guest count and age verification).

The state is stored in DynamoDB using a single-table pattern with
``PK = CUSTOMER#<phone>`` and ``SK = PROFILE``.  Each turn updates the profile
so future messages can reuse the collected information and avoid repeated
questions.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
import re
from typing import Dict, Optional

import boto3
from botocore.exceptions import ClientError

from common.logger import custom_logger
from common.rules_loader import load_ruleset


LOGGER = custom_logger()

RULESET = load_ruleset()
MESSAGES = RULESET["messages"]
PROMPTS = MESSAGES["prompts"]
ERRORS = MESSAGES["errors"]
SUMMARY_CFG = MESSAGES["summary"]
FALLBACK_MESSAGE = MESSAGES["fallback"]
SUPERVISOR_CFG = RULESET["supervisor"]
FLOW_DEFINITION = {step["id"]: step for step in RULESET["flow"]}
INITIAL_STEP_EXISTING = RULESET["initial_step_existing"]
INITIAL_STEP_NEW = RULESET["initial_step_new"]
SUPERVISOR_CODE = SUPERVISOR_CFG.get("trigger_code", "חביתוש123")
COMPLETION_REPEAT_KEY = "completed_repeat"


def _get_table_name() -> str:
    table_name = os.environ.get("DYNAMODB_TABLE")
    if not table_name:
        raise RuntimeError("DYNAMODB_TABLE environment variable is required")
    return table_name


def _get_table():
    return boto3.resource("dynamodb").Table(_get_table_name())


POSITIVE_KEYWORDS = {"כן", "בוודאי", "כמובן", "בטח", "yes", "y"}
NEGATIVE_KEYWORDS = {"לא", "no", "לאו", "לא רוצה"}


@dataclass
class ConversationState:
    """Serializable representation of the customer/order status."""

    phone_number: str
    current_step: str = "start"
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company_name: Optional[str] = None
    event_address: Optional[str] = None
    event_date: Optional[str] = None
    guest_count: Optional[int] = None
    age_verified: Optional[bool] = None
    wants_new_order: Optional[bool] = None

    def as_item(self) -> Dict[str, object]:
        item: Dict[str, object] = {
            "PK": f"CUSTOMER#{self.phone_number}",
            "SK": "PROFILE",
            "current_step": self.current_step,
        }
        if self.first_name:
            item["first_name"] = self.first_name
        if self.last_name:
            item["last_name"] = self.last_name
        if self.company_name:
            item["company_name"] = self.company_name
        if self.event_address:
            item["event_address"] = self.event_address
        if self.event_date:
            item["event_date"] = self.event_date
        if self.guest_count is not None:
            item["guest_count"] = self.guest_count
        if self.age_verified is not None:
            item["age_verified"] = self.age_verified
        if self.wants_new_order is not None:
            item["wants_new_order"] = self.wants_new_order
        return item

    @classmethod
    def from_item(
        cls, phone_number: str, item: Optional[Dict[str, object]]
    ) -> "ConversationState":
        if not item:
            return cls(phone_number=phone_number)
        return cls(
            phone_number=phone_number,
            current_step=item.get("current_step", "start"),
            first_name=item.get("first_name"),
            last_name=item.get("last_name"),
            company_name=item.get("company_name"),
            event_address=item.get("event_address"),
            event_date=item.get("event_date"),
            guest_count=item.get("guest_count"),
            age_verified=item.get("age_verified"),
            wants_new_order=item.get("wants_new_order"),
        )


def _load_state(phone_number: str) -> ConversationState:
    table = _get_table()
    try:
        response = table.get_item(
            Key={"PK": f"CUSTOMER#{phone_number}", "SK": "PROFILE"}
        )
    except ClientError as exc:
        LOGGER.error(
            "Failed to load conversation state",
            extra={"phone_number": phone_number, "error": exc.response["Error"]},
        )
        raise
    return ConversationState.from_item(phone_number, response.get("Item"))


def _save_state(state: ConversationState) -> None:
    table = _get_table()
    try:
        table.put_item(Item=state.as_item())
    except ClientError as exc:
        LOGGER.error(
            "Failed to persist conversation state",
            extra={"phone_number": state.phone_number, "error": exc.response["Error"]},
        )
        raise


def _is_positive(message: str) -> bool:
    normalized = message.strip().lower()
    return any(keyword.lower() in normalized for keyword in POSITIVE_KEYWORDS)


def _is_negative(message: str) -> bool:
    normalized = message.strip().lower()
    return any(keyword.lower() in normalized for keyword in NEGATIVE_KEYWORDS)


def _extract_name(message: str) -> Optional[tuple[str, str]]:
    tokens = [token for token in re.split(r"\s+", message.strip()) if token]
    if len(tokens) < 2:
        return None
    first_name = tokens[0]
    last_name = " ".join(tokens[1:])
    return first_name, last_name


def _extract_guest_count(message: str) -> Optional[int]:
    match = re.search(r"(\d+)", message)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _full_name(state: ConversationState) -> str:
    return " ".join(filter(None, [state.first_name, state.last_name])).strip()


def _format_prompt(prompt_key: str, state: ConversationState, **extra: object) -> str:
    template = PROMPTS.get(prompt_key, "")
    context = {
        "full_name": _full_name(state),
        "first_name": state.first_name or "",
        "last_name": state.last_name or "",
        "company_name": state.company_name or "",
        "event_address": state.event_address or "",
        "event_date": state.event_date or "",
        "guest_count": state.guest_count if state.guest_count is not None else "",
    }
    context.update(extra)
    try:
        return template.format(**context)
    except Exception:
        return template


def _build_summary(state: ConversationState) -> str:
    customer_lines = []
    full_name = _full_name(state)
    if full_name:
        customer_lines.append(SUMMARY_CFG["name_known"].format(full_name=full_name))
    else:
        customer_lines.append(SUMMARY_CFG["name_missing"])

    customer_lines.append(SUMMARY_CFG["phone"].format(phone_number=state.phone_number))

    if state.company_name:
        customer_lines.append(
            SUMMARY_CFG["company_known"].format(company_name=state.company_name)
        )
    else:
        customer_lines.append(SUMMARY_CFG["company_missing"])

    if state.event_address:
        customer_lines.append(
            SUMMARY_CFG["address_known"].format(event_address=state.event_address)
        )
    else:
        customer_lines.append(SUMMARY_CFG["address_missing"])

    order_lines = []
    if state.event_date:
        order_lines.append(
            SUMMARY_CFG["event_date_known"].format(event_date=state.event_date)
        )
    else:
        order_lines.append(SUMMARY_CFG["event_date_missing"])

    if state.guest_count is not None:
        order_lines.append(
            SUMMARY_CFG["guest_count_known"].format(guest_count=state.guest_count)
        )
        if state.guest_count < 60:
            order_lines.append(SUMMARY_CFG["guest_reco_small"])
        elif state.guest_count <= 120:
            order_lines.append(
                SUMMARY_CFG["guest_reco_medium"].format(
                    price_self_service=state.guest_count * 100
                )
            )
        else:
            order_lines.append(
                SUMMARY_CFG["guest_reco_large"].format(
                    price_staffed=state.guest_count * 80
                )
            )
    else:
        order_lines.append(SUMMARY_CFG["guest_count_missing"])
        order_lines.append(SUMMARY_CFG["guest_reco_unknown"])

    if state.age_verified is None:
        order_lines.append(SUMMARY_CFG["age_pending"])
    elif state.age_verified:
        order_lines.append(SUMMARY_CFG["age_ok"])
    else:
        order_lines.append(SUMMARY_CFG["age_failed"])

    if state.wants_new_order is True:
        order_lines.append(SUMMARY_CFG["order_status_open"])
    elif state.wants_new_order is False:
        order_lines.append(SUMMARY_CFG["order_status_declined"])
    else:
        order_lines.append(SUMMARY_CFG["order_status_unknown"])

    return "\n".join(
        [SUMMARY_CFG["customer_header"]]
        + customer_lines
        + ["", SUMMARY_CFG["order_header"]]
        + order_lines
    )


def _menu_for_havitush(state: ConversationState) -> str:
    summary = _build_summary(state)
    lines = [SUPERVISOR_CFG.get("greeting", "שלום חביתוש!")]
    lines.extend(SUPERVISOR_CFG.get("menu_lines", []))
    menu = "\n".join(lines)
    return f"{summary}\n\n{menu}"


def _age_verification_message(state: ConversationState) -> str:
    summary = _build_summary(state)
    if state.age_verified:
        message = _format_prompt("completed_success", state)
    else:
        message = _format_prompt("completed_underage", state)
    return f"{summary}\n\n{message}"


def _prompt_for_next_step(state: ConversationState) -> str:
    summary = _build_summary(state)
    step = FLOW_DEFINITION.get(state.current_step)
    if not step:
        return summary
    prompt_key = step.get("prompt")
    if not prompt_key:
        return summary
    prompt = _format_prompt(prompt_key, state)
    return f"{summary}\n\n{prompt}"


def _initial_step(state: ConversationState) -> str:
    if state.first_name or state.last_name:
        state.current_step = INITIAL_STEP_EXISTING
    else:
        state.current_step = INITIAL_STEP_NEW
    return _prompt_for_next_step(state)


def _handle_collect_name(state: ConversationState, message: str) -> str:
    parsed = _extract_name(message)
    if not parsed:
        summary = _build_summary(state)
        return f"{summary}\n\n{ERRORS['name_retry']}"
    state.first_name, state.last_name = parsed
    next_step = FLOW_DEFINITION.get("collect_name", {}).get("next", "ask_new_order")
    state.current_step = next_step
    return _prompt_for_next_step(state)


def _handle_guest_count(state: ConversationState, message: str) -> str:
    guests = _extract_guest_count(message)
    if guests is None:
        summary = _build_summary(state)
        return f"{summary}\n\n{ERRORS['guest_count_retry']}"
    state.guest_count = guests
    next_step = FLOW_DEFINITION.get("collect_guest_count", {}).get(
        "next", "confirm_age"
    )
    state.current_step = next_step
    return _prompt_for_next_step(state)


def _handle_age_confirmation(state: ConversationState, message: str) -> str:
    step_def = FLOW_DEFINITION.get("confirm_age", {})
    if _is_positive(message):
        state.age_verified = True
        state.current_step = step_def.get("positive_next", "completed")
    elif _is_negative(message):
        state.age_verified = False
        state.current_step = step_def.get("negative_next", "halted_underage")
    else:
        summary = _build_summary(state)
        return f"{summary}\n\n{ERRORS['age_retry']}"
    return _age_verification_message(state)


def _handle_new_order(state: ConversationState, message: str) -> str:
    step_def = FLOW_DEFINITION.get("ask_new_order", {})
    if _is_positive(message):
        state.wants_new_order = True
        state.current_step = step_def.get("positive_next", "collect_company")
        return _prompt_for_next_step(state)
    if _is_negative(message):
        state.wants_new_order = False
        state.current_step = step_def.get("negative_next", "completed_no_order")
        summary = _build_summary(state)
        message_text = _format_prompt("completed_no_order", state)
        return f"{summary}\n\n{message_text}"
    summary = _build_summary(state)
    return f"{summary}\n\n{ERRORS['new_order_retry']}"


def _handle_completed(state: ConversationState) -> str:
    summary = _build_summary(state)
    step_def = FLOW_DEFINITION.get(state.current_step, {})
    prompt_key = step_def.get("prompt", COMPLETION_REPEAT_KEY)
    message = _format_prompt(prompt_key, state)
    return f"{summary}\n\n{message}"


def advance_conversation(state: ConversationState, message: str) -> str:
    """Advance the conversation state machine and return the next reply."""

    stripped = message.strip()
    if stripped == SUPERVISOR_CODE:
        state.current_step = "havitush_menu"
        return _menu_for_havitush(state)

    if state.current_step in {"start", "havitush_menu"}:
        return _initial_step(state)

    if state.current_step == "confirm_name":
        step_def = FLOW_DEFINITION.get("confirm_name", {})
        if _is_positive(message):
            state.current_step = step_def.get("positive_next", "ask_new_order")
            return _prompt_for_next_step(state)
        if _is_negative(message):
            state.current_step = step_def.get("negative_next", "collect_name")
            return _prompt_for_next_step(state)
        summary = _build_summary(state)
        return f"{summary}\n\n{ERRORS['confirm_name_retry']}"

    if state.current_step == "collect_name":
        return _handle_collect_name(state, message)

    if state.current_step == "ask_new_order":
        return _handle_new_order(state, message)

    if state.current_step == "collect_company":
        state.company_name = stripped
        state.current_step = FLOW_DEFINITION.get("collect_company", {}).get(
            "next", "collect_address"
        )
        return _prompt_for_next_step(state)

    if state.current_step == "collect_address":
        state.event_address = stripped
        state.current_step = FLOW_DEFINITION.get("collect_address", {}).get(
            "next", "collect_event_date"
        )
        return _prompt_for_next_step(state)

    if state.current_step == "collect_event_date":
        state.event_date = stripped
        state.current_step = FLOW_DEFINITION.get("collect_event_date", {}).get(
            "next", "collect_guest_count"
        )
        return _prompt_for_next_step(state)

    if state.current_step == "collect_guest_count":
        return _handle_guest_count(state, message)

    if state.current_step == "confirm_age":
        return _handle_age_confirmation(state, message)

    if state.current_step in {"completed", "completed_no_order", "halted_underage"}:
        return _handle_completed(state)

    # Default fallback
    summary = _build_summary(state)
    return f"{summary}\n\n{FALLBACK_MESSAGE}"


class ConversationFlow:
    """Persistence aware facade used by the Lambda handler."""

    def __init__(self, phone_number: str):
        self.state = _load_state(phone_number)

    def handle(self, message: str) -> str:
        reply = advance_conversation(self.state, message)
        _save_state(self.state)
        return reply


__all__ = [
    "ConversationFlow",
    "ConversationState",
    "advance_conversation",
]
