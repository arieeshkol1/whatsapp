import os

os.environ.setdefault("SECRET_NAME", "dummy")
os.environ.setdefault("DYNAMODB_TABLE", "dummy")
os.environ.setdefault("RULES_TABLE_NAME", "dummy-rules")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("STATE_MACHINE_IMPORT_MODE", "minimal")

from backend.state_machine.processing.customer_flow import (
    ConversationState,
    advance_conversation,
)


def test_initial_prompt_without_profile():
    state = ConversationState(phone_number="123")
    reply = advance_conversation(state, "שלום")
    assert "פרטי לקוח" in reply
    assert "איך תרצה" in reply
    assert state.current_step == "collect_name"


def test_collect_name_and_progress():
    state = ConversationState(phone_number="123", current_step="collect_name")
    reply = advance_conversation(state, "דנה כהן")
    assert "האם תרצה לבצע הזמנה" in reply
    assert state.first_name == "דנה"
    assert state.last_name == "כהן"
    assert state.current_step == "ask_new_order"


def test_guest_count_requires_number():
    state = ConversationState(phone_number="123", current_step="collect_guest_count")
    reply = advance_conversation(state, "הרבה אנשים")
    assert "לא הצלחתי לקלוט" in reply
    assert state.guest_count is None
    assert state.current_step == "collect_guest_count"


def test_age_confirmation_positive():
    state = ConversationState(phone_number="123", current_step="confirm_age")
    reply = advance_conversation(state, "כן")
    assert "נחזור אליך" in reply
    assert state.age_verified is True
    assert state.current_step == "completed"


def test_havitush_code_invokes_menu():
    state = ConversationState(phone_number="123")
    reply = advance_conversation(state, "חביתוש123")
    assert "שלום חביתוש" in reply
    assert "לקוחות" in reply
