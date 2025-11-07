from backend.common.conversation_state import (
    extract_state_updates_from_message,
    format_order_progress_summary,
    merge_conversation_state,
)


def test_extract_state_updates_from_message_detects_age_and_guests():
    message = "כן, כל המשתתפים מעל גיל 18 ויש לנו 85 אורחים"
    updates = extract_state_updates_from_message(message)

    assert updates["age_verified"] is True
    assert updates["guest_count"] == 85


def test_extract_state_updates_from_message_detects_date_and_address():
    message = "תאריך האירוע הוא 15/12/2025 והכתובת רחוב הדוגמה 10 תל אביב"
    updates = extract_state_updates_from_message(message)

    assert updates["event_date"] == "2025-12-15"
    assert updates["event_address"] == "רחוב הדוגמה 10 תל אביב"


def test_merge_and_format_conversation_state():
    base_state = {"customer_name": "אריאל אשכול"}
    updates = {"age_verified": True, "guest_count": 120}

    merged = merge_conversation_state(base_state, updates)
    assert merged["customer_name"] == "אריאל אשכול"
    assert merged["age_verified"] is True
    assert merged["guest_count"] == 120
    assert "last_updated_at" in merged

    summary = format_order_progress_summary(merged)
    assert summary is not None
    assert "כל המשתתפים באירוע מעל גיל 18" in summary
    assert "מספר משתתפים" in summary
