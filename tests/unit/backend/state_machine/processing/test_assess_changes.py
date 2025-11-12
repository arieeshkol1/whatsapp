import importlib.util
from pathlib import Path


MODULE_PATH = Path("backend/state_machine/processing/assess_changes.py")


def _load_module():
    spec = importlib.util.spec_from_file_location("assess_changes", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_assess_changes_returns_event_when_disabled():
    payload = {"foo": "bar"}
    module = _load_module()
    processor = module.AssessChanges(payload)

    assert processor.assess_and_apply() == payload


def test_conversation_key_variants_match_history_schema():
    module = _load_module()

    variants = module._conversation_key_variants("+972502425777")

    assert "NUMBER#972502425777" in variants
    assert "NUMBER#+972502425777" in variants
    assert "+972502425777" in variants
    assert "NUMBER#972502425777\n" in variants
    assert "+972502425777\n" in variants


def test_conversation_partition_keys_include_multiple_numbers():
    module = _load_module()

    keys = module._conversation_partition_keys(
        "+972524347196",
        "972502649476",
        "909607985563422",
        None,
        "   ",
    )

    assert "NUMBER#+972524347196" in keys
    assert "NUMBER#972524347196" in keys
    assert "NUMBER#972502649476" in keys
    assert "972502649476" in keys
    assert "NUMBER#909607985563422" in keys


def test_rules_partition_key_variants_cover_normalized_destination():
    module = _load_module()

    variants = module._rules_partition_key_variants("972502649476")

    assert "972502649476" in variants
    assert "+972502649476" in variants
    assert "RULESET#972502649476" in variants
    assert "RULESET#+972502649476" in variants


def test_rules_partition_key_variants_handle_trailing_newline():
    module = _load_module()

    variants = module._rules_partition_key_variants("972502649476\n")

    assert "972502649476" in variants
    assert "972502649476\n" in variants
    assert "RULESET#972502649476" in variants
    assert "RULESET#972502649476\n" in variants


def test_prior_context_limits_recent_history_to_maximum():
    module = _load_module()

    processor = module.AssessChanges({})

    over_limit = module._MAX_RECENT_HISTORY + 5
    conversation_items = [
        {
            "from_number": f"from-{idx}",
            "type": "text",
            "text": f"message-{idx}",
            "whatsapp_timestamp": f"{idx}",
            "whatsapp_id": f"id-{idx}",
        }
        for idx in range(over_limit)
    ]

    context = processor._build_prior_context(
        "+1234567890",
        "+1987654321",
        "phone-id",
        42,
        {"Name": "Tester"},
        conversation_items,
        None,
    )

    assert context["recent_history_count"] == module._MAX_RECENT_HISTORY
    assert len(context["recent_history"]) == module._MAX_RECENT_HISTORY
    assert context["recent_history"][0]["text"] == "message-0"
    assert context["recent_history"][-1]["text"] == (
        f"message-{module._MAX_RECENT_HISTORY - 1}"
    )


def test_load_conversation_items_prefers_filtered_matches():
    module = _load_module()

    class FakeTable:
        def __init__(self, responses):
            self._responses = {key: list(value) for key, value in responses.items()}

        def query(self, **kwargs):
            expr = kwargs.get("KeyConditionExpression")
            partition_key = None
            if expr is not None and hasattr(expr, "_values"):
                _, partition_key = expr._values
            use_filter = "FilterExpression" in kwargs
            bucket = self._responses.get((partition_key, use_filter), [])
            if bucket:
                return bucket.pop(0)
            return {"Items": []}

    class FakeDynamo:
        def __init__(self, table):
            self._table = table

        def Table(self, name):
            assert name == "history-table"
            return self._table

    table = FakeTable(
        {
            ("NUMBER#+15551234567", True): [{"Items": []}],
            ("NUMBER#15551234567", True): [
                {
                    "Items": [
                        {
                            "conversation_id": "7",
                            "text": "matched",
                            "from_number": "15551234567",
                        }
                    ]
                }
            ],
        }
    )

    processor = module.AssessChanges({})
    processor._conversation_table_name = "history-table"
    processor._conversation_history_limit = 10
    processor._dynamodb_resource = FakeDynamo(table)

    results = processor._load_conversation_items("+15551234567", 7)

    assert results and results[0]["text"] == "matched"


def test_load_conversation_items_falls_back_to_recent_when_no_filtered_match():
    module = _load_module()

    class FakeTable:
        def __init__(self, responses):
            self._responses = {key: list(value) for key, value in responses.items()}

        def query(self, **kwargs):
            expr = kwargs.get("KeyConditionExpression")
            partition_key = None
            if expr is not None and hasattr(expr, "_values"):
                _, partition_key = expr._values
            use_filter = "FilterExpression" in kwargs
            bucket = self._responses.get((partition_key, use_filter), [])
            if bucket:
                return bucket.pop(0)
            return {"Items": []}

    class FakeDynamo:
        def __init__(self, table):
            self._table = table

        def Table(self, name):
            assert name == "history-table"
            return self._table

    table = FakeTable(
        {
            ("NUMBER#+15551234567", True): [{"Items": []}],
            ("NUMBER#15551234567", True): [{"Items": []}],
            ("NUMBER#15551234567", False): [
                {
                    "Items": [
                        {
                            "conversation_id": "3",
                            "text": "recent",
                            "from_number": "15551234567",
                        }
                    ]
                }
            ],
        }
    )

    processor = module.AssessChanges({})
    processor._conversation_table_name = "history-table"
    processor._conversation_history_limit = 10
    processor._dynamodb_resource = FakeDynamo(table)

    results = processor._load_conversation_items("+15551234567", 99)

    assert results and results[0]["text"] == "recent"


def test_load_conversation_items_scan_fallback(monkeypatch):
    monkeypatch.setenv("ASSESS_TOLERANT_SCAN", "true")
    module = _load_module()

    class FakeTable:
        def query(self, **kwargs):
            return {"Items": []}

        def scan(self, **kwargs):
            return {
                "Items": [
                    {
                        "PK": {"S": "NUMBER#15551234567"},
                        "SK": {"S": "MESSAGE#1"},
                        "from_number": {"S": "15551234567"},
                        "text": {"S": "scanned-newest"},
                        "whatsapp_timestamp": {"N": "200"},
                    },
                    {
                        "PK": {"S": "NUMBER#15551234567"},
                        "SK": {"S": "MESSAGE#0"},
                        "from_number": {"S": "15551234567"},
                        "text": {"S": "scanned-oldest"},
                        "whatsapp_timestamp": {"N": "100"},
                    },
                ]
            }

    class FakeDynamo:
        def __init__(self, table):
            self._table = table

        def Table(self, name):
            assert name == "history-table"
            return self._table

    table = FakeTable()

    processor = module.AssessChanges({})
    processor._conversation_table_name = "history-table"
    processor._conversation_history_limit = 10
    processor._dynamodb_resource = FakeDynamo(table)

    results = processor._load_conversation_items("+15551234567", None, "15551234567")

    assert [item["text"] for item in results] == ["scanned-newest", "scanned-oldest"]
