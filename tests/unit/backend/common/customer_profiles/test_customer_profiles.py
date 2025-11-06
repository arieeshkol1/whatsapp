import json

from common.customer_profiles import (
    format_customer_summary,
    load_customer_profile,
)


class DummyHelper:
    def __init__(self):
        self.storage = {}

    def get_customer_profile(self, normalized_phone, sort_key):
        return self.storage.get((normalized_phone, sort_key))

    def put_customer_profile(self, normalized_phone, profile, sort_key):
        self.storage[(normalized_phone, sort_key)] = {"profile": profile}


def test_load_customer_profile(tmp_path, monkeypatch):
    data = [
        {
            "לקוח": {
                "שם": "אריאל",
                "שם_משפחה": "אשכול",
                "מספר_טלפון": "+972501234567",
                "כתובת": "רחוב הדוגמה 10, תל אביב",
            },
            "הזמנות": [
                {
                    "מספר_הזמנה": "ORD-1",
                    "מספר_טלפון_מזמין": "+972501234567",
                }
            ],
        }
    ]
    customer_file = tmp_path / "customer_profiles.json"
    customer_file.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(
        "common.customer_profiles._CUSTOMER_DATA_PATH", customer_file, raising=False
    )

    profile = load_customer_profile("972501234567")
    assert profile is not None
    assert profile["לקוח"]["שם"] == "אריאל"

    summary = format_customer_summary(profile)
    assert "אריאל" in summary
    assert "ORD-1" in summary


def test_load_customer_profile_missing_returns_none(monkeypatch, tmp_path):
    customer_file = tmp_path / "customer_profiles.json"
    customer_file.write_text("[]", encoding="utf-8")
    monkeypatch.setattr(
        "common.customer_profiles._CUSTOMER_DATA_PATH", customer_file, raising=False
    )

    assert load_customer_profile("+111") is None


def test_load_customer_profile_uses_dynamodb(monkeypatch):
    helper = DummyHelper()
    helper.storage[("+972501234567", "PROFILE#0")] = {
        "profile": {
            "לקוח": {"שם": "משה", "מספר_טלפון": "+972501234567"},
            "הזמנות": [],
        }
    }

    monkeypatch.setattr(
        "common.customer_profiles._get_dynamodb_helper",
        lambda: helper,
        raising=False,
    )

    profile = load_customer_profile("972501234567")
    assert profile is not None
    assert profile["לקוח"]["שם"] == "משה"


def test_template_profile_persisted_to_dynamodb(tmp_path, monkeypatch):
    data = [
        {
            "לקוח": {
                "שם": "נועה",
                "שם_משפחה": "כהן",
                "מספר_טלפון": "+972585551212",
            },
            "הזמנות": [],
        }
    ]
    customer_file = tmp_path / "customer_profiles.json"
    customer_file.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

    helper = DummyHelper()

    monkeypatch.setattr(
        "common.customer_profiles._CUSTOMER_DATA_PATH", customer_file, raising=False
    )
    monkeypatch.setattr(
        "common.customer_profiles._get_dynamodb_helper",
        lambda: helper,
        raising=False,
    )

    profile = load_customer_profile("+972585551212")
    assert profile is not None
    assert (
        helper.storage[("+972585551212", "PROFILE#0")]["profile"]["לקוח"]["שם"]
        == "נועה"
    )
