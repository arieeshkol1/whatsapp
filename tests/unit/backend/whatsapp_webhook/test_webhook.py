import asyncio
import importlib
import json
from types import SimpleNamespace

import pytest


class DummyRequest(SimpleNamespace):
    headers: dict
    query_params: dict
    path_params: dict


class DummyDynamoHelper:
    def __init__(self):
        self.put_calls = []
        self.latest_items = {}

    def put_item(self, data):
        self.put_calls.append(data)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def get_latest_item_by_pk(self, partition_key: str):
        return self.latest_items.get(partition_key)


class DummyStepFunctions:
    def __init__(self):
        self.start_calls = []

    def start_execution(self, **kwargs):
        self.start_calls.append(kwargs)
        return {"executionArn": "arn:aws:states:us-east-1:123:execution:dummy"}


@pytest.fixture
def webhook_module(monkeypatch):
    monkeypatch.setenv("DYNAMODB_TABLE", "dummy-table")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv(
        "STATE_MACHINE_ARN", "arn:aws:states:us-east-1:123:stateMachine:dummy"
    )
    module = importlib.import_module("backend.whatsapp_webhook.api.v1.routers.webhook")
    importlib.reload(module)
    module.dynamodb_helper = DummyDynamoHelper()
    module.stepfunctions_client = DummyStepFunctions()
    return module


def test_status_payload_is_ignored(webhook_module):
    request = DummyRequest(headers={}, query_params={}, path_params={})
    body = {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "statuses": [
                                {
                                    "id": "wamid.HBgMockStatus",
                                    "status": "sent",
                                }
                            ]
                        }
                    }
                ]
            }
        ]
    }

    result = asyncio.run(webhook_module.post_chatbot_webhook(request, body))

    assert result == {"message": "ok", "details": "Status notification ignored"}
    assert webhook_module.dynamodb_helper.put_calls == []
    assert webhook_module.stepfunctions_client.start_calls == []


def test_text_message_triggers_state_machine(webhook_module, monkeypatch):
    request = DummyRequest(headers={}, query_params={}, path_params={})
    body = {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "messaging_product": "whatsapp",
                            "metadata": {
                                "display_phone_number": "15550001111",
                                "phone_number_id": "PN-123",
                            },
                            "messages": [
                                {
                                    "from": "+15551234567",
                                    "id": "wamid.abc",
                                    "timestamp": "1762208436",
                                    "type": "text",
                                    "text": {"body": "שלום"},
                                }
                            ],
                        }
                    }
                ]
            }
        ]
    }

    # Pretend this is the latest message so conversation id increments
    helper = webhook_module.dynamodb_helper
    helper.latest_items = {}

    result = asyncio.run(webhook_module.post_chatbot_webhook(request, body))

    assert result == {"message": "ok", "details": "Received message"}
    assert len(helper.put_calls) == 1
    assert len(webhook_module.stepfunctions_client.start_calls) == 1

    call = webhook_module.stepfunctions_client.start_calls[0]
    assert call["stateMachineArn"].endswith(":stateMachine:dummy")

    payload = json.loads(call["input"])
    assert payload["input"]["from"] == "+15551234567"
    assert payload["input"]["to"] == "15550001111"
    assert payload["input"]["message_type"] == "text"
    assert payload["input"]["message_body"] == "שלום"
    assert payload["conversation_id"] == 1
