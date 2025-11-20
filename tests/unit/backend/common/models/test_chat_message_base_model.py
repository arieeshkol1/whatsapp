import pytest
from uuid import uuid4
from backend.common.models.message_base_model import (
    MessageBaseModel,
)


@pytest.fixture(scope="package")  # used for all tests
def correlation_id():
    return str(uuid4())


@pytest.fixture
def chat_message_base_model(correlation_id) -> MessageBaseModel:
    return MessageBaseModel(
        PK="12345678987",
        SK="MESSAGE#2024-06-19 03:41:42.269532+00:00",
        created_at="2024-06-19 03:41:42.269532+00:00",
        from_number="12345678987",
        type="text",
        whatsapp_id="wamid.DBgMATczCjA2ODI5MTg5FQICBhgUM0FCOUMzNxUxNkT2RUM2OTU5QTIA",
        whatsapp_timestamp="1718768502",
        correlation_id=correlation_id,
        conversation_id=7,
    )


def test_chat_message_base_model(
    chat_message_base_model: MessageBaseModel, correlation_id
):
    # Check the model attributes
    assert chat_message_base_model.PK == "12345678987"
    assert chat_message_base_model.SK == "MESSAGE#2024-06-19 03:41:42.269532+00:00"
    assert chat_message_base_model.created_at == "2024-06-19 03:41:42.269532+00:00"
    assert chat_message_base_model.from_number == "12345678987"
    assert chat_message_base_model.type == "text"
    assert (
        chat_message_base_model.whatsapp_id
        == "wamid.DBgMATczCjA2ODI5MTg5FQICBhgUM0FCOUMzNxUxNkT2RUM2OTU5QTIA"
    )
    assert chat_message_base_model.whatsapp_timestamp == "1718768502"
    assert chat_message_base_model.correlation_id == correlation_id
    assert chat_message_base_model.conversation_id == 7

    # Check the model_dump() method
    chat_message_dict = chat_message_base_model.model_dump(exclude_none=True)
    assert chat_message_dict == {
        "PK": "12345678987",
        "SK": "MESSAGE#2024-06-19 03:41:42.269532+00:00",
        "created_at": "2024-06-19 03:41:42.269532+00:00",
        "from_number": "12345678987",
        "type": "text",
        "whatsapp_id": "wamid.DBgMATczCjA2ODI5MTg5FQICBhgUM0FCOUMzNxUxNkT2RUM2OTU5QTIA",
        "whatsapp_timestamp": "1718768502",
        "correlation_id": correlation_id,
        "conversation_id": 7,
    }


def test_chat_message_from_dynamodb_item():
    dynamodb_item = {
        "PK": {"S": "12345678987"},
        "SK": {"S": "MESSAGE#2024-06-19 03:41:42.269532+00:00"},
        "created_at": {"S": "2024-06-19 03:41:42.269532+00:00"},
        "from_number": {"S": "12345678987"},
        "type": {"S": "text"},
        "whatsapp_id": {
            "S": "wamid.DBgMATczCjA2ODI5MTg5FQICBhgUM0FCOUMzNxUxNkT2RUM2OTU5QTIA"
        },
        "whatsapp_timestamp": {"S": "1718768502"},
        "correlation_id": {"S": str(uuid4())},
        "conversation_id": {"N": "42"},
        "system_response": {"M": {"text": {"S": "hello"}}},
        "Response": {
            "M": {
                "reply": {"S": "hello"},
                "metadata": {"M": {"foo": {"S": "bar"}}},
            }
        },
        "System_Response": {
            "M": {
                "reply": {"S": "hello"},
                "metadata": {"M": {"foo": {"S": "bar"}}},
            }
        },
    }

    chat_message_instance = MessageBaseModel.from_dynamodb_item(dynamodb_item)
    assert chat_message_instance.PK == "12345678987"
    assert chat_message_instance.SK == "MESSAGE#2024-06-19 03:41:42.269532+00:00"
    assert chat_message_instance.conversation_id == 42
    assert chat_message_instance.system_response == {"text": "hello"}
    assert chat_message_instance.Response == {
        "reply": "hello",
        "metadata": {"foo": "bar"},
    }
    assert chat_message_instance.System_Response == {
        "reply": "hello",
        "metadata": {"foo": "bar"},
    }
