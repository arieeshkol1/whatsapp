from common.models.message_base_model import (
    MessageBaseModel,
    _deserialize_attribute,
)


class TextMessageModel(MessageBaseModel):
    """
    Class that represents a Chat Message item with text.
    All additional attributes are inherited from the MessageBaseModel.

    Attributes:
        PK: str: Primary Key for the DynamoDB item (business "to" number)
        SK: str: Sort Key for the DynamoDB item (ISO-8601 timestamp)
        to_number: str: Business phone number (duplicate of PK for GSI lookups).
        from_number: str: Phone number of the sender.
        timestamp: str: Local-time timestamp of the message.
        type: str: Type of message (text, image, video, etc).
        user_message: str: Text of the message.
        correlation_id: Optional(str): Correlation ID for the message.
        conversation_id: int: Numeric identifier for the conversation/topic this
            message belongs to.
    """

    user_message: str

    @classmethod
    def from_dynamodb_item(cls, dynamodb_item: dict) -> "TextMessageModel":
        return cls(
            PK=dynamodb_item["PK"]["S"],
            SK=dynamodb_item["SK"]["S"],
            to_number=dynamodb_item.get("to_number", {}).get("S")
            or dynamodb_item["PK"]["S"],
            from_number=dynamodb_item["from_number"]["S"],
            timestamp=dynamodb_item["timestamp"]["S"],
            type=dynamodb_item["type"]["S"],
            user_message=dynamodb_item["user_message"]["S"],
            correlation_id=dynamodb_item.get("correlation_id", {}).get("S"),
            conversation_id=int(
                dynamodb_item.get("conversation_id", {}).get("N", "1") or 1
            ),
            system_response=_deserialize_attribute(
                dynamodb_item.get("system_response")
            ),
            Response=_deserialize_attribute(dynamodb_item.get("Response")),
            System_Response=_deserialize_attribute(
                dynamodb_item.get("System_Response") or dynamodb_item.get("Response")
            ),
        )
