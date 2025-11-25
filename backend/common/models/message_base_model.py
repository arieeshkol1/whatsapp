from typing import Any, Dict, Optional

from boto3.dynamodb.types import TypeDeserializer


_deserializer = TypeDeserializer()


def _deserialize_attribute(value: Optional[Dict[str, Any]]) -> Optional[Any]:
    if not isinstance(value, dict):
        return None

    try:
        return _deserializer.deserialize(value)
    except Exception:
        return None


from pydantic import BaseModel, Field


class MessageBaseModel(BaseModel):
    """
    Class that represents a Chat Message item (Base Model).

    Attributes:
        PK: str: Primary Key for the DynamoDB item (business "to" number)
        SK: str: Sort Key for the DynamoDB item (ISO-8601 timestamp)
        to_number: str: Business phone number (duplicate of PK for GSI lookups).
        from_number: str: Phone number of the sender.
        timestamp: str: Local-time timestamp of the message.
        type: str: Type of message (text, image, video, etc).
        correlation_id: Optional(str): Correlation ID for the message.
        conversation_id: int: Numeric identifier for the conversation/topic this
            message belongs to.
    """

    PK: str = Field(pattern=r"^\d{6,15}$")
    SK: str
    to_number: str
    timestamp: str
    from_number: str
    type: str
    correlation_id: Optional[str] = None
    conversation_id: int = Field(default=1, ge=1)
    system_response: Optional[Dict[str, Any]] = None
    Response: Optional[Dict[str, Any]] = None
    System_Response: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dynamodb_item(cls, dynamodb_item: dict) -> "MessageBaseModel":
        return cls(
            PK=dynamodb_item["PK"]["S"],
            SK=dynamodb_item["SK"]["S"],
            to_number=dynamodb_item.get("to_number", {}).get("S")
            or dynamodb_item["PK"]["S"],
            from_number=dynamodb_item["from_number"]["S"],
            timestamp=dynamodb_item["timestamp"]["S"],
            type=dynamodb_item["type"]["S"],
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
