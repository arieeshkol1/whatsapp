import copy
from typing import Any, Dict, Optional

from state_machine.base_step_function import BaseStepFunction
from common.logger import custom_logger

logger = custom_logger()


def _string_attr(value: Optional[Any]) -> Optional[Dict[str, str]]:
    if value is None:
        return None
    text = str(value)
    if text == "":
        return None
    return {"S": text}


def _number_attr(value: Optional[Any]) -> Optional[Dict[str, str]]:
    if value is None:
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
    return {"N": str(number)}


TYPE_SPECIFIC_FIELDS = {
    "text": ("message_body", "text"),
    "image": ("image_url", "image_url"),
    "video": ("video_url", "video_url"),
    "voice": ("voice_url", "voice_url"),
    "interactive": ("interactive_payload", "interactive_payload"),
}


class Adapter(BaseStepFunction):
    """Transforms webhook-friendly payloads into the DynamoDB-stream shape."""

    def __init__(self, event: Dict[str, Any]):
        super().__init__(event, logger=logger)

    def transform_input(self) -> Dict[str, Any]:
        raw_payload: Dict[str, Any] = (
            copy.deepcopy(self.event) if isinstance(self.event, dict) else {}
        )

        message_type = raw_payload.get("message_type")
        from_number = raw_payload.get("from") or raw_payload.get("from_number")
        to_number = raw_payload.get("to") or raw_payload.get("to_number")
        message_body = raw_payload.get("message_body")
        wa_id = raw_payload.get("wa_id")
        last_seen_at = raw_payload.get("last_seen_at")
        message_id = raw_payload.get("message_id")
        correlation_id = raw_payload.get("correlation_id")
        conversation_id = raw_payload.get("conversation_id")

        new_image: Dict[str, Dict[str, str]] = {}

        for key, value in (
            ("type", message_type),
            ("from_number", from_number),
            ("to_number", to_number),
            ("whatsapp_id", wa_id),
            ("last_seen_at", last_seen_at),
            ("message_id", message_id),
            ("correlation_id", correlation_id),
        ):
            attr = _string_attr(value)
            if attr:
                new_image[key] = attr

        number_attr = _number_attr(conversation_id)
        if number_attr:
            new_image["conversation_id"] = number_attr

        specific_mapping = TYPE_SPECIFIC_FIELDS.get(message_type)
        if specific_mapping:
            source_key, target_key = specific_mapping
            specific_attr = _string_attr(raw_payload.get(source_key))
            if specific_attr:
                new_image[target_key] = specific_attr
        elif message_type == "text" and message_body:
            new_image.setdefault("text", _string_attr(message_body))

        if message_type == "text" and "text" not in new_image:
            new_image["text"] = _string_attr(message_body)

        if message_type != "text":
            new_image.pop("text", None)

        adapter_output: Dict[str, Any] = {
            "input": {"dynamodb": {"NewImage": new_image}},
            "raw_event": raw_payload,
        }

        derived_fields = {
            "from_number": from_number,
            "to_number": to_number,
            "message_type": message_type,
            "whatsapp_id": wa_id,
            "last_seen_at": last_seen_at,
            "message_id": message_id,
        }
        if message_type == "text":
            derived_fields["text"] = message_body

        if conversation_id is not None:
            try:
                derived_fields["conversation_id"] = int(conversation_id)
            except (TypeError, ValueError):
                pass

        for key, value in derived_fields.items():
            if value is not None:
                adapter_output[key] = value

        if "features" in raw_payload and isinstance(raw_payload["features"], dict):
            adapter_output["features"] = copy.deepcopy(raw_payload["features"])
        if "metadata" in raw_payload and isinstance(raw_payload["metadata"], dict):
            adapter_output["metadata"] = copy.deepcopy(raw_payload["metadata"])

        logger.debug(
            "Adapter produced DynamoDB-style event", extra={"keys": list(new_image)}
        )

        return adapter_output
