import copy
from typing import Any, Dict, Optional

from state_machine.base_step_function import BaseStepFunction
from common.logger import custom_logger

logger = custom_logger()


def _extract_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    """Return the canonical payload carrying the incoming message details."""

    if not isinstance(event, dict):
        return {}

    payload = event.get("input")
    if isinstance(payload, dict):
        return payload

    return event


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
    "text": ("message_body", "user_message"),
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
        raw_event: Dict[str, Any] = (
            copy.deepcopy(self.event) if isinstance(self.event, dict) else {}
        )
        payload = copy.deepcopy(_extract_payload(raw_event))

        if payload.get("dynamodb"):
            logger.debug(
                "Adapter received DynamoDB-style payload; returning passthrough"
            )
            adapter_output = copy.deepcopy(raw_event)
            adapter_output.setdefault("raw_event", payload)
            return adapter_output

        message_type = payload.get("message_type")
        if message_type is None and payload.get("message_body"):
            message_type = "text"

        from_number = payload.get("from") or payload.get("from_number")
        to_number = payload.get("to") or payload.get("to_number")
        message_body = payload.get("message_body")
        wa_id = payload.get("wa_id")
        last_seen_at = payload.get("last_seen_at")
        correlation_id = payload.get("correlation_id") or raw_event.get(
            "correlation_id"
        )
        conversation_id = payload.get("conversation_id") or raw_event.get(
            "conversation_id"
        )
        message_timestamp = payload.get("message_timestamp")

        new_image: Dict[str, Dict[str, str]] = {}

        for key, value in (
            ("type", message_type),
            ("from_number", from_number),
            ("to_number", to_number),
            ("last_seen_at", last_seen_at),
            ("SK", message_timestamp),
            ("timestamp", message_timestamp),
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
            specific_attr = _string_attr(payload.get(source_key))
            if specific_attr:
                new_image[target_key] = specific_attr
        elif message_type == "text" and message_body:
            new_image.setdefault("user_message", _string_attr(message_body))

        if message_type == "text" and "user_message" not in new_image:
            new_image["user_message"] = _string_attr(message_body)

        if message_type != "text":
            new_image.pop("user_message", None)

        adapter_output: Dict[str, Any] = {
            "input": {"dynamodb": {"NewImage": new_image}},
            "raw_event": payload,
        }

        derived_fields = {
            "from_number": from_number,
            "to_number": to_number,
            "message_type": message_type,
            "last_seen_at": last_seen_at,
            "message_sort_key": message_timestamp,
            "timestamp": message_timestamp,
        }
        if message_type == "text":
            derived_fields["text"] = message_body
            derived_fields["user_message"] = message_body

        if conversation_id is not None:
            try:
                derived_fields["conversation_id"] = int(conversation_id)
            except (TypeError, ValueError):
                pass

        for key, value in derived_fields.items():
            if value is not None:
                adapter_output[key] = value

        if "features" in raw_event and isinstance(raw_event["features"], dict):
            adapter_output["features"] = copy.deepcopy(raw_event["features"])
        if "metadata" in raw_event and isinstance(raw_event["metadata"], dict):
            adapter_output["metadata"] = copy.deepcopy(raw_event["metadata"])

        if correlation_id and "correlation_id" not in adapter_output:
            adapter_output["correlation_id"] = correlation_id

        if raw_event.get("input") is not payload:
            # Preserve the outer payload for downstream debugging
            adapter_output.setdefault("original_event", raw_event)

        logger.debug(
            "Adapter produced DynamoDB-style event", extra={"keys": list(new_image)}
        )

        return adapter_output
