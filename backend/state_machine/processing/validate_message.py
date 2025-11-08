import os

from state_machine.base_step_function import BaseStepFunction
from common.logger import custom_logger

logger = custom_logger()


TYPE_REQUIREMENTS = {
    "text": ("text", "NewImage.text.S is required for text messages"),
    "image": ("image_url", "NewImage.image_url.S is required for image messages"),
    "video": ("video_url", "NewImage.video_url.S is required for video messages"),
    "voice": ("voice_url", "NewImage.voice_url.S is required for voice messages"),
    "interactive": (
        "interactive_payload",
        "NewImage.interactive_payload.S is required for interactive messages",
    ),
}


class ValidateMessage(BaseStepFunction):
    """
    Validates the incoming message event (typically DynamoDB stream record),
    extracts the core fields, and raises a clear error if anything is missing.
    """

    def __init__(self, event):
        super().__init__(event, logger=logger)

    def _require(self, value, msg: str):
        if value is None:
            raise ValueError(msg)
        return value

    def validate_input(self):
        self.logger.info("Starting validate_input for the chatbot")

        evt = self.event or {}
        raw_event = evt.get("raw_event", evt)
        dd = evt.get("input", {}).get("dynamodb", {})

        new_image = dd.get("NewImage")
        if new_image:
            # Extract the needed fields from NewImage
            from_number = new_image.get("from_number", {}).get("S")
            msg_type = new_image.get("type", {}).get("S")
            text = new_image.get("text", {}).get("S")
            whatsapp_id = new_image.get("whatsapp_id", {}).get("S")
            correlation_id = new_image.get("correlation_id", {}).get("S")
            conversation_id_value = new_image.get("conversation_id", {}).get("N")

            if conversation_id_value is None:
                self.logger.warning(
                    "Missing conversation_id in DynamoDB image; defaulting to 1"
                )
                conversation_id = 1
            else:
                try:
                    conversation_id = int(conversation_id_value)
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        "NewImage.conversation_id.N must be numeric"
                    ) from exc

            # If any core field is missing from the DynamoDB image, try to
            # hydrate it from the top-level event payload before enforcing
            # required checks. This covers synthetic or replayed events that do
            # not persist every attribute.
            if not from_number:
                from_number = (
                    raw_event.get("from")
                    or raw_event.get("from_number")
                    or evt.get("from_number")
                    or evt.get("from")
                )
            if not text:
                text = raw_event.get("message_body") or evt.get("text")
            if not whatsapp_id:
                whatsapp_id = raw_event.get("wa_id") or evt.get("whatsapp_id")
            if not correlation_id:
                correlation_id = raw_event.get("correlation_id") or evt.get(
                    "correlation_id"
                )
            if msg_type is None:
                fallback_type = raw_event.get("message_type") or evt.get("message_type")
                if not fallback_type and (text or evt.get("text")):
                    fallback_type = "text"
                if fallback_type:
                    self.logger.warning(
                        "Missing message type in DynamoDB image; defaulting to %s",
                        fallback_type,
                    )
                    msg_type = fallback_type
        else:
            self.logger.warning(
                "DynamoDB NewImage not supplied; falling back to direct event payload"
            )
            from_number = (
                raw_event.get("from")
                or raw_event.get("from_number")
                or evt.get("from_number")
                or evt.get("from")
            )
            text = raw_event.get("message_body") or evt.get("text")
            msg_type = (
                raw_event.get("message_type")
                or evt.get("message_type")
                or ("text" if text else None)
            )
            whatsapp_id = raw_event.get("wa_id") or evt.get("whatsapp_id")
            correlation_id = raw_event.get("correlation_id") or evt.get(
                "correlation_id"
            )
            conversation_id_value = raw_event.get(
                "conversation_id", evt.get("conversation_id", 1)
            )
            try:
                conversation_id = int(conversation_id_value)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "event.conversation_id must be numeric when provided"
                ) from exc

        # Required checks (tighten/relax to your needs)
        self._require(from_number, "NewImage.from_number.S is required")
        self._require(msg_type, "NewImage.type.S is required")
        self._require(whatsapp_id, "NewImage.whatsapp_id.S is required")

        required_field, error_message = TYPE_REQUIREMENTS.get(msg_type, (None, None))
        if required_field:
            field_value = None
            if new_image:
                field_value = new_image.get(required_field, {}).get("S")
            if field_value is None:
                fallback_map = {
                    "text": text,
                    "image_url": raw_event.get("image_url") or evt.get("image_url"),
                    "video_url": raw_event.get("video_url") or evt.get("video_url"),
                    "voice_url": raw_event.get("voice_url") or evt.get("voice_url"),
                    "interactive_payload": raw_event.get("interactive_payload")
                    or evt.get("interactive_payload"),
                }
                field_value = fallback_map.get(required_field)
            self._require(field_value, error_message)
            if (
                new_image
                and required_field not in new_image
                and field_value is not None
            ):
                new_image[required_field] = {"S": str(field_value)}
            if required_field == "text" and not text and field_value:
                text = field_value

        # Log a safe preview
        self.logger.info(
            "Validated message",
            extra={
                "from_number_masked": (
                    (from_number[:4] + "***" + from_number[-2:])
                    if from_number
                    else "<none>"
                ),
                "msg_type": msg_type,
                "has_text": bool(text),
                "whatsapp_id": whatsapp_id,
                "correlation_id": correlation_id or "<none>",
                "conversation_id": conversation_id,
            },
        )

        # Enrich the event for downstream steps
        evt["validated"] = True
        evt["message_type"] = msg_type
        evt["from_number"] = from_number
        evt["whatsapp_id"] = whatsapp_id
        evt["conversation_id"] = conversation_id
        if text:
            evt["text"] = text

        features = evt.setdefault("features", {})
        assess_changes_flag = os.getenv("ASSESS_CHANGES_FEATURE", "off")
        features.setdefault("assess_changes", assess_changes_flag)

        self.logger.info("Validation finished successfully")
        return evt

    # Optional alias if your SFN/mapping uses a different method name
    def validate_message(self):
        return self.validate_input()
