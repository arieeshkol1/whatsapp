from state_machine.base_step_function import BaseStepFunction
from common.logger import custom_logger

logger = custom_logger()


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
        dd = evt.get("input", {}).get("dynamodb", {})

        new_image = dd.get("NewImage", {})
        if not new_image:
            raise ValueError("DynamoDB NewImage is missing in event.input.dynamodb")

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
                raise ValueError("NewImage.conversation_id.N must be numeric") from exc

        # Required checks (tighten/relax to your needs)
        self._require(from_number, "NewImage.from_number.S is required")
        self._require(msg_type, "NewImage.type.S is required")
        self._require(whatsapp_id, "NewImage.whatsapp_id.S is required")

        # For text messages, ensure text exists
        if msg_type == "text":
            self._require(text, "NewImage.text.S is required for text messages")

        # Log a safe preview
        self.logger.info(
            "Validated message",
            extra={
                "from_number_masked": (from_number[:4] + "***" + from_number[-2:])
                if from_number
                else "<none>",
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

        self.logger.info("Validation finished successfully")
        return evt

    # Optional alias if your SFN/mapping uses a different method name
    def validate_message(self):
        return self.validate_input()
