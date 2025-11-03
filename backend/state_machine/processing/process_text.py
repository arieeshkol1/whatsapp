# Built-in imports
from datetime import datetime
import re

# Own imports
from state_machine.base_step_function import BaseStepFunction
from common.enums import WhatsAppMessageTypes
from common.logger import custom_logger

from state_machine.processing.bedrock_agent import call_bedrock_agent


logger = custom_logger()
ALLOWED_MESSAGE_TYPES = WhatsAppMessageTypes.__members__

HEBREW_PATTERN = re.compile(r"[א-ת]")
HEBREW_INPUT_REQUIRED_MESSAGE = "אנא שלחו את בקשתכם בעברית כדי שאוכל לסייע."
HEBREW_OUTPUT_FALLBACK_MESSAGE = "סליחה, לא הצלחתי לעבד את הבקשה. אנא נסחו אותה שוב בעברית."  # pragma: allowlist secret


class ProcessText(BaseStepFunction):
    """
    This class contains methods that serve as the "text processing" for the State Machine.
    """

    def __init__(self, event):
        super().__init__(event, logger=logger)

    def process_text(self):
        """
        Method to validate the input message and process the expected text response.
        """

        self.logger.info("Starting process_text for the chatbot")

        # TODO: Add more robust "text processing" logic here (actual response)
        self.text = (
            self.event.get("input", {})
            .get("dynamodb", {})
            .get("NewImage", {})
            .get("text", {})
            .get("S", "")
        ).strip()

        if not self.text or not HEBREW_PATTERN.search(self.text):
            self.logger.info(
                "Incoming message is missing Hebrew characters; sending guidance",
                extra={"sample": self.text[:30]},
            )
            self.response_message = HEBREW_INPUT_REQUIRED_MESSAGE
        else:
            augmented_prompt = (
                f"הודעת לקוח: {self.text}\n\n"
                "אנא הגב בעברית בלבד, בסגנון חם ומזמין של הסומלייה הדיגיטלית של האוויטוש."
            )
            self.response_message = call_bedrock_agent(
                session_id=self.correlation_id,
                input_text=augmented_prompt,
            )

            if not self.response_message or not HEBREW_PATTERN.search(
                self.response_message
            ):
                self.logger.warning(
                    "Agent response missing Hebrew characters; using fallback",
                    extra={"response_preview": (self.response_message or "")[:50]},
                )
                self.response_message = HEBREW_OUTPUT_FALLBACK_MESSAGE

        self.logger.info(f"Generated response message: {self.response_message}")
        self.logger.info("Validation finished successfully")

        self.event["response_message"] = self.response_message

        return self.event
