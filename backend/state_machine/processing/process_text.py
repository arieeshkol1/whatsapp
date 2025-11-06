# Built-in imports
from datetime import datetime

# Own imports
from state_machine.base_step_function import BaseStepFunction
from common.enums import WhatsAppMessageTypes
from common.logger import custom_logger

from state_machine.processing.customer_flow import ConversationFlow


logger = custom_logger()
ALLOWED_MESSAGE_TYPES = WhatsAppMessageTypes.__members__


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

        record = self.event.get("input", {}).get("dynamodb", {}).get("NewImage", {})

        self.text = record.get("text", {}).get("S", "DEFAULT_RESPONSE")
        phone_number = record.get("from_number", {}).get("S", "unknown")

        conversation = ConversationFlow(phone_number)
        self.response_message = conversation.handle(self.text)

        self.logger.info(f"Generated response message: {self.response_message}")
        self.logger.info("Validation finished successfully")

        self.event["response_message"] = self.response_message

        return self.event
