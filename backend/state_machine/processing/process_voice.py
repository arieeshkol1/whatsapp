# Built-in imports
from datetime import datetime

# Own imports
from state_machine.base_step_function import BaseStepFunction
from common.enums import WhatsAppMessageTypes
from common.logger import custom_logger

from state_machine.processing.bedrock_agent import call_bedrock_agent


logger = custom_logger()
ALLOWED_MESSAGE_TYPES = WhatsAppMessageTypes.__members__


class ProcessVoice(BaseStepFunction):
    """
    This class contains methods that serve as the "voice processing" for the State Machine.
    """

    def __init__(self, event):
        super().__init__(event, logger=logger)

    def process_voice(self):
        """
        Method to process the voice input message and convert it to text.
        """

        self.logger.info("Starting process_voice for the chatbot")

        # TODO: Add real voice to text conversion here...

        self.response_message = "אני עדיין לא יכול לעבד הודעות קוליות. אנא שלחו הודעת טקסט בעברית כדי שאוכל לעזור."

        self.logger.info(
            "Generated response message for voice input",
            extra={"response": self.response_message},
        )

        self.event["response_message"] = self.response_message

        return self.event
