# Own imports
<<<<<<< HEAD
from ..base_step_function import BaseStepFunction
from common.enums import WhatsAppMessageTypes
from common.logger import custom_logger

from .bedrock_agent import call_bedrock_agent

=======
from state_machine.base_step_function import BaseStepFunction
from common.logger import custom_logger

>>>>>>> origin/main

logger = custom_logger()


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

        response_text = (
            "NOT IMPLEMENTED. PLEASE ANSWER: "
            "<I am not able to process voice messages yet>."
        )

        self.logger.info(f"Generated response message: {response_text}")

        self.event["response_message"] = response_text

        return self.event
