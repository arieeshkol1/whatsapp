from state_machine.base_step_function import BaseStepFunction
from common.logger import custom_logger
from state_machine.processing.bedrock_agent import call_bedrock_agent  # noqa: F401 (placeholder)

logger = custom_logger()


class ProcessVoice(BaseStepFunction):
    """Voice processing placeholder for the State Machine."""

    def __init__(self, event):
        super().__init__(event, logger=logger)

    def process_voice(self):
        """Convert voice to text (placeholder) and return a response."""
        self.logger.info("Starting process_voice for the chatbot")

        # TODO: implement voice-to-text
        response_text = (
            "NOT IMPLEMENTED. PLEASE ANSWER: "
            "<I am not able to process voice messages yet>."
        )

        self.logger.info(f"Generated response message: {response_text}")
        self.event["response_message"] = response_text
        return self.event
