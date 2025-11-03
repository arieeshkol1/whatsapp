# Own imports
from state_machine.base_step_function import BaseStepFunction
from common.logger import custom_logger

from state_machine.processing.bedrock_agent import call_bedrock_agent


logger = custom_logger()


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
            .get("S", "DEFAULT_RESPONSE")
        )

        # TODO: Update "acnowledged" message to a more complex response
        # TODO: Add more complex "text processing" logic here with memory and sessions...
        self.response_message = call_bedrock_agent(
            session_id=self.correlation_id,
            input_text=self.text,
        )

        self.logger.info(f"Generated response message: {self.response_message}")
        self.logger.info("Validation finished successfully")

        self.event["response_message"] = self.response_message

        return self.event
