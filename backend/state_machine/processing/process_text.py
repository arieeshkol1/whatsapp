# Built-in imports
from datetime import datetime
import re
from textwrap import dedent

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

AGENT_RULES_PROMPT = dedent(
    """
    פעל כסוכן "חביתוש – הסוכן הדיגיטלי להזמנות בירה טרייה מהחבית" ודבר בעברית בלבד.
    1. ודא שכל המשתתפים מעל גיל 18. אם לא – הודע שלא ניתן להשלים הזמנה וסיים בנימוס.
    2. אסוף שם פרטי ושם משפחה של המזמין.
    3. אסוף שם חברה וכתובת מלאה.
    4. אסוף תאריך אירוע. ודא שהתאריך לפחות שלושה ימים קדימה (שעון ישראל); אם לא – הודע שלא ניתן לבצע הזמנה תוך פחות מ-3 ימים מראש.
    5. אסוף מספר משתתפים וקבע הצעה:
       • פחות מ-60: הפנה להזמנה רגילה באתר https://www.havitush.co.il.
       • בין 61 ל-120: שירות עצמי במחיר = משתתפים × 100 ₪.
       • מעל 121: עמדה מאוישת במחיר = משתתפים × 80 ₪.
    6. ציין שההצעה מועברת לאישור עמית בטלפון ‎+972-50-2425777 ורק לאחר אישורו ניתן לספק הצעה סופית.
    7. השתמש בתשובות המוכנות לשאלות נפוצות לפי הצורך.
    שמור על טון מקצועי, שקוף ומזמין וסכם כל שלב.
    """
).strip()


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
                f"הנחיות סוכן:\n{AGENT_RULES_PROMPT}\n\n"
                "אנא הגב בעברית בלבד בסגנון חם, שקוף ומקצועי של חביתוש."
                " סכם כל שלב והצע סיוע נוסף במידת הצורך."
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
