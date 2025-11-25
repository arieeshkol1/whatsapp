# Built-in imports
import json
import os
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Annotated, Dict, Any, Optional
from uuid import uuid4

# External imports
from fastapi import APIRouter, Header, Query, Request, Response, status
import boto3

# Own imports
from common.models.text_message_model import TextMessageModel
from common.logger import custom_logger
from common.helpers.dynamodb_helper import DynamoDBHelper
from common.helpers.secrets_helper import SecretsHelper

# Initialize Secrets Manager Helper
DEFAULT_SECRET_NAME = "/dev/aws-whatsapp-chatbot"
SECRET_NAME = os.environ.get("SECRET_NAME", DEFAULT_SECRET_NAME)
secrets_helper = SecretsHelper(SECRET_NAME)

# Initialize DynamoDB Helper
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]
ENDPOINT_URL = os.environ.get("ENDPOINT_URL")  # Used for local testing
CONVERSATION_TIMEOUT_MINUTES = int(
    os.environ.get("CONVERSATION_TIMEOUT_MINUTES", "180")
)
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN")
ASSESS_CHANGES_FEATURE = os.environ.get("ASSESS_CHANGES_FEATURE", "off")

stepfunctions_client = boto3.client("stepfunctions")
dynamodb_helper = DynamoDBHelper(table_name=DYNAMODB_TABLE, endpoint_url=ENDPOINT_URL)


router = APIRouter()
logger = custom_logger()


def _normalize_phone_number(phone_number: Optional[str]) -> str:
    if phone_number is None:
        return ""

    digits = "".join(ch for ch in str(phone_number) if ch.isdigit())
    return digits or str(phone_number)


@router.get("/webhook", tags=["Chatbot"])
async def get_chatbot_webhook(
    hub_challenge_query_param: str = Query(..., alias="hub.challenge"),
    hub_verify_token_query_param: str = Query(..., alias="hub.verify_token"),
):
    try:
        correlation_id = str(uuid4())
        logger.append_keys(correlation_id=correlation_id)
        logger.info("Started chatbot handler for get_chatbot_webhook()")
        logger.info("Finished get_chatbot_webhook() successfully")

        # TODO: Remove these logs after initial validations
        logger.debug(f"hub_challenge_query_param: {hub_challenge_query_param}")
        logger.debug(f"hub_verify_token_query_param: {hub_verify_token_query_param}")

        # TODO: MIGRATE TOKEN VALIDATION TO DEDICATED AUTHORIZER!!!
        AWS_API_KEY_TOKEN = secrets_helper.get_secret_value("AWS_API_KEY_TOKEN")
        if hub_verify_token_query_param == AWS_API_KEY_TOKEN:
            return Response(
                content=hub_challenge_query_param,
                status_code=status.HTTP_200_OK,
                headers={"Content-Type": "text/html;charset=UTF-8"},
            )

        return {"error": "Invalid authorization or authentication"}

    except Exception as e:
        logger.error(f"Error in get_chatbot_webhook(): {e}")
        raise e


@router.post("/webhook", tags=["Chatbot"])
async def post_chatbot_webhook(
    request: Request,  # Only for initial debugging purposes
    input_body: dict,
):
    try:
        correlation_id = str(uuid4())
        logger.append_keys(correlation_id=correlation_id)
        logger.info(
            input_body, message_details="Received body in post_chatbot_webhook()"
        )
        logger.info("Started chatbot handler for post_chatbot_webhook()")
        logger.info("Finished post_chatbot_webhook() successfully")

        # TODO: Remove these logs after initial validations
        logger.debug(f"HEADERS: {request.headers}")
        logger.debug(f"QUERY_PARAMS: {request.query_params}")
        logger.debug(f"PATH_PARAMS: {request.path_params}")
        logger.debug(f"INPUT_BODY: {input_body}")

        value_payload = _extract_value_payload(input_body)
        message = _extract_first_message(value_payload)
        if message is None:
            statuses = value_payload.get("statuses") or []
            if statuses:
                logger.info(
                    statuses,
                    message_details="Received WhatsApp status notification",
                )
            else:
                logger.warning(
                    value_payload,
                    message_details="Webhook payload did not include messages",
                )
            return {"message": "ok", "details": "Status notification ignored"}

        wpp_from_phone_number = message["from"]
        wpp_id = message["id"]
        wpp_timestamp = message["timestamp"]
        wpp_type = message["type"]
        created_at_dt = datetime.now().astimezone()
        created_at = created_at_dt.isoformat()

        # Initialize the Message Model based on the type of message
        message_item = None
        normalized_from_number = _normalize_phone_number(wpp_from_phone_number)

        if wpp_type == "text":
            to_number = metadata.get("display_phone_number") if metadata else None
            normalized_to_number = _normalize_phone_number(to_number)

            if not normalized_to_number:
                logger.error("Missing destination phone number in webhook payload")
                return {
                    "error": "Destination phone number not provided",
                    "details": "Unable to persist interaction without business number",
                }

            conversation_id = _determine_conversation_id(
                normalized_to_number,
                normalized_from_number,
                created_at_dt,
            )

            message_item = TextMessageModel(
                PK=normalized_to_number,
                SK=created_at,
                to_number=normalized_to_number,
                from_number=wpp_from_phone_number,
                timestamp=created_at,
                type=wpp_type,
                user_message=message["text"]["body"],
                correlation_id=correlation_id,
                conversation_id=conversation_id,
            )
            logger.info(
                message_item.model_dump(),
                message_details="Successfully created TextMessageModel instance",
            )
        # TODO: Add other types of messages (image, voice, video, etc)

        # Save the message to DynamoDB
        if message_item:
            result = dynamodb_helper.put_item(
                message_item.model_dump(exclude_none=True)
            )
            logger.debug(result, message_details="DynamoDB put_item() result")

            metadata = (
                value_payload.get("metadata", {})
                if isinstance(value_payload, dict)
                else {}
            )
            try:
                state_machine_event = _build_state_machine_event(
                    message,
                    metadata,
                    conversation_id,
                    correlation_id,
                )
                _start_state_machine(state_machine_event)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("Failed to start message processing state machine")

        result = {"message": "ok", "details": "Received message"}
        return result

    except Exception as e:
        logger.error(f"Error in post_chatbot_webhook(): {e}")
        raise e


def _extract_conversation_id_value(raw_value) -> int:
    if raw_value is None:
        return 0
    if isinstance(raw_value, Decimal):
        return int(raw_value)
    if isinstance(raw_value, (int, float)):
        return int(raw_value)
    if isinstance(raw_value, str) and raw_value.isdigit():
        return int(raw_value)
    return 0


def _determine_conversation_id(
    to_number: str, from_number: str, created_at: datetime
) -> int:
    """Determine the conversation id for the incoming message."""

    if not to_number or not from_number:
        return 1

    partition_keys = [to_number, f"NUMBER#{to_number}"]
    latest_item = dynamodb_helper.get_latest_item_by_pk_and_from(
        partition_keys, from_number
    )
    if not latest_item:
        return 1

    last_conversation = _extract_conversation_id_value(
        latest_item.get("conversation_id")
    )
    if last_conversation < 1:
        last_conversation = 1

    last_timestamp_raw = latest_item.get("timestamp")
    last_created_at: datetime | None = None
    if isinstance(last_timestamp_raw, str):
        try:
            last_created_at = datetime.fromisoformat(last_timestamp_raw)
        except ValueError:
            last_created_at = None

    if last_created_at is None:
        return last_conversation

    time_delta = created_at - last_created_at
    if time_delta <= timedelta(minutes=CONVERSATION_TIMEOUT_MINUTES):
        return last_conversation

    return last_conversation + 1


def _extract_value_payload(input_body: dict) -> dict:
    """Safely extract the WhatsApp value payload from the webhook body."""

    entries = input_body.get("entry") or []
    for entry in entries:
        changes = entry.get("changes") or []
        for change in changes:
            value = change.get("value")
            if value:
                return value
    return {}


def _extract_first_message(value_payload: dict) -> dict | None:
    """Return the first WhatsApp message within the value payload."""

    messages = value_payload.get("messages") or []
    if not messages:
        return None
    return messages[0]


def _sanitize_execution_component(component: Optional[str]) -> str:
    if not component:
        return ""

    sanitized = re.sub(r"[^0-9A-Za-z_-]", "_", component)
    sanitized = sanitized.strip("_")
    return sanitized[:60]


def _build_state_machine_event(
    message: Dict[str, Any],
    metadata: Dict[str, Any],
    conversation_id: int,
    correlation_id: str,
) -> Dict[str, Any]:
    message_type = message.get("type")
    message_body = (
        message.get("text", {}).get("body") if message_type == "text" else None
    )

    payload: Dict[str, Any] = {
        "from": message.get("from"),
        "to": metadata.get("display_phone_number"),
        "message_type": message_type or ("text" if message_body else None),
        "message_body": message_body,
        "wa_id": message.get("id"),
        "last_seen_at": message.get("timestamp"),
        "message_id": message.get("id"),
        "profile_name": message.get("profile", {}).get("name"),
        "phone_number_id": metadata.get("phone_number_id"),
        "channel": "whatsapp",
        "correlation_id": correlation_id,
        "conversation_id": conversation_id,
        "message_timestamp": created_at,
    }

    if message_type == "image":
        payload["image_url"] = message.get("image", {}).get("link")
    elif message_type == "video":
        payload["video_url"] = message.get("video", {}).get("link")
    elif message_type == "voice":
        payload["voice_url"] = message.get("voice", {}).get("link")
    elif message_type == "interactive":
        payload["interactive_payload"] = message.get("interactive")

    clean_payload = {k: v for k, v in payload.items() if v not in (None, "")}

    event: Dict[str, Any] = {
        "input": clean_payload,
        "conversation_id": conversation_id,
        "correlation_id": correlation_id,
    }

    if metadata:
        event["metadata"] = metadata

    features = {}
    if ASSESS_CHANGES_FEATURE:
        features["assess_changes"] = ASSESS_CHANGES_FEATURE
    if features:
        event["features"] = features

    return event


def _start_state_machine(event_payload: Dict[str, Any]) -> None:
    if not STATE_MACHINE_ARN:
        logger.warning("STATE_MACHINE_ARN not configured; skipping state machine start")
        return

    input_payload = json.dumps(event_payload)

    name_parts = [
        _sanitize_execution_component(event_payload.get("input", {}).get("from")),
        _sanitize_execution_component(event_payload.get("input", {}).get("message_id")),
    ]
    name_parts = [part for part in name_parts if part]

    start_kwargs: Dict[str, Any] = {
        "stateMachineArn": STATE_MACHINE_ARN,
        "input": input_payload,
    }
    if name_parts:
        start_kwargs["name"] = "-".join(name_parts)[:80]

    stepfunctions_client.start_execution(**start_kwargs)
