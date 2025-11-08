# Built-in imports
import json
import os
import re
import time
from decimal import Decimal
from typing import Any, Optional

import boto3

# External imports
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.data_classes.dynamo_db_stream_event import (
    DynamoDBRecord,
)

# Own imports
from common.logger import custom_logger

LOGGER = custom_logger()

step_function_client = boto3.client("stepfunctions")
ENABLE_STREAM_TRIGGER = os.environ.get("ENABLE_STREAM_TRIGGER", "off").lower()


def _json_default(value):
    """Serialize Decimal values so the Step Functions payload can be encoded."""

    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")


def _extract_attribute_value(attribute: Optional[Any]) -> str:
    """Return the raw value from a DynamoDB attribute wrapper."""

    if attribute is None:
        return "NOT_FOUND"

    value = getattr(attribute, "value", attribute)
    if value is None:
        return "NOT_FOUND"

    return str(value)


def _sanitize_execution_component(component: str, fallback: str = "NOT_FOUND") -> str:
    """Sanitize a string so it can be used inside a Step Functions execution name."""

    if not component:
        return fallback

    sanitized = re.sub(r"[^0-9A-Za-z_-]", "_", component)
    sanitized = sanitized.strip("_")

    if not sanitized:
        return fallback

    return sanitized[:40]


def trigger_sm(record: DynamoDBRecord, logger: Logger = None) -> str:
    """
    Handler for triggering the Step Function's execution.

    Args:
        record (DynamoDBRecord): Event from from DynamoDB Stream Record.
        logger (Logger, optional): Logger object. Defaults to None.

    Returns:
        None
    """
    try:
        logger = logger or LOGGER
        log_message = {
            "METHOD": "trigger_sm",
        }
        if ENABLE_STREAM_TRIGGER != "on":
            logger.info("Stream trigger disabled; skipping state machine start")
            return ""

        state_machine_arn = os.environ.get("STATE_MACHINE_ARN", "")

        log_message["MESSAGE"] = f"triggering state machine {state_machine_arn}"
        log_message["RECORD"] = record.raw_event

        # Extract the necessary information from the DynamoDB Stream Record for Execution Name
        from_message_attr = record.dynamodb.new_image.get("from_number")
        correlation_id_attr = record.dynamodb.new_image.get("correlation_id")

        from_message = _sanitize_execution_component(
            _extract_attribute_value(from_message_attr)
        )
        correlation_id = _sanitize_execution_component(
            _extract_attribute_value(correlation_id_attr)
        )

        exec_name = f"{time.strftime('%Y%m%dT%H%M%S')}_{from_message}_{correlation_id}"

        logger.append_keys(correlation_id=correlation_id)
        logger.debug(log_message)

        # Generate state machine input event with the same DynamoDBRecord dict
        state_machine_input = {"input": record.raw_event}

        logger.debug(state_machine_input, message_details="State Machine Input")

        response = step_function_client.start_execution(
            stateMachineArn=state_machine_arn,
            input=json.dumps(state_machine_input, default=_json_default),
            name=exec_name,
        )
        return response.get("executionArn")
    except Exception as err:
        log_message["EXCEPTION"] = str(err)
        logger.error(str(log_message))
        raise
