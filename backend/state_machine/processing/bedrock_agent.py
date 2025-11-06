# Built-in imports
import os
from typing import Optional

import boto3
from botocore.exceptions import ClientError, EventStreamError

# Own imports
from common.logger import custom_logger


ENVIRONMENT = os.environ.get("ENVIRONMENT")

logger = custom_logger()

# Create a bedrock runtime client
bedrock_agent_runtime_client = boto3.client("bedrock-agent-runtime")
ssm_client = boto3.client("ssm")


def get_ssm_parameter(parameter_name):
    """
    Fetches the parameter value from SSM Parameter Store.
    """
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    return response["Parameter"]["Value"]


def call_bedrock_agent(input_text: str, session_id: Optional[str] = None) -> str:
    """Invoke the Bedrock agent and aggregate the streamed response text."""

    # TODO: Update to use PowerTools SSM Params for optimization
    agent_alias_param = get_ssm_parameter(
        f"/{ENVIRONMENT}/aws-wpp/bedrock-agent-alias-id-full-string"
    ).strip()
    agent_alias_id = agent_alias_param.split("|")[-1]
    agent_id = get_ssm_parameter(f"/{ENVIRONMENT}/aws-wpp/bedrock-agent-id").strip()

    resolved_session_id = session_id or "TempSessionBedrock"

    logger.debug(
        {
            "message": "Invoking Bedrock agent",
            "agent_id": agent_id,
            "agent_alias_id": agent_alias_id,
            "session_id": resolved_session_id,
        }
    )

    try:
        response = bedrock_agent_runtime_client.invoke_agent(
            agentAliasId=agent_alias_id,
            agentId=agent_id,
            enableTrace=False,
            inputText=input_text,
            sessionId=resolved_session_id,
        )

        stream = response.get("completion") or []
        text_response = ""
        for event in stream:
            if "chunk" in event and event["chunk"].get("bytes"):
                text_response += event["chunk"]["bytes"].decode()
            elif event.get("error"):
                error = event["error"]
                logger.error(
                    {
                        "message": "Bedrock agent returned an error",
                        "error": error,
                        "session_id": resolved_session_id,
                    }
                )
                raise RuntimeError(f"Bedrock agent invocation failed: {error}")
            elif event.get("returnControl"):
                logger.debug(
                    {
                        "message": "Bedrock agent returned control",
                        "session_id": resolved_session_id,
                    }
                )
    except bedrock_agent_runtime_client.exceptions.AccessDeniedException as exc:
        logger.error(
            {
                "message": "Access denied when invoking Bedrock agent",
                "session_id": resolved_session_id,
                "error": str(exc),
            }
        )
        raise PermissionError("Access denied when invoking Bedrock agent") from exc
    except EventStreamError as exc:
        parsed_response = getattr(exc, "parsed_response", {}) or {}
        error_type = parsed_response.get("errorType")
        error_message = parsed_response.get("errorMessage") or str(exc)
        logger.error(
            {
                "message": "Error received from Bedrock event stream",
                "session_id": resolved_session_id,
                "error_type": error_type,
                "error_message": error_message,
            }
        )
        if error_type and "accessdenied" in error_type.lower():
            raise PermissionError(error_message) from exc
        raise RuntimeError(error_message) from exc
    except ClientError as exc:
        logger.error(
            {
                "message": "Client error when invoking Bedrock agent",
                "session_id": resolved_session_id,
                "error": exc.response.get("Error", {}),
            }
        )
        raise RuntimeError("Client error when invoking Bedrock agent") from exc

    logger.info(text_response)

    # TODO: Add better error handling and validations/checks

    return text_response
