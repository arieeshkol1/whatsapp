# Built-in imports
import os
from typing import Optional

import boto3
from botocore.exceptions import ClientError, EventStreamError

# Own imports
from common.logger import custom_logger


ENVIRONMENT = os.environ.get("ENVIRONMENT")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

logger = custom_logger()

# Create AWS clients using the Lambda-provided region
bedrock_agent_runtime_client = boto3.client(
    "bedrock-agent-runtime", region_name=AWS_REGION
)
ssm_client = boto3.client("ssm", region_name=AWS_REGION)


def get_ssm_parameter(parameter_name):
    """
    Fetches the parameter value from SSM Parameter Store.
    """
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    return response["Parameter"]["Value"]


FALLBACK_RESPONSE = (
    "We couldn't complete your request right now. Please try again in a moment."
)


def _resolve_agent_configuration() -> tuple[str, str]:
    """Return the Bedrock agent and alias IDs using env vars or SSM as a fallback."""

    agent_id = os.environ.get("AGENT_ID")
    agent_alias_id = os.environ.get("AGENT_ALIAS_ID")

    if agent_id and agent_alias_id:
        return agent_id, agent_alias_id

    # TODO: Update to use PowerTools SSM Params for optimization
    agent_alias_param = get_ssm_parameter(
        f"/{ENVIRONMENT}/aws-wpp/bedrock-agent-alias-id-full-string"
    ).strip()
    resolved_alias_id = agent_alias_param.split("|")[-1]
    resolved_agent_id = get_ssm_parameter(
        f"/{ENVIRONMENT}/aws-wpp/bedrock-agent-id"
    ).strip()

    return resolved_agent_id, resolved_alias_id


def call_bedrock_agent(
    *args, session_id: Optional[str] = None, input_text: Optional[str] = None
) -> str:
    """Invoke the Bedrock agent and aggregate the streamed response text.

    Historically ``input_text`` was the first positional argument. The
    signature now allows both the legacy positional call and the preferred
    keyword usage while guarding against ambiguous invocations.
    """

    if args and input_text is not None:
        raise TypeError("Provide input_text either positionally or as a keyword, not both")

    if len(args) > 1:
        raise TypeError("call_bedrock_agent accepts a single positional argument")

    resolved_input_text: Optional[str] = input_text
    if args:
        resolved_input_text = args[0]

    if resolved_input_text is None:
        raise ValueError("input_text is required to invoke the Bedrock agent")

    agent_id, agent_alias_id = _resolve_agent_configuration()

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
            inputText=resolved_input_text,
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
        logger.info(
            {
                "message": "Returning fallback response after access denial",
                "session_id": resolved_session_id,
            }
        )
        return FALLBACK_RESPONSE
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
            logger.info(
                {
                    "message": "Returning fallback response after event stream access denial",
                    "session_id": resolved_session_id,
                }
            )
            return FALLBACK_RESPONSE
        raise RuntimeError(error_message) from exc
    except ClientError as exc:
        error_details = exc.response.get("Error", {})
        logger.error(
            {
                "message": "Client error when invoking Bedrock agent",
                "session_id": resolved_session_id,
                "error": error_details,
            }
        )
        error_code = (error_details.get("Code") or "").lower()
        if "accessdenied" in error_code:
            logger.info(
                {
                    "message": "Returning fallback response after client access denial",
                    "session_id": resolved_session_id,
                }
            )
            return FALLBACK_RESPONSE
        raise RuntimeError("Client error when invoking Bedrock agent") from exc

    logger.info(text_response)

    # TODO: Add better error handling and validations/checks

    return text_response
