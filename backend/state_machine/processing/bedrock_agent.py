# Built-in imports
import os
import re
from functools import lru_cache
from typing import Optional, Tuple

import boto3
from botocore.exceptions import ClientError, EventStreamError

# Own imports
from common.logger import custom_logger


logger = custom_logger()

_DEFAULT_SESSION_ID = "default-session"
_MAX_SESSION_ID_LENGTH = 128
_SESSION_ID_PATTERN = re.compile(r"[^0-9a-zA-Z._:-]+")

_ENVIRONMENT_FALLBACK = "dev"
_AGENT_ID_ENV_VARS = ("BEDROCK_AGENT_ID", "AGENT_ID")
_AGENT_ALIAS_ENV_VARS = (
    "BEDROCK_AGENT_ALIAS_ID",
    "AGENT_ALIAS_ID",
    "BEDROCK_AGENT_ALIAS_ID_FULL_STRING",
    "AGENT_ALIAS_ID_FULL_STRING",
)


def _bedrock_client():
    return boto3.client("bedrock-agent-runtime")


def _ssm_client():
    return boto3.client("ssm")


def _environment_name() -> str:
    env_name = os.environ.get("ENVIRONMENT") or os.environ.get("ENV")
    if not env_name:
        logger.debug(
            "ENVIRONMENT not set; falling back to %s for parameter resolution",
            _ENVIRONMENT_FALLBACK,
        )
        env_name = _ENVIRONMENT_FALLBACK
    return env_name


def _parameter_name(suffix: str) -> str:
    return f"/{_environment_name()}/aws-wpp/{suffix}"


@lru_cache(maxsize=8)
def get_ssm_parameter(parameter_name: str) -> str:
    """Fetch a parameter from SSM Parameter Store (cached)."""

    response = _ssm_client().get_parameter(Name=parameter_name, WithDecryption=True)
    return response["Parameter"]["Value"]


def _first_defined(*candidates: Optional[str]) -> Optional[str]:
    for candidate in candidates:
        if candidate:
            stripped = candidate.strip()
            if stripped:
                return stripped
    return None


def _normalize_alias(alias_value: str) -> str:
    value = alias_value.strip()
    if "|" in value:
        value = value.split("|")[-1].strip()
    if value.startswith("arn:"):
        value = value.rsplit("/", 1)[-1]
    return value


@lru_cache(maxsize=1)
def _resolve_agent_configuration() -> Tuple[str, str]:
    """Return the agent and alias identifiers from env vars or SSM."""

    agent_id = _first_defined(*(os.environ.get(var) for var in _AGENT_ID_ENV_VARS))
    alias_value = _first_defined(
        *(os.environ.get(var) for var in _AGENT_ALIAS_ENV_VARS)
    )

    if agent_id is None:
        agent_id = get_ssm_parameter(_parameter_name("bedrock-agent-id")).strip()

    if alias_value is None:
        alias_value = get_ssm_parameter(
            _parameter_name("bedrock-agent-alias-id-full-string")
        ).strip()

    alias_id = _normalize_alias(alias_value)

    if not agent_id:
        raise RuntimeError("Unable to resolve Bedrock agent identifier")
    if not alias_id:
        raise RuntimeError("Unable to resolve Bedrock agent alias identifier")

    return agent_id, alias_id


def _sanitize_session_id(session_id: Optional[str]) -> str:
    """Ensure the supplied session identifier only uses supported characters."""

    if not session_id:
        return _DEFAULT_SESSION_ID

    sanitized = _SESSION_ID_PATTERN.sub("-", session_id)
    sanitized = sanitized.strip("-")
    if not sanitized:
        return _DEFAULT_SESSION_ID
    if len(sanitized) > _MAX_SESSION_ID_LENGTH:
        sanitized = sanitized[:_MAX_SESSION_ID_LENGTH]
    return sanitized


def call_bedrock_agent(input_text: str, session_id: Optional[str] = None) -> str:
    """Invoke the Bedrock agent and aggregate the streamed response text."""

    agent_id, agent_alias_id = _resolve_agent_configuration()

    resolved_session_id = _sanitize_session_id(session_id)
    if session_id and session_id != resolved_session_id:
        logger.debug(
            "Sanitized session identifier",
            extra={
                "original_session_id": session_id,
                "sanitized_session_id": resolved_session_id,
            },
        )

    logger.debug(
        {
            "message": "Invoking Bedrock agent",
            "agent_id": agent_id,
            "agent_alias_id": agent_alias_id,
            "session_id": resolved_session_id,
        }
    )

    bedrock_client = _bedrock_client()

    try:
        response = bedrock_client.invoke_agent(
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
    except bedrock_client.exceptions.AccessDeniedException as exc:  # type: ignore[attr-defined]
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

    return text_response


__all__ = ["call_bedrock_agent"]
