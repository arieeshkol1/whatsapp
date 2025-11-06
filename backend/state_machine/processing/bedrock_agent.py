# state_machine/processing/bedrock_agent.py
import os
import re
from functools import lru_cache
from typing import List, Optional, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from aws_lambda_powertools import Logger

logger = Logger(service="wpp-chatbot-sm-general")


def _runtime(region: str):
    return boto3.client("bedrock-agent-runtime", region_name=region)


def _ssm(region: str):
    return boto3.client("ssm", region_name=region)


def _safe_strip(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _resolve_environment() -> Optional[str]:
    """Best-effort environment resolution for Parameter Store lookups."""

    env = _safe_strip(os.environ.get("ENVIRONMENT"))
    if env:
        return env

    secret_name = _safe_strip(os.environ.get("SECRET_NAME"))
    if secret_name:
        trimmed = secret_name.strip("/")
        if trimmed:
            return _safe_strip(trimmed.split("/", 1)[0])

    return None


def _resolve_namespace() -> Optional[str]:
    """Infer the resource namespace ("aws-wpp") for SSM parameter construction."""

    override = _safe_strip(os.environ.get("BEDROCK_AGENT_PARAMETER_NAMESPACE"))
    if override:
        return override

    function_name = _safe_strip(os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))
    if function_name:
        parts = [segment for segment in function_name.split("-") if segment]
        if len(parts) >= 2:
            return "-".join(parts[:2])
        if parts:
            return parts[0]

    return "aws-wpp"


def _resolve_parameter_name(suffix: str, override_env_var: str) -> Optional[str]:
    """Return the fully-qualified parameter name for the supplied suffix."""

    override_name = _safe_strip(os.environ.get(override_env_var))
    if override_name:
        return override_name

    env = _resolve_environment()
    if not env:
        logger.debug("Unable to resolve ENVIRONMENT for Bedrock agent fallback")
        return None

    namespace = _resolve_namespace()
    if not namespace:
        logger.debug("Unable to resolve namespace for Bedrock agent fallback")
        return None

    return f"/{env}/{namespace}/{suffix}"


@lru_cache(maxsize=1)
def _agent_parameters_from_ssm(region: str) -> Tuple[Optional[str], Optional[str]]:
    """Fetch the agent ID and alias ID pair from SSM if present."""

    agent_id_parameter = _resolve_parameter_name(
        "bedrock-agent-id", "BEDROCK_AGENT_ID_PARAMETER_NAME"
    )
    alias_parameter = _resolve_parameter_name(
        "bedrock-agent-alias-id-full-string",
        "BEDROCK_AGENT_ALIAS_ID_PARAMETER_NAME",
    )

    if not agent_id_parameter and not alias_parameter:
        return None, None

    agent_id: Optional[str] = None
    agent_alias_id: Optional[str] = None

    try:
        if agent_id_parameter:
            resp = _ssm(region).get_parameter(
                Name=agent_id_parameter,
                WithDecryption=False,
            )
            agent_id = _safe_strip(resp["Parameter"]["Value"])
    except ClientError as exc:  # pragma: no cover - defensive guard
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code not in {"ParameterNotFound", "AccessDeniedException"}:
            logger.warning(
                "Unexpected error fetching bedrock-agent-id parameter", exc_info=exc
            )

    try:
        if alias_parameter:
            resp = _ssm(region).get_parameter(
                Name=alias_parameter,
                WithDecryption=False,
            )
            alias_value = _safe_strip(resp["Parameter"]["Value"])
            if alias_value and "|" in alias_value:
                maybe_agent, maybe_alias = alias_value.split("|", maxsplit=1)
                maybe_agent = _safe_strip(maybe_agent)
                maybe_alias = _safe_strip(maybe_alias)
                if maybe_agent and not agent_id:
                    agent_id = maybe_agent
                agent_alias_id = maybe_alias
            else:
                agent_alias_id = alias_value
    except ClientError as exc:  # pragma: no cover - defensive guard
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code not in {"ParameterNotFound", "AccessDeniedException"}:
            logger.warning(
                "Unexpected error fetching bedrock-agent-alias parameter", exc_info=exc
            )

    return agent_id, agent_alias_id


SESSION_ID_SANITIZER = re.compile(r"[^0-9A-Za-z._:-]+")


def _sanitize_session_id(session_id: str) -> str:
    """Ensure the supplied session identifier only uses Bedrock-supported characters."""

    sanitized = SESSION_ID_SANITIZER.sub("-", session_id)
    sanitized = sanitized.replace("|", "-")
    stripped = sanitized.strip("-")
    return stripped or "default-session"


def call_bedrock_agent(
    *ignored_args: object,
    region: Optional[str] = None,
    agent_id: Optional[str] = None,
    agent_alias_id: Optional[str] = None,
    session_id: str,
    input_text: str,
    enable_trace: bool = False,
) -> str:
    """
    Invoke the Bedrock Agent and return the concatenated streamed text response.
    - Reads AGENT_ID and AGENT_ALIAS_ID from env if not provided.
    - Falls back to SSM Parameter Store parameters when the environment variables are
      absent.
    - Accepts and ignores any positional arguments for backwards compatibility with
      older call sites that passed the event payload positionally.
    """
    if ignored_args:
        logger.debug(
            "call_bedrock_agent received unexpected positional args; ignoring",
            extra={"positional_arg_count": len(ignored_args)},
        )

    # Region: let Lambda supply it
    region = region or os.environ.get("AWS_REGION", "us-east-1")

    # Prefer function args, fall back to env
    agent_id = agent_id or os.environ.get("AGENT_ID")
    agent_alias_id = agent_alias_id or os.environ.get("AGENT_ALIAS_ID")

    if not agent_id or not agent_alias_id:
        cached_agent_id, cached_alias_id = _agent_parameters_from_ssm(region)
        agent_id = agent_id or cached_agent_id
        agent_alias_id = agent_alias_id or cached_alias_id

    if not agent_id:
        raise RuntimeError("AGENT_ID is not set (env or parameter missing)")
    if not agent_alias_id:
        raise RuntimeError("AGENT_ALIAS_ID is not set (env or parameter missing)")

    rt = _runtime(region)

    sanitized_session_id = _sanitize_session_id(session_id)

    try:
        resp = rt.invoke_agent(
            agentId=agent_id,
            agentAliasId=agent_alias_id,
            sessionId=sanitized_session_id,
            inputText=input_text,
            enableTrace=enable_trace,
        )
    except (BotoCoreError, ClientError) as exc:
        logger.exception("InvokeAgent failed")
        msg = getattr(exc, "response", {}).get("Error", {}).get("Message") or str(exc)
        raise RuntimeError(f"InvokeAgent failed: {msg}") from exc

    stream = resp.get("completion")
    if not stream:
        return ""

    chunks: List[str] = []
    try:
        for event in stream:
            if (chunk := event.get("chunk")) and (data := chunk.get("bytes")):
                try:
                    chunks.append(data.decode("utf-8", errors="replace"))
                except Exception:
                    chunks.append(str(data))
            elif (trace := event.get("trace")) is not None:
                logger.debug({"trace": trace})
            elif (message := event.get("message")) is not None:
                logger.debug({"message": message})
    except Exception as exc:
        logger.exception("Error while reading Agent event stream")
        raise RuntimeError("Error while reading Agent event stream") from exc

    return "".join(chunks).strip()
