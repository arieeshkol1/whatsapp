# state_machine/processing/bedrock_agent.py
import os
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


@lru_cache(maxsize=1)
def _agent_parameters_from_ssm(region: str) -> Tuple[Optional[str], Optional[str]]:
    """Fetch the agent ID and alias ID pair from SSM if present."""

    env = os.environ.get("ENVIRONMENT")
    if not env:
        return None, None

    base_path = f"/{env}/aws-wpp"
    agent_id: Optional[str] = None
    agent_alias_id: Optional[str] = None

    try:
        resp = _ssm(region).get_parameter(
            Name=f"{base_path}/bedrock-agent-id",
            WithDecryption=False,
        )
        agent_id = resp["Parameter"]["Value"].strip() or None
    except ClientError as exc:  # pragma: no cover - defensive guard
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code not in {"ParameterNotFound", "AccessDeniedException"}:
            logger.warning(
                "Unexpected error fetching bedrock-agent-id parameter", exc_info=exc
            )

    try:
        resp = _ssm(region).get_parameter(
            Name=f"{base_path}/bedrock-agent-alias-id-full-string",
            WithDecryption=False,
        )
        alias_value = resp["Parameter"]["Value"].strip()
        if "|" in alias_value:
            maybe_agent, maybe_alias = alias_value.split("|", maxsplit=1)
            if maybe_agent and not agent_id:
                agent_id = maybe_agent
            agent_alias_id = maybe_alias or None
        else:
            agent_alias_id = alias_value or None
    except ClientError as exc:  # pragma: no cover - defensive guard
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code not in {"ParameterNotFound", "AccessDeniedException"}:
            logger.warning(
                "Unexpected error fetching bedrock-agent-alias parameter", exc_info=exc
            )

    return agent_id, agent_alias_id


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

    try:
        resp = rt.invoke_agent(
            agentId=agent_id,
            agentAliasId=agent_alias_id,
            sessionId=session_id,
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
