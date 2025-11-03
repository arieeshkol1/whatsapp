# state_machine/processing/bedrock_agent.py
import os
from typing import List, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from aws_lambda_powertools import Logger

logger = Logger(service="wpp-chatbot-sm-general")


def _runtime(region: str):
    return boto3.client("bedrock-agent-runtime", region_name=region)


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
    - Does NOT use SSM Parameter Store.
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
