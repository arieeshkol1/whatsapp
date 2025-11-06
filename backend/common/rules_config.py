"""Helpers for loading runtime conversation rules from DynamoDB."""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from common.logger import custom_logger

logger = custom_logger()


def _rules_table_name() -> Optional[str]:
    """Return the configured DynamoDB table name for rule storage."""

    table_name = os.environ.get("RULES_TABLE_NAME")
    if not table_name:
        logger.debug("RULES_TABLE_NAME environment variable is not set")
        return None
    return table_name


@lru_cache(maxsize=1)
def _rules_table() -> Optional[Any]:
    """Return a cached DynamoDB Table resource for rule lookups."""

    table_name = _rules_table_name()
    if not table_name:
        return None

    try:
        resource = boto3.resource("dynamodb")
        return resource.Table(table_name)
    except (BotoCoreError, ClientError) as exc:  # pragma: no cover - defensive
        logger.error(
            "Unable to initialise DynamoDB table for rules",
            extra={"table_name": table_name, "error": str(exc)},
        )
        return None


def _ruleset_partition_key(ruleset_id: Optional[str]) -> str:
    identifier = (ruleset_id or os.environ.get("RULESET_ID") or "default").strip()
    return f"RULESET#{identifier or 'default'}"


def _ruleset_sort_key(version: Optional[str]) -> str:
    resolved = (version or os.environ.get("RULESET_VERSION") or "CURRENT").strip()
    return f"VERSION#{resolved or 'CURRENT'}"


def reset_rules_cache() -> None:
    """Clear cached resources to support unit tests."""

    _rules_table.cache_clear()
    _load_rules_document.cache_clear()


@lru_cache(maxsize=32)
def _load_rules_document(
    ruleset_id: Optional[str] = None, version: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Fetch the persisted rule document for the supplied identifiers."""

    table = _rules_table()
    if not table:
        return None

    pk = _ruleset_partition_key(ruleset_id)
    sk = _ruleset_sort_key(version)

    try:
        response = table.get_item(Key={"PK": pk, "SK": sk})
    except (BotoCoreError, ClientError) as exc:
        logger.error(
            "Failed to load rule document",
            extra={"table_name": table.name, "pk": pk, "sk": sk, "error": str(exc)},
        )
        return None

    item = response.get("Item") if isinstance(response, dict) else None
    if not isinstance(item, dict):
        return None

    return item


def get_rules_text(
    ruleset_id: Optional[str] = None, version: Optional[str] = None
) -> Optional[str]:
    """Return a newline-joined block describing the active rule set."""

    document = _load_rules_document(ruleset_id, version)
    if not document:
        return None

    # Prefer explicit text payloads first
    explicit_text = document.get("instruction_text") or document.get(
        "instructions_text"
    )
    if isinstance(explicit_text, str) and explicit_text.strip():
        return explicit_text.strip()

    instructions = document.get("instructions") or document.get("rules")
    if isinstance(instructions, list):
        joined = "\n".join(
            str(item).strip() for item in instructions if str(item).strip()
        )
        return joined or None

    if isinstance(instructions, str) and instructions.strip():
        return instructions.strip()

    return None
