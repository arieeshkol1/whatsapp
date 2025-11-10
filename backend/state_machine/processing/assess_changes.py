"""Placeholder module for Bedrock-driven change assessments."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

from common.logger import custom_logger


USER_INFO_TABLE_NAME = "UserInfo"  # <-- adjust if your env/stack uses a different name


class AssessChanges:
    """Collects and evaluates proposed updates via Bedrock (feature gated)."""

    def __init__(self, event: Dict[str, Any] | None = None) -> None:
        self.event: Dict[str, Any] = event or {}
        self.logger = custom_logger()
        self._dynamodb = None
        self._user_info_table = None

    # ---------------------------
    # Public API (invoked by Step Functions/Lambda)
    # ---------------------------
    def assess_and_apply(self) -> Dict[str, Any]:
        """
        Enrich the event with UserInfo.Name (if present) so the next step (Process Text)
        can leverage it. Returns the original event if the table/item/attribute is missing.
        """
        phone = self._extract_phone_number(self.event)
        if not phone:
            self.logger.warning("AssessChanges: could not extract phone number from event; returning unchanged.")
            return self.event

        try:
            item = self._get_user_info_item(phone)
        except Exception as exc:  # be resilient; never break the flow
            self.logger.exception("AssessChanges: failed to read UserInfo for phone=%s: %s", phone, exc)
            return self.event

        if not item:
            self.logger.info("AssessChanges: no UserInfo record for phone=%s; returning unchanged.", phone)
            return self.event

        name_payload = self._parse_name_attribute(item.get("Name"))
        if name_payload is None:
            self.logger.info("AssessChanges: UserInfo.Name missing/empty for phone=%s; returning unchanged.", phone)
            return self.event

        # Inject into the event passed to the next step.
        # Structured location:
        self.event.setdefault("user_info", {})["name"] = name_payload
        # Flat alias (handy for templates / downstream access):
        self.event["user_info_name"] = name_payload

        self.logger.debug("AssessChanges: injected user_info.name for phone=%s", phone)
        return self.event

    # ---------------------------
    # Internals
    # ---------------------------
    def _extract_phone_number(self, event: Dict[str, Any]) -> Optional[str]:
        """
        Try several common shapes seen in this project/state machine to obtain the accepted phone number.
        """
        # Normalized top-level (after Adapt/Validate)
        phone = event.get("from_number")
        if phone:
            return phone

        # Raw input shape (before adaptation)
        phone = event.get("input", {}).get("from")
        if phone:
            return phone

        # Dynamodb-adapted shape (rarely needed here but safe to try)
        dyn = event.get("dynamodb") or event.get("input", {}).get("dynamodb") or {}
        if isinstance(dyn, dict):
            from_attr = dyn.get("from_number")
            if isinstance(from_attr, dict):
                return from_attr.get("S")
            if isinstance(from_attr, str):
                return from_attr

        return None

    def _get_user_info_item(self, phone: str) -> Optional[Dict[str, Any]]:
        """
        Read the UserInfo table. Primary key is the accepted phone number.
        Be flexible about the key attribute name (PK vs PhoneNumber).
        """
        table = self._get_user_info_table()
        # Try common key names used in this repo
        key_candidates = [
            {"PK": phone},
            {"PhoneNumber": phone},
            # Fallbacks if your table prefixes the key (uncomment if needed)
            # {"PK": f"NUMBER#{phone}"},
            # {"PhoneNumber": f"NUMBER#{phone}"},
        ]

        for key in key_candidates:
            try:
                resp = table.get_item(Key=key)
                item = resp.get("Item")
                if item:
                    return item
            except ClientError as e:
                # If it's a validation/key schema mismatch error, try next candidate
                self.logger.debug("AssessChanges: get_item failed for key %s: %s", key, e)

        return None

    def _parse_name_attribute(self, name_attr: Any) -> Optional[Dict[str, Any]]:
        """
        'Name' can be:
          - already a dict (DynamoDB Map unmarshalled by boto3), or
          - a JSON string, or
          - None/missing.
        Return a dict or None.
        """
        if name_attr is None:
            return None
        if isinstance(name_attr, dict):
            return name_attr
        if isinstance(name_attr, str):
            name_attr = name_attr.strip()
            if not name_attr:
                return None
            try:
                parsed = json.loads(name_attr)
                return parsed if isinstance(parsed, dict) else {"value": parsed}
            except json.JSONDecodeError:
                # Not JSON—wrap as a simple value
                return {"value": name_attr}
        # Any other scalar type: wrap
        return {"value": name_attr}

    def _get_user_info_table(self):
        if self._user_info_table is None:
            if self._dynamodb is None:
                self._dynamodb = boto3.resource("dynamodb")
            self._user_info_table = self._dynamodb.Table(USER_INFO_TABLE_NAME)
        return self._user_info_table
