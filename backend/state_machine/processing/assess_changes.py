##Collect and evaluate proposed updates via Bedrock (feature gated)."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError
from common.logger import custom_logger

# Adjusted for your real table schema
USER_INFO_TABLE_NAME = "UserInfo"  # DynamoDB table name
USER_INFO_PK_NAME = "PhoneNumber"  # Primary key attribute


class AssessChanges:
    """Enriches event with UserInfo.Name (if present) for downstream steps."""

    def __init__(self, event: Optional[Dict[str, Any]] = None) -> None:
        self.event: Dict[str, Any] = event or {}
        self.logger = custom_logger()
        self._dynamodb = boto3.resource("dynamodb")
        self._user_info_table = self._dynamodb.Table(USER_INFO_TABLE_NAME)

    # ----------------------------------------------------------------------
    def assess_and_apply(self) -> Dict[str, Any]:
        """
        1. Read UserInfo by PhoneNumber (PK)
        2. Inject its Name attribute into the event
        3. Return enriched event for Process Text
        """
        phone = self._extract_phone_number(self.event)
        if not phone:
            self.logger.warning("AssessChanges: could not extract phone number.")
            return self.event

        try:
            item = self._get_user_info_item(phone)
        except Exception as exc:
            self.logger.exception(
                "AssessChanges: failed to read UserInfo for %s: %s", phone, exc
            )
            return self.event

        if not item:
            self.logger.info("AssessChanges: no UserInfo record for %s.", phone)
            return self.event

        name_payload = self._parse_name_attribute(item.get("Name"))
        if name_payload is None:
            self.logger.info("AssessChanges: UserInfo.Name empty for %s.", phone)
            return self.event

        # Enrich the event
        self.event.setdefault("user_info", {})["name"] = name_payload
        self.event["user_info_name"] = name_payload
        self.logger.debug("AssessChanges: injected user_info.name for %s", phone)

        return self.event

    # ----------------------------------------------------------------------
    def _extract_phone_number(self, event: Dict[str, Any]) -> Optional[str]:
        """Locate the phone number field across different event shapes."""
        if event.get("from_number"):
            return event["from_number"]
        if event.get("input", {}).get("from"):
            return event["input"]["from"]
        dyn = event.get("dynamodb") or event.get("input", {}).get("dynamodb") or {}
        if isinstance(dyn, dict):
            fn = dyn.get("from_number")
            if isinstance(fn, dict):
                return fn.get("S")
            if isinstance(fn, str):
                return fn
        return None

    def _get_user_info_item(self, phone: str) -> Optional[Dict[str, Any]]:
        """Query DynamoDB by PhoneNumber."""
        try:
            resp = self._user_info_table.get_item(Key={USER_INFO_PK_NAME: phone})
            return resp.get("Item")
        except ClientError as e:
            self.logger.error("AssessChanges: DynamoDB get_item failed: %s", e)
            return None

    def _parse_name_attribute(self, name_attr: Any) -> Optional[Dict[str, Any]]:
        """Normalize 'Name' to a dict."""
        if not name_attr:
            return None
        if isinstance(name_attr, dict):
            return name_attr
        if isinstance(name_attr, str):
            try:
                parsed = json.loads(name_attr)
                return parsed if isinstance(parsed, dict) else {"value": parsed}
            except json.JSONDecodeError:
                return {"value": name_attr}
        return {"value": name_attr}
