# backend/state_machine/processing/send_message.py

from __future__ import annotations

import os
from typing import Any, Callable, Dict, Optional

import boto3  # required for secrets manager (lazy-used only)
from botocore.exceptions import BotoCoreError, ClientError

from backend.common.logger import custom_logger

# NOTE:
# We intentionally avoid importing or instantiating anything that performs network
# calls at import time. This keeps unit test *collection* free of AWS/env coupling.
#
# If you need MetaAPI integration, it's resolved lazily the first time you send a
# message (and is safely mockable in tests).

# NOTE:
# We intentionally avoid importing or instantiating anything that performs network
# calls at import time. This keeps unit test *collection* free of AWS/env coupling.
#
# If you need MetaAPI integration, it's resolved lazily the first time you send a
# message (and is safely mockable in tests).


logger = custom_logger()


def _get_secrets_client():
    """Lazy creator for AWS Secrets Manager client."""

    return boto3.client("secretsmanager")


def get_secret_value(secret_name: str) -> str:
    """
    Fetch a secret string from AWS Secrets Manager.

    This function is NOT called at import time. It is safe to mock in unit tests.
    """
    try:
        resp = _get_secrets_client().get_secret_value(SecretId=secret_name)
        secret = resp.get("SecretString") or ""
        if not secret:
            raise RuntimeError(f"Secret '{secret_name}' returned empty SecretString")
        return secret
    except (ClientError, BotoCoreError) as exc:
        logger.error(
            {
                "message": "Failed to fetch secret",
                "secret_name": secret_name,
                "error": str(exc),
            }
        )
        raise

    try:
        resp = _get_secrets_client().get_secret_value(SecretId=secret_name)
        secret = resp.get("SecretString") or ""
        if not secret:
            raise RuntimeError(f"Secret '{secret_name}' returned empty SecretString")
        return secret
    except (ClientError, BotoCoreError) as exc:
        logger.error(
            {
                "message": "Failed to fetch secret",
                "secret_name": secret_name,
                "error": str(exc),
            }
        )
        raise

    try:
        resp = _get_secrets_client().get_secret_value(SecretId=secret_name)
        secret = resp.get("SecretString") or ""
        if not secret:
            raise RuntimeError(f"Secret '{secret_name}' returned empty SecretString")
        return secret
    except (ClientError, BotoCoreError) as exc:
        logger.error(
            {
                "message": "Failed to fetch secret",
                "secret_name": secret_name,
                "error": str(exc),
            }
        )
        raise


class SendMessage:
    """
    A light wrapper responsible for sending messages through the Meta (WhatsApp) API.

    Design goals:
    - No network or AWS calls during import/initialization unless absolutely necessary.
    - Dependency injection friendly (pass your own meta_api or token provider).
    - Easy to mock in tests.
    """

    def __init__(
        self,
        meta_api: Optional[Any] = None,
        # If you provide a token directly, we won't call Secrets Manager.
        auth_token: Optional[str] = None,
        # If you want us to fetch from Secrets Manager on demand, provide a secret name,
        # or set the env var SECRET_NAME. (We do NOT resolve it at import time.)
        secret_name: Optional[str] = None,
        # Custom token provider for advanced cases; signature: () -> str
        token_provider: Optional[Callable[[], str]] = None,
    ) -> None:
        self._meta_api = meta_api  # can be a ready-to-use instance or None (lazy)
        self._explicit_token = auth_token
        self._secret_name = secret_name or os.environ.get("SECRET_NAME", "")
        self._token_provider = token_provider
        self._meta_api_factory = _default_meta_api_factory

        logger.debug(
            {
                "message": "SendMessage initialized",
                "has_meta_api": bool(meta_api),
                "has_explicit_token": bool(auth_token),
                "has_secret_name": bool(self._secret_name),
                "has_token_provider": bool(token_provider),
            }
        )

    # ---------- Public API (kept minimal & mockable) ----------

    def send_text(self, to_phone: str, text: str) -> Dict[str, Any]:
        """
        Send a plain text WhatsApp message.
        This will lazily resolve MetaAPI + token on first use.
        """

        meta = self._ensure_meta_api()
        payload = {"to": to_phone, "type": "text", "text": {"body": text}}
        logger.debug({"message": "Sending text", "to": to_phone, "payload": payload})
        return meta.send_message(payload)

    def send_template(
        self,
        to_phone: str,
        template_name: str,
        language_code: str = "en_US",
        components: Optional[list] = None,
    ) -> Dict[str, Any]:
        """
        Send a WhatsApp template message.
        """

        meta = self._ensure_meta_api()
        payload = {
            "to": to_phone,
            "type": "template",
            "template": {
                "name": template_name,
                "language": {"code": language_code},
            },
        }
        if components:
            payload["template"]["components"] = components

        logger.debug(
            {
                "message": "Sending template",
                "to": to_phone,
                "template_name": template_name,
                "language_code": language_code,
                "components": components,
            }
        )
        return meta.send_message(payload)

    def send_media(
        self,
        to_phone: str,
        media_id: Optional[str] = None,
        media_link: Optional[str] = None,
        media_type: str = "image",  # "image" | "audio" | "video" | "document"
        caption: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Send a media message by ID or public link.
        """

        if not media_id and not media_link:
            raise ValueError("Either media_id or media_link must be provided")

        meta = self._ensure_meta_api()
        media_payload: Dict[str, Any] = {}
        if media_id:
            media_payload["id"] = media_id
        if media_link:
            media_payload["link"] = media_link
        if caption:
            media_payload["caption"] = caption
        if filename and media_type == "document":
            media_payload["filename"] = filename

        payload = {"to": to_phone, "type": media_type, media_type: media_payload}

        logger.debug(
            {
                "message": "Sending media",
                "to": to_phone,
                "media_type": media_type,
                "media_id": media_id,
                "media_link": media_link,
                "caption": caption,
                "filename": filename,
            }
        )
        return meta.send_message(payload)

    # ---------- Internals ----------

    def _ensure_meta_api(self):
        """
        Lazily construct the MetaAPI dependency with a valid auth token.
        We import MetaAPI only here to avoid import-time side effects for tests.
        """

        if self._meta_api is not None:
            return self._meta_api

        token = self._resolve_token()

        # Import here so simply importing SendMessage doesn't drag in integrations
        from ..integrations.meta.api_requests import MetaAPI  # local import, lazy

        self._meta_api = self._meta_api_factory(MetaAPI, token)
        logger.debug({"message": "MetaAPI created lazily"})
        return self._meta_api

    def _resolve_token(self) -> str:
        """
        Resolve the auth token in this priority:
        1) Explicit token passed to constructor
        2) Custom token_provider()
        3) Secret read from Secrets Manager using provided secret name (or env SECRET_NAME)

        Nothing is fetched until this method is called.
        """

        if self._explicit_token:
            return self._explicit_token

        if self._token_provider:
            token = self._token_provider()
            if not token:
                raise RuntimeError("token_provider() returned an empty token")
            return token

        if not self._secret_name:
            raise RuntimeError(
                "No auth token available. Provide 'auth_token', or a 'token_provider', "
                "or set 'secret_name' (or env SECRET_NAME) for Secrets Manager."
            )

        logger.debug(
            {
                "message": "Fetching token from Secrets Manager",
                "secret_name": self._secret_name,
            }
        )
        token = get_secret_value(self._secret_name)
        if not token:
            raise RuntimeError(f"Empty token fetched from secret '{self._secret_name}'")
        return token


def _default_meta_api_factory(MetaAPICls, token: str):
    """
    Factory to build MetaAPI. Split out for easy mocking in unit tests.
    """

    return MetaAPICls(auth_token=token)
