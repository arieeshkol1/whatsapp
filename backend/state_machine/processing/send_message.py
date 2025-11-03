import os
import json
from typing import Iterable, Optional, Tuple

import boto3
import requests
from botocore.exceptions import ClientError

from state_machine.base_step_function import BaseStepFunction
from common.logger import custom_logger

logger = custom_logger()

DEFAULT_GRAPH_VERSION = "v21.0"
DEFAULT_CONNECT_TIMEOUT = 5
DEFAULT_READ_TIMEOUT = 20

secrets = boto3.client("secretsmanager")


def _mask_e164(number: str) -> str:
    if not number:
        return "<none>"
    n = number.strip()
    return "***" if len(n) <= 6 else f"{n[:4]}***{n[-2:]}"


def _preview(text: str, limit: int = 160) -> str:
    if text is None:
        return "<none>"
    t = str(text)
    return t if len(t) <= limit else (t[:limit] + f"... (+{len(t)-limit} chars)")


def _parse_stage_list(raw: Optional[str]) -> list[str]:
    if not raw:
        return []
    stages = [stage.strip() for stage in raw.split(",") if stage.strip()]
    return stages


def _load_secret_json(*, stages: Iterable[str]) -> dict:
    secret_name = os.environ.get("SECRET_NAME")
    if not secret_name:
        raise RuntimeError(
            "SECRET_NAME env var is required (Secrets Manager name/ARN)."
        )

    last_error: Optional[ClientError] = None
    for version_stage in stages:
        try:
            resp = secrets.get_secret_value(
                SecretId=secret_name, VersionStage=version_stage
            )
            break
        except ClientError as exc:
            last_error = exc
            logger.error(
                "Failed to fetch secret",
                extra={
                    "secret_name": secret_name,
                    "version_stage": version_stage,
                    "error_code": getattr(exc, "response", {})
                    .get("Error", {})
                    .get("Code"),
                },
            )
    else:
        if last_error is None:
            raise RuntimeError("No Secret Manager stages provided")
        raise RuntimeError(
            "Unable to retrieve WhatsApp credentials from Secrets Manager"
        ) from last_error

    if "SecretString" not in resp:
        raise RuntimeError("Secret is binary; expected a plaintext JSON SecretString.")
    try:
        data = json.loads(resp["SecretString"])
    except json.JSONDecodeError:
        raise RuntimeError("Expected JSON in SecretString, but parsing failed.")
    return data


def _initial_secret_stages() -> list[str]:
    configured = _parse_stage_list(os.environ.get("SECRET_VERSION_STAGE"))
    return configured or ["AWSCURRENT"]


def _retry_secret_stages() -> list[str]:
    configured = _parse_stage_list(os.environ.get("SECRET_RETRY_VERSION_STAGES"))
    if configured:
        return configured
    # Default retry order attempts a pending rotation first, then the current one.
    return ["AWSPENDING", "AWSCURRENT"]


def _get_creds_from_secret(
    *, stages: Optional[Iterable[str]] = None
) -> Tuple[str, str, str]:
    """
    Returns (token, phone_id, base_url) from AWSCURRENT version of the secret.
    No caching: always read fresh to avoid stale tokens.
    """

    data = _load_secret_json(stages=stages or _initial_secret_stages())

    token = data.get("META_TOKEN")
    phone_id = data.get("META_PHONE_NUMBER_ID")
    base_from_secret = data.get("META_BASE_URL")

    if not token:
        raise RuntimeError(
            "Secret key META_TOKEN is missing in /dev/aws-whatsapp-chatbot."
        )
    if not phone_id:
        raise RuntimeError(
            "Secret key META_PHONE_NUMBER_ID is missing in /dev/aws-whatsapp-chatbot."
        )

    if base_from_secret:
        base_url = base_from_secret.rstrip("/")
    else:
        meta_endpoint = os.environ.get(
            "META_ENDPOINT", "https://graph.facebook.com/"
        ).rstrip("/")
        base_url = f"{meta_endpoint}/{DEFAULT_GRAPH_VERSION}"

    return token, phone_id, base_url


def _post_whatsapp_message(
    *, token: str, phone_id: str, base_url: str, payload: dict
) -> dict:
    url = f"{base_url}/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    logger.info("POST WhatsApp Message", extra={"endpoint": url})
    resp = requests.post(
        url,
        headers=headers,
        json=payload,
        timeout=(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT),
    )

    body_text = resp.text
    logger.info(
        "Meta response",
        extra={"status_code": resp.status_code, "body_preview": body_text[:2000]},
    )

    if 200 <= resp.status_code < 300:
        try:
            return resp.json()
        except Exception:
            return {"ok": True, "raw": body_text}

    try:
        err = resp.json()
    except Exception:
        err = {"error": {"message": body_text}}

    meta_error = err.get("error", {}) or {}
    raise Exception(
        json.dumps(
            {
                "status_code": resp.status_code or None,
                "meta_error": {
                    "message": meta_error.get("message"),
                    "type": meta_error.get("type"),
                    "code": meta_error.get("code"),
                    "subcode": meta_error.get("error_subcode"),
                    "error_data": meta_error.get("error_data"),
                },
                "endpoint": url,
            }
        )
    )


def _should_retry_on_oauth(exception_json: str) -> bool:
    """
    Returns True if it's the classic expired token: code 190 subcode 463.
    """

    try:
        data = json.loads(exception_json)
        me = data.get("meta_error") or {}
        return me.get("code") == 190 and me.get("subcode") == 463
    except Exception:
        return False


class SendMessage(BaseStepFunction):
    def __init__(self, event):
        super().__init__(event, logger=logger)

    def send_message(self):
        self.logger.info("Starting send_message for the chatbot")

        # Extract inputs
        text_message = self.event.get("response_message", "DEFAULT_RESPONSE_MESSAGE")
        phone_number = (
            self.event.get("input", {})
            .get("dynamodb", {})
            .get("NewImage", {})
            .get("from_number", {})
            .get("S")
        )
        original_message_id = (
            self.event.get("input", {})
            .get("dynamodb", {})
            .get("NewImage", {})
            .get("whatsapp_id", {})
            .get("S")
        )

        self.logger.info(
            "Prepared WhatsApp send payload",
            extra={
                "to_number_masked": _mask_e164(phone_number),
                "text_preview": _preview(text_message),
                "original_message_id": original_message_id or "<none>",
            },
        )

        # Build payload (free-form text)
        payload = {
            "messaging_product": "whatsapp",
            "to": phone_number,
            "type": "text",
            "text": {"preview_url": False, "body": text_message},
        }

        # Try 1: use current secret
        stages_used = _initial_secret_stages()
        token, phone_id, base_url = _get_creds_from_secret(stages=stages_used)
        self.logger.info(
            "Using WA creds",
            extra={
                "source": "secretsmanager",
                "phone_id_tail": str(phone_id)[-4:],
                "base_url": base_url,
                "secret_version_stage": stages_used,
            },
        )

        try:
            response = _post_whatsapp_message(
                token=token, phone_id=phone_id, base_url=base_url, payload=payload
            )
        except Exception as e:
            err_str = str(e)
            self.logger.error(
                "Error in POST WhatsApp Message Meta API Response",
                extra={"exception": err_str},
            )

            # If expired token (190/463), refresh secret once and retry
            if _should_retry_on_oauth(err_str):
                self.logger.info(
                    "Detected expired token (190/463). Re-reading secret and retrying once."
                )
                retry_stages = _retry_secret_stages()
                token, phone_id, base_url = _get_creds_from_secret(stages=retry_stages)
                self.logger.info(
                    "Retrying send with refreshed WA creds",
                    extra={
                        "source": "secretsmanager",
                        "phone_id_tail": str(phone_id)[-4:],
                        "base_url": base_url,
                        "secret_version_stage": retry_stages,
                    },
                )
                response = _post_whatsapp_message(
                    token=token, phone_id=phone_id, base_url=base_url, payload=payload
                )
            else:
                raise

        self.logger.info(
            "WhatsApp message sent successfully",
            extra={
                "http_status": 200,
                "to_number_masked": _mask_e164(phone_number),
                "original_message_id": original_message_id or "<none>",
            },
        )

        self.event["send_message_response_status_code"] = 200
        self.event["send_message_response"] = response
        return self.event
