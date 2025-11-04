import os
import json
from typing import Iterable, Optional, Tuple

import boto3
import requests
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter, Retry

from state_machine.base_step_function import BaseStepFunction
from common.logger import custom_logger


class SendAttemptError(Exception):
    """Wraps send failures with metadata about the attempted secret stage."""

    def __init__(
        self, stage_used: Optional[str], base_url: Optional[str], original: Exception
    ):
        self.stage_used = stage_used
        self.base_url = base_url
        self.original = original
        super().__init__(str(original))


logger = custom_logger()

# Defaults
DEFAULT_GRAPH_VERSION = "v21.0"
DEFAULT_CONNECT_TIMEOUT = 5
DEFAULT_READ_TIMEOUT = 20

# Shared clients
secrets = boto3.client("secretsmanager")

# Robust HTTP retries for transient errors (429/5xx)
_session = requests.Session()
_retries = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("POST",),
    raise_on_status=False,
)
_session.mount("https://", HTTPAdapter(max_retries=_retries))


# -----------------------
# Utility helpers
# -----------------------
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


def _normalize_to_e164(num: Optional[str]) -> Optional[str]:
    """Normalize '972...' => '+972...' if no leading '+' is present."""
    if not num:
        return num
    n = str(num).strip()
    if n.startswith("+"):
        return n
    if n and n[0].isdigit():
        return f"+{n}"
    return n


def _secret_stage_order_from_event(event_obj: dict) -> Optional[list[str]]:
    """
    Allow test events to override stage order:
    { "secret_stage_order": ["AWSPREVIOUS", "AWSCURRENT"] }
    """
    try:
        order = event_obj.get("secret_stage_order")
        if isinstance(order, list) and all(isinstance(x, str) for x in order):
            return [x.strip() for x in order if x.strip()]
    except Exception:
        pass
    return None


# -----------------------
# Secret loading
# -----------------------
def _load_secret_json(*, stages: Iterable[str]) -> tuple[dict, str]:
    # Default to your provided secret name if env var is not set.
    secret_name = os.environ.get("SECRET_NAME", "/dev/aws-whatsapp-chatbot")
    if not secret_name:
        raise RuntimeError(
            "SECRET_NAME env var is required (Secrets Manager name/ARN)."
        )

    last_error: Optional[ClientError] = None
    resp = None
    version_stage = None

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
            resp = None

    if resp is None:
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

    return data, version_stage


def _initial_secret_stages() -> list[str]:
    """
    Prefer stages that actually exist in your account setup:
    - Your screenshot shows AWSCURRENT and AWSPREVIOUS (no AWSPENDING).
    Allow env override if needed.
    """
    configured = _parse_stage_list(os.environ.get("SECRET_VERSION_STAGE"))
    if configured:
        return configured
    return ["AWSCURRENT", "AWSPREVIOUS"]


def _retry_secret_stages() -> list[str]:
    """
    Retry order—try the other label first.
    """
    configured = _parse_stage_list(os.environ.get("SECRET_RETRY_VERSION_STAGES"))
    if configured:
        return configured
    return ["AWSPREVIOUS", "AWSCURRENT"]


def _get_creds_from_secret(
    *, stages: Optional[Iterable[str]] = None
) -> Tuple[str, str, str, str]:
    """
    Returns (token, phone_id, base_url, stage_used) from Secrets Manager.
    Always reads fresh and explicitly ignores any env META_TOKEN.
    """

    if os.environ.get("META_TOKEN"):
        logger.warning(
            "Ignoring META_TOKEN from environment in favor of Secrets Manager value."
        )

    data, stage_used = _load_secret_json(stages=stages or _initial_secret_stages())

    token = data.get("META_TOKEN")
    phone_id = data.get("META_PHONE_NUMBER_ID")
    base_from_secret = data.get("META_BASE_URL")

    if not token:
        raise RuntimeError("Secret key META_TOKEN is missing in the configured secret.")
    if not phone_id:
        raise RuntimeError(
            "Secret key META_PHONE_NUMBER_ID is missing in the configured secret."
        )

    if base_from_secret:
        base_url = base_from_secret.rstrip("/")
    else:
        meta_endpoint = os.environ.get(
            "META_ENDPOINT", "https://graph.facebook.com/"
        ).rstrip("/")
        base_url = f"{meta_endpoint}/{DEFAULT_GRAPH_VERSION}"

    return token, phone_id, base_url, stage_used


# -----------------------
# Meta API
# -----------------------
def _post_whatsapp_message(
    *, token: str, phone_id: str, base_url: str, payload: dict
) -> dict:
    url = f"{base_url}/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    logger.info("POST WhatsApp Message", extra={"endpoint": url})
    resp = _session.post(
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

    # Log full error and headers for debugging/rate-limit insight
    try:
        headers_snapshot = dict(resp.headers)
    except Exception:
        headers_snapshot = {}

    logger.error(
        "Meta error details",
        extra={"meta_error": meta_error, "headers": headers_snapshot},
    )

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
    Returns True if token is expired/invalid.
    Classic: code 190 subcode 463 (expired) or 467 (invalidated).
    """
    try:
        data = json.loads(exception_json)
        me = data.get("meta_error") or {}
        return me.get("code") == 190 and me.get("subcode") in (463, 467)
    except Exception:
        return False


# -----------------------
# Step Function wrapper
# -----------------------
class SendMessage(BaseStepFunction):
    def __init__(self, event):
        super().__init__(event, logger=logger)

    def _build_text_payload(
        self, *, to_number: str, body: str, original_message_id: Optional[str]
    ) -> dict:
        payload = {
            "messaging_product": "whatsapp",
            "to": to_number,
            "type": "text",
            "text": {"preview_url": False, "body": body},
        }
        if original_message_id:
            payload["context"] = {"message_id": original_message_id}
        return payload

    def _send_once_with_fresh_secret(
        self, *, payload: dict, stages: Iterable[str]
    ) -> Tuple[dict, str, str, str]:
        # Fetch fresh creds right before calling Meta
        token, phone_id, base_url, stage_used = _get_creds_from_secret(stages=stages)
        self.logger.info(
            "Using WA creds",
            extra={
                "source": "secretsmanager",
                "phone_id_tail": str(phone_id)[-4:],
                "base_url": base_url,
                "secret_version_stage": stage_used,
            },
        )
        try:
            response = _post_whatsapp_message(
                token=token, phone_id=phone_id, base_url=base_url, payload=payload
            )
        except Exception as exc:  # pragma: no cover - re-raised with metadata
            raise SendAttemptError(stage_used, base_url, exc) from exc

        return response, phone_id, base_url, stage_used

    def send_message(self):
        self.logger.info("Starting send_message for the chatbot")

        # Extract inputs
        text_message = self.event.get("response_message", "DEFAULT_RESPONSE_MESSAGE")
        customer_summary = self.event.get("customer_summary")

        if customer_summary:
            text_message = str(text_message)
            if customer_summary not in text_message:
                text_message = f"{text_message}\n\n{customer_summary}"

        phone_number_raw = (
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

        phone_number = _normalize_to_e164(phone_number_raw)

        self.logger.info(
            "Prepared WhatsApp send payload",
            extra={
                "to_number_masked": _mask_e164(phone_number),
                "text_preview": _preview(text_message),
                "original_message_id": original_message_id or "<none>",
            },
        )

        # Build free-form text payload
        payload = self._build_text_payload(
            to_number=phone_number,
            body=text_message,
            original_message_id=original_message_id,
        )

        # Allow per-event override of secret stage order for testing rotation
        stage_order_override = _secret_stage_order_from_event(
            self.event
        ) or _secret_stage_order_from_event(self.event.get("event", {}) or {})

        first_stage_order = stage_order_override or _initial_secret_stages()
        retry_stage_order = stage_order_override or _retry_secret_stages()

        # Attempt 1: read from Secrets Manager (e.g., AWSCURRENT first)
        try:
            (
                response,
                phone_id_1,
                base_url_1,
                stage_used_1,
            ) = self._send_once_with_fresh_secret(
                payload=payload, stages=first_stage_order
            )
        except Exception as e:
            err_str = str(e)
            stage_used_1 = getattr(e, "stage_used", None)
            base_url_1 = getattr(e, "base_url", None)
            self.logger.error(
                "Error in POST WhatsApp Message Meta API Response",
                extra={"exception": err_str, "first_stage_order": first_stage_order},
            )

            # Retry only if it's OAuth expiration AND we have a different stage to try
            if _should_retry_on_oauth(err_str):
                # Compute an alternate list that EXCLUDES the stage we just used
                next_candidates = [s for s in retry_stage_order if s != stage_used_1]

                if not next_candidates:
                    # Clear guidance: token is expired in AWSCURRENT and there's no alternate stage to try
                    raise RuntimeError(
                        json.dumps(
                            {
                                "hint": "Expired WhatsApp token in Secrets Manager.",
                                "action": "Update META_TOKEN in the secret (AWSCURRENT) with a valid token, or test with AWSPREVIOUS if it is still valid by overriding stage order.",
                                "secret_name": os.environ.get(
                                    "SECRET_NAME", "/dev/aws-whatsapp-chatbot"
                                ),
                                "stage_used": stage_used_1,
                                "base_url_used": base_url_1,
                                "meta_error": json.loads(err_str).get("meta_error", {}),
                            }
                        )
                    )

                # Try an alternate stage (e.g., AWSPREVIOUS if present)
                (
                    response,
                    phone_id_2,
                    base_url_2,
                    stage_used_2,
                ) = self._send_once_with_fresh_secret(
                    payload=payload, stages=next_candidates
                )
            else:
                # Non-oauth errors: bubble up
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
