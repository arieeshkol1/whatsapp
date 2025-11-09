import os
import re
import json
from typing import Iterable, Optional, Tuple, List, Dict, Any

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

# ---- Defaults ----
DEFAULT_GRAPH_VERSION = "v21.0"
DEFAULT_CONNECT_TIMEOUT = 5
DEFAULT_READ_TIMEOUT = 20

# AWS clients
secrets = boto3.client("secretsmanager")

# HTTP session with retries for 429/5xx
_session = requests.Session()
_retries = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("POST", "GET"),
    raise_on_status=False,
)
_session.mount("https://", HTTPAdapter(max_retries=_retries))


# -----------------------
# Utilities
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


def _parse_stage_list(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [stage.strip() for stage in raw.split(",") if stage.strip()]


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


def _secret_stage_order_from_event(event_obj: dict) -> Optional[List[str]]:
    """
    Allow test events to override stage order:
    { "secret_stage_order": ["AWSPENDING", "AWSCURRENT", "AWSPREVIOUS"] }
    """
    try:
        order = event_obj.get("secret_stage_order")
        if isinstance(order, list) and all(isinstance(x, str) for x in order):
            return [x.strip() for x in order if x.strip()]
    except Exception:
        pass
    return None


# -----------------------
# Secrets Manager helpers
# -----------------------
def _choose_secret_names() -> List[str]:
    """Try primary, then optional fallback secret (env FALLBACK_SECRET_NAME)."""
    primary = os.environ.get("SECRET_NAME", "/dev/aws-whatsapp-chatbot")
    fallback = os.environ.get("FALLBACK_SECRET_NAME")
    names = [primary]
    if fallback and fallback != primary:
        names.append(fallback)
    return names


def _discover_available_stages(secret_name: str) -> List[str]:
    """
    Discover version labels; if IAM denies, fallback to common labels.
    Preferred order: AWSCURRENT, AWSPENDING, AWSPREVIOUS
    """
    try:
        resp = secrets.list_secret_version_ids(
            SecretId=secret_name, IncludeDeprecated=True
        )
        labels: List[str] = []
        for ver in resp.get("Versions", []):
            for lbl in ver.get("VersionStages", []):
                if lbl not in labels:
                    labels.append(lbl)
        prefs = ["AWSCURRENT", "AWSPENDING", "AWSPREVIOUS"]
        return [lbl for lbl in prefs if lbl in labels]
    except ClientError as exc:
        logger.error(
            "Failed to list secret version ids",
            extra={
                "secret_name": secret_name,
                "error_code": getattr(exc, "response", {}).get("Error", {}).get("Code"),
            },
        )
        return ["AWSCURRENT", "AWSPREVIOUS"]


def _load_secret_json(*, secret_name: str, version_stage: str) -> dict:
    resp = secrets.get_secret_value(SecretId=secret_name, VersionStage=version_stage)
    if "SecretString" not in resp:
        raise RuntimeError("Secret is binary; expected a plaintext JSON SecretString.")
    try:
        return json.loads(resp["SecretString"])
    except json.JSONDecodeError:
        raise RuntimeError("Expected JSON in SecretString, but parsing failed.")


def _build_base_url() -> str:
    """
    Always use a stable graph version unless explicitly overridden by env.
    (Ignore META_BASE_URL from the secret to avoid being stuck on v20.)
    """
    override = os.environ.get("META_BASE_URL_OVERRIDE")
    if override:
        return str(override).rstrip("/")
    endpoint = os.environ.get("META_ENDPOINT", "https://graph.facebook.com/").rstrip(
        "/"
    )
    return f"{endpoint}/{DEFAULT_GRAPH_VERSION}"


def _get_creds_from_secret(
    *, secret_name: str, version_stage: str
) -> Tuple[str, str, str, dict]:
    """
    Returns (token, phone_id, base_url, raw_secret) from the given secret and stage.
    Reads fresh each time; ignores any env META_TOKEN.
    """
    if os.environ.get("META_TOKEN"):
        logger.warning("Ignoring META_TOKEN env in favor of Secrets Manager value.")

    data = _load_secret_json(secret_name=secret_name, version_stage=version_stage)

    token = data.get("META_TOKEN")
    phone_id = data.get("META_PHONE_NUMBER_ID")
    if not token:
        raise RuntimeError(f"META_TOKEN missing in {secret_name} ({version_stage})")
    if not phone_id:
        raise RuntimeError(
            f"META_PHONE_NUMBER_ID missing in {secret_name} ({version_stage})"
        )

    base_url = _build_base_url()
    return token, phone_id, base_url, data


# -----------------------
# Meta API helpers
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


def _is_oauth_expired_from_json(err: dict) -> bool:
    me = (err or {}).get("meta_error") or {}
    return me.get("code") == 190 and me.get("subcode") in (463, 467)


def _is_oauth_expired_str(exception_json: str) -> bool:
    try:
        return _is_oauth_expired_from_json(json.loads(exception_json))
    except Exception:
        return False


# ---- Preflight (no app creds required): GET /{phone_id}?fields=... ----
_EXPIRED_MSG_RE = re.compile(
    r"expired on (?P<dow>[A-Za-z]+),?\s*(?P<date>[\d\-A-Za-z]+)\s*(?P<time>[\d:]+)\s*(?P<tz>[A-Z]+)",
    re.IGNORECASE,
)


def _preflight_token_read(
    *, base_url: str, token: str, phone_id: str
) -> Dict[str, Any]:
    """
    Calls GET /{phone_id}?fields=id,display_phone_number,verified_name with the token.
    - If 200 -> {'ok': True, 'status': 200, 'data': {...}}
    - If 401 -> {'ok': False, 'status': 401, 'error': {...}, 'expired_at_hint': "..."} when possible
    - Else -> {'ok': False, 'status': <code>, 'error': {...}}
    """
    url = f"{base_url}/{phone_id}"
    params = {
        "fields": "id,display_phone_number,verified_name",
        "access_token": token,
    }
    try:
        resp = _session.get(
            url, params=params, timeout=(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT)
        )
        status = resp.status_code
        body = {}
        try:
            body = resp.json()
        except Exception:
            body = {"raw": resp.text}

        if 200 <= status < 300:
            return {"ok": True, "status": status, "data": body}

        # Extract expiry hint if present
        expired_hint = None
        try:
            msg = (body.get("error") or {}).get("message") or ""
            m = _EXPIRED_MSG_RE.search(msg)
            if m:
                expired_hint = f"{m.group('dow')}, {m.group('date')} {m.group('time')} {m.group('tz')}"
        except Exception:
            pass

        return {
            "ok": False,
            "status": status,
            "error": body,
            "expired_at_hint": expired_hint,
        }
    except Exception as exc:
        return {"ok": False, "status": None, "error": {"exception": str(exc)}}


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

    def _try_send_with(
        self, *, secret_name: str, stage: str, payload: dict
    ) -> Tuple[dict, str, str, str]:
        token, phone_id, base_url, raw_secret = _get_creds_from_secret(
            secret_name=secret_name, version_stage=stage
        )
        self.logger.info(
            "Using WA creds",
            extra={
                "source": "secretsmanager",
                "secret_name": secret_name,
                "phone_id_tail": str(phone_id)[-4:],
                "base_url": base_url,
                "secret_version_stage": stage,
            },
        )

        # Preflight: skip obvious expired/invalid tokens before POST
        pf = _preflight_token_read(base_url=base_url, token=token, phone_id=phone_id)
        self.logger.info(
            "Token preflight (read phone node)",
            extra={"secret_name": secret_name, "stage": stage, "preflight": pf},
        )

        if not pf.get("ok"):
            # If Meta is clearly saying expired/invalid -> treat as OAuth expired and try next stage
            pf_err = pf.get("error") or {}
            # Normalize to the same shape our _is_oauth_expired_* expects
            normalized_err = {
                "meta_error": {
                    "message": (
                        (pf_err.get("error") or {}).get("message")
                        if isinstance(pf_err, dict) and "error" in pf_err
                        else (
                            pf_err.get("message") if isinstance(pf_err, dict) else None
                        )
                    ),
                    "type": (
                        (pf_err.get("error") or {}).get("type")
                        if isinstance(pf_err, dict) and "error" in pf_err
                        else (pf_err.get("type") if isinstance(pf_err, dict) else None)
                    ),
                    "code": (
                        (pf_err.get("error") or {}).get("code")
                        if isinstance(pf_err, dict) and "error" in pf_err
                        else (pf_err.get("code") if isinstance(pf_err, dict) else None)
                    ),
                    "error_subcode": (
                        (pf_err.get("error") or {}).get("error_subcode")
                        if isinstance(pf_err, dict) and "error" in pf_err
                        else (
                            pf_err.get("error_subcode")
                            if isinstance(pf_err, dict)
                            else None
                        )
                    ),
                }
            }
            if _is_oauth_expired_from_json(normalized_err):
                raise SendAttemptError(
                    stage,
                    base_url,
                    Exception(
                        json.dumps(
                            {
                                "status": pf.get("status"),
                                "meta_error": normalized_err.get("meta_error"),
                                "preflight": True,
                                "expired_at_hint": pf.get("expired_at_hint"),
                            }
                        )
                    ),
                )

        # If preflight OK, try to send
        try:
            response = _post_whatsapp_message(
                token=token, phone_id=phone_id, base_url=base_url, payload=payload
            )
        except Exception as exc:
            raise SendAttemptError(stage, base_url, exc) from exc
        return response, phone_id, base_url, stage

    def send_message(self):
        self.logger.info("Starting send_message for the chatbot")

        # Extract inputs
        text_message_raw = self.event.get("response_message")
        customer_summary = self.event.get("customer_summary")
        order_progress_summary = self.event.get("order_progress_summary")

        if text_message_raw is None:
            text_message = "DEFAULT_RESPONSE_MESSAGE"
        else:
            text_message = str(text_message_raw)
            if not text_message.strip():
                text_message = "DEFAULT_RESPONSE_MESSAGE"
        for supplemental in (customer_summary, order_progress_summary):
            if supplemental and supplemental not in text_message:
                text_message = f"{text_message}\n\n{supplemental}"

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

        payload = self._build_text_payload(
            to_number=phone_number,
            body=text_message,
            original_message_id=original_message_id,
        )

        # Stage order override (from test event) or discovery/env
        stage_order_override = _secret_stage_order_from_event(
            self.event
        ) or _secret_stage_order_from_event(self.event.get("event", {}) or {})

        errors_by_secret: Dict[str, List[dict]] = {}
        tried_any = False

        for secret_name in _choose_secret_names():
            discovered = _discover_available_stages(secret_name)
            if stage_order_override:
                stages = [
                    s
                    for s in stage_order_override
                    if s
                    in set(discovered or ["AWSCURRENT", "AWSPREVIOUS", "AWSPENDING"])
                ]
            else:
                stages = discovered or ["AWSCURRENT", "AWSPREVIOUS", "AWSPENDING"]

            self.logger.info(
                "Resolved secret stages to try",
                extra={"secret_name": secret_name, "stages": stages},
            )

            for stage in stages:
                tried_any = True
                try:
                    response, phone_id, base_url, stage_used = self._try_send_with(
                        secret_name=secret_name, stage=stage, payload=payload
                    )
                    # Success – return immediately
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

                except SendAttemptError as wrapped:
                    err_str = str(wrapped.original)
                    stage_used = getattr(wrapped, "stage_used", stage)
                    base_url_used = getattr(wrapped, "base_url", None)

                    errors_by_secret.setdefault(secret_name, []).append(
                        {
                            "stage": stage_used,
                            "base_url": base_url_used,
                            "error": _safe_json_loads(err_str),
                        }
                    )

                    self.logger.error(
                        "Send attempt failed",
                        extra={
                            "secret_name": secret_name,
                            "stage_used": stage_used,
                            "base_url_used": base_url_used,
                            "exception": err_str,
                        },
                    )

                    # If token expired – try next stage; else bubble up now
                    if _is_oauth_expired_str(err_str):
                        continue
                    else:
                        raise

        if not tried_any:
            raise RuntimeError("No secret names/stages available to attempt.")

        raise RuntimeError(
            json.dumps(
                {
                    "hint": "All candidate WhatsApp tokens failed (likely expired/invalid in every stage).",
                    "action": "Update META_TOKEN in AWSCURRENT with a valid long-lived System User token for the WABA. Optionally stage under AWSPENDING and test via secret_stage_order.",
                    "secret_names_tried": _choose_secret_names(),
                    "per_secret_results": errors_by_secret,
                }
            )
        )


def _safe_json_loads(s: str) -> dict:
    try:
        return json.loads(s)
    except Exception:
        return {"raw": s}
