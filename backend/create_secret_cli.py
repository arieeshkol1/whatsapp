"""Command-line helpers for provisioning the WhatsApp Secrets Manager entry."""

from __future__ import annotations

# Built-in imports
import argparse
import json
import logging
import sys
from typing import Dict, Iterable, Tuple

# External imports
import boto3
from botocore.exceptions import ClientError


LOG_FORMAT = "%(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("create-secret")


def parse_extra_items(extra_items: Iterable[str]) -> Dict[str, str]:
    """Parse additional key=value pairs for the secret payload."""
    parsed: Dict[str, str] = {}
    for item in extra_items:
        if "=" not in item:
            raise ValueError(
                "Invalid extra item '{item}'. Expected format KEY=VALUE.".format(
                    item=item
                )
            )
        key, value = item.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise ValueError(
                "Invalid extra item '{item}'. Key name cannot be empty.".format(
                    item=item
                )
            )
        parsed[key] = value
    return parsed


def build_secret_payload(args: argparse.Namespace) -> Dict[str, str]:
    """Build the JSON structure stored in Secrets Manager."""
    payload = {
        "AWS_API_KEY_TOKEN": args.verify_token,
        "META_TOKEN": args.meta_token,
        "META_FROM_PHONE_NUMBER_ID": args.phone_number_id,
    }
    payload.update(parse_extra_items(args.extra or []))
    return payload


def create_or_update_secret(
    *,
    secret_name: str,
    payload: Dict[str, str],
    region: str | None,
    profile: str | None,
    force_overwrite: bool,
) -> Tuple[bool, str]:
    """Create the secret or update it when it already exists."""
    session_kwargs = {}
    if profile:
        session_kwargs["profile_name"] = profile

    session = boto3.Session(**session_kwargs)
    client_kwargs = {}
    if region:
        client_kwargs["region_name"] = region

    client = session.client("secretsmanager", **client_kwargs)
    secret_string = json.dumps(payload)

    try:
        client.create_secret(Name=secret_name, SecretString=secret_string)
        return True, "Secret created"
    except client.exceptions.ResourceExistsException:
        if not force_overwrite:
            return False, (
                "Secret already exists. Use --force-overwrite to push a new version."
            )
        client.put_secret_value(SecretId=secret_name, SecretString=secret_string)
        return True, "Existing secret updated"
    except ClientError as error:  # pragma: no cover - defensive
        raise RuntimeError(f"Failed to create secret: {error}") from error


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    """Configure and parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Provision the Secrets Manager entry consumed by the WhatsApp chatbot."
        )
    )
    parser.add_argument(
        "--secret-name",
        required=True,
        help="Name or ARN of the secret to create (e.g. /dev/aws-whatsapp-chatbot)",
    )
    parser.add_argument(
        "--meta-token",
        required=True,
        help="Long-lived Meta access token used for outbound WhatsApp messages.",
    )
    parser.add_argument(
        "--phone-number-id",
        required=True,
        help="Phone number ID from the Meta WhatsApp Business configuration.",
    )
    parser.add_argument(
        "--verify-token",
        required=True,
        help="Webhook verify token that must match the Meta configuration.",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="AWS region where the secret should be created (defaults to SDK config).",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Named AWS profile to load credentials from (optional).",
    )
    parser.add_argument(
        "--force-overwrite",
        action="store_true",
        help="Update the secret if it already exists.",
    )
    parser.add_argument(
        "--extra",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help=(
            "Additional key/value pairs to include in the JSON secret. "
            "Pass multiple times for several entries."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    """Entrypoint used by the CLI command."""
    try:
        args = parse_args(argv)
        payload = build_secret_payload(args)
        success, message = create_or_update_secret(
            secret_name=args.secret_name,
            payload=payload,
            region=args.region,
            profile=args.profile,
            force_overwrite=args.force_overwrite,
        )
        if success:
            logger.info(message)
            logger.info(
                "Stored %s with keys: %s",
                args.secret_name,
                ", ".join(sorted(payload.keys())),
            )
            return 0
        logger.error(message)
        return 1
    except ValueError as error:
        logger.error(str(error))
        return 1
    except RuntimeError as error:  # pragma: no cover - defensive
        logger.error(str(error))
        return 1


if __name__ == "__main__":  # pragma: no cover - CLI execution path
    sys.exit(main())
