"""Utility to drop and recreate the Interaction-history DynamoDB table.

The script deletes the table if it exists, recreates it with the expected
PK/SK schema and DynamoDB Streams, and optionally seeds example items that
include the Bedrock response payloads (System_Response/Response fields).

Usage:
    python scripts/recreate_interaction_history_table.py --region us-east-1 \
        --table-name Interaction-history \
        --seed-file assets/dynamodb/interaction_history_seed.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

import boto3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recreate the interaction history table and seed data."
    )
    parser.add_argument(
        "--table-name",
        default="Interaction-history",
        help="Target DynamoDB table name (default: Interaction-history)",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="AWS region to target (defaults to environment/CLI config)",
    )
    parser.add_argument(
        "--seed-file",
        type=Path,
        default=None,
        help=(
            "Optional JSON file containing an array of items to seed after creation. "
            "Items should be plain JSON objects; nested maps will be stored as is."
        ),
    )
    return parser.parse_args()


def wait_for_table(table_name: str, dynamodb_client: Any, waiter: str) -> None:
    dynamodb_client.get_waiter(waiter).wait(TableName=table_name)


def delete_table_if_exists(table_name: str, dynamodb_client: Any) -> None:
    try:
        dynamodb_client.describe_table(TableName=table_name)
    except dynamodb_client.exceptions.ResourceNotFoundException:
        print(f"Table {table_name} does not exist; nothing to delete.")
        return

    print(f"Deleting existing table {table_name}...")
    dynamodb_client.delete_table(TableName=table_name)
    wait_for_table(table_name, dynamodb_client, "table_not_exists")
    print(f"Deleted {table_name}.")


def create_table(table_name: str, dynamodb_resource: Any, stream_enabled: bool) -> None:
    print(f"Creating table {table_name}...")
    dynamodb_resource.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "to_number", "AttributeType": "S"},
            {"AttributeName": "from_number", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "GSI_To_From",
                "KeySchema": [
                    {"AttributeName": "to_number", "KeyType": "HASH"},
                    {"AttributeName": "from_number", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={
            "StreamEnabled": stream_enabled,
            "StreamViewType": "NEW_IMAGE",
        },
    )
    wait_for_table(table_name, dynamodb_resource.meta.client, "table_exists")
    print(f"Created {table_name} with streams={'on' if stream_enabled else 'off' }.")


def load_seed_items(seed_file: Path) -> List[Dict[str, Any]]:
    try:
        payload = json.loads(seed_file.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Seed file {seed_file} is not valid JSON: {exc}") from exc

    if isinstance(payload, dict):
        payload = [payload]

    if not isinstance(payload, list):
        raise SystemExit(
            "Seed file must contain either a single object or an array of objects."
        )

    items: List[Dict[str, Any]] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise SystemExit(
                f"Seed entry at index {index} is not an object: {type(item)}"
            )
        items.append(item)
    return items


def seed_table(
    table_name: str, items: Iterable[Dict[str, Any]], dynamodb_resource: Any
) -> None:
    table = dynamodb_resource.Table(table_name)
    buffered_items = list(items)
    for item in buffered_items:
        table.put_item(Item=item)
    print(f"Seeded {table_name} with {len(buffered_items)} items.")


def main() -> int:
    args = parse_args()
    session_kwargs = {"region_name": args.region} if args.region else {}
    session = boto3.Session(**session_kwargs)
    dynamodb_resource = session.resource("dynamodb")
    dynamodb_client = session.client("dynamodb")

    delete_table_if_exists(args.table_name, dynamodb_client)
    # Preserve the stream configuration expected by the processing pipeline.
    create_table(args.table_name, dynamodb_resource, stream_enabled=True)

    if args.seed_file:
        items = load_seed_items(args.seed_file)
        seed_table(args.table_name, items, dynamodb_resource)

    return 0


if __name__ == "__main__":
    sys.exit(main())
