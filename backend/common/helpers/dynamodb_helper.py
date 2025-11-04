# Built-in imports
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

# Own imports
from common.logger import custom_logger

logger = custom_logger()


class DynamoDBHelper:
    """Custom DynamoDB Helper for simplifying CRUD operations."""

    def __init__(self, table_name: str, endpoint_url: str = None) -> None:
        """
        :param table_name (str): Name of the DynamoDB table to connect with.
        :param endpoint_url (Optional(str)): Endpoint for DynamoDB (only for local tests).
        """
        self.table_name = table_name
        self.dynamodb_client = boto3.client("dynamodb", endpoint_url=endpoint_url)
        self.dynamodb_resource = boto3.resource("dynamodb", endpoint_url=endpoint_url)
        self.table = self.dynamodb_resource.Table(self.table_name)

    def get_item_by_pk_and_sk(
        self, partition_key: str, sort_key: str
    ) -> Dict[str, Any]:
        """
        Method to get a single DynamoDB item from the primary key (pk+sk).
        :param partition_key (str): partition key value.
        :param sort_key (str): sort key value.
        """
        logger.info(
            f"Starting get_item_by_pk_and_sk with"
            f"pk: ({partition_key}) and sk: ({sort_key})"
        )

        # The structure key for a single-table-design "PK" and "SK" naming
        primary_key_dict = {
            "PK": {
                "S": partition_key,
            },
            "SK": {
                "S": sort_key,
            },
        }
        try:
            response = self.dynamodb_client.get_item(
                TableName=self.table_name,
                Key=primary_key_dict,
            )
            return response["Item"] if "Item" in response else {}

        except ClientError as error:
            logger.error(
                f"get_item operation failed for: "
                f"table_name: {self.table_name}."
                f"pk: {partition_key}."
                f"sk: {sort_key}."
                f"error: {error}."
            )
            raise error

    def query_by_pk_and_sk_begins_with(
        self, partition_key: str, sort_key_portion: str
    ) -> List[Dict[str, Any]]:
        """
        Method to run a query against DynamoDB with partition key and the sort
        key with <begins-with> functionality on it.
        :param partition_key (str): partition key value.
        :param sort_key_portion (str): sort key portion to use in query.
        """
        logger.info(
            f"Starting query_by_pk_and_sk_begins_with with"
            f"pk: ({partition_key}) and sk: ({sort_key_portion})"
        )

        all_items: List[Dict[str, Any]] = []
        try:
            # The structure key for a single-table-design "PK" and "SK" naming
            key_condition = Key("PK").eq(partition_key) & Key("SK").begins_with(
                sort_key_portion
            )
            limit = 50

            # Initial query before pagination
            response = self.table.query(
                KeyConditionExpression=key_condition,
                Limit=limit,
            )
            if "Items" in response:
                all_items.extend(response["Items"])

            # Pagination loop for possible following queries
            while "LastEvaluatedKey" in response:
                response = self.table.query(
                    KeyConditionExpression=key_condition,
                    Limit=limit,
                    ExclusiveStartKey=response["LastEvaluatedKey"],
                )
                if "Items" in response:
                    all_items.extend(response["Items"])

            return all_items
        except ClientError as error:
            logger.error(
                f"query operation failed for: "
                f"table_name: {self.table_name}."
                f"pk: {partition_key}."
                f"sort_key_portion: {sort_key_portion}."
                f"error: {error}."
            )
            raise error

    def put_item(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Method to add a single DynamoDB item.
        :param data (dict): Item to be added in a JSON format (without the "S", "N", "B" approach).
        """
        logger.info("Starting put_item operation.")
        logger.debug(data, message_details=f"Data to be added to {self.table_name}")

        try:
            response = self.table.put_item(
                TableName=self.table_name,
                Item=data,
            )
            logger.info(response)
            return response
        except ClientError as error:
            logger.error(
                f"put_item operation failed for: "
                f"table_name: {self.table_name}."
                f"data: {data}."
                f"error: {error}."
            )
            raise error

    def get_latest_item_by_pk(self, partition_key: str) -> Optional[Dict[str, Any]]:
        """Return the most recent item for a partition key (by sort key)."""

        logger.info(
            "Starting get_latest_item_by_pk", extra={"partition_key": partition_key}
        )

        try:
            response = self.table.query(
                KeyConditionExpression=Key("PK").eq(partition_key),
                ScanIndexForward=False,
                Limit=1,
            )
        except ClientError as error:
            logger.error(
                "get_latest_item_by_pk operation failed",
                extra={
                    "table_name": self.table_name,
                    "pk": partition_key,
                    "error": str(error),
                },
            )
            raise

        items = response.get("Items") or []
        return items[0] if items else None

    def query_by_conversation(
        self, partition_key: str, conversation_id: int, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Return messages for a given partition and conversation id."""

        logger.info(
            "Starting query_by_conversation",
            extra={
                "partition_key": partition_key,
                "conversation_id": conversation_id,
                "limit": limit,
            },
        )

        all_items: List[Dict[str, Any]] = []
        last_evaluated_key: Optional[Dict[str, Any]] = None

        try:
            while True:
                query_kwargs: Dict[str, Any] = {
                    "KeyConditionExpression": Key("PK").eq(partition_key),
                    "ScanIndexForward": True,
                    "FilterExpression": Attr("conversation_id").eq(conversation_id),
                    "Limit": limit,
                }
                if last_evaluated_key:
                    query_kwargs["ExclusiveStartKey"] = last_evaluated_key

                response = self.table.query(**query_kwargs)
                items = response.get("Items", [])
                if items:
                    all_items.extend(items)

                last_evaluated_key = response.get("LastEvaluatedKey")
                if not last_evaluated_key or len(all_items) >= limit:
                    break

            return all_items[:limit]
        except ClientError as error:
            logger.error(
                "query_by_conversation operation failed",
                extra={
                    "table_name": self.table_name,
                    "pk": partition_key,
                    "conversation_id": conversation_id,
                    "error": str(error),
                },
            )
            raise
