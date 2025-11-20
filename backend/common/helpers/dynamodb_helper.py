# Built-in imports
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

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

    def get_conversation_state(
        self, partition_key: str, conversation_id: int
    ) -> Optional[Dict[str, Any]]:
        """Return structured state persisted for a conversation if present."""

        sort_key = f"STATE#{conversation_id}"
        try:
            response = self.table.get_item(Key={"PK": partition_key, "SK": sort_key})
        except ClientError as error:
            logger.error(
                "get_conversation_state operation failed",
                extra={
                    "table_name": self.table_name,
                    "pk": partition_key,
                    "sk": sort_key,
                    "error": str(error),
                },
            )
            raise

        if not response:
            return None

        item = response.get("Item")
        if not isinstance(item, dict):
            return None

        state = item.get("state")
        return state if isinstance(state, dict) else None

    def put_conversation_state(
        self, partition_key: str, conversation_id: int, state: Dict[str, Any]
    ) -> None:
        """Persist structured state for a conversation."""

        sort_key = f"STATE#{conversation_id}"
        try:
            self.table.put_item(
                Item={
                    "PK": partition_key,
                    "SK": sort_key,
                    "state": state,
                    "last_updated_at": datetime.utcnow().isoformat(),
                }
            )
        except ClientError as error:
            logger.error(
                "put_conversation_state operation failed",
                extra={
                    "table_name": self.table_name,
                    "pk": partition_key,
                    "sk": sort_key,
                    "error": str(error),
                },
            )
            raise

    def update_system_response(
        self,
        partition_keys: List[str],
        whatsapp_id: str,
        system_response: Dict[str, Any],
        full_response: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Attach system response metadata to a stored message.

        Tries the provided partition key candidates in order until a matching
        message is found.
        """

        if not whatsapp_id or not system_response:
            return

        for partition_key in partition_keys:
            if not partition_key:
                continue

            try:
                response = self.table.query(
                    KeyConditionExpression=Key("PK").eq(partition_key),
                    FilterExpression=Attr("whatsapp_id").eq(whatsapp_id),
                    Limit=1,
                    ScanIndexForward=False,
                )
            except ClientError as error:
                logger.error(
                    "Failed to query message for system response attachment",
                    extra={
                        "table_name": self.table_name,
                        "pk": partition_key,
                        "whatsapp_id": whatsapp_id,
                        "error": str(error),
                    },
                )
                continue

            items = response.get("Items") if isinstance(response, dict) else None
            if not items:
                continue

            sort_key = items[0].get("SK")
            if not isinstance(sort_key, str):
                continue

            try:
                update_parts = ["system_response = :system_response"]
                expression_attribute_values: Dict[str, Any] = {
                    ":system_response": system_response
                }

                full_response_payload = full_response or system_response

                expression_attribute_names: Optional[Dict[str, str]] = None

                if full_response_payload is not None:
                    update_parts.extend(
                        ["#response = :response", "#system_response_full = :response"]
                    )
                    expression_attribute_values[":response"] = full_response_payload
                    expression_attribute_names = {
                        "#response": "Response",
                        "#system_response_full": "System_Response",
                    }

                update_expression = "SET " + ", ".join(update_parts)

                update_kwargs: Dict[str, Any] = {
                    "Key": {"PK": partition_key, "SK": sort_key},
                    "UpdateExpression": update_expression,
                    "ExpressionAttributeValues": expression_attribute_values,
                }
                if expression_attribute_names:
                    update_kwargs[
                        "ExpressionAttributeNames"
                    ] = expression_attribute_names

                self.table.update_item(**update_kwargs)
                return
            except ClientError as error:
                logger.error(
                    "Failed to update system response",
                    extra={
                        "table_name": self.table_name,
                        "pk": partition_key,
                        "sk": sort_key,
                        "whatsapp_id": whatsapp_id,
                        "error": str(error),
                    },
                )
                continue

    def get_customer_profile(
        self, normalized_phone: str, sort_key: str
    ) -> Optional[Dict[str, Any]]:
        """Return the stored customer profile for the supplied phone number."""

        partition_key = f"CUSTOMER#{normalized_phone}"
        try:
            response = self.table.get_item(Key={"PK": partition_key, "SK": sort_key})
        except ClientError as error:
            logger.error(
                "get_customer_profile operation failed",
                extra={
                    "table_name": self.table_name,
                    "pk": partition_key,
                    "sk": sort_key,
                    "error": str(error),
                },
            )
            raise

        return response.get("Item") if response else None

    def put_customer_profile(
        self, normalized_phone: str, profile: Dict[str, Any], sort_key: str
    ) -> None:
        """Persist customer profile data and related orders to DynamoDB."""

        partition_key = f"CUSTOMER#{normalized_phone}"
        orders: Iterable[Dict[str, Any]] = profile.get("הזמנות") or profile.get(
            "orders", []
        )

        try:
            with self.table.batch_writer() as batch:
                batch.put_item(
                    Item={
                        "PK": partition_key,
                        "SK": sort_key,
                        "profile": profile,
                        "last_updated_at": datetime.utcnow().isoformat(),
                    }
                )

                for index, order in enumerate(orders):
                    if not isinstance(order, dict):
                        continue
                    order_id = (
                        order.get("מספר_הזמנה")
                        or order.get("order_id")
                        or f"AUTO#{index + 1}"
                    )
                    batch.put_item(
                        Item={
                            "PK": partition_key,
                            "SK": f"ORDER#{order_id}",
                            "order": order,
                        }
                    )
        except ClientError as error:
            logger.error(
                "put_customer_profile operation failed",
                extra={
                    "table_name": self.table_name,
                    "pk": partition_key,
                    "error": str(error),
                },
            )
            raise
