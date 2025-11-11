# Built-in imports
import os
from typing import Any, Dict, Optional

DEFAULT_AGENT_FOUNDATION_MODEL_ID = "amazon.nova-lite-v1:0"
FALLBACK_AGENT_FOUNDATION_MODEL_ID = "amazon.nova-lite-v1:0"
MODELS_REQUIRING_INFERENCE_PROFILE = {
    "anthropic.claude-3-5-haiku-20241022-v1:0",
}

USERS_INFO_TABLE_DEFAULT_NAME = "UsersInfo"
USER_DATA_TABLE_DEFAULT_NAME = "UserData"

# External imports
from aws_cdk import (
    Duration,
    aws_bedrock,
    aws_dynamodb,
    aws_iam,
    aws_lambda,
    aws_lambda_event_sources,
    aws_logs,
    aws_opensearchserverless as oss,
    aws_ssm,
    aws_secretsmanager,
    aws_s3,
    aws_s3_deployment as s3d,
    aws_stepfunctions as aws_sfn,
    aws_stepfunctions_tasks as aws_sfn_tasks,
    aws_apigateway as aws_apigw,
    custom_resources as cr,
    CfnOutput,
    RemovalPolicy,
    Stack,
    Tags,
)
from constructs import Construct


class ChatbotAPIStack(Stack):
    """
    Class to create the ChatbotAPI resources, which includes the API Gateway,
    Lambda Functions, DynamoDB Table, Streams and Async Processes Infrastructure.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        main_resources_name: str,
        app_config: Dict[str, Any],
        **kwargs,
    ) -> None:
        """
        :param scope (Construct): Parent of this stack, usually an 'App' or a 'Stage', but could be any construct.
        :param construct_id (str): The construct ID of this stack (same as aws-cdk Stack 'construct_id').
        :param main_resources_name (str): The main unique identified of this stack.
        :param app_config (Dict[str, Any]): Dictionary with relevant configuration values for the stack.
        """
        super().__init__(scope, construct_id, **kwargs)

        # Input parameters
        self.construct_id = construct_id
        self.main_resources_name = main_resources_name
        self.app_config = app_config
        self.deployment_environment = self.app_config["deployment_environment"]

        # Parameter to enable/disable RAG
        self.enable_rag = self.app_config["enable_rag"]
        self.bedrock_agent_foundation_model_id = self.app_config.get(
            "bedrock_agent_foundation_model_id",
            DEFAULT_AGENT_FOUNDATION_MODEL_ID,
        )
        self.bedrock_agent_inference_profile_arn = self.app_config.get(
            "bedrock_agent_inference_profile_arn"
        )
        self.bedrock_agent_effective_foundation_model_id = (
            self._resolve_bedrock_foundation_model_id()
        )

        # Placeholder for optional resources initialised in later steps
        self.rules_dynamodb_table = None

        # Main methods for the deployment
        self.import_secrets()
        self.create_dynamodb_table()
        self.create_users_info_table()
        self.create_user_data_table()  # NEW: UserData table (+ Name GSI)
        self.create_lambda_layers()
        self.create_lambda_functions()
        self.create_dynamodb_streams()
        self.create_rest_api()
        self.configure_rest_api()
        self.create_state_machine_tasks()
        self.create_state_machine_definition()
        self.create_state_machine()
        self.create_bedrock_components()

        # Generate CloudFormation outputs
        self.generate_cloudformation_outputs()

    def import_secrets(self) -> None:
        """
        Method to import the AWS Secrets for the Lambda Functions.
        """
        self.secret_chatbot = aws_secretsmanager.Secret.from_secret_name_v2(
            self,
            "Secret-Chatbot",
            secret_name=self.app_config["secret_name"],
        )

    def create_dynamodb_table(self):
        """
        Create DynamoDB table for storing the conversations.
        """
        self.dynamodb_table = aws_dynamodb.Table(
            self,
            "DynamoDB-Table-Chatbot",
            table_name=self.app_config["table_name"],
            partition_key=aws_dynamodb.Attribute(
                name="PK", type=aws_dynamodb.AttributeType.STRING
            ),
            sort_key=aws_dynamodb.Attribute(
                name="SK", type=aws_dynamodb.AttributeType.STRING
            ),
            stream=aws_dynamodb.StreamViewType.NEW_IMAGE,
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )
        Tags.of(self.dynamodb_table).add("Name", self.app_config["table_name"])

    def create_users_info_table(self) -> None:
        """Create (or name-reserve) the UsersInfo DynamoDB table."""
        # Support both keys for backwards compatibility, preferring explicit USER_INFO_TABLE
        users_info_table_name = self.app_config.get(
            "users_info_table_name", USERS_INFO_TABLE_DEFAULT_NAME
        )
        users_info_table_name = self.app_config.get(
            "USER_INFO_TABLE", users_info_table_name
        )

        self.users_info_table = aws_dynamodb.Table(
            self,
            "DynamoDB-Table-UsersInfo",
            table_name=users_info_table_name,
            partition_key=aws_dynamodb.Attribute(
                name="PhoneNumber", type=aws_dynamodb.AttributeType.STRING
            ),
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )
        Tags.of(self.users_info_table).add("Name", users_info_table_name)

    def create_user_data_table(self) -> None:
        """Create UserData DynamoDB table (PK=PhoneNumber) and make 'Name' a queryable string via GSI."""
        # Allow override via either key
        user_data_table_name = self.app_config.get(
            "user_data_table_name", USER_DATA_TABLE_DEFAULT_NAME
        )
        user_data_table_name = self.app_config.get(
            "USER_DATA_TABLE", user_data_table_name
        )

        self.user_data_table = aws_dynamodb.Table(
            self,
            "DynamoDB-Table-UserData",
            table_name=user_data_table_name,
            partition_key=aws_dynamodb.Attribute(
                name="PhoneNumber", type=aws_dynamodb.AttributeType.STRING
            ),
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )
        Tags.of(self.user_data_table).add("Name", user_data_table_name)

        # --- NEW: make 'Name' (string) queryable with a GSI ---
        # You can still write 'Name' as a regular attribute with no schema,
        # but this index allows efficient queries by Name.
        self.user_data_table.add_global_secondary_index(
            index_name="NameIndex",
            partition_key=aws_dynamodb.Attribute(
                name="Name", type=aws_dynamodb.AttributeType.STRING
            ),
            projection_type=aws_dynamodb.ProjectionType.ALL,
        )

    def create_lambda_layers(self) -> None:
        """
        Create the Lambda layers that are necessary for the additional runtime
        dependencies of the Lambda Functions.
        """

        # Layer for "LambdaPowerTools" (for logging, traces, observability, etc)
        self.lambda_layer_powertools = aws_lambda.LayerVersion.from_layer_version_arn(
            self,
            "Layer-PowerTools",
            layer_version_arn=f"arn:aws:lambda:{self.region}:017000801446:layer:AWSLambdaPowertoolsPythonV2:71",
        )

        # Layer for "common" Python requirements (fastapi, mangum, pydantic, ...)
        self.lambda_layer_common = aws_lambda.LayerVersion(
            self,
            "Layer-Common",
            code=aws_lambda.Code.from_asset("lambda-layers/common/modules"),
            compatible_runtimes=[
                aws_lambda.Runtime.PYTHON_3_11,
            ],
            description="Lambda Layer for Python with <common> library",
            removal_policy=RemovalPolicy.DESTROY,
            compatible_architectures=[aws_lambda.Architecture.X86_64],
        )

    def create_lambda_functions(self) -> None:
        """
        Create the Lambda Functions for the solution.
        """
        # Get relative path for folder that contains Lambda function source
        # ! Note--> we must obtain parent dirs to create path (that"s why there is "os.path.dirname()")
        PATH_TO_LAMBDA_FUNCTION_FOLDER = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "backend",
        )

        # Lambda Function for WhatsApp input messages (Meta WebHook)
        self.lambda_whatsapp_webhook = aws_lambda.Function(
            self,
            "Lambda-WhatsApp-Webhook",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="whatsapp_webhook/api/v1/main.handler",
            function_name=f"{self.main_resources_name}-input",
            code=aws_lambda.Code.from_asset(PATH_TO_LAMBDA_FUNCTION_FOLDER),
            timeout=Duration.seconds(20),
            memory_size=512,
            environment={
                "ENVIRONMENT": self.app_config["deployment_environment"],
                "LOG_LEVEL": self.app_config["log_level"],
                "DYNAMODB_TABLE": self.dynamodb_table.table_name,
                "SECRET_NAME": self.app_config["secret_name"],
                "ASSESS_CHANGES_FEATURE": "true",
                "AWS_REGION": self.region,
            },
            layers=[
                self.lambda_layer_powertools,
                self.lambda_layer_common,
            ],
        )
        self.dynamodb_table.grant_read_write_data(self.lambda_whatsapp_webhook)
        self.secret_chatbot.grant_read(self.lambda_whatsapp_webhook)

        # Lambda Function for receiving the messages from DynamoDB Streams
        # ... and triggering the State Machine for processing the messages
        self.lambda_trigger_state_machine = aws_lambda.Function(
            self,
            "Lambda-Trigger-Message-Processing",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="trigger/trigger_handler.lambda_handler",
            function_name=f"{self.main_resources_name}-trigger-state-machine",
            code=aws_lambda.Code.from_asset(PATH_TO_LAMBDA_FUNCTION_FOLDER),
            timeout=Duration.seconds(20),
            memory_size=512,
            environment={
                "ENVIRONMENT": self.app_config["deployment_environment"],
                "LOG_LEVEL": self.app_config["log_level"],
                "ASSESS_CHANGES_FEATURE": "true",
                "AWS_REGION": self.region,
            },
            layers=[
                self.lambda_layer_powertools,
                self.lambda_layer_common,
            ],
        )

        # Lambda Function that will run the State Machine steps for processing the messages
        # TODO: In the future, can be migrated to MULTIPLE Lambda Functions for each step...
        self.lambda_state_machine_process_message = aws_lambda.Function(
            self,
            "Lambda-SM-Process-Message",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="state_machine/state_machine_handler.lambda_handler",
            function_name=f"{self.main_resources_name}-state-machine-lambda",
            code=aws_lambda.Code.from_asset(PATH_TO_LAMBDA_FUNCTION_FOLDER),
            timeout=Duration.seconds(60),
            memory_size=512,
            environment=self._build_state_machine_lambda_environment(),
            layers=[
                self.lambda_layer_powertools,
                self.lambda_layer_common,
            ],
        )
        self.secret_chatbot.grant_read(self.lambda_state_machine_process_message)
        self.dynamodb_table.grant_read_write_data(
            self.lambda_state_machine_process_message
        )
        if hasattr(self, "users_info_table"):
            self.users_info_table.grant_read_write_data(
                self.lambda_state_machine_process_message
            )
        if hasattr(self, "user_data_table"):
            self.user_data_table.grant_read_write_data(
                self.lambda_state_machine_process_message
            )
        if self.rules_dynamodb_table:
            self.rules_dynamodb_table.grant_read_data(
                self.lambda_state_machine_process_message
            )
        dynamodb_actions = [
            "dynamodb:GetItem",
            "dynamodb:PutItem",
            "dynamodb:UpdateItem",
            "dynamodb:DescribeTable",
        ]
        # Allow explicit access by ARN as well (in addition to grant helpers)
        resources_arns = []
        if hasattr(self, "users_info_table"):
            resources_arns.append(self.users_info_table.table_arn)
        if hasattr(self, "user_data_table"):
            resources_arns.append(self.user_data_table.table_arn)
        if resources_arns:
            self.lambda_state_machine_process_message.add_to_role_policy(
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
                    actions=dynamodb_actions,
                    resources=resources_arns,
                )
            )
        if self.rules_dynamodb_table:
            self.lambda_state_machine_process_message.add_to_role_policy(
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
                    actions=dynamodb_actions,
                    resources=[self.rules_dynamodb_table.table_arn],
                )
            )
        self.lambda_state_machine_process_message.role.add_managed_policy(
            aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonSSMReadOnlyAccess",
            ),
        )
        self.lambda_state_machine_process_message.role.add_managed_policy(
            aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonBedrockFullAccess",
            ),
        )
        self.lambda_state_machine_process_message.role.add_to_policy(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=["bedrock:InvokeAgent"],
                resources=[
                    f"arn:aws:bedrock:{self.region}:{self.account}:agent/*",
                    f"arn:aws:bedrock:{self.region}:{self.account}:agent-alias/*",
                ],
            )
        )

        # Lambda Function for the Bedrock Agent Group (fetch recipes)
        bedrock_agent_lambda_role = aws_iam.Role(
            self,
            "BedrockAgentLambdaRole",
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Role for Bedrock Agent Lambda",
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole",
                ),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonBedrockFullAccess",
                ),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonDynamoDBFullAccess",
                ),
            ],
        )

        # Lambda for the Action Group (used for Bedrock Agents)
        # Note: Single Lambda for all Action Groups for now...
        self.lambda_action_groups = aws_lambda.Function(
            self,
            "Lambda-AG-Generic",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="bedrock_agent/lambda_function.lambda_handler",
            function_name=f"{self.main_resources_name}-bedrock-action-groups",
            code=aws_lambda.Code.from_asset(PATH_TO_LAMBDA_FUNCTION_FOLDER),
            timeout=Duration.seconds(60),
            memory_size=512,
            environment={
                "ENVIRONMENT": self.app_config["deployment_environment"],
                "LOG_LEVEL": self.app_config["log_level"],
                "TABLE_NAME": self.app_config["agents_data_table_name"],
                "AWS_REGION": self.region,
            },
            role=bedrock_agent_lambda_role,
        )

    def _build_state_machine_lambda_environment(self) -> Dict[str, str]:
        """Compose environment variables for the state machine processor Lambda."""

        base_environment: Dict[str, str] = {
