# Built-in imports
import os
from typing import Any, Dict, Optional

# Bedrock model selection (Nova Lite)
DEFAULT_AGENT_FOUNDATION_MODEL_ID = "amazon.nova-lite-v1:0"
FALLBACK_AGENT_FOUNDATION_MODEL_ID = "amazon.nova-lite-v1:0"

# Models which require an inference profile (kept for future compatibility)
MODELS_REQUIRING_INFERENCE_PROFILE = {
    "anthropic.claude-3-5-haiku-20241022-v1:0",
}

# UsersInfo data model (table + attributes used by the app)
USERS_INFO_TABLE_DEFAULT_NAME = "UsersInfo"  # Table name (plural)
# Explicitly expose the model to Lambdas
USERS_INFO_PK_NAME = "PhoneNumber"  # Partition key (string)
USERS_INFO_NAME_ATTR = "Name"  # App-level attribute (string, not a key)

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
    Creates the Chatbot API resources: API Gateway, Lambdas, DynamoDB tables,
    Step Functions (V1 + V2), and optional Bedrock components.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        main_resources_name: str,
        app_config: Dict[str, Any],
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Input parameters
        self.construct_id = construct_id
        self.main_resources_name = main_resources_name
        self.app_config = app_config
        self.deployment_environment = self.app_config["deployment_environment"]

        # Feature flags and model/inference-profile config
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

        # Optional resources place-holders
        self.rules_dynamodb_table = None

        # Main flow
        self.import_secrets()
        self.create_dynamodb_table()
        self.create_users_info_table()
        self.create_lambda_layers()
        self.create_lambda_functions()
        self.create_dynamodb_streams()
        self.create_rest_api()
        self.configure_rest_api()
        self.create_state_machine_tasks()
        self.create_state_machine_definition()
        self.create_state_machine()
        self.create_bedrock_components()
        self.generate_cloudformation_outputs()

    # ---------------------------------------------------------------------
    # Secrets
    # ---------------------------------------------------------------------
    def import_secrets(self) -> None:
        self.secret_chatbot = aws_secretsmanager.Secret.from_secret_name_v2(
            self,
            "Secret-Chatbot",
            secret_name=self.app_config["secret_name"],
        )

    # ---------------------------------------------------------------------
    # DynamoDB tables
    # ---------------------------------------------------------------------
    def create_dynamodb_table(self) -> None:
        """Conversation/events table (PK/SK, streams enabled)."""
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
        """
        UsersInfo table with PK = PhoneNumber.
        If the table exists already with the same name, this will manage it by name.
        """
        users_info_table_name = self.app_config.get(
            "users_info_table_name", USERS_INFO_TABLE_DEFAULT_NAME
        )
        self.users_info_table = aws_dynamodb.Table(
            self,
            "DynamoDB-Table-UsersInfo",
            table_name=users_info_table_name,
            partition_key=aws_dynamodb.Attribute(
                name=USERS_INFO_PK_NAME, type=aws_dynamodb.AttributeType.STRING
            ),
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )
        Tags.of(self.users_info_table).add("Name", users_info_table_name)

    # ---------------------------------------------------------------------
    # Lambda layers
    # ---------------------------------------------------------------------
    def create_lambda_layers(self) -> None:
        self.lambda_layer_powertools = aws_lambda.LayerVersion.from_layer_version_arn(
            self,
            "Layer-PowerTools",
            layer_version_arn=(
                f"arn:aws:lambda:{self.region}:017000801446:"
                "layer:AWSLambdaPowertoolsPythonV2:71"
            ),
        )

        self.lambda_layer_common = aws_lambda.LayerVersion(
            self,
            "Layer-Common",
            code=aws_lambda.Code.from_asset("lambda-layers/common/modules"),
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_11],
            description="Lambda Layer for Python with <common> library",
            removal_policy=RemovalPolicy.DESTROY,
            compatible_architectures=[aws_lambda.Architecture.X86_64],
        )

    # ---------------------------------------------------------------------
    # Lambda functions
    # ---------------------------------------------------------------------
    def create_lambda_functions(self) -> None:
        base_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "backend",
        )

        # WhatsApp webhook Lambda
        self.lambda_whatsapp_webhook = aws_lambda.Function(
            self,
            "Lambda-WhatsApp-Webhook",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="whatsapp_webhook/api/v1/main.handler",
            function_name=f"{self.main_resources_name}-input",
            code=aws_lambda.Code.from_asset(base_path),
            timeout=Duration.seconds(20),
            memory_size=512,
            environment={
                "ENVIRONMENT": self.app_config["deployment_environment"],
                "LOG_LEVEL": self.app_config["log_level"],
                "DYNAMODB_TABLE": self.dynamodb_table.table_name,
                "SECRET_NAME": self.app_config["secret_name"],
            },
            layers=[self.lambda_layer_powertools, self.lambda_layer_common],
        )
        self.dynamodb_table.grant_read_write_data(self.lambda_whatsapp_webhook)
        self.secret_chatbot.grant_read(self.lambda_whatsapp_webhook)

        # Stream trigger Lambda
        self.lambda_trigger_state_machine = aws_lambda.Function(
            self,
            "Lambda-Trigger-Message-Processing",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="trigger/trigger_handler.lambda_handler",
            function_name=f"{self.main_resources_name}-trigger-state-machine",
            code=aws_lambda.Code.from_asset(base_path),
            timeout=Duration.seconds(20),
            memory_size=512,
            environment={
                "ENVIRONMENT": self.app_config["deployment_environment"],
                "LOG_LEVEL": self.app_config["log_level"],
            },
            layers=[self.lambda_layer_powertools, self.lambda_layer_common],
        )

        # State machine step runner Lambda
        self.lambda_state_machine_process_message = aws_lambda.Function(
            self,
            "Lambda-SM-Process-Message",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="state_machine/state_machine_handler.lambda_handler",
            function_name=f"{self.main_resources_name}-state-machine-lambda",
            code=aws_lambda.Code.from_asset(base_path),
            timeout=Duration.seconds(60),
            memory_size=512,
            environment=self._build_state_machine_lambda_environment(),
            layers=[self.lambda_layer_powertools, self.lambda_layer_common],
        )
        self.secret_chatbot.grant_read(self.lambda_state_machine_process_message)
        self.dynamodb_table.grant_read_write_data(
            self.lambda_state_machine_process_message
        )
        if hasattr(self, "users_info_table"):
            self.users_info_table.grant_read_write_data(
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
        self.lambda_state_machine_process_message.add_to_role_policy(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=dynamodb_actions,
                resources=[self.users_info_table.table_arn],
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
                "AmazonSSMReadOnlyAccess"
            )
        )
        self.lambda_state_machine_process_message.role.add_managed_policy(
            aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonBedrockFullAccess"
            )
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

        # Bedrock Action Groups Lambda + role
        bedrock_agent_lambda_role = aws_iam.Role(
            self,
            "BedrockAgentLambdaRole",
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Role for Bedrock Agent Lambda",
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonBedrockFullAccess"
                ),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonDynamoDBFullAccess"
                ),
            ],
        )

        self.lambda_action_groups = aws_lambda.Function(
            self,
            "Lambda-AG-Generic",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="bedrock_agent/lambda_function.lambda_handler",
            function_name=f"{self.main_resources_name}-bedrock-action-groups",
            code=aws_lambda.Code.from_asset(base_path),
            timeout=Duration.seconds(60),
            memory_size=512,
            environment={
                "ENVIRONMENT": self.app_config["deployment_environment"],
                "LOG_LEVEL": self.app_config["log_level"],
                "TABLE_NAME": self.app_config["agents_data_table_name"],
            },
            role=bedrock_agent_lambda_role,
        )

    def _build_state_machine_lambda_environment(self) -> Dict[str, str]:
        """
        Compose environment variables for the state machine processor Lambda.
        Adds UsersInfo data model attributes and forces AssessChanges feature ON by default.
        """
        base_environment: Dict[str, str] = {
            "ENVIRONMENT": self.app_config["deployment_environment"],
            "LOG_LEVEL": self.app_config["log_level"],
            "SECRET_NAME": self.app_config["secret_name"],
            "META_ENDPOINT": self.app_config["meta_endpoint"],
            # Default to ON so AssessChanges runs in direct Lambda invokes, too
            "ASSESS_CHANGES_FEATURE": self.app_config.get(
                "ASSESS_CHANGES_FEATURE",
                "on",
            ),
            "USER_INFO_TABLE": self.app_config.get(
                "USER_INFO_TABLE",
                self.app_config.get(
                    "users_info_table_name",
                    USERS_INFO_TABLE_DEFAULT_NAME,
                ),
            ),
            # Expose UsersInfo data model explicitly
            "USER_INFO_PK_NAME": USERS_INFO_PK_NAME,  # "PhoneNumber"
            "USER_INFO_NAME_ATTRIBUTE": USERS_INFO_NAME_ATTR,  # "Name"
        }

        optional_values: Dict[str, Optional[str]] = {
            "AGENT_ID": self.app_config.get("bedrock_agent_id"),
            "BEDROCK_AGENT_ID": self.app_config.get("bedrock_agent_id"),
            "AGENT_ALIAS_ID": self.app_config.get("bedrock_agent_alias_id"),
            "BEDROCK_AGENT_ALIAS_ID": self.app_config.get("bedrock_agent_alias_id"),
            "USER_INFO_TABLE": (
                self.users_info_table.table_name
                if hasattr(self, "users_info_table")
                else None
            ),
        }
        for key, value in optional_values.items():
            if value:
                base_environment[key] = value

        optional_rules_environment = {
            "RULES_TABLE_NAME": self.app_config.get("rules_table_name"),
            "RULESET_ID": self.app_config.get("ruleset_id"),
            "RULESET_VERSION": self.app_config.get("ruleset_version"),
        }
        for key, value in optional_rules_environment.items():
            if value:
                base_environment[key] = value

        if self.app_config.get("RULES_TABLE"):
            base_environment["RULES_TABLE"] = self.app_config["RULES_TABLE"]
        elif self.rules_dynamodb_table:
            base_environment["RULES_TABLE"] = self.rules_dynamodb_table.table_name

        return base_environment

    # ---------------------------------------------------------------------
    # DynamoDB streams to trigger state machine
    # ---------------------------------------------------------------------
    def create_dynamodb_streams(self) -> None:
        self.lambda_trigger_state_machine.add_event_source(
            aws_lambda_event_sources.DynamoEventSource(
                self.dynamodb_table,
                starting_position=aws_lambda.StartingPosition.TRIM_HORIZON,
                batch_size=1,
            )
        )

    # ---------------------------------------------------------------------
    # REST API
    # ---------------------------------------------------------------------
    def create_rest_api(self):
        self.api_method_options_public = aws_apigw.MethodOptions(
            api_key_required=False,
            authorization_type=aws_apigw.AuthorizationType.NONE,
        )
        rest_api_name = self.app_config["api_gw_name"]
        self.api = aws_apigw.LambdaRestApi(
            self,
            "RESTAPI",
            rest_api_name=rest_api_name,
            description=(
                f"REST API Gateway for {self.main_resources_name} in "
                f"{self.deployment_environment} environment"
            ),
            handler=self.lambda_whatsapp_webhook,
            deploy_options=aws_apigw.StageOptions(
                stage_name=self.deployment_environment,
                description=f"REST API for {self.main_resources_name}",
                metrics_enabled=True,
            ),
            default_cors_preflight_options=aws_apigw.CorsOptions(
                allow_origins=aws_apigw.Cors.ALL_ORIGINS,
                allow_methods=["GET", "POST"],
                allow_headers=["*"],
            ),
            default_method_options=self.api_method_options_public,
            endpoint_types=[aws_apigw.EndpointType.REGIONAL],
            cloud_watch_role=False,
            proxy=False,
        )
        # Remove output Endpoint
        self.api.node.try_remove_child("Endpoint")

    def configure_rest_api(self):
        root_resource_api = self.api.root.add_resource("api")
        root_resource_v1 = root_resource_api.add_resource("v1")

        root_resource_docs = root_resource_v1.add_resource("docs")
        root_resource_docs_proxy = root_resource_docs.add_resource("{path}")

        root_resource_chatbot = root_resource_v1.add_resource("webhook")
        api_lambda_integration_chatbot = aws_apigw.LambdaIntegration(
            self.lambda_whatsapp_webhook
        )

        # /api/v1/webhook
        root_resource_chatbot.add_method("GET", api_lambda_integration_chatbot)
        root_resource_chatbot.add_method("POST", api_lambda_integration_chatbot)

        # /api/v1/docs
        root_resource_docs.add_method("GET", api_lambda_integration_chatbot)
        # /api/v1/docs/openapi.json
        root_resource_docs_proxy.add_method("GET", api_lambda_integration_chatbot)

    # ---------------------------------------------------------------------
    # Step Functions - tasks
    # ---------------------------------------------------------------------
    def create_state_machine_tasks(self) -> None:
        # V1
        self.task_adapt_message = aws_sfn_tasks.LambdaInvoke(
            self,
            "Task-Adapter",
            state_name="Adapt Message",
            lambda_function=self.lambda_state_machine_process_message,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "Adapter",
                        "method_name": "transform_input",
                    },
                }
            ),
            output_path="$.Payload",
        )
        self.task_validate_message = aws_sfn_tasks.LambdaInvoke(
            self,
            "Task-ValidateMessage",
            state_name="Validate Message",
            lambda_function=self.lambda_state_machine_process_message,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "ValidateMessage",
                        "method_name": "validate_input",
                    },
                }
            ),
            output_path="$.Payload",
        )
        self.task_pass_text = aws_sfn.Pass(
            self, "Task-Text", comment="Text message", state_name="Text"
        )
        self.task_pass_voice = aws_sfn.Pass(
            self, "Task-Voice", comment="Voice message", state_name="Voice"
        )
        self.task_pass_image = aws_sfn.Pass(
            self, "Task-Image", comment="Image message", state_name="Image"
        )
        self.task_pass_video = aws_sfn.Pass(
            self, "Task-Video", comment="Video message", state_name="Video"
        )
        self.task_process_text = aws_sfn_tasks.LambdaInvoke(
            self,
            "Task-ProcessText",
            state_name="Process Text",
            lambda_function=self.lambda_state_machine_process_message,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "ProcessText",
                        "method_name": "process_text",
                    },
                }
            ),
            output_path="$.Payload",
        )
        self.task_process_voice = aws_sfn_tasks.LambdaInvoke(
            self,
            "Task-ProcessVoice",
            state_name="Process Voice",
            lambda_function=self.lambda_state_machine_process_message,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "ProcessVoice",
                        "method_name": "process_voice",
                    },
                }
            ),
            output_path="$.Payload",
        )
        self.task_send_message = aws_sfn_tasks.LambdaInvoke(
            self,
            "Task-SendMessage",
            state_name="Send Message",
            lambda_function=self.lambda_state_machine_process_message,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "SendMessage",
                        "method_name": "send_message",
                    },
                }
            ),
            output_path="$.Payload",
        )
        self.task_not_implemented = aws_sfn.Pass(
            self, "Task-NotImplemented", comment="Not implemented yet"
        )
        self.task_process_success = aws_sfn_tasks.LambdaInvoke(
            self,
            "Task-Success",
            state_name="Process Success",
            lambda_function=self.lambda_state_machine_process_message,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "Success",
                        "method_name": "process_success",
                    },
                }
            ),
            output_path="$.Payload",
        )
        self.task_process_failure = aws_sfn_tasks.LambdaInvoke(
            self,
            "Task-Failure",
            state_name="Process Failure",
            lambda_function=self.lambda_state_machine_process_message,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "Failure",
                        "method_name": "process_failure",
                    },
                }
            ),
            output_path="$.Payload",
        )
        self.task_success = aws_sfn.Succeed(
            self, id="Succeed", comment="Successful execution of State Machine"
        )
        self.task_failure = aws_sfn.Fail(
            self,
            id="Exception Handling Finished",
            comment="State Machine Exception or Failure",
        )

        # ---------------- V2 tasks ----------------
        self.v2_task_adapt_input = aws_sfn.Pass(
            self,
            "TaskV2-AdaptInput",
            state_name="AdaptInput",
            parameters={
                "input.$": "$.input",
                "dynamodb": {
                    "from_number": {"S.$": "$.input.from"},
                    "to_number": {"S.$": "$.input.to"},
                    "type": {"S.$": "$.input.message_type"},
                    "text": {"S.$": "$.input.message_body"},
                    "whatsapp_id": {"S.$": "$.input.wa_id"},
                    "last_seen_at": {"S.$": "$.input.last_seen_at"},
                },
            },
        )
        self.v2_task_adapt_message = aws_sfn_tasks.LambdaInvoke(
            self,
            "TaskV2-Adapter",
            state_name="Adapt Message",
            lambda_function=self.lambda_state_machine_process_message,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "Adapter",
                        "method_name": "transform_input",
                    },
                }
            ),
            output_path="$.Payload",
        )
        self.v2_task_validate_message = aws_sfn_tasks.LambdaInvoke(
            self,
            "TaskV2-ValidateMessage",
            state_name="Validate Message",
            lambda_function=self.lambda_state_machine_process_message,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "ValidateMessage",
                        "method_name": "validate_input",
                    },
                }
            ),
            output_path="$.Payload",
        )
        self.v2_task_pass_text = aws_sfn.Pass(
            self,
            "TaskV2-Text",
            comment="Indicates that the message type is Text",
            state_name="Text",
        )
        # Force the feature flag ON so the flow always reaches Assess Changes
        self.v2_enable_assess_changes = aws_sfn.Pass(
            self,
            "TaskV2-EnableAssessChanges",
            state_name="Enable Assess Changes",
            parameters={"assess_changes": "on"},
            result_path="$.features",
        )
        self.v2_task_pass_voice = aws_sfn.Pass(
            self,
            "TaskV2-Voice",
            comment="Indicates that the message type is Voice",
            state_name="Voice",
        )
        self.v2_task_pass_image = aws_sfn.Pass(
            self,
            "TaskV2-Image",
            comment="Indicates that the message type is Image",
            state_name="Image",
        )
        self.v2_task_pass_video = aws_sfn.Pass(
            self,
            "TaskV2-Video",
            comment="Indicates that the message type is Video",
            state_name="Video",
        )
        self.v2_task_assess_changes = aws_sfn_tasks.LambdaInvoke(
            self,
            "TaskV2-AssessChanges",
            state_name="Assess Changes",
            lambda_function=self.lambda_state_machine_process_message,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "AssessChanges",
                        "method_name": "assess_and_apply",
                    },
                }
            ),
            output_path="$.Payload",
        )
        self.v2_task_process_text = aws_sfn_tasks.LambdaInvoke(
            self,
            "TaskV2-ProcessText",
            state_name="Process Text",
            lambda_function=self.lambda_state_machine_process_message,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "ProcessText",
                        "method_name": "process_text",
                    },
                }
            ),
            output_path="$.Payload",
        )
        self.v2_task_process_voice = aws_sfn_tasks.LambdaInvoke(
            self,
            "TaskV2-ProcessVoice",
            state_name="Process Voice",
            lambda_function=self.lambda_state_machine_process_message,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "ProcessVoice",
                        "method_name": "process_voice",
                    },
                }
            ),
            output_path="$.Payload",
        )
        self.v2_task_send_message = aws_sfn_tasks.LambdaInvoke(
            self,
            "TaskV2-SendMessage",
            state_name="Send Message",
            lambda_function=self.lambda_state_machine_process_message,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "SendMessage",
                        "method_name": "send_message",
                    },
                }
            ),
            output_path="$.Payload",
        )
        self.v2_task_not_implemented = aws_sfn.Pass(
            self, "TaskV2-NotImplemented", comment="Not implemented yet"
        )
        self.v2_task_process_success = aws_sfn_tasks.LambdaInvoke(
            self,
            "TaskV2-Success",
            state_name="Process Success",
            lambda_function=self.lambda_state_machine_process_message,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "Success",
                        "method_name": "process_success",
                    },
                }
            ),
            output_path="$.Payload",
        )
        self.v2_task_process_failure = aws_sfn_tasks.LambdaInvoke(
            self,
            "TaskV2-Failure",
            state_name="Process Failure",
            lambda_function=self.lambda_state_machine_process_message,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "Failure",
                        "method_name": "process_failure",
                    },
                }
            ),
            output_path="$.Payload",
        )
        self.v2_task_success = aws_sfn.Succeed(
            self, id="SucceedV2", comment="Successful execution of State Machine V2"
        )
        self.v2_task_failure = aws_sfn.Fail(
            self,
            id="ExceptionHandlingFinishedV2",
            comment="State Machine V2 Exception or Failure",
        )

    # ---------------------------------------------------------------------
    # Step Functions - definitions
    # ---------------------------------------------------------------------
    def create_state_machine_definition(self) -> None:
        # V1 definition
        self.choice_text = aws_sfn.Condition.string_equals("$.message_type", "text")
        self.choice_image = aws_sfn.Condition.string_equals("$.message_type", "image")
        self.choice_video = aws_sfn.Condition.string_equals("$.message_type", "video")
        self.choice_voice = aws_sfn.Condition.string_equals("$.message_type", "voice")

        self.state_machine_definition = self.task_adapt_message.next(
            self.task_validate_message.next(
                aws_sfn.Choice(self, "Message Type?")
                .when(self.choice_text, self.task_pass_text)
                .when(self.choice_voice, self.task_pass_voice)
                .when(self.choice_image, self.task_pass_image)
                .when(self.choice_video, self.task_pass_video)
            )
        )
        self.task_pass_text.next(self.task_process_text.next(self.task_send_message))
        self.task_pass_voice.next(self.task_process_voice.next(self.task_pass_text))
        self.task_pass_image.next(self.task_not_implemented)
        self.task_pass_video.next(self.task_not_implemented)
        self.task_not_implemented.next(self.task_send_message)
        self.task_send_message.next(self.task_process_success)
        self.task_process_success.next(self.task_success)

        # V2 definition
        self.choice_text_v2 = aws_sfn.Condition.string_equals("$.message_type", "text")
        self.choice_image_v2 = aws_sfn.Condition.string_equals(
            "$.message_type", "image"
        )
        self.choice_video_v2 = aws_sfn.Condition.string_equals(
            "$.message_type", "video"
        )
        self.choice_voice_v2 = aws_sfn.Condition.string_equals(
            "$.message_type", "voice"
        )

        self.assess_changes_enabled_condition = aws_sfn.Condition.string_equals(
            "$.features.assess_changes",
            "on",
        )

        self.state_machine_definition_v2 = self.v2_task_adapt_input.next(
            self.v2_task_adapt_message.next(
                self.v2_task_validate_message.next(
                    aws_sfn.Choice(self, "Message Type? V2")
                    .when(self.choice_text_v2, self.v2_task_pass_text)
                    .when(self.choice_voice_v2, self.v2_task_pass_voice)
                    .when(self.choice_image_v2, self.v2_task_pass_image)
                    .when(self.choice_video_v2, self.v2_task_pass_video)
                )
            )
        )

        # Choice to route via Assess Changes when feature flag is on
        self.v2_choice_assess_changes = aws_sfn.Choice(
            self,
            "Assess Changes Enabled?",
            comment="Routes through AssessChanges when feature flag is enabled",
        )
        self.v2_choice_assess_changes.when(
            self.assess_changes_enabled_condition,
            self.v2_task_assess_changes.next(self.v2_task_process_text),
        )
        self.v2_choice_assess_changes.otherwise(self.v2_task_process_text)

        # Force assess_changes flag ON before the Choice
        self.v2_task_pass_text.next(
            self.v2_enable_assess_changes.next(self.v2_choice_assess_changes)
        )
        self.v2_task_process_text.next(self.v2_task_send_message)
        self.v2_task_pass_voice.next(
            self.v2_task_process_voice.next(self.v2_task_pass_text)
        )
        self.v2_task_pass_image.next(self.v2_task_not_implemented)
        self.v2_task_pass_video.next(self.v2_task_not_implemented)
        self.v2_task_not_implemented.next(self.v2_task_send_message)
        self.v2_task_send_message.next(self.v2_task_process_success)
        self.v2_task_process_success.next(self.v2_task_success)

    # ---------------------------------------------------------------------
    # Step Functions - state machines
    # ---------------------------------------------------------------------
    def create_state_machine(self) -> None:
        # V1
        log_group_name = f"/aws/vendedlogs/states/{self.main_resources_name}"
        existing_log_group = self.node.try_find_child("StateMachine-LogGroup")
        if existing_log_group and isinstance(existing_log_group, aws_logs.LogGroup):
            self.state_machine_log_group = existing_log_group
        else:
            self.state_machine_log_group = aws_logs.LogGroup(
                self,
                "StateMachine-LogGroup",
                log_group_name=log_group_name,
                removal_policy=RemovalPolicy.DESTROY,
            )
            Tags.of(self.state_machine_log_group).add("Name", log_group_name)

        self.state_machine = aws_sfn.StateMachine(
            self,
            "StateMachine-ProcessMessage",
            state_machine_name=f"{self.main_resources_name}-process-message",
            state_machine_type=aws_sfn.StateMachineType.EXPRESS,
            definition_body=aws_sfn.DefinitionBody.from_chainable(
                self.state_machine_definition
            ),
            logs=aws_sfn.LogOptions(
                destination=self.state_machine_log_group,
                include_execution_data=True,
                level=aws_sfn.LogLevel.ALL,
            ),
        )

        # V2
        log_group_name_v2 = (
            f"/aws/vendedlogs/states/{self.main_resources_name}-process-message-v2"
        )
        existing_log_group_v2 = self.node.try_find_child("StateMachine-LogGroupV2")
        if existing_log_group_v2 and isinstance(
            existing_log_group_v2, aws_logs.LogGroup
        ):
            self.state_machine_log_group_v2 = existing_log_group_v2
        else:
            self.state_machine_log_group_v2 = aws_logs.LogGroup(
                self,
                "StateMachine-LogGroupV2",
                log_group_name=log_group_name_v2,
                removal_policy=RemovalPolicy.DESTROY,
            )
            Tags.of(self.state_machine_log_group_v2).add("Name", log_group_name_v2)

        self.state_machine_v2 = aws_sfn.StateMachine(
            self,
            "StateMachine-ProcessMessageV2",
            state_machine_name=f"{self.main_resources_name}-process-message-v2",
            state_machine_type=aws_sfn.StateMachineType.EXPRESS,
            definition_body=aws_sfn.DefinitionBody.from_chainable(
                self.state_machine_definition_v2
            ),
            logs=aws_sfn.LogOptions(
                destination=self.state_machine_log_group_v2,
                include_execution_data=True,
                level=aws_sfn.LogLevel.ALL,
            ),
            role=self.state_machine.role,
        )

        # Grant starts
        self.state_machine.grant_start_execution(self.lambda_trigger_state_machine)
        self.state_machine_v2.grant_start_execution(self.lambda_whatsapp_webhook)
        self.state_machine_v2.grant_start_execution(self.lambda_trigger_state_machine)

        # Env to trigger functions
        self.lambda_trigger_state_machine.add_environment(
            "STATE_MACHINE_V1_ARN",
            self.state_machine.state_machine_arn,
        )
        self.lambda_trigger_state_machine.add_environment(
            "STATE_MACHINE_ARN",
            self.state_machine_v2.state_machine_arn,
        )
        self.lambda_trigger_state_machine.add_environment(
            "ENABLE_STREAM_TRIGGER", "off"
        )
        self.lambda_whatsapp_webhook.add_environment(
            "STATE_MACHINE_ARN", self.state_machine_v2.state_machine_arn
        )

    # ---------------------------------------------------------------------
    # Bedrock components (optional KB + agent)
    # ---------------------------------------------------------------------
    def create_bedrock_components(self) -> None:
        # Paths
        path_to_kb_folder = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "knowledge_base",
        )
        path_to_custom_resources = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "custom_resources",
        )

        # Agents data table
        self.agents_data_dynamodb_table = aws_dynamodb.Table(
            self,
            "DynamoDB-Table-AgentsData",
            table_name=self.app_config["agents_data_table_name"],
            partition_key=aws_dynamodb.Attribute(
                name="PK", type=aws_dynamodb.AttributeType.STRING
            ),
            sort_key=aws_dynamodb.Attribute(
                name="SK", type=aws_dynamodb.AttributeType.STRING
            ),
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )
        Tags.of(self.agents_data_dynamodb_table).add(
            "Name", self.app_config["agents_data_table_name"]
        )

        # Optional rules table
        rules_table_name = self.app_config.get("rules_table_name")
        if rules_table_name:
            self.rules_dynamodb_table = aws_dynamodb.Table(
                self,
                "DynamoDB-Table-Rules",
                table_name=rules_table_name,
                partition_key=aws_dynamodb.Attribute(
                    name="PK", type=aws_dynamodb.AttributeType.STRING
                ),
                sort_key=aws_dynamodb.Attribute(
                    name="SK", type=aws_dynamodb.AttributeType.STRING
                ),
                billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
                removal_policy=RemovalPolicy.DESTROY,
            )
            Tags.of(self.rules_dynamodb_table).add("Name", rules_table_name)
        else:
            self.rules_dynamodb_table = None

        # Allow bedrock to call action-groups lambda
        self.lambda_action_groups.add_permission(
            "AllowBedrock",
            principal=aws_iam.ServicePrincipal("bedrock.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"arn:aws:bedrock:{self.region}:{self.account}:agent/*",
        )

        # Bedrock Agent role
        bedrock_agent_role = aws_iam.Role(
            self,
            "BedrockAgentRole",
            assumed_by=aws_iam.ServicePrincipal("bedrock.amazonaws.com"),
            description="Role for Bedrock Agent",
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonBedrockFullAccess"
                ),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AWSLambda_FullAccess"
                ),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "CloudWatchLogsFullAccess"
                ),
            ],
        )
        self.users_info_table.grant_read_write_data(bedrock_agent_role)
        bedrock_agent_role.add_to_policy(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=[
                    "bedrock:InvokeModel",
                    "bedrock:InvokeModelWithResponseStream",
                    "bedrock:InvokeModelEndpoint",
                    "bedrock:InvokeModelEndpointAsync",
                ],
                resources=["*"],
            )
        )

        if self.enable_rag:
            # KB bucket
            s3_bucket_kb = aws_s3.Bucket(
                self,
                "S3-KB",
                bucket_name=f"{self.main_resources_name}-kb-assets-{self.account}",
                auto_delete_objects=True,
                versioned=True,
                encryption=aws_s3.BucketEncryption.S3_MANAGED,
                block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
                removal_policy=RemovalPolicy.DESTROY,
            )
            s3_bucket_kb.grant_read_write(
                aws_iam.ServicePrincipal("bedrock.amazonaws.com")
            )

            s3d.BucketDeployment(
                self,
                "S3Upload-KB",
                sources=[s3d.Source.asset(path_to_kb_folder)],
                destination_bucket=s3_bucket_kb,
                destination_key_prefix="docs/",
            )

            # OSS policies
            enc_pol = oss.CfnSecurityPolicy(
                self,
                "OpenSearchServerlessEncryptionPolicy",
                name="encryption-policy",
                policy='{"Rules":[{"ResourceType":"collection","Resource":["collection/*"]}],"AWSOwnedKey":true}',
                type="encryption",
                description="Encryption policy for the opensearch serverless collection",
            )
            net_pol = oss.CfnSecurityPolicy(
                self,
                "OpenSearchServerlessNetworkPolicy",
                name="network-policy",
                policy=(
                    '[{"Description":"Public access for collection","Rules":[{"ResourceType":"dashboard",'
                    '"Resource":["collection/*"]},{"ResourceType":"collection","Resource":["collection/*"]}],"AllowFromPublic":true}]'
                ),
                type="network",
                description="Network policy for the opensearch serverless collection",
            )

            oss_collection = oss.CfnCollection(
                self,
                "OpenSearchCollection-KB",
                name="pdf-collection",
                description="Collection for the PDF documents",
                standby_replicas="DISABLED",
                type="VECTORSEARCH",
            )
            oss_collection.add_dependency(enc_pol)
            oss_collection.add_dependency(net_pol)

            # KB role
            bedrock_kb_role = aws_iam.Role(
                self,
                "IAMRole-BedrockKB",
                role_name=f"{self.main_resources_name}-bedrock-kb-role",
                assumed_by=aws_iam.ServicePrincipal("bedrock.amazonaws.com"),
                managed_policies=[
                    aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                        "AmazonBedrockFullAccess"
                    ),
                    aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                        "AmazonOpenSearchServiceFullAccess"
                    ),
                    aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                        "AmazonS3FullAccess"
                    ),
                    aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                        "CloudWatchLogsFullAccess"
                    ),
                    # NOTE: For troubleshooting only. Remove in production.
                    aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                        "AdministratorAccess"
                    ),
                ],
            )

            # Custom resource to create an index
            index_name = "kb-docs"
            create_index_lambda = aws_lambda.Function(
                self,
                "Index",
                runtime=aws_lambda.Runtime.PYTHON_3_11,
                handler="create_oss_index.handler",
                code=aws_lambda.Code.from_asset(path_to_custom_resources),
                timeout=Duration.seconds(300),
                environment={
                    "COLLECTION_ENDPOINT": oss_collection.attr_collection_endpoint,
                    "INDEX_NAME": index_name,
                    "REGION": self.region,
                },
                layers=[self.lambda_layer_common],
            )
            create_index_lambda.role.add_to_principal_policy(
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        "es:ESHttpPut",
                        "es:*",
                        "iam:CreateServiceLinkedRole",
                        "iam:PassRole",
                        "iam:ListUsers",
                        "iam:ListRoles",
                        "aoss:*",
                    ],
                    resources=["*"],
                )
            )
            # Access policy
            access_pol = oss.CfnAccessPolicy(
                self,
                "OpenSearchServerlessAccessPolicy",
                name=f"{self.main_resources_name}-data-access-policy",
                policy=(
                    f'[{{"Description":"Access for bedrock","Rules":[{{"ResourceType":"index","Resource":["index/*/*"],'
                    f'"Permission":["aoss:*"]}},{{"ResourceType":"collection","Resource":["collection/*"],"Permission":["aoss:*"]}}],'
                    f'"Principal":["{bedrock_agent_role.role_arn}","{bedrock_kb_role.role_arn}",'
                    f'"{create_index_lambda.role.role_arn}","arn:aws:iam::{self.account}:root"]}}]'
                ),
                type="data",
                description="Data access policy for the opensearch serverless collection",
            )
            oss_collection.add_dependency(access_pol)

            # Trigger index create
            aoss_lambda_params = {
                "FunctionName": create_index_lambda.function_name,
                "InvocationType": "RequestResponse",
            }
            trigger_lambda_cr = cr.AwsCustomResource(
                self,
                "IndexCreateCustomResource",
                on_create=cr.AwsSdkCall(
                    service="Lambda",
                    action="invoke",
                    parameters=aoss_lambda_params,
                    physical_resource_id=cr.PhysicalResourceId.of("Parameter.ARN"),
                ),
                policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                    resources=cr.AwsCustomResourcePolicy.ANY_RESOURCE
                ),
                removal_policy=RemovalPolicy.DESTROY,
                timeout=Duration.seconds(300),
            )
            trigger_lambda_cr.node.add_dependency(access_pol)
            trigger_lambda_cr.node.add_dependency(oss_collection)

            # Knowledge base
            bedrock_knowledge_base = aws_bedrock.CfnKnowledgeBase(
                self,
                "BedrockKB",
                name="kbdocs",
                description=(
                    "Bedrock knowledge base that contains relevant projects "
                    "for the user."
                ),
                role_arn=bedrock_kb_role.role_arn,
                knowledge_base_configuration=aws_bedrock.CfnKnowledgeBase.KnowledgeBaseConfigurationProperty(
                    type="VECTOR",
                    vector_knowledge_base_configuration=aws_bedrock.CfnKnowledgeBase.VectorKnowledgeBaseConfigurationProperty(
                        embedding_model_arn=(
                            f"arn:aws:bedrock:{self.region}::foundation-model/"
                            "amazon.titan-embed-text-v1"
                        )
                    ),
                ),
                storage_configuration=aws_bedrock.CfnKnowledgeBase.StorageConfigurationProperty(
                    type="OPENSEARCH_SERVERLESS",
                    opensearch_serverless_configuration=aws_bedrock.CfnKnowledgeBase.OpenSearchServerlessConfigurationProperty(
                        collection_arn=oss_collection.attr_arn,
                        vector_index_name=index_name,
                        field_mapping=aws_bedrock.CfnKnowledgeBase.OpenSearchServerlessFieldMappingProperty(
                            metadata_field="AMAZON_BEDROCK_METADATA",
                            text_field="AMAZON_BEDROCK_TEXT_CHUNK",
                            vector_field="bedrock-knowledge-base-default-vector",
                        ),
                    ),
                ),
            )
            bedrock_knowledge_base.add_dependency(oss_collection)
            bedrock_knowledge_base.node.add_dependency(trigger_lambda_cr)

            # Data source
            bedrock_data_source = aws_bedrock.CfnDataSource(
                self,
                "Bedrock-DataSource",
                name="KbDataSource",
                knowledge_base_id=bedrock_knowledge_base.ref,
                description=(
                    "The S3 data source definition for the bedrock knowledge base."
                ),
                data_source_configuration=aws_bedrock.CfnDataSource.DataSourceConfigurationProperty(
                    s3_configuration=aws_bedrock.CfnDataSource.S3DataSourceConfigurationProperty(
                        bucket_arn=s3_bucket_kb.bucket_arn,
                        inclusion_prefixes=["docs"],
                    ),
                    type="S3",
                ),
                vector_ingestion_configuration=aws_bedrock.CfnDataSource.VectorIngestionConfigurationProperty(
                    chunking_configuration=aws_bedrock.CfnDataSource.ChunkingConfigurationProperty(
                        chunking_strategy="FIXED_SIZE",
                        fixed_size_chunking_configuration=aws_bedrock.CfnDataSource.FixedSizeChunkingConfigurationProperty(
                            max_tokens=300, overlap_percentage=20
                        ),
                    )
                ),
            )
            bedrock_data_source.node.add_dependency(bedrock_knowledge_base)

        # Bedrock Agent (Nova Lite)
        foundation_model_identifier = (
            self.bedrock_agent_inference_profile_arn
            or self.bedrock_agent_foundation_model_id
        )
        self.bedrock_agent = aws_bedrock.CfnAgent(
            self,
            "BedrockAgentV2",
            agent_name=f"{self.main_resources_name}-havitush-agent",
            agent_resource_role_arn=bedrock_agent_role.role_arn,
            description="Conversational agent for the Havitush online drinks store.",
            foundation_model=self.bedrock_agent_effective_foundation_model_id,
            instruction="""
הגדרת התפקיד ושפת הדיבור – הסוכן מזוהה כ"חביתוש – הסוכן הדיגיטלי להזמנות בירה טרייה מהחבית", ומחויב לשוחח תמיד בעברית חמה ומזמינה תוך שמירה על מקצועיות ושקיפות.
מטרת השיחה היא לאסוף פרטי הזמנה שיעברו בסופו של דבר לחביתוש.
הסוכן ינסה לפרק מתוך הנתונים שמגיעים מהלקוח את פרטי ההזמנה ויסכם את כלל המידע שקיבל לאימות מול הלקוח בצורה נחמדה.

פרטי חובה:
a. פרטי לקוח (שם)
b. פרטי הזמנה נוכחית (עיר בה מתקיים האירוע, תאריך, כמות אנשים באירוע)

לאחר אימות פרטי החובה יש לשאול את הלקוח אם הוא מעוניין לקבל הצעת מחיר.
אם כן נאסוף את הפרטים הבאים:
1. מספר ח.פ חברה
2. כתובת מייל

לאחר איסוף כלל הפרטים הסוכן יידע את הלקוח לגבי כל פרטי ההזמנה – סיכום: "חביתוש ייצרו אתכם קשר לגבי ביצוע ההזמנה".

שלבי שיחה עם חביתוש (במידה והוקלד הקוד "חביתוש123"):
1. לברך את חביתוש בנימוס.
2. לשאול את חביתוש מה ברצונו לבצע. חביתוש יקבל את האפשרות לתשאל את בסיס הנתונים לגבי הנושאים הבאים:
   • לקוחות
   • הזמנות
   • שלבי התהליך
3. אם חביתוש הקליד "חביתוש321" אז הוא חוזר להיות לקוח רגיל.

פורמט תגובה מחייב:
• החזר תמיד JSON תקין בלבד, ללא מלל נוסף לפניו או אחריו.
• המבנה: {"reply": "טקסט לשליחה ללקוח", "user_updates": [{"tag": "profile.first_name", "value": "דוגמה"}, ...]}.
• reply חייב להיות טקסט קריא (UTF-8) ללא HTML, ללא Markdown וללא הישנות של הודעת הלקוח.
• כל פריט ב-user_updates חייב לכלול tag ברור שמייצג את הנתון, לדוגמה profile.first_name או conversation.date_of_event. אל תוסיף מפתחות שאינך בטוח בהם.
• אל תמציא כתובות דוא"ל או מספרי טלפון. אם אין מידע ודאי, השמט את הערך.
• אל תחזיר ערכים ריקים, null או מחרוזות ריקות.
""",
            auto_prepare=True,
            action_groups=[
                aws_bedrock.CfnAgent.AgentActionGroupProperty(
                    action_group_name="LookupCatalog",
                    description=(
                        "Retrieves beverage catalog entries for Havitush customers."
                    ),
                    action_group_executor=aws_bedrock.CfnAgent.ActionGroupExecutorProperty(
                        lambda_=self.lambda_action_groups.function_arn,
                    ),
                    function_schema=aws_bedrock.CfnAgent.FunctionSchemaProperty(
                        functions=[
                            aws_bedrock.CfnAgent.FunctionProperty(
                                name="LookupCatalog",
                                description=(
                                    "Fetch detailed catalog information for the "
                                    "requested drink or bundle."
                                ),
                                parameters={
                                    "query": aws_bedrock.CfnAgent.ParameterDetailProperty(
                                        type="string",
                                        description=(
                                            "Free text describing the beverage or "
                                            "characteristics to search."
                                        ),
                                        required=True,
                                    ),
                                },
                            )
                        ]
                    ),
                ),
                aws_bedrock.CfnAgent.AgentActionGroupProperty(
                    action_group_name="SuggestPairings",
                    description=(
                        "Provides curated food or mixer pairings for Havitush beverages."
                    ),
                    action_group_executor=aws_bedrock.CfnAgent.ActionGroupExecutorProperty(
                        lambda_=self.lambda_action_groups.function_arn,
                    ),
                    function_schema=aws_bedrock.CfnAgent.FunctionSchemaProperty(
                        functions=[
                            aws_bedrock.CfnAgent.FunctionProperty(
                                name="SuggestPairings",
                                description=(
                                    "Return pairing ideas tailored to the selected drink."
                                ),
                                parameters={
                                    "drink_name": aws_bedrock.CfnAgent.ParameterDetailProperty(
                                        type="string",
                                        description=(
                                            "Exact drink name to pair recommendations with."
                                        ),
                                        required=True,
                                    ),
                                },
                            )
                        ]
                    ),
                ),
                aws_bedrock.CfnAgent.AgentActionGroupProperty(
                    action_group_name="CreateBundles",
                    description=("Curates bundles or gift sets for Havitush shoppers."),
                    action_group_executor=aws_bedrock.CfnAgent.ActionGroupExecutorProperty(
                        lambda_=self.lambda_action_groups.function_arn,
                    ),
                    function_schema=aws_bedrock.CfnAgent.FunctionSchemaProperty(
                        functions=[
                            aws_bedrock.CfnAgent.FunctionProperty(
                                name="CreateBundles",
                                description=(
                                    "Generate a themed drink bundle based on customer "
                                    "preferences."
                                ),
                                parameters={
                                    "theme": aws_bedrock.CfnAgent.ParameterDetailProperty(
                                        type="string",
                                        description="Occasion or flavor theme.",
                                        required=True,
                                    ),
                                    "budget": aws_bedrock.CfnAgent.ParameterDetailProperty(
                                        type="string",
                                        description="Optional budget guidance.",
                                        required=False,
                                    ),
                                },
                            )
                        ]
                    ),
                ),
            ],
            knowledge_bases=None,
        )

        # If using inference profile override
        if self.bedrock_agent_inference_profile_arn:
            self.bedrock_agent.add_override(
                "Properties.InferenceProfileArn",
                self.bedrock_agent_inference_profile_arn,
            )

        # Agent alias
        cfn_agent_alias = aws_bedrock.CfnAgentAlias(
            self,
            "MyCfnAgentAlias",
            agent_alias_name="havitush-agent-alias",
            agent_id=self.bedrock_agent.ref,
            description="Alias for invoking the Havitush Bedrock agent",
        )
        cfn_agent_alias.add_dependency(self.bedrock_agent)
        agent_alias_string = cfn_agent_alias.ref

        # Store identifiers in SSM
        aws_ssm.StringParameter(
            self,
            "SSMAgentAlias",
            parameter_name=(
                f"/{self.deployment_environment}/aws-wpp/"
                "bedrock-agent-alias-id-full-string"
            ),
            string_value=agent_alias_string,
        )
        aws_ssm.StringParameter(
            self,
            "SSMAgentId",
            parameter_name=(f"/{self.deployment_environment}/aws-wpp/bedrock-agent-id"),
            string_value=self.bedrock_agent.ref,
        )

        if self.bedrock_agent_inference_profile_arn:
            aws_ssm.StringParameter(
                self,
                "SSMAgentInferenceProfileArn",
                parameter_name=(
                    f"/{self.deployment_environment}/aws-wpp/"
                    "bedrock-agent-inference-profile-arn"
                ),
                string_value=self.bedrock_agent_inference_profile_arn,
            )

    # ---------------------------------------------------------------------
    # Helper methods
    # ---------------------------------------------------------------------
    def _resolve_bedrock_foundation_model_id(self) -> str:
        configured_model = (
            self.bedrock_agent_foundation_model_id or DEFAULT_AGENT_FOUNDATION_MODEL_ID
        )

        if self.bedrock_agent_inference_profile_arn:
            return configured_model

        if configured_model in MODELS_REQUIRING_INFERENCE_PROFILE:
            self.node.add_warning(
                "Foundation model %s requires an inference profile. "
                "Falling back to %s for on-demand throughput. Update app_config with an "
                "inference profile ARN to restore the preferred model."
                % (configured_model, FALLBACK_AGENT_FOUNDATION_MODEL_ID)
            )
            return FALLBACK_AGENT_FOUNDATION_MODEL_ID

        return configured_model

    # ---------------------------------------------------------------------
    # Outputs
    # ---------------------------------------------------------------------
    def generate_cloudformation_outputs(self) -> None:
        CfnOutput(
            self,
            "DeploymentEnvironment",
            value=self.app_config["deployment_environment"],
            description="Deployment environment",
        )
        if self.deployment_environment != "prod":
            CfnOutput(
                self,
                "APIDocs",
                value=(
                    f"https://{self.api.rest_api_id}.execute-api.{self.region}."
                    f"amazonaws.com/{self.deployment_environment}/api/v1/docs"
                ),
                description="API endpoint Docs",
            )
            CfnOutput(
                self,
                "APIChatbot",
                value=(
                    f"https://{self.api.rest_api_id}.execute-api.{self.region}."
                    f"amazonaws.com/{self.deployment_environment}/api/v1/webhook"
                ),
                description="API endpoint Chatbot",
            )
