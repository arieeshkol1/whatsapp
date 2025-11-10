# Built-in imports
import os
from typing import Any, Dict, Optional

# Foundation model configuration
DEFAULT_AGENT_FOUNDATION_MODEL_ID = "amazon.nova-lite-v1:0"
FALLBACK_AGENT_FOUNDATION_MODEL_ID = "amazon.nova-lite-v1:0"
MODELS_REQUIRING_INFERENCE_PROFILE = {
    "anthropic.claude-3-5-haiku-20241022-v1:0",
}

# UsersInfo data model
USERS_INFO_TABLE_DEFAULT_NAME = "UsersInfo"
USERS_INFO_PK_NAME = "PhoneNumber"
USERS_INFO_NAME_ATTR = "Name"

# External imports
from aws_cdk import (
    Duration,
    aws_bedrock,
    aws_dynamodb,
    aws_iam,
    aws_lambda,
    aws_lambda_event_sources,
    aws_logs,
    aws_ssm,
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
    Creates the chatbot API, Lambdas, DynamoDB, Step Functions (V1 and V2),
    and the Bedrock Agent. V2 routes Text -> Assess Changes -> Process Text.
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

        # Inputs
        self.construct_id = construct_id
        self.main_resources_name = main_resources_name
        self.app_config = app_config
        self.deployment_environment = self.app_config["deployment_environment"]

        # Optional features
        self.enable_rag = self.app_config.get("enable_rag", False)
        self.bedrock_agent_foundation_model_id = self.app_config.get(
            "bedrock_agent_foundation_model_id", DEFAULT_AGENT_FOUNDATION_MODEL_ID
        )
        self.bedrock_agent_inference_profile_arn = self.app_config.get(
            "bedrock_agent_inference_profile_arn"
        )
        self.bedrock_agent_effective_foundation_model_id = (
            self._resolve_bedrock_foundation_model_id()
        )

        # Placeholders
        self.rules_dynamodb_table = None

        # Build
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
        self.secret_chatbot = aws_ssm.StringParameter.from_string_parameter_name(
            self,
            "Secret-Chatbot",
            string_parameter_name=self.app_config["secret_name"],
        )

    # ---------------------------------------------------------------------
    # DynamoDB
    # ---------------------------------------------------------------------
    def create_dynamodb_table(self) -> None:
        """Create main conversation table (stream enabled)."""
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
        Import the existing UsersInfo table so the stack does NOT try to create it.
        We provide table_arn to enable grants.
        """
        users_info_table_name = self.app_config.get(
            "users_info_table_name", USERS_INFO_TABLE_DEFAULT_NAME
        )
        users_info_table_arn = f"arn:aws:dynamodb:{self.region}:{self.account}:table/{users_info_table_name}"
        self.users_info_table = aws_dynamodb.Table.from_table_attributes(
            self,
            "UsersInfoTable",
            table_name=users_info_table_name,
            table_arn=users_info_table_arn,
        )
        # Tagging imported resource isn't supported with CFN, so skip explicit Tags here.

    # ---------------------------------------------------------------------
    # Lambda Layers
    # ---------------------------------------------------------------------
    def create_lambda_layers(self) -> None:
        # Powertools (version can be adjusted if needed)
        self.lambda_layer_powertools = aws_lambda.LayerVersion.from_layer_version_arn(
            self,
            "Layer-PowerTools",
            layer_version_arn=f"arn:aws:lambda:{self.region}:017000801446:layer:AWSLambdaPowertoolsPythonV2:71",
        )

        # Minimal "common" layer shipped with repo
        self.lambda_layer_common = aws_lambda.LayerVersion(
            self,
            "Layer-Common",
            code=aws_lambda.Code.from_asset("lambda-layers/common/modules"),
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_11],
            description="Common Python deps",
            removal_policy=RemovalPolicy.DESTROY,
            compatible_architectures=[aws_lambda.Architecture.X86_64],
        )

    # ---------------------------------------------------------------------
    # Lambdas
    # ---------------------------------------------------------------------
    def create_lambda_functions(self) -> None:
        base_backend_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "backend",
        )

        # WhatsApp Webhook (API)
        self.lambda_whatsapp_webhook = aws_lambda.Function(
            self,
            "Lambda-WhatsApp-Webhook",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="whatsapp_webhook/api/v1/main.handler",
            function_name=f"{self.main_resources_name}-input",
            code=aws_lambda.Code.from_asset(base_backend_path),
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

        # Trigger state machine from DynamoDB Stream
        self.lambda_trigger_state_machine = aws_lambda.Function(
            self,
            "Lambda-Trigger-Message-Processing",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="trigger/trigger_handler.lambda_handler",
            function_name=f"{self.main_resources_name}-trigger-state-machine",
            code=aws_lambda.Code.from_asset(base_backend_path),
            timeout=Duration.seconds(20),
            memory_size=512,
            environment={
                "ENVIRONMENT": self.app_config["deployment_environment"],
                "LOG_LEVEL": self.app_config["log_level"],
            },
            layers=[self.lambda_layer_powertools, self.lambda_layer_common],
        )

        # Single Lambda that handles all StepFunction tasks
        self.lambda_state_machine_process_message = aws_lambda.Function(
            self,
            "Lambda-SM-Process-Message",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="state_machine/state_machine_handler.lambda_handler",
            function_name=f"{self.main_resources_name}-state-machine-lambda",
            code=aws_lambda.Code.from_asset(base_backend_path),
            timeout=Duration.seconds(60),
            memory_size=512,
            environment=self._build_state_machine_lambda_environment(),
            layers=[self.lambda_layer_powertools, self.lambda_layer_common],
        )
        # Secrets manager param read
        self.lambda_state_machine_process_message.role.add_managed_policy(
            aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonSSMReadOnlyAccess"
            )
        )
        # Bedrock access
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
        self.dynamodb_table.grant_read_write_data(
            self.lambda_state_machine_process_message
        )
        # Explicit grants for UsersInfo
        self.users_info_table.grant_read_write_data(
            self.lambda_state_machine_process_message
        )

        # Lambda for Bedrock Agent action groups
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
            code=aws_lambda.Code.from_asset(base_backend_path),
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
        # Core env
        base_environment: Dict[str, str] = {
            "ENVIRONMENT": self.app_config["deployment_environment"],
            "LOG_LEVEL": self.app_config["log_level"],
            "SECRET_NAME": self.app_config["secret_name"],
            "META_ENDPOINT": self.app_config["meta_endpoint"],
            # UsersInfo wiring for AssessChanges
            "USER_INFO_TABLE": self.app_config.get(
                "USER_INFO_TABLE",
                self.app_config.get(
                    "users_info_table_name", USERS_INFO_TABLE_DEFAULT_NAME
                ),
            ),
            "USER_INFO_PK_NAME": USERS_INFO_PK_NAME,
            "USER_INFO_NAME_ATTRIBUTE": USERS_INFO_NAME_ATTR,
        }

        optional_values: Dict[str, Optional[str]] = {
            "AGENT_ID": self.app_config.get("bedrock_agent_id"),
            "BEDROCK_AGENT_ID": self.app_config.get("bedrock_agent_id"),
            "AGENT_ALIAS_ID": self.app_config.get("bedrock_agent_alias_id"),
            "BEDROCK_AGENT_ALIAS_ID": self.app_config.get("bedrock_agent_alias_id"),
        }
        for k, v in optional_values.items():
            if v:
                base_environment[k] = v

        # Rules (optional)
        if self.app_config.get("rules_table_name"):
            base_environment["RULES_TABLE"] = self.app_config["rules_table_name"]

        return base_environment

    # ---------------------------------------------------------------------
    # Streams
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
    # API Gateway
    # ---------------------------------------------------------------------
    def create_rest_api(self) -> None:
        rest_api_name = self.app_config["api_gw_name"]
        self.api = aws_apigw.LambdaRestApi(
            self,
            "RESTAPI",
            rest_api_name=rest_api_name,
            description=f"REST API Gateway for {self.main_resources_name} in {self.deployment_environment}",
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
            default_method_options=aws_apigw.MethodOptions(
                api_key_required=False,
                authorization_type=aws_apigw.AuthorizationType.NONE,
            ),
            endpoint_types=[aws_apigw.EndpointType.REGIONAL],
            cloud_watch_role=False,
            proxy=False,
        )
        # Hide Endpoint output
        self.api.node.try_remove_child("Endpoint")

    def configure_rest_api(self) -> None:
        root_resource_api = self.api.root.add_resource("api")
        root_resource_v1 = root_resource_api.add_resource("v1")

        docs = root_resource_v1.add_resource("docs")
        docs_proxy = docs.add_resource("{path}")

        webhook = root_resource_v1.add_resource("webhook")
        integration = aws_apigw.LambdaIntegration(self.lambda_whatsapp_webhook)

        webhook.add_method("GET", integration)
        webhook.add_method("POST", integration)
        docs.add_method("GET", integration)
        docs_proxy.add_method("GET", integration)

    # ---------------------------------------------------------------------
    # Step Functions — tasks
    # ---------------------------------------------------------------------
    def create_state_machine_tasks(self) -> None:
        # V1 tasks
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
            self, "Task-Text", comment="Text", state_name="Text"
        )
        self.task_pass_voice = aws_sfn.Pass(
            self, "Task-Voice", comment="Voice", state_name="Voice"
        )
        self.task_pass_image = aws_sfn.Pass(
            self, "Task-Image", comment="Image", state_name="Image"
        )
        self.task_pass_video = aws_sfn.Pass(
            self, "Task-Video", comment="Video", state_name="Video"
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
        self.task_success = aws_sfn.Succeed(
            self, id="Succeed", comment="Successful execution of State Machine"
        )

        # ---------------- V2 tasks ----------------
        # V2: AdaptInput pass (as per your JSON)
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
            self, "TaskV2-Text", comment="Text", state_name="Text"
        )
        self.v2_task_pass_voice = aws_sfn.Pass(
            self, "TaskV2-Voice", comment="Voice", state_name="Voice"
        )
        self.v2_task_pass_image = aws_sfn.Pass(
            self, "TaskV2-Image", comment="Image", state_name="Image"
        )
        self.v2_task_pass_video = aws_sfn.Pass(
            self, "TaskV2-Video", comment="Video", state_name="Video"
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
        self.v2_task_success = aws_sfn.Succeed(
            self, id="SucceedV2", comment="Successful execution of State Machine V2"
        )

    # ---------------------------------------------------------------------
    # Step Functions — definitions
    # ---------------------------------------------------------------------
    def create_state_machine_definition(self) -> None:
        # V1 definition
        choice_text = aws_sfn.Condition.string_equals("$.message_type", "text")
        choice_image = aws_sfn.Condition.string_equals("$.message_type", "image")
        choice_video = aws_sfn.Condition.string_equals("$.message_type", "video")
        choice_voice = aws_sfn.Condition.string_equals("$.message_type", "voice")

        self.state_machine_definition = self.task_adapt_message.next(
            self.task_validate_message.next(
                aws_sfn.Choice(self, "Message Type?")
                .when(choice_text, self.task_pass_text)
                .when(choice_voice, self.task_pass_voice)
                .when(choice_image, self.task_pass_image)
                .when(choice_video, self.task_pass_video)
            )
        )
        self.task_pass_text.next(self.task_process_text.next(self.task_send_message))
        self.task_pass_voice.next(self.task_process_voice.next(self.task_pass_text))
        self.task_pass_image.next(self.task_not_implemented)
        self.task_pass_video.next(self.task_not_implemented)
        self.task_not_implemented.next(self.task_send_message)
        self.task_send_message.next(self.task_process_success)
        self.task_process_success.next(self.task_success)

        # V2 definition (per your JSON: Text -> Assess Changes -> Process Text)
        choice_text_v2 = aws_sfn.Condition.string_equals("$.message_type", "text")
        choice_image_v2 = aws_sfn.Condition.string_equals("$.message_type", "image")
        choice_video_v2 = aws_sfn.Condition.string_equals("$.message_type", "video")
        choice_voice_v2 = aws_sfn.Condition.string_equals("$.message_type", "voice")

        self.state_machine_definition_v2 = self.v2_task_adapt_input.next(
            self.v2_task_adapt_message.next(
                self.v2_task_validate_message.next(
                    aws_sfn.Choice(self, "Message Type? V2")
                    .when(choice_text_v2, self.v2_task_pass_text)
                    .when(choice_voice_v2, self.v2_task_pass_voice)
                    .when(choice_image_v2, self.v2_task_pass_image)
                    .when(choice_video_v2, self.v2_task_pass_video)
                )
            )
        )

        # Text -> Assess Changes -> Process Text
        self.v2_task_pass_text.next(
            self.v2_task_assess_changes.next(self.v2_task_process_text)
        )
        # Voice path -> Process Voice -> back to Text (then Assess Changes chain above)
        self.v2_task_pass_voice.next(
            self.v2_task_process_voice.next(self.v2_task_pass_text)
        )
        self.v2_task_pass_image.next(self.v2_task_not_implemented)
        self.v2_task_pass_video.next(self.v2_task_not_implemented)
        self.v2_task_not_implemented.next(self.v2_task_send_message)
        self.v2_task_process_text.next(self.v2_task_send_message)
        self.v2_task_send_message.next(self.v2_task_process_success)
        self.v2_task_process_success.next(self.v2_task_success)

    # ---------------------------------------------------------------------
    # Step Functions — resources
    # ---------------------------------------------------------------------
    def create_state_machine(self) -> None:
        # V1
        log_group_name_v1 = f"/aws/vendedlogs/states/{self.main_resources_name}"
        self.state_machine_log_group = aws_logs.LogGroup(
            self,
            "StateMachine-LogGroup",
            log_group_name=log_group_name_v1,
            removal_policy=RemovalPolicy.DESTROY,
        )
        Tags.of(self.state_machine_log_group).add("Name", log_group_name_v1)
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

        # Permissions to start
        self.state_machine.grant_start_execution(self.lambda_trigger_state_machine)
        self.state_machine_v2.grant_start_execution(self.lambda_whatsapp_webhook)
        self.state_machine_v2.grant_start_execution(self.lambda_trigger_state_machine)

        # Lambda envs for ARNs
        self.lambda_trigger_state_machine.add_environment(
            "STATE_MACHINE_V1_ARN", self.state_machine.state_machine_arn
        )
        self.lambda_trigger_state_machine.add_environment(
            "STATE_MACHINE_ARN", self.state_machine_v2.state_machine_arn
        )
        self.lambda_trigger_state_machine.add_environment(
            "ENABLE_STREAM_TRIGGER", "off"
        )
        self.lambda_whatsapp_webhook.add_environment(
            "STATE_MACHINE_ARN", self.state_machine_v2.state_machine_arn
        )

    # ---------------------------------------------------------------------
    # Bedrock Agent (with action_groups to avoid actionGroupId=null)
    # ---------------------------------------------------------------------
    def create_bedrock_components(self) -> None:
        # Permission so Bedrock can invoke the Action Group Lambda
        self.lambda_action_groups.add_permission(
            "AllowBedrock",
            principal=aws_iam.ServicePrincipal("bedrock.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"arn:aws:bedrock:{self.region}:{self.account}:agent/*",
        )

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
        # Allow Agent to use UsersInfo if needed
        self.users_info_table.grant_read_write_data(bedrock_agent_role)

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
פורמט תגובה מחייב: {"reply": "טקסט ללקוח", "user_updates": [{"tag":"profile.first_name","value":"דוגמה"}]}
""",
            auto_prepare=True,
            action_groups=[
                aws_bedrock.CfnAgent.AgentActionGroupProperty(
                    action_group_name="LookupCatalog",
                    description="Retrieves beverage catalog entries for Havitush customers.",
                    action_group_executor=aws_bedrock.CfnAgent.ActionGroupExecutorProperty(
                        lambda_=self.lambda_action_groups.function_arn
                    ),
                    function_schema=aws_bedrock.CfnAgent.FunctionSchemaProperty(
                        functions=[
                            aws_bedrock.CfnAgent.FunctionProperty(
                                name="LookupCatalog",
                                description="Fetch detailed catalog information for the requested drink or bundle.",
                                parameters={
                                    "query": aws_bedrock.CfnAgent.ParameterDetailProperty(
                                        type="string",
                                        description="Free text describing the beverage or characteristics to search.",
                                        required=True,
                                    )
                                },
                            )
                        ]
                    ),
                ),
                aws_bedrock.CfnAgent.AgentActionGroupProperty(
                    action_group_name="SuggestPairings",
                    description="Provides curated food or mixer pairings for Havitush beverages.",
                    action_group_executor=aws_bedrock.CfnAgent.ActionGroupExecutorProperty(
                        lambda_=self.lambda_action_groups.function_arn
                    ),
                    function_schema=aws_bedrock.CfnAgent.FunctionSchemaProperty(
                        functions=[
                            aws_bedrock.CfnAgent.FunctionProperty(
                                name="SuggestPairings",
                                description="Return pairing ideas tailored to the selected drink.",
                                parameters={
                                    "drink_name": aws_bedrock.CfnAgent.ParameterDetailProperty(
                                        type="string",
                                        description="Exact drink name to pair recommendations with.",
                                        required=True,
                                    )
                                },
                            )
                        ]
                    ),
                ),
                aws_bedrock.CfnAgent.AgentActionGroupProperty(
                    action_group_name="CreateBundles",
                    description="Curates bundles or gift sets for Havitush shoppers.",
                    action_group_executor=aws_bedrock.CfnAgent.ActionGroupExecutorProperty(
                        lambda_=self.lambda_action_groups.function_arn
                    ),
                    function_schema=aws_bedrock.CfnAgent.FunctionSchemaProperty(
                        functions=[
                            aws_bedrock.CfnAgent.FunctionProperty(
                                name="CreateBundles",
                                description="Generate a themed drink bundle based on customer preferences.",
                                parameters={
                                    "theme": aws_bedrock.CfnAgent.ParameterDetailProperty(
                                        type="string",
                                        description="Occasion or flavor theme for the bundle.",
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
        )

        if self.bedrock_agent_inference_profile_arn:
            # Inject property until exposed by higher-level CDK
            self.bedrock_agent.add_override(
                "Properties.InferenceProfileArn",
                self.bedrock_agent_inference_profile_arn,
            )

        # Alias for invocation
        cfn_agent_alias = aws_bedrock.CfnAgentAlias(
            self,
            "BedrockAgentAlias",
            agent_alias_name="havitush-agent-alias",
            agent_id=self.bedrock_agent.ref,
            description="Alias for invoking the Havitush Bedrock agent",
        )
        cfn_agent_alias.add_dependency(self.bedrock_agent)

        # Persist IDs for Lambdas via SSM
        aws_ssm.StringParameter(
            self,
            "SSMAgentAlias",
            parameter_name=f"/{self.deployment_environment}/aws-wpp/bedrock-agent-alias-id-full-string",
            string_value=cfn_agent_alias.ref,  # <AGENT_ID>|<AGENT_ALIAS_ID>
        )
        aws_ssm.StringParameter(
            self,
            "SSMAgentId",
            parameter_name=f"/{self.deployment_environment}/aws-wpp/bedrock-agent-id",
            string_value=self.bedrock_agent.ref,
        )
        if self.bedrock_agent_inference_profile_arn:
            aws_ssm.StringParameter(
                self,
                "SSMAgentInferenceProfileArn",
                parameter_name=(
                    f"/{self.deployment_environment}/aws-wpp/bedrock-agent-inference-profile-arn"
                ),
                string_value=self.bedrock_agent_inference_profile_arn,
            )

    # ---------------------------------------------------------------------
    # Helpers / Outputs
    # ---------------------------------------------------------------------
    def _resolve_bedrock_foundation_model_id(self) -> str:
        configured_model = (
            self.bedrock_agent_foundation_model_id or DEFAULT_AGENT_FOUNDATION_MODEL_ID
        )
        if self.bedrock_agent_inference_profile_arn:
            return configured_model
        if configured_model in MODELS_REQUIRING_INFERENCE_PROFILE:
            self.node.add_warning(
                "Foundation model %s requires an inference profile. Falling back to %s "
                "for on-demand throughput."
                % (configured_model, FALLBACK_AGENT_FOUNDATION_MODEL_ID)
            )
            return FALLBACK_AGENT_FOUNDATION_MODEL_ID
        return configured_model

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
                value=f"https://{self.api.rest_api_id}.execute-api.{self.region}.amazonaws.com/{self.deployment_environment}/api/v1/docs",
                description="API Docs",
            )
            CfnOutput(
                self,
                "APIChatbot",
                value=f"https://{self.api.rest_api_id}.execute-api.{self.region}.amazonaws.com/{self.deployment_environment}/api/v1/webhook",
                description="Webhook endpoint",
            )
