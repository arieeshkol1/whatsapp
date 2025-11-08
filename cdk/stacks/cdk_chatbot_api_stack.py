# Built-in imports
import os
from typing import Any, Dict, Optional

DEFAULT_AGENT_FOUNDATION_MODEL_ID = "amazon.nova-lite-v1:0"
FALLBACK_AGENT_FOUNDATION_MODEL_ID = "amazon.nova-lite-v1:0"
MODELS_REQUIRING_INFERENCE_PROFILE = {
    "anthropic.claude-3-5-haiku-20241022-v1:0",
}

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

        self.customers_table = aws_dynamodb.Table(
            self,
            "DynamoDB-Table-Customers",
            table_name="Customers",
            partition_key=aws_dynamodb.Attribute(
                name="PK", type=aws_dynamodb.AttributeType.STRING
            ),
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )
        Tags.of(self.customers_table).add("Name", "Customers")

        self.users_info_table = aws_dynamodb.Table(
            self,
            "DynamoDB-Table-UsersInfo",
            table_name="UsersInfo",
            partition_key=aws_dynamodb.Attribute(
                name="PhoneNumber", type=aws_dynamodb.AttributeType.STRING
            ),
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )
        Tags.of(self.users_info_table).add("Name", "UsersInfo")

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
        self.customers_table.grant_read_write_data(
            self.lambda_state_machine_process_message
        )
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
            },
            role=bedrock_agent_lambda_role,
        )

    def _build_state_machine_lambda_environment(self) -> Dict[str, str]:
        """Compose environment variables for the state machine processor Lambda."""

        base_environment: Dict[str, str] = {
            "ENVIRONMENT": self.app_config["deployment_environment"],
            "LOG_LEVEL": self.app_config["log_level"],
            "SECRET_NAME": self.app_config["secret_name"],
            "META_ENDPOINT": self.app_config["meta_endpoint"],
            "ASSESS_CHANGES_FEATURE": self.app_config.get(
                "ASSESS_CHANGES_FEATURE", "off"
            ),
            "USER_INFO_TABLE": self.app_config.get(
                "USER_INFO_TABLE", self.users_info_table.table_name
            ),
        }

        optional_values: Dict[str, Optional[str]] = {
            "AGENT_ID": self.app_config.get("bedrock_agent_id"),
            "BEDROCK_AGENT_ID": self.app_config.get("bedrock_agent_id"),
            "AGENT_ALIAS_ID": self.app_config.get("bedrock_agent_alias_id"),
            "BEDROCK_AGENT_ALIAS_ID": self.app_config.get("bedrock_agent_alias_id"),
            "CUSTOMERS_TABLE_NAME": self.customers_table.table_name,
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

    def create_dynamodb_streams(self) -> None:
        """
        Method to create the DynamoDB Streams for the Lambda Function that will
        process the incoming messages and trigger the State Machine.
        """

        # Stream the DynamoDB Events to the Lambda Function for processing
        self.lambda_trigger_state_machine.add_event_source(
            aws_lambda_event_sources.DynamoEventSource(
                self.dynamodb_table,
                starting_position=aws_lambda.StartingPosition.TRIM_HORIZON,
                batch_size=1,
            )
        )

    def create_rest_api(self):
        """
        Method to create the REST-API Gateway for exposing the chatbot
        functionalities.
        """

        # API Method Options for the REST-API Gateway
        # TODO: Currently public, as validation happens in the Lambda Function for now
        self.api_method_options_public = aws_apigw.MethodOptions(
            api_key_required=False,
            authorization_type=aws_apigw.AuthorizationType.NONE,
        )

        # TODO: Add domain_name with custom DNS
        # TODO: Enable custom models and schema validations
        rest_api_name = self.app_config["api_gw_name"]
        self.api = aws_apigw.LambdaRestApi(
            self,
            "RESTAPI",
            rest_api_name=rest_api_name,
            description=f"REST API Gateway for {self.main_resources_name} in {self.deployment_environment} environment",
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
            proxy=False,  # Proxy disabled to have more control
        )

        # Method to remove the "CloudFormation Output" to avoid exposing the endpoint
        self.api.node.try_remove_child("Endpoint")

    def configure_rest_api(self):
        """
        Method to configure the REST-API Gateway with resources and methods.
        """

        # Define REST-API resources
        root_resource_api = self.api.root.add_resource("api")
        root_resource_v1 = root_resource_api.add_resource("v1")

        # Endpoints for automatic Swagger docs (no auth required)
        root_resource_docs = root_resource_v1.add_resource("docs")
        root_resource_docs_proxy = root_resource_docs.add_resource("{path}")

        # Endpoints for the main functionalities
        root_resource_chatbot = root_resource_v1.add_resource("webhook")

        # Define all API-Lambda integrations for the API methods
        api_lambda_integration_chatbot = aws_apigw.LambdaIntegration(
            self.lambda_whatsapp_webhook
        )

        # API-Path: "/api/v1/webhook"
        root_resource_chatbot.add_method("GET", api_lambda_integration_chatbot)
        root_resource_chatbot.add_method("POST", api_lambda_integration_chatbot)

        # API-Path: "/api/v1/docs"
        root_resource_docs.add_method("GET", api_lambda_integration_chatbot)

        # API-Path: "/api/v1/docs/openapi.json
        root_resource_docs_proxy.add_method("GET", api_lambda_integration_chatbot)

    def create_state_machine_tasks(self) -> None:
        """ "
        Method to create the tasks for the Step Function State Machine.
        """

        # TODO: create abstraction to reuse the definition of tasks

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

        # Pass States to simplify State Machine UI understanding
        self.task_pass_text = aws_sfn.Pass(
            self,
            "Task-Text",
            comment="Indicates that the message type is Text",
            state_name="Text",
        )

        self.task_pass_voice = aws_sfn.Pass(
            self,
            "Task-Voice",
            comment="Indicates that the message type is Voice",
            state_name="Voice",
        )

        self.task_pass_image = aws_sfn.Pass(
            self,
            "Task-Image",
            comment="Indicates that the message type is Image",
            state_name="Image",
        )

        self.task_pass_video = aws_sfn.Pass(
            self,
            "Task-Video",
            comment="Indicates that the message type is Video",
            state_name="Video",
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
            self,
            "Task-NotImplemented",
            comment="Not implemented yet",
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
            self,
            id="Succeed",
            comment="Successful execution of State Machine",
        )

        self.task_failure = aws_sfn.Fail(
            self,
            id="Exception Handling Finished",
            comment="State Machine Exception or Failure",
        )

        # Duplicate tasks for the V2 state machine so both definitions can coexist.
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
                        "method_name": "assess_changes",
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
            self,
            "TaskV2-NotImplemented",
            comment="Not implemented yet",
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
            self,
            id="SucceedV2",
            comment="Successful execution of State Machine V2",
        )

        self.v2_task_failure = aws_sfn.Fail(
            self,
            id="ExceptionHandlingFinishedV2",
            comment="State Machine V2 Exception or Failure",
        )

    def create_state_machine_definition(self) -> None:
        """
        Method to create the Step Function State Machine definition.
        """

        # Conditions to simplify Choices in the State Machine
        # TODO: Add enums here for MessageType
        self.choice_text = aws_sfn.Condition.string_equals("$.message_type", "text")
        self.choice_image = aws_sfn.Condition.string_equals("$.message_type", "image")
        self.choice_video = aws_sfn.Condition.string_equals("$.message_type", "video")
        self.choice_voice = aws_sfn.Condition.string_equals("$.message_type", "voice")

        # State Machine event type initial configuration entrypoints
        self.state_machine_definition = self.task_validate_message.next(
            aws_sfn.Choice(self, "Message Type?")
            .when(self.choice_text, self.task_pass_text)
            .when(self.choice_voice, self.task_pass_voice)
            .when(self.choice_image, self.task_pass_image)
            .when(self.choice_video, self.task_pass_video)
        )

        # Pass States entrypoints
        self.task_pass_text.next(
            self.task_process_text.next(self.task_send_message),
        )
        self.task_pass_voice.next(
            self.task_process_voice.next(self.task_pass_text),
        )
        self.task_pass_image.next(self.task_not_implemented)
        self.task_pass_video.next(self.task_not_implemented)

        self.task_not_implemented.next(self.task_send_message)

        self.task_send_message.next(self.task_process_success)

        self.task_process_success.next(self.task_success)

        # TODO: Add failure handling for the State Machine with "process_failure"
        # self.task_process_failure.next(self.task_failure)

        # Conditions and definition for the V2 state machine (includes Assess Changes step).
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

        self.state_machine_definition_v2 = self.v2_task_validate_message.next(
            aws_sfn.Choice(self, "Message Type? V2")
            .when(self.choice_text_v2, self.v2_task_pass_text)
            .when(self.choice_voice_v2, self.v2_task_pass_voice)
            .when(self.choice_image_v2, self.v2_task_pass_image)
            .when(self.choice_video_v2, self.v2_task_pass_video)
        )

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

        self.v2_task_pass_text.next(self.v2_choice_assess_changes)

        self.v2_task_process_text.next(self.v2_task_send_message)

        self.v2_task_pass_voice.next(
            self.v2_task_process_voice.next(self.v2_task_pass_text)
        )
        self.v2_task_pass_image.next(self.v2_task_not_implemented)
        self.v2_task_pass_video.next(self.v2_task_not_implemented)

        self.v2_task_not_implemented.next(self.v2_task_send_message)

        self.v2_task_send_message.next(self.v2_task_process_success)

        self.v2_task_process_success.next(self.v2_task_success)

        # TODO: Add failure handling for the State Machine with "process_failure" in V2 as well
        # self.v2_task_process_failure.next(self.v2_task_failure)

    def create_state_machine(self) -> None:
        """
        Method to create the Step Function State Machine for processing the messages.
        """

        log_group_name = f"/aws/vendedlogs/states/{self.main_resources_name}"
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
                self.state_machine_definition,
            ),
            logs=aws_sfn.LogOptions(
                destination=self.state_machine_log_group,
                include_execution_data=True,
                level=aws_sfn.LogLevel.ALL,
            ),
        )

        self.state_machine.grant_start_execution(self.lambda_trigger_state_machine)

        # Add additional environment variables to the Lambda Functions
        self.lambda_trigger_state_machine.add_environment(
            "STATE_MACHINE_ARN",
            self.state_machine.state_machine_arn,
        )

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
                self.state_machine_definition_v2,
            ),
            logs=aws_sfn.LogOptions(
                destination=self.state_machine_log_group_v2,
                include_execution_data=True,
                level=aws_sfn.LogLevel.ALL,
            ),
            role=self.state_machine.role,
        )

    def create_bedrock_components(self) -> None:
        """
        Method to create the Bedrock Agent for the chatbot.
        """
        # TODO: refactor this huge function into independent methods... and eventually custom constructs!

        # Get relative path for folder that contains the kb assets
        # ! Note--> we must obtain parent dirs to create path (that"s why there is "os.path.dirname()")
        PATH_TO_KB_FOLDER = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "knowledge_base",
        )
        PATH_TO_CUSTOM_RESOURCES = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "custom_resources",
        )

        # Generic "PK" and "SK", to leverage Single-Table-Design
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

        # Add permissions to the Lambda function resource policy. You use a resource-based policy to allow an AWS service to invoke your function.
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
                    "AmazonBedrockFullAccess",
                ),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AWSLambda_FullAccess",
                ),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "CloudWatchLogsFullAccess",
                ),
            ],
        )
        self.users_info_table.grant_read_write_data(bedrock_agent_role)
        # Add additional IAM actions for the bedrock agent
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

        # Create the S3 bucket for uploading the KB assets
        if self.enable_rag:
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

            # Upload assets to S3 bucket KB at deployment time
            s3d.BucketDeployment(
                self,
                "S3Upload-KB",
                sources=[s3d.Source.asset(PATH_TO_KB_FOLDER)],
                destination_bucket=s3_bucket_kb,
                destination_key_prefix="docs/",
            )

            # Create opensearch serverless collection requires a security policy of type encryption. The policy must be a string and the resource contains the collections it is applied to.
            opensearch_serverless_encryption_policy = oss.CfnSecurityPolicy(
                self,
                "OpenSearchServerlessEncryptionPolicy",
                name="encryption-policy",
                policy='{"Rules":[{"ResourceType":"collection","Resource":["collection/*"]}],"AWSOwnedKey":true}',
                type="encryption",
                description="Encryption policy for the opensearch serverless collection",
            )

            # We also need a security policy of type network so that the collection becomes accessable. The policy must be a string and the resource contains the collections it is applied to.
            opensearch_serverless_network_policy = oss.CfnSecurityPolicy(
                self,
                "OpenSearchServerlessNetworkPolicy",
                name="network-policy",
                policy='[{"Description":"Public access for collection","Rules":[{"ResourceType":"dashboard","Resource":["collection/*"]},{"ResourceType":"collection","Resource":["collection/*"]}],"AllowFromPublic":true}]',
                type="network",
                description="Network policy for the opensearch serverless collection",
            )

            # Create the OpenSearch Collection
            opensearch_serverless_collection = oss.CfnCollection(
                self,
                "OpenSearchCollection-KB ",
                name="pdf-collection",
                description="Collection for the PDF documents",
                standby_replicas="DISABLED",
                type="VECTORSEARCH",
            )

            opensearch_serverless_collection.add_dependency(
                opensearch_serverless_encryption_policy
            )
            opensearch_serverless_collection.add_dependency(
                opensearch_serverless_network_policy
            )

            # Role for the Bedrock KB
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
                    # TROUBLESHOOTING: Add additional permissions for the KB
                    aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                        "AdministratorAccess"
                    ),  # TODO: DELETE THIS LINE IN PRODUCTION
                ],
            )

            # Create a Custom Resource for the OpenSearch Index (not supported by CDK yet)
            # TODO: Replace to L1 or L2 construct when available!!!!!!
            # Define the index name
            index_name = "kb-docs"

            # Define the Lambda function that creates a new index in the opensearch serverless collection
            create_index_lambda = aws_lambda.Function(
                self,
                "Index",
                runtime=aws_lambda.Runtime.PYTHON_3_11,
                handler="create_oss_index.handler",
                code=aws_lambda.Code.from_asset(PATH_TO_CUSTOM_RESOURCES),
                timeout=Duration.seconds(300),
                environment={
                    "COLLECTION_ENDPOINT": opensearch_serverless_collection.attr_collection_endpoint,
                    "INDEX_NAME": index_name,
                    "REGION": self.region,
                },
                layers=[self.lambda_layer_common],  # To add requests library
            )

            # Define IAM permission policy for the Lambda function. This function calls the OpenSearch Serverless API to create a new index in the collection and must have the "aoss" permissions.
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

            # Finally we can create a complete data access policy for the collection that also includes the lambda function that will create the index. The policy must be a string and the resource contains the collections it is applied to.
            opensearch_serverless_access_policy = oss.CfnAccessPolicy(
                self,
                "OpenSearchServerlessAccessPolicy",
                name=f"{self.main_resources_name}-data-access-policy",
                policy=f'[{{"Description":"Access for bedrock","Rules":[{{"ResourceType":"index","Resource":["index/*/*"],"Permission":["aoss:*"]}},{{"ResourceType":"collection","Resource":["collection/*"],"Permission":["aoss:*"]}}],"Principal":["{bedrock_agent_role.role_arn}","{bedrock_kb_role.role_arn}","{create_index_lambda.role.role_arn}","arn:aws:iam::{self.account}:root"]}}]',
                type="data",
                description="Data access policy for the opensearch serverless collection",
            )

            # Add dependencies to the collection
            opensearch_serverless_collection.add_dependency(
                opensearch_serverless_access_policy
            )

            # Define the request body for the lambda invoke api call that the custom resource will use
            aossLambdaParams = {
                "FunctionName": create_index_lambda.function_name,
                "InvocationType": "RequestResponse",
            }

            # On creation of the stack, trigger the Lambda function we just defined
            trigger_lambda_cr = cr.AwsCustomResource(
                self,
                "IndexCreateCustomResource",
                on_create=cr.AwsSdkCall(
                    service="Lambda",
                    action="invoke",
                    parameters=aossLambdaParams,
                    physical_resource_id=cr.PhysicalResourceId.of("Parameter.ARN"),
                ),
                policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                    resources=cr.AwsCustomResourcePolicy.ANY_RESOURCE
                ),
                removal_policy=RemovalPolicy.DESTROY,
                timeout=Duration.seconds(300),
            )

            # Define IAM permission policy for the custom resource
            trigger_lambda_cr.grant_principal.add_to_principal_policy(
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
                    actions=["lambda:*", "iam:CreateServiceLinkedRole", "iam:PassRole"],
                    resources=["*"],
                )
            )

            # Only trigger the custom resource after the opensearch access policy has been applied to the collection
            trigger_lambda_cr.node.add_dependency(opensearch_serverless_access_policy)
            trigger_lambda_cr.node.add_dependency(opensearch_serverless_collection)

            # Create the Bedrock KB
            bedrock_knowledge_base = aws_bedrock.CfnKnowledgeBase(
                self,
                "BedrockKB",
                name="kbdocs",
                description="Bedrock knowledge base that contains a relevant projects for the user.",
                role_arn=bedrock_kb_role.role_arn,
                knowledge_base_configuration=aws_bedrock.CfnKnowledgeBase.KnowledgeBaseConfigurationProperty(
                    type="VECTOR",
                    vector_knowledge_base_configuration=aws_bedrock.CfnKnowledgeBase.VectorKnowledgeBaseConfigurationProperty(
                        embedding_model_arn=f"arn:aws:bedrock:{self.region}::foundation-model/amazon.titan-embed-text-v1"
                    ),
                ),
                storage_configuration=aws_bedrock.CfnKnowledgeBase.StorageConfigurationProperty(
                    type="OPENSEARCH_SERVERLESS",
                    opensearch_serverless_configuration=aws_bedrock.CfnKnowledgeBase.OpenSearchServerlessConfigurationProperty(
                        collection_arn=opensearch_serverless_collection.attr_arn,
                        vector_index_name=index_name,
                        field_mapping=aws_bedrock.CfnKnowledgeBase.OpenSearchServerlessFieldMappingProperty(
                            metadata_field="AMAZON_BEDROCK_METADATA",  # Must match to Lambda Function
                            text_field="AMAZON_BEDROCK_TEXT_CHUNK",  # Must match to Lambda Function
                            vector_field="bedrock-knowledge-base-default-vector",  # Must match to Lambda Function
                        ),
                    ),
                ),
            )

            # Add dependencies to the KB
            bedrock_knowledge_base.add_dependency(opensearch_serverless_collection)
            bedrock_knowledge_base.node.add_dependency(trigger_lambda_cr)

            # Create the datasource for the bedrock KB
            bedrock_data_source = aws_bedrock.CfnDataSource(
                self,
                "Bedrock-DataSource",
                name="KbDataSource",
                knowledge_base_id=bedrock_knowledge_base.ref,
                description="The S3 data source definition for the bedrock knowledge base containing information about projects.",
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
            # Only trigger the custom resource when the kb is completed
            bedrock_data_source.node.add_dependency(bedrock_knowledge_base)

        # # TODO: Add the automation for the KB ingestion
        # # ... (manual for now when docs refreshed... could be automated)

        # Create the Bedrock Agent with KB and Agent Groups
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
            # Amazon Nova Lite model configured for fast, high-quality responses.
            foundation_model=self.bedrock_agent_effective_foundation_model_id,
            instruction="""
אתה "חביתוש – הסוכן הדיגיטלי להזמנות בירה טרייה מהחבית". דבר תמיד בעברית חמה ומזמינה וסייע ללקוחות להזמין שירותים או חבילות בהתאם לכללים הבאים:

1. שלבי שיחה חובה עם לקוח:
   • וידוא גיל: שאל פעם אחת אם כל המשתתפים מעל גיל 18. אם התשובה חיובית (כן או ביטוי מאשר אחר) המשך מיד לשלב הבא; אם התשובה שלילית – הודע "מצטער, לא ניתן לבצע הזמנה אם אחד מהמשתתפים מתחת לגיל 18" וסיים בנימוס.
   • פרטי המזמין: אסוף שם פרטי ושם משפחה, ובכל פעם שאתה מקבל אותם או כתובת דוא"ל עדכן את conversation_state_updates עם המפתחות customer_first_name, customer_last_name ו-customer_email.
   • פרטי חברה: אסוף שם חברה וכתובת מלאה.
   • תאריך האירוע: ודא שהתאריך לפחות 3 ימים מהיום (שעון ישראל). אם פחות – הודע "לא ניתן לבצע הזמנה תוך פחות מ-3 ימים מראש" וסיים בנימוס.
   • מספר משתתפים: לאחר קבלת הכמות, הפנה לפי הכללים הבאים:
       - פחות מ-60 משתתפים: שלח קישור להזמנה רגילה באתר https://www.havitush.co.il.
       - בין 61 ל-120 משתתפים: הצע שירות עצמי וחישב מחיר = מספר משתתפים × 100 ₪.
       - מעל 121 משתתפים: הצע עמדה מאוישת וחישב מחיר = מספר משתתפים × 80 ₪.

2. לאחר חישוב ההצעה: אל תספק את ההצעה ללקוח מיד. דווח שהפרטים יועברו לאישור עמית בטלפון ‎+972-50-2425777 ורק לאחר קבלת אישור ממנו אפשר לחזור ללקוח עם הצעה סופית.

3. תשובות מוכנות לשאלות כלליות (ענה בהקשר המתאים):
   • "מה זה חביתוש?" – שירות משלוח בירה טרייה מהחבית עד הבית או המשרד תוך 90 דקות.
   • "האם חביתוש מייצר בירה?" – לא, חביתוש מביא את מותגי הבירה המובילים בצורה הטרייה ביותר.
   • "יש משלוחים בסוף השבוע?" – כן, 7 ימים בשבוע עד 23:00 בלילה.
   • "האם מגיעים גם לאירועים?" – כן, חביתוש מספק גם למשרדים, חברות ואירועים פרטיים בתיאום מראש.

4. עבודה מול עמית: עמית הוא מאמן הסוכן ומאשר כל הצעת מחיר. אין לפנות ללקוח עם הצעה לפני אישור עמית, ויש לעדכן שניתן ליצור איתו קשר לעריכת חוקי השיחה.

שמור על שפה מקצועית, שקופה ואמפתית. סכם כל שלב והצע עזרה נוספת בעת הצורך.
""",
            auto_prepare=True,
            action_groups=[
                aws_bedrock.CfnAgent.AgentActionGroupProperty(
                    action_group_name="LookupCatalog",
                    description="Retrieves beverage catalog entries for Havitush customers.",
                    action_group_executor=aws_bedrock.CfnAgent.ActionGroupExecutorProperty(
                        lambda_=self.lambda_action_groups.function_arn,
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
                                    ),
                                },
                            )
                        ]
                    ),
                ),
                aws_bedrock.CfnAgent.AgentActionGroupProperty(
                    action_group_name="SuggestPairings",
                    description="Provides curated food or mixer pairings for Havitush beverages.",
                    action_group_executor=aws_bedrock.CfnAgent.ActionGroupExecutorProperty(
                        lambda_=self.lambda_action_groups.function_arn,
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
                                    ),
                                },
                            )
                        ]
                    ),
                ),
                aws_bedrock.CfnAgent.AgentActionGroupProperty(
                    action_group_name="CreateBundles",
                    description="Curates bundles or gift sets for Havitush shoppers.",
                    action_group_executor=aws_bedrock.CfnAgent.ActionGroupExecutorProperty(
                        lambda_=self.lambda_action_groups.function_arn,
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
                                        description="Optional budget guidance shared by the customer.",
                                        required=False,
                                    ),
                                },
                            )
                        ]
                    ),
                ),
            ],
            knowledge_bases=(
                [
                    (
                        aws_bedrock.CfnAgent.AgentKnowledgeBaseProperty(
                            description="Knowledge base with curated tasting notes, catalog entries, and brand storytelling for Havitush drinks.",
                            knowledge_base_id=bedrock_knowledge_base.ref,
                        )
                    ),
                ]
                if self.enable_rag
                else None
            ),
        )

        if self.bedrock_agent_inference_profile_arn:
            # The CloudFormation property was introduced before CDK exposed a typed field.
            # Manually override the synthesized template so Bedrock uses the required
            # inference profile when invoking the foundation model.
            self.bedrock_agent.add_override(
                "Properties.InferenceProfileArn",
                self.bedrock_agent_inference_profile_arn,
            )

        # Create an alias for the bedrock agent
        cfn_agent_alias = aws_bedrock.CfnAgentAlias(
            self,
            "MyCfnAgentAlias",
            agent_alias_name="havitush-agent-alias",
            agent_id=self.bedrock_agent.ref,
            description="Alias for invoking the Havitush Bedrock agent",
        )
        cfn_agent_alias.add_dependency(self.bedrock_agent)

        # This string will be as <AGENT_ID>|<AGENT_ALIAS_ID>
        agent_alias_string = cfn_agent_alias.ref

        # Create SSM Parameters for the agent alias to use in the Lambda functions
        # Note: can not be added as Env-Vars due to circular dependency. Thus, SSM Params (decouple)
        aws_ssm.StringParameter(
            self,
            "SSMAgentAlias",
            parameter_name=f"/{self.deployment_environment}/aws-wpp/bedrock-agent-alias-id-full-string",
            string_value=agent_alias_string,
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

    def _resolve_bedrock_foundation_model_id(self) -> str:
        """Return the model identifier that should back the agent orchestration step."""

        configured_model = self.bedrock_agent_foundation_model_id or (
            DEFAULT_AGENT_FOUNDATION_MODEL_ID
        )

        if self.bedrock_agent_inference_profile_arn:
            return configured_model

        if configured_model in MODELS_REQUIRING_INFERENCE_PROFILE:
            self.node.add_warning(
                "Foundation model %s requires an inference profile. Falling back to %s for"
                " on-demand throughput. Update app_config with an inference profile ARN to"
                " restore the preferred model."
                % (
                    configured_model,
                    FALLBACK_AGENT_FOUNDATION_MODEL_ID,
                )
            )
            return FALLBACK_AGENT_FOUNDATION_MODEL_ID

        return configured_model

    def generate_cloudformation_outputs(self) -> None:
        """
        Method to add the relevant CloudFormation outputs.
        """

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
                description="API endpoint Docs",
            )

            CfnOutput(
                self,
                "APIChatbot",
                value=f"https://{self.api.rest_api_id}.execute-api.{self.region}.amazonaws.com/{self.deployment_environment}/api/v1/webhook",
                description="API endpoint Chatbot",
            )
