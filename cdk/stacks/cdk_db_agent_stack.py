"""Dedicated stack for deploying the Bedrock DB Agent.

This stack keeps the DB Agent definition aligned with the main Chatbot stack
while allowing independent deployments.
"""

# Built-in imports
import os
from typing import Any, Dict, Optional

# External imports
from aws_cdk import (
    CfnOutput,
    Duration,
    Stack,
    aws_bedrock,
    aws_dynamodb,
    aws_iam,
    aws_lambda,
    aws_ssm,
)
from constructs import Construct

DEFAULT_AGENT_FOUNDATION_MODEL_ID = "amazon.nova-lite-v1:0"
DB_AGENT_DEFAULT_FOUNDATION_MODEL_ID = "anthropic.claude-3-5-sonnet-20241022-v2:0"
FALLBACK_AGENT_FOUNDATION_MODEL_ID = "amazon.nova-lite-v1:0"
MODELS_REQUIRING_INFERENCE_PROFILE = {
    "anthropic.claude-3-5-haiku-20241022-v1:0",
}
USER_DATA_TABLE_DEFAULT_NAME = "UserData"


class DbAgentStack(Stack):
    """Create the Bedrock DB Agent resources."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        environment_name: str,
        main_resources_name: str,
        account_id: str,
        app_config: Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.construct_id = construct_id
        self.environment_name = environment_name
        self.main_resources_name = main_resources_name
        self.account_id = account_id
        self.app_config = app_config
        self.deployment_environment = environment_name

        self.db_agent_foundation_model_id = self.app_config.get(
            "db_agent_foundation_model_id",
            DB_AGENT_DEFAULT_FOUNDATION_MODEL_ID,
        )
        self.db_agent_inference_profile_arn = self.app_config.get(
            "db_agent_inference_profile_arn",
        )
        self.db_agent_effective_foundation_model_id = (
            self._resolve_bedrock_foundation_model_id(
                self.db_agent_foundation_model_id,
                self.db_agent_inference_profile_arn,
            )
        )

        self._create_lambda_action_group()
        self._create_bedrock_agent_components()
        self._create_cloudformation_outputs()

    def _create_lambda_action_group(self) -> None:
        """Create Lambda used by the DB Agent action groups."""

        path_to_lambda_folder = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "backend",
        )

        db_agent_lambda_role = aws_iam.Role(
            self,
            "DbAgentLambdaRole",
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Role for DB Agent Lambda",
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

        self.interaction_table = aws_dynamodb.Table.from_table_name(
            self,
            "InteractionHistoryTable",
            self.app_config["table_name"],
        )

        user_data_table_name = self.app_config.get(
            "user_data_table_name",
            USER_DATA_TABLE_DEFAULT_NAME,
        )
        user_data_table_name = self.app_config.get(
            "USER_DATA_TABLE",
            user_data_table_name,
        )
        self.user_data_table = aws_dynamodb.Table.from_table_name(
            self,
            "UserDataTable",
            user_data_table_name,
        )

        self.lambda_db_action_groups = aws_lambda.Function(
            self,
            "Lambda-DB-Agent-AG",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="db_agent/lambda_function.lambda_handler",
            function_name=f"{self.main_resources_name}-db-agent-action-groups",
            code=aws_lambda.Code.from_asset(path_to_lambda_folder),
            timeout=Duration.seconds(60),
            memory_size=512,
            environment={
                "ENVIRONMENT": self.deployment_environment,
                "LOG_LEVEL": self.app_config.get("log_level", "INFO"),
                "USER_DATA_TABLE": self.user_data_table.table_name,
                "INTERACTION_TABLE": self.interaction_table.table_name,
            },
            role=db_agent_lambda_role,
        )

        self.lambda_db_action_groups.add_permission(
            "AllowDbBedrock",
            principal=aws_iam.ServicePrincipal("bedrock.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"arn:aws:bedrock:{self.region}:{self.account}:agent/*",
        )

        self.user_data_table.grant_read_write_data(self.lambda_db_action_groups)
        self.interaction_table.grant_read_write_data(self.lambda_db_action_groups)

    def _create_bedrock_agent_components(self) -> None:
        """Define the Bedrock DB Agent, alias, and IAM role."""

        db_bedrock_agent_role = aws_iam.Role(
            self,
            "DbBedrockAgentRole",
            assumed_by=aws_iam.ServicePrincipal("bedrock.amazonaws.com"),
            description="Role for DB Bedrock Agent",
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

        self.user_data_table.grant_read_write_data(db_bedrock_agent_role)
        self.interaction_table.grant_read_write_data(db_bedrock_agent_role)
        db_bedrock_agent_role.add_to_policy(
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

        db_action_groups = [
            aws_bedrock.CfnAgent.AgentActionGroupProperty(
                action_group_name="QueryUserData",
                description="Retrieve customer profile records from the UserData table.",
                action_group_executor=aws_bedrock.CfnAgent.ActionGroupExecutorProperty(
                    lambda_=self.lambda_db_action_groups.function_arn,
                ),
                function_schema=aws_bedrock.CfnAgent.FunctionSchemaProperty(
                    functions=[
                        aws_bedrock.CfnAgent.FunctionProperty(
                            name="QueryUserData",
                            description="Fetch a user profile from the existing UserData table by phone number.",
                            parameters={
                                "phone_number": aws_bedrock.CfnAgent.ParameterDetailProperty(
                                    type="string",
                                    description="Phone number (primary key) to read from the UserData table.",
                                    required=True,
                                ),
                            },
                        )
                    ]
                ),
            ),
            aws_bedrock.CfnAgent.AgentActionGroupProperty(
                action_group_name="QueryInteractionHistory",
                description="Retrieve the past WhatsApp interactions stored in DynamoDB.",
                action_group_executor=aws_bedrock.CfnAgent.ActionGroupExecutorProperty(
                    lambda_=self.lambda_db_action_groups.function_arn,
                ),
                function_schema=aws_bedrock.CfnAgent.FunctionSchemaProperty(
                    functions=[
                        aws_bedrock.CfnAgent.FunctionProperty(
                            name="QueryInteractionHistory",
                            description="Fetch historical conversation records using the interaction table keys.",
                            parameters={
                                "partition_key": aws_bedrock.CfnAgent.ParameterDetailProperty(
                                    type="string",
                                    description="Partition key value for the interaction history table.",
                                    required=True,
                                ),
                                "sort_key_prefix": aws_bedrock.CfnAgent.ParameterDetailProperty(
                                    type="string",
                                    description="Optional sort key prefix for filtering.",
                                    required=False,
                                ),
                            },
                        )
                    ]
                ),
            ),
        ]

        db_bedrock_agent = aws_bedrock.CfnAgent(
            self,
            "DbBedrockAgent",
            agent_name=f"{self.main_resources_name}-db-agent",
            agent_resource_role_arn=db_bedrock_agent_role.role_arn,
            description=(
                "Backend DB agent that queries user context tables for the WhatsApp workflow."
            ),
            foundation_model=self.db_agent_effective_foundation_model_id,
            instruction="""
You are the DB AGENT supporting the primary WhatsApp agent. Your responsibilities:
- Use the QueryUserData and QueryInteractionHistory tools for any user profile or history lookups.
- Always respond with JSON only, containing any retrieved records.
""",
            auto_prepare=True,
            action_groups=db_action_groups,
        )

        if self.db_agent_inference_profile_arn:
            db_bedrock_agent.add_override(
                "Properties.InferenceProfileArn",
                self.db_agent_inference_profile_arn,
            )

        db_agent_alias = aws_bedrock.CfnAgentAlias(
            self,
            "DbBedrockAgentAlias",
            agent_alias_name="db-agent-alias",
            agent_id=db_bedrock_agent.ref,
            description="Alias for invoking the DB Bedrock agent",
        )
        db_agent_alias.add_dependency(db_bedrock_agent)

        aws_ssm.StringParameter(
            self,
            "SSMDBAgentAlias",
            parameter_name=f"/{self.deployment_environment}/{self.main_resources_name}/bedrock-db-agent-alias-id-full-string",
            string_value=db_agent_alias.ref,
        )
        aws_ssm.StringParameter(
            self,
            "SSMDBAgentId",
            parameter_name=f"/{self.deployment_environment}/{self.main_resources_name}/bedrock-db-agent-id",
            string_value=db_bedrock_agent.ref,
        )

    def _create_cloudformation_outputs(self) -> None:
        """Expose useful metadata as CloudFormation outputs."""

        CfnOutput(
            self,
            "DeploymentEnvironment",
            value=self.deployment_environment,
            description="Deployment environment",
        )

        CfnOutput(
            self,
            "DbAgentName",
            value=f"{self.main_resources_name}-db-agent",
            description="Name of the Bedrock DB agent",
        )

    def _resolve_bedrock_foundation_model_id(
        self,
        configured_model: Optional[str] = None,
        inference_profile_arn: Optional[str] = None,
    ) -> str:
        """Return the model identifier that should back the agent orchestration step."""

        configured_model = configured_model or DEFAULT_AGENT_FOUNDATION_MODEL_ID

        if inference_profile_arn:
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
