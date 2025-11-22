#!/usr/bin/env python3

# Built-in imports
import os

# External imports
import aws_cdk as cdk

# Own imports
from helpers.add_tags import add_tags_to_app
from stacks.cdk_chatbot_api_stack import ChatbotAPIStack
from stacks.cdk_db_agent_stack import DbAgentStack


print("--> Deployment AWS configuration (safety first):")
print("CDK_DEFAULT_ACCOUNT", os.environ.get("CDK_DEFAULT_ACCOUNT"))
print("CDK_DEFAULT_REGION", os.environ.get("CDK_DEFAULT_REGION"))


app: cdk.App = cdk.App()


# Configurations for the deployment (obtained from env vars and CDK context)
DEPLOYMENT_ENVIRONMENT = os.environ["DEPLOYMENT_ENVIRONMENT"]  # Fail if not set
MAIN_RESOURCES_NAME = app.node.try_get_context("main_resources_name")
# TODO: enhance app_config to be a data class (improve keys/typings)
APP_CONFIG = app.node.try_get_context("app_config")[DEPLOYMENT_ENVIRONMENT]


stack: ChatbotAPIStack = ChatbotAPIStack(
    app,
    f"{MAIN_RESOURCES_NAME}-chatbot-api-{DEPLOYMENT_ENVIRONMENT}",
    MAIN_RESOURCES_NAME,
    APP_CONFIG,
    env={
        "account": os.environ.get("CDK_DEFAULT_ACCOUNT"),
        "region": os.environ.get("CDK_DEFAULT_REGION"),
    },
    description=f"Stack for {MAIN_RESOURCES_NAME} chatbot-api infrastructure in {DEPLOYMENT_ENVIRONMENT} environment",
)

add_tags_to_app(
    app,
    MAIN_RESOURCES_NAME,
    DEPLOYMENT_ENVIRONMENT,
)

if APP_CONFIG.get("enable_db_agent", False):
    DbAgentStack(
        app,
        f"{MAIN_RESOURCES_NAME}-db-agent-{DEPLOYMENT_ENVIRONMENT}",
        environment_name=DEPLOYMENT_ENVIRONMENT,
        main_resources_name=MAIN_RESOURCES_NAME,
        account_id=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        app_config=APP_CONFIG,
        env={
            "account": os.environ.get("CDK_DEFAULT_ACCOUNT"),
            "region": os.environ.get("CDK_DEFAULT_REGION"),
        },
        description=(
            f"Stack for {MAIN_RESOURCES_NAME} DB agent infrastructure in {DEPLOYMENT_ENVIRONMENT} environment"
        ),
    )

app.synth()
