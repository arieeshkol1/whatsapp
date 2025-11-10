# Built-in imports
import os

# External imports
import aws_cdk as core
import aws_cdk.assertions as assertions

# Own imports
from cdk.stacks.cdk_chatbot_api_stack import ChatbotAPIStack

app: core.App = core.App()
stack: ChatbotAPIStack = ChatbotAPIStack(
    scope=app,
    construct_id="santi-chatbot-api-test",
    main_resources_name="santi-chatbot",
    app_config={
        "deployment_environment": "test",
        "log_level": "DEBUG",
        "table_name": "aws-whatsapp-poc-test1",
        "agents_data_table_name": "aws-whatsapp-poc-test2",
        "rules_table_name": "aws-whatsapp-rules-test",
        "ruleset_id": "default",
        "ruleset_version": "CURRENT",
        "api_gw_name": "wpp-test",
        "secret_name": "test-secret",
        "enable_rag": True,
        "meta_endpoint": "https://fake-endpoint.com",
        "users_info_table_name": "aws-whatsapp-users-info-test",
        # Optionally allow overriding UserData name here:
        # "user_data_table_name": "aws-whatsapp-user-data-test",
    },
)
template: assertions.Template = assertions.Template.from_stack(stack)


def test_app_synthesize_ok():
    app.synth()


def test_dynamodb_table_created():
    # We expect at least 3 tables now: main chat table, UsersInfo, and UserData
    match = template.find_resources(
        type="AWS::DynamoDB::Table",
    )
    assert len(match) >= 3


def test_users_info_table_exists_with_phone_pk():
    template.has_resource_properties(
        "AWS::DynamoDB::Table",
        {
            "TableName": "aws-whatsapp-users-info-test",
            "KeySchema": [{"AttributeName": "PhoneNumber", "KeyType": "HASH"}],
        },
    )


def test_user_data_table_exists_with_phone_pk():
    # Default name is "UserData" unless overridden in app_config
    template.has_resource_properties(
        "AWS::DynamoDB::Table",
        {
            "TableName": "UserData",
            "KeySchema": [{"AttributeName": "PhoneNumber", "KeyType": "HASH"}],
        },
    )


def test_lambda_function_created():
    match = template.find_resources(
        type="AWS::Lambda::Function",
    )
    assert len(match) >= 8


def test_api_gateway_created():
    match = template.find_resources(
        type="AWS::ApiGateway::RestApi",
    )
    assert len(match) == 1


def test_state_machine_lambda_has_user_info_env():
    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "Environment": {
                "Variables": {"USER_INFO_TABLE": assertions.Match.any_value()}
            }
        },
    )


def test_state_machine_lambda_has_user_data_env():
    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "Environment": {
                "Variables": {"USER_DATA_TABLE": assertions.Match.any_value()}
            }
        },
    )
