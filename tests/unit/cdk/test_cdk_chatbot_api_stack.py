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
        "ASSESS_CHANGES_FEATURE": "off",
        "USER_INFO_TABLE": "UsersInfo",
        "RULES_TABLE": "aws-whatsapp-rules-test",
    },
)
template: assertions.Template = assertions.Template.from_stack(stack)


def test_app_synthesize_ok():
    app.synth()


def test_dynamodb_table_created():
    match = template.find_resources(
        type="AWS::DynamoDB::Table",
    )
    assert len(match) >= 3


def test_users_info_table_schema():
    template.has_resource(
        "AWS::DynamoDB::Table",
        assertions.Match.object_like(
            {
                "DeletionPolicy": "Retain",
                "UpdateReplacePolicy": "Retain",
                "Properties": assertions.Match.object_like(
                    {
                        "TableName": "UsersInfo",
                        "BillingMode": "PAY_PER_REQUEST",
                        "KeySchema": [
                            {
                                "AttributeName": "PhoneNumber",
                                "KeyType": "HASH",
                            }
                        ],
                        "AttributeDefinitions": [
                            {
                                "AttributeName": "PhoneNumber",
                                "AttributeType": "S",
                            }
                        ],
                    }
                ),
            }
        ),
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


def test_state_machine_lambda_has_assess_changes_env_vars():
    template.has_resource_properties(
        "AWS::Lambda::Function",
        assertions.Match.object_like(
            {
                "Handler": "state_machine/state_machine_handler.lambda_handler",
                "Environment": {
                    "Variables": assertions.Match.object_like(
                        {
                            "ASSESS_CHANGES_FEATURE": "off",
                            "USER_INFO_TABLE": "UsersInfo",
                            "RULES_TABLE": "aws-whatsapp-rules-test",
                        }
                    )
                },
            }
        ),
    )


def test_state_machine_lambda_uses_v2_state_machine_by_default():
    resources = template.find_resources(
        type="AWS::Lambda::Function",
    )
    target = None
    for resource in resources.values():
        if (
            resource["Properties"].get("Handler")
            == "trigger/trigger_handler.lambda_handler"
        ):
            target = resource
            break

    assert target is not None, "Trigger lambda not found in synthesized template"

    variables = target["Properties"].get("Environment", {}).get("Variables", {})
    v2_ref = variables.get("STATE_MACHINE_ARN", {}).get("Ref")
    v1_ref = variables.get("STATE_MACHINE_V1_ARN", {}).get("Ref")
    trigger_flag = variables.get("ENABLE_STREAM_TRIGGER")

    assert v2_ref is not None and v2_ref.startswith("StateMachineProcessMessageV2")
    assert v1_ref is not None and v1_ref.startswith("StateMachineProcessMessage")
    assert trigger_flag == "off"


def test_state_machine_lambda_has_dynamodb_permissions():
    template.has_resource_properties(
        "AWS::IAM::Policy",
        assertions.Match.object_like(
            {
                "PolicyDocument": {
                    "Statement": assertions.Match.array_with(
                        [
                            assertions.Match.object_like(
                                {
                                    "Action": [
                                        "dynamodb:GetItem",
                                        "dynamodb:PutItem",
                                        "dynamodb:UpdateItem",
                                        "dynamodb:DescribeTable",
                                    ]
                                }
                            )
                        ]
                    )
                }
            }
        ),
    )


def test_two_state_machines_defined():
    resources = template.find_resources(
        type="AWS::StepFunctions::StateMachine",
    )
    assert len(resources) == 2

    names = {
        resource["Properties"].get("StateMachineName")
        for resource in resources.values()
        if resource["Properties"].get("StateMachineName")
    }
    assert any(name.endswith("process-message") for name in names)
    assert any(name.endswith("process-message-v2") for name in names)


def test_webhook_lambda_has_state_machine_env_var():
    resources = template.find_resources("AWS::Lambda::Function")
    target = None
    for logical_id, resource in resources.items():
        props = resource.get("Properties", {})
        if props.get("Handler") == "whatsapp_webhook/api/v1/main.handler":
            target = props
            break

    assert target is not None, "Webhook lambda not found"

    variables = target.get("Environment", {}).get("Variables", {})
    arn_ref = variables.get("STATE_MACHINE_ARN", {}).get("Ref")

    assert arn_ref is not None and arn_ref.startswith("StateMachineProcessMessageV2")
