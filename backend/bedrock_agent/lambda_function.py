# NOTE: This is a super-MVP code for testing. Still has a lot of gaps to solve/fix. Do not use in prod.
# TODO: Refactor solution to a standalone router for all Action Groups

from typing import Dict, List, Optional

from bedrock_agent.dynamodb_helper import query_dynamodb_pk_sk


CATALOG_PK = "CATALOG#HAVITUSH"
PAIRINGS_PK = "PAIRINGS#HAVITUSH"
BUNDLES_PK = "BUNDLES#HAVITUSH"


def _sanitize_sort_key_portion(raw_value: Optional[str]) -> str:
    if not raw_value:
        return ""
    cleaned = raw_value.strip().upper()
    return cleaned.replace(" ", "#")


def _stringify_items(
    items: List[Dict], detail_key: str, fallback_message: str
) -> List[str]:
    results: List[str] = []
    for item in items:
        details = item.get(detail_key)
        if isinstance(details, dict):
            name = details.get("name") or details.get("title")
            description = details.get("description")
            price = details.get("price")
            fragments = [
                fragment for fragment in [name, description, price] if fragment
            ]
            if fragments:
                results.append(" - ".join(fragments))
                continue
        if isinstance(details, list):
            for entry in details:
                if isinstance(entry, str):
                    results.append(entry)
        elif isinstance(details, str):
            results.append(details)
    if not results:
        results.append(fallback_message)
    return results


def action_group_lookup_catalog(parameters):
    query_value = None
    for param in parameters:
        if param["name"] == "query":
            query_value = param["value"]
            break

    items = query_dynamodb_pk_sk(
        partition_key=CATALOG_PK,
        sort_key_portion=_sanitize_sort_key_portion(query_value),
    )
    print("CATALOG ITEMS: ", items)

    return _stringify_items(
        items,
        detail_key="catalog_details",
        fallback_message=(
            "No catalog entries were found for that description. "
            "Share more details and I'll keep looking!"
        ),
    )


def action_group_suggest_pairings(parameters):
    drink_name = None
    for param in parameters:
        if param["name"] == "drink_name":
            drink_name = param["value"]
            break

    items = query_dynamodb_pk_sk(
        partition_key=PAIRINGS_PK,
        sort_key_portion=_sanitize_sort_key_portion(drink_name),
    )
    print("PAIRINGS ITEMS: ", items)

    return _stringify_items(
        items,
        detail_key="pairing_suggestions",
        fallback_message=(
            "I don't have curated pairings yet for that drink. "
            "Let me know the flavor profile and I'll craft ideas for you!"
        ),
    )


def action_group_create_bundles(parameters):
    theme = None
    budget = None
    for param in parameters:
        if param["name"] == "theme":
            theme = param["value"]
        if param["name"] == "budget":
            budget = param["value"]

    sort_key = _sanitize_sort_key_portion(theme)
    if budget:
        sort_key = f"{sort_key}#{_sanitize_sort_key_portion(budget)}"

    items = query_dynamodb_pk_sk(
        partition_key=BUNDLES_PK,
        sort_key_portion=sort_key,
    )
    print("BUNDLE ITEMS: ", items)

    return _stringify_items(
        items,
        detail_key="bundle_recommendations",
        fallback_message=(
            "I couldn't find a ready-made bundle for that occasion. "
            "Would you like me to craft one manually?"
        ),
    )


def lambda_handler(event, context):
    action_group = event["actionGroup"]
    _function = event["function"]
    parameters = event.get("parameters", [])

    print("PARAMETERS ARE: ", parameters)
    print("ACTION GROUP IS: ", action_group)

    # TODO: enhance this If-Statement approach to a dynamic one...
    if action_group == "LookupCatalog":
        results = action_group_lookup_catalog(parameters)
    elif action_group == "SuggestPairings":
        results = action_group_suggest_pairings(parameters)
    elif action_group == "CreateBundles":
        results = action_group_create_bundles(parameters)
    else:
        raise ValueError(f"Action Group <{action_group}> not supported.")

    # Convert the list of events to a string to be able to return it in the response as a string
    results_string = "\n-".join(results)
    response_body = {"TEXT": {"body": results_string}}

    action_response = {
        "actionGroup": action_group,
        "function": _function,
        "functionResponse": {"responseBody": response_body},
    }

    function_response = {
        "response": action_response,
        "messageVersion": event["messageVersion"],
    }
    print("Response: {}".format(function_response))

    return function_response
