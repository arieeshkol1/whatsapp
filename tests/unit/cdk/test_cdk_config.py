import json
from pathlib import Path


def test_dev_table_name_matches_interaction_history():
    repo_root = Path(__file__).resolve().parents[3]
    cdk_config = json.loads((repo_root / "cdk.json").read_text())

    dev_table_name = cdk_config["context"]["app_config"]["dev"]["table_name"]

    assert dev_table_name == "Interaction-history"
