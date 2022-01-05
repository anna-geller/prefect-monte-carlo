"""
To test the flow with no tags:
prefect run --name 02_add_custom_lineage_tags --param lineage_tags=null --execute
"""
from prefect import Flow, Parameter
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.monte_carlo.monte_carlo_lineage import (
    MonteCarloCreateOrUpdateNodeWithTags,
)


FLOW_NAME = "02_add_custom_lineage_tags"
STORAGE = GitHub(
    repo="anna-geller/prefect-monte-carlo",
    path=f"flows/{FLOW_NAME}.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)
monte_carlo = MonteCarloCreateOrUpdateNodeWithTags(
    object_type="table", resource_name="bigquery-2021-12-09T11:47:30.306Z"
)


with Flow(FLOW_NAME, storage=STORAGE) as flow:
    api_key_id = PrefectSecret("MONTE_CARLO_API_KEY_ID")
    api_token = PrefectSecret("MONTE_CARLO_API_SECRET_KEY")
    full_table_name = Parameter(
        "full_table_name", default="prefect-community:jaffle_shop.raw_payments"
    )
    lineage_tags = Parameter(
        "lineage_tags",
        default=[{"propertyName": "data_owner", "propertyValue": "sales"}],
    )
    monte_carlo(
        node_name=full_table_name,
        object_id=full_table_name,
        lineage_tags=lineage_tags,
        api_key_id=api_key_id,
        api_token=api_token,
    )
