import pandas as pd
from prefect import task, Flow, Task
from prefect.run_configs import LocalRun
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.monte_carlo.monte_carlo_lineage import (
    MonteCarloCreateOrUpdateLineage,
)


DATASET = "raw_customers"
FLOW_NAME = "01_extract_load_update_lineage"
STORAGE = GitHub(
    repo="anna-geller/prefect-monte-carlo",
    path=f"flows/{FLOW_NAME}.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)
monte_carlo = MonteCarloCreateOrUpdateLineage(
    source=dict(
        node_name=f"source_{DATASET}",
        object_id=f"source_{DATASET}",
        object_type="table",
        resource_name="ecommerce_system",
        tags=[{"propertyName": "dataset_owner", "propertyValue": "marketing"}],
    ),
    destination=dict(
        node_name=f"prefect-community:jaffle_shop.{DATASET}",
        object_id=f"prefect-community:jaffle_shop.{DATASET}",
        object_type="table",
        resource_name="bigquery-2021-12-09T11:47:30.306Z",
        tags=[{"propertyName": "dataset_owner", "propertyValue": "marketing"}],
    ),
    expire_at="2042-01-01T00:00:00.000",
)


@task
def extract_and_load(dataset: str) -> None:
    file = f"https://raw.githubusercontent.com/anna-geller/jaffle_shop/main/data/{dataset}.csv"
    df = pd.read_csv(file)
    # logic to load data to your data warehouse


with Flow(FLOW_NAME, storage=STORAGE, run_config=LocalRun(labels=["dev"]),) as flow:
    api_key_id = PrefectSecret("MONTE_CARLO_API_KEY_ID")
    api_token = PrefectSecret("MONTE_CARLO_API_SECRET_KEY")
    ingestion = extract_and_load(DATASET)
    lineage = monte_carlo(
        api_key_id=api_key_id, api_token=api_token, upstream_tasks=[ingestion]
    )
