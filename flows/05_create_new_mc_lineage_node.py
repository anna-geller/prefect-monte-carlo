from prefect import Flow, task
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.monte_carlo.monte_carlo_lineage import (
    MonteCarloCreateOrUpdateLineage,
)

# prefect-community:dwh.orders
SOURCE = "prefect-community:dwh.orders"
DESTINATION = "data_science_forecast"
monte_carlo = MonteCarloCreateOrUpdateLineage(
    destination=dict(
        node_name=DESTINATION,
        object_id=DESTINATION,
        object_type="custom-bi-report",  # C = Custom node in Monte Carlo
        resource_name="bigquery-2021-12-09T11:47:30.306Z",
        tags=[{"propertyName": "dataset_owner", "propertyValue": "data_science_team"}],
    ),
    source=dict(
        node_name=SOURCE,
        object_id=SOURCE,
        object_type="table",
        resource_name="bigquery-2021-12-09T11:47:30.306Z",
        tags=[{"propertyName": "dataset_owner", "propertyValue": "sales"}],
    ),
    expire_at="2022-02-23T14:00:00.000",
    # expire_at="2042-01-01T00:00:00.000",
)


@task
def generate_sales_forecast() -> None:
    pass


with Flow("sales_forecast") as flow:
    api_key_id = PrefectSecret("MONTE_CARLO_API_KEY_ID")
    api_token = PrefectSecret("MONTE_CARLO_API_SECRET_KEY")
    forecast = generate_sales_forecast()
    lineage = monte_carlo(
        api_key_id=api_key_id, api_token=api_token, upstream_tasks=[forecast]
    )

if __name__ == "__main__":
    flow.run()