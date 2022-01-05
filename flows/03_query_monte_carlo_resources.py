from prefect import Flow, task
from prefect.storage import GitHub
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.monte_carlo.monte_carlo_lineage import MonteCarloGetResources

FLOW_NAME = "03_query_monte_carlo_resources"
STORAGE = GitHub(
    repo="anna-geller/prefect-monte-carlo",
    path=f"flows/{FLOW_NAME}.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)
monte_carlo = MonteCarloGetResources()


@task(log_stdout=True)
def print_resources(output):
    print(output)


with Flow(FLOW_NAME, storage=STORAGE) as flow:
    api_key_id = PrefectSecret("MONTE_CARLO_API_KEY_ID")
    api_token = PrefectSecret("MONTE_CARLO_API_SECRET_KEY")
    resources = monte_carlo(api_key_id=api_key_id, api_token=api_token)
    print_resources(resources)
