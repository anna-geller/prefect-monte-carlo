from prefect import Flow, task
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.monte_carlo.monte_carlo_lineage import MonteCarloGetResources

monte_carlo = MonteCarloGetResources()


@task(log_stdout=True)
def print_resources(output):
    for item in output:
        print(item)


with Flow("query_monte_carlo_resources") as flow:
    api_key_id = PrefectSecret("MONTE_CARLO_API_KEY_ID")
    api_token = PrefectSecret("MONTE_CARLO_API_SECRET_KEY")
    resources = monte_carlo(api_key_id=api_key_id, api_token=api_token)
    print_resources(resources)

if __name__ == "__main__":
    flow.run()
