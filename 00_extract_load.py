import pandas as pd
from prefect import task, Flow, Parameter
from prefect.run_configs import LocalRun
from prefect.storage import GitHub


FLOW_NAME = "00_extract_load"
STORAGE = GitHub(
    repo="anna-geller/prefect-monte-carlo",
    path=f"flows/{FLOW_NAME}.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)


@task
def extract_and_load(dataset: str) -> None:
    file = f"https://raw.githubusercontent.com/anna-geller/jaffle_shop/main/data/{dataset}.csv"
    df = pd.read_csv(file)
    # logic to load data to your data warehouse


with Flow(FLOW_NAME, storage=STORAGE, run_config=LocalRun(labels=["dev"]),) as flow:
    dataset = Parameter("dataset", default="raw_customers")
    ingestion = extract_and_load(dataset)
