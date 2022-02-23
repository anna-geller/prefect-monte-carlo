from datetime import datetime, timedelta
import os
import pandas as pd
import pendulum
import random
import tempfile
from uuid import uuid4

from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage as gcs

from prefect import task, Flow, Parameter
from prefect.triggers import always_run
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.monte_carlo.monte_carlo_lineage import (
    MonteCarloCreateOrUpdateNodeWithTags,
)

# prefect-community:dwh.raw_orders
BUCKET_NAME = "prefect-community"
PROJECT_NAME = "prefect-community"
monte_carlo = MonteCarloCreateOrUpdateNodeWithTags(
    object_type="table",
    resource_name="bigquery-2021-12-09T11:47:30.306Z",
    trigger=always_run,
)


@task(log_stdout=True)
def get_source_file_name() -> str:
    now = pendulum.now(tz="UTC")
    date_string = now.to_date_string().replace("-", "_")
    time_string = now.to_time_string().replace(":", "_")
    return f"{date_string}__{time_string}.parquet"


@task(log_stdout=True)
def get_destination_file_name(file_name: str, schema_name: str, tbl_name: str):
    return f"{schema_name}/{tbl_name}/{file_name}"


@task(log_stdout=True)
def get_gcs_uri(destination_file_name: str):
    return f"gs://{BUCKET_NAME}/{destination_file_name}"


@task(log_stdout=True)
def get_full_table_name(bq_schema_name: str, bq_tbl_name: str):
    return f"{PROJECT_NAME}:{bq_schema_name}.{bq_tbl_name}"


@task(max_retries=3, retry_delay=timedelta(minutes=1), log_stdout=True)
def extract_and_load_raw_data(
    creds: dict, source_file_name: str, destination_file_name: str
):
    records = []
    for _ in range(100):
        single_row = dict(
            id=str(uuid4()),
            user_id=random.randint(1, 1001),
            order_date=datetime.utcnow().date(),
            status=random.choice(
                ["completed", "placed", "shipped", "return_pending", "returned"]
            ),
        )
        records.append(single_row)
    df = pd.DataFrame(records)
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["status"] = df["status"].astype("category")
    with tempfile.TemporaryDirectory() as tmpdir:
        local_file_path = os.path.join(tmpdir, source_file_name)
        df.to_parquet(local_file_path, index=False)
        upload_local_file_to_gcs(creds, local_file_path, destination_file_name)
        print(f"File {source_file_name} saved to GCS")


def upload_local_file_to_gcs(
    creds: dict, source_file_name: str, destination_file_name: str
):
    credentials = service_account.Credentials.from_service_account_info(creds)
    gcs_client = gcs.Client(project=PROJECT_NAME, credentials=credentials)
    bucket = gcs_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name=destination_file_name)
    blob.upload_from_filename(source_file_name)


@task(max_retries=3, retry_delay=timedelta(minutes=1), log_stdout=True)
def load_to_bq(creds: dict, bq_schema_name: str, bq_table_name: str, gsc_uri: str):
    credentials = service_account.Credentials.from_service_account_info(creds)
    bq_client = bigquery.Client(credentials=credentials)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField(name="id", field_type="STRING", mode="REQUIRED"),
            bigquery.SchemaField(name="user_id", field_type="INTEGER", mode="REQUIRED"),
            bigquery.SchemaField(
                name="order_date", field_type="TIMESTAMP", mode="REQUIRED"
            ),
            bigquery.SchemaField(name="status", field_type="STRING", mode="REQUIRED"),
        ],
    )
    table_id = f"{PROJECT_NAME}.{bq_schema_name}.{bq_table_name}"
    load_job = bq_client.load_table_from_uri(gsc_uri, table_id, job_config=job_config)
    load_job.result()  # Waits for the job to complete.
    destination_table = bq_client.get_table(table_id)
    print("The table has now {} rows.".format(destination_table.num_rows))


with Flow("dwh_raw_orders") as flow:
    bq_schema = Parameter("bq_schema", default="dwh")
    bq_tbl = Parameter("bq_tbl", default="raw_orders")
    creds_dict = PrefectSecret("GCP_CREDENTIALS")
    api_key_id = PrefectSecret("MONTE_CARLO_API_KEY_ID")
    api_token = PrefectSecret("MONTE_CARLO_API_SECRET_KEY")
    local_file_name = get_source_file_name()
    dest_file_name = get_destination_file_name(local_file_name, bq_schema, bq_tbl)
    gcs_uri = get_gcs_uri(dest_file_name)
    full_table_name = get_full_table_name(bq_schema, bq_tbl)
    extract = extract_and_load_raw_data(creds_dict, local_file_name, dest_file_name)
    load = load_to_bq(creds_dict, bq_schema, bq_tbl, gcs_uri, upstream_tasks=[extract])
    monte_carlo(
        node_name=full_table_name,
        object_id=full_table_name,
        lineage_tags=[
            {"propertyName": "data_owner", "propertyValue": "sales"},
            {"propertyName": "data_source_system", "propertyValue": "shop"},
            # {"propertyName": "a_brand_new_tag", "propertyValue": "workshop"},
        ],
        api_key_id=api_key_id,
        api_token=api_token,
        upstream_tasks=[load],
        # prefect_context_tags=True
    )

if __name__ == "__main__":
    # flow.visualize()
    flow.run()
