from datetime import timedelta
import os
import names
import pandas as pd
import pendulum
import tempfile

from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage as gcs

from prefect import task, Flow, Parameter
from prefect.tasks.secrets import PrefectSecret


BUCKET_NAME = "prefect-community"
PROJECT_NAME = "prefect-community"


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


@task(max_retries=3, retry_delay=timedelta(minutes=1), log_stdout=True)
def extract_and_load_raw_data(
        creds: dict, source_file_name: str, destination_file_name: str
):
    records = []
    for id_ in range(1, 1001):
        single_row = dict(
            id=id_,
            first_name=names.get_first_name(),
            last_name=names.get_last_name()
        )
        records.append(single_row)
    df = pd.DataFrame(records)
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
def load_to_bq(creds: dict, bq_schema: str, tbl_name: str, gsc_uri: str):
    credentials = service_account.Credentials.from_service_account_info(creds)
    bq_client = bigquery.Client(credentials=credentials)
    table_id = f"{PROJECT_NAME}.{bq_schema}.{tbl_name}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField(name="id", field_type="INTEGER", mode="REQUIRED"),
            bigquery.SchemaField(name="first_name", field_type="STRING", mode="REQUIRED"),
            bigquery.SchemaField(name="last_name", field_type="STRING", mode="REQUIRED"),
        ],
    )
    load_job = bq_client.load_table_from_uri(gsc_uri, table_id, job_config=job_config)
    load_job.result()  # Waits for the job to complete.
    destination_table = bq_client.get_table(table_id)
    print("The table has now {} rows.".format(destination_table.num_rows))


with Flow("dwh_raw_customers") as flow:
    bq_schema = Parameter("bq_schema", default="dwh")
    bq_tbl_name = Parameter("bq_tbl_name", default="raw_customers")
    creds_dict = PrefectSecret("GCP_CREDENTIALS")
    local_file_name = get_source_file_name()
    dest_file_name = get_destination_file_name(local_file_name, bq_schema, bq_tbl_name)
    gcs_uri = get_gcs_uri(dest_file_name)
    extract = extract_and_load_raw_data(creds_dict, local_file_name, dest_file_name)
    load = load_to_bq(
        creds_dict, bq_schema, bq_tbl_name, gcs_uri, upstream_tasks=[extract]
    )

if __name__ == "__main__":
    flow.run()
