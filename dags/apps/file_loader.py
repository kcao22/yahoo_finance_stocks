import os

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

from apps import gcp_utils


class BigQueryFileLoader:
    def __init__(self, schema: str, table: str):
        self.schema = schema
        self.table = table



@task
def get_most_recent_ingress_file(prefix: str):
    most_recent_blob = None
    ingress_bucket = Variable.get("ingress_bucket")
    sorted_blobs = gcp_utils.list_gcs_bucket_blobs_by_update(bucket_name=ingress_bucket, prefix=prefix, reverse=True)
    if sorted_blobs:
        most_recent_blob = sorted_blobs[0]
        print(f"Most recent file found: {sorted_blobs[0].name}")
    else:
        raise AirflowSkipException(f"No files found in bucket {ingress_bucket}. Skipping...")
    return most_recent_blob[0]


@task
def load_ingress_file_to_ingress_table(schema: str, table: str):
    gcp_utils.load_file_to_bigquery(
        dataset_id=schema,
        table_id=table,
        source_file_path=
    )