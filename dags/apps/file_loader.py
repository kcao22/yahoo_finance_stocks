import os

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

from apps import gcp_utils


class BigQueryFileLoader:
    def __init__(self, table_dataset_id: str, table_id: str, operation: str):
        self.table_dataset_id = table_dataset_id
        self.table_id = table_id
        self.operation = operation

    def serialize(self):
        return self.__dict__

    def deserialize(self, data):
        self.__dict__.update(data)

    def build_dag(self):





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
def load_ingress_file_to_ingress_table(table_schema: str, dataset_id: str, table_id: str, blob_uri: str, write_operation: str, rows_to_skip: int):
    gcp_utils.load_gcs_file_to_bigquery(
        dataset_id=dataset_id,
        table_id=table_id,
        blob_uri=blob_uri,
        operation=write_operation,
        schema=table_schema,
        rows_to_skip=rows_to_skip
    )


@task
def write_ingress_to_ods(target_dataset_id: str, target_table_id: str, primary_keys: list[str], operation: str):
    query = ""
    write_disposition = None
    if operation == "merge":
        query = gcp_utils.generate_merge_query(
            dataset_id=target_dataset_id,
            table_id=target_table_id,
            primary_keys=primary_keys
        )
    elif operation == "append":
        query = gcp_utils.generate_select_from_ingress_query(
            dataset_id=target_dataset_id,
            table_id=target_table_id
        )
        write_disposition = "WRITE_APPEND"
    elif operation == "replace":
        query = gcp_utils.generate_select_from_ingress_query(
            dataset_id=target_dataset_id,
            table_id=target_table_id   
        )
        write_disposition = "WRITE_TRUNCATE"
    gcp_utils.execute_bq_query(
        query=query,
        write_disposition=write_disposition
    )

