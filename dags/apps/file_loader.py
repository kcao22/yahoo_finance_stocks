from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from apps import gcp_utils


class BigQueryFileLoader:
    def set_parameters(
        self,
        table_dataset_id: str,
        table_id: str,
        operation: str,
        primary_keys: list[str] = [],
        rows_to_skip: int = 1,
    ):
        self.table_dataset_id = table_dataset_id
        self.table_id = table_id
        self.operation = operation

    def serialize(self):
        return self.__dict__

    def deserialize(self, data):
        self.__dict__.update(data)

    def build_dag(self):
        file_config = fetch_ods_schema(self.serialize())
        file_config = get_most_recent_ingress_file(file_config.serialize())
        file_config = load_ingress_file_to_ingress_table(file_config.serialize())
        file_config = write_ingress_to_ods(file_config.serialize())


@task
def fetch_ods_schema(data: dict):
    file_loader = BigQueryFileLoader()
    file_loader.deserialize(data)

    _, file_loader.table_schema = gcp_utils.fetch_ingress_ods_schemas(
        source_name=file_loader.table_dataset_id, table_name=file_loader.table_id
    )
    return file_loader.serialize()


@task
def get_most_recent_ingress_file(data: dict):
    file_loader = BigQueryFileLoader()
    file_loader.deserialize(data)

    most_recent_blob = None
    ingress_bucket = Variable.get("ingress_bucket")
    sorted_blobs = gcp_utils.list_gcs_bucket_blobs_by_update(
        bucket_name=ingress_bucket, prefix=file_loader.prefix, reverse=True
    )
    if sorted_blobs:
        most_recent_blob = sorted_blobs[0].name
        print(f"Most recent file found: {most_recent_blob}")
    else:
        raise AirflowSkipException(
            f"No files found in bucket {ingress_bucket}. Skipping..."
        )
    file_loader.most_recent_blob_uri = f"gs://{ingress_bucket}/{most_recent_blob}"
    return file_loader.serialize()


@task
def load_ingress_file_to_ingress_table(data: dict):

    file_loader = BigQueryFileLoader()
    file_loader.deserialize(data)

    gcp_utils.load_gcs_file_to_bigquery(
        dataset_id=file_loader.dataset_id,
        table_id=file_loader.table_id,
        blob_uri=file_loader.blob_uri,
        operation=file_loader.operation,
        schema=file_loader.table_schema,
        rows_to_skip=file_loader.rows_to_skip,
    )
    return file_loader.serialize()


@task
def write_ingress_to_ods(data: dict):
    file_loader = BigQueryFileLoader()
    file_loader.deserialize(data)

    query = ""
    write_disposition = None
    if file_loader.operation == "merge":
        query = gcp_utils.generate_merge_query(
            dataset_id=file_loader.table_dataset_id,
            table_id=file_loader.table_id,
            primary_keys=file_loader.primary_keys,
        )
    elif file_loader.operation == "append":
        query = gcp_utils.generate_select_from_ingress_query(
            dataset_id=file_loader.table_dataset_id, table_id=file_loader.table_id
        )
        write_disposition = "WRITE_APPEND"
    elif file_loader.operation == "replace":
        query = gcp_utils.generate_select_from_ingress_query(
            dataset_id=file_loader.table_dataset_id, table_id=file_loader.table_id
        )
        write_disposition = "WRITE_TRUNCATE"
    gcp_utils.execute_bq_query(query=query, write_disposition=write_disposition)
    return file_loader.serialize()
