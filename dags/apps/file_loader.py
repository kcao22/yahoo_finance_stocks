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
        self.primary_keys = primary_keys
        self.rows_to_skip = rows_to_skip

    def serialize(self):
        return self.__dict__

    def deserialize(self, data):
        self.__dict__.update(data)

    def build_dag(self):
        initial_data = self.serialize()
        file_config_dict = fetch_ingress_ods_schema(initial_data)
        file_config_dict = get_most_recent_ingress_file(file_config_dict)
        file_config_dict = archive_most_recent_ingress_file(file_config_dict)
        file_config_dict = load_archive_file_to_ingress_table(file_config_dict)
        write_ingress_to_ods(file_config_dict)


@task
def fetch_ingress_ods_schema(data: dict):
    file_loader = BigQueryFileLoader()
    file_loader.deserialize(data)

    file_loader.ingress_schema, file_loader.ods_schema = gcp_utils.fetch_ingress_ods_schemas(
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
        bucket_name=ingress_bucket, prefix=f"data_sources/{file_loader.table_dataset_id}/{file_loader.table_id}/", reverse=True
    )
    if sorted_blobs:
        most_recent_blob = sorted_blobs[0].name
        print(f"Most recent file found: {most_recent_blob}")
    else:
        raise AirflowSkipException(
            f"No files found in bucket {ingress_bucket}. Skipping..."
        )
    file_loader.most_recent_blob = most_recent_blob
    file_loader.most_recent_blob_uri = f"gs://{ingress_bucket}/{most_recent_blob}"
    return file_loader.serialize()


@task
def archive_most_recent_ingress_file(data: dict):
    file_loader = BigQueryFileLoader()
    file_loader.deserialize(data)

    gcp_utils.copy_gcs_blob(
        source_bucket_name=Variable.get("ingress_bucket"),
        source_blob_name=file_loader.most_recent_blob,
        destination_bucket_name=Variable.get("archive_bucket"),
        destination_blob_name=file_loader.most_recent_blob
    )

    file_loader.archive_blob_uri = f"gs://{Variable.get('archive_bucket')}/{file_loader.most_recent_blob}"

    return file_loader.serialize()


@task
def load_archive_file_to_ingress_table(data: dict):

    file_loader = BigQueryFileLoader()
    file_loader.deserialize(data)

    # Truncate ingress table
    gcp_utils.execute_bq_query(
        query=f"TRUNCATE TABLE `ingress_{file_loader.table_dataset_id}.{file_loader.table_id}`"
    )

    gcp_utils.load_gcs_file_to_bigquery(
        dataset_id=f"ingress_{file_loader.table_dataset_id}",
        table_id=file_loader.table_id,
        blob_uri=file_loader.archive_blob_uri,
        operation=file_loader.operation,
        schema=file_loader.ingress_schema,
        rows_to_skip=file_loader.rows_to_skip,
    )
    gcp_utils.delete_gcs_blob(
        bucket_name=Variable.get("ingress_bucket"),
        blob_name=file_loader.most_recent_blob
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
            file_name=file_loader.most_recent_blob_uri
        )
    elif file_loader.operation == "append":
        query = gcp_utils.generate_select_from_ingress_query(
            dataset_id=file_loader.table_dataset_id, table_id=file_loader.table_id, file_name=file_loader.most_recent_blob_uri
        )
        write_disposition = "WRITE_APPEND"
    elif file_loader.operation == "replace":
        query = gcp_utils.generate_select_from_ingress_query(
            dataset_id=file_loader.table_dataset_id, table_id=file_loader.table_id, file_name=file_loader.most_recent_blob_uri
        )
        write_disposition = "WRITE_TRUNCATE"
    gcp_utils.execute_bq_query(query=query, dataset_id=f"ods_{file_loader.table_dataset_id}", table_id=file_loader.table_id, write_disposition=write_disposition)
    return file_loader.serialize()
