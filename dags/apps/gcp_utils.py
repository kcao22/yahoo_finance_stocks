import yaml

from google.cloud import bigquery, storage
from airflow.models import Variable


def _create_client(service: str) -> set:
    """
    Instantiates GCP client based on service. Note json credentials have been configured as a part of local
    gcloud setup and subsequent mounting of file to airflow via docker volumes.
    """
    if service == "bq":
        return bigquery.Client(project=Variable.get("gcp_project_id"))
    elif service == "gcs":
        return storage.Client(project=Variable.get("gcp_project_id"))
    else:
        raise ValueError(f"Unsupported service: {service}")


def list_bigquery_datasets():
    client = _create_client(service="bq")
    print("Fetching datasets...")
    datasets = client.list_datasets()
    for dataset in datasets:
        print(f"Dataset ID: {dataset.dataset_id}")
    if not datasets:
        print("No datasets found.")
        return set()
    return set(dataset.dataset_id for dataset in datasets)


def list_bigquery_tables(dataset_id: str) -> set:
    client = _create_client(service="bq")
    print(f"Fetching tables for dataset: {dataset_id}...")
    tables = client.list_tables(dataset_id)
    for table in tables:
        print(f"Table ID: {table.table_id}")
    if not tables:
        print("No tables found.")
        return set()
    return set(table.table_id for table in tables)


def list_bigquery_schemas(dataset_id: str) -> set:
    client = _create_client(service="bq")
    tables = list_bigquery_tables(dataset_id)
    for table in tables:
        print(f"Table ID: {table.table_id}")
        table_ref = client.get_table(table)
        print("Schema:")
        for field in table_ref.schema:
            print(f"\tColumn: {field.name}\tType: ({field.field_type})")


def create_bigquery_table(dataset_id: str, table_id: str, schema: list):
    client = _create_client(service="bq")
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)
    print(f"Table {table.dataset_id}.{table.table_id} created.")


def delete_bigquery_table(dataset_id: str, table_id: str):
    client = _create_client(service="bq")
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    client.delete_table(
        table_ref,
        delete_contents=True,
        not_found_ok=True
    )
    print(f"Table {dataset_id}.{table_id} deleted.")


def load_file_to_bigquery(dataset_id: str, table_id: str, source_file_path: str, source_file_type: str, operation: str, schema: list, rows_to_skip: int = 1):
    client = _create_client(service="bq")
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    source_format = None
    if source_file_type == "csv":
        source_format = bigquery.SourceFormat.CSV
    else:
        raise ValueError(f"File type {source_file_type} not supported.")
    operation = None
    if operation == "append":
        operation = bigquery.WriteDisposition.WRITE_APPEND
    elif operation == "overwrite":
        operation = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
        raise ValueError(f"Operation {operation} not supported.")
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=source_format,
        skip_leading_rows=rows_to_skip,
        autodetect=False,
        write_disposition=operation
    )
    with open(source_file_path, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    load_job.result()
    print(f"File {source_file_path} loaded to {dataset_id}.{table_id}.")


def _fetch_ingress_ods_schemas(source_name: str, table_name: str) -> tuple:
    with open("config/bq_schemas.yaml", "r") as infile:
        config = yaml.safe_load(infile)
    source_config = None
    ingress_config = None
    ods_config = None
    for source in config["sources"]:
        if source == source_name:
            source_config = source
            break
    if not source_config:
        raise ValueError(f"Source {source_name} not found in config.")
    for table in source_config["tables"]:
        if table["table_name"] == table_name:
            if table["dataset_type"] == "ingress":
                ingress_config = table["schema"]
            elif table["dataset_type"] == "ods":
                ods_config = table["schema"]
    return ingress_config, ods_config


def _build_using_statement(ods_config: list[dict]) -> str:
    lines = []
    for field in ods_config:
        name = field["name"]
        dtype = field["type"]
        lines.append(f"SAFE_CAST(S.{name} AS {dtype}) AS {name}")
    return ",\n".join(lines)


def _build_update_statement(ods_config: list[dict]) -> str:
    lines = []
    for field in ods_config:
        name = field["name"]
        lines.append(f"D.{name} = S.{name}")
    return ",\n".join(lines)


def _build_insert_statement(ods_config: list[dict]) -> str:
    names = []
    values = []
    for field in ods_config:
        name = field["name"]
        names.append(name)
        values.append(f"S.{name}")
    cols_str = ", ".join(names)
    vals_str = ", ".join(values)
    return f"INSERT ({cols_str}) VALUES ({vals_str})"


def _build_primary_keys_statement(primary_keys: list[str]) -> str:
    lines = [f"D.{primary_key} = S.{primary_key}" for primary_key in primary_keys]
    return " AND ".join(lines)


def generate_merge_query(dataset_id: str, table_id: str, primary_keys: list):
    _, ods_config = _fetch_ingress_ods_schemas(source_name=dataset_id, table_name=table_id)
    return f"""
    MERGE `ods_{dataset_id}.{table_id}` D
    USING (
        SELECT
            {_build_using_statement(ods_config)}
        FROM `ingress_{dataset_id}.{table_id}` S
    ) S
    ON {_build_primary_keys_statement(primary_keys)}
    WHEN MATCHED THEN
        UPDATE SET
            {_build_update_statement(ods_config)}
    WHEN NOT MATCHED THEN
        {_build_insert_statement(ods_config)}
    """


def query_bigquery(query: str):
    client = _create_client(service="bq")
    query_job = client.query(query)
    return query_job.result()


def write_to_bigquery(dataset_id: str, table_id: str, dataframe):
    client = _create_client(service="bq")
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    load_job = client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
    load_job.result()
    print(f"Data loaded to {dataset_id}.{table_id}.")


def list_gcs_bucket_blobs(bucket_name: str):
    client = _create_client(service="gcs")
    blobs = client.list_blobs(Variable.get(bucket_name))
    return blobs


def upload_to_gcs(bucket_name: str, source_file_path: str, destination_blob_name: str):
    client = _create_client(service="gcs")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    print(f"File {source_file_path} uploaded to {bucket_name}/{destination_blob_name}.")


def delete_gcs_blob(bucket_name: str, blob_name: str):
    client = _create_client(service="gcs")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()
    print(f"Blob {blob_name} deleted from bucket {bucket_name}.")


def download_from_gcs(bucket_name: str, blob_name: str, destination_file_path: str):
    client = _create_client(service="gcs")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(destination_file_path)
    print(f"Blob {blob_name} downloaded from bucket {bucket_name} to {destination_file_path}.")


def copy_gcs_blob(source_bucket_name: str, source_blob_name: str, destination_bucket_name: str, destination_blob_name: str):
    client = _create_client(service="gcs")
    source_bucket = client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)
    destination_bucket = client.bucket(destination_bucket_name)
    source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)
    print(f"Blob {source_blob_name} copied from bucket {source_bucket_name} to {destination_bucket_name} as {destination_blob_name}.")
