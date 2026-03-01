import os

import yaml
from airflow.models import Variable
from google.cloud import bigquery, storage


def _create_client(service: str) -> set:
    """Instantiate a Google Cloud client for the requested service.

    :param service: Short name of the service to instantiate. Supported
        values are ``"bq"`` for BigQuery and ``"gcs"`` for Cloud Storage.
    :return: A service client instance (BigQuery or Storage client).
    :raises ValueError: If an unsupported service name is provided.
    Note:
        JSON credentials are expected to be configured for the environment
        (e.g. via gcloud and mounted to Airflow containers).
    """
    if service == "bq":
        return bigquery.Client(project=Variable.get("gcp_project_id"))
    elif service == "gcs":
        return storage.Client(project=Variable.get("gcp_project_id"))
    else:
        raise ValueError(f"Unsupported service: {service}")


def list_bigquery_datasets():
    """List dataset IDs in the configured BigQuery project.

    :return: A set of dataset IDs (strings). If no datasets found, returns an
        empty set.
    """
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
    """List table IDs for a given BigQuery dataset.

    :param dataset_id: The dataset identifier to list tables from.
    :return: A set of table IDs (strings). If no tables found, returns an empty set.
    """
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
    """Print schema information for all tables in a dataset.

    :param dataset_id: The dataset identifier to inspect.
    :return: None. Schema information is printed to stdout.
    """
    client = _create_client(service="bq")
    tables = list_bigquery_tables(dataset_id)
    for table in tables:
        print(f"Table ID: {table.table_id}")
        table_ref = client.get_table(table)
        print("Schema:")
        for field in table_ref.schema:
            print(f"\tColumn: {field.name}\tType: ({field.field_type})")


def create_bigquery_table(dataset_id: str, table_id: str, schema: list):
    """Create a BigQuery table with the provided schema.

    :param dataset_id: The dataset in which to create the table.
    :param table_id: The name of the table to create.
    :param schema: A list of BigQuery schema field definitions (e.g. instances
        of ``bigquery.SchemaField``) describing the table columns.
    :return: The created BigQuery table object.
    """
    client = _create_client(service="bq")
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)
    print(f"Table {table.dataset_id}.{table.table_id} created.")


def delete_bigquery_table(dataset_id: str, table_id: str):
    """Delete a BigQuery table if it exists.

    :param dataset_id: Dataset containing the table to delete.
    :param table_id: Table identifier to delete.
    :return: None. Prints a confirmation message on success.
    """
    client = _create_client(service="bq")
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    client.delete_table(table_ref, delete_contents=True, not_found_ok=True)
    print(f"Table {dataset_id}.{table_id} deleted.")


def load_local_file_to_bigquery(
    dataset_id: str,
    table_id: str,
    source_file_path: str,
    source_file_type: str,
    operation: str,
    schema: list,
    rows_to_skip: int = 1,
):
    """Load a local file into a BigQuery table.

    :param dataset_id: Target BigQuery dataset.
    :param table_id: Target BigQuery table.
    :param source_file_path: Local path to the source file to load.
    :param source_file_type: File format (currently supports ``"csv"``).
    :param operation: Write disposition; either ``"append"`` or ``"overwrite"``.
    :param schema: List of BigQuery schema fields (used when autodetect is False).
    :param rows_to_skip: Number of header rows to skip (default: 1).
    :return: None. Waits for the load job to complete and prints a confirmation.
    :raises ValueError: If an unsupported file type or operation is provided.
    """
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
        write_disposition=operation,
    )
    with open(source_file_path, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file, table_ref, job_config=job_config
        )
    load_job.result()
    print(f"File {source_file_path} loaded to {dataset_id}.{table_id}.")


def load_gcs_file_to_bigquery(
    dataset_id: str,
    table_id: str,
    blob_uri: str,
    operation: str,
    schema: list,
    rows_to_skip: int = 1,
):
    """
    Downloads an object from GCS and writes file to a BigQuery table.
    :param blob_uri: The URI of the object in the GCS bucket.
    dataset_id: Target BigQuery dataset.
    table_id: Target BigQuery table.
    """
    client = _create_client(service="bq")
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    source_format = None
    file_type = os.path.splitext(os.path.basename(blob_uri))[1]
    if file_type == "csv":
        source_format = bigquery.SourceFormat.CSV
    else:
        raise ValueError(f"Format {file_type} support not yet implemented.")
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=source_format,
        skip_leading_rows=rows_to_skip,
        autodetect=False,
        write_disposition=operation,
    )
    load_job = client.load_table_from_uri(blob_uri, table_id, job_config=job_config)
    load_job.result()
    print(f"File {blob_uri} loaded to {dataset_id}.{table_id}.")


def fetch_ingress_ods_schemas(source_name: str, table_name: str) -> tuple:
    """Fetch ingress and ODS schemas for a given source/table from YAML.

    :param source_name: The logical source (usually the dataset id) to look up
        in the ``config/bq_schemas.yaml`` file.
    :param table_name: The table name to find within the source's table list.
    :return: A tuple of (ingress_schema, ods_schema) where each element is the
        schema structure loaded from YAML (or ``None`` if not present).
    :raises ValueError: If the source is not found in the configuration file.
    """
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


def _build_using_statement(ods_config: list[dict], file_name: str) -> str:
    """Build the SELECT list used in a MERGE USING clause.

    :param ods_config: List of field definitions (each a dict containing at
        least ``name`` and ``type`` keys).
    :param file_name: String name of the file that the data record is being loaded from.
    :return: A string containing comma-separated SAFE_CAST expressions for use
        in the USING subquery.
    """
    lines = []
    for field in ods_config:
        name = field["name"]
        dtype = field["type"]
        lines.append(f"SAFE_CAST(S.{name} AS {dtype}) AS {name}")
    lines.append("CURRENT_TIMESTAMP() AS load_datetime")
    lines.append(f"{file_name} AS load_filename")
    return ",\n".join(lines)


def _build_update_statement(ods_config: list[dict]) -> str:
    """Build the UPDATE SET clause for a MERGE statement.

    :param ods_config: List of field definition dicts containing a ``name`` key.
    :return: A string containing comma-separated assignment expressions for
        the UPDATE clause.
    """
    lines = []
    for field in ods_config:
        name = field["name"]
        lines.append(f"D.{name} = S.{name}")
    return ",\n".join(lines)


def _build_insert_statement(ods_config: list[dict]) -> str:
    """Build the INSERT clause for a MERGE statement.

    :param ods_config: List of field definition dicts containing a ``name`` key.
    :return: A string representing an INSERT statement fragment with the
        column list and corresponding S.* values.
    """
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
    """Build the equality condition for primary keys used in MERGE ON.

    :param primary_keys: List of primary key column names.
    :return: A string joining equality comparisons with ``AND`` for the MERGE
        ON clause.
    """
    lines = [f"D.{primary_key} = S.{primary_key}" for primary_key in primary_keys]
    return " AND ".join(lines)


def generate_merge_query(dataset_id: str, table_id: str, primary_keys: list):
    """Generate a BigQuery MERGE query to merge ingress into ODS.

    :param dataset_id: Logical dataset/source name used in config and table
        naming (used to build ``ingress_`` and ``ods_`` table identifiers).
    :param table_id: Table name to merge.
    :param primary_keys: List of primary key column names used for matching rows.
    :return: A string containing the MERGE SQL statement.
    """
    _, ods_config = fetch_ingress_ods_schemas(
        source_name=dataset_id, table_name=table_id
    )
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


def generate_select_from_ingress_query(dataset_id: str, table_id: str):
    _, ods_config = fetch_ingress_ods_schemas(
        source_name=dataset_id, table_name=table_id
    )
    return f"""
    SELECT
    {_build_using_statement(ods_config)}
    FROM `ingress_{dataset_id}.{table_id}` S
    """


def execute_bq_query(
    query: str,
    dataset_id: str = None,
    table_id: str = None,
    write_disposition: str = None,
):
    """Execute a SQL query against BigQuery and return the result iterator.

    :param query: SQL query string to execute.
    :return: The BigQuery query result iterator (rows can be iterated over).
    """
    job_config = None
    destination_table = ""
    if dataset_id and table_id:
        if not write_disposition:
            raise ValueError("Please provide a write disposition for query.")
        destination_table = f"`{dataset_id}.{table_id}`"
    elif (dataset_id and not table_id) or (table_id and not dataset_id):
        raise ValueError("Please provide dataset and table if job config is needed.")
    job_config = bigquery.QueryJobConfig(
        destination=destination_table, write_disposition=write_disposition
    )
    client = _create_client(service="bq")
    query_job = client.query(query, job_config=job_config)
    return query_job.result()


def write_df_to_bigquery(dataset_id: str, table_id: str, dataframe):
    """Load a pandas DataFrame into a BigQuery table.

    :param dataset_id: Target BigQuery dataset.
    :param table_id: Target BigQuery table.
    :param dataframe: A pandas DataFrame to load.
    :return: None. Waits for the load job to finish and prints a confirmation.
    """
    client = _create_client(service="bq")
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    load_job = client.load_table_from_dataframe(
        dataframe, table_ref, job_config=job_config
    )
    load_job.result()
    print(f"Data loaded to {dataset_id}.{table_id}.")


def list_gcs_bucket_blobs(bucket_name: str, prefix: str):
    """List blobs in a Cloud Storage bucket under a prefix.

    :param bucket_name: Airflow Variable name or actual bucket name to list.
    :param prefix: Blob prefix to filter results.
    :return: An iterator of Blob objects from Google Cloud Storage.
    """
    client = _create_client(service="gcs")
    blobs = client.list_blobs(Variable.get(bucket_name), prefix=prefix)
    return blobs


def list_gcs_bucket_blobs_by_update(
    bucket_name: str, prefix: str, reverse: bool = True
):
    """Return blobs from a bucket sorted by their update timestamp.

    :param bucket_name: Airflow Variable name or actual bucket name to list.
    :param prefix: Currently unused; reserved for future filtering by prefix.
    :param reverse: If True, newest-first; otherwise oldest-first.
    :return: A list of Blob objects sorted by their ``updated`` attribute.
    """
    client = _create_client(service="gcs")
    blobs = client.list_blobs(Variable.get(bucket_name), prefix=prefix)
    return [blob for blob in blobs].sort(lambda x: x.update, reverse=reverse)


def upload_to_gcs(bucket_name: str, source_file_path: str, destination_blob_name: str):
    """Upload a local file to a Google Cloud Storage bucket.

    :param bucket_name: Name of the destination GCS bucket.
    :param source_file_path: Local file path to upload.
    :param destination_blob_name: Destination path/name in the bucket.
    :return: None. Prints a confirmation on success.
    """
    client = _create_client(service="gcs")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    print(f"File {source_file_path} uploaded to {bucket_name}/{destination_blob_name}.")


def delete_gcs_blob(bucket_name: str, blob_name: str):
    """Delete a blob from a GCS bucket.

    :param bucket_name: Name of the bucket containing the blob.
    :param blob_name: Name/path of the blob to delete.
    :return: None. Prints a confirmation on success.
    """
    client = _create_client(service="gcs")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()
    print(f"Blob {blob_name} deleted from bucket {bucket_name}.")


def download_from_gcs(bucket_name: str, blob_name: str, destination_file_path: str):
    """Download a blob from GCS to a local path.

    :param bucket_name: Name of the source GCS bucket.
    :param blob_name: Name/path of the blob to download.
    :param destination_file_path: Local destination file path.
    :return: None. Prints a confirmation on success.
    """
    client = _create_client(service="gcs")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(destination_file_path)
    print(
        f"Blob {blob_name} downloaded from bucket {bucket_name} to {destination_file_path}."
    )


def copy_gcs_blob(
    source_bucket_name: str,
    source_blob_name: str,
    destination_bucket_name: str,
    destination_blob_name: str,
):
    """Copy a blob from one GCS bucket to another.

    :param source_bucket_name: Name of the bucket containing the source blob.
    :param source_blob_name: Name/path of the source blob.
    :param destination_bucket_name: Name of the target bucket.
    :param destination_blob_name: Destination name/path in the target bucket.
    :return: None. Prints a confirmation on success.
    """
    client = _create_client(service="gcs")
    source_bucket = client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)
    destination_bucket = client.bucket(destination_bucket_name)
    source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)
    print(
        f"Blob {source_blob_name} copied from bucket {source_bucket_name} to {destination_bucket_name} as {destination_blob_name}."
    )
