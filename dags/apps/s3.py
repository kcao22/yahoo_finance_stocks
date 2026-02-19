import boto3

from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

from apps.print_utils import print_logging_info_decorator


def _create_client():
    client = boto3.client(
        "s3",
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
    )
    return client


@print_logging_info_decorator(redacted_params=["body"])
def put_object(bucket: str, key: str, body: str, **kwargs) -> None:
    """
    Puts object to target s3 bucket.
    :param bucket: S3 target bucket name.
    :param key: Full prefix and name of file.
    :param body: Content of file.
    :param kwargs: Keyword arguments.
    :return: File key.
    """
    client = _create_client()
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        **kwargs
    )


@print_logging_info_decorator
def get_object(bucket: str, key: str, **kwargs) -> dict:
    """
    Gets object from a bucket.
    :param bucket: Bucket name.
    :param key: Full path of the object.
    :param kwargs: Keyword arguments.
    :return: Object content.
    """
    client = _create_client()
    return client.get_object(
        Bucket=bucket,
        Key=key,
        **kwargs
    )


@print_logging_info_decorator
def copy_object(source_bucket: str, source_key: str, target_bucket: str, target_key: str, **kwargs) -> None:
    """
    Copies object from a source bucket to target bucket.
    :param source_bucket: Source bucket name.
    :param source_key: Full path of the source object.
    :param target_bucket: Target bucket name.
    :param target_key: Full path of target object.
    :param kwargs: Keyword arguments.
    :return: None.
    """
    print(f"Getting object from bucket {source_bucket} with key {source_key}")
    client = _create_client()
    client.copy_object(
        Bucket=target_bucket,
        Key=target_key,
        CopySource={"Bucket": source_bucket, "Key": source_key},
    )


@print_logging_info_decorator
def delete_object(bucket: str, key: str, **kwargs) -> None:
    client = _create_client()
    client.delete_object(
        Bucket=bucket,
        Key=key,
        **kwargs
    )


@print_logging_info_decorator
def download_file(bucket: str, key: str, filename: str, **kwargs) -> None:
    """
    Downloads file from target S3 bucket to /tmp directory.
    :param bucket: S3 target bucket name.
    :param key: Full prefix and name of file.
    :param filename: Name of file to be downloaded.
    :param kwargs: Keyword arguments.
    :return: Path of downloaded file.
    """
    client = _create_client()
    client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=filename,
        **kwargs
    )
    return filename


@print_logging_info_decorator
def list_objects(bucket: str, prefix: str = "", **kwargs):
    contents = []
    client = _create_client()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, **kwargs):
        contents.extend(page.get("Contents", []))
    return {
        "Contents": contents
    }


@print_logging_info_decorator
def get_most_recent_file(bucket: str, prefix: str = "") -> str:
    files = list_objects(
        bucket=bucket,
        prefix=prefix
    )
    if not files.get("Contents"):
        raise AirflowSkipException(f"No files found in bucket {bucket} with prefix {prefix}")
    most_recent_file = {}
    for file in files["Contents"]:
        if most_recent_file == {}:
            most_recent_file = file
            continue
        elif file["LastModified"] > most_recent_file["LastModified"]:
            most_recent_file = file
    if most_recent_file and most_recent_file["Key"]:
        if most_recent_file["Key"].startswith("/"):
            most_recent_file["Key"] = most_recent_file["Key"][1:]
    print(f"Most recent file found from {bucket} with prefix {prefix}: {most_recent_file['Key']}")
    return most_recent_file["Key"] if most_recent_file else None
