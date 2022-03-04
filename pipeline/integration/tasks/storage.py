from ast import Str
from minio import Minio
from minio.error import S3Error
from airflow.decorators import task
from airflow.models import variable

class StorageException(Exception):
    pass

@task
def create_minio_client() -> dict:
    endpoint = variable.get("MINIO_ENDPOINT")
    access_key = variable.get("MINIO_ACCESS_KEY")
    secret_key = variable.get("MINIO_SECRET_KEY")

    try:
        client = Minio(endpoint, access_key=access_key, secret_key=secret_key)
    except S3Error as e:
        raise StorageException(str(e))
    
    return {"minio_client":client}

@task
def create_bucket(client :Minio, bucket_name :Str) -> None: #Need to rethink what's a task and what's a func that's part of a task
    try:
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
        else:
            #Log bucket already exists?
            pass
    except S3Error as e:
        raise StorageException(str(e))


@task
def write_file_to_client(client :Minio, bucket_name :str, file_name :str,
        file_path :str) -> None:
    try:
        client.fput_object(bucket_name, file_name, file_path)
    except S3Error as e:
        raise StorageException(str(e))
