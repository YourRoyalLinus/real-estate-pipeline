import gzip
from json import dumps
from minio import Minio
from airflow.decorators import task

class FileException(Exception):
    pass

def write_zipped_json_to(json :dict, file_path: str) -> None:
    with open(file_path, 'wb') as tmp_file:
        bytes = dumps(json).encode("utf-8")
        tmp_file.write(bytes)

    #Log
    return

@task
def upload_file(results :dict, file_name: str, client: Minio):
    try:
        properties = results["properties"]
    except KeyError as e:
        raise FileException(str(e))
    

