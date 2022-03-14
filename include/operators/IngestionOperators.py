import datetime
import json
from datetime import datetime
from pendulum import UTC
from typing import TYPE_CHECKING, Optional, Sequence
from io import StringIO, BytesIO
from dateutil import parser
from airflow import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.utils.ingestion_utils import hash_json, compress_json 
from include.utils.ingestion_utils import decompress_json
import tenacity

if TYPE_CHECKING:
    from airflow.utils.context import Context

class IngressAPIDataOperator(BaseOperator):

    template_fields :Sequence[str] = (
                                        "endpoint", "timestamp",
                                        "http_conn_id", "http_headers", 
                                        "http_params", "s3_conn_id", 
                                        "bucket_name", "bucket_region", 
                                        "bucket_key"
                                    )

    _http_retry_args = {
        "wait" : tenacity.wait.wait_exponential(),
        "stop" : tenacity.stop.stop_after_attempt(5),
    }

    def __init__(self,
                *, 
                endpoint :str, 
                timestamp :str, 
                http_conn_id :str, 
                http_headers :Optional[dict] = None,
                http_params :Optional[dict] = None, 
                s3_conn_id :str, 
                s3_bucket_name :str, 
                s3_bucket_region :str, 
                s3_bucket_key :str,
                **kwargs
    ) -> None:
            super().__init__(**kwargs)
            self.http_conn_id = http_conn_id
            self.s3_conn_id = s3_conn_id
            self.http_headers = http_headers
            self.http_params = http_params
            self.endpoint = endpoint
            self.timestamp = timestamp
            self.bucket_name = s3_bucket_name
            self.bucket_region = s3_bucket_region
            self.bucket_key = s3_bucket_key
            
    def _handle_api_ingestion(operator, http_hook, s3_hook, log) -> None:
        response = http_hook.run_with_advanced_retry(
            headers=operator.http_headers,
            endpoint=operator.endpoint,
            data=operator.http_params,
            _retry_args=operator._http_retry_args
        )

        try:
            http_hook.check_response(response)
        except AirflowException as e:
            log.warning(f"Fetching '{operator.endpoint}' returned an exception: "
                        f"{str(e)}")
            return 

        if not s3_hook.check_for_bucket(operator.bucket_name):
            s3_hook.create_bucket(operator.bucket_name, operator.bucket_region)
        
        compressed_bytes = compress_json(json.dumps(response.json()))
        s3_hook.load_bytes(compressed_bytes, 
                            operator.bucket_key + ".gz",
                            bucket_name=operator.bucket_name, 
                            replace=True)

    def execute(self, context :'Context') -> None:
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        http_hook = HttpHook(http_conn_id=self.http_conn_id, method="GET")
        self._handle_api_ingestion(http_hook, s3_hook, self.log)
    
class PreProcessDataOperator(BaseOperator):

    template_fields :Sequence[str] = (
                                        "endpoint", "timestamp",
                                        "cutoff_timestamp", "data_key", 
                                        "date_field_key", "s3_conn_id", 
                                        "src_bucket", "dest_bucket", 
                                        "bucket_region", "bucket_key"
                                    )
                                    
    def __init__(self,
                *, 
                data_key :Optional[str] = None,
                date_field_key :str, 
                endpoint :str, 
                timestamp :str, 
                cutoff_timestamp :datetime,
                s3_conn_id :str, 
                s3_src_bucket :str,
                s3_dest_bucket :str,
                s3_bucket_region :str,
                s3_bucket_key :str,
                **kwargs
    ) -> None:
            super().__init__(**kwargs)
            self.data_key = data_key
            self.date_field_key = date_field_key
            self.endpoint = endpoint
            self.timestamp = timestamp
            self.cutoff_timestamp = cutoff_timestamp
            self.s3_conn_id = s3_conn_id
            self.src_bucket = s3_src_bucket
            self.dest_bucket = s3_dest_bucket
            self.bucket_region = s3_bucket_region
            self.bucket_key = s3_bucket_key

    def _load_data_stream(operator, s3_hook, log) -> StringIO:
        with BytesIO(s3_hook.get_key(
                            key=operator.bucket_key + ".gz",
                            bucket_name=operator.src_bucket
                    ).get()["Body"].read()
        ) as io:
            try:
                data = decompress_json(io.getvalue())
            except OSError as e:
                log.error("Loading data from S3 returned an exception: "
                         f"{str(e)}")
        return data

    def _filter_and_preprocess(operator, raw_data) -> dict:
        new_data = []
        for entry in range(0, len(raw_data)):
            record_timestamp = parser.parse(
                                    raw_data[entry][operator.date_field_key]
                                ).replace(tzinfo=UTC)
            if record_timestamp > operator.cutoff_timestamp or datetime.min:
                raw_data[entry]["fingerprint"] = hash_json(raw_data, entry)
                
                new_data.append(raw_data[entry])

        return new_data
    
    def _preprocess_data(operator, s3_hook, log) -> None:
        data_json = operator._load_data_stream(s3_hook, log)
        contains_metadata = bool(operator.data_key)
        try:
            raw_data :list[dict] = data_json[operator.data_key] \
                                    if contains_metadata else data_json
        except KeyError:
            log.warning(f"No data to process from {operator.bucket_key}")
            return
        
        preprocessed_data = operator._filter_and_preprocess(raw_data)

        if preprocessed_data:
            if contains_metadata:
                data_json[operator.data_key] = preprocessed_data
            else:
                data_json = preprocessed_data
        
            compressed_bytes = compress_json(json.dumps(data_json))

            if not s3_hook.check_for_bucket(operator.dest_bucket):
                s3_hook.create_bucket(operator.dest_bucket, 
                                      operator.bucket_region)

            s3_hook.load_bytes(compressed_bytes, 
                               operator.bucket_key + ".gz",
                               bucket_name=operator.dest_bucket, 
                               replace=True)
        else:
            log.info(f"No new data from {operator.bucket_key}")
            
    def execute(self, context :'Context') -> None:
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        self._preprocess_data(s3_hook, self.log)