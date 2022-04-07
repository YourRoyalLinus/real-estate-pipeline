import datetime
import json
from datetime import datetime
from pendulum import UTC
from typing import TYPE_CHECKING, Optional, Sequence
from io import BytesIO
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
    """
    A class that wraps `airflow.models.baseoperator.BaseOperator` to create
    a task for fetching from an API and storing gzip-compressed
    data into an S3 bucket.

    Arguments:
        endpoint: API endpoint to make a request to.
        http_conn_id: Name of the Airflow HTTP connection id.
        http_headers: Optional dictionary of headers for the HTTP request.
        http_params:  Optional dictionary of parameters for the HTTP request.
        s3_conn_id: Name of the Airflow S3 connection id.
        s3_bucket_name: Name for the S3 bucket.
        s3_bucket_region: AWS region for the S3 bucket.
        s3_bucket_key: Bucket key for the fetched data.
    """

    _docs = """
    ### Purpose

    Abstractly, this task will make an HTTP request to an API endpoint, 
    retrying up to 5 times on failure. Upon finding data, it will compress it
    using gzip-compression and store the results in an S3 bucket. If the bucket
    does not exist, this task will create it.

    Failure to connect to the API endpoint is logged in Airflow; any other
    error is raised.

    #### Outputs
        
        This task creates an S3 bucket in the specified region of 
        gzip-compressed API data, compartmentalized by timestamp.

    #### Requirements

        - An API endpoint to make a request to
        - Airflow HTTP connection ID
        - Airflow S3 connection ID
        - Raw data S3 bucket name
        - S3 bucket region
        - Bucket key to identify data in S3 bucket

    #### Notes

        Headers and params for the HTTP request are considered optional,
        but most APIs will require one or both.
    """

    _http_retry_args = {
        "wait" : tenacity.wait.wait_exponential(),
        "stop" : tenacity.stop.stop_after_attempt(5),
    }

    # Used in airflow.models.BaseOperator
    template_fields :Sequence[str] = (
        "endpoint", "http_conn_id", 
        "http_headers", "http_params", 
        "s3_conn_id", "bucket_name", 
        "bucket_region", "bucket_key"
    )

    def __init__(self,
                *, 
                endpoint :str, 
                http_conn_id :str, 
                http_headers :Optional[dict] = None,
                http_params :Optional[dict] = None, 
                s3_conn_id :str, 
                s3_bucket_name :str, 
                s3_bucket_region :str, 
                s3_bucket_key :str,
                doc_md :Optional[str] = _docs,
                **kwargs
    ) -> None:
            super().__init__(**kwargs)
            self.http_conn_id = http_conn_id
            self.s3_conn_id = s3_conn_id
            self.http_headers = http_headers
            self.http_params = http_params
            self.endpoint = endpoint
            self.bucket_name = s3_bucket_name
            self.bucket_region = s3_bucket_region
            self.bucket_key = s3_bucket_key
            self.doc_md = doc_md
            
    def _handle_api_ingestion(operator, http_hook, s3_hook, log) -> None:
        """
        Make a request to the API endpoint, compress using gzip, and write 
        the data received from the API to an S3 bucket. If the S3 bucket 
        doesn't exist, create a new one.

        If the HTTP request fails, it will retry up to 5 times before
        logging an exception.
        
        Arguments:
            operator: The `IngressAPIDataOperator` being handled.
            http_hook: Airflow `HttpHook` to make requests to the API.
            s3_hook: Airflow `S3Hook` to access S3 storage.
            log: Reference to the Airflow `logger`.

        Returns:
            `None`
        """
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
        """
        Implemented method inherited from `BaseOperator` that is invoked by
        Airflow during task execution.

        Arguments:
            context: Airflow `Context`
        """
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        http_hook = HttpHook(http_conn_id=self.http_conn_id, method="GET")
        self._handle_api_ingestion(http_hook, s3_hook, self.log)
    
class PreprocessDataOperator(BaseOperator):
    """
    A class that wraps `airflow.models.baseoperator.BaseOperator` to create
    a task for loading gzip-compressed data, performing preprocessing
    and filtering operations and storing the gzip-compressed results in a new
    S3 bucket.

    Filtering removes any data where the following predicate is true:
    `raw_data[entry][operator.date_field_key] <= cutoff_timestamp`. The default
    time zone is `UTC`.

    Preprocessing adds a CDC key called `fingerprint`, which is a hash of
    all values of the entry of the JSON sorted by key for each non-filtered
    value found from the JSON object in the source bucket.  

    Arguments:
        data_key: Key to identify the data to be processed in the JSON.
        date_field_key: Key to identify the date field used to filter.  
        cutoff_timestamp: Datetime to filter against.
        s3_conn_id: Name of the Airflow S3 connection id. 
        s3_src_bucket: Name for the S3 bucket containing source data.
        s3_dest_bucket: Name for the S3 bucket to store the processed data.
        s3_bucket_region: AWS region for the S3 bucket.
        s3_bucket_key: Bucket key for the source and resultant data.
    """

    _docs = """
    ### Purpose

    Abstractly, this task will load gzip-compressed data from an S3 bucket,
    perform filtering and preprocessing operations, and compress the results
    using gzip-compression and store the unstructured data in a new S3 bucket 

    Empty data before or after filtering is logged to Airflow; any other
    error is raised.

    #### Outputs
        
        This task creates an S3 bucket in the specified region of 
        filtered, preprocessed, and gzip-compressed unstructured data,
        compartmentalized by timestamp.

    #### Requirements

        - S3 bucket with gzip-compressed raw source data
        - S3 Bucket key for source data 
        - S3 bucket to store preprocessed data
        - Airflow HTTP connection ID
        - Airflow S3 connection ID
        - Key to identify a date field
        - cutoff timestamp to filter by
        - Bucket key to identify data in S3 bucket

    #### Notes

        `data_key` is not required, but it must be used if you have a 
        complex JSON structure.
            - E.G: You include metadata as a separate entry on the same level
            as your API data.
    """

    # Used in airflow.models.BaseOperator
    template_fields :Sequence[str] = (
        "cutoff_timestamp", 
        "data_key", "date_field_key", 
        "s3_conn_id", "src_bucket", 
        "dest_bucket", "bucket_region", 
        "bucket_key"
    )
                                    
    def __init__(self,
                *, 
                data_key :Optional[str] = None,
                date_field_key :str, 
                cutoff_timestamp :datetime,
                s3_conn_id :str, 
                s3_src_bucket :str,
                s3_dest_bucket :str,
                s3_bucket_region :str,
                s3_bucket_key :str,
                doc_md :Optional[str] = _docs,
                **kwargs
    ) -> None:
            super().__init__(**kwargs)
            self.data_key = data_key
            self.date_field_key = date_field_key
            self.cutoff_timestamp = cutoff_timestamp
            self.s3_conn_id = s3_conn_id
            self.src_bucket = s3_src_bucket
            self.dest_bucket = s3_dest_bucket
            self.bucket_region = s3_bucket_region
            self.bucket_key = s3_bucket_key
            self.doc_md = doc_md

    def _load_data_stream(operator, s3_hook, log) -> dict:
        """
        Loads and decompresses gzip-compressed data from 
        `operator.bucket_key` in `operator.src_bucket`. `OSError` exceptions 
        are logged to Airflow.

        Arguments:
            operator: The `PreprocessDataOperator` being handled.
            s3_hook: Airflow `S3Hook` to access S3 storage.
            log: Reference to the Airflow `logger`.

        Returns:
            A `dict` representing the decompressed JSON.
        """
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
        """
        For each entry in `raw_data`, filter any record where 
        `raw_data[entry][operator.date_field_key] <= operator.cutoff_timestamp`.
        If `operator.cutoff_timestamp` is `None`, `datetime.min` is used 
        instead. 
        
        For each non-filtered record, create a fingerprint key in 
        `raw_data[entry]` whose value is a hash of `raw_data[entry]`, sorted by 
        keys.

        Arguments:
            operator: The `PreprocessDataOperator` being handled.
            raw_data: `list` of `dict` objects representing JSONs.
        
        Returns:
            The filtered and preprocessed `list` of entries from `raw_data`.
        """
        new_data = []
        for entry in range(0, len(raw_data)):
            record_timestamp = parser.parse(
                                    raw_data[entry][operator.date_field_key]
                                ).replace(tzinfo=UTC)
            if record_timestamp > operator.cutoff_timestamp \
                                if operator.cutoff_timestamp else datetime.min:
                raw_data[entry]["fingerprint"] = hash_json(raw_data, entry)
                
                new_data.append(raw_data[entry])

        return new_data
    
    def _preprocess_data(operator, s3_hook, log) -> None:
        """
        Load, filter, and preprocess data found in the object at 
        `operator.bucket_key` in `operator.src_bucket`. Store the results in
        `operator.dest_bucket`. 

        If no data is found, or the filtered data is empty, log to Airflow.

        Arguments:
            operator: The `PreprocessDataOperator` being handled.
            s3_hook: Airflow `S3Hook` to access S3 storage.
            log: Reference to the Airflow `logger`.

        Returns:
            `None`
        """
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
        """
        Implemented method inherited from BaseOperator that is invoked by
        Airflow during task execution.

        Arguments:
            context: Airflow `Context`
        """
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        self._preprocess_data(s3_hook, self.log)