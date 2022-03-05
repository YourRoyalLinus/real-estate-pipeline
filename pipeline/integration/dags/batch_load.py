from io import StringIO
from datetime import datetime
from airflow import DAG, AirflowException
from airflow.models.variable import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import tenacity
import logging
from flatten_json import flatten

default_args = {
    "depends_on_past" : False,
    "retries" : 0,
    "catchup" : False
}

http_retry_args = {
    "wait" : tenacity.wait.wait_exponential(),
    "stop" : tenacity.stop.stop_after_attempt(5),
}

s3_conn_id = "s3_conn"
http_conn_id = "real_estate_api"
timestamp = "{{ ts_nodash }}"

def fetch_and_store_api_data(endpoint :str, timestamp :str) -> None: 
    headers = {
        'x-rapidapi-host' : Variable.get('API_HOST'), 
        'x-rapidapi-key' : Variable.get('API_KEY') 
    }
    params = {
        "city" : "Saratoga",
        "state_code" : "NY",
        "offset" : 0,
        "limit" : 200
    }
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    http_hook = HttpHook(http_conn_id=http_conn_id, method="GET")
    response = http_hook.run_with_advanced_retry(
        headers=headers,
        endpoint=endpoint,
        data=params,
        _retry_args=http_retry_args
    )
    
    bucket = "real-estate-dwh-integration"
    if not s3_hook.check_for_bucket(bucket):
        s3_hook.create_bucket(bucket, "us-east-1")

    try:
        http_hook.check_response(response)
    except AirflowException:
        s3_hook.load_string("", 
                            f"/{timestamp}/{endpoint}_{timestamp}.json",
                            bucket_name=bucket, replace=True)
        return

    s3_hook.load_string(response.text, 
                        f"/{timestamp}/{endpoint}_{timestamp}.json",
                        bucket_name=bucket, replace=True)

def process_raw_data(endpoint :str, timestamp :str):
    bucket = "real-estate-dwh-integration"
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)

    data_stream = StringIO(s3_hook.read_key(
                        key=f"/{timestamp}/{endpoint}_{timestamp}.json",
                        bucket_name=bucket))

    data = json.loads(data_stream.getvalue())
    data_stream.close()
    
    try:
        properties :dict = data["properties"]
    except KeyError:
        logging.info("No data to process from "
                    f"/{timestamp}/{endpoint}_{timestamp}.json")
        return

    for entry in range(0, len(properties)):
        flattened_json = flatten(properties[entry])
        properties[entry] = flattened_json

    processed_data_string = json.dumps(flattened_json)
    s3_hook.load_string(processed_data_string, 
                        f"/processed/{timestamp}/{endpoint}_{timestamp}.json",
                        bucket_name=bucket, replace=True)
    
with DAG(dag_id="load_daily_batch", 
    description="Fetch the daily batch of data from the Real Estate API",
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 4, 17, 50, 0),
    end_date=datetime(2022, 4, 1, 11, 59, 59),
    default_args=default_args
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    endpoints = ["v2/list-sold", "v2/list-for-rent", "v2/list-for-sale"]
    groups = []
    for g_id in range(0, len(endpoints)):
        ep = endpoints[g_id]
        with TaskGroup(group_id=f"group_{g_id+1}".format(ep)) as tg_1:
            fetch_and_store_data = PythonOperator(
                task_id='fetch_and_store_{}_data'.format(ep.split("/")[1]),
                python_callable=fetch_and_store_api_data,
                op_kwargs={"endpoint":ep, "timestamp":timestamp}
            ) 
            preprocess_stored_data = PythonOperator(
                task_id='preprocess_{}_data'.format(ep.split("/")[1]),
                python_callable=process_raw_data,
                op_kwargs={"endpoint":ep, "timestamp":timestamp}
            )

            fetch_and_store_data >> preprocess_stored_data
            groups.append(tg_1) 

            
    start >> groups >> end