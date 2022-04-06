from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.variable import Variable
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from include.operators.IngestionOperators import IngressAPIDataOperator
from include.operators.IngestionOperators import PreProcessDataOperator

default_args = {
    "depends_on_past" : False,
    "retries" : 0
}
#Reading these is delayed until task execution
timestamp = "{{ ds }}"
last_success_timestamp = "{{ prev_start_date_success }}"
endpoints = ["list-sold", "list-for-rent", "list-for-sale"] 

with DAG(dag_id="daily_real_estate_api_batch_load", 
    description="Fetch the daily batch of data from the Real Estate API",
    schedule_interval='@daily',
    start_date=datetime(2022, 4, 1, 0, 0, 0),
    end_date=datetime(2022, 4, 29, 0, 0, 0),
    render_template_as_native_obj=True,
    catchup=False,
    default_args=default_args
) as dag:
    batch_config = Variable.get("properties_batch_config", 
                                deserialize_json=True)

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    with TaskGroup("ingest_property_data") as ingest_api_data: 
        for endpoint in endpoints:
            endpoint_fmt = endpoint.replace("-", "_")
            s3_bucket_key = f"/{timestamp}/{endpoint_fmt}_{timestamp}.json"

            fetch_and_store_data = IngressAPIDataOperator(
                task_id = "fetch_and_store_{}_data".format(endpoint_fmt),
                endpoint=endpoint,
                timestamp=timestamp,
                http_conn_id=batch_config["http_conn_id"],
                http_headers="{{ var.json.api_headers }}",
                http_params="{{ var.json.api_data }}",
                s3_conn_id=batch_config["s3_conn_id"],
                s3_bucket_name=batch_config["s3_raw_bucket"],
                s3_bucket_region=batch_config["s3_bucket_region"],
                s3_bucket_key=s3_bucket_key
            )
            
            preprocess_stored_data = PreProcessDataOperator(
                task_id="preprocess_{}_data".format(endpoint_fmt),
                data_key = "properties",
                date_field_key="last_update",
                endpoint=endpoint,
                timestamp=timestamp,
                cutoff_timestamp=last_success_timestamp,
                s3_conn_id=batch_config["s3_conn_id"],
                s3_src_bucket=batch_config["s3_raw_bucket"],
                s3_dest_bucket=batch_config["s3_unstructured_bucket"],
                s3_bucket_region=batch_config["s3_bucket_region"],
                s3_bucket_key=s3_bucket_key
            )
                        
            extract_and_transform_data = DatabricksSubmitRunOperator(
                task_id="extract_and_transform_{}_data".format(endpoint_fmt), 
                existing_cluster_id=batch_config["spark_cluster_id"], 
                notebook_task={
                    "notebook_path": batch_config["spark_notebook_path"], 
                    "base_parameters": {
                        "aws_bucket_name": batch_config["s3_unstructured_bucket"],
                        "aws_region": batch_config["s3_bucket_region"],
                        "data_endpoint": endpoint
                    }
                }
            )

            fetch_and_store_data >> \
            preprocess_stored_data >> \
            extract_and_transform_data

    start >> ingest_api_data >> end