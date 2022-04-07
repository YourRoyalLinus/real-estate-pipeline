from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.variable import Variable
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from include.operators.IngestionOperators import IngressAPIDataOperator
from include.operators.IngestionOperators import PreprocessDataOperator

docs = """
### Daily Real Estate Batch Load

#### Purpose

This DAG makes extracts, loads, and models sold property, for sale property, 
and for rent property data from the following [RapidAPI Real Estate API](https://rapidapi.com/apidojo/api/realty-in-us/)
and stages a set of Delta tables ready to be loaded into a data warehouse.

#### Outputs

This pipeline creates the following Delta tables on the Databricks cluster:

    - `staging.addresses`: Addresses table
    - `staging.agents`: Real Estate Agents table
    - `staging.offices`: Real Estate Agencies table
    - `staging.mls`: Multiple Listing Service table
    - `staging.neighborhoods`: Many-to-many Neighborhoods table
    - `staging.photos`: Many-to-one Property photos table
    - `staging.phones`: Many-to-one Office and Agent phones table
    - `staging.properties`: Properties for sale/for rent/sold table

#### Requirements

    - `properties_batch_config` Airflow variable containing keys for:
        - X-RapidAPI-Host and X-RapidAPI-Key passed as headers in HTTP request
        - city, state_code, limit, offset passed as params in HTTP request 
        - Airflow HTTP connection ID
        - Airflow S3 connection ID
        - Raw data S3 bucket name
        - Unstructured data S3 bucket name
        - S3 bucket region
        - Databricks cluster ID
        - Databricks notebook path

    - The `databricks_default` Airflow connection must also be updated with
    the correct Databricks host instance and authentication information

#### Notes

    All tables are extracted and modeled from nested JSONs object within
    each property JSON.
"""

default_args = {
    "depends_on_past" : False,
    "retries" : 0
}
#Reading these is delayed until task execution
timestamp = "{{ ds }}"
last_success_timestamp = "{{ prev_start_date_success }}"
endpoints = ["list-sold", "list-for-rent", "list-for-sale"] 

with DAG(dag_id="daily_real_estate_batch_load", 
    description="Fetch the daily batch of data from the Real Estate API",
    schedule_interval='@daily',
    start_date=datetime(2022, 4, 1, 0, 0, 0),
    end_date=datetime(2022, 4, 29, 0, 0, 0),
    render_template_as_native_obj=True,
    catchup=False,
    doc_md=docs,
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
                http_conn_id=batch_config["http_conn_id"],
                http_headers="{{ var.json.api_headers }}",
                http_params="{{ var.json.api_data }}",
                s3_conn_id=batch_config["s3_conn_id"],
                s3_bucket_name=batch_config["s3_raw_bucket"],
                s3_bucket_region=batch_config["s3_bucket_region"],
                s3_bucket_key=s3_bucket_key
            )
            
            preprocess_stored_data = PreprocessDataOperator(
                task_id="preprocess_{}_data".format(endpoint_fmt),
                data_key = "properties",
                date_field_key="last_update",
                cutoff_timestamp=last_success_timestamp,
                s3_conn_id=batch_config["s3_conn_id"],
                s3_src_bucket=batch_config["s3_raw_bucket"],
                s3_dest_bucket=batch_config["s3_unstructured_bucket"],
                s3_bucket_region=batch_config["s3_bucket_region"],
                s3_bucket_key=s3_bucket_key
            )
                        
            transform_data = DatabricksSubmitRunOperator(
                task_id="transform_{}_data".format(endpoint_fmt), 
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
            transform_data

    start >> ingest_api_data >> end