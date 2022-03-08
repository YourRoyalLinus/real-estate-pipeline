from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from include.integration import fetch_and_store_api_data, process_raw_data

default_args = {
    "depends_on_past" : False,
    "retries" : 0,
    "catchup" : False
}

timestamp = "{{ ts_nodash }}"

with DAG(dag_id="load_daily_batch", 
    description="Fetch the daily batch of data from the Real Estate API",
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 4, 17, 50, 0),
    end_date=datetime(2022, 4, 1, 11, 59, 59),
    default_args=default_args,
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