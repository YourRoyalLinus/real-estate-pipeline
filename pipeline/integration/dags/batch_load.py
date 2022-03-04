from airflow.decorators import dag
from datetime import datetime
from tasks import *

default_args = {
    "depends_on_past" : False,
    "retries" : 1,
    "catchup" : False
}

@dag(dag_id="load_daily_batch", 
    description="Fetch the daily batch of data from the Real Estate API",
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 14, 0, 0, 0),
    end_date=datetime(2022, 4, 1, 11, 59, 59),
    default_args=default_args
)
def taskflow():
    api_calls = [
        fetch_v2_for_rent_properties,
        fetch_v2_for_sale_properties,
        fetch_v2_sold_properties,
        fetch_v2_mls_properties
    ]
    try:
        start_session()
        #get minio client

        #for call in api_calls
        #make call
        #given json format into file
        #write file to mio
    except APIExcpetion as e:
        #log
        pass
    finally:
        cleanup_session()


dag = taskflow()
