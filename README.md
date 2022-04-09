# Real Estate Pipeline
An Airflow ETL data pipeline for extracting Real Estate API data and loading into and transforming in Databricks.

The Airflow DAG in this repo represents a pipeline to fetch, stage, and model data from an [API](https://rapidapi.com/apidojo/api/realty-in-us/) producing real estate data into Delta tables on Databricks that are ready to be loaded into a data warehouse. The data is requested from the API, compressed, and stored in a raw S3 bucket for preprocessing. It is then decompressed, filtered by date to ensure idempotency, and a CDC fingerprint column is added. This data is recompressed then stored in a new unstructured data S3 bucket. From there, the unstructured data is loaded into Databricks where the main Property table and any related nested tables are staged, modeled, and loaded into Delta tables on the Databricks cluster, ready for a final load into the data warehouse.  

**Airflow Version Used**

`2.2.4`

**Providers Used**
```
apache-airflow-providers-amazon==3.0.0
apache-airflow-providers-databricks==2.2.0
apache-airflow-providers-http==2.0.3
```

## How to Use

**Prerequisites**
- Airflow instance
- S3 Storage (AWS or AWS compatible, like MinIO)
- Databricks workspace
- RapidAPI Key

If you don't have the prerequisites, directions for setup/installation can be found here:
- [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html)
- [Minio](https://docs.min.io/docs/minio-quickstart-guide.html)
- [Databricks](https://docs.databricks.com/getting-started/index.html)
- [RapidAPI Registration](https://rapidapi.com/)

Loading the DAG and includes into Airflow will depend on how you've configured your Airflow environment. For a default setup, the simplest way would be to point the `dags_folder` and `plugins_folder` to the local dags/ and include/ folders from this repo. You could also directly copy the `batch_load.py` file into the `dags_folder` of your Airflow configuration and copy the include/ folder into the `plugins_folder` of your Airflow configuration. The location of these folders can be found in your `airflow.cfg` file.

A premium Databricks workspace is required as the Airflow DatabricksSubmitRunOperator executes the task using the Databricks API, which isn't supported by the Community Edition. 

MinIO can be used in place of an AWS account for S3 storage and can be configured locally. 

RapidAPI registration is free, however there are [bandwidth limitations](https://rapidapi.com/apidojo/api/realty-in-us/pricing) for free use. 

If you wish to use a different API you may need to make some changes to the source code, depending on the extra requirements from the API and the format the of data.
  - The `endpoints` variable at the top of the DAG would need to be changed to reflect the new APIs endpoints, for example.

Replace <YOUR_VARIABLE> with the variables for your environment 
  - Example - "http_conn_id": "<HTTP_CONNECTION_ID>" => "http_conn_id": "my_airflow_http_connection"

### Steps
1. Create an HTTP Connection in Airflow. Set the Host to `https://realty-in-us.p.rapidapi.com/properties/v2/`
2. Create an S3 Connection in Airflow.
    - If you're using MinIO, you will need to populate the Extra field with the following:   
        `{"aws_access_key_id": "<ACESS_KEY>", "aws_secret_access_key": "<SECRET_KEY>", "host": "<STORAGE_IP_ADDRESS OR DOMAIN NAME>"}`
3. Edit the `databricks_default` Connection. Set the Host, Login, and Password to your Databricks Workspace and login. 
    - This connection is created by default when installing the Databricks Airflow Provider
    - More information on  Databricks authentication can be found [here](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html)
4. Create an Airflow Variable called `properties_batch_config` with the following Val:

    ```
    {
      "http_conn_id": "<HTTP_CONNECTION_ID>", 
      "s3_conn_id": "<S3_CONNECTION_ID>", 
      "s3_raw_bucket": "<RAW_BUCKET_NAME>",
      "s3_unstructured_bucket": "<UNSTRUCTURED_BUCKET_NAME>", 
      "s3_bucket_region": "<AWS_REGION>", 
      "spark_cluster_id": "<CLUSTER_ID>", 
      "spark_notebook_path": "<NOTEBOOK_PATH>"
    }
    ```
5. Create an Airflow Variable called `api_data` with the following Val:

    ```
    {
      "offset": "<PAGE_OFFSET_VALUE>", 
      "limit": "<API_REQUEST_LIMIT>", 
      "city": "<CITY_NAME>", 
      "state_code": "<STATE_CODE>"
    }
    ```
6. Create an Airflow Variable called `api_headers` with the following Val:

    ```
    {
      "X-RapidAPI-Host": "realty-in-us.p.rapidapi.com",
      "X-RapidAPI-Key": "<API_KEY>"
    }
    ```

You should now be able to run the pipeline in Airflow!
  
