# Real Estate Pipeline
An Airflow ELT data pipeline for extracting Real Estate API data to loading and transforming in Databricks.

The Airflow DAG in this repo represents a pipeline to fetch, stage, and model data from an [API](https://rapidapi.com/apidojo/api/realty-in-us/) producing real estate data into Delta tables on Databricks that are ready to be loaded into a data warehouse. The data is requested from the API, compressed, and stored in a raw S3 bucket for preprocessing. It is then decompressed, filtered by date to ensure idempotency, and a CDC fingerprint column is added. This data is stored recompressed in a new unstructured data S3 bucket. From there, the unstructured data is loaded into Databricks where the main Property table and any related nested tables are staged, modeled, and loaded into Delta tables on the Databricks cluster, ready for a final load into the data warehouse.  

**Airflow Version Used**

`2.2.4`

**Providers Used**
```
apache-airflow-providers-amazon==3.0.0
apache-airflow-providers-databricks==2.2.0
apache-airflow-providers-http==2.0.3
```
