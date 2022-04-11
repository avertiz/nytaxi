# nytaxi
This project is meant to demonstrate a full ELT pipeline using Google Cloud Platform. The data that will be used is the NY yellow taxi trip records: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page.

Services/Technology that will be used:
- Google Cloud Storage to store raw data
- Google BigQuery as the data warehouse
- Google Cloud Composer to orchestrate/schedule pipeline

## Prerequisites
- A Google Cloud Platform Account

### CloudStorage
1. Create a storage bucket. I named mine nytaxi_data_raw
    - When choosing a location, it is important to remain consistent across all  GCP services within the project. I chose us-central1 (Iowa) 

### BigQuery
1. Create a dataset within the project.
    - Run setup/nytaxi_table_setup.py
        - This will create schema of all tables
    - Run setup/nytaxi_dim_table_populate.py
        - This will populate dimension tables
2. The fact table will be populated with the data found on https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

### Cloud Composer (Airflow)
- First DAG (airflow/dags/download_taxi_to_gcs_and_bq.py):
    - Pulls a given month of yellow cab data from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    - Loads csv into blob then pulls from blob and populates staging table 
    - Runs on an ad-hoc basis initially, will setup later to pull all files
- Second DAG is TODO

### DBT
- Uses staging table as source and populates fact table. Work in progress...