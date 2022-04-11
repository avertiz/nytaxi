import pandas as pd
import hashlib

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from google.cloud import storage, bigquery

# CLI arguments to provide: '{"year_month":"YYYY-MM"}'

def get_file(year_month):
    # format of date needs to be YYYY_MM
    url = f'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{year_month}.csv'
    data = pd.read_csv(url)
    return(data)

def upload_to_gcs(bucket_name, destination_blob_name, **kwargs):
    data = get_file(year_month = kwargs["year_month"])
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(data.to_csv(index=False), 'text/csv')

def delete_staging():
    # First clear staging table
    client = bigquery.Client()
    sql = f'DELETE FROM `{client.project}.nytaxi.trip_stage` WHERE true;'
    query_job = client.query(sql)
    query_job.result()

def load_staging_from_bucket(year_month): 
    # next load staging table
    client = bigquery.Client()
    table_id = f"{client.project}.nytaxi.trip_stage"
    url = f'gs://nytaxi_data_raw/yellow_tripdata_{year_month}.csv'

    job_config = bigquery.LoadJobConfig(
        schema = [
            bigquery.SchemaField("vendor_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("tpep_pickup_datetime", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("tpep_dropoff_datetime", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("passenger_count", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("trip_distance", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("rate_code_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("store_and_fwd_flag", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("pu_location_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("do_location_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("payment_type_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("fare_amount", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("extra", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("mta_tax", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("tip_amount", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("tolls_amount", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("improvement_surcharge", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("total_amount", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("congestion_surcharge", "STRING", mode="NULLABLE"),
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )    

    load_job = client.load_table_from_uri(
        url, table_id, job_config=job_config
    )
    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print(f"Loaded {destination_table.num_rows} rows.")

with DAG(
    dag_id="download_taxi_to_gcs_and_bq",
    schedule_interval=None,
    default_args={
        "owner": "airflow",
        "start_date": days_ago(1),
        "depends_on_past": False,
        "retries": 0,
    },
    catchup=False,
    max_active_runs=1,
    tags=['nytaxi'],
) as dag:

    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket_name": 'nytaxi_data_raw',
            'destination_blob_name': f"""yellow_tripdata_{{{{ dag_run.conf['year_month'] }}}}.csv""",
            'year_month': "{{ dag_run.conf['year_month'] }}"
        },
    )

    delete_staging_task = PythonOperator(
        task_id="delete_staging_task",
        python_callable=delete_staging,
    )

    load_staging_from_bucket_task = PythonOperator(
        task_id="load_staging_from_bucket_task",
        python_callable=load_staging_from_bucket,
        op_kwargs={'year_month': "{{ dag_run.conf['year_month'] }}"},
    )

    upload_to_gcs_task >> delete_staging_task >> load_staging_from_bucket_task