import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from google.cloud import storage
from google.cloud import bigquery

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

def load_to_bq_staging():
    pass

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

    upload_to_gcs_task