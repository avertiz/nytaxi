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
    df = pd.read_csv(f'gs://nytaxi_data_raw/yellow_tripdata_{year_month}.csv')
    df = df.head(100)
    hashes = []
    for _, row in df.iterrows():
        hashes.append(hashlib.md5(str(row).encode('utf-8')).hexdigest())
    df['row_hash'] = hashes

    rows_to_insert = []
    for row in df.iterrows():
        temp_dict = {
            'vendor_id':row['VendorID'],
            'tpep_pickup_datetime':row['tpep_pickup_datetime'],
            'tpep_dropoff_datetime':row['tpep_dropoff_datetime'],
            'passenger_count':row['passenger_count'],
            'trip_distance':row['trip_distance'],
            'rate_code_id':row['RatecodeID'],
            'store_and_fwd_flag':row['store_and_fwd_flag'],
            'pu_location_id':row['PULocationID'],
            'do_location_id':row['DOLocationID'],
            'payment_type_id':row['payment_type'],
            'fare_amount':row['fare_amount'],
            'extra':row['extra'],
            'mta_tax':row['mta_tax'],
            'tip_amount':row['tip_amount'],
            'tolls_amount':row['tolls_amount'],
            'improvement_surcharge':row['improvement_surcharge'],
            'total_amount':row['total_amount'],
            'congestion_surcharge':row['congestion_surcharge'],
            'row_hash':row['row_hash'],
        }
        rows_to_insert.append(temp_dict)        

    query_client = bigquery.Client()
    table_id = f"{query_client.project}.nytaxi.trip_stage"
    errors = query_client.insert_rows_json(
        table_id, rows_to_insert, row_ids=[None] * len(rows_to_insert)
    )
    if errors == []:
        print(f"{len(rows_to_insert)} rows have been added to {table_id}.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

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