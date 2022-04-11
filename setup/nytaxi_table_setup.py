from google.cloud import bigquery

def create_vendor_table(client):
    table_id = f"{client.project}.nytaxi.vendor_dim"
    schema = [
        bigquery.SchemaField("vendor_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("vendor_name", "STRING", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(
        f'Created table {table.project}.{table.dataset_id}.{table.table_id}'
    )

def create_zone_table(client):
    table_id = f"{client.project}.nytaxi.zone_dim"
    schema = [
        bigquery.SchemaField("location_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("borough", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("zone", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("service_zone", "STRING", mode="NULLABLE"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(
        f'Created table {table.project}.{table.dataset_id}.{table.table_id}'
    )    

def create_ratecode_table(client):
    table_id = f"{client.project}.nytaxi.rate_code_dim"
    schema = [
        bigquery.SchemaField("rate_code_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("rate_code_name", "STRING", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(
        f'Created table {table.project}.{table.dataset_id}.{table.table_id}'
    )

def create_store_and_fwd_flag_table(client):
    table_id = f"{client.project}.nytaxi.store_and_fwd_flag_dim"
    schema = [
        bigquery.SchemaField("store_and_fwd_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("store_and_fwd_name", "STRING", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(
        f'Created table {table.project}.{table.dataset_id}.{table.table_id}'
    )   

def create_payment_type_table(client):
    table_id = f"{client.project}.nytaxi.payment_type_dim"
    schema = [
        bigquery.SchemaField("payment_type_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("payment_type_name", "STRING", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(
        f'Created table {table.project}.{table.dataset_id}.{table.table_id}'
    )

def create_trip_stage_table(client):
    table_id = f"{client.project}.nytaxi.trip_stage"
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
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(
        f'Created table {table.project}.{table.dataset_id}.{table.table_id}'
    )

def create_trip_table(client):
    table_id = f"{client.project}.nytaxi.trip_fact"
    schema = [
        bigquery.SchemaField("vendor_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("tpep_pickup_datetime", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("tpep_dropoff_datetime", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("passenger_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("trip_distance", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("rate_code_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("store_and_fwd_flag", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pu_location_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("do_location_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("payment_type_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("fare_amount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("extra", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("mta_tax", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("tip_amount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("tolls_amount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("improvement_surcharge", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("total_amount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("congestion_surcharge", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("row_hash", "BYTES", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(
        f'Created table {table.project}.{table.dataset_id}.{table.table_id}'
    )

def main():
    client = bigquery.Client()
    create_vendor_table(client=client)
    create_zone_table(client=client)
    create_ratecode_table(client=client)
    create_store_and_fwd_flag_table(client=client)
    create_payment_type_table(client=client)
    create_trip_stage_table(client=client)
    create_trip_table(client=client)

if __name__ == '__main__':    
    main()