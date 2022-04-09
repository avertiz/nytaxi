from google.cloud import bigquery

def create_vendor_table(client):
    table_id = "spheric-gecko-346716.nytaxi.vendor_dim"
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
    table_id = "spheric-gecko-346716.nytaxi.zone_dim"
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
    table_id = "spheric-gecko-346716.nytaxi.rate_code_dim"
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
    table_id = "spheric-gecko-346716.nytaxi.store_and_fwd_flag_dim"
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
    table_id = "spheric-gecko-346716.nytaxi.payment_type_dim"
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
    table_id = "spheric-gecko-346716.nytaxi.trip_stage"
    schema = [
        bigquery.SchemaField("vendor_id", "STRING", mode="REPEATED"),
        bigquery.SchemaField("tpep_pickup_datetime", "STRING", mode="REPEATED"),
        bigquery.SchemaField("tpep_dropoff_datetime", "STRING", mode="REPEATED"),
        bigquery.SchemaField("passenger_count", "STRING", mode="REPEATED"),
        bigquery.SchemaField("trip_distance", "STRING", mode="REPEATED"),
        bigquery.SchemaField("rate_code_id", "STRING", mode="REPEATED"),
        bigquery.SchemaField("store_and_fwd_flag", "STRING", mode="REPEATED"),
        bigquery.SchemaField("pu_location_id", "STRING", mode="REPEATED"),
        bigquery.SchemaField("do_location_id", "STRING", mode="REPEATED"),
        bigquery.SchemaField("payment_type_id", "STRING", mode="REPEATED"),
        bigquery.SchemaField("fare_amount", "STRING", mode="REPEATED"),
        bigquery.SchemaField("extra", "STRING", mode="REPEATED"),
        bigquery.SchemaField("mta_tax", "STRING", mode="REPEATED"),
        bigquery.SchemaField("tip_amount", "STRING", mode="REPEATED"),
        bigquery.SchemaField("tolls_amount", "STRING", mode="REPEATED"),
        bigquery.SchemaField("improvement_surcharge", "STRING", mode="REPEATED"),
        bigquery.SchemaField("total_amount", "STRING", mode="REPEATED"),
        bigquery.SchemaField("congestion_surcharge", "STRING", mode="REPEATED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(
        f'Created table {table.project}.{table.dataset_id}.{table.table_id}'
    )

def create_trip_table(client):
    table_id = "spheric-gecko-346716.nytaxi.trip_fact"
    schema = [
        bigquery.SchemaField("vendor_id", "INTEGER", mode="REPEATED"),
        bigquery.SchemaField("tpep_pickup_datetime", "TIMESTAMP", mode="REPEATED"),
        bigquery.SchemaField("tpep_dropoff_datetime", "TIMESTAMP", mode="REPEATED"),
        bigquery.SchemaField("passenger_count", "INTEGER", mode="REPEATED"),
        bigquery.SchemaField("trip_distance", "FLOAT", mode="REPEATED"),
        bigquery.SchemaField("rate_code_id", "INTEGER", mode="REPEATED"),
        bigquery.SchemaField("store_and_fwd_flag", "STRING", mode="REPEATED"),
        bigquery.SchemaField("pu_location_id", "INTEGER", mode="REPEATED"),
        bigquery.SchemaField("do_location_id", "INTEGER", mode="REPEATED"),
        bigquery.SchemaField("payment_type_id", "INTEGER", mode="REPEATED"),
        bigquery.SchemaField("fare_amount", "FLOAT", mode="REPEATED"),
        bigquery.SchemaField("extra", "FLOAT", mode="REPEATED"),
        bigquery.SchemaField("mta_tax", "FLOAT", mode="REPEATED"),
        bigquery.SchemaField("tip_amount", "FLOAT", mode="REPEATED"),
        bigquery.SchemaField("tolls_amount", "FLOAT", mode="REPEATED"),
        bigquery.SchemaField("improvement_surcharge", "FLOAT", mode="REPEATED"),
        bigquery.SchemaField("total_amount", "FLOAT", mode="REPEATED"),
        bigquery.SchemaField("congestion_surcharge", "FLOAT", mode="REPEATED"),
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