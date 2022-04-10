from google.cloud import bigquery
import requests
from pprint import pprint

def update_dim_table(table_name, rows_to_insert, client):
    table_id = f"{client.project}.nytaxi.{table_name}"
    errors = client.insert_rows_json(
        table_id, rows_to_insert, row_ids=[None] * len(rows_to_insert)
    )
    if errors == []:
        print(f"{len(rows_to_insert)} rows have been added to {table_id}.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

def get_zone_lookup_data():
    url = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
    data = requests.get(url)
    data_list = []
    iter_lines = data.iter_lines()
    next(iter_lines, None) # skipping header row
    for line in iter_lines:
        temp_list = line.decode('utf-8').split(',')
        temp_dict = {
            "location_id":int(temp_list[0]),
            "borough":temp_list[1].strip('\"'),
            "zone":temp_list[2].strip('\"'),
            "service_zone":temp_list[3].strip('\"'),
        }
        data_list.append(temp_dict)
    return(data_list)

def main():
    client = bigquery.Client()

    rows_to_insert_vendor = [
        {"vendor_id": 1, "vendor_name": 'Creative Mobile Technologies, LLC'},
        {"vendor_id": 2, "vendor_name": 'VeriFone Inc.'},
    ]
    update_dim_table(table_name = 'vendor_dim',rows_to_insert=rows_to_insert_vendor, client=client)

    rows_to_insert_zone = get_zone_lookup_data()
    update_dim_table(table_name = 'zone_dim',rows_to_insert=rows_to_insert_zone, client=client)

    rows_to_insert_ratecode = [
        {"rate_code_id": 1, "rate_code_name": 'Standard rate'},
        {"rate_code_id": 2, "rate_code_name": 'JFK'},
        {"rate_code_id": 3, "rate_code_name": 'Newark'},
        {"rate_code_id": 4, "rate_code_name": 'Nassau or Westchester'},
        {"rate_code_id": 5, "rate_code_name": 'Negotiated fare'},
        {"rate_code_id": 6, "rate_code_name": 'Group ride'},
    ]
    update_dim_table(table_name = 'rate_code_dim',rows_to_insert=rows_to_insert_ratecode, client=client)

    rows_to_insert_store_and_fwd_flag = [
        {"store_and_fwd_id": 'Y', "store_and_fwd_name": 'store and forward trip'},
        {"store_and_fwd_id": 'N', "store_and_fwd_name": 'not a store and forward trip'},
    ]
    update_dim_table(table_name = 'store_and_fwd_flag_dim',rows_to_insert=rows_to_insert_store_and_fwd_flag, client=client)

    rows_to_insert_payment_type = [
        {"payment_type_id": 1, "payment_type_name": 'Credit card'},
        {"payment_type_id": 2, "payment_type_name": 'Cash'},
        {"payment_type_id": 3, "payment_type_name": 'No charge'},
        {"payment_type_id": 4, "payment_type_name": 'Dispute'},
        {"payment_type_id": 5, "payment_type_name": 'Unknown'},
        {"payment_type_id": 6, "payment_type_name": 'Voided trip'},
    ]
    update_dim_table(table_name = 'payment_type_dim',rows_to_insert=rows_to_insert_payment_type, client=client)

if __name__ == '__main__':
    main()