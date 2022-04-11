{{ config(materialized='incremental') }}

with source_data as (

    SELECT
        CAST(CAST(vendor_id AS FLOAT64) AS INT64) AS vendor_id,
        CAST(tpep_pickup_datetime AS DATETIME) AS tpep_pickup_datetime,
        CAST(tpep_dropoff_datetime AS DATETIME) AS tpep_dropoff_datetime,
        CAST(CAST(passenger_count AS FLOAT64) AS INT64) AS passenger_count,
        CAST(trip_distance AS FLOAT64) AS trip_distance,
        CAST(CAST(rate_code_id AS FLOAT64) AS INT64) AS rate_code_id,
        store_and_fwd_flag,
        CAST(pu_location_id AS INT64) AS pu_location_id,
        CAST(do_location_id AS INT64) AS do_location_id,
        CAST(CAST(payment_type_id AS FLOAT64) AS INT64) AS payment_type_id,
        CAST(fare_amount AS FLOAT64) AS fare_amount,
        CAST(extra AS FLOAT64) AS extra,
        CAST(mta_tax AS FLOAT64) AS mta_tax,
        CAST(tip_amount AS FLOAT64) AS tip_amount,
        CAST(tolls_amount AS FLOAT64) AS tolls_amount,
        CAST(improvement_surcharge AS FLOAT64) AS improvement_surcharge,
        CAST(total_amount AS FLOAT64) AS total_amount,
        CAST(congestion_surcharge AS FLOAT64) AS congestion_surcharge,
        SHA256( CONCAT(COALESCE(vendor_id,
                ''), COALESCE(tpep_pickup_datetime,
                ''), COALESCE(tpep_pickup_datetime,
                ''), COALESCE(passenger_count,
                ''), COALESCE(trip_distance,
                ''), COALESCE(rate_code_id,
                ''), COALESCE(store_and_fwd_flag,
                ''), COALESCE(pu_location_id,
                ''), COALESCE(do_location_id,
                ''), COALESCE(payment_type_id,
                ''), COALESCE(fare_amount,
                ''), COALESCE(extra,
                ''), COALESCE(mta_tax,
                ''), COALESCE(tip_amount,
                ''), COALESCE(tolls_amount,
                ''), COALESCE(improvement_surcharge,
                ''), COALESCE(total_amount,
                ''), COALESCE(congestion_surcharge,
                '') ) ) AS row_hash 
    FROM `spheric-gecko-346716.nytaxi.trip_stage`

)

select *
from source_data