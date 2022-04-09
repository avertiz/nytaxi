# nytaxi
This project is meant to demonstrate a full ELT pipeline using Google Cloud Platform. The data that will be used is the NY yellow taxi trip records: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page.

Services/Technology that will be used:
- Google Cloud Storage to store raw data
- Google BigQuery as the data warehouse
- Google Cloud Composer to orchestrate/schedule pipeline

## Prerequisites
- A Google Cloud Platform Account

## CloudStorage
1. Create a storage bucket. I named mine nytaxi_data_raw
    - When choosing a location, it is important to remain consistent across all  GCP services within the project. I chose us-central1 (Iowa) 

## BigQuery
1. Create a dataset within the project. I named my dataset nytaxi 

## CloudComposer