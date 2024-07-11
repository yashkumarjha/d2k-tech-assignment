# Data Extraction

# Task: Automate the downloading of the dataset from the Year 2019.

import os
import requests
from retrying import retry

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
TARGET_DIR = "/yellow_tripdata_2024-01.parquet"

os.makedirs(TARGET_DIR, exist_ok=True)

@retry(stop_max_attempt_number=5, wait_fixed=2000)
def download_file(url, target_path):
    response = requests.get(url)
    response.raise_for_status()  
    with open(target_path, 'wb') as f:
        f.write(response.content)
    print(f"Downloaded {url} to {target_path}")

for month in range(1, 13):
    file_url = BASE_URL.format(month)
    file_path = os.path.join(TARGET_DIR, f"yellow_tripdata_2019-{month:02d}.csv")
    try:
        download_file(file_url, file_path)
    except requests.RequestException as e:
        print(f"Failed to download {file_url}: {e}")

print("The data extraction is done successfully.")

# Data Processing

# Task: Clean and transform the data using Python and Pandas.

# Requirements:

# Remove any trips that have missing or corrupt data.
# Derive new columns such as trip duration and average speed.
# Aggregate data to calculate total trips and average fare per day.

# Steps for Data Processing :-
# 1. Clean the data : Remove the trips with missing or corrupt the data. And then ensure that the necessary columns are present and contain valid data.
# 2. Transform the data : Derive new columns such as trip duration and average speed.
# 3. Aggregate the data : Calculate the total trips and average fare per day.

import pandas as pd
import os

TARGET_DIR = "./nyc_taxi_data_2019"
PROCESSED_DIR = "./processed_data"

os.makedirs(PROCESSED_DIR, exist_ok=True)

def clean_and_transform(file_path):
    df = pd.read_csv(file_path)

    # Remove trips with missing or corrupt data
    df.dropna(inplace=True)
    df = df[(df['tpep_pickup_datetime'] != 0) & (df['tpep_dropoff_datetime'] != 0)]
    df = df[df['trip_distance'] > 0]
    df = df[df['fare_amount'] > 0]

    # Convert datetime columns to datetime objects
    df['pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # Derive new columns
    df['trip_duration'] = (df['dropoff_datetime'] - df['pickup_datetime']).dt.total_seconds() / 60
    df['average_speed'] = df['trip_distance'] / (df['trip_duration'] / 60)

    # Aggregate data
    df['date'] = df['pickup_datetime'].dt.date
    aggregated_df = df.groupby('date').agg(
        total_trips=('trip_duration', 'count'),
        average_fare=('fare_amount', 'mean')
    ).reset_index()

    # Save the cleaned and aggregated data
    processed_file_path = os.path.join(PROCESSED_DIR, os.path.basename(file_path))
    aggregated_file_path = os.path.join(PROCESSED_DIR, f"aggregated_{os.path.basename(file_path)}")
    df.to_csv(processed_file_path, index=False)
    aggregated_df.to_csv(aggregated_file_path, index=False)

    print(f"Processed {file_path}")

for file_name in os.listdir(TARGET_DIR):
    if file_name.endswith('.csv'):
        clean_and_transform(os.path.join(TARGET_DIR, file_name))

print("All files processed.")
