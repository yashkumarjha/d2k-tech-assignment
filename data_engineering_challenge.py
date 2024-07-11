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


# Data Loading
# Objective:
# Load the processed New York Taxi Trip data into an SQLite database. The task involves designing a schema suitable for querying trip metrics and using SQL to load data into the database efficiently.

# Steps for Data Loading
# Design the Database Schema:

# Define a schema that includes tables for the cleaned trip data and aggregated daily metrics.
# Load Data into SQLite Database:

# Use Python and SQLAlchemy to load the data efficiently.


CREATE TABLE trips_2019 (
    pickup_datetime TEXT,
    dropoff_datetime TEXT,
    trip_duration REAL,
    average_speed REAL,
    trip_distance REAL,
    fare_amount REAL,
    date TEXT
);

CREATE TABLE daily_metrics_2019 (
    date TEXT,
    total_trips INTEGER,
    average_fare REAL
);

# Creating the script 'load_data.py'
import pandas as pd
from sqlalchemy import create_engine
import os

DATABASE_URI = 'sqlite:///nyc_taxi_data.db'
engine = create_engine(DATABASE_URI)

PROCESSED_DIR = "./processed_data"

def load_file_to_db(file_path, table_name):
    df = pd.read_csv(file_path)
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"Loaded {file_path} into {table_name}")

def create_tables():
    with engine.connect() as conn:
        conn.execute('''
        CREATE TABLE IF NOT EXISTS trips_2019 (
            pickup_datetime TEXT,
            dropoff_datetime TEXT,
            trip_duration REAL,
            average_speed REAL,
            trip_distance REAL,
            fare_amount REAL,
            date TEXT
        )
        ''')
        conn.execute('''
        CREATE TABLE IF NOT EXISTS daily_metrics_2019 (
            date TEXT,
            total_trips INTEGER,
            average_fare REAL
        )
        ''')

# Create the tables
create_tables()

# Load the processed data into the trips_2019 table
for file_name in os.listdir(PROCESSED_DIR):
    if file_name.startswith('yellow_tripdata') and file_name.endswith('.csv'):
        load_file_to_db(os.path.join(PROCESSED_DIR, file_name), 'trips_2019')

# Load the aggregated data into the daily_metrics_2019 table
for file_name in os.listdir(PROCESSED_DIR):
    if file_name.startswith('aggregated_yellow_tripdata') and file_name.endswith('.csv'):
        load_file_to_db(os.path.join(PROCESSED_DIR, file_name), 'daily_metrics_2019')

print("All files loaded into the database.")



# Data Analysis and Reporting
# Objective:
# Generate insights and reports from the SQLite database by developing SQL queries and creating visualizations to represent the findings.

# 1. Peak hours for taxi usage :-
SELECT strftime('%H', pickup_datetime) AS hour, COUNT(*) AS total_trips
FROM trips_2019
GROUP BY hour
ORDER BY total_trips DESC;

# 2. Effect of passenger count on trip fare :-
SELECT passenger_count, AVG(fare_amount) AS average_fare
FROM trips_2019
GROUP BY passenger_count
ORDER BY passenger_count;

# 3. Trends in usage over the year :-
SELECT strftime('%Y-%m', pickup_datetime) AS month, COUNT(*) AS total_trips
FROM trips_2019
GROUP BY month
ORDER BY month;

# Creating the script 'analyze_and_visualize.py'
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

DATABASE_URI = 'sqlite:///nyc_taxi_data.db'
engine = create_engine(DATABASE_URI)

def plot_peak_hours():
    query = '''
    SELECT strftime('%H', pickup_datetime) AS hour, COUNT(*) AS total_trips
    FROM trips_2019
    GROUP BY hour
    ORDER BY total_trips DESC;
    '''
    df = pd.read_sql(query, engine)
    plt.figure(figsize=(10, 6))
    plt.bar(df['hour'], df['total_trips'])
    plt.title('Peak Hours for Taxi Usage')
    plt.xlabel('Hour of the Day')
    plt.ylabel('Total Trips')
    plt.show()

def plot_passenger_count_vs_fare():
    query = '''
    SELECT passenger_count, AVG(fare_amount) AS average_fare
    FROM trips_2019
    GROUP BY passenger_count
    ORDER BY passenger_count;
    '''
    df = pd.read_sql(query, engine)
    plt.figure(figsize=(10, 6))
    plt.bar(df['passenger_count'], df['average_fare'])
    plt.title('Effect of Passenger Count on Trip Fare')
    plt.xlabel('Passenger Count')
    plt.ylabel('Average Fare ($)')
    plt.show()

def plot_trends_over_year():
    query = '''
    SELECT strftime('%Y-%m', pickup_datetime) AS month, COUNT(*) AS total_trips
    FROM trips_2019
    GROUP BY month
    ORDER BY month;
    '''
    df = pd.read_sql(query, engine)
    plt.figure(figsize=(10, 6))
    plt.plot(df['month'], df['total_trips'], marker='o')
    plt.title('Trends in Taxi Usage Over the Year')
    plt.xlabel('Month')
    plt.ylabel('Total Trips')
    plt.xticks(rotation=45)
    plt.show()

# Plot visualizations
plot_peak_hours()
plot_passenger_count_vs_fare()
plot_trends_over_year()
