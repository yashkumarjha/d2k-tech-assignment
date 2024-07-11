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
