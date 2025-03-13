#!/usr/bin/env python3

import os.path
import zipfile
from pathlib import Path
from datetime import datetime
from urllib.request import urlretrieve

# Script configuration
data_path = Path(os.path.dirname(__file__))
data_url = (
    "https://www.kaggle.com/api/v1/datasets/download/adelanseur/taxi-trips-chicago-2024"
)
zipped_filename = "taxi-trips-chicago-2024.zip"
data_filename = "Taxi_Trips_-_2024_20240408.csv"
weekly_data_path = data_path / "weekly"
logging_prefix = "      ↳"


def download_data():
    print("[1/3] Download data from Kaggle")

    if (data_path / zipped_filename).exists():
        print(f"{logging_prefix} {zipped_filename} found, skipping")
        return

    def show_progress(block_num, block_size, total_size):
        print(
            f"{logging_prefix} Progress: {round(block_num * block_size / total_size * 100)}%",
            end="\r",
        )

    urlretrieve(data_url, data_path / zipped_filename, show_progress)
    print(f"{logging_prefix} Progress: Done!")


def unzip_data():
    print("[2/3] Unpack data")

    if (data_path / data_filename).exists():
        print(f"{logging_prefix} {data_filename} found, skipping")
        return

    print(f"{logging_prefix} Upacking {zipped_filename}...")
    with zipfile.ZipFile(data_path / zipped_filename, "r") as zip_ref:
        zip_ref.extractall(data_path)


def split_data():
    print("[3/3] Split data into weekly batches")

    if weekly_data_path.exists():
        print(f"{logging_prefix} {weekly_data_path} found, skipping")
        return

    weekly_data_path.mkdir(parents=True, exist_ok=True)

    # Keep track of the CSV header that we need to add to individual batches
    csv_header = None
    with (data_path / data_filename).open() as datafile:
        while line := datafile.readline():
            # Capture the first line as the header
            if not csv_header:
                csv_header = line
                continue

            datetime_string = line.strip().split(",")[2]
            datetime_object = datetime.strptime(datetime_string, "%m/%d/%Y %I:%M:%S %p")

            batch_filename = f"trips-{datetime_object.isocalendar().year}-week-{datetime_object.isocalendar().week}.csv"
            batch_path = weekly_data_path / batch_filename

            new_batch = not batch_path.exists()
            with batch_path.open("a") as batch_file:
                if new_batch:
                    print(f"      ↳ {batch_path}")
                    batch_file.write(csv_header)
                batch_file.write(line)


if __name__ == "__main__":
    download_data()
    unzip_data()
    split_data()
