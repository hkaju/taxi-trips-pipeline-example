#!/usr/bin/env python3

import os.path
import zipfile
from pathlib import Path
from datetime import datetime
from urllib.request import urlretrieve

# Script configuration
data_path = Path(os.environ.get("DATA_PATH", os.path.dirname(__file__)))

downloaded_data_path = data_path / "downloaded"
downloaded_data_path.mkdir(exist_ok=True, parents=True)
output_data_path = data_path / "out"
output_data_path.mkdir(exist_ok=True, parents=True)

data_url = (
    "https://www.kaggle.com/api/v1/datasets/download/adelanseur/taxi-trips-chicago-2024"
)
zipped_data_path = downloaded_data_path / "taxi-trips-chicago-2024.zip"
unzipped_data_path = downloaded_data_path / "Taxi_Trips_-_2024_20240408.csv"


def download_data():
    if zipped_data_path.exists():
        return

    print(f"Downloading {data_url} to {zipped_data_path}")
    urlretrieve(data_url, zipped_data_path)


def unzip_data():
    if (unzipped_data_path).exists():
        return

    print(f"Unpacking {zipped_data_path} to {unzipped_data_path}")
    with zipfile.ZipFile(zipped_data_path, "r") as zip_ref:
        zip_ref.extractall(unzipped_data_path.parent)


def split_data():
    print(f"Splitting {unzipped_data_path} to {output_data_path}")
    batches = {}
    # Keep track of the CSV header that we need to add to individual batches
    csv_header = None
    with unzipped_data_path.open() as datafile:
        while line := datafile.readline():
            # Capture the first line as the header
            if not csv_header:
                csv_header = line
                continue

            datetime_string = line.strip().split(",")[2]
            datetime_object = datetime.strptime(datetime_string, "%m/%d/%Y %I:%M:%S %p")

            batch_filename = f"trips-{datetime_object.isocalendar().year}-week-{datetime_object.isocalendar().week}.csv"
            if batch_filename not in batches:
                batches[batch_filename] = [csv_header]
            batches[batch_filename].append(line)

    for batch_filename in batches:
        batch_path = output_data_path / batch_filename

        with batch_path.open("w") as batch_file:
            batch_file.writelines(batches[batch_filename])
        print(f"Saved {batch_path}")


if __name__ == "__main__":
    download_data()
    unzip_data()
    split_data()
