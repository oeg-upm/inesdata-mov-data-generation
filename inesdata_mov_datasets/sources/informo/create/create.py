import asyncio
import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd

from inesdata_mov_datasets.utils import download_objs, read_settings


def download_informo(
    bucket: str,
    prefix: str,
    output_path: str,
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
):
    """Download from minIO a day's raw data of Informo endpoint.

    Args:
        bucket (str): bucket name
        prefix (str): path to raw data directory from minio
        output_path (str): local path to store output from minio
        endpoint_url (str): url of minio bucket
        aws_access_key_id (str): minio user
        aws_secret_access_key (str): minio password
    """
    loop = asyncio.new_event_loop()
    loop.run_until_complete(
        download_objs(
            bucket, prefix, output_path, endpoint_url, aws_access_key_id, aws_secret_access_key
        )
    )


def generate_df_from_file(content: dict) -> pd.DataFrame:
    """Generate a day's pandas dataframe from a single file downloaded from MinIO.

    Args:
        content (dict): traffic info from a file

    Returns:
        pd.DataFrame: day's pandas dataframe from a single file downloaded from MinIO
    """
    day_df = pd.DataFrame([])
    try:
        if len(content) != 0:
            day_df = pd.DataFrame(content["pm"])
            if not day_df.empty:
                day_df["datetime"] = pd.to_datetime(content["fecha_hora"], dayfirst=True)
                # Add date col
                day_df["date"] = pd.to_datetime(day_df["datetime"].dt.date)
    except Exception as e:
        print(e)
        traceback.print_exc()
    return day_df


def generate_day_df(storage_path: str, date: str):
    """Generate a day's pandas dataframe from a whole day's files downloaded from MinIO.

    Args:
        storage_path (str): local path to store resulting df
        date (str): a date formatted in YYYY/MM/DD
    """
    dfs = []
    Path(storage_path + f"/raw/informo/{date}").mkdir(parents=True, exist_ok=True)
    files = os.listdir(storage_path + f"/raw/informo/{date}/")
    print(f"files count: {len(files)}")
    for file in files:
        print(f"generating df from {file}")
        with open(storage_path + f"/raw/informo/{date}/" + file, "r") as f:
            content = json.load(f)
        df = generate_df_from_file(content["pms"])
        dfs.append(df)

    final_df = pd.concat(dfs)
    # sort values
    final_df = final_df.sort_values(by="datetime")
    # export final df
    Path(storage_path + f"/processed/informo/{date}").mkdir(parents=True, exist_ok=True)
    processed_storage_path = storage_path + f"/processed/informo/{date}"
    final_df.to_csv(processed_storage_path + "/informo_processed.csv")
    print(final_df.shape)


if __name__ == "__main__":
    start = datetime.now()

    settings = read_settings(path="/home/code/inesdata-mov/data-generation/config_dev.yaml")
    # Download day's raw data from minio
    date = sys.argv[1] if len(sys.argv) > 1 else datetime.today().strftime("%Y/%m/%d")
    # date = datetime.strptime('2024/02/09', '%Y/%m/%d').strftime('%Y/%m/%d')
    print(f"Generating EMT dataset for date: {date}")

    storage_config = settings.storage.config
    download_informo(
        bucket=storage_config.minio.bucket,
        prefix=f"raw/informo/{date}/",
        output_path=storage_config.local.path,
        endpoint_url=storage_config.minio.endpoint,
        aws_access_key_id=storage_config.minio.access_key,
        aws_secret_access_key=storage_config.minio.secret_key,
    )
    generate_day_df(storage_path=storage_config.local.path, date=date)

    end = datetime.now()
    print(end - start)
