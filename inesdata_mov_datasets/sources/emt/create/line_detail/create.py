import asyncio
import json
import logging
import os
import tempfile
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd

from inesdata_mov_datasets.settings import Settings
from inesdata_mov_datasets.utils import download_objs

# Logger
logging.basicConfig(
    filename="app.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def download_line_detail(
    bucket: str,
    prefix: str,
    output_path: str,
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
):
    """Download from minIO a day's raw data of EMT's line_detail endpoint.

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
        content (dict): line_detail info from a file

    Returns:
        pd.DataFrame: day's pandas dataframe from a single file downloaded from MinIO
    """
    day_df = pd.DataFrame([])
    try:
        if len(content["data"]) != 0:
            day_df = pd.DataFrame(content["data"][0]["timeTable"])
            day_df = day_df.join(pd.json_normalize(day_df["Direction1"]))
            day_df.drop(columns=["Direction1", "Direction2"], inplace=True)
            if not day_df.empty:
                day_df["datetime"] = pd.to_datetime(content["datetime"])
                # Add date col
                day_df["date"] = pd.to_datetime(day_df["datetime"].dt.date)
                # Add line col
                day_df["line"] = content["data"][0]["line"]
                # rename day type col to be the same as in calendar
                day_df.rename(columns={"idDayType": "dayType"}, inplace=True)
                # Get selected cols
                day_df = day_df[
                    [
                        "dayType",
                        "StartTime",
                        "StopTime",
                        "MinimunFrequency",
                        "MaximumFrequency",
                        "datetime",
                        "date",
                        "line",
                    ]
                ].drop_duplicates()
    except Exception as e:
        logging.error(e)
        logging.error(traceback.format_exc())
    return day_df


def generate_day_df(storage_path: str, date: str) -> pd.DataFrame:
    """Generate a day's pandas dataframe from a whole day's files downloaded from MinIO.

    Args:
        storage_path (str): local path to store resulting df
        date (str): a date formatted in YYYY/MM/DD

    Returns:
        pd.DataFrame: day's pandas dataframe
    """
    dfs = []
    raw_storage_dir = Path(storage_path) / Path("raw") / "emt" / date / "line_detail"
    raw_storage_dir.mkdir(parents=True, exist_ok=True)
    files = os.listdir(raw_storage_dir)
    logging.info(f"files count: {len(files)}")
    for file in files:
        logging.info(f"generating df from {file}")
        filename = raw_storage_dir / file
        with open(filename, "r") as f:
            content = json.load(f)
        df = generate_df_from_file(content)
        dfs.append(df)

    if len(dfs) > 0:
        final_df = pd.concat(dfs)
        # sort values
        final_df = final_df.sort_values(by=["datetime", "line"])
        # export final df
        # processed_storage_dir = Path(storage_path) / Path("processed") / "emt" / date
        # Path(processed_storage_dir).mkdir(parents=True, exist_ok=True)
        # final_df.to_csv(processed_storage_dir / "line_detail_processed.csv", index=None)
        print(f"Created EMT line df {final_df.shape}")
        return final_df
    else:
        return pd.DataFrame([])


def create_line_detail_emt(settings: Settings, date: str) -> pd.DataFrame:
    """Create dataset from EMT line_detail endpoint.

    Args:
        settings (Settings): project settings
        date (str): a date formatted in YYYY/MM/DD

    Returns:
        pd.DataFrame: df from EMT line_detail endpoint
    """
    # Download day's raw data from minio
    logging.info(f"Generating EMT line_detail dataset for date: {date}")

    with tempfile.TemporaryDirectory() as tmpdirname:
        logging.debug(tmpdirname)
        start = datetime.now()
        storage_config = settings.storage.config
        storage_path = storage_config.local.path  # tmpdirname
        if settings.storage.default != "local":
            download_line_detail(
                bucket=storage_config.minio.bucket,
                prefix=f"raw/emt/{date}/line_detail/",
                output_path=storage_path,
                endpoint_url=storage_config.minio.endpoint,
                aws_access_key_id=storage_config.minio.access_key,
                aws_secret_access_key=storage_config.minio.secret_key,
            )
        df = generate_day_df(storage_path=storage_path, date=date)

        end = datetime.now()
        logging.info(end - start)
        return df
