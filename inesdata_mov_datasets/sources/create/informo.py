import asyncio
import json
import os
import tempfile
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd
from loguru import logger

from inesdata_mov_datasets.handlers.logger import instantiate_logger
from inesdata_mov_datasets.settings import Settings
from inesdata_mov_datasets.utils import download_objs


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
        logger.error(e)
        logger.error(traceback.format_exc())
    return day_df


def generate_day_df(storage_path: str, date: str):
    """Generate a day's pandas dataframe from a whole day's files downloaded from MinIO.

    Args:
        storage_path (str): local path to store resulting df
        date (str): a date formatted in YYYY/MM/DD
    """
    dfs = []
    raw_storage_dir = Path(storage_path) / Path("raw") / "informo" / date
    raw_storage_dir.mkdir(parents=True, exist_ok=True)
    files = os.listdir(raw_storage_dir)
    logger.info(f"#{len(files)} files from INFORMO endpoint")
    for file in files:
        filename = raw_storage_dir / file
        with open(filename, "r") as f:
            content = json.load(f)
        df = generate_df_from_file(content["pms"])
        dfs.append(df)

    if len(dfs) > 0:
        final_df = pd.concat(dfs)
        # sort values
        final_df = final_df.sort_values(by="datetime")
        # export final df
        processed_storage_dir = Path(storage_path) / Path("processed") / "informo" / date
        date_formatted = date.replace("/", "")
        Path(processed_storage_dir).mkdir(parents=True, exist_ok=True)
        final_df.to_csv(processed_storage_dir / f"informo_{date_formatted}.csv", index=None)
        logger.info(f"Created INFORMO df of shape {final_df.shape}")
    else:
        logger.debug("There is no data to create")


def create_informo(settings: Settings, date: str):
    """Create dataset from Informo endpoint.

    Args:
        settings (Settings): project settings
        date (str): a date formatted in YYYY/MM/DD
    """
    try:
        # Logger
        instantiate_logger(settings, "INFORMO", "create")
        # Download day's raw data from minio
        logger.info(f"Creating INFORMO dataset for date: {date}")

        with tempfile.TemporaryDirectory() as tmpdirname:
            start = datetime.now()
            storage_config = settings.storage.config
            storage_path = storage_config.local.path  # tmpdirname
            if settings.storage.default != "local":
                download_informo(
                    bucket=storage_config.minio.bucket,
                    prefix=f"raw/informo/{date}/",
                    output_path=storage_path,
                    endpoint_url=storage_config.minio.endpoint,
                    aws_access_key_id=storage_config.minio.access_key,
                    aws_secret_access_key=storage_config.minio.secret_key,
                )
            generate_day_df(storage_path=storage_path, date=date)

            end = datetime.now()
            logger.debug(f"Time duration of INFORMO dataset creation {end - start}")
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
