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


def download_aemet(
    bucket: str,
    prefix: str,
    output_path: str,
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
):
    """Download from minIO a day's raw data of AEMET endpoint.

    Args:
        bucket (str): bucket name
        prefix (str): path to raw data directory from minio
        output_path (str): local path to store output from minio
        endpoint_url (str): url of minio bucket
        aws_access_key_id (str): minio user
        aws_secret_access_key (str): minio password
    """
    try:
        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            download_objs(
                bucket, prefix, output_path, endpoint_url, aws_access_key_id, aws_secret_access_key
            )
        )
    except Exception as e:
        logger.error(e)


def generate_df_from_file(content: dict, date: str) -> pd.DataFrame:
    """Generate a day's pandas dataframe from a single file downloaded from MinIO.

    Args:
        content (dict): aemet info from a file
        date (str): a date formatted in YYYY/MM/DD

    Returns:
        pd.DataFrame: day's pandas dataframe from a single file downloaded from MinIO
    """
    day_df = pd.DataFrame([])
    day_df_final = pd.DataFrame([])
    try:
        if len(content[0]["prediccion"]) != 0:
            day_df = pd.DataFrame(content[0]["prediccion"]["dia"])
            # filter values
            day_df["fecha"] = pd.to_datetime(day_df["fecha"])
            day_df = day_df[day_df["fecha"].dt.strftime("%Y/%m/%d") == date]
            if not day_df.empty:
                # reorganize cols
                cols_to_ignore = ["fecha", "orto", "ocaso"]
                # Add date cols
                # day_df_final["datetime"] = pd.to_datetime(day_df['fecha'])
                # day_df_final["date"] = pd.to_datetime(day_df_final["datetime"].dt.date)
                for col in day_df.columns:
                    if col not in cols_to_ignore:
                        day_df_aux = pd.DataFrame(day_df[col].values[0]).rename(
                            columns={"value": f"{col}_value", "descripcion": f"{col}_descripcion"}
                        )
                        if not day_df_final.empty:
                            day_df_final = day_df_final.merge(
                                day_df_aux, on="periodo", how="outer"
                            )
                        else:
                            day_df_final = day_df_aux
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
    return day_df_final


def generate_day_df(storage_path: str, date: str):
    """Generate a day's pandas dataframe from a whole day's files downloaded from MinIO.

    Args:
        storage_path (str): local path to store resulting df
        date (str): a date formatted in YYYY/MM/DD
    """
    dfs = []
    raw_storage_dir = Path(storage_path) / Path("raw") / "aemet" / date
    raw_storage_dir.mkdir(parents=True, exist_ok=True)
    files = os.listdir(raw_storage_dir)
    logger.info(f"#{len(files)} files from AEMET endpoint")
    for file in files:
        filename = raw_storage_dir / file
        with open(filename, "r") as f:
            content = json.load(f)
        df = generate_df_from_file(content, date)
        dfs.append(df)
    if len(dfs) > 0:
        final_df = pd.concat(dfs)
        if 'periodo' in final_df.columns:
            # fix hour col
            final_df["periodo"] = (
                final_df["periodo"]
                .str.pad(width=4, side="right", fillchar="0")
                .replace(r"(\d{2})(\d+)", r"\1:\2", regex=True)
            )
            # add datetime col
            final_df["datetime"] = pd.to_datetime(date + " " + final_df["periodo"])
            final_df.drop(columns="periodo", inplace=True)
            # sort values
            final_df = final_df.sort_values(by="datetime")
            # export final df
            processed_storage_dir = Path(storage_path) / Path("processed") / "aemet" / date
            date_formatted = date.replace("/", "")
            Path(processed_storage_dir).mkdir(parents=True, exist_ok=True)
            final_df.to_csv(processed_storage_dir / f"aemet_{date_formatted}.csv", index=None)
            logger.info(f"Created AEMET df of shape {final_df.shape}")
        else:
            logger.debug("There is no data to create")
    else:
        logger.debug("There is no data to create")


def create_aemet(settings: Settings, date: str):
    """Create dataset from AEMET endpoint.

    Args:
        settings (Settings): project settings
        date (str): a date formatted in YYYY/MM/DD
    """
    try:
        # Logger
        instantiate_logger(settings, "AEMET", "create")
        # Download day's raw data from minio
        logger.info(f"Creating AEMET dataset for date: {date}")

        with tempfile.TemporaryDirectory() as tmpdirname:
            start = datetime.now()
            storage_config = settings.storage.config
            storage_path = storage_config.local.path  # tmpdirname
            if settings.storage.default != "local":
                download_aemet(
                    bucket=storage_config.minio.bucket,
                    prefix=f"raw/aemet/{date}/",
                    output_path=storage_path,
                    endpoint_url=storage_config.minio.endpoint,
                    aws_access_key_id=storage_config.minio.access_key,
                    aws_secret_access_key=storage_config.minio.secret_key,
                )
            generate_day_df(storage_path=storage_path, date=date)

            end = datetime.now()
            logger.debug(f"Time duration of AEMET dataset creation {end - start}")
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
