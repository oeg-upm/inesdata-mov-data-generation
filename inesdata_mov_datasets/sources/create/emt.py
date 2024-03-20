import json
import os
import tempfile
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd
from loguru import logger

from inesdata_mov_datasets.handlers.logger import import_create_logger
from inesdata_mov_datasets.settings import Settings
from inesdata_mov_datasets.utils import async_download


def generate_calendar_df_from_file(content: dict) -> pd.DataFrame:
    """Generate a day's pandas dataframe from a single file downloaded from MinIO.

    Args:
        content (dict): calendar info from a file

    Returns:
        pd.DataFrame: day's pandas dataframe from a single file downloaded from MinIO
    """
    day_df = pd.DataFrame([])
    try:
        if len(content["data"]) != 0:
            day_df = pd.DataFrame(content["data"])
            if not day_df.empty:
                day_df["datetime"] = pd.to_datetime(content["datetime"])
                # Add date col
                day_df["date"] = pd.to_datetime(day_df["date"], dayfirst=True)
                # Get selected cols
                # day_df = day_df[['date', 'dayType']]
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
    return day_df


def generate_calendar_day_df(storage_path: str, date: str) -> pd.DataFrame:
    """Generate a day's pandas dataframe from a whole day's files downloaded from MinIO.

    Args:
        storage_path (str): local path to store resulting df
        date (str): a date formatted in YYYY/MM/DD

    Returns:
        pd.DataFrame: day's pandas dataframe
    """
    dfs = []
    raw_storage_dir = Path(storage_path) / Path("raw") / "emt" / date / "calendar"
    raw_storage_dir.mkdir(parents=True, exist_ok=True)
    files = os.listdir(raw_storage_dir)
    logger.info(f"#{len(files)} files from EMT calendar endpoint")
    for file in files:
        filename = raw_storage_dir / file
        with open(filename, "r") as f:
            content = json.load(f)
        df = generate_calendar_df_from_file(content[0])
        dfs.append(df)

    if len(dfs) > 0:
        final_df = pd.concat(dfs)
        # sort values
        final_df = final_df.sort_values(by=["datetime"])
        # export final df
        # processed_storage_dir = Path(storage_path) / Path("processed") / "emt" / date
        # Path(processed_storage_dir).mkdir(parents=True, exist_ok=True)
        # final_df.to_csv(processed_storage_dir / "calendar_processed.csv", index=None)
        logger.info(f"Created EMT calendar df of shape {final_df.shape}")
        return final_df
    else:
        return pd.DataFrame([])


def create_calendar_emt(settings: Settings, date: str) -> pd.DataFrame:
    """Create dataset from EMT calendar endpoint.

    Args:
        settings (Settings): project settings
        date (str): a date formatted in YYYY/MM/DD

    Returns:
        pd.DataFrame: df from EMT calendar endpoint
    """
    # Download day's raw data from minio
    logger.info(f"Creating EMT calendar dataset for date: {date}")

    with tempfile.TemporaryDirectory() as tmpdirname:
        start = datetime.now()
        storage_config = settings.storage.config
        storage_path = storage_config.local.path  # tmpdirname
        if settings.storage.default != "local":
            async_download(
                bucket=storage_config.minio.bucket,
                prefix=f"raw/emt/{date}/calendar/",
                output_path=storage_path,
                endpoint_url=storage_config.minio.endpoint,
                aws_access_key_id=storage_config.minio.access_key,
                aws_secret_access_key=storage_config.minio.secret_key,
            )
        df = generate_calendar_day_df(storage_path=storage_path, date=date)

        end = datetime.now()
        logger.debug(f"Time duration of EMT calendar dataset creation {end - start}")
        return df


def generate_line_df_from_file(content: dict) -> pd.DataFrame:
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
        logger.error(e)
        logger.error(traceback.format_exc())
    return day_df


def generate_line_day_df(storage_path: str, date: str) -> pd.DataFrame:
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
    logger.info(f"#{len(files)} files from EMT line_detail endpoint")
    for file in files:
        filename = raw_storage_dir / file
        with open(filename, "r") as f:
            content = json.load(f)
        df = generate_line_df_from_file(content)
        dfs.append(df)

    if len(dfs) > 0:
        final_df = pd.concat(dfs)
        # sort values
        final_df = final_df.sort_values(by=["datetime", "line"])
        # export final df
        # processed_storage_dir = Path(storage_path) / Path("processed") / "emt" / date
        # Path(processed_storage_dir).mkdir(parents=True, exist_ok=True)
        # final_df.to_csv(processed_storage_dir / "line_detail_processed.csv", index=None)
        logger.info(f"Created EMT line df of shape {final_df.shape}")
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
    logger.info(f"Creating EMT line_detail dataset for date: {date}")

    with tempfile.TemporaryDirectory() as tmpdirname:
        start = datetime.now()
        storage_config = settings.storage.config
        storage_path = storage_config.local.path  # tmpdirname
        if settings.storage.default != "local":
            async_download(
                bucket=storage_config.minio.bucket,
                prefix=f"raw/emt/{date}/line_detail/",
                output_path=storage_path,
                endpoint_url=storage_config.minio.endpoint,
                aws_access_key_id=storage_config.minio.access_key,
                aws_secret_access_key=storage_config.minio.secret_key,
            )
        df = generate_line_day_df(storage_path=storage_path, date=date)

        end = datetime.now()
        logger.debug(f"Time duration of EMT line dataset creation {end - start}")
        return df


def generate_eta_df_from_file(content: dict) -> pd.DataFrame:
    """Generate a day's pandas dataframe from a single file downloaded from MinIO.

    Args:
        content (dict): ETA info from a file

    Returns:
        pd.DataFrame: day's pandas dataframe from a single file downloaded from MinIO
    """
    day_df = pd.DataFrame([])
    try:
        if len(content["data"]) != 0:
            day_df = pd.DataFrame(content["data"][0]["Arrive"])
            if not day_df.empty:
                day_df["datetime"] = pd.to_datetime(content["datetime"])
                # Add date col
                day_df["date"] = pd.to_datetime(day_df["datetime"].dt.date)
                # Get selected cols
                # day_df = day_df[[ "line","stop","bus","date","datetime","geometry","DistanceBus","estimateArrive"]]
                # Add lat lon cols
                day_df["positionBus"] = day_df["geometry"].apply(lambda row: row["coordinates"])
                day_df["positionBusLon"] = day_df["positionBus"].apply(pd.Series)[0]
                day_df["positionBusLat"] = day_df["positionBus"].apply(pd.Series)[1]
                day_df = day_df.drop(columns=["geometry", "positionBus"])
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
    return day_df


def generate_eta_day_df(storage_path: str, date: str) -> pd.DataFrame:
    """Generate a day's pandas dataframe from a whole day's files downloaded from MinIO.

    Args:
        storage_path (str): local path to store resulting df
        date (str): a date formatted in YYYY/MM/DD

    Returns:
        pd.DataFrame: day's pandas dataframe
    """
    dfs = []
    raw_storage_dir = Path(storage_path) / Path("raw") / "emt" / date / "eta"
    raw_storage_dir.mkdir(parents=True, exist_ok=True)
    files = os.listdir(raw_storage_dir)
    logger.info(f"#{len(files)} files from EMT ETA endpoint")
    for file in files:
        filename = raw_storage_dir / file
        with open(filename, "r") as f:
            content = json.load(f)
        df = generate_eta_df_from_file(content)
        dfs.append(df)

    if len(dfs) > 0:
        final_df = pd.concat(dfs)
        # sort values
        final_df = final_df.sort_values(by=["datetime", "bus", "line", "stop"])
        # export final df
        # processed_storage_dir = Path(storage_path) / Path("processed") / "emt" / date
        # Path(processed_storage_dir).mkdir(parents=True, exist_ok=True)
        # final_df.to_csv(processed_storage_dir / "eta_processed.csv", index=None)
        logger.info(f"Created EMT ETA df of shape {final_df.shape}")
        return final_df
    else:
        return pd.DataFrame([])


def create_eta_emt(settings: Settings, date: str) -> pd.DataFrame:
    """Create dataset from EMT ETA endpoint.

    Args:
        settings (Settings): project settings
        date (str): a date formatted in YYYY/MM/DD

    Returns:
        pd.DataFrame: df from EMT ETA endpoint
    """
    # Download day's raw data from minio
    logger.info(f"Creating EMT ETA dataset for date: {date}")

    with tempfile.TemporaryDirectory() as tmpdirname:
        start = datetime.now()
        storage_config = settings.storage.config
        storage_path = storage_config.local.path  # tmpdirname
        if settings.storage.default != "local":
            async_download(
                bucket=storage_config.minio.bucket,
                prefix=f"raw/emt/{date}/eta/",
                output_path=storage_path,
                endpoint_url=storage_config.minio.endpoint,
                aws_access_key_id=storage_config.minio.access_key,
                aws_secret_access_key=storage_config.minio.secret_key,
            )
        df = generate_eta_day_df(storage_path=storage_path, date=date)

        end = datetime.now()
        logger.debug(f"Time duration of EMT ETA dataset creation {end - start}")
        return df


def join_calendar_line_datasets(calendar_df: pd.DataFrame, line_df: pd.DataFrame) -> pd.DataFrame:
    """Join EMT calendar and line_detail datasets.

    Args:
        calendar_df (pd.DataFrame): calendar dataset
        line_df (pd.DataFrame): line_detail dataset

    Returns:
        pd.DataFrame: joined dataset
    """
    try:
        calendar_line_df = line_df.merge(calendar_df, on=["date", "dayType"])
        calendar_line_df.drop(columns="datetime_y", inplace=True)
        calendar_line_df.rename(columns={"datetime_x": "datetime"}, inplace=True)
        return calendar_line_df
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        return pd.DataFrame([])


def join_eta_dataset(calendar_line_df: pd.DataFrame, eta_df: pd.DataFrame) -> pd.DataFrame:
    """Join EMT calendar+line_detail (previously joined dataset) with ETA dataset.

    Args:
        calendar_line_df (pd.DataFrame): calendar and line_detail previousy joined dataset
        eta_df (pd.DataFrame): ETA dataset

    Returns:
        pd.DataFrame: joined dataset
    """
    try:
        # fix lines that begin with zeros
        calendar_line_df["line"] = calendar_line_df["line"].apply(str).str.lstrip("0")
        eta_df["line"] = eta_df["line"].apply(str).str.lstrip("0")
        # left join on line col
        df = eta_df.merge(calendar_line_df, on="line", how="left")
        df.drop(columns=["datetime_y", "date_y"], inplace=True)
        df.rename(columns={"datetime_x": "datetime", "date_x": "date"}, inplace=True)
        return df
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        return pd.DataFrame([])


def create_emt(settings: Settings, date: str):
    """Create and export joined dataset from all EMT endpoints.

    Args:
        settings (Settings): project settings
        date (str): a date formatted in YYYY/MM/DD
    """
    # Logger
    import_create_logger(settings, "EMT")
    start = datetime.now()
    storage_path = settings.storage.config.local.path
    logger.info(f"Creating EMT dataset for date: {date}")
    try:
        calendar_df = create_calendar_emt(settings, date)
        line_detail_df = create_line_detail_emt(settings, date)
        eta_df = create_eta_emt(settings, date)
        if not calendar_df.empty and not line_detail_df.empty and not eta_df.empty:
            calendar_line_df = join_calendar_line_datasets(calendar_df, line_detail_df)
            df = join_eta_dataset(calendar_line_df, eta_df)

            # sort values
            df = df.sort_values(by=["datetime", "bus", "line", "stop"])

            # export final df
            Path(storage_path + f"/processed/emt/{date}").mkdir(parents=True, exist_ok=True)
            date_formatted = date.replace("/", "")
            processed_storage_path = storage_path + f"/processed/emt/{date}"
            df.to_csv(processed_storage_path + f"/emt_{date_formatted}.csv", index=None)
            logger.info(f"Created EMT df of shape {df.shape}")
        else:
            logger.debug("There is no data to create")
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())

    end = datetime.now()
    logger.debug(f"Time duration of EMT dataset creation {end - start}")
