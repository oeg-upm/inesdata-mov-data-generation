import sys
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd

from inesdata_mov_datasets.sources.emt.create.calendar.create import create_calendar_emt
from inesdata_mov_datasets.sources.emt.create.eta.create import create_eta_emt
from inesdata_mov_datasets.sources.emt.create.line_detail.create import create_line_detail_emt
from inesdata_mov_datasets.utils import read_settings


def join_calendar_line_datasets(calendar_df: pd.DataFrame, line_df: pd.DataFrame) -> pd.DataFrame:
    """Join EMT calendar and line_detail datasets.

    Args:
        calendar_df (pd.DataFrame): calendar dataset
        line_df (pd.DataFrame): line_detail dataset

    Returns:
        pd.DataFrame: joined dataset
    """
    try:
        calendar_line_df = line_df.merge(calendar_df, on=["date", 'dayType'])
        calendar_line_df.drop(columns="datetime_y", inplace=True)
        calendar_line_df.rename(columns={"datetime_x": "datetime"}, inplace=True)
        return calendar_line_df
    except Exception as e:
        print(e)
        traceback.print_exc()
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
        calendar_line_df["line"] = calendar_line_df["line"].apply(str)
        df = eta_df.merge(calendar_line_df, on=["date", "line"], how="left")
        df.drop(columns="datetime_y", inplace=True)
        df.rename(columns={"datetime_x": "datetime"}, inplace=True)
        print(df)
        return df
    except Exception as e:
        print(e)
        traceback.print_exc()
        return pd.DataFrame([])


def create_emt(storage_path: str, date: str):
    """Create and export joined dataset from all EMT endpoints.

    Args:
        storage_path (str): local path to store resulting df
        date (str): a date formatted in YYYY/MM/DD
    """
    start = datetime.now()
    try:
        calendar_df = create_calendar_emt(date)
        line_detail_df = create_line_detail_emt(date)
        eta_df = create_eta_emt(date)
        calendar_line_df = join_calendar_line_datasets(calendar_df, line_detail_df)
        df = join_eta_dataset(calendar_line_df, eta_df)

        # sort values
        df = df.sort_values(by=["datetime", "bus", "line", "stop"])

        # export final df
        Path(storage_path + f"/processed/emt/{date}").mkdir(parents=True, exist_ok=True)
        processed_storage_path = storage_path + f"/processed/emt/{date}"
        df.to_csv(processed_storage_path + "/emt_processed.csv")
        print(df.shape)
    except Exception as e:
        print(e)
        traceback.print_exc()

    end = datetime.now()
    print(end - start)


if __name__ == "__main__":
    start = datetime.now()

    settings = read_settings(path="/home/code/inesdata-mov/data-generation/config_dev.yaml")
    # Download day's raw data from minio
    date = sys.argv[1] if len(sys.argv) > 1 else datetime.today().strftime("%Y/%m/%d")
    # date = datetime.strptime('2024/02/09', '%Y/%m/%d').strftime('%Y/%m/%d')
    print(f"Generating EMT dataset for date: {date}")

    storage_path = settings.storage.config.local.path
    create_emt(storage_path, date)

    end = datetime.now()
    print(end - start)
