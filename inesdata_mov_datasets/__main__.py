"""Command line interface for inesdata_mov_datasets."""

from datetime import datetime, timedelta
from enum import Enum

import pandas as pd
import typer
import asyncio

from inesdata_mov_datasets.sources.aemet.create.create import create_aemet
from inesdata_mov_datasets.sources.aemet.extract.extract import get_aemet
from inesdata_mov_datasets.sources.emt.create.create import create_emt
from inesdata_mov_datasets.sources.emt.extract.extract import get_emt
from inesdata_mov_datasets.sources.informo.create.create import create_informo
from inesdata_mov_datasets.sources.informo.extract.extract import get_informo
from inesdata_mov_datasets.utils import minio_connection, read_settings

app = typer.Typer(add_completion=False)


class Sources(str, Enum):
    """Data sources public class.

    Args:
        str: name of source (emt, aemet, informo, all)
        Enum: enum object of all sources
    """

    all = "all"
    emt = "emt"
    aemet = "aemet"
    informo = "informo"


@app.command()
def extract(
    config_path: str = typer.Option(default=..., help="Path to configuration yaml file"),
    sources: Sources = typer.Option(
        default=Sources.all.value, help="Possible sources to extract."
    ),
):
    """Extract raw data from the sources configurated."""
    # read settings
    config = read_settings(config_path)

    # read settings
    config = read_settings(config_path)
    
    if config.storage.default == 'minio':
        minio_client = minio_connection(config)
        #EMT
        if sources.emt:
            asyncio.run(get_emt(config, minio_client))
        #Aemet
        if sources.aemet:
            get_aemet(config, minio_client)
        #Informo
        if sources.informo:
            get_informo(config, minio_client)
            
    if config.storage.default == 'local':
        #EMT
        if sources.emt:
            asyncio.run(get_emt(config))
        #Aemet
        if sources.aemet:
            get_aemet(config)
        #Informo
        if sources.informo:
            get_informo(config)
        

    print("Extracted data")


@app.command()
def create(
    config_path: str = typer.Option(
        default="/home/code/inesdata-mov/data-generation/config.yaml",
        help="Path to configuration yaml file",
    ),
    start_date: datetime = typer.Option(
        default=datetime.today().strftime("%Y%m%d"),
        formats=["%Y%m%d"],
        help="Start date in format YYYYMMDD",
    ),
    end_date: datetime = typer.Option(
        default=datetime.today().strftime("%Y%m%d"),
        formats=["%Y%m%d"],
        help="End date in format YYYYMMDD",
    ),
    sources: Sources = typer.Option(
        default=Sources.all.value, help="Possible sources to generate (emt, informo, aemet, all)."
    ),
):
    """Create mobility datasets in a given date range from raw data. Please, run first gather command to get the raw data.

    Execution example: python inesdata_mov_datasets/__main__.py create --config-path=/home/code/inesdata-mov/data-generation/config_dev.yaml --start-date=20240219 --end-date=20240220 --sources=informo
    """
    # read settings
    settings = read_settings(config_path)
    print(f"Create {sources.value} dataset from {start_date} to {end_date}")
    dates = pd.date_range(
        pd.to_datetime(start_date), pd.to_datetime(end_date) - timedelta(days=1), freq="d"
    )
    for date in dates:
        date_formatted = date.strftime("%Y/%m/%d")
        if sources.value == sources.emt or sources.value == sources.all:
            print("EMT")
            create_emt(settings=settings, date=date_formatted)
        if sources.value == sources.aemet or sources.value == sources.all:
            print("AEMET")
            create_aemet(settings=settings, date=date_formatted)
        if sources.value == sources.informo or sources.value == sources.all:
            print("INFORMO")
            create_informo(settings=settings, date=date_formatted)


if __name__ == "__main__":
    app()
