"""Command line interface for inesdata_mov_datasets."""

from datetime import datetime
from enum import Enum

import typer
import asyncio

from inesdata_mov_datasets.utils import read_settings, minio_connection
from inesdata_mov_datasets.sources.aemet.extract.extract import get_aemet
from inesdata_mov_datasets.sources.informo.extract.extract import get_informo
from inesdata_mov_datasets.sources.emt.extract.extract import get_emt

app = typer.Typer()


class Sources(str, Enum):
    all = "all"
    emt = "emt"
    aemet = "aemet"
    informo = "informo"

@app.command()
def extract(config_path: str = typer.Option(default=..., help="Path to configuration yaml file"), sources: Sources = typer.Option(default=Sources.all.value, help="Possible sources to extract.")):
    """extract raw data from the sources configurated."""

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
    config: str = typer.Option(default=..., help="Path to configuration yaml file"),
    start_date: datetime = typer.Option(
        default=..., formats=["%Y%m%d"], help="Start date in format YYYYMMDD"
    ),
    end_date: datetime = typer.Option(
        default=..., formats=["%Y%m%d"], help="End date in format YYYYMMDD"
    ),
):
    """Create movility datasets in a given date range from raw data. Please, run first gather command to get the raw data."""

    # read settings
    settings = read_settings(config)
    print("Create dataset")


if __name__ == "__main__":
    app()
