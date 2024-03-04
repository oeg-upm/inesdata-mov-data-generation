"""Command line interface for inesdata_mov_datasets."""

import asyncio
from datetime import datetime, timedelta
from enum import Enum

import pandas as pd
import typer

from inesdata_mov_datasets.sources.create.aemet import create_aemet
from inesdata_mov_datasets.sources.create.emt import create_emt
from inesdata_mov_datasets.sources.create.informo import create_informo
from inesdata_mov_datasets.sources.extract.aemet import get_aemet
from inesdata_mov_datasets.sources.extract.emt import get_emt
from inesdata_mov_datasets.sources.extract.informo import get_informo
from inesdata_mov_datasets.utils import read_settings

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
    config_path: str = typer.Option(help="Path to configuration yaml file"),
    sources: Sources = typer.Option(
        default=Sources.all.value, help="Possible sources to extract."
    ),
):
    """Extract raw data from the sources configurated."""
    # read settings
    settings = read_settings(config_path)

    # EMT
    if sources.value == sources.emt or sources.value == sources.all:
        asyncio.run(get_emt(settings))
    # Aemet
    if sources.value == sources.aemet or sources.value == sources.all:
        asyncio.run(get_aemet(settings))
    # Informo
    if sources.value == sources.informo or sources.value == sources.all:
        asyncio.run(get_informo(settings))

    print("Extracted data")


@app.command()
def create(
    config_path: str = typer.Option(help="Path to configuration yaml file"),
    start_date: datetime = typer.Option(
        default=datetime.today(),
        formats=["%Y%m%d"],
        help="Start date in format YYYYMMDD",
    ),
    end_date: datetime = typer.Option(
        default=(datetime.today() + timedelta(days=1)),
        formats=["%Y%m%d"],
        help="End date in format YYYYMMDD",
    ),
    sources: Sources = typer.Option(
        default=Sources.all.value, help="Possible sources to generate."
    ),
):
    """Create mobility datasets in a given date range from raw data. Please, run first extract command to get the raw data.

    Execution example: python inesdata_mov_datasets create --config-path=/home/code/inesdata-mov/data-generation/.config_dev.yaml --start-date=20240219 --end-date=20240220 --sources=informo
    """
    # read settings
    settings = read_settings(config_path)
    print(
        f"Create {sources.value} dataset from {start_date.strftime('%Y/%m/%d')} to {end_date.strftime('%Y/%m/%d')}"
    )
    dates = pd.date_range(start_date, end_date - timedelta(days=1), freq="d")
    for date in dates:
        date_formatted = date.strftime("%Y/%m/%d")
        if sources.value == sources.emt or sources.value == sources.all:
            create_emt(settings=settings, date=date_formatted)
            print("- - - - - - -")
        if sources.value == sources.aemet or sources.value == sources.all:
            create_aemet(settings=settings, date=date_formatted)
            print("- - - - - - -")
        if sources.value == sources.informo or sources.value == sources.all:
            create_informo(settings=settings, date=date_formatted)
            print("- - - - - - -")
    print("Created data")


if __name__ == "__main__":
    app()
