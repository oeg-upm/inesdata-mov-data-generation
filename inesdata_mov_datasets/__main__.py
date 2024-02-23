"""Command line interface for inesdata_mov_datasets."""

from datetime import datetime

import typer

from inesdata_mov_datasets.utils import read_settings

app = typer.Typer(add_completion=False)


@app.command()
def gather(config: str = typer.Option(default=..., help="Path to configuration yaml file")):
    """Gather raw data from the sources configurated."""

    # read settings
    settings = read_settings(config)

    print("Gathering data")


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
