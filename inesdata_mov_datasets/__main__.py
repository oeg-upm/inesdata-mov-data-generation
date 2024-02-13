"""Command line interface for inesdata_mov_datasets."""

from datetime import datetime

import typer
import yaml

from inesdata_mov_datasets.settings import Settings

app = typer.Typer()


def _read_settings(path: str) -> Settings:
    """Read settings from yaml file

    Args:
        path (str): path to configuration file.

    Returns:
        Settings: Pydantic object to manage paramters.
    """
    with open(path, "r") as file:
        return Settings(**yaml.safe_load(file))


@app.command()
def gather(config: str = typer.Option(default=..., help="Path to configuration yaml file")):
    """Gather raw data from the sources configurated."""

    # read settings
    settings = _read_settings(config)

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
    settings = _read_settings(config)
    print("Create dataset")


if __name__ == "__main__":
    app()
