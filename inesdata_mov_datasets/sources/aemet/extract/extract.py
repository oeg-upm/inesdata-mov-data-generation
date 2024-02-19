"""Gather raw data from aemet."""
import datetime
import io
import json

import requests

from inesdata_mov_datasets.settings import Settings
from inesdata_mov_datasets.utils import minio_connection, read_settings


def get_weather_madrid(config: Settings):
    """Request aemet API to get data from Madrid weather.

    Args:
        config (Settings): Object with the config file.
    """

    url_madrid = (
        "https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/28079"
    )

    headers = {
        "api_key": config.sources.aemet.credentials.api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    r = requests.get(url_madrid, headers=headers)
    r_json = requests.get(r.json()["datos"]).json()

    save_weather(config, r_json)

    return


def save_weather(config: Settings, data: json):
    """Save weather json.

    Args:
        config (Settings): Object with the config file.
        data (json): Data with weather in json format.
    """
    current_datetime = datetime.datetime.now().replace(second=0)  # current date without seconds
    formatted_date_day = current_datetime.strftime(
        "%Y%m%d"
    )  # formatted date year|month|day all together
    formatted_date_slash = current_datetime.strftime(
        "%Y/%m/%d"
    )  # formatted date year/month/day for storage in Minio

    if config.storage.default == "minio":
        minio_client = minio_connection(config)
        object_name = (
            f"/raw/aemet/{formatted_date_slash}/madrid/madrid_weather_{formatted_date_day}.json"
        )
        response_json_str = json.dumps(data)
        minio_client.put_object(
            config.storage.config.minio.bucket,
            object_name,
            io.BytesIO(response_json_str.encode("utf-8")),
            len(response_json_str),
        )

    return


if __name__ == "__main__":
    config = read_settings("/home/jfog/proyects/inesdata-mov/data-generation/jfog-newconfig.yaml")
    get_weather_madrid(config)
