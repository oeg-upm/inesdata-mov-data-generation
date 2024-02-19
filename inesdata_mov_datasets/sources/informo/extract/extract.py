"""Gather raw data from aemet."""
import datetime
import io
import json
import xmltodict

import requests

from inesdata_mov_datasets.settings import Settings
from inesdata_mov_datasets.utils import minio_connection, read_settings


def get_informo(config: Settings):
    """Request informo API to get data from Madrid traffic.

    Args:
        config (Settings): Object with the config file.
    """
    url_informo = (
        "https://informo.madrid.es/informo/tmadrid/pm.xml"
    )

    r = requests.get(url_informo)
    if r.status_code == 200:
        # Parse XML
        xml_dict = xmltodict.parse(r.content)
    else:
        print("Error:", r.status_code)
        
    save_informo(config, xml_dict)
    
    return
        


def save_informo(config: Settings, data: json):
    """Save informo json.

    Args:
        config (Settings): Object with the config file.
        data (json): Data with informo in json format.
    """
    current_datetime = datetime.datetime.now().replace(second=0)  # current date without seconds
    formatted_date = current_datetime.strftime(
        "%Y-%m-%dT%H:%M"
    )  # formatted date according to ISO86001 without seconds
    formatted_date_slash = current_datetime.strftime(
        "%Y/%m/%d"
    )  # formatted date year/month/day for storage in Minio

    if config.storage.default == "minio":
        minio_client = minio_connection(config)
        object_name = (
            f"/raw/informo/{formatted_date_slash}/traffic_{formatted_date}.json"
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
    get_informo(config)
