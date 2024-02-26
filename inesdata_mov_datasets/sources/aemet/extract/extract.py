"""Gather raw data from aemet."""

import datetime
import io
import json
from pathlib import Path
from minio import Minio

import requests

from inesdata_mov_datasets.settings import Settings
from inesdata_mov_datasets.utils import read_settings, check_minio_file_exists, check_local_file_exists


def get_aemet(config: Settings, minio_client: Minio=None):
    """Request aemet API to get data from Madrid weather.

    Args:
        config (Settings): Object with the config file.
    """
    print("EXTRACTING AEMET")
    now = datetime.datetime.now()
    
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

    save_aemet(config, r_json, minio_client)
    
    end = datetime.datetime.now()
    print("Time duration", end - now)
    print("EXTRACTED AEMET")
    print("- - - - - - -")


def save_aemet(config: Settings, data: json, minio_client: Minio=None):
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
        # Define object name
        object_name = Path("raw") / "aemet" / formatted_date_slash / f"aemet_{formatted_date_day}.json"
        
        if not check_minio_file_exists(minio_client, config.storage.config.minio.bucket, str(object_name)):
            # Convert data to JSON string
            response_json_str = json.dumps(data)
            
            # Create BytesIO object from JSON string
            response_bytes = io.BytesIO(response_json_str.encode("utf-8"))
            
            # Put object in Minio bucket
            minio_client.put_object(
                config.storage.config.minio.bucket,
                str(object_name),
                response_bytes,
                len(response_json_str)
            )

    if config.storage.default == "local":
        # Define object name and path for local storage
        object_name = f"aemet_{formatted_date_day}.json"
        local_path = Path(config.storage.config.local.path) / "raw" / "aemet" / formatted_date_slash
        
        if not check_local_file_exists(local_path, object_name):
            # Convert data to JSON string
            response_json_str = json.dumps(data)
            
            # Create directories if they don't exist
            local_path.mkdir(parents=True, exist_ok=True)
            
            # Write JSON data to file
            with open(local_path / object_name, "w") as file:
                file.write(response_json_str)