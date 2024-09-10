"""Gather raw data from aemet."""

import datetime
import json
import traceback
from pathlib import Path
import pytz

import requests
from loguru import logger

from inesdata_mov_datasets.handlers.logger import instantiate_logger
from inesdata_mov_datasets.settings import Settings
from inesdata_mov_datasets.utils import check_local_file_exists, check_s3_file_exists, upload_objs


async def get_aemet(config: Settings):
    """Request aemet API to get data from Madrid weather.

    Args:
        config (Settings): Object with the config file.
    """
    try:
        # Logger
        instantiate_logger(config, "AEMET", "extract")
        logger.info("Extracting AEMET")
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

        await save_aemet(config, r_json)

        end = datetime.datetime.now()
        logger.debug(f"Time duration of AEMET extraction {end - now}")
        logger.info("Extracted AEMET")
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())


async def save_aemet(config: Settings, data: json):
    """Save weather json.

    Args:
        config (Settings): Object with the config file.
        data (json): Data with weather in json format.
    """
    
    # Get the timezone from Madrid and formated the dates for the object_name of the files
    europe_timezone = pytz.timezone("Europe/Madrid")
    current_datetime = datetime.datetime.now(europe_timezone).replace(second=0)
    formatted_date_day = current_datetime.strftime(
        "%Y%m%d"
    )  # formatted date year|month|day all together
    formatted_date_slash = current_datetime.strftime(
        "%Y/%m/%d"
    )  # formatted date year/month/day for storage in Minio

    if config.storage.default == "minio":
        # Define object name
        object_name = (
            Path("raw") / "aemet" / formatted_date_slash / f"aemet_{formatted_date_day}.json"
        )

        if not await check_s3_file_exists(
            endpoint_url=config.storage.config.minio.endpoint,
            aws_secret_access_key=config.storage.config.minio.secret_key,
            aws_access_key_id=config.storage.config.minio.access_key,
            bucket_name=config.storage.config.minio.bucket,
            object_name=str(object_name),
        ):
            # Convert data to JSON string
            response_json_str = json.dumps(data)

            # Create dict and upload into s3
            aemet_dict_upload = {}
            aemet_dict_upload[str(object_name)] = response_json_str
            await upload_objs(
                config.storage.config.minio.bucket,
                config.storage.config.minio.endpoint,
                config.storage.config.minio.access_key,
                config.storage.config.minio.secret_key,
                aemet_dict_upload,
            )
        else:
            logger.debug("Already called AEMET today")

    if config.storage.default == "local":
        # Define object name and path for local storage
        object_name = f"aemet_{formatted_date_day}.json"
        local_path = (
            Path(config.storage.config.local.path) / "raw" / "aemet" / formatted_date_slash
        )

        if not check_local_file_exists(local_path, object_name):
            # Convert data to JSON string
            response_json_str = json.dumps(data)

            # Create directories if they don't exist
            local_path.mkdir(parents=True, exist_ok=True)

            # Write JSON data to file
            with open(local_path / object_name, "w") as file:
                file.write(response_json_str)
        else:
            logger.debug("Already called AEMET today")
