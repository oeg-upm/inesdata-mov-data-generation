"""Gather raw data from aemet."""
import datetime
import json
from pathlib import Path

import requests
import xmltodict

from inesdata_mov_datasets.settings import Settings
from inesdata_mov_datasets.utils import check_local_file_exists, check_s3_file_exists, upload_objs


async def get_informo(config: Settings):
    """Request informo API to get data from Madrid traffic.

    Args:
        config (Settings): Object with the config file.
    """
    print("EXTRACTING INFORMO")
    now = datetime.datetime.now()
    url_informo = "https://informo.madrid.es/informo/tmadrid/pm.xml"

    r = requests.get(url_informo)
    if r.status_code == 200:
        # Parse XML
        xml_dict = xmltodict.parse(r.content)
    else:
        print("Error:", r.status_code)

    await save_informo(config, xml_dict)

    end = datetime.datetime.now()
    print("Time duration", end - now)

    print("EXTRACTED INFORMO")
    print("- - - - - - -")


async def save_informo(config: Settings, data: json):
    """Save informo json.

    Args:
        config (Settings): Object with the config file.
        data (json): Data with informo in json format.
    """

    # Get the last update date from the response
    date_from_file = data["pms"]["fecha_hora"]
    dt = datetime.datetime.strptime(date_from_file, "%d/%m/%Y %H:%M:%S")

    # Formatear el objeto datetime en el formato deseado
    formated_date = dt.strftime("%Y-%m-%dT%H%M")

    current_datetime = datetime.datetime.now().replace(second=0)  # current date without seconds

    formatted_date_slash = current_datetime.strftime(
        "%Y/%m/%d"
    )  # formatted date year/month/day for storage in Minio

    if config.storage.default == "minio":
        # Define the object name
        object_name = (
            Path("raw") / "informo" / formatted_date_slash / f"informo_{formated_date}.json"
        )
        # Check if the Minio object exists
        if not await check_s3_file_exists(
            endpoint_url=config.storage.config.minio.endpoint,
            aws_secret_access_key=config.storage.config.minio.secret_key,
            aws_access_key_id=config.storage.config.minio.access_key,
            bucket_name=config.storage.config.minio.bucket,
            object_name=str(object_name),
        ):
            # Convert data to JSON string
            response_json_str = json.dumps(data)

            informo_dict_upload = {}
            informo_dict_upload[str(object_name)] = response_json_str
            await upload_objs(
                config.storage.config.minio.bucket,
                config.storage.config.minio.endpoint,
                config.storage.config.minio.access_key,
                config.storage.config.minio.secret_key,
                informo_dict_upload,
            )

    if config.storage.default == "local":
        object_name = f"informo_{formated_date}.json"
        path_save_informo = (
            Path(config.storage.config.local.path) / "raw" / "informo" / formatted_date_slash
        )
        # Check if the file exists
        if not check_local_file_exists(path_save_informo, object_name):
            # Convert data to JSON string
            response_json_str = json.dumps(data)

            # Create directories if they don't exist
            path_save_informo.mkdir(parents=True, exist_ok=True)

            # Write JSON data to file
            with open(path_save_informo / object_name, "w") as file:
                file.write(response_json_str)
