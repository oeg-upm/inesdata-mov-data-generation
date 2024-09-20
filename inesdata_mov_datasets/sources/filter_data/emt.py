import asyncio
import datetime
import json
import os
import sys
import traceback
from pathlib import Path

import aiohttp
import pytz
import requests
from loguru import logger

sys.path.append("/home/mlia/proyectos/data-generation/")

from inesdata_mov_datasets.settings import Settings
from inesdata_mov_datasets.utils import (
    check_local_file_exists,
    check_s3_file_exists,
    read_obj,
    upload_objs,
)


async def login_emt(config: Settings, object_login_name: str, local_path: Path = None) -> str:
    """Make the call to Login endpoint EMT.

    Args:
        config (Settings): Object with the config file.
        object_login_name (str): Name of the object which is onna be saved.
        local_path (Path): Local path to save login response.

    Returns:
        str: token from the login
    """
    if config.sources.emt.credentials.x_client_id is not None:
        headers = {
            "X-ClientId": config.sources.emt.credentials.x_client_id,
            "passKey": config.sources.emt.credentials.passkey,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    else:
        headers = {
            "email": config.sources.emt.credentials.email,
            "password": config.sources.emt.credentials.password,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    r = requests.get(
        "https://openapi.emtmadrid.es/v2/mobilitylabs/user/login/", headers=headers, verify=True
    )
    try:
        login_json = r.json()
        login_json_str = json.dumps(login_json)

        token = login_json["data"][0]["accessToken"]

        if config.storage.default == "minio":
            # Dict to upload s3 asynchronously
            login_dict_upload = {}
            login_dict_upload[str(object_login_name)] = login_json_str
            await upload_objs(
                config.storage.config.minio.bucket,
                config.storage.config.minio.endpoint,
                config.storage.config.minio.access_key,
                config.storage.config.minio.secret_key,
                login_dict_upload,
            )

        if config.storage.default == "local":
            os.makedirs(local_path, exist_ok=True)
            with open(os.path.join(local_path, object_login_name), "w") as file:
                file.write(login_json_str)

        return token
    except Exception as e:
        logger.error("Error in login call to the server")
        logger.error(e)
        return ""


async def token_control(config: Settings, date_slash: str, date_day: str) -> str:
    """Get existing token from EMT API or regenerate it if its deprecated.

    Args:
        config (Settings): Object with the config file.
        date_slash (str): date format for object name
        date_day (str): date format for object name

    Returns:
       str: Token from EMT Login.
    """
    if config.storage.default == "minio":
        object_login_name = Path("raw") / "emt" / date_slash / "login" / f"login_{date_day}.json"

        # Check if file already exists so we have made the call already
        if not await check_s3_file_exists(
            endpoint_url=config.storage.config.minio.endpoint,
            aws_secret_access_key=config.storage.config.minio.secret_key,
            aws_access_key_id=config.storage.config.minio.access_key,
            bucket_name=config.storage.config.minio.bucket,
            object_name=str(object_login_name),
        ):
            token = await login_emt(config, object_login_name)
            return token

        # If it exists, get the token from the json
        else:
            response = await read_obj(
                config.storage.config.minio.bucket,
                config.storage.config.minio.endpoint,
                config.storage.config.minio.access_key,
                config.storage.config.minio.secret_key,
                str(object_login_name),
            )

            data = json.loads(response)
            token = data["data"][0]["accessToken"]

            # Catching an error that ocurrs when the server doesnt provide the token expiration
            try:
                expiration_date_unix = data["data"][0]["tokenDteExpiration"]["$date"]
            except:
                logger.error(
                    "Error saving time expiration from login. Solving the problem retrying the call."
                )
                token = await login_emt(config, object_login_name)
                return token

            expiration_date = datetime.datetime.utcfromtimestamp(
                expiration_date_unix / 1000
            )  #  miliseconds to seconds
            now = datetime.datetime.now()

            # Compare the time expiration of the token withthe actual date
            if now >= expiration_date:  # reset token
                token = await login_emt(config, object_login_name)
                return token
            # Get the token that already exists
            elif now < expiration_date:
                return token

    elif config.storage.default == "local":
        dir_path = Path(config.storage.config.local.path) / "raw" / "emt" / date_slash / "login"
        object_login_name = f"login_{date_day}.json"

        # Check if file already exists so we have made the call already
        if not check_local_file_exists(dir_path, object_login_name):
            token = await login_emt(config, object_login_name, local_path=dir_path)
            return token

        # If it exists, get the token from the json
        else:
            with open(os.path.join(dir_path, object_login_name), "r") as file:
                response = file.read()
                data = json.loads(response)
                token = data["data"][0]["accessToken"]

                # Catching an error that ocurrs when the server doesnt provide the token expiration
                try:
                    expiration_date_unix = data["data"][0]["tokenDteExpiration"]["$date"]
                except:
                    logger.error(
                        "Error saving time expiration from login. Solving the problem retrying the call."
                    )
                    token = await login_emt(config, object_login_name, local_path=dir_path)
                    return token

                expiration_date = datetime.datetime.utcfromtimestamp(
                    expiration_date_unix / 1000
                )  #  miliseconds to seconds
                now = datetime.datetime.now()
                # Compare the time expiration of the token withthe actual date
                if now >= expiration_date:  # reset token
                    token = await login_emt(config, object_login_name, local_path=dir_path)
                    return token
                # Get the token that already exists
                elif now < expiration_date:
                    return token


async def get_calendar(
    session: aiohttp,
    startDate: str,
    endDate: str,
    headers: json,
) -> json:
    """Call Calendar endpoint EMT.

    Args:
        session (aiohttp): Call session to make faster the calls to the same API.
        startDate (str): Start date of the date you want to check.
        endDate (str): End date of the date you want to check.
        headers (json): Headers of the http petition.

    Returns:
        json: Response of the petition in json format.
    """
    calendar_url = (
        f"https://openapi.emtmadrid.es/v1/transport/busemtmad/calendar/{startDate}/{endDate}/"
    )
    async with session.get(calendar_url, headers=headers) as response:
        try:
            return await response.json()
        except Exception as e:
            logger.error("Error in calendar call to the server")
            logger.error(e)
            return {"code": -1}


async def get_line_detail(
    session: aiohttp,
    date: str,
    line_id: str,
    headers: json,
) -> json:
    """Call line_detail endpoint EMT.

    Args:
        session (aiohttp): Call session to make faster the calls to the same API.
        date (str): Date reference of the petition (we use the date of the done petition).
        line_id (str): Id of the line.
        headers (json): Headers of the petition.

    Returns:
        json: Response of the petition in json format.
    """
    line_detail_url = (
        f"https://openapi.emtmadrid.es/v1/transport/busemtmad/lines/{line_id}/info/{date}/"
    )
    async with session.get(line_detail_url, headers=headers) as response:
        try:
            return await response.json()
        except Exception as e:
            logger.error(f"Error in line_detail call line {line_id} to the server")
            logger.error(e)
            return {"code": -1}


async def get_eta(session: aiohttp, stop_id: str, line_id: str, headers: json) -> json:
    """Make the API call to ETA endpoint.

    Args:
        session (aiohttp): Call session to make faster the calls to the same API.
        stop_id (str): Id of the bus stop.
        line_id (str): Id of the bus line.
        headers (json): Headers of the http call.

    Returns:
        json: Response of the petition in json format.
    """
    body = {
        "cultureInfo": "ES",
        "Text_StopRequired_YN": "N",
        "Text_EstimationsRequired_YN": "Y",
        "Text_IncidencesRequired_YN": "N",
    }
    eta_url = (
        f"https://openapi.emtmadrid.es/v2/transport/busemtmad/stops/{stop_id}/arrives/{line_id}"
    )

    async with session.post(eta_url, headers=headers, json=body) as response:
        try:
            return await response.json()
        except Exception as e:
            logger.error(f"Error in ETA call stop {stop_id} to the server")
            logger.error(e)
            return {"code": -1}


async def get_filter_emt(config: Settings, stop_id: str, line_id: str):
    """Get all the data from EMT endpoints.

    Args:
        config (Settings): Object with the config file.
        stop_id (str): The stop id.
        line_id (str): The line id.
    """
    try:
        # Get the timezone from Madrid and formated the dates for the object_name of the files
        europe_timezone = pytz.timezone("Europe/Madrid")
        current_datetime = datetime.datetime.now(europe_timezone).replace(second=0)
        formatted_date_day = current_datetime.strftime(
            "%Y%m%d"
        )  # formatted date year|month|day all together
        formatted_date_slash = current_datetime.strftime(
            "%Y/%m/%d"
        )  # formatted date year/month/day for storage in Minio

        access_token = await token_control(
            config, formatted_date_slash, formatted_date_day
        )  # Obtain token from EMT

        # Headers for requests to the EMT API
        headers = {
            "accessToken": access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        async with aiohttp.ClientSession() as session:
            # List to store tasks asynchronously
            calendar_tasks = []
            eta_tasks = []

            # ETA
            eta_task = asyncio.ensure_future(get_eta(session, stop_id, line_id, headers))

            eta_tasks.append(eta_task)

            # Calendar
            calendar_task = asyncio.ensure_future(
                get_calendar(session, formatted_date_day, formatted_date_day, headers)
            )
            calendar_tasks.append(calendar_task)

            logger.info("Extracted EMT")

            return eta_task.result(), calendar_task.result()

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
