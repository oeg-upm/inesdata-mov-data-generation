"""Gather raw data from EMT."""

import asyncio
import datetime
import json
import traceback

import requests

import aiohttp
import pytz
from loguru import logger

from inesdata_mov_datasets.settings import Settings
from inesdata_mov_datasets.sources.extract.emt import (
    get_calendar,
    token_control,
)

from inesdata_mov_datasets.utils import read_settings


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

def get_eta_sync(stop_id: str, line_id: str, headers: json) -> json:
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

    try:
        response = requests.post(eta_url, headers=headers, json=body)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Error in ETA call stop {stop_id} to the server")
        logger.error(e)
        return {"code": -1}
    
def get_calendar_sync(start_date: str, end_date: str, headers: dict) -> dict:
    """Call Calendar endpoint EMT.

    Args:
        start_date (str): Start date of the date you want to check.
        end_date (str): End date of the date you want to check.
        headers (dict): Headers of the HTTP petition.

    Returns:
        dict: Response of the petition in JSON format.
    """
    calendar_url = f"https://openapi.emtmadrid.es/v1/transport/busemtmad/calendar/{start_date}/{end_date}/"
    
    try:
        # Realiza la llamada GET y obtiene la respuesta
        response = requests.get(calendar_url, headers=headers)
        response.raise_for_status()  # Lanza una excepción si la respuesta es un error
        return response.json()  # Devuelve la respuesta en formato JSON
    except Exception as e:
        logger.error("Error in calendar call to the server")
        logger.error(e)
        return {"code": -1}  # Devuelve un código de error en caso de fallo

from pathlib import Path
import os
from inesdata_mov_datasets.utils import check_local_file_exists
def login_emt_sync(config: Settings, object_login_name: str, local_path: Path ):
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
        os.makedirs(local_path, exist_ok=True)
        with open(os.path.join(local_path, object_login_name), "w") as file:
            file.write(login_json_str)

        return token
    except Exception as e:
        logger.error("Error in login call to the server")
        logger.error(e)
        return ""
    
def token_control_sync(config: Settings, date_slash: str, date_day: str):
    dir_path = Path(config.storage.config.local.path) / "raw" / "emt" / date_slash / "login"
    object_login_name = f"login_{date_day}.json"

    if not check_local_file_exists(dir_path, object_login_name):
        token = login_emt_sync(config, object_login_name, local_path=dir_path)
        return token
    # If it exists, get the token from the json
    else:
        with open(os.path.join(dir_path, object_login_name), "r") as file:
            response = file.read()
            data = json.loads(response)
            token = data["data"][0]["accessToken"]
            
            #Catching an error that ocurrs when the server doesnt provide the token expiration
            try:
                expiration_date_unix = data["data"][0]["tokenDteExpiration"]["$date"]
            except:
                logger.error(f"Error saving time expiration from login. Solving the problem retrying the call.")
                token = login_emt_sync(config, object_login_name, local_path=dir_path)
                return token
            
            expiration_date = datetime.datetime.utcfromtimestamp(
                expiration_date_unix / 1000
            )  #  miliseconds to seconds
            now = datetime.datetime.now()
            # Compare the time expiration of the token withthe actual date
            if now >= expiration_date:  # reset token
                token = login_emt_sync(config, object_login_name, local_path=dir_path)
                return token
            # Get the token that already exists
            elif now < expiration_date:
                return token

def get_filter_emt_sync(config: Settings, stop_id: str, line_id: str):
    """Get all the data from EMT endpoints.

    Args:
        config (Settings): Object with the config file.
        stop_id (str): The stop id.
        line_id (str): The line id.
    """
    try:
        # st = datetime.datetime.now()
        # Get the timezone from Madrid and format the dates for the object_name of the files
        europe_timezone = pytz.timezone("Europe/Madrid")
        current_datetime = datetime.datetime.now(europe_timezone).replace(second=0)
        formatted_date_day = current_datetime.strftime("%Y%m%d")
        formatted_date_slash = current_datetime.strftime("%Y/%m/%d")

        # Obtain token from EMT
        access_token =token_control_sync(config, formatted_date_slash, formatted_date_day)

        headers = {
            "accessToken": access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        # Realizar ambas solicitudes de forma sincrónica
        eta_response = get_eta_sync(stop_id, line_id, headers)  # Reemplazar con una función sincrónica
        calendar_response = get_calendar_sync(formatted_date_day, formatted_date_day, headers)  # Reemplazar con una función sincrónica
        

        logger.info("Extracted EMT")
        # et = datetime.datetime.now()
        # print('sincrono',(et-st).total_seconds())
        return eta_response, calendar_response

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())

async def get_filter_emt(config: Settings, stop_id: str, line_id: str):
    """Get all the data from EMT endpoints.

    Args:
        config (Settings): Object with the config file.
        stop_id (str): The stop id.
        line_id (str): The line id.
    """
    try:
        # st = datetime.datetime.now()
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

            eta_responses = await asyncio.gather(*eta_tasks)
            calendar_responses = await asyncio.gather(*calendar_tasks)

            logger.info("Extracted EMT")

            # et = datetime.datetime.now()
            # print('asincrono',(et-st).total_seconds())
            return eta_responses[0], calendar_responses[0]

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())

# if __name__ == "__main__":
#     sett = read_settings('/home/mlia/proyectos/data-generation/config_dev.yaml')
#     # asyncio.run(get_filter_emt_sync(config=sett, stop_id='29', line_id='27'))
#     print('asi ----->',asyncio.run(get_filter_emt(config=sett, stop_id='29', line_id='27')))
#     print('sy ----->',get_filter_emt_sync(config=sett, stop_id='29', line_id='27'))


