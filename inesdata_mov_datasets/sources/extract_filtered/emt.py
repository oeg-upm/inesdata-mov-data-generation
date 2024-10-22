"""Gather raw data from EMT."""

import asyncio
import datetime
import json
import traceback

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

async def get_filter_emt_2(config: Settings, stop_id: str, line_id: str):
    """Get all the data from EMT endpoints.

    Args:
        config (Settings): Object with the config file.
        stop_id (str): The stop id.
        line_id (str): The line id.
    """
    try:
        st = datetime.datetime.now()
        # Get the timezone from Madrid and format the dates for the object_name of the files
        europe_timezone = pytz.timezone("Europe/Madrid")
        current_datetime = datetime.datetime.now(europe_timezone).replace(second=0)
        formatted_date_day = current_datetime.strftime("%Y%m%d")
        formatted_date_slash = current_datetime.strftime("%Y/%m/%d")

        # Obtain token from EMT
        access_token = await token_control(config, formatted_date_slash, formatted_date_day)

        headers = {
            "accessToken": access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        async with aiohttp.ClientSession() as session:
            # Perform both requests concurrently using asyncio.gather
            eta_response, calendar_response = await asyncio.gather(
                get_eta(session, stop_id, line_id, headers),
                get_calendar(session, formatted_date_day, formatted_date_day, headers)
            )

            logger.info("Extracted EMT")
            et = datetime.datetime.now()
            print('second test', (et - st).total_seconds())
            return eta_response, calendar_response

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())


async def get_filter_emt_1(config: Settings, stop_id: str, line_id: str):
    """Get all the data from EMT endpoints.

    Args:
        config (Settings): Object with the config file.
        stop_id (str): The stop id.
        line_id (str): The line id.
    """
    try:
        st = datetime.datetime.now()
        # Get the timezone from Madrid and format the dates for the object_name of the files
        europe_timezone = pytz.timezone("Europe/Madrid")
        current_datetime = datetime.datetime.now(europe_timezone).replace(second=0)
        formatted_date_day = current_datetime.strftime("%Y%m%d")
        formatted_date_slash = current_datetime.strftime("%Y/%m/%d")

        # Obtain token from EMT
        access_token = await token_control(config, formatted_date_slash, formatted_date_day)

        headers = {
            "accessToken": access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        async with aiohttp.ClientSession() as session:
            # Perform both requests concurrently using asyncio.gather
            eta_task = get_eta(session, stop_id, line_id, headers)
            calendar_task = get_calendar(session, formatted_date_day, formatted_date_day, headers)

            eta_response, calendar_response = await asyncio.gather(eta_task, calendar_task)

            logger.info("Extracted EMT")
            et = datetime.datetime.now()
            print('first test',(et-st).total_seconds())
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
        st = datetime.datetime.now()
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

            et = datetime.datetime.now()
            print('ORIGINAL',(et-st).total_seconds())
            return eta_responses[0], calendar_responses[0]

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())



