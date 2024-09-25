"""Gather raw data from EMT."""

import asyncio
import datetime
import json
import os
import traceback
from pathlib import Path

import aiohttp
import pytz
import requests
from loguru import logger

from inesdata_mov_datasets.settings import Settings
from inesdata_mov_datasets.utils import (
    check_local_file_exists,
    check_s3_file_exists,
    read_obj,
    upload_objs,
)

from inesdata_mov_datasets.sources.extract.emt import login_emt, token_control, get_calendar, get_line_detail

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

            eta_responses = await asyncio.gather(*eta_tasks)
            calendar_responses = await asyncio.gather(*calendar_tasks)
            
            logger.info("Extracted EMT")
            return eta_responses[0], calendar_responses[0]

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
