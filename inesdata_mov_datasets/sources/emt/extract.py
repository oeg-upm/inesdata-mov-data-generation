"""Gather raw data from emt."""
import asyncio
import datetime
import io
import json

import aiohttp
import requests
from minio import Minio
from minio.error import S3Error

from inesdata_mov_datasets.settings import Settings
from inesdata_mov_datasets.utils import read_settings


def minio_connection(configuration: Settings) -> Minio:
    """Manage connection to minio server.

    Args:
        configuration (dict): Dictionary with the configuration of the proyect.

    Returns:
        Minio: Object with the MinIO client.
    """
    minio_client = Minio(
        configuration.storage.config.minio.endpoint,
        access_key=configuration.storage.config.minio.access_key,
        secret_key=configuration.storage.config.minio.secret_key,
        region="us-east-1",
        secure=configuration.storage.config.minio.secure,
    )
    return minio_client


def login_emt(config: Settings) -> str:
    """Get token from EMT API.

    Args:
        config (Settings): Object with the config file.

    Returns:
       str: Token from EMT Login.
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
        "https://openapi.emtmadrid.es/v2/mobilitylabs/user/login/", headers=headers, verify=False
    )
    formato_json = r.json()

    return formato_json["data"][0]["accessToken"]


async def get_eta(session: aiohttp, stop_id: str, headers: json) -> json:
    """Make the API call to ETA endpoint.

    Args:
        session (aiohttp): Call session to make faster the calls to the same API.
        url (str): Url of the endpoint.
        stop_id (str): Id of the bus stop.
        headers (json): Headers of the http call.
        body (json): Body of the post petition.

    Returns:
        json: Response of the petition in json format.
    """
    body = {
        "cultureInfo": "ES",
        "Text_StopRequired_YN": "N",
        "Text_EstimationsRequired_YN": "Y",
        "Text_IncidencesRequired_YN": "N",
    }
    eta_url = f"https://openapi.emtmadrid.es/v2/transport/busemtmad/stops/{stop_id}/arrives/"
    async with session.post(eta_url, headers=headers, json=body) as response:
        return await response.json()


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
        return await response.json()


async def get_line_detail(
    session: aiohttp,
    date: str,
    stop_id: str,
    headers: json,
) -> json:
    """Call line_detail endpoint EMT.

    Args:
        session (aiohttp): Call session to make faster the calls to the same API.
        date (str): Date reference of the petition (we use the date of the done petition).
        stop_id (str): Id of the stop.
        headers (json): Headers of the petition.

    Returns:
        json: Response of the petition in json format.
    """
    line_detail_url = (
        f"https://openapi.emtmadrid.es/v1/transport/busemtmad/lines/{stop_id}/info/{date}"
    )
    async with session.get(line_detail_url, headers=headers) as response:
        return await response.json()


def check_file_exists(minio_client: Minio, bucket_name: str, object_name: str) -> bool:
    """Check if a file exists.

    Args:
        minio_client (Minio): Client of the Minio bucket.
        bucket_name (str): Bucket name.
        object_name (str): Object name.

    Raises:
        e: S3Error if file is not detected.

    Returns:
        bool: True if dile is detected, False otherwise.
    """
    try:
        minio_client.stat_object(bucket_name, object_name)
        return True
    except S3Error as e:
        if e.code == "NoSuchKey":
            return False
        else:
            raise e


async def main(config: Settings):
    minio_client = minio_connection(config)  # Start Minio connection

    access_token = login_emt(config)  # Obtain token from EMT

    # Headers for requests to the EMT API
    headers = {
        "accessToken": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    bucket_name = config.storage.config.minio.bucket

    # Date formatting
    current_datetime = datetime.datetime.now().replace(second=0)  # current date without seconds
    formatted_date = current_datetime.strftime(
        "%Y-%m-%dT%H:%M"
    )  # formatted date according to ISO86001 without seconds
    formatted_date_day = current_datetime.strftime(
        "%Y%m%d"
    )  # formatted date year|month|day all together
    formatted_date_slash = current_datetime.strftime(
        "%Y/%m/%d"
    )  # formatted date year/month/day for storage in Minio


    async with aiohttp.ClientSession() as session:
        # List to store tasks asynchronously
        calendar_tasks = []
        eta_tasks = []
        line_detail_tasks = []
        first_stop = config.sources.emt.stops[0]

        object_calendar_name = (
            f"/raw/emt/{formatted_date_slash}/calendar/calendar_{formatted_date_day}.json"
        )

        # Make request to the line_detail endpoint checking if the request has not been made today
        first_object_line_detail_name = f"/raw/emt/{formatted_date_slash}/line_detail/line_detail_{first_stop}_{formatted_date_day}.json"
        if not check_file_exists(minio_client, bucket_name, first_object_line_detail_name):
            for stop_id in config.sources.emt.stops:
                line_detail_task = asyncio.ensure_future(
                    get_line_detail(session, formatted_date_day, stop_id, headers)
                )
                line_detail_tasks.append(line_detail_task)
        else:
            print("Already called line_detail")

        # Make request to the calendar endpoint
        if not check_file_exists(minio_client, bucket_name, object_calendar_name):
            calendar_task = asyncio.ensure_future(
                get_calendar(session, formatted_date_day, formatted_date_day, headers)
            )
            calendar_tasks.append(calendar_task)

        else:
            print("Already called Calendar")

        # Make requests to the eta for each stop
        for stop_id in config.sources.emt.stops:
            eta_task = asyncio.ensure_future(get_eta(session, stop_id, headers))
            eta_tasks.append(eta_task)

        # Wait for all tasks to complete
        calendar_response = await asyncio.gather(*calendar_tasks)
        eta_responses = await asyncio.gather(*eta_tasks)
        line_detail_responses = await asyncio.gather(*line_detail_tasks)


        if line_detail_responses:
            for stop_id, response in zip(config.sources.emt.stops, line_detail_responses):
                object_line_detail_name = f"/raw/emt/{formatted_date_slash}/line_detail/line_detail_{stop_id}_{formatted_date_day}.json"
                try:
                    response_json_str = json.dumps(response)
                    minio_client.put_object(
                        bucket_name,
                        object_line_detail_name,
                        io.BytesIO(response_json_str.encode("utf-8")),
                        len(response_json_str),
                    )
                except:
                    print("Error for stop ", stop_id, "Line detail ")

        # Store the calendar response in MinIO if present
        if calendar_response:
            calendar_json_str = json.dumps(calendar_response)
            minio_client.put_object(
                bucket_name,
                object_calendar_name,
                io.BytesIO(calendar_json_str.encode("utf-8")),
                len(calendar_json_str),
            )

        # Store the bus stop responses in MinIO
        for stop_id, response in zip(config.sources.emt.stops, eta_responses):
            object_eta_name = (
                f"/raw/emt/{formatted_date_slash}/eta/eta_{stop_id}_{formatted_date}.json"
            )

            try:
                response_json_str = json.dumps(response)
                minio_client.put_object(
                    bucket_name,
                    object_eta_name,
                    io.BytesIO(response_json_str.encode("utf-8")),
                    len(response_json_str),
                )

            except:
                print("Error for stop ", stop_id, "Time arrival ")


if __name__ == "__main__":
    config = read_settings("/home/jfog/proyects/inesdata-mov/data-generation/jfog-newconfig.yaml")
    response = login_emt(config)

    asyncio.run(main(config))
