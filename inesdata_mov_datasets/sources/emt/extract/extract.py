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
from inesdata_mov_datasets.utils import minio_connection, read_settings


def login_emt(config: Settings, minio_client: Minio, object_login_name: str) -> str:
    """Make the call to Login endpoint EMT.

    Args:
        config (Settings): Object with the config file.
        minio_client (Minio): Object with Minio connection.
        object_login_name (str): Name of the object which is onna be saved.

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
        "https://openapi.emtmadrid.es/v2/mobilitylabs/user/login/", headers=headers, verify=False
    )
    login_json = r.json()
    login_json_str = json.dumps(login_json)

    token = login_json["data"][0]["accessToken"]

    minio_client.put_object(
        config.storage.config.minio.bucket,
        object_login_name,
        io.BytesIO(login_json_str.encode("utf-8")),
        len(login_json_str),
    )

    return token


def token_control(config: Settings, minio_client: Minio, date_slash: str, date_day: str) -> str:
    """Get existing token from EMT API or regenerate it if its deprecated.

    Args:
        config (Settings): Object with the config file.
        minio_client (Minio): Object with Minio connection.
        date_slash (str): date format for object name
        date_day (str): date format for object name

    Returns:
       str: Token from EMT Login.
    """
    object_login_name = f"/raw/emt/{date_slash}/login/login_{date_day}.json"
    if not check_file_exists(minio_client, config.storage.config.minio.bucket, object_login_name):
        token = login_emt(config, minio_client, object_login_name)

        return token

    else:
        response = minio_client.get_object(
            bucket_name=config.storage.config.minio.bucket,
            object_name=object_login_name,
        )

        data = json.load(response)
        token = data["data"][0]["accessToken"]
        expiration_date_unix = data["data"][0]["tokenDteExpiration"]["$date"]

        expiration_date = datetime.datetime.utcfromtimestamp(
            expiration_date_unix / 1000
        )  #  miliseconds to seconds
        now = datetime.datetime.now()
        # Compare
        if now >= expiration_date:  # reset token
            token = login_emt(config, minio_client, object_login_name)
            return token

        elif now < expiration_date:
            return token


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
        f"https://openapi.emtmadrid.es/v1/transport/busemtmad/lines/{line_id}/info/{date}"
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

    now = datetime.datetime.now()

    minio_client = minio_connection(config)  # Start Minio connection

    access_token = token_control(
        config, minio_client, formatted_date_slash, formatted_date_day
    )  # Obtain token from EMT

    # Headers for requests to the EMT API
    headers = {
        "accessToken": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    bucket_name = config.storage.config.minio.bucket

    # Date formatting

    async with aiohttp.ClientSession() as session:
        # List to store tasks asynchronously
        calendar_tasks = []
        eta_tasks = []
        line_detail_tasks = []

        object_calendar_name = (
            f"/raw/emt/{formatted_date_slash}/calendar/calendar_{formatted_date_day}.json"
        )

        # Make request to the line_detail endpoint checking if the request has not been made today
        lines_called = 0
        for line_id in config.sources.emt.lines:
            object_line_detail_name = f"/raw/emt/{formatted_date_slash}/line_detail/line_detail_{line_id}_{formatted_date_day}.json"
            if not check_file_exists(minio_client, bucket_name, object_line_detail_name):
                line_detail_task = asyncio.ensure_future(
                    get_line_detail(session, formatted_date_day, line_id, headers)
                )
                line_detail_tasks.append(line_detail_task)
            else:
                lines_called += 1

        print("Already called " + str(lines_called) + " lines")

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

        errors_ld = 0
        errors_eta = 0

        if line_detail_responses:
            for line_id, response in zip(config.sources.emt.lines, line_detail_responses):
                object_line_detail_name = f"/raw/emt/{formatted_date_slash}/line_detail/line_detail_{line_id}_{formatted_date_day}.json"
                try:
                    response_json_str = json.dumps(response)
                    if response["code"] == "00":
                        minio_client.put_object(
                            bucket_name,
                            object_line_detail_name,
                            io.BytesIO(response_json_str.encode("utf-8")),
                            len(response_json_str),
                        )
                    else:
                        errors_ld += 1
                except:
                    print("Error for line ", line_id, "Line detail ")

        # Store the calendar response in MinIO if present
        if calendar_response:
            calendar_json_str = json.dumps(calendar_response)

            if response["code"] == "00":
                minio_client.put_object(
                    bucket_name,
                    object_calendar_name,
                    io.BytesIO(calendar_json_str.encode("utf-8")),
                    len(calendar_json_str),
                )
            else:
                print("Error in calendar")

        # Store the bus stop responses in MinIO
        list_stops_error = []
        for stop_id, response in zip(config.sources.emt.stops, eta_responses):
            object_eta_name = (
                f"/raw/emt/{formatted_date_slash}/eta/eta_{stop_id}_{formatted_date}.json"
            )

            try:
                response_json_str = json.dumps(response)
                if response["code"] == "00":
                    minio_client.put_object(
                        bucket_name,
                        object_eta_name,
                        io.BytesIO(response_json_str.encode("utf-8")),
                        len(response_json_str),
                    )
                else:
                    errors_eta += 1
                    list_stops_error.append(stop_id)

            except:
                print("Error for stop ", stop_id, "Time arrival ")

        print("Errors in Line Detail: ", errors_ld)
        print("Errors in ETA:", errors_eta, "list of stops erroring:", list_stops_error)

        if errors_eta > 0:
            list_stops_error_retry = []
            eta_tasks2 = []
            errors_eta_retry = 0
            # Make requests that failed to the eta for each stop
            for stop_id in list_stops_error:
                eta_task = asyncio.ensure_future(get_eta(session, stop_id, headers))
                eta_tasks2.append(eta_task)

            eta_responses2 = await asyncio.gather(*eta_tasks2)
            for stop_id, response in zip(list_stops_error, eta_responses2):
                object_eta_name = (
                    f"/raw/emt/{formatted_date_slash}/eta/eta_{stop_id}_{formatted_date}.json"
                )

                try:
                    response_json_str = json.dumps(response)
                    if response["code"] == "00":
                        minio_client.put_object(
                            bucket_name,
                            object_eta_name,
                            io.BytesIO(response_json_str.encode("utf-8")),
                            len(response_json_str),
                        )
                    else:
                        errors_eta_retry += 1
                        list_stops_error_retry.append(stop_id)

                except:
                    print("Error for stop ", stop_id, "Time arrival ")

        print(
            "Errors in ETA AFTER RETRYING:",
            errors_eta_retry,
            "list of stops erroring AFTER RETRYING:",
            list_stops_error_retry,
        )
        end = datetime.datetime.now()
        print("Time ", end - now)


if __name__ == "__main__":
    config = read_settings("/home/jfog/proyects/inesdata-mov/data-generation/jfog-newconfig.yaml")

    asyncio.run(main(config))
