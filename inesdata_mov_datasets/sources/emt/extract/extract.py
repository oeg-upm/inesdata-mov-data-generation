"""Extract raw data from emt."""
import asyncio
import datetime
import io
import json
import os
from pathlib import Path

import aiohttp
import pytz
import requests
from minio import Minio

from inesdata_mov_datasets.settings import Settings
from inesdata_mov_datasets.sources.emt.extract.calendar.extract import get_calendar
from inesdata_mov_datasets.sources.emt.extract.eta.extract import get_eta
from inesdata_mov_datasets.sources.emt.extract.line_detail.extract import get_line_detail
from inesdata_mov_datasets.utils import (
    check_local_file_exists,
    check_minio_file_exists,
)


def login_emt(
    config: Settings, object_login_name: str, minio_client: Minio = None, local_path: Path = None
) -> str:
    """Make the call to Login endpoint EMT.

    Args:
        config (Settings): Object with the config file.
        minio_client (Minio, optional): Object with Minio connection. Defaults to None.
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
        "https://openapi.emtmadrid.es/v2/mobilitylabs/user/login/", headers=headers, verify=False
    )
    login_json = r.json()
    login_json_str = json.dumps(login_json)

    token = login_json["data"][0]["accessToken"]

    if config.storage.default == "minio":
        minio_client.put_object(
            config.storage.config.minio.bucket,
            str(object_login_name),
            io.BytesIO(login_json_str.encode("utf-8")),
            len(login_json_str),
        )

    if config.storage.default == "local":
        os.makedirs(local_path, exist_ok=True)
        with open(os.path.join(local_path, object_login_name), "w") as file:
            file.write(login_json_str)

    return token


def token_control(
    config: Settings, date_slash: str, date_day: str, minio_client: Minio = None
) -> str:
    """Get existing token from EMT API or regenerate it if its deprecated.

    Args:
        config (Settings): Object with the config file.
        minio_client (Minio, optional): Object with Minio connection. Defaults to None.
        date_slash (str): date format for object name
        date_day (str): date format for object name

    Returns:
       str: Token from EMT Login.
    """
    if minio_client:
        object_login_name = Path("raw") / "emt" / date_slash / "login" / f"login_{date_day}.json"
        if not check_minio_file_exists(
            minio_client, config.storage.config.minio.bucket, str(object_login_name)
        ):
            token = login_emt(config, object_login_name, minio_client=minio_client)
            return token

        else:
            response = minio_client.get_object(
                bucket_name=config.storage.config.minio.bucket,
                object_name=str(object_login_name),
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

    else:
        dir_path = Path(config.storage.config.local.path) / "raw" / "emt" / date_slash / "login"
        object_login_name = f"login_{date_day}.json"
        if not check_local_file_exists(dir_path, object_login_name):
            token = login_emt(config, object_login_name, local_path=dir_path)
            return token

        else:
            with open(os.path.join(dir_path, object_login_name), "r") as file:
                response = file.read()
                data = json.loads(response)
                token = data["data"][0]["accessToken"]
                expiration_date_unix = data["data"][0]["tokenDteExpiration"]["$date"]

                expiration_date = datetime.datetime.utcfromtimestamp(
                    expiration_date_unix / 1000
                )  #  miliseconds to seconds
                now = datetime.datetime.now()
                # Compare
                if now >= expiration_date:  # reset token
                    token = login_emt(config, object_login_name, local_path=dir_path)
                    return token

                elif now < expiration_date:
                    return token


async def get_emt(config: Settings, minio_client: Minio = None):
    """Get all the data from EMT endpoints.

    Args:
        config (Settings): Object with the config file..
        minio_client (Minio, optional): Object with Minio connection. Defaults to None.
    """
    
    print("EXTRACTING EMT")
    europe_timezone = pytz.timezone("Europe/Madrid")
    current_datetime = datetime.datetime.now(europe_timezone).replace(second=0)
    formatted_date = current_datetime.strftime(
        "%Y-%m-%dT%H%M"
    )  # formatted date according to ISO86001 without seconds
    formatted_date_day = current_datetime.strftime(
        "%Y%m%d"
    )  # formatted date year|month|day all together
    formatted_date_slash = current_datetime.strftime(
        "%Y/%m/%d"
    )  # formatted date year/month/day for storage in Minio

    now = datetime.datetime.now()

    access_token = token_control(
        config, formatted_date_slash, formatted_date_day, minio_client=minio_client
    )  # Obtain token from EMT

    # Headers for requests to the EMT API
    headers = {
        "accessToken": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    bucket_name = config.storage.config.minio.bucket

    async with aiohttp.ClientSession() as session:
        # List to store tasks asynchronously
        calendar_tasks = []
        eta_tasks = []
        line_detail_tasks = []

        # Make request to the line_detail endpoint checking if the request has not been made today
        lines_called = 0
        lines_not_called = []
        for line_id in config.sources.emt.lines:
            if config.storage.default == "minio":
                object_line_detail_name = (
                    Path("raw")
                    / "emt"
                    / formatted_date_slash
                    / "line_detail"
                    / f"line_detail_{line_id}_{formatted_date_day}.json"
                )
                if not check_minio_file_exists(
                    minio_client, bucket_name, str(object_line_detail_name)
                ):
                    line_detail_task = asyncio.ensure_future(
                        get_line_detail(session, formatted_date_day, line_id, headers)
                    )
                    line_detail_tasks.append(line_detail_task)
                    lines_not_called.append(line_id)
                else:
                    lines_called += 1

            elif config.storage.default == "local":
                object_line_detail_name = f"line_detail_{line_id}_{formatted_date_day}.json"
                path_dir_line_detail = (
                    Path(config.storage.config.local.path)
                    / "raw"
                    / "emt"
                    / formatted_date_slash
                    / "line_detail"
                )
                if not check_local_file_exists(path_dir_line_detail, object_line_detail_name):
                    line_detail_task = asyncio.ensure_future(
                        get_line_detail(session, formatted_date_day, line_id, headers)
                    )
                    line_detail_tasks.append(line_detail_task)
                    lines_not_called.append(line_id)
                else:
                    lines_called += 1

        print("Already called " + str(lines_called) + "lines")

        if config.storage.default == "minio":
            object_calendar_name = (
                Path("raw")
                / "emt"
                / formatted_date_slash
                / "calendar"
                / f"calendar_{formatted_date_day}.json"
            )
            # Make request to the calendar endpoint
            if not check_minio_file_exists(minio_client, bucket_name, str(object_calendar_name)):
                calendar_task = asyncio.ensure_future(
                    get_calendar(session, formatted_date_day, formatted_date_day, headers)
                )
                calendar_tasks.append(calendar_task)
            else:
                print("Already called Calendar")

        if config.storage.default == "local":
            object_calendar_name = f"calendar_{formatted_date_day}.json"
            path_dir_calendar = (
                Path(config.storage.config.local.path)
                / "raw"
                / "emt"
                / formatted_date_slash
                / "calendar"
            )

            if not check_local_file_exists(path_dir_calendar, object_calendar_name):
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
            for line_id, response in zip(lines_not_called, line_detail_responses):
                try:
                    response_json_str = json.dumps(response)
                    if response["code"] == "00":
                        if config.storage.default == "minio":
                            object_line_detail_name = (
                                Path("raw")
                                / "emt"
                                / formatted_date_slash
                                / "line_detail"
                                / f"line_detail_{line_id}_{formatted_date_day}.json"
                            )
                            minio_client.put_object(
                                bucket_name,
                                str(object_line_detail_name),
                                io.BytesIO(response_json_str.encode("utf-8")),
                                len(response_json_str),
                            )
                        if config.storage.default == "local":
                            object_line_detail_name = (
                                f"line_detail_{line_id}_{formatted_date_day}.json"
                            )
                            os.makedirs(path_dir_line_detail, exist_ok=True)
                            with open(
                                os.path.join(path_dir_line_detail, object_line_detail_name), "w"
                            ) as file:
                                file.write(response_json_str)
                    else:
                        errors_ld += 1
                except:
                    print("Error for line ", line_id, "Line detail ")

        # Store the calendar response in MinIO if present
        if calendar_response:
            try:
                calendar_json_str = json.dumps(calendar_response)
                if calendar_response[0]["code"] == "00":
                    if config.storage.default == "minio":
                        minio_client.put_object(
                            bucket_name,
                            str(object_calendar_name),
                            io.BytesIO(calendar_json_str.encode("utf-8")),
                            len(calendar_json_str),
                        )
                    if config.storage.default == "local":
                        os.makedirs(path_dir_calendar, exist_ok=True)
                        with open(
                            os.path.join(path_dir_calendar, object_calendar_name), "w"
                        ) as file:
                            file.write(calendar_json_str)
                else:
                    print("Error in calendar")
            except:
                print("Error for calendar endpoint")

        # Store the bus stop responses in MinIO
        list_stops_error = []
        for stop_id, response in zip(config.sources.emt.stops, eta_responses):
            try:
                response_json_str = json.dumps(response)
                if response["code"] == "00":
                    if config.storage.default == "minio":
                        object_eta_name = (
                            Path("raw")
                            / "emt"
                            / formatted_date_slash
                            / "eta"
                            / f"eta_{stop_id}_{formatted_date}.json"
                        )
                        minio_client.put_object(
                            bucket_name,
                            str(object_eta_name),
                            io.BytesIO(response_json_str.encode("utf-8")),
                            len(response_json_str),
                        )
                    if config.storage.default == "local":
                        object_eta_name = f"eta_{stop_id}_{formatted_date}.json"
                        path_dir_eta = (
                            Path(config.storage.config.local.path)
                            / "raw"
                            / "emt"
                            / formatted_date_slash
                            / "eta"
                        )
                        os.makedirs(path_dir_eta, exist_ok=True)
                        with open(os.path.join(path_dir_eta, object_eta_name), "w") as file:
                            file.write(response_json_str)

                else:  # 200 CODE BUT ERROR IN RESPONSE JSON
                    errors_eta += 1
                    list_stops_error.append(stop_id)

            except:
                errors_eta += 1
                list_stops_error.append(stop_id)
                print("Error for stop ", stop_id, "Time arrival")

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
                try:
                    response_json_str = json.dumps(response)
                    if response == -1:
                        errors_eta_retry += 1
                        list_stops_error_retry.append(stop_id)
                        continue

                    if response["code"] == "00":
                        if config.storage.default == "minio":
                            object_eta_name = (
                                Path("raw")
                                / "emt"
                                / formatted_date_slash
                                / "eta"
                                / f"eta_{stop_id}_{formatted_date}.json"
                            )

                            minio_client.put_object(
                                bucket_name,
                                str(object_eta_name),
                                io.BytesIO(response_json_str.encode("utf-8")),
                                len(response_json_str),
                            )
                        if config.storage.default == "local":
                            object_eta_name = f"eta_{stop_id}_{formatted_date}.json"
                            path_dir_eta = (
                                Path(config.storage.config.local.path)
                                / "raw"
                                / "emt"
                                / formatted_date_slash
                                / "eta"
                            )
                            os.makedirs(path_dir_eta, exist_ok=True)
                            with open(os.path.join(path_dir_eta, object_eta_name), "w") as file:
                                file.write(response_json_str)

                    else:
                        errors_eta_retry += 1
                        list_stops_error_retry.append(stop_id)

                except:
                    print("Error for stop ", stop_id, "Time arrival ")
                    errors_eta_retry += 1
                    list_stops_error_retry.append(stop_id)

        print(
            "Errors in ETA AFTER RETRYING:",
            errors_eta_retry,
            "list of stops erroring AFTER RETRYING:",
            list_stops_error_retry,
        )
        end = datetime.datetime.now()
        print("Time ", end - now)
        print("EXTRACTED EMT")
        print("- - - - - - -")
