"""Extract raw data from emt."""
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

from inesdata_mov_datasets.handlers.logger import import_extract_logger
from inesdata_mov_datasets.settings import Settings
from inesdata_mov_datasets.utils import (
    check_local_file_exists,
    check_s3_file_exists,
    read_obj,
    upload_objs,
)


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


async def get_eta(session: aiohttp, stop_id: str, headers: json) -> json:
    """Make the API call to ETA endpoint.

    Args:
        session (aiohttp): Call session to make faster the calls to the same API.
        stop_id (str): Id of the bus stop.
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
    eta_url = f"https://openapi.emtmadrid.es/v2/transport/busemtmad/stops/{stop_id}/arrives/"
    async with session.post(eta_url, headers=headers, json=body) as response:
        try:
            return await response.json()
        except Exception as e:
            logger.error(f"Error in ETA call stop {stop_id} to the server")
            logger.error(e)
            return {"code": -1}


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
            
            #Catching an error that ocurrs when the server doesnt provide the token expiration
            try:
                expiration_date_unix = data["data"][0]["tokenDteExpiration"]["$date"]
            except:
                logger.error(f"Error saving time expiration from login. Solving the problem retrying the call.")
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
                
                #Catching an error that ocurrs when the server doesnt provide the token expiration
                try:
                    expiration_date_unix = data["data"][0]["tokenDteExpiration"]["$date"]
                except:
                    logger.error(f"Error saving time expiration from login. Solving the problem retrying the call.")
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


async def get_emt(config: Settings):
    """Get all the data from EMT endpoints.

    Args:
        config (Settings): Object with the config file..
    """
    try:
        # Logger
        import_extract_logger(config, "EMT")
        logger.info("Extracting EMT")

        # Get the timezone from Madrid and formated the dates for the object_name of the files
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
                    # If the files are not saved, append the task of the line_detail request
                    if not await check_s3_file_exists(
                        endpoint_url=config.storage.config.minio.endpoint,
                        aws_secret_access_key=config.storage.config.minio.secret_key,
                        aws_access_key_id=config.storage.config.minio.access_key,
                        bucket_name=config.storage.config.minio.bucket,
                        object_name=str(object_line_detail_name),
                    ):
                        line_detail_task = asyncio.ensure_future(
                            get_line_detail(session, formatted_date_day, line_id, headers)
                        )
                        line_detail_tasks.append(line_detail_task)
                        lines_not_called.append(line_id)

                    # line already called
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
                    # If the files are not saved, append the task of the line_detail request
                    if not check_local_file_exists(path_dir_line_detail, object_line_detail_name):
                        line_detail_task = asyncio.ensure_future(
                            get_line_detail(session, formatted_date_day, line_id, headers)
                        )
                        line_detail_tasks.append(line_detail_task)
                        lines_not_called.append(line_id)

                    # line already called
                    else:
                        lines_called += 1

            logger.debug(f"Already called {lines_called} lines")

            # Calendar endpoint task
            if config.storage.default == "minio":
                object_calendar_name = (
                    Path("raw")
                    / "emt"
                    / formatted_date_slash
                    / "calendar"
                    / f"calendar_{formatted_date_day}.json"
                )
                # If the file are not saved, append the task of the calendar request
                if not await check_s3_file_exists(
                    endpoint_url=config.storage.config.minio.endpoint,
                    aws_secret_access_key=config.storage.config.minio.secret_key,
                    aws_access_key_id=config.storage.config.minio.access_key,
                    bucket_name=config.storage.config.minio.bucket,
                    object_name=str(object_calendar_name),
                ):
                    calendar_task = asyncio.ensure_future(
                        get_calendar(session, formatted_date_day, formatted_date_day, headers)
                    )
                    calendar_tasks.append(calendar_task)
                else:
                    logger.debug("Already called Calendar")

            if config.storage.default == "local":
                object_calendar_name = f"calendar_{formatted_date_day}.json"
                path_dir_calendar = (
                    Path(config.storage.config.local.path)
                    / "raw"
                    / "emt"
                    / formatted_date_slash
                    / "calendar"
                )
                # If the file are not saved, append the task of the calendar request
                if not check_local_file_exists(path_dir_calendar, object_calendar_name):
                    calendar_task = asyncio.ensure_future(
                        get_calendar(session, formatted_date_day, formatted_date_day, headers)
                    )
                    calendar_tasks.append(calendar_task)
                else:
                    logger.debug("Already called Calendar")

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
                line_detail_dict_upload = {}
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
                                # Add to the dict the good responses
                                line_detail_dict_upload[
                                    object_line_detail_name
                                ] = response_json_str

                            if config.storage.default == "local":
                                object_line_detail_name = (
                                    f"line_detail_{line_id}_{formatted_date_day}.json"
                                )
                                os.makedirs(path_dir_line_detail, exist_ok=True)
                                with open(
                                    os.path.join(path_dir_line_detail, object_line_detail_name),
                                    "w",
                                ) as file:
                                    file.write(response_json_str)
                        else:
                            errors_ld += 1
                            logger.error(f"Error code {response['code']} in line {line_id} in line_detail")
                    except Exception as e:
                        logger.error(e)

                # Upload the dict to s3 asynchronously if dict contains something (This means minio flag in convig was enabled)
                if line_detail_dict_upload:
                    await upload_objs(
                        config.storage.config.minio.bucket,
                        config.storage.config.minio.endpoint,
                        config.storage.config.minio.access_key,
                        config.storage.config.minio.secret_key,
                        line_detail_dict_upload,
                    )

            # Store the calendar response if present
            if calendar_response:
                calendar_dict_upload = {}
                try:
                    calendar_json_str = json.dumps(calendar_response)
                    if calendar_response[0]["code"] == "00":
                        if config.storage.default == "minio":
                            calendar_dict_upload[str(object_calendar_name)] = calendar_json_str

                            # Upload to s3 asynchronously
                            await upload_objs(
                                config.storage.config.minio.bucket,
                                config.storage.config.minio.endpoint,
                                config.storage.config.minio.access_key,
                                config.storage.config.minio.secret_key,
                                calendar_dict_upload,
                            )
                        if config.storage.default == "local":
                            os.makedirs(path_dir_calendar, exist_ok=True)
                            with open(
                                os.path.join(path_dir_calendar, object_calendar_name), "w"
                            ) as file:
                                file.write(calendar_json_str)
                    else:
                        logger.error(f"Error code {response['code']} in calendar")
                except Exception as e:
                    logger.error(e)
                    logger.error(traceback.format_exc())

            # Store the bus stop responses in MinIO
            list_stops_error = []
            eta_dict_upload = {}
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
                            eta_dict_upload[object_eta_name] = response_json_str

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
                        logger.error(f"Error code {response['code']} in stop {stop_id} in EMT ETA")

                except Exception as e:
                    errors_eta += 1
                    list_stops_error.append(stop_id)
                    logger.error(e)
                    logger.error(traceback.format_exc())

            # Upload the dict to s3 asynchronously if dict contains something (This means minio flag in convig was enabled)
            if eta_dict_upload:
                await upload_objs(
                    config.storage.config.minio.bucket,
                    config.storage.config.minio.endpoint,
                    config.storage.config.minio.access_key,
                    config.storage.config.minio.secret_key,
                    eta_dict_upload,
                )

            logger.error(f"{errors_ld} errors in Line Detail")
            logger.error(f"{errors_eta} errors in ETA, list of stops erroring: {list_stops_error}")

            # Retry the failed petitions
            if errors_eta > 0:
                eta_dict_upload = {}
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
                                eta_dict_upload[object_eta_name] = response_json_str

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
                                with open(
                                    os.path.join(path_dir_eta, object_eta_name), "w"
                                ) as file:
                                    file.write(response_json_str)

                        else:
                            errors_eta_retry += 1
                            list_stops_error_retry.append(stop_id)
                            logger.error(f"Error code {response['code']} in stop {stop_id} in ETA after retrying")
                    except Exception as e:
                        errors_eta_retry += 1
                        list_stops_error_retry.append(stop_id)
                        logger.error(e)
                        logger.error(traceback.format_exc())
                logger.error(
                    f"{errors_eta_retry} errors in ETA after retrying, "
                    + f"list of stops erroring after retrying:, {list_stops_error_retry}"
                )

            # Upload the dict to s3 asynchronously if dict contains something (This means minio flag in convig was enabled)
            if eta_dict_upload:
                await upload_objs(
                    config.storage.config.minio.bucket,
                    config.storage.config.minio.endpoint,
                    config.storage.config.minio.access_key,
                    config.storage.config.minio.secret_key,
                    eta_dict_upload,
                )

            end = datetime.datetime.now()
            logger.debug(f"Time duration of EMT extraction {end - now}")
            logger.info("Extracted EMT")
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
