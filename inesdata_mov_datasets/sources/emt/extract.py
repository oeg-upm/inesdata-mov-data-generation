"""Gather raw data from emt."""

import asyncio
import datetime
import io
import json

import aiohttp
import yaml
from minio import Minio
from minio.error import S3Error


def load_configuration(path: str) -> dict:
    """Load config file.

    Args:
        path (str): path of the config file

    Returns:
        dict: with the config file loaded
    """
    with open(path, "r") as file:
        configuration = yaml.safe_load(file)
    return configuration


def minio_connection(configuration: dict) -> Minio:
    """Manage connection to minio server.

    Args:
        configuration (dict): dictionary with the configuration of the proyect 

    Returns:
        Minio: object with the MinIO client
    """
    minio_client = Minio(
        configuration["minio"]["endpoint"],
        access_key=configuration["minio"]["access_key"],
        secret_key=configuration["minio"]["secret_key"],
        region="us-east-1",
        secure=configuration["minio"]["secure"],
    )
    return minio_client


async def get_eta(
    session: aiohttp, url: str, stop_id: str, headers: json, body: json
):
    """Make the API call to ETA endpoint.

    Args:
        session (aiohttp.ClientSession): call session to make faster the calls to the same endpoint
        url (str): url of the endpoint
        stop_id (str): id of the bus stop
        headers (json): headers of the http call
        body (json): body of the post petition

    Returns:
        _type_: response of the petition in json format
    """
    eta_url = url.format(stopId=stop_id)
    async with session.post(eta_url, headers=headers, json=body) as response:
        return await response.json()


async def get_calendar(
    session: aiohttp, url: str, startDate: str, endDate: str, headers: json,
):
    calendar_url = url.format(start=startDate, end=endDate)
    async with session.get(calendar_url, headers=headers) as response:
        return await response.json()
    

async def get_lineDetail(
    session: aiohttp, url: str, date: str, stop_id: str, headers: json,
):
    lineDetail_url = url.format(stopId=stop_id, date=date)
    async with session.get(lineDetail_url, headers=headers) as response:
        return await response.json()


def check_file_exists(minio_client, bucket_name, object_name):
    try:
        minio_client.stat_object(bucket_name, object_name)
        return True
    except S3Error as e:
        if e.code == "NoSuchKey":
            return False
        else:
            raise e


async def main():
    config = load_configuration("/home/jfog/proyects/inesdata-mov/data-generation/config.yaml")
    minio_client = minio_connection(config)
    access_token = "36306b78-ca5f-11ee-b928-8688096f1cdc"
    headers = {
        "accessToken": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    body = {
        "cultureInfo": "ES",
        "Text_StopRequired_YN": "N",
        "Text_EstimationsRequired_YN": "Y",
        "Text_IncidencesRequired_YN": "N",
    }
    current_datetime = datetime.datetime.now().replace(second=0)

    bucket_name = "inesdata-mov"


    formatted_date = current_datetime.strftime("%Y-%m-%dT%H:%M")
    formatted_date_day = current_datetime.strftime("%Y%m%d")
    formatted_date_slash = current_datetime.strftime("%Y/%m/%d")
    
    object_calendar_name = f"/raw/emt/{formatted_date_slash}/calendar/calendar_{formatted_date_day}.json"
    
    async with aiohttp.ClientSession() as session:
        # Lista para almacenar las tareas de manera asíncrona
        calendar_tasks = []
        eta_tasks = []
        lineDetail_tasks = []
        first_stop = config["emt"]["stops"]["plazaCastilla"][0]
        
        # Realizar solicitud al endpoint del lineDetail comprobando que no se ha hecho la peticion aun en el dia de hoy
        first_object_lineDetail_name = f"/raw/emt/{formatted_date_slash}/lineDetail/lineDetail_{first_stop}_{formatted_date_day}.json"
        if not check_file_exists(minio_client, bucket_name, first_object_lineDetail_name):
            for stop_id in config["emt"]["stops"]["plazaCastilla"]:
                lineDetail_task = asyncio.ensure_future(
                    get_lineDetail(session, config["emt"]["endpoints"]["lineDetail"], formatted_date_day, stop_id, headers)
                )
                lineDetail_tasks.append(lineDetail_task)
        else:
            print("ya llamé a LineDetail")
        
        # Realizar solicitud al endpoint del calendario
        if not check_file_exists(minio_client, bucket_name, object_calendar_name):
            calendar_task = asyncio.ensure_future(
                get_calendar(session, config["emt"]["endpoints"]["calendar"], formatted_date_day, formatted_date_day, headers)
            )
            calendar_tasks.append(calendar_task)
            
        else:
            print("ya llamé a Calendar")


        # Realizar solicitudes al eta de cada parada
        for stop_id in config["emt"]["stops"]["plazaCastilla"]:
            eta_task = asyncio.ensure_future(
                get_eta(session, config["emt"]["endpoints"]["etaUrl"], stop_id, headers, body)
            )
            eta_tasks.append(eta_task)

        # Esperar a que todas las tareas se completen
        calendar_response, *eta_and_lineDetail_responses = await asyncio.gather(*calendar_tasks, *eta_tasks, *lineDetail_tasks)

        # Separar las respuestas de ETA y LineDetail
        eta_responses = eta_and_lineDetail_responses[:len(eta_tasks)]
        lineDetail_responses = eta_and_lineDetail_responses[len(eta_tasks):]

        if lineDetail_responses:
            for stop_id, response in zip(config["emt"]["stops"]["plazaCastilla"], lineDetail_responses):
                object_lineDetail_name = f"/raw/emt/{formatted_date_slash}/lineDetail/lineDetail_{stop_id}_{formatted_date_day}.json"
                try:
                    response_json_str = json.dumps(response)
                    minio_client.put_object(
                        bucket_name,
                        object_lineDetail_name,
                        io.BytesIO(response_json_str.encode("utf-8")),
                        len(response_json_str),
                    )
                except:
                    print("Error parada ",stop_id, "Line detail ")


                
        # Almacenar la respuesta del calendario en MinIO si está presente
        if calendar_response:
            calendar_json_str = json.dumps(calendar_response)
            minio_client.put_object(
                bucket_name,
                object_calendar_name,
                io.BytesIO(calendar_json_str.encode("utf-8")),
                len(calendar_json_str),
            )

        # Almacenar las respuestas de las paradas de autobús en MinIO
        for stop_id, response in zip(config["emt"]["stops"]["plazaCastilla"], eta_responses):
            object_eta_name = f"/raw/emt/{formatted_date_slash}/eta/eta_{stop_id}_{formatted_date}.json"
            
            try:
                
                response_json_str = json.dumps(response)

                minio_client.put_object(
                    bucket_name,
                    object_eta_name,
                    io.BytesIO(response_json_str.encode("utf-8")),
                    len(response_json_str),
                )

            except:
                print("Error parada ",stop_id, "Time arrival ")
    


if __name__ == "__main__":
    config = load_configuration("/home/jfog/proyects/inesdata-mov/data-generation/config.yaml")
    '''
    minio_client = minio_connection(config)
    print(minio_client)
    '''
    asyncio.run(main())
