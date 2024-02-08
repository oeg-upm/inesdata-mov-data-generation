# File that calls EMT endpoints asynchronously and stores the output.
import asyncio
import datetime
import io
import json

import aiohttp
import yaml
from minio import Minio


def load_configuration(path:str) -> dict:
    """Function that loads config file

    Args:
        path (str): path of the config file

    Returns:
        _type_: dict with the config file loaded
    """
    with open(path, 'r') as file:
        configuration = yaml.safe_load(file)
    return configuration


def minio_connection(configuration:str) -> Minio:
    """Manage connection to minio server.

    Args:
        configuration (str): _description_

    Returns:
        Minio: _description_
    """
    minio_client = Minio(
        configuration['minio']['endpoint'],
        access_key=configuration['minio']['access_key'],
        secret_key=configuration['minio']['secret_key'],
        region='us-east-1',
        secure=configuration['minio']['secure']
    )
    return minio_client

async def get_eta(session:aiohttp.ClientSession(), url:str, stop_id:str, headers:json, body:json):
    """Async function that makes the API call to ETA endpoint.

    Args:
        session (aiohttp.ClientSession): call session to make faster the calls to the same endpoint
        url (str): url of the endpoint
        stop_id (str): id of the bus stop
        headers (json): _description_
        body (json): _description_

    Returns:
        _type_: _description_
    """
    eta_url = url.format(stopId=stop_id)
    async with session.post(eta_url, headers=headers, json=body) as response:
        return await response.json()




async def main():
    config = load_configuration("/home/jfog/proyects/inesdata-mov/data-generation/config.yaml")
    minio_client = minio_connection(config)
    access_token = "2190d854-c5ca-11ee-8388-0a663bdb57f9"
    headers = {
        "accessToken": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    body = {
        "cultureInfo": "ES",
        "Text_StopRequired_YN": "N",
        "Text_EstimationsRequired_YN": "Y",
        "Text_IncidencesRequired_YN": "N"
    }

    current_datetime = datetime.datetime.now().replace(second=0)

    bucket_name = "inesdata-mov"
    object_name_prefix = "eta"

    formatted_date = current_datetime.strftime("%Y-%m-%dT%H:%M")


    async with aiohttp.ClientSession() as session:
        tasks = []
        for stop_id in config["emt"]["stops"]["plazaCastilla"]:
            task = asyncio.ensure_future(get_eta(session, config["emt"]["endpoints"]["etaUrl"], stop_id, headers, body))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        for stop_id, response in zip(config["emt"]["stops"]["plazaCastilla"], responses):
            # Construye el nombre del objeto utilizando el prefijo, el ID de la parada y la fecha formateada
            object_name = f"/raw/emt/2024/02/08/eta/{object_name_prefix}_{stop_id}_{formatted_date}.json"

            # Convierte la respuesta JSON a una cadena JSON
            response_json_str = json.dumps(response)

            # Carga la cadena JSON en MinIO
            #minio_client.put_object(bucket_name, object_name, io.BytesIO(response_json_str.encode('utf-8')), len(response_json_str))


if __name__ == "__main__":
    config = load_configuration("/home/jfog/proyects/inesdata-mov/data-generation/config.yaml")
    '''
    minio_client = minio_connection(config)
    print(minio_client)
    '''
    asyncio.run(main())
