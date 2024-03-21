"""File with utils functions."""
import asyncio
import os
from pathlib import Path
import botocore
from botocore.client import Config as BotoConfig
import aiofiles.os
import yaml
from aiobotocore.session import ClientCreatorContext, get_session
from loguru import logger

from inesdata_mov_datasets.settings import Settings

def list_objs2(client: ClientCreatorContext, bucket: str, prefix: str) -> list:
    """List objects from s3 bucket.

    Args:
        client (ClientCreatorContext): Client with s3 connection.
        bucket (str): Name of the bucket.
        prefix (str): Prefix to list.

    Returns:
        list: List of the objects listed.
    """
    
    session = botocore.session.get_session()
    client = session.create_client(
        "s3",
        endpoint_url="http://dev-data.labs.gmv.com:9000",
        aws_secret_access_key="BScID2EyoF0Cgy9oTSTX!",
        aws_access_key_id="jfog",
        use_ssl=False,
    )

    #s3 = session.resource("s3").Bucket("inesdata-mov")

    
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if len(keys) % 1000 == 0:
            logger.debug(len(keys))
        for c in result.get("Contents", []):
            keys.append(c.get("Key"))

    return keys

def async_download(
    bucket: str,
    prefix: str,
    output_path: str,
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
):
    """Download from minIO a day's raw data of an EMT's endpoint.

    Args:
        bucket (str): bucket name
        prefix (str): path to raw data directory from minio
        output_path (str): local path to store output from minio
        endpoint_url (str): url of minio bucket
        aws_access_key_id (str): minio user
        aws_secret_access_key (str): minio password
    """
    loop = asyncio.new_event_loop()
    loop.run_until_complete(
        download_objs(
            bucket, prefix, output_path, endpoint_url, aws_access_key_id, aws_secret_access_key
        )
    )


async def list_objs(client: ClientCreatorContext, bucket: str, prefix: str) -> list:
    """List objects from s3 bucket.

    Args:
        client (ClientCreatorContext): Client with s3 connection.
        bucket (str): Name of the bucket.
        prefix (str): Prefix to list.

    Returns:
        list: List of the objects listed.
    """
    paginator = client.get_paginator("list_objects")
    keys = []
    async for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for c in result.get("Contents", []):
            keys.append(c.get("Key"))

    return keys


async def get_obj(client: ClientCreatorContext, bucket: str, key: str) -> str:
    """Get an object from s3.

    Args:
        client (ClientCreatorContext): Client with s3 connection.
        bucket (str): Name of the bucket.
        key (str): Object to request.

    Returns:
        str: Content of the object.
    """
    resp = await client.get_object(Bucket=bucket, Key=key)
    obj = await resp["Body"].read()
    return obj


async def download_obj(
    client: ClientCreatorContext, bucket: str, key: str, output_path: str, semaphore=None
):
    """Download object from s3.

    Args:
        client (ClientCreatorContext): Client with s3 connection.
        bucket (str): Name of the bucket.
        key (str): Object to request.
        output_path (str): Local path to store output from minio.
    """
    async with semaphore:
        await aiofiles.os.makedirs(os.path.dirname(os.path.join(output_path, key)), exist_ok=True)
        obj = await get_obj(client, bucket, key)

        async with aiofiles.open(os.path.join(output_path, key), "w") as out:
            await out.write(obj.decode())


async def download_objs(
    bucket: str,
    prefix: str,
    output_path: str,
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
):
    """Download objects from s3.

    Args:
        bucket (str): Bucket name.
        prefix (str): Path to raw data directory from minio.
        output_path (str): Local path to store output from minio.
        endpoint_url (str): Url of minio bucket.
        aws_access_key_id (str): Minio user.
        aws_secret_access_key (str): Minio password.
    """
    session = get_session()
    async with session.create_client(
        "s3",
        endpoint_url=endpoint_url,
        aws_secret_access_key=aws_secret_access_key,
        aws_access_key_id=aws_access_key_id,
    ) as client:
        
        if "/eta" in prefix:
            metadata_path = prefix + "metadata.txt"
            response = await client.get_object(Bucket = bucket, Key = metadata_path)
            async with response['Body'] as stream:
                keys = await stream.read()
                keys = keys.decode('utf-8')

            semaphore = asyncio.BoundedSemaphore(10000)
            keys_list = keys.split('\n')
            tasks = [download_obj(client, bucket, key, output_path, semaphore) for key in keys_list]
            
            '''
            response  = await client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
            list_prefix = [prefix['Prefix'] for prefix in response.get('CommonPrefixes', [])]
            for hour_prefix in list_prefix[:2]:
                logger.debug(f"Hour preffix {hour_prefix}")
                keys = list_objs2(client, bucket, hour_prefix)
                semaphore = asyncio.BoundedSemaphore(10000)
                tasks = [download_obj(client, bucket, key, output_path, semaphore) for key in keys]
            '''

            await asyncio.gather(*tasks)
            
        else:
            keys = list_objs2(client, bucket, prefix)
            semaphore = asyncio.BoundedSemaphore(10000)
            tasks = [download_obj(client, bucket, key, output_path, semaphore) for key in keys]

            await asyncio.gather(*tasks)


async def read_obj(
    bucket: str,
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    object_name: str,
) -> str:
    """Read a single object from s3.

    Args:
        bucket (str): Bucket name.
        endpoint_url (str): Url of minio bucket.
        aws_access_key_id (str): Minio user.
        aws_secret_access_key (str): Minio password.
        object_name (str): Name of the object.

    Returns:
        str: Content from object.
    """
    session = get_session()
    async with session.create_client(
        "s3",
        endpoint_url=endpoint_url,
        aws_secret_access_key=aws_secret_access_key,
        aws_access_key_id=aws_access_key_id,
    ) as client:
        resp = await client.get_object(Bucket=bucket, Key=object_name)
        obj = await resp["Body"].read()
        data_str = obj.decode("utf-8")
        return data_str


async def upload_obj(client: ClientCreatorContext, bucket: str, key: str, object_value: str):
    """Upload an object to s3.

    Args:
        client (ClientCreatorContext): Client with s3 connection.
        bucket (str): Bucket name.
        key (str): Name of the object.
        object_value (str): Content of the object.
    """
    await client.put_object(Bucket=bucket, Key=str(key), Body=object_value.encode("utf-8"))

def upload_metadata(
    bucket: str,
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    keys: list
):
    session = botocore.session.get_session()
    client = session.create_client(
        "s3",
        endpoint_url=endpoint_url,
        aws_secret_access_key=aws_secret_access_key,
        aws_access_key_id=aws_access_key_id,
        use_ssl=False,
    )
    #We get the prefix of the eta bucket from a name of a key
    prefix = "/".join(keys[0].split('/')[:-1]) + '/metadata.txt'
    try:
        response = client.get_object(Bucket=bucket, Key=prefix)
        #we get the previous content of the file
        content = response['Body'].read().decode('utf-8')
        #add the new names of files written
        new_content = content + '\n' + '\n'.join(keys)
    except :
        #if metadata file does not exist
        new_content = '\n'.join(keys)

    # upload s3
    client.put_object(Bucket=bucket, Key=prefix, Body=new_content.encode('utf-8'))
    
async def upload_objs(
    bucket: str,
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    objects_dict: dict,
):
    """Upload objects to s3.

    Args:
        bucket (str): Bucket name.
        endpoint_url (str): Url of minio bucket.
        aws_access_key_id (str): Minio user.
        aws_secret_access_key (str): Minio password.
        objects_dict (dict): Dict ofobjects to upload.
    """
    session = get_session()
    async with session.create_client(
        "s3",
        endpoint_url=endpoint_url,
        aws_secret_access_key=aws_secret_access_key,
        aws_access_key_id=aws_access_key_id,
    ) as client:
        keys = objects_dict.keys()
        tasks = [upload_obj(client, bucket, key, objects_dict[key]) for key in keys]
        await asyncio.gather(*tasks)


def read_settings(path: str) -> Settings:
    """Read settings from yaml file.

    Args:
        path (str): path to configuration file.

    Returns:
        Settings: Pydantic object to manage paramters.
    """
    with open(path, "r") as file:
        return Settings(**yaml.safe_load(file))


def check_local_file_exists(path_dir: Path, object_name: str) -> bool:
    """Check if a local file exists.

    Args:
        path_dir (str): Dir path of the file.
        object_name (str): Object name of the file.

    Returns:
        bool: True if file exists, False otherwise
    """
    # Create a Path object for the file
    file_path = Path(path_dir) / object_name

    # Check if the file exists
    if file_path.exists():
        return True
    else:
        return False


async def check_s3_file_exists(
    endpoint_url: str,
    aws_secret_access_key: str,
    aws_access_key_id: str,
    bucket_name: str,
    object_name: str,
) -> bool:
    """Check if a file exists in an S3 bucket.

    Args:
        endpoint_url (str): The endpoint URL of the S3 service.
        aws_secret_access_key (str): The AWS secret access key.
        aws_access_key_id (str): The AWS access key ID.
        bucket_name (str): Bucket name.
        object_name (str): Object name.

    Returns:
        bool: True if file is detected, False otherwise.
    """
    session = get_session()
    async with session.create_client(
        "s3",
        endpoint_url=endpoint_url,
        aws_secret_access_key=aws_secret_access_key,
        aws_access_key_id=aws_access_key_id,
    ) as client:
        try:
            await client.head_object(Bucket=bucket_name, Key=object_name)
            return True
        except:
            return False
