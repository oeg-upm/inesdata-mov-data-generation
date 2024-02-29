import asyncio
import os
from minio import Minio
from pathlib import Path

import aiofiles.os
import yaml
from aiobotocore.session import get_session
from minio.error import S3Error

from inesdata_mov_datasets.settings import Settings


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


async def list_objs(client, bucket, prefix):
    paginator = client.get_paginator("list_objects")
    keys = []
    async for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for c in result.get("Contents", []):
            keys.append(c.get("Key"))

    return keys


async def get_obj(client, bucket, key):
    resp = await client.get_object(Bucket=bucket, Key=key)
    obj = await resp["Body"].read()
    return obj


async def download_obj(client, bucket, key, output_path):
    await aiofiles.os.makedirs(os.path.dirname(os.path.join(output_path, key)), exist_ok=True)
    obj = await get_obj(client, bucket, key)

    async with aiofiles.open(os.path.join(output_path, key), "w") as out:
        await out.write(obj.decode())


async def download_objs(
    bucket, prefix, output_path, endpoint_url, aws_access_key_id, aws_secret_access_key
):
    session = get_session()
    async with session.create_client(
        "s3",
        endpoint_url=endpoint_url,
        aws_secret_access_key=aws_secret_access_key,
        aws_access_key_id=aws_access_key_id,
    ) as client:
        keys = await list_objs(client, bucket, prefix)

        tasks = [download_obj(client, bucket, key, output_path) for key in keys]

        await asyncio.gather(*tasks)


def read_settings(path: str) -> Settings:
    """Read settings from yaml file

    Args:
        path (str): path to configuration file.

    Returns:
        Settings: Pydantic object to manage paramters.
    """
    with open(path, "r") as file:
        return Settings(**yaml.safe_load(file))
    

def minio_connection(configuration: Settings) -> Minio:
    """Manage connection to minio server.

    Args:
        config (Settings): Object with the config file.

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

def check_minio_file_exists(minio_client: Minio, bucket_name: str, object_name: str) -> bool:
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