import asyncio
import os

import aiofiles.os
import yaml
from aiobotocore.session import get_session

from inesdata_mov_datasets.settings import Settings


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
