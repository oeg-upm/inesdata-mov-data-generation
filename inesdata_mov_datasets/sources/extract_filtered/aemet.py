"""Gather raw data from aemet."""

import traceback

import requests
from loguru import logger

from inesdata_mov_datasets.settings import Settings


def get_filter_aemet(config: Settings):
    """Request aemet API to get data from Madrid weather.

    Args:
        config (Settings): Object with the config file.
    """
    try:
        url_madrid = (
            "https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/28079"
        )

        headers = {
            "api_key": config.sources.aemet.credentials.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        r = requests.get(url_madrid, headers=headers)
        r_json = requests.get(r.json()["datos"]).json()

        logger.info("Extracted AEMET")

        return r_json

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
