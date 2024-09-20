"""Gather raw data from Informo."""

import sys
import traceback

import requests
import xmltodict
from loguru import logger

sys.path.append("/home/mlia/proyectos/data-generation/")

# from inesdata_mov_datasets.handlers.logger import instantiate_logger
from inesdata_mov_datasets.settings import Settings


async def get_filter_informo(config: Settings):
    """Request informo API to get data from Madrid traffic.

    Args:
        config (Settings): Object with the config file.
    """
    try:
        url_informo = "https://informo.madrid.es/informo/tmadrid/pm.xml"

        r = requests.get(url_informo)

        # Parse XML
        xml_dict = xmltodict.parse(r.content)
        # await save_informo(config, xml_dict)

        logger.info("Extracted INFORMO")

        return xml_dict["pms"]

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
