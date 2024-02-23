"""Extract file to get calendar info from EMT."""
import json

import aiohttp
from aiohttp.client_exceptions import ContentTypeError


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
        except ContentTypeError:
            print("Error in calendar call")
            return -1
