"""Extract file to get time arrival bus info from EMT."""
import json

import aiohttp
from aiohttp.client_exceptions import ContentTypeError


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
        except ContentTypeError:
            #print("Error in ETA call stop", stop_id)
            return -1
