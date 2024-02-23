"""Extract file to get line_detail info from EMT."""
import json
import aiohttp
from aiohttp.client_exceptions import ContentTypeError

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
        f"https://openapi.emtmadrid.es/v1/transport/busemtmad/lines/{line_id}/info/{date}"
    )
    async with session.get(line_detail_url, headers=headers) as response:
        try: 
            return await response.json()
        except ContentTypeError as e:
            print("Error in line_detail call line", line_id)
            return -1