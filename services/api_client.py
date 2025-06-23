
import aiohttp
import asyncio
import json

from typing import Literal, get_args
from utils.logger.logger import AsyncLogger

logger = AsyncLogger()

class APIClient:
    __API = "https://ogcapi.bdl.lasy.gov.pl/collections/"
    __ENDPOINTS = Literal[
        'all',
        'rdlp',
        'nadlesnictwa',
        'lesnictwa',
        'RDLP_Bialystok_wydzielenia',
        'RDLP_Katowice_wydzielenia',
        'RDLP_Krakow_wydzielenia',
        'RDLP_Krosno_wydzielenia',
        'RDLP_Lublin_wydzielenia',
        'RDLP_Lodz_wydzielenia',
        'RDLP_Olsztyn_wydzielenia',
        'RDLP_Pila_wydzielenia',
        'RDLP_Poznan_wydzielenia',
        'RDLP_Szczecin_wydzielenia',
        'RDLP_Szczecinek_wydzielenia',
        'RDLP_Torun_wydzielenia',
        'RDLP_Wroclaw_wydzielenia',
        'RDLP_Zielona_Gora_wydzielenia',
        'RDLP_Gdansk_wydzielenia',
        'RDLP_Radom_wydzielenia',
        'RDLP_Warszawa_wydzielenia'
    ]
    __ENDPOINTS_LIST = [
        'rdlp',
        'nadlesnictwa',
        'lesnictwa',
        'RDLP_Bialystok_wydzielenia',
        'RDLP_Katowice_wydzielenia',
        'RDLP_Krakow_wydzielenia',
        'RDLP_Krosno_wydzielenia',
        'RDLP_Lublin_wydzielenia',
        'RDLP_Lodz_wydzielenia',
        'RDLP_Olsztyn_wydzielenia',
        'RDLP_Pila_wydzielenia',
        'RDLP_Poznan_wydzielenia',
        'RDLP_Szczecin_wydzielenia',
        'RDLP_Szczecinek_wydzielenia',
        'RDLP_Torun_wydzielenia',
        'RDLP_Wroclaw_wydzielenia',
        'RDLP_Zielona_Gora_wydzielenia',
        'RDLP_Gdansk_wydzielenia',
        'RDLP_Radom_wydzielenia',
        'RDLP_Warszawa_wydzielenia'
    ]

    async def __fetch_data_page(self, session: aiohttp.ClientSession, url: str):
        logger.log("INFO", f"Fetching data from {url}")
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                logger.log("ERROR", f"Failed to fetch data from {url}: {response.status}")
                return {}


    @logger.log_time_exec
    async def __fetch_everything(self, session: aiohttp.ClientSession, endpoint: str, limit: int = 1000):
        data = []
        # fetch first page of data and extract total number of items from response json
        request_url = f"{self.__API}{endpoint}/items"
        initial = await self.__fetch_data_page(session, request_url + f"?f=json&limit={limit}&offset=0")
        data.append(initial)
        total = initial.get('numberMatched', 0)
        logger.log("INFO", f"Total items found for {endpoint}: {total}")
        # split fetch jobs into batches if there is more than set limit
        if total > limit:
            tasks = [
                self.__fetch_data_page(session, request_url + f"?f=json&limit={limit}&offset={offset}")
                for offset in range(limit, total + limit, limit)
            ]

            results = await asyncio.gather(*tasks)
            data.extend(results)
            logger.log("INFO", f"Fetched {total} data entries from {endpoint}")
        return data

    async def fetch_data(self, endpoints: list[__ENDPOINTS]):
        logger.log("INFO", f"Fetching data for endpoints: {endpoints}")
        print(len(endpoints))
        options = get_args(self.__ENDPOINTS)
        for item in endpoints:
            assert item in options, logger.log("ERROR", f"Invalid endpoint: {endpoints}. Must be one of {self.__ENDPOINTS}")
        limit = 1000
        data = []
        try:
            async with aiohttp.ClientSession() as session:
                if "all" in endpoints:
                    logger.log("INFO", "Fetching data for 'all' endpoints.")
                    for endpoint in self.__ENDPOINTS_LIST:
                        data.extend(await self.__fetch_everything(session, endpoint, limit))
                    return data
                elif len(endpoints) == 1:
                    logger.log("INFO", f"Fetching data for single endpoint: {endpoints[0]}.")
                    data = await self.__fetch_everything(session, endpoints[0], limit)
                    return data
                else:
                    logger.log("INFO", f"Fetching data for multiple endpoints: {endpoints}.")
                    for endpoint in endpoints:
                        data.extend(await self.__fetch_everything(session, endpoint, limit))
                    return data
        except Exception as e:
            logger.log("ERROR", f"Failed to fetch data for 'all' endpoints: {str(e)}")
            return []


if __name__ == "__main__":
    async def main():
        api_client = APIClient()
        test_data = await api_client.fetch_data(['RDLP_Bialystok_wydzielenia', 'lesnictwa'])
        with open("test_data.json", "w") as f:
            f.write(json.dumps(test_data))

    asyncio.run(main())