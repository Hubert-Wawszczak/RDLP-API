
import aiohttp
import aiofiles
import asyncio

from pathlib import Path
from datetime import datetime
from typing import Literal, get_args
from utils.logger.logger import AsyncLogger


logger = AsyncLogger()

class APIClient:
    __API = "https://ogcapi.bdl.lasy.gov.pl/collections/"
    __ENDPOINTS = Literal[
        'all',
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

    @staticmethod
    async def __get_item_total(session: aiohttp.ClientSession, url: str):
        """
        Fetches the total number of items available at the given API endpoint.

        Args:
            session (aiohttp.ClientSession): The active HTTP session.
            url (str): The URL to fetch the item count from.

        Returns:
            int: The total number of items, or 0 if the request fails.
        """
        logger.log("INFO", f"Fetching item total from {url}")
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('numberMatched', 0)
            else:
                logger.log("ERROR", f"Failed to fetch item total from {url}: {response.status}")
                return 0

    @staticmethod
    async def __fetch_data_page(session: aiohttp.ClientSession, url: str, offset: int = 0, save_dir: Path = Path('G:\\PilarzOPS\\RDLP-API\\temp_data')):
        """
        Fetches a single page of data from the API and saves it to a file.

        Args:
            session (aiohttp.ClientSession): The active HTTP session.
            url (str): The URL to fetch data from.
            offset (int, optional): The offset for pagination. Defaults to 0.
            save_dir (Path, optional): Directory to save the fetched data. Defaults to 'G:\\PilarzOPS\\RDLP-API\\temp_data'.

        Returns:
            bool: True if data was fetched and saved successfully, False otherwise.
        """
        save_dir.mkdir(exist_ok=True)

        filename = url.split('/')[-2] + f"_{offset}_" + str(int(datetime.now().timestamp())) + ".json"
        async with session.get(url) as response:
            if response.status == 200:
                text = await response.text(encoding='utf-8')
                async with aiofiles.open(save_dir / filename, "w", encoding='utf-8') as f:
                    # async for chunk in response.content.iter_chunked(1024):
                    await f.write(text)
                return True
            else:
                logger.log("ERROR", f"Failed to fetch data from {url}: {response.status}")
                return False


    @logger.log_time_exec
    async def __fetch_everything(self, session: aiohttp.ClientSession, endpoint: str, limit: int = 1000):
        """
        Fetches all data for a given endpoint, handling pagination and saving each page.

        Args:
            session (aiohttp.ClientSession): The active HTTP session.
            endpoint (str): The API endpoint to fetch data from.
            limit (int, optional): Number of items per page. Defaults to 1000.

        Returns:
            bool: True if all data was fetched successfully.
        """

        # fetch first page of data
        request_url = f"{self.__API}{endpoint}/items"
        await self.__fetch_data_page(session, request_url + f"?f=json&limit={limit}&offset=0")

        # get total number of items for given endpoint
        total = await self.__get_item_total(session, request_url + '?f=json&limit=1&skipGeometry=true')
        logger.log("INFO", f"Total items found for {endpoint}: {total}")
        # split fetch jobs into batches if there is more than set limit
        if total > limit:
            logger.log("INFO", f"Fetching data in batches of {limit} for {endpoint}. Total items: {total}")
            tasks = [
                self.__fetch_data_page(session, request_url + f"?f=json&limit={limit}&offset={offset}", offset)
                for offset in range(limit, total + limit, limit)
            ]
            await asyncio.gather(*tasks)
            logger.log("INFO", f"Fetched {total} data entries from {endpoint}")
        return True

    async def fetch_data(self, endpoints: list[__ENDPOINTS], limit: int = 1000):
        """
        Fetches data for the specified endpoints, supporting single, multiple, or all endpoints.

        Args:
            endpoints (list[__ENDPOINTS]): List of endpoint names to fetch data from.
            limit (int, optional): Number of items per page. Defaults to 1000.

        Returns:
            bool: True if data was fetched successfully, False otherwise.
        """

        options = get_args(self.__ENDPOINTS)
        for item in endpoints:
            assert item in options, logger.log("ERROR", f"Invalid endpoint: {endpoints}. Must be one of {self.__ENDPOINTS}")
        try:
            async with aiohttp.ClientSession() as session:
                if "all" in endpoints:
                    logger.log("INFO", "Fetching data for 'all' endpoints.")
                    tasks = [
                        self.__fetch_everything(session, e, limit)
                        for e in self.__ENDPOINTS_LIST
                    ]
                    await asyncio.gather(*tasks)
                    return True
                elif len(endpoints) == 1:
                    logger.log("INFO", f"Fetching data for single specified endpoint: {endpoints[0]}.")
                    await self.__fetch_everything(session, endpoints[0], limit)
                    return True
                else:
                    logger.log("INFO", f"Fetching data for multiple specified endpoints: {endpoints}.")
                    tasks = [
                        self.__fetch_everything(session, e, limit)
                        for e in endpoints
                    ]
                    await asyncio.gather(*tasks)
                    return True
        except Exception as e:
            logger.log("ERROR", f"Failed to fetch data for 'all' endpoints: {str(e)}")
            return False


if __name__ == "__main__":
    async def main():
        api_client = APIClient()
        await api_client.fetch_data(['RDLP_Bialystok_wydzielenia', 'RDLP_Lublin_wydzielenia'])

    asyncio.run(main())