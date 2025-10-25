import asyncio
from collections import defaultdict

import aiofiles
import json
import re
import io
import csv
import unicodedata
import shapely.geometry, shapely.wkt
import unittest

from pathlib import Path
from datetime import datetime
from pydantic import ValidationError
from pydantic_geojson import MultiPolygonModel
from utils.validators import RDLPData
from utils.logger.logger import AsyncLogger
from db.connection import DBConnection

logger = AsyncLogger()


class DataLoader:
    """
    Loads, validates, and inserts RDLP data from JSON files into the database.
    """

    __ENDPOINTS_DICT = {
        'RDLP_Bialystok_wydzielenia': 'bialystok',
        'RDLP_Katowice_wydzielenia': 'katowice',
        'RDLP_Krakow_wydzielenia': 'krakow',
        'RDLP_Krosno_wydzielenia': 'krosno',
        'RDLP_Lublin_wydzielenia': 'lublin',
        'RDLP_Lodz_wydzielenia': 'lodz',
        'RDLP_Olsztyn_wydzielenia': 'olsztyn',
        'RDLP_Pila_wydzielenia': 'pila',
        'RDLP_Poznan_wydzielenia': 'poznan',
        'RDLP_Szczecin_wydzielenia': 'szczecin',
        'RDLP_Szczecinek_wydzielenia': 'szczecinek',
        'RDLP_Torun_wydzielenia': 'torun',
        'RDLP_Wroclaw_wydzielenia': 'wroclaw',
        'RDLP_Zielona_Gora_wydzielenia': 'zielona_gora',
        'RDLP_Gdansk_wydzielenia': 'gdansk',
        'RDLP_Radom_wydzielenia': 'radom',
        'RDLP_Warszawa_wydzielenia': 'warszawa'
    }

    def __init__(self, data_dir: Path):
        self.data_dir: Path = data_dir
        self.batch_size: int = 1

        self.connection = DBConnection()
        self.pool_size: int = 15
        self.pool = None

    def __validate_data(self, data, file: Path):
        """
        Validates a single data item using the RDLPData model.

        Args:
            data (dict): The data item to validate.
            file (Path): The file from which the data was loaded.

        Returns:
            RDLPData or list: Validated data item or an empty list if validation fails.
        """

        try:
            item = {
                "id": data.get("id", {}),
                "area_type": data.get("properties", {}).get("area_type"),
                "a_i_num": data.get("properties", {}).get("a_i_num"),
                "silvicult": data.get("properties", {}).get("silvicult"),
                "stand_stru": data.get("properties", {}).get("stand_stru"),
                "sub_area": data.get("properties", {}).get("sub_area"),
                "species_cd": data.get("properties", {}).get("species_cd"),
                "spec_age": data.get("properties", {}).get("spec_age"),
                "nazwa": data.get("properties", {}).get("nazwa"),
                "adr_for": data.get("properties", {}).get("adr_for"),
                "site_type": data.get("properties", {}).get("site_type"),
                "forest_fun": data.get("properties", {}).get("forest_fun"),
                "rotat_age": data.get("properties", {}).get("rotat_age"),
                "prot_categ": data.get("properties", {}).get("prot_categ"),
                "part_cd": data.get("properties", {}).get("part_cd"),
                "a_year": data.get("properties", {}).get("a_year"),
                "geometry": MultiPolygonModel(**data.get('geometry', {})),
                "load_time": datetime.now(),
                "forest_range_name": "asdf",  # wtf is this
                "rdlp_name": self.__ENDPOINTS_DICT[re.search(".*_wydzielenia", file.name.split('/')[-1]).group()]
            }
            validated = RDLPData(**item)
            return validated
        except ValidationError as e:
            logger.log("ERROR", f"{file} Validation error {e.json()}")
            return []
        except Exception as e:
            logger.log("ERROR", f"{file} Unexpected error: {e}")
            return []

    async def __get_single_item(self, data):
        """
        Asynchronously yields each feature item from the data.

        Args:
            data (dict): The JSON data containing features.

        Yields:
            dict: A single feature item.
        """

        for item in data.get("features", []):
            yield item

    async def __process_file(self, file_path: Path):
        """
        Processes a single JSON file: reads, parses, and validates its data.

        Args:
            file_path (Path): Path to the JSON file.

        Returns:
            list: List of validated data items.
        """

        try:
            async with aiofiles.open(file_path) as file:
                file_content = await file.read()
                json_data = json.loads(file_content)
                validated_data = []

                async for item in self.__get_single_item(json_data):
                    vitem = self.__validate_data(item, file_path)
                    validated_data.append(vitem)
            return validated_data
        except FileNotFoundError:
            logger.log("ERROR", f"File not found: {file_path}")
            return []
        except Exception as e:
            logger.log("ERROR", f"Error processing file {file_path}: {e}")
            return []

    async def __process_single_batch(self, batch):
        """
        Processes a batch of files concurrently.

        Args:
            batch (list[Path]): List of file paths in the batch.

        Returns:
            list: List of results from processing each file.
        """

        if batch is not None:
            tasks = [ self.__process_file(f) for f in batch if f.is_file() ]
            logger.log("INFO", f"Processing batch of {len(tasks)} files for endpoint {batch[0].name.split('_wydzielenia')[0]}")
            return await asyncio.gather(*tasks)
        else:
            raise FileNotFoundError

    async def __batch_process_files(self):
        """
        Asynchronously yields batches of files to be processed.

        Yields:
            coroutine: Coroutine for processing a batch of files.
        """

        files = list(self.data_dir.glob("*.json"))
        total_files = len(files)

        for i in range(0, total_files, self.batch_size):
            batch = self.__process_single_batch(files[i:i + self.batch_size])
            yield batch

    async def insert_data(self):
        """
        Processes all data files and inserts validated records into the database.

        Returns:
            None
        """

        if self.pool is None:
            logger.log("INFO", "Creating database connection pool.")
            self.pool = await self.connection.create_pool(self.pool_size // 2, self.pool_size)

        async for batch_future in self.__batch_process_files():
            batch_results = await batch_future
            # Flatten and filter out invalid items
            validated_items = [
                item.model_dump() for sublist in batch_results for item in sublist if item
            ]
            logger.log("INFO", f"Validated {len(validated_items)} items in current batch.")
            if not validated_items:
                continue

            # Group records by table name
            table_groups = {}
            for item in validated_items:
                table_name = f"{item['rdlp_name']}_wydzielenia"
                if isinstance(item['geometry'], dict):
                    geom = shapely.geometry.shape(item['geometry'])
                    item['geometry'] = shapely.wkt.dumps(geom)
                table_groups.setdefault(table_name, []).append(item)

            # Insert each group into its table
            async with self.pool.acquire() as conn:
               for table_name, items in table_groups.items():
                   columns = list(items[0].keys())
                   col_names = ', '.join([f'"{col}"' if col != 'geometry' else '"geometry"' for col in columns])
                   placeholders = ', '.join([f'${i+1}' if col != 'geometry' else f'ST_GeomFromText(${i+1}, 4326)' for i, col in enumerate(columns)])
                   sql = (f'INSERT INTO public.rdlp_{table_name} ({col_names}) VALUES ({placeholders})'
                          f'ON CONFLICT ("adr_for", "rdlp_name") DO NOTHING'
                          )
                   rows = [tuple(item[col] for col in columns) for item in items]
                   await conn.executemany(sql, rows)
            logger.log("INFO", f"Inserted {len(rows)} records into {table_name}.")



if __name__ == "__main__":
    async def main():
        data_dir = Path(__file__).parent / 'api_data'
        loader = DataLoader(data_dir)

        await loader.insert_data()
    asyncio.run(main())