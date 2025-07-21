import asyncio
from collections import defaultdict

import aiofiles
import json
import re
import io
import unicodedata

from pathlib import Path
from datetime import datetime
from pydantic import ValidationError
from pydantic_geojson import MultiPolygonModel
from utils.validators import RDLPData
from utils.logger.logger import AsyncLogger
from db.connection import DBConnection

logger = AsyncLogger()

class DataLoader:
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
        for item in data.get("features", []):
            yield item

    async def __process_file(self, file_path: Path):
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
        if batch is not None:
            tasks = [ self.__process_file(f) for f in batch if f.is_file() ]
            return await asyncio.gather(*tasks)
        else:
            raise FileNotFoundError

    async def __batch_process_files(self):
        files = list(self.data_dir.glob("*.json"))
        total_files = len(files)

        for i in range(0, total_files, self.batch_size):
            batch = self.__process_single_batch(files[i:i + self.batch_size])
            yield batch

    async def insert_data(self):
        try:
            async with await self.connection.create_pool(self.pool_size // 2, self.pool_size) as pool:
                async for batch in self.__batch_process_files():
                    try:
                        logger.log("INFO", "Inserting items")
                        # flatten the list of lists and filter out None values
                        records = [item for sublist in await batch for item in sublist if item]
                        if not records:
                            logger.log("INFO","No valid records found in this batch.")
                            continue

                        grouped = defaultdict(list)
                        for record in records:
                            table_name = f"{record.rdlp_name}_wydzielenia"
                            grouped[table_name].append(record)

                        for table_name, group_records in grouped.items():
                            try:
                                logger.log("INFO", f"Inserting {len(group_records)} records into table {table_name}")
                                columns = list(group_records[0].model_dump().keys())
                                output = io.BytesIO()
                                file = open("test.asdf", "a")
                                for record in group_records:
                                    row = [
                                        unicodedata.normalize('NFC', str(record.model_dump().get(col, "")))
                                        for col in columns
                                    ]

                                    output.write(('\t'.join(row) + '\n').encode('utf-8'))
                                output.seek(0)
                                lines = output.getvalue().decode('utf-8').splitlines()

                                for idx, line in enumerate(lines):
                                    if 40 < len(line) < 50 or idx < 5:
                                        print(f"Line {idx}: {line}")
                                async with pool.acquire() as conn:
                                    print(await conn.fetchval("show CLIENT_ENCODING;"))
                                    await conn.copy_to_table(
                                        table_name=table_name,
                                        schema_name="rdlp",
                                        source=output,
                                        columns=columns,
                                        format='text',
                                        delimiter='\t',
                                        encoding='utf-8'
                                    )
                            except Exception as e:
                                file.writelines("\n\n".join(row))
                                logger.log("ERROR", f"Failed to insert records into {table_name}: {e}")
                    except Exception as e:
                        logger.log("ERROR", f"Failed to process batch: {e}")
        except Exception as e:
            logger.log("ERROR", f"error in insert data: {e}")


if __name__ == "__main__":
    async def main():
        data_dir = Path("G:\\PilarzOPS\\RDLP-API\\temp_data")
        loader = DataLoader(data_dir)

        await loader.insert_data()
    asyncio.run(main())