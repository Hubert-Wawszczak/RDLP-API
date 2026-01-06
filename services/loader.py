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
        Aligned with WydzielenieResponse schema from backend.

        Args:
            data (dict): The data item to validate.
            file (Path): The file from which the data was loaded.

        Returns:
            RDLPData or list: Validated data item or an empty list if validation fails.
        """

        try:
            properties = data.get("properties", {})
            geometry_data = data.get('geometry')
            
            # Extract id - can be at feature level, in properties, or as a_i_num
            # According to documentation, G_COMPARTMENT uses a_i_num instead of id
            feature_id = data.get("id")
            properties_id = properties.get("id")
            a_i_num = properties.get("a_i_num")
            
            # Use id if available, otherwise use a_i_num (for G_COMPARTMENT files)
            item_id = feature_id or properties_id or a_i_num
            
            # Early return if id is missing - this is likely not a wydzielenia file
            if not item_id:
                return None
            
            # Extract RDLP name from filename or path
            # Try to match pattern like "RDLP_*_wydzielenia" first (old API format)
            filename = file.name.split('/')[-1]
            match = re.search(".*_wydzielenia", filename)
            
            if match:
                # Old API format
                rdlp_name = self.__ENDPOINTS_DICT.get(match.group())
            else:
                # Try to extract from ZIP file structure or properties
                # Check if file is in extracted directory (from ZIP)
                path_str = str(file)
                if 'extracted' in path_str:
                    # Try to find RDLP name from parent directory or properties
                    # Look for BDL_XX_XX pattern in path
                    bdl_match = re.search(r'BDL_(\d+)_', path_str)
                    if bdl_match:
                        # Map BDL region code to RDLP name
                        region_code = bdl_match.group(1)
                        rdlp_mapping = {
                            '01': 'bialystok', '02': 'katowice', '03': 'krakow', '04': 'krosno',
                            '05': 'lublin', '06': 'lodz', '07': 'olsztyn', '08': 'pila',
                            '09': 'poznan', '10': 'szczecin', '11': 'szczecinek', '12': 'torun',
                            '13': 'wroclaw', '14': 'zielona_gora', '15': 'gdansk',
                            '16': 'radom', '17': 'warszawa'
                        }
                        rdlp_name = rdlp_mapping.get(region_code)
                    else:
                        # Try to get from properties
                        rdlp_name = properties.get("rdlp_name")
                else:
                    # Try to get from properties
                    rdlp_name = properties.get("rdlp_name")
            
            if not rdlp_name:
                logger.log("ERROR", f"{file} Could not extract RDLP name from filename or properties")
                return None
            
            # Extract forest_range_name from properties or use default
            forest_range_name = properties.get("forest_range_name") or properties.get("nazwa") or "unknown"
            
            # Build item dictionary with optional fields
            item = {
                "id": item_id,
                "area_type": properties.get("area_type"),
                "a_i_num": properties.get("a_i_num"),
                "silvicult": properties.get("silvicult"),
                "stand_stru": properties.get("stand_stru"),
                "sub_area": properties.get("sub_area"),
                "species_cd": properties.get("species_cd"),
                "spec_age": properties.get("spec_age"),
                "nazwa": properties.get("nazwa"),
                "adr_for": properties.get("adr_for"),
                "site_type": properties.get("site_type"),
                "forest_fun": properties.get("forest_fun"),
                "rotat_age": properties.get("rotat_age"),
                "prot_categ": properties.get("prot_categ"),
                "part_cd": properties.get("part_cd"),
                "a_year": properties.get("a_year"),
                "geometry": geometry_data if geometry_data else None,
                "forest_range_name": forest_range_name,
                "rdlp_name": rdlp_name
            }
            
            # Validate required fields
            if not item.get("id"):
                # Skip silently - this file doesn't have required fields, likely not a wydzielenia
                # Return None instead of [] to distinguish from validation errors
                return None
            if not item.get("adr_for"):
                logger.log("ERROR", f"{file} Missing required field: adr_for")
                return []
            
            validated = RDLPData(**item)
            return validated
        except ValidationError as e:
            # Don't log validation errors for files that are clearly not wydzielenia
            file_str = str(file).upper()
            
            # Skip all G_ files except G_COMPARTMENT (wydzielenia)
            if 'G_' in file_str and 'G_COMPARTMENT' not in file_str:
                return None
            
            # Skip SUBAREA files
            if 'SUBAREA' in file_str:
                return None
            
            # Check error message for missing id (likely not a wydzielenia)
            error_str = str(e).upper()
            if 'id' in error_str and 'required' in error_str:
                return None
            
            # Only parse JSON if we need to log the error
            try:
                error_json = e.json() if hasattr(e, 'json') else str(e)
                if 'id' in error_json.upper() and 'required' in error_json.upper():
                    return None
            except:
                error_json = str(e)
                
            logger.log("ERROR", f"{file} Validation error {error_json}")
            return []
        except Exception as e:
            # Don't log errors for files that are clearly not wydzielenia
            file_str = str(file).upper()
            
            # Skip all G_ files except G_COMPARTMENT (wydzielenia)
            if 'G_' in file_str and 'G_COMPARTMENT' not in file_str:
                return None
            
            # Skip SUBAREA files
            if 'SUBAREA' in file_str:
                return None
                
            logger.log("ERROR", f"{file} Unexpected error: {e}")
            return []

    async def __get_single_item(self, data):
        """
        Asynchronously yields each feature item from the data.
        Handles both GeoJSON FeatureCollection format and single Feature format.

        Args:
            data (dict): The JSON/GeoJSON data containing features.

        Yields:
            dict: A single feature item.
        """
        # Handle GeoJSON FeatureCollection format
        if "features" in data:
            for item in data.get("features", []):
                yield item
        # Handle single Feature format (from Shapefile conversion)
        elif data.get("type") == "Feature":
            yield data
        # Handle array of features
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    yield item

    async def __process_file(self, file_path: Path):
        """
        Processes a single JSON file: reads, parses, and validates its data.

        Args:
            file_path (Path): Path to the JSON file.

        Returns:
            list: List of validated data items.
        """
        # Skip files that shouldn't be processed (e.g., G_SUBAREA)
        if not self.__should_process_file(file_path):
            return []

        try:
            async with aiofiles.open(file_path) as file:
                file_content = await file.read()
                json_data = json.loads(file_content)
                validated_data = []

                async for item in self.__get_single_item(json_data):
                    vitem = self.__validate_data(item, file_path)
                    # Only append if validation returned a valid RDLPData object (not None or empty list)
                    if vitem is not None:
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

    def __should_process_file(self, file_path: Path) -> bool:
        """
        Determines if a file should be processed based on its name.
        Only processes wydzielenia (G_COMPARTMENT) files, skips all other types.
        
        According to documentation:
        - G_COMPARTMENT = wydzielenia (divisions/compartments) - PROCESS
        - G_SUBAREA = podpowierzchnie (subareas) - SKIP
        - G_FOREST_RANGE = lesnictwa (forest ranges) - SKIP
        - G_INSPECTORATE = inspektoraty (inspectorates) - SKIP
        
        Args:
            file_path (Path): Path to the file
            
        Returns:
            bool: True if file should be processed (wydzielenia), False otherwise
        """
        filename = file_path.name.upper()
        
        # Process G_COMPARTMENT files (wydzielenia/compartments)
        if 'G_COMPARTMENT' in filename:
            return True
        
        # Process old API format files (RDLP_*_wydzielenia)
        if '_wydzielenia' in filename:
            return True
        
        # Skip all other G_ prefixed files (G_SUBAREA, G_FOREST_RANGE, G_INSPECTORATE, etc.)
        if filename.startswith('G_'):
            logger.log("INFO", f"Skipping non-wydzielenia file: {file_path.name} (not G_COMPARTMENT)")
            return False
        
        # Skip SUBAREA files (without G_ prefix)
        if 'SUBAREA' in filename:
            logger.log("INFO", f"Skipping non-wydzielenia file: {file_path.name}")
            return False
        
        # For old API JSON files (without G_ prefix), process them
        if filename.endswith('.JSON') or filename.endswith('.GEOJSON'):
            return True
        
        # Default: skip unknown files
        logger.log("INFO", f"Skipping unknown file type: {file_path.name}")
        return False
    
    async def __batch_process_files(self):
        """
        Asynchronously yields batches of files to be processed.
        Searches for .json and .geojson files in the data directory and extracted subdirectories.
        Filters out files that are not wydzielenia (e.g., G_SUBAREA).

        Yields:
            coroutine: Coroutine for processing a batch of files.
        """
        # Search for JSON and GeoJSON files in data directory and extracted subdirectories
        files = []
        
        # Find JSON files in root data directory
        files.extend(self.data_dir.glob("*.json"))
        
        # Find GeoJSON files in root data directory
        files.extend(self.data_dir.glob("*.geojson"))
        
        # Find JSON and GeoJSON files in extracted subdirectories
        extracted_dir = self.data_dir / 'extracted'
        if extracted_dir.exists():
            files.extend(extracted_dir.rglob("*.json"))
            files.extend(extracted_dir.rglob("*.geojson"))
        
        # Filter out files that shouldn't be processed
        files = [f for f in files if self.__should_process_file(f)]
        
        total_files = len(files)
        logger.log("INFO", f"Found {total_files} files to process (after filtering)")

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

            # Group records by table name and process geometry
            table_groups = {}
            for item in validated_items:
                table_name = f"{item['rdlp_name']}_wydzielenia"
                
                # Convert geometry from GeoJSON to WKT if present
                if item.get('geometry') is not None:
                    if isinstance(item['geometry'], dict):
                        try:
                            geom = shapely.geometry.shape(item['geometry'])
                            item['geometry'] = shapely.wkt.dumps(geom)
                        except Exception as e:
                            logger.log("ERROR", f"Failed to convert geometry to WKT: {e}")
                            item['geometry'] = None
                    elif isinstance(item['geometry'], str):
                        # Already in WKT format, keep as is
                        pass
                    else:
                        # Unknown format, set to None
                        item['geometry'] = None
                
                table_groups.setdefault(table_name, []).append(item)

            # Insert each group into its table
            async with self.pool.acquire() as conn:
               for table_name, items in table_groups.items():
                   if not items:
                       continue
                   
                   # Filter out load_time if present (not in backend schema)
                   columns = [col for col in items[0].keys() if col != 'load_time']
                   
                   col_names = ', '.join([f'"{col}"' if col != 'geometry' else '"geometry"' for col in columns])
                   placeholders = ', '.join([
                       f'${i+1}' if col != 'geometry' else f'ST_GeomFromText(${i+1}, 4326)' 
                       for i, col in enumerate(columns)
                   ])
                   
                   sql = (f'INSERT INTO rdlp.{table_name}_partition ({col_names}) VALUES ({placeholders})'
                          f'ON CONFLICT ("adr_for", "rdlp_name") DO NOTHING'
                          )
                   rows = [tuple(item.get(col) for col in columns) for item in items]
                   await conn.executemany(sql, rows)
                   logger.log("INFO", f"Inserted {len(rows)} records into rdlp.{table_name}_partition.")
        await self.connection.close()



if __name__ == "__main__":
    async def main():
        data_dir = Path(__file__).parent / 'api_data'
        loader = DataLoader(data_dir)

        await loader.insert_data()
    asyncio.run(main())