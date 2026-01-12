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
import geopandas as gpd

from pathlib import Path
from datetime import datetime
from pydantic import ValidationError
from utils.validators import RDLPData
from utils.logger.logger import AsyncLogger
from db.connection import DBConnection
from services.txt_loader import load_all_descriptive_data, merge_geometry_with_descriptive_data
from services.shapefile_converter import find_shapefiles

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
            # G_SUBAREA already contains all fields, G_COMPARTMENT will be merged with F_* tables
            item = {
                "id": item_id,
                "area_type": properties.get("area_type"),
                "a_i_num": a_i_num if a_i_num else item_id,  # Use a_i_num if available, otherwise use item_id
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
            
            # Convert numeric fields if they are strings
            if item.get("sub_area") and isinstance(item["sub_area"], str):
                try:
                    item["sub_area"] = float(item["sub_area"])
                except (ValueError, TypeError):
                    item["sub_area"] = None
            
            if item.get("rotat_age") and isinstance(item["rotat_age"], str):
                try:
                    item["rotat_age"] = int(item["rotat_age"])
                except (ValueError, TypeError):
                    item["rotat_age"] = None
            
            if item.get("spec_age") and isinstance(item["spec_age"], str):
                try:
                    item["spec_age"] = int(item["spec_age"])
                except (ValueError, TypeError):
                    item["spec_age"] = None
            
            if item.get("a_year") and isinstance(item["a_year"], str):
                try:
                    item["a_year"] = int(item["a_year"])
                except (ValueError, TypeError):
                    item["a_year"] = None
            
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

    async def __process_file(self, file_path: Path, descriptive_data: dict = None):
        """
        Processes a single JSON file: reads, parses, and validates its data.
        Optionally merges with descriptive data from TXT files.

        Args:
            file_path (Path): Path to the JSON file.
            descriptive_data (dict, optional): Descriptive data from F_* tables indexed by arodes_int_num.

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
                    # Merge with descriptive data if available
                    if descriptive_data:
                        item = merge_geometry_with_descriptive_data(item, descriptive_data)
                    
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

    async def __process_single_batch(self, batch, descriptive_data: dict = None):
        """
        Processes a batch of files concurrently.

        Args:
            batch (list[Path]): List of file paths in the batch.
            descriptive_data (dict, optional): Descriptive data from F_* tables.

        Returns:
            list: List of results from processing each file.
        """

        if batch is not None:
            tasks = [ self.__process_file(f, descriptive_data) for f in batch if f.is_file() ]
            logger.log("INFO", f"Processing batch of {len(tasks)} files for endpoint {batch[0].name.split('_wydzielenia')[0]}")
            return await asyncio.gather(*tasks)
        else:
            raise FileNotFoundError

    def __should_process_file(self, file_path: Path) -> bool:
        """
        Determines if a file should be processed based on its name.
        Processes wydzielenia files - G_SUBAREA (has all descriptive fields) or G_COMPARTMENT.
        
        According to documentation (DataDescription.pdf):
        - G_SUBAREA = wydzielenia (subareas) with all descriptive fields - PROCESS (preferred)
        - G_COMPARTMENT = oddziały (compartments) with only basic fields - PROCESS (fallback)
        - G_FOREST_RANGE = lesnictwa (forest ranges) - SKIP
        - G_INSPECTORATE = inspektoraty (inspectorates) - SKIP
        
        G_SUBAREA contains: a_i_num, adr_for, area_type, site_type, silvicult, forest_fun,
        stand_stru, rotat_age, sub_area, prot_categ, species_cd, part_cd, spec_age, a_year
        G_COMPARTMENT contains only: a_i_num, adr_for, a_year
        
        Args:
            file_path (Path): Path to the file
            
        Returns:
            bool: True if file should be processed (wydzielenia), False otherwise
        """
        filename = file_path.name.upper()
        
        # Process G_SUBAREA files (wydzielenia with all descriptive fields) - PREFERRED
        if 'G_SUBAREA' in filename:
            return True
        
        # Process G_COMPARTMENT files (oddziały/compartments) - FALLBACK if G_SUBAREA not available
        if 'G_COMPARTMENT' in filename:
            logger.log("INFO", f"Processing G_COMPARTMENT file: {file_path.name} (will merge with F_* tables if available)")
            return True
        
        # Process old API format files (RDLP_*_wydzielenia)
        if '_wydzielenia' in filename:
            return True
        
        # Skip other G_ prefixed files (G_FOREST_RANGE, G_INSPECTORATE, etc.)
        if filename.startswith('G_'):
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
        Also loads descriptive data from TXT files (F_* tables) and merges it with geometry data.

        Yields:
            coroutine: Coroutine for processing a batch of files.
        """
        # Load descriptive data from TXT files
        # Try to find TXT files in extracted subdirectories (from ZIP files)
        descriptive_data = {}
        txt_files_found = False
        
        # First, try to find TXT files in extracted subdirectories
        extracted_dir = self.data_dir / 'extracted'
        if extracted_dir.exists():
            # Look for TXT files in each extracted ZIP directory
            for zip_dir in extracted_dir.iterdir():
                if zip_dir.is_dir():
                    if (zip_dir / 'f_subarea.txt').exists() or (zip_dir / 'F_SUBAREA.txt').exists():
                        logger.log("INFO", f"Loading descriptive data from {zip_dir}")
                        zip_descriptive_data = load_all_descriptive_data(zip_dir)
                        # Merge with existing data (later ZIPs override earlier ones)
                        descriptive_data.update(zip_descriptive_data)
                        txt_files_found = True
        
        # If not found in extracted, try root data directory
        if not txt_files_found:
            for potential_dir in [self.data_dir, self.data_dir.parent]:
                if (potential_dir / 'f_subarea.txt').exists() or (potential_dir / 'F_SUBAREA.txt').exists():
                    logger.log("INFO", f"Loading descriptive data from {potential_dir}")
                    descriptive_data = load_all_descriptive_data(potential_dir)
                    txt_files_found = True
                    break
        
        if not txt_files_found:
            logger.log("INFO", "No descriptive TXT files found, processing geometry data only")
        else:
            logger.log("INFO", f"Loaded descriptive data for {len(descriptive_data)} records")
        
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
            batch = self.__process_single_batch(files[i:i + self.batch_size], descriptive_data)
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
            
            # Note: Database tables should be initialized by backend Alembic migrations
            # Tables are expected to exist before data insertion

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
                   
                   # Use main table instead of partition - PostgreSQL will automatically route to correct partition
                   # This ensures data goes to the right partition even if partition name doesn't match exactly
                   sql = (f'INSERT INTO rdlp.{table_name} ({col_names}) VALUES ({placeholders})'
                          f'ON CONFLICT ("adr_for", "rdlp_name") DO NOTHING'
                          )
                   rows = [tuple(item.get(col) for col in columns) for item in items]
                   await conn.executemany(sql, rows)
                   logger.log("INFO", f"Inserted {len(rows)} records into rdlp.{table_name}.")
        # Don't close connection here - allow further operations

    async def close(self):
        """Close database connection pool."""
        if self.pool:
            await self.connection.close()
            self.pool = None

    async def insert_g_tables_data(self):
        """
        Loads data from G_INSPECTORATE, G_FOREST_RANGE, G_SUBAREA files into respective tables.
        These are separate geometry tables for reference data.
        """
        if self.pool is None:
            logger.log("INFO", "Creating database connection pool for G_* tables.")
            self.pool = await self.connection.create_pool(self.pool_size // 2, self.pool_size)

        # Find all G_* GeoJSON files
        files = []
        files.extend(self.data_dir.glob("*.json"))
        files.extend(self.data_dir.glob("*.geojson"))
        
        extracted_dir = self.data_dir / 'extracted'
        if extracted_dir.exists():
            files.extend(extracted_dir.rglob("*.json"))
            files.extend(extracted_dir.rglob("*.geojson"))

        # Filter for G_INSPECTORATE, G_FOREST_RANGE, G_SUBAREA files
        g_inspectorate_files = [f for f in files if 'G_INSPECTORATE' in f.name.upper() and (f.name.upper().endswith('.JSON') or f.name.upper().endswith('.GEOJSON'))]
        g_forest_range_files = [f for f in files if 'G_FOREST_RANGE' in f.name.upper() and (f.name.upper().endswith('.JSON') or f.name.upper().endswith('.GEOJSON'))]
        g_subarea_files = [f for f in files if 'G_SUBAREA' in f.name.upper() and (f.name.upper().endswith('.JSON') or f.name.upper().endswith('.GEOJSON')) and 'WYDZELENIA' not in f.name.upper()]

        logger.log("INFO", f"Found {len(g_inspectorate_files)} G_INSPECTORATE files, {len(g_forest_range_files)} G_FOREST_RANGE files, {len(g_subarea_files)} G_SUBAREA files")

        # Load G_INSPECTORATE
        if g_inspectorate_files:
            await self.__load_g_table('G_INSPECTORATE', g_inspectorate_files, ['a_i_num', 'adr_for', 'i_name', 'a_year'])

        # Load G_FOREST_RANGE
        if g_forest_range_files:
            await self.__load_g_table('G_FOREST_RANGE', g_forest_range_files, ['a_i_num', 'adr_for', 'f_r_name', 'a_year'])

        # Load G_SUBAREA
        if g_subarea_files:
            await self.__load_g_table('G_SUBAREA', g_subarea_files, ['a_i_num', 'adr_for', 'area_type', 'site_type', 'silvicult', 'forest_fun', 'stand_stru', 'rotat_age', 'sub_area', 'prot_categ', 'species_cd', 'part_cd', 'spec_age', 'a_year'])

    async def __load_g_table(self, table_name: str, files: list, fields: list):
        """
        Loads data from GeoJSON or Shapefile files into a G_* table.
        
        Args:
            table_name: Name of the table (e.g., 'G_INSPECTORATE')
            files: List of file paths to process (.json, .geojson, or .shp)
            fields: List of field names to extract from properties
        """
        logger.log("INFO", f"Loading {table_name} data from {len(files)} files")
        
        all_items = []
        for file_path in files:
            try:
                # Check if file is Shapefile
                if file_path.suffix.lower() == '.shp':
                    # Load Shapefile directly using geopandas
                    logger.log("INFO", f"Loading Shapefile: {file_path.name}")
                    gdf = gpd.read_file(file_path)
                    
                    for idx, row in gdf.iterrows():
                        # Build item dictionary from GeoDataFrame row
                        item = {}
                        for field in fields:
                            value = row.get(field)
                            # Convert numeric fields
                            if field in ['a_i_num', 'a_year', 'rotat_age', 'spec_age']:
                                if value is not None and value != '':
                                    try:
                                        item[field] = int(value)
                                    except (ValueError, TypeError):
                                        item[field] = None
                                else:
                                    item[field] = None
                            elif field == 'sub_area':
                                if value is not None and value != '':
                                    try:
                                        item[field] = float(value)
                                    except (ValueError, TypeError):
                                        item[field] = None
                                else:
                                    item[field] = None
                            else:
                                item[field] = value
                        
                        # Convert geometry to WKT
                        if row.geometry is not None:
                            try:
                                item['geometry'] = shapely.wkt.dumps(row.geometry)
                            except Exception as e:
                                logger.log("ERROR", f"Failed to convert geometry to WKT for {file_path.name}: {e}")
                                item['geometry'] = None
                        else:
                            item['geometry'] = None
                        
                        all_items.append(item)
                else:
                    # Load GeoJSON file
                    async with aiofiles.open(file_path) as f:
                        file_content = await f.read()
                        json_data = json.loads(file_content)
                        
                        # Handle FeatureCollection format
                        features = []
                        if "features" in json_data:
                            features = json_data["features"]
                        elif json_data.get("type") == "Feature":
                            features = [json_data]
                        elif isinstance(json_data, list):
                            features = [item for item in json_data if isinstance(item, dict)]
                        
                        for feature in features:
                            properties = feature.get('properties', {})
                            geometry_data = feature.get('geometry')
                            
                            # Build item dictionary
                            item = {}
                            for field in fields:
                                value = properties.get(field)
                                # Convert numeric fields
                                if field in ['a_i_num', 'a_year', 'rotat_age', 'spec_age']:
                                    if value is not None and value != '':
                                        try:
                                            item[field] = int(value)
                                        except (ValueError, TypeError):
                                            item[field] = None
                                    else:
                                        item[field] = None
                                elif field == 'sub_area':
                                    if value is not None and value != '':
                                        try:
                                            item[field] = float(value)
                                        except (ValueError, TypeError):
                                            item[field] = None
                                    else:
                                        item[field] = None
                                else:
                                    item[field] = value
                            
                            # Convert geometry to WKT
                            if geometry_data:
                                if isinstance(geometry_data, dict):
                                    try:
                                        geom = shapely.geometry.shape(geometry_data)
                                        item['geometry'] = shapely.wkt.dumps(geom)
                                    except Exception as e:
                                        logger.log("ERROR", f"Failed to convert geometry to WKT for {file_path.name}: {e}")
                                        item['geometry'] = None
                                elif isinstance(geometry_data, str):
                                    item['geometry'] = geometry_data
                                else:
                                    item['geometry'] = None
                            else:
                                item['geometry'] = None
                            
                            all_items.append(item)
                    
            except Exception as e:
                logger.log("ERROR", f"Error processing {file_path} for {table_name}: {e}")
                continue

        if not all_items:
            logger.log("INFO", f"No data to insert into {table_name}")
            return

        # Insert data into database
        async with self.pool.acquire() as conn:
            columns = fields + ['geometry']
            col_names = ', '.join([f'"{col}"' if col != 'geometry' else '"geometry"' for col in columns])
            placeholders = ', '.join([
                f'${i+1}' if col != 'geometry' else f'ST_GeomFromText(${i+1}, 4326)' 
                for i, col in enumerate(columns)
            ])
            
            sql = f'INSERT INTO rdlp.{table_name} ({col_names}) VALUES ({placeholders})'
            rows = [tuple(item.get(col) for col in columns) for item in all_items]
            await conn.executemany(sql, rows)
            logger.log("INFO", f"Inserted {len(rows)} records into rdlp.{table_name}")


if __name__ == "__main__":
    async def main():
        data_dir = Path(__file__).parent / 'api_data'
        loader = DataLoader(data_dir)

        await loader.insert_data()
    asyncio.run(main())