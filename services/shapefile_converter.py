"""
Module for converting Shapefile to GeoJSON format.
"""
import geopandas as gpd
from pathlib import Path
from utils.logger.logger import AsyncLogger
import json

logger = AsyncLogger()


def convert_shapefile_to_geojson(shp_path: Path, output_path: Path = None) -> Path:
    """
    Converts a Shapefile to GeoJSON format.
    
    Args:
        shp_path (Path): Path to the .shp file
        output_path (Path, optional): Path for output GeoJSON file. 
                                     If None, creates file with same name as .shp but .geojson extension.
    
    Returns:
        Path: Path to the created GeoJSON file
    """
    try:
        if output_path is None:
            output_path = shp_path.parent / f"{shp_path.stem}.geojson"
        
        logger.log("INFO", f"Converting Shapefile {shp_path.name} to GeoJSON")
        
        # Read Shapefile using geopandas
        gdf = gpd.read_file(shp_path)
        
        # Convert to GeoJSON
        gdf.to_file(output_path, driver='GeoJSON')
        
        logger.log("INFO", f"Successfully converted to {output_path.name}")
        return output_path
    except Exception as e:
        logger.log("ERROR", f"Error converting Shapefile {shp_path}: {str(e)}")
        raise


def find_shapefiles(directory: Path) -> list[Path]:
    """
    Finds all .shp files in a directory (recursively).
    
    Args:
        directory (Path): Directory to search
        
    Returns:
        list[Path]: List of paths to .shp files
    """
    return list(directory.rglob("*.shp"))


def find_geojson_files(directory: Path) -> list[Path]:
    """
    Finds all .geojson files in a directory (recursively).
    
    Args:
        directory (Path): Directory to search
        
    Returns:
        list[Path]: List of paths to .geojson files
    """
    return list(directory.rglob("*.geojson"))


def find_json_files(directory: Path) -> list[Path]:
    """
    Finds all .json files in a directory (recursively).
    
    Args:
        directory (Path): Directory to search
        
    Returns:
        list[Path]: List of paths to .json files
    """
    return list(directory.rglob("*.json"))


def convert_all_shapefiles_in_directory(directory: Path) -> list[Path]:
    """
    Converts all Shapefiles in a directory to GeoJSON.
    
    Args:
        directory (Path): Directory containing Shapefiles
        
    Returns:
        list[Path]: List of paths to created GeoJSON files
    """
    shapefiles = find_shapefiles(directory)
    geojson_files = []
    
    for shp_path in shapefiles:
        try:
            geojson_path = convert_shapefile_to_geojson(shp_path)
            geojson_files.append(geojson_path)
        except Exception as e:
            logger.log("ERROR", f"Failed to convert {shp_path}: {str(e)}")
            continue
    
    return geojson_files

