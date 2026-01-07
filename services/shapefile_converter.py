"""
Module for converting Shapefile to GeoJSON format.
Handles all Shapefile component files (.shp, .shx, .dbf, .prj, .cpg, .sbn, .sbx, .xml).
"""
import geopandas as gpd
from pathlib import Path
from utils.logger.logger import AsyncLogger
import json

logger = AsyncLogger()


def validate_shapefile_components(shp_path: Path) -> dict:
    """
    Validates that all required Shapefile component files are present.
    
    Required files:
    - .shp (geometry) - REQUIRED
    - .shx (index) - REQUIRED
    - .dbf (attributes) - REQUIRED
    
    Optional but recommended:
    - .prj (projection) - RECOMMENDED
    - .cpg (encoding) - OPTIONAL
    - .sbn, .sbx (spatial index) - OPTIONAL
    - .shp.xml (metadata) - OPTIONAL
    
    Args:
        shp_path (Path): Path to the .shp file
    
    Returns:
        dict: Status of each component file
    """
    base_name = shp_path.stem
    directory = shp_path.parent
    
    components = {
        'shp': shp_path.exists(),
        'shx': (directory / f"{base_name}.shx").exists(),
        'dbf': (directory / f"{base_name}.dbf").exists(),
        'prj': (directory / f"{base_name}.prj").exists(),
        'cpg': (directory / f"{base_name}.cpg").exists(),
        'sbn': (directory / f"{base_name}.sbn").exists(),
        'sbx': (directory / f"{base_name}.sbx").exists(),
        'xml': (directory / f"{base_name}.shp.xml").exists(),
    }
    
    return components


def convert_shapefile_to_geojson(shp_path: Path, output_path: Path = None) -> Path:
    """
    Converts a Shapefile to GeoJSON format.
    Automatically handles all Shapefile component files (.shp, .shx, .dbf, .prj, etc.).
    
    Geopandas automatically finds and uses all component files in the same directory:
    - .shp - geometry data (required)
    - .shx - index file (required)
    - .dbf - attribute data (required)
    - .prj - projection information (recommended)
    - .cpg - encoding information (optional)
    - .sbn, .sbx - spatial index files (optional)
    - .shp.xml - metadata (optional)
    
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
        
        # Validate Shapefile components
        components = validate_shapefile_components(shp_path)
        
        # Check required files
        required_files = ['shp', 'shx', 'dbf']
        missing_required = [f for f in required_files if not components.get(f)]
        
        if missing_required:
            missing_exts = ', '.join([f".{ext}" for ext in missing_required])
            logger.log("ERROR", f"Missing required Shapefile components for {shp_path.name}: {missing_exts}")
            raise FileNotFoundError(f"Missing required Shapefile components: {missing_exts}")
        
        # Log component status
        optional_files = ['prj', 'cpg', 'sbn', 'sbx', 'xml']
        present_optional = [f for f in optional_files if components.get(f)]
        if present_optional:
            logger.log("INFO", f"Shapefile {shp_path.name} has optional components: {', '.join(present_optional)}")
        
        if not components.get('prj'):
            logger.log("WARNING", f"Shapefile {shp_path.name} missing .prj file (projection information)")
        
        logger.log("INFO", f"Converting Shapefile {shp_path.name} to GeoJSON")
        
        # Read Shapefile using geopandas
        # Geopandas automatically finds and uses all component files (.shx, .dbf, .prj, etc.)
        gdf = gpd.read_file(shp_path)
        
        # Log number of features and columns
        logger.log("INFO", f"Shapefile {shp_path.name} contains {len(gdf)} features with {len(gdf.columns)} columns")
        
        # Convert to GeoJSON
        gdf.to_file(output_path, driver='GeoJSON')
        
        logger.log("INFO", f"Successfully converted {shp_path.name} to {output_path.name}")
        return output_path
    except FileNotFoundError:
        raise
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

