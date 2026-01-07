"""
Module for loading and parsing TXT files with descriptive data (F_* tables).
These files contain detailed information that needs to be joined with geometry data from G_COMPARTMENT.
"""
import csv
import io
from pathlib import Path
from typing import Dict, List, Optional
from collections import defaultdict
from utils.logger.logger import AsyncLogger

logger = AsyncLogger()


def load_txt_file(file_path: Path, encoding: str = 'utf-8') -> List[Dict]:
    """
    Loads a TXT file (tab-separated) and returns a list of dictionaries.
    
    Args:
        file_path: Path to the TXT file
        encoding: File encoding (default: utf-8)
    
    Returns:
        List of dictionaries, one per row
    """
    try:
        with open(file_path, 'r', encoding=encoding, errors='replace') as f:
            # Read first line as header
            header_line = f.readline().strip()
            headers = [h.strip() for h in header_line.split('\t')]
            
            # Read remaining lines
            reader = csv.DictReader(f, fieldnames=headers, delimiter='\t')
            rows = []
            for row in reader:
                # Clean up values (remove whitespace, handle empty strings)
                cleaned_row = {}
                for key, value in row.items():
                    if value is None or value.strip() == '':
                        cleaned_row[key] = None
                    else:
                        cleaned_row[key] = value.strip()
                rows.append(cleaned_row)
            
            logger.log("INFO", f"Loaded {len(rows)} rows from {file_path.name}")
            return rows
    except UnicodeDecodeError:
        # Try with different encoding
        logger.log("WARNING", f"UTF-8 failed for {file_path.name}, trying cp1250")
        return load_txt_file(file_path, encoding='cp1250')
    except Exception as e:
        logger.log("ERROR", f"Failed to load {file_path}: {e}")
        return []


def load_f_subarea(file_path: Path) -> Dict[int, Dict]:
    """
    Loads F_SUBAREA.txt file and returns a dictionary indexed by arodes_int_num.
    
    Args:
        file_path: Path to F_SUBAREA.txt
    
    Returns:
        Dictionary: {arodes_int_num: {field1: value1, ...}}
    """
    rows = load_txt_file(file_path)
    result = {}
    
    for row in rows:
        arodes_int_num = row.get('arodes_int_num')
        if arodes_int_num:
            try:
                arodes_int_num = int(arodes_int_num)
                result[arodes_int_num] = {
                    'area_type_cd': row.get('area_type_cd'),
                    'site_type_cd': row.get('site_type_cd'),
                    'silviculture_cd': row.get('silviculture_cd'),
                    'stand_struct_cd': row.get('stand_struct_cd'),
                    'forest_func_cd': row.get('forest_func_cd'),
                    'rotation_age': row.get('rotation_age'),
                    'sub_area': row.get('sub_area'),
                    'a_year': row.get('a_year'),
                }
            except (ValueError, TypeError):
                continue
    
    logger.log("INFO", f"Loaded {len(result)} records from F_SUBAREA")
    return result


def load_f_arodes(file_path: Path) -> Dict[int, Dict]:
    """
    Loads F_ARODES.txt file and returns a dictionary indexed by arodes_int_num.
    
    Args:
        file_path: Path to F_ARODES.txt
    
    Returns:
        Dictionary: {arodes_int_num: {adr_for: value, ...}}
    """
    rows = load_txt_file(file_path)
    result = {}
    
    for row in rows:
        arodes_int_num = row.get('arodes_int_num')
        if arodes_int_num:
            try:
                arodes_int_num = int(arodes_int_num)
                result[arodes_int_num] = {
                    'adr_for': row.get('adress_forest'),
                    'a_year': row.get('a_year'),
                }
            except (ValueError, TypeError):
                continue
    
    logger.log("INFO", f"Loaded {len(result)} records from F_ARODES")
    return result


def load_f_arod_category(file_path: Path) -> Dict[int, str]:
    """
    Loads F_AROD_CATEGORY.txt and returns dominant protection category (prot_rank_order = 1).
    
    Args:
        file_path: Path to F_AROD_CATEGORY.txt
    
    Returns:
        Dictionary: {arodes_int_num: prot_category_cd}
    """
    rows = load_txt_file(file_path)
    result = {}
    
    for row in rows:
        arodes_int_num = row.get('arodes_int_num')
        prot_rank_order = row.get('prot_rank_order')
        prot_category_cd = row.get('prot_category_cd')
        
        if arodes_int_num and prot_rank_order == '1' and prot_category_cd:
            try:
                arodes_int_num = int(arodes_int_num)
                # Only keep the dominant category (rank = 1)
                if arodes_int_num not in result:
                    result[arodes_int_num] = prot_category_cd
            except (ValueError, TypeError):
                continue
    
    logger.log("INFO", f"Loaded {len(result)} dominant protection categories from F_AROD_CATEGORY")
    return result


def load_f_storey_species(file_path: Path) -> Dict[int, Dict]:
    """
    Loads F_STOREY_SPECIES.txt and returns dominant species information.
    According to documentation, dominant species has:
    - ST_RANK_ORDER_ACT = 1 (in F_AROD_STOREY)
    - SP_RANK_ORDER_ACT = 1 (in F_STOREY_SPECIES)
    - STOREY_CD in ('DRZEW', 'IP', 'IIP')
    
    For now, we'll get species with SP_RANK_ORDER_ACT = 1.
    Full implementation would require joining with F_AROD_STOREY.
    
    Args:
        file_path: Path to F_STOREY_SPECIES.txt
    
    Returns:
        Dictionary: {arodes_int_num: {species_cd, spec_age, part_cd}}
    """
    rows = load_txt_file(file_path)
    result = {}
    
    for row in rows:
        arodes_int_num = row.get('arodes_int_num')
        sp_rank_order_act = row.get('sp_rank_order_act')
        
        # Only process dominant species (rank = 1)
        if arodes_int_num and sp_rank_order_act == '1':
            try:
                arodes_int_num = int(arodes_int_num)
                result[arodes_int_num] = {
                    'species_cd': row.get('species_cd'),
                    'species_age': row.get('species_age'),
                    'part_cd_act': row.get('part_cd_act'),
                }
            except (ValueError, TypeError):
                continue
    
    logger.log("INFO", f"Loaded {len(result)} dominant species from F_STOREY_SPECIES")
    return result


def load_f_inspectorate(file_path: Path) -> Dict[int, str]:
    """
    Loads F_INSPECTORATE.txt and returns inspectorate names.
    
    Args:
        file_path: Path to F_INSPECTORATE.txt
    
    Returns:
        Dictionary: {arodes_int_num: inspectorate_name}
    """
    rows = load_txt_file(file_path)
    result = {}
    
    for row in rows:
        arodes_int_num = row.get('arodes_int_num')
        inspectorate_name = row.get('inspectorate_name')
        
        if arodes_int_num and inspectorate_name:
            try:
                arodes_int_num = int(arodes_int_num)
                result[arodes_int_num] = inspectorate_name
            except (ValueError, TypeError):
                continue
    
    logger.log("INFO", f"Loaded {len(result)} inspectorate names from F_INSPECTORATE")
    return result


def _find_file(directory: Path, filename_variants: List[str]) -> Optional[Path]:
    """
    Finds a file in directory trying different filename variants (case-insensitive).
    
    Args:
        directory: Directory to search
        filename_variants: List of possible filenames (e.g., ['f_subarea.txt', 'F_SUBAREA.txt'])
    
    Returns:
        Path to found file or None
    """
    for variant in filename_variants:
        path = directory / variant
        if path.exists():
            return path
    return None


def load_all_descriptive_data(directory: Path) -> Dict:
    """
    Loads all descriptive TXT files from a directory and returns combined data.
    Handles both lowercase and uppercase filenames.
    
    Args:
        directory: Directory containing TXT files
    
    Returns:
        Dictionary with all loaded data indexed by arodes_int_num
    """
    result = defaultdict(dict)
    
    # Load F_SUBAREA (try both lowercase and uppercase)
    f_subarea_path = _find_file(directory, ['f_subarea.txt', 'F_SUBAREA.txt'])
    if f_subarea_path:
        f_subarea = load_f_subarea(f_subarea_path)
        for arodes_int_num, data in f_subarea.items():
            result[arodes_int_num].update(data)
    
    # Load F_ARODES (for adr_for if missing)
    f_arodes_path = _find_file(directory, ['f_arodes.txt', 'F_ARODES.txt'])
    if f_arodes_path:
        f_arodes = load_f_arodes(f_arodes_path)
        for arodes_int_num, data in f_arodes.items():
            if 'adr_for' not in result[arodes_int_num]:
                result[arodes_int_num].update(data)
    
    # Load F_AROD_CATEGORY (dominant protection category)
    f_arod_category_path = _find_file(directory, ['f_arod_category.txt', 'F_AROD_CATEGORY.txt'])
    if f_arod_category_path:
        f_arod_category = load_f_arod_category(f_arod_category_path)
        for arodes_int_num, prot_categ in f_arod_category.items():
            result[arodes_int_num]['prot_category_cd'] = prot_categ
    
    # Load F_STOREY_SPECIES (dominant species)
    f_storey_species_path = _find_file(directory, ['f_storey_species.txt', 'F_STOREY_SPECIES.txt'])
    if f_storey_species_path:
        f_storey_species = load_f_storey_species(f_storey_species_path)
        for arodes_int_num, species_data in f_storey_species.items():
            result[arodes_int_num].update({
                'species_cd': species_data.get('species_cd'),
                'spec_age': species_data.get('species_age'),
                'part_cd': species_data.get('part_cd_act'),
            })
    
    # Load F_INSPECTORATE (for forest_range_name)
    f_inspectorate_path = _find_file(directory, ['f_inspectorate.txt', 'F_INSPECTORATE.txt'])
    if f_inspectorate_path:
        f_inspectorate = load_f_inspectorate(f_inspectorate_path)
        for arodes_int_num, inspectorate_name in f_inspectorate.items():
            result[arodes_int_num]['inspectorate_name'] = inspectorate_name
    
    logger.log("INFO", f"Loaded descriptive data for {len(result)} records")
    return dict(result)


def merge_geometry_with_descriptive_data(
    geometry_data: Dict,
    descriptive_data: Dict[int, Dict]
) -> Dict:
    """
    Merges geometry data from G_COMPARTMENT with descriptive data from F_* tables.
    
    Args:
        geometry_data: Dictionary with geometry data (from G_COMPARTMENT GeoJSON)
        descriptive_data: Dictionary with descriptive data indexed by arodes_int_num
    
    Returns:
        Merged dictionary with all fields
    """
    properties = geometry_data.get('properties', {})
    a_i_num = properties.get('a_i_num')
    
    if not a_i_num:
        return geometry_data
    
    try:
        a_i_num = int(a_i_num)
    except (ValueError, TypeError):
        return geometry_data
    
    # Get descriptive data for this a_i_num
    desc_data = descriptive_data.get(a_i_num, {})
    
    # Merge properties
    merged_properties = properties.copy()
    
    # Map F_SUBAREA fields to our schema
    if desc_data.get('area_type_cd'):
        merged_properties['area_type'] = desc_data['area_type_cd']
    if desc_data.get('site_type_cd'):
        merged_properties['site_type'] = desc_data['site_type_cd']
    if desc_data.get('silviculture_cd'):
        merged_properties['silvicult'] = desc_data['silviculture_cd']
    if desc_data.get('stand_struct_cd'):
        merged_properties['stand_stru'] = desc_data['stand_struct_cd']
    if desc_data.get('forest_func_cd'):
        merged_properties['forest_fun'] = desc_data['forest_func_cd']
    if desc_data.get('rotation_age'):
        try:
            merged_properties['rotat_age'] = int(desc_data['rotation_age'])
        except (ValueError, TypeError):
            pass
    if desc_data.get('sub_area'):
        try:
            merged_properties['sub_area'] = float(desc_data['sub_area'])
        except (ValueError, TypeError):
            pass
    if desc_data.get('prot_category_cd'):
        merged_properties['prot_categ'] = desc_data['prot_category_cd']
    if desc_data.get('species_cd'):
        merged_properties['species_cd'] = desc_data['species_cd']
    if desc_data.get('spec_age'):
        try:
            merged_properties['spec_age'] = int(desc_data['spec_age'])
        except (ValueError, TypeError):
            pass
    if desc_data.get('part_cd'):
        merged_properties['part_cd'] = desc_data['part_cd']
    if desc_data.get('adr_for') and not merged_properties.get('adr_for'):
        merged_properties['adr_for'] = desc_data['adr_for']
    if desc_data.get('inspectorate_name'):
        merged_properties['forest_range_name'] = desc_data['inspectorate_name']
    
    # Create merged geometry data
    merged = geometry_data.copy()
    merged['properties'] = merged_properties
    
    return merged

