import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import unittest
from utils.validators import RDLPData


class TestRDLPData(unittest.TestCase):
    """Test cases for RDLPData validator"""

    def test_rdlp_data_minimal_valid(self):
        """Test RDLPData with only required fields"""
        data = {
            "id": 1,
            "adr_for": "test123",
            "forest_range_name": "Test Range",
            "rdlp_name": "bialystok"
        }
        result = RDLPData(**data)
        self.assertEqual(result.id, 1)
        self.assertEqual(result.adr_for, "test123")
        self.assertEqual(result.forest_range_name, "Test Range")
        self.assertEqual(result.rdlp_name, "bialystok")

    def test_rdlp_data_all_fields(self):
        """Test RDLPData with all fields populated"""
        data = {
            "id": 1,
            "area_type": "type1",
            "a_i_num": 123,
            "silvicult": "silv",
            "stand_stru": "stru",
            "sub_area": 10.5,
            "species_cd": "spc",
            "spec_age": 25,
            "nazwa": "Test Name",
            "adr_for": "test123",
            "site_type": "site",
            "forest_fun": "fun",
            "rotat_age": 30,
            "prot_categ": "cat",
            "part_cd": "part",
            "a_year": 2020,
            "geometry": {"type": "MultiPolygon", "coordinates": []},
            "forest_range_name": "Test Range",
            "rdlp_name": "krakow"
        }
        result = RDLPData(**data)
        self.assertEqual(result.area_type, "type1")
        self.assertEqual(result.a_i_num, 123)
        self.assertEqual(result.sub_area, 10.5)
        self.assertEqual(result.spec_age, 25)
        self.assertEqual(result.rotat_age, 30)
        self.assertEqual(result.a_year, 2020)

    def test_rdlp_data_optional_fields_none(self):
        """Test RDLPData with all optional fields as None"""
        data = {
            "id": 1,
            "adr_for": "test123",
            "forest_range_name": "Test Range",
            "rdlp_name": "warszawa",
            "area_type": None,
            "a_i_num": None,
            "silvicult": None,
            "stand_stru": None,
            "sub_area": None,
            "species_cd": None,
            "spec_age": None,
            "nazwa": None,
            "site_type": None,
            "forest_fun": None,
            "rotat_age": None,
            "prot_categ": None,
            "part_cd": None,
            "a_year": None,
            "geometry": None
        }
        result = RDLPData(**data)
        self.assertIsNone(result.area_type)
        self.assertIsNone(result.geometry)

    def test_rdlp_data_missing_required_id(self):
        """Test RDLPData validation fails without id"""
        data = {
            "adr_for": "test123",
            "forest_range_name": "Test Range",
            "rdlp_name": "bialystok"
        }
        with self.assertRaises(Exception):
            RDLPData(**data)

    def test_rdlp_data_missing_required_adr_for(self):
        """Test RDLPData validation fails without adr_for"""
        data = {
            "id": 1,
            "forest_range_name": "Test Range",
            "rdlp_name": "bialystok"
        }
        with self.assertRaises(Exception):
            RDLPData(**data)

    def test_rdlp_data_missing_required_forest_range_name(self):
        """Test RDLPData validation fails without forest_range_name"""
        data = {
            "id": 1,
            "adr_for": "test123",
            "rdlp_name": "bialystok"
        }
        with self.assertRaises(Exception):
            RDLPData(**data)

    def test_rdlp_data_missing_required_rdlp_name(self):
        """Test RDLPData validation fails without rdlp_name"""
        data = {
            "id": 1,
            "adr_for": "test123",
            "forest_range_name": "Test Range"
        }
        with self.assertRaises(Exception):
            RDLPData(**data)

    def test_rdlp_data_numeric_types(self):
        """Test RDLPData with various numeric types"""
        data = {
            "id": 1,
            "adr_for": "test123",
            "forest_range_name": "Test Range",
            "rdlp_name": "bialystok",
            "a_i_num": 999,
            "sub_area": 123.456,
            "spec_age": 50,
            "rotat_age": 100,
            "a_year": 2024
        }
        result = RDLPData(**data)
        self.assertIsInstance(result.a_i_num, int)
        self.assertIsInstance(result.sub_area, float)
        self.assertIsInstance(result.spec_age, int)

    def test_rdlp_data_geometry_geojson(self):
        """Test RDLPData with GeoJSON geometry"""
        geometry = {
            "type": "MultiPolygon",
            "coordinates": [
                [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
            ]
        }
        data = {
            "id": 1,
            "adr_for": "test123",
            "forest_range_name": "Test Range",
            "rdlp_name": "bialystok",
            "geometry": geometry
        }
        result = RDLPData(**data)
        self.assertEqual(result.geometry, geometry)
        self.assertEqual(result.geometry["type"], "MultiPolygon")

    def test_rdlp_data_empty_strings(self):
        """Test RDLPData with empty strings for optional fields"""
        data = {
            "id": 1,
            "adr_for": "test123",
            "forest_range_name": "Test Range",
            "rdlp_name": "bialystok",
            "area_type": "",
            "nazwa": ""
        }
        result = RDLPData(**data)
        self.assertEqual(result.area_type, "")
        self.assertEqual(result.nazwa, "")

    def test_rdlp_data_all_rdlp_names(self):
        """Test RDLPData with different RDLP names"""
        rdlp_names = [
            "bialystok", "katowice", "krakow", "krosno", "lublin",
            "lodz", "olsztyn", "pila", "poznan", "szczecin",
            "szczecinek", "torun", "wroclaw", "zielona_gora",
            "gdansk", "radom", "warszawa"
        ]
        
        for rdlp_name in rdlp_names:
            data = {
                "id": 1,
                "adr_for": f"test_{rdlp_name}",
                "forest_range_name": "Test Range",
                "rdlp_name": rdlp_name
            }
            result = RDLPData(**data)
            self.assertEqual(result.rdlp_name, rdlp_name)

if __name__ == "__main__":
    unittest.main()

