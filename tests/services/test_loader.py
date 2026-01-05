import unittest
from unittest.mock import AsyncMock, MagicMock, patch, call
from pathlib import Path
import asyncio
import json

from services.loader import DataLoader
from utils.validators import RDLPData

class TestDataLoader(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.data_dir = Path("tests/data")
        self.loader = DataLoader(self.data_dir)
        self.sample_file = Path("RDLP_Bialystok_wydzielenia.json")
        self.sample_data = {
            "features": [
                {
                    "id": 1,
                    "properties": {
                        "area_type": "type1",
                        "a_i_num": 123,
                        "silvicult": "silv",
                        "stand_stru": "stru",
                        "sub_area": 10.5,
                        "species_cd": "spc",
                        "spec_age": 10,
                        "nazwa": "name",
                        "adr_for": "adr123",
                        "site_type": "site",
                        "forest_fun": "fun",
                        "rotat_age": 20,
                        "prot_categ": "cat",
                        "part_cd": "part",
                        "a_year": 2020
                    },
                    "geometry": {
                        "type": "MultiPolygon",
                        "coordinates": [[[[0,0],[1,0],[1,1],[0,1],[0,0]]]]
                    }
                }
            ]
        }

    @patch("services.loader.aiofiles.open")
    @patch("services.loader.json.loads")
    async def test_process_file_valid(self, mock_json_loads, mock_aio_open):
        mock_file = AsyncMock()
        mock_file.read.return_value = "{}"
        mock_aio_open.return_value.__aenter__.return_value = mock_file
        mock_json_loads.return_value = self.sample_data

        with patch.object(self.loader, "_DataLoader__validate_data", return_value=MagicMock()):
            result = await self.loader._DataLoader__process_file(self.sample_file)
            self.assertEqual(len(result), 1)

    @patch("services.loader.aiofiles.open", side_effect=FileNotFoundError)
    async def test_process_file_not_found(self, mock_aio_open):
        result = await self.loader._DataLoader__process_file(self.sample_file)
        self.assertEqual(result, [])

    @patch("services.loader.aiofiles.open")
    @patch("services.loader.json.loads", side_effect=json.JSONDecodeError("Invalid JSON", "", 0))
    async def test_process_file_invalid_json(self, mock_json_loads, mock_aio_open):
        mock_file = AsyncMock()
        mock_file.read.return_value = "invalid json"
        mock_aio_open.return_value.__aenter__.return_value = mock_file
        
        result = await self.loader._DataLoader__process_file(self.sample_file)
        self.assertEqual(result, [])

    def test_validate_data_invalid(self):
        with patch("services.loader.logger"):
            result = self.loader._DataLoader__validate_data({}, self.sample_file)
            self.assertEqual(result, [])

    def test_validate_data_missing_id(self):
        with patch("services.loader.logger"):
            data = {"properties": {"adr_for": "test123"}}
            result = self.loader._DataLoader__validate_data(data, self.sample_file)
            self.assertEqual(result, [])

    def test_validate_data_missing_adr_for(self):
        with patch("services.loader.logger"):
            data = {"id": 1, "properties": {}}
            result = self.loader._DataLoader__validate_data(data, self.sample_file)
            self.assertEqual(result, [])

    def test_validate_data_valid_minimal(self):
        with patch("services.loader.logger"):
            data = {
                "id": 1,
                "properties": {
                    "adr_for": "test123"
                }
            }
            result = self.loader._DataLoader__validate_data(data, self.sample_file)
            self.assertIsInstance(result, RDLPData)
            self.assertEqual(result.id, 1)
            self.assertEqual(result.adr_for, "test123")
            self.assertEqual(result.rdlp_name, "bialystok")

    def test_validate_data_valid_full(self):
        with patch("services.loader.logger"):
            data = {
                "id": 1,
                "properties": {
                    "area_type": "type1",
                    "a_i_num": 123,
                    "adr_for": "test123",
                    "nazwa": "Test Name",
                    "forest_range_name": "Test Range"
                },
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [[[[0,0],[1,0],[1,1],[0,1],[0,0]]]]
                }
            }
            result = self.loader._DataLoader__validate_data(data, self.sample_file)
            self.assertIsInstance(result, RDLPData)
            self.assertEqual(result.area_type, "type1")
            self.assertEqual(result.a_i_num, 123)
            self.assertEqual(result.forest_range_name, "Test Range")

    def test_validate_data_forest_range_name_fallback(self):
        with patch("services.loader.logger"):
            data = {
                "id": 1,
                "properties": {
                    "adr_for": "test123",
                    "nazwa": "Fallback Name"
                }
            }
            result = self.loader._DataLoader__validate_data(data, self.sample_file)
            self.assertEqual(result.forest_range_name, "Fallback Name")

    def test_validate_data_forest_range_name_default(self):
        with patch("services.loader.logger"):
            data = {
                "id": 1,
                "properties": {
                    "adr_for": "test123"
                }
            }
            result = self.loader._DataLoader__validate_data(data, self.sample_file)
            self.assertEqual(result.forest_range_name, "unknown")

    def test_validate_data_invalid_filename(self):
        with patch("services.loader.logger"):
            invalid_file = Path("invalid_filename.json")
            data = {"id": 1, "properties": {"adr_for": "test123"}}
            result = self.loader._DataLoader__validate_data(data, invalid_file)
            self.assertEqual(result, [])

    async def test_get_single_item(self):
        data = {"features": [{"id": 1}, {"id": 2}, {"id": 3}]}
        items = []
        async for item in self.loader._DataLoader__get_single_item(data):
            items.append(item)
        self.assertEqual(len(items), 3)
        self.assertEqual(items[0]["id"], 1)

    async def test_get_single_item_empty(self):
        data = {"features": []}
        items = []
        async for item in self.loader._DataLoader__get_single_item(data):
            items.append(item)
        self.assertEqual(len(items), 0)

    async def test_get_single_item_no_features(self):
        data = {}
        items = []
        async for item in self.loader._DataLoader__get_single_item(data):
            items.append(item)
        self.assertEqual(len(items), 0)

    @patch("services.loader.DBConnection")
    async def test_insert_data_empty(self, mock_db_conn):
        self.loader.pool = AsyncMock()
        async def empty_async_gen():
            return
            yield

        self.loader._DataLoader__batch_process_files = empty_async_gen
        await self.loader.insert_data()
        mock_db_conn.assert_not_called()

    @patch("services.loader.shapely.geometry.shape")
    @patch("services.loader.shapely.wkt.dumps")
    async def test_insert_data_with_geometry(self, mock_wkt_dumps, mock_shape):
        self.loader.pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        self.loader.pool.acquire.return_value = mock_conn
        
        mock_wkt_dumps.return_value = "GEOMETRY_WKT"
        
        validated_items = [{
            "id": 1,
            "adr_for": "test123",
            "rdlp_name": "bialystok",
            "forest_range_name": "Test Range",
            "geometry": {"type": "MultiPolygon", "coordinates": [[[[0,0],[1,0],[1,1],[0,1],[0,0]]]]}
        }]
        
        async def batch_gen():
            yield [validated_items]
        
        self.loader._DataLoader__batch_process_files = batch_gen()
        
        await self.loader.insert_data()
        
        mock_conn.executemany.assert_called_once()

    @patch("services.loader.logger")
    async def test_batch_process_files(self, mock_logger):
        # Create temporary test files
        test_files = [
            self.data_dir / "RDLP_Bialystok_wydzielenia_0_123.json",
            self.data_dir / "RDLP_Krakow_wydzielenia_0_456.json"
        ]
        
        # Mock glob to return test files
        with patch.object(self.loader.data_dir, "glob", return_value=test_files):
            with patch.object(self.loader, "_DataLoader__process_single_batch", new_callable=AsyncMock) as mock_batch:
                mock_batch.return_value = [[], []]
                self.loader.batch_size = 1
                
                batches = []
                async for batch in self.loader._DataLoader__batch_process_files():
                    batches.append(batch)
                
                self.assertEqual(len(batches), 2)

if __name__ == "__main__":
    unittest.main()