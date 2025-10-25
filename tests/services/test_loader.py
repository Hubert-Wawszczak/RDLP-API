import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path
import asyncio

from services.loader import DataLoader

class TestDataLoader(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.data_dir = Path("tests/data")
        self.loader = DataLoader(self.data_dir)
        self.sample_file = self.data_dir / "RDLP_Bialystok_wydzielenia.json"
        self.sample_data = {
            "features": [
                {
                    "id": 1,
                    "properties": {
                        "area_type": "type1",
                        "a_i_num": "num1",
                        "silvicult": "silv",
                        "stand_stru": "stru",
                        "sub_area": "sub",
                        "species_cd": "spc",
                        "spec_age": 10,
                        "nazwa": "name",
                        "adr_for": "adr",
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


    def test_validate_data_invalid(self):
        with patch("services.loader.logger"):
            result = self.loader._DataLoader__validate_data({}, self.sample_file)
            self.assertEqual(result, [])

    @patch("services.loader.DBConnection")
    async def test_insert_data_empty(self, mock_db_conn):
        self.loader.pool = AsyncMock()
        async def empty_async_gen():
            return
            yield

        self.loader._DataLoader__batch_process_files = empty_async_gen
        await self.loader.insert_data()
        mock_db_conn.assert_not_called()

if __name__ == "__main__":
    unittest.main()