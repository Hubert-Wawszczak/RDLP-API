import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import unittest
from unittest.mock import AsyncMock, patch, MagicMock
import asyncio

from services.api_client import APIClient
from services.loader import DataLoader
from db.connection import DBConnection


class TestIntegration(unittest.IsolatedAsyncioTestCase):
    """Integration tests for the full data pipeline"""

    def setUp(self):
        self.data_dir = Path("tests/data")
        self.api_client = APIClient(save_dir=self.data_dir)
        self.loader = DataLoader(self.data_dir)

    @patch("services.loader.DBConnection")
    @patch("services.api_client.aiohttp.ClientSession")
    async def test_full_pipeline_success(self, mock_session, mock_db_conn):
        """Test the complete pipeline from API fetch to database insert"""
        # Mock API response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"numberMatched": 100})
        mock_response.text = AsyncMock(return_value='{"features": []}')
        mock_session.get.return_value.__aenter__.return_value = mock_response

        # Mock file operations
        with patch("services.api_client.aiofiles.open") as mock_aiofiles:
            mock_file = AsyncMock()
            mock_aiofiles.return_value.__aenter__.return_value = mock_file

            # Fetch data
            endpoints = ["RDLP_Bialystok_wydzielenia"]
            with patch("services.api_client.get_args", return_value=("all", "RDLP_Bialystok_wydzielenia")):
                with patch("services.api_client.APIClient._APIClient__fetch_everything", new_callable=AsyncMock) as mock_fetch:
                    mock_fetch.return_value = True
                    result = await self.api_client.fetch_data(endpoints, limit=1000)
                    self.assertTrue(result)

        # Mock database operations
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire.return_value = mock_conn
        self.loader.pool = mock_pool

        # Mock file processing
        with patch("services.loader.aiofiles.open") as mock_aiofiles:
            with patch("services.loader.json.loads") as mock_json:
                mock_file = AsyncMock()
                mock_file.read = AsyncMock(return_value='{"features": []}')
                mock_aiofiles.return_value.__aenter__.return_value = mock_file
                mock_json.return_value = {"features": []}

                async def empty_batch():
                    yield []
                
                self.loader._DataLoader__batch_process_files = empty_batch()
                await self.loader.insert_data()

    @patch("services.loader.DBConnection")
    async def test_loader_with_valid_data(self, mock_db_conn):
        """Test loader with valid data structure"""
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        mock_pool.acquire.return_value = mock_conn
        self.loader.pool = mock_pool

        # Create mock validated data
        validated_data = {
            "id": 1,
            "adr_for": "test123",
            "rdlp_name": "bialystok",
            "forest_range_name": "Test Range",
            "geometry": None
        }

        with patch("services.loader.aiofiles.open") as mock_aiofiles:
            with patch("services.loader.json.loads") as mock_json:
                mock_file = AsyncMock()
                mock_file.read = AsyncMock(return_value='{"features": []}')
                mock_aiofiles.return_value.__aenter__.return_value = mock_file
                mock_json.return_value = {"features": []}

                async def batch_with_data():
                    yield [[validated_data]]
                
                self.loader._DataLoader__batch_process_files = batch_with_data()
                await self.loader.insert_data()

    async def test_api_client_and_loader_integration(self):
        """Test that API client and loader work together"""
        # Mock API client to create a file
        with patch("services.api_client.aiohttp.ClientSession") as mock_session:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value={"numberMatched": 50})
            mock_response.text = AsyncMock(return_value='{"features": [{"id": 1, "properties": {"adr_for": "test123"}}]}')
            mock_session.get.return_value.__aenter__.return_value = mock_response

            with patch("services.api_client.aiofiles.open") as mock_aiofiles:
                mock_file = AsyncMock()
                mock_aiofiles.return_value.__aenter__.return_value = mock_file

                # Test that files are created
                endpoints = ["RDLP_Bialystok_wydzielenia"]
                with patch("services.api_client.get_args", return_value=("all", "RDLP_Bialystok_wydzielenia")):
                    with patch("services.api_client.APIClient._APIClient__fetch_everything", new_callable=AsyncMock) as mock_fetch:
                        mock_fetch.return_value = True
                        result = await self.api_client.fetch_data(endpoints, limit=1000)
                        self.assertTrue(result)

    @patch("db.connection.asyncpg.connect")
    @patch("db.connection.asyncpg.create_pool")
    async def test_db_connection_integration(self, mock_create_pool, mock_connect):
        """Test database connection integration"""
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=1)
        mock_conn.is_closed = AsyncMock(return_value=False)
        mock_connect.return_value = mock_conn

        mock_pool = AsyncMock()
        mock_create_pool.return_value = mock_pool

        db = DBConnection()
        await db.connect()
        self.assertTrue(await db.is_connected())

        pool = await db.create_pool()
        self.assertIsNotNone(pool)

        await db.close()
        self.assertFalse(await db.is_connected())

if __name__ == "__main__":
    unittest.main()

