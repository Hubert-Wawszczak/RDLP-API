import unittest
from unittest.mock import AsyncMock, patch, MagicMock
from pathlib import Path
from services.api_client import APIClient

class TestAPIClient(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = APIClient()

    @patch("services.api_client.aiohttp.ClientSession")
    @patch("services.api_client.APIClient._APIClient__fetch_data_page", new_callable=AsyncMock)
    @patch("services.api_client.APIClient._APIClient__get_item_total", new_callable=AsyncMock)
    async def test_fetch_everything_batches(self, mock_get_total, mock_fetch_page, mock_session):
        mock_get_total.return_value = 2500
        mock_fetch_page.return_value = True
        result = await self.client._APIClient__fetch_everything(mock_session, "RDLP_Bialystok_wydzielenia", limit=1000)
        self.assertTrue(result)
        self.assertEqual(mock_fetch_page.await_count, 4)  # 1 initial + 3 batches

    @patch("services.api_client.aiohttp.ClientSession")
    @patch("services.api_client.APIClient._APIClient__fetch_data_page", new_callable=AsyncMock)
    @patch("services.api_client.APIClient._APIClient__get_item_total", new_callable=AsyncMock)
    async def test_fetch_everything_single_page(self, mock_get_total, mock_fetch_page, mock_session):
        mock_get_total.return_value = 500
        mock_fetch_page.return_value = True
        result = await self.client._APIClient__fetch_everything(mock_session, "RDLP_Bialystok_wydzielenia", limit=1000)
        self.assertTrue(result)
        self.assertEqual(mock_fetch_page.await_count, 1)  # Only initial page

    @patch("services.api_client.aiohttp.ClientSession")
    @patch("services.api_client.APIClient._APIClient__fetch_data_page", new_callable=AsyncMock)
    @patch("services.api_client.APIClient._APIClient__get_item_total", new_callable=AsyncMock)
    async def test_fetch_everything_exact_limit(self, mock_get_total, mock_fetch_page, mock_session):
        mock_get_total.return_value = 1000
        mock_fetch_page.return_value = True
        result = await self.client._APIClient__fetch_everything(mock_session, "RDLP_Bialystok_wydzielenia", limit=1000)
        self.assertTrue(result)
        self.assertEqual(mock_fetch_page.await_count, 1)  # Only initial page

    @patch("services.api_client.aiohttp.ClientSession")
    @patch("services.api_client.APIClient._APIClient__get_item_total", new_callable=AsyncMock)
    async def test_get_item_total_success(self, mock_get_total, mock_session):
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"numberMatched": 1500})
        mock_session.get.return_value.__aenter__.return_value = mock_response
        
        result = await self.client._APIClient__get_item_total(mock_session, "http://test.com")
        self.assertEqual(result, 1500)

    @patch("services.api_client.aiohttp.ClientSession")
    @patch("services.api_client.APIClient._APIClient__get_item_total", new_callable=AsyncMock)
    async def test_get_item_total_failure(self, mock_get_total, mock_session):
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_session.get.return_value.__aenter__.return_value = mock_response
        
        result = await self.client._APIClient__get_item_total(mock_session, "http://test.com")
        self.assertEqual(result, 0)

    @patch("services.api_client.aiofiles.open")
    @patch("services.api_client.aiohttp.ClientSession")
    async def test_fetch_data_page_success(self, mock_session, mock_aiofiles):
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text = AsyncMock(return_value='{"features": []}')
        mock_session.get.return_value.__aenter__.return_value = mock_response
        
        mock_file = AsyncMock()
        mock_aiofiles.return_value.__aenter__.return_value = mock_file
        
        result = await self.client._APIClient__fetch_data_page(mock_session, "http://test.com/endpoint")
        self.assertTrue(result)
        mock_file.write.assert_awaited_once()

    @patch("services.api_client.aiohttp.ClientSession")
    async def test_fetch_data_page_failure(self, mock_session):
        mock_response = AsyncMock()
        mock_response.status = 404
        mock_session.get.return_value.__aenter__.return_value = mock_response
        
        result = await self.client._APIClient__fetch_data_page(mock_session, "http://test.com/endpoint")
        self.assertFalse(result)

    @patch("services.api_client.aiohttp.ClientSession")
    @patch("services.api_client.APIClient._APIClient__fetch_everything", new_callable=AsyncMock)
    async def test_fetch_data_all(self, mock_fetch_everything, mock_session):
        mock_fetch_everything.return_value = True
        endpoints = ["all"]
        with patch("services.api_client.get_args", return_value=("all", "RDLP_Bialystok_wydzielenia")):
            result = await self.client.fetch_data(endpoints, limit=1000)
        self.assertTrue(result)
        self.assertGreater(mock_fetch_everything.await_count, 0)

    @patch("services.api_client.aiohttp.ClientSession")
    @patch("services.api_client.APIClient._APIClient__fetch_everything", new_callable=AsyncMock)
    async def test_fetch_data_single_endpoint(self, mock_fetch_everything, mock_session):
        mock_fetch_everything.return_value = True
        endpoints = ["RDLP_Bialystok_wydzielenia"]
        with patch("services.api_client.get_args", return_value=("all", "RDLP_Bialystok_wydzielenia")):
            result = await self.client.fetch_data(endpoints, limit=1000)
        self.assertTrue(result)
        mock_fetch_everything.assert_awaited_once()

    @patch("services.api_client.aiohttp.ClientSession")
    @patch("services.api_client.APIClient._APIClient__fetch_everything", new_callable=AsyncMock)
    async def test_fetch_data_multiple_endpoints(self, mock_fetch_everything, mock_session):
        mock_fetch_everything.return_value = True
        endpoints = ["RDLP_Bialystok_wydzielenia", "RDLP_Lublin_wydzielenia"]
        with patch("services.api_client.get_args", return_value=("all", "RDLP_Bialystok_wydzielenia", "RDLP_Lublin_wydzielenia")):
            result = await self.client.fetch_data(endpoints, limit=1000)
        self.assertTrue(result)
        self.assertEqual(mock_fetch_everything.await_count, 2)

    @patch("services.api_client.aiohttp.ClientSession")
    async def test_fetch_data_invalid_endpoint(self, mock_session):
        endpoints = ["invalid_endpoint"]
        with patch("services.api_client.get_args", return_value=("all", "RDLP_Bialystok_wydzielenia")):
            with self.assertRaises(AssertionError):
                await self.client.fetch_data(endpoints, limit=1000)

    @patch("services.api_client.aiohttp.ClientSession")
    @patch("services.api_client.APIClient._APIClient__fetch_everything", new_callable=AsyncMock)
    async def test_fetch_data_exception_handling(self, mock_fetch_everything, mock_session):
        mock_fetch_everything.side_effect = Exception("Network error")
        endpoints = ["RDLP_Bialystok_wydzielenia"]
        with patch("services.api_client.get_args", return_value=("all", "RDLP_Bialystok_wydzielenia")):
            result = await self.client.fetch_data(endpoints, limit=1000)
        self.assertFalse(result)

    def test_init_with_custom_save_dir(self):
        custom_dir = Path("/custom/path")
        client = APIClient(save_dir=custom_dir)
        self.assertEqual(client.save_dir, custom_dir)

    def test_init_creates_save_dir(self):
        with patch("pathlib.Path.mkdir") as mock_mkdir:
            client = APIClient()
            mock_mkdir.assert_called_once_with(exist_ok=True)

if __name__ == "__main__":
    unittest.main()