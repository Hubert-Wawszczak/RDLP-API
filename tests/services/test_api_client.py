import unittest
from unittest.mock import AsyncMock, patch, MagicMock
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
        print("await count: "+str(mock_fetch_page.await_count))
        self.assertEqual(mock_fetch_page.await_count, 4)  # 1 initial + 2 batches

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

if __name__ == "__main__":
    unittest.main()