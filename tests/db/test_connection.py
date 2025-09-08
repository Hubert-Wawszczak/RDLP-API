import unittest
from unittest.mock import AsyncMock, patch, MagicMock
from db.connection import DBConnection

class TestDBConnection(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.db = DBConnection("dev")

    @patch("db.connection.asyncpg.connect", new_callable=AsyncMock)
    @patch("db.connection.Settings")
    async def test_connect_success(self, mock_settings, mock_connect):
        mock_conn = AsyncMock()
        mock_conn.fetchval.return_value = True
        mock_conn.is_closed.return_value = False
        mock_connect.return_value = mock_conn
        self.db._DBConnection__connection = None

        result = await self.db.connect()
        self.assertTrue(result)
        self.assertTrue(await self.db.is_connected())

    @patch("db.connection.asyncpg.connect", new_callable=AsyncMock)
    @patch("db.connection.Settings")
    async def test_connect_already_connected(self, mock_settings, mock_connect):
        mock_conn = AsyncMock()
        mock_conn.is_closed.return_value = False
        self.db._DBConnection__connection = mock_conn

        result = await self.db.connect()
        self.assertTrue(result)
        mock_connect.assert_not_called()

    @patch("db.connection.asyncpg.create_pool", new_callable=AsyncMock)
    @patch("db.connection.Settings")
    async def test_create_pool_success(self, mock_settings, mock_create_pool):
        mock_pool = AsyncMock()
        mock_create_pool.return_value = mock_pool
        self.db._DBConnection__pool = None

        pool = await self.db.create_pool()
        self.assertIs(pool, mock_pool)

    async def test_is_connected_false(self):
        self.db._DBConnection__connection = None
        self.assertFalse(await self.db.is_connected())

    async def test_execute_query_not_connected(self):
        self.db._DBConnection__connection = None
        with self.assertRaises(RuntimeError):
            await self.db.execute_query("SELECT 1;")

    async def test_execute_command_not_connected(self):
        self.db._DBConnection__connection = None
        with self.assertRaises(RuntimeError):
            await self.db.execute_command("UPDATE ...")

    @patch("db.connection.logger.log")
    async def test_close_already_closed(self, mock_log):
        self.db._DBConnection__connection = None
        await self.db.close()
        mock_log.assert_called()

if __name__ == "__main__":
    unittest.main()