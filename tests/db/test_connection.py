import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import unittest
from unittest.mock import AsyncMock, patch, MagicMock
from db.connection import DBConnection
import asyncpg

class TestDBConnection(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.db = DBConnection()

    @patch("db.connection.asyncpg.connect", new_callable=AsyncMock)
    @patch("db.connection.Settings")
    async def test_connect_success(self, mock_settings, mock_connect):
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=1)
        mock_conn.is_closed = AsyncMock(return_value=False)
        mock_connect.return_value = mock_conn
        self.db._DBConnection__connection = None

        result = await self.db.connect()
        self.assertTrue(result)
        self.assertTrue(await self.db.is_connected())

    @patch("db.connection.asyncpg.connect", new_callable=AsyncMock)
    @patch("db.connection.Settings")
    async def test_connect_already_connected(self, mock_settings, mock_connect):
        mock_conn = AsyncMock()
        mock_conn.is_closed = AsyncMock(return_value=False)
        self.db._DBConnection__connection = mock_conn

        result = await self.db.connect()
        self.assertTrue(result)
        mock_connect.assert_not_called()

    @patch("db.connection.asyncpg.connect", new_callable=AsyncMock)
    @patch("db.connection.Settings")
    async def test_connect_failure(self, mock_settings, mock_connect):
        mock_connect.side_effect = asyncpg.exceptions.PostgresConnectionError("Connection failed")
        self.db._DBConnection__connection = None

        with self.assertRaises(asyncpg.exceptions.PostgresConnectionError):
            await self.db.connect()

    @patch("db.connection.asyncpg.connect", new_callable=AsyncMock)
    @patch("db.connection.Settings")
    async def test_connect_fetchval_false(self, mock_settings, mock_connect):
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=False)
        mock_conn.is_closed = AsyncMock(return_value=False)
        mock_connect.return_value = mock_conn
        self.db._DBConnection__connection = None

        result = await self.db.connect()
        self.assertFalse(result)

    @patch("db.connection.asyncpg.create_pool", new_callable=AsyncMock)
    @patch("db.connection.Settings")
    async def test_create_pool_success(self, mock_settings, mock_create_pool):
        mock_pool = AsyncMock()
        mock_create_pool.return_value = mock_pool
        self.db._DBConnection__pool = None

        pool = await self.db.create_pool(min_size=2, max_size=10)
        self.assertIs(pool, mock_pool)
        mock_create_pool.assert_called_once()

    @patch("db.connection.asyncpg.create_pool", new_callable=AsyncMock)
    @patch("db.connection.Settings")
    async def test_create_pool_already_exists(self, mock_settings, mock_create_pool):
        mock_pool = AsyncMock()
        self.db._DBConnection__pool = mock_pool

        pool = await self.db.create_pool()
        self.assertIs(pool, mock_pool)
        mock_create_pool.assert_not_called()

    @patch("db.connection.asyncpg.create_pool", new_callable=AsyncMock)
    @patch("db.connection.Settings")
    async def test_create_pool_failure(self, mock_settings, mock_create_pool):
        mock_create_pool.side_effect = asyncpg.exceptions.PostgresConnectionError("Pool creation failed")
        self.db._DBConnection__pool = None

        with self.assertRaises(asyncpg.exceptions.PostgresConnectionError):
            await self.db.create_pool()

    async def test_is_connected_false(self):
        self.db._DBConnection__connection = None
        self.assertFalse(await self.db.is_connected())

    async def test_is_connected_closed(self):
        mock_conn = AsyncMock()
        mock_conn.is_closed = AsyncMock(return_value=True)
        self.db._DBConnection__connection = mock_conn
        self.assertFalse(await self.db.is_connected())

    async def test_execute_query_not_connected(self):
        self.db._DBConnection__connection = None
        with self.assertRaises(RuntimeError):
            await self.db.execute_query("SELECT 1;")

    @patch("db.connection.logger.log")
    async def test_execute_query_success(self, mock_log):
        mock_conn = AsyncMock()
        mock_conn.is_closed = AsyncMock(return_value=False)
        mock_conn.fetch = AsyncMock(return_value=[{"id": 1, "name": "test"}])
        self.db._DBConnection__connection = mock_conn

        result = await self.db.execute_query("SELECT * FROM test;")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], 1)

    @patch("db.connection.logger.log")
    async def test_execute_query_failure(self, mock_log):
        mock_conn = AsyncMock()
        mock_conn.is_closed = AsyncMock(return_value=False)
        mock_conn.fetch = AsyncMock(side_effect=Exception("Query failed"))
        self.db._DBConnection__connection = mock_conn

        with self.assertRaises(Exception):
            await self.db.execute_query("SELECT * FROM test;")

    async def test_execute_command_not_connected(self):
        self.db._DBConnection__connection = None
        with self.assertRaises(RuntimeError):
            await self.db.execute_command("UPDATE test SET name = 'test';")

    @patch("db.connection.logger.log")
    async def test_execute_command_success(self, mock_log):
        mock_conn = AsyncMock()
        mock_conn.is_closed = AsyncMock(return_value=False)
        mock_conn.execute = AsyncMock(return_value="UPDATE 1")
        self.db._DBConnection__connection = mock_conn

        result = await self.db.execute_command("UPDATE test SET name = 'test';")
        self.assertEqual(result, "UPDATE 1")

    @patch("db.connection.logger.log")
    async def test_execute_command_failure(self, mock_log):
        mock_conn = AsyncMock()
        mock_conn.is_closed = AsyncMock(return_value=False)
        mock_conn.execute = AsyncMock(side_effect=Exception("Command failed"))
        self.db._DBConnection__connection = mock_conn

        with self.assertRaises(Exception):
            await self.db.execute_command("UPDATE test SET name = 'test';")

    @patch("db.connection.logger.log")
    async def test_close_success(self, mock_log):
        mock_conn = AsyncMock()
        mock_conn.is_closed = AsyncMock(return_value=False)
        mock_conn.close = AsyncMock()
        self.db._DBConnection__connection = mock_conn

        await self.db.close()
        mock_conn.close.assert_awaited_once()
        self.assertIsNone(self.db._DBConnection__connection)

    @patch("db.connection.logger.log")
    async def test_close_already_closed(self, mock_log):
        self.db._DBConnection__connection = None
        await self.db.close()
        mock_log.assert_called()

    @patch("db.connection.logger.log")
    async def test_close_exception(self, mock_log):
        mock_conn = AsyncMock()
        mock_conn.is_closed = AsyncMock(return_value=False)
        mock_conn.close = AsyncMock(side_effect=Exception("Close failed"))
        self.db._DBConnection__connection = mock_conn

        await self.db.close()
        # Should handle exception gracefully
        self.assertIsNone(self.db._DBConnection__connection)

if __name__ == "__main__":
    unittest.main()