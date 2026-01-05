import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import unittest
from unittest.mock import patch, MagicMock
import asyncio

from utils.logger.logger import AsyncLogger

class TestAsyncLogger(unittest.TestCase):
    def setUp(self):
        # Ensure singleton is reset for each test
        AsyncLogger._instances = {}
        self.logger = AsyncLogger()

    @patch('utils.logger.logger.logging.getLogger')
    def test_logger_initialization(self, mock_get_logger):
        self.logger._AsyncLogger__initialize_logger()
        mock_get_logger.assert_called_with('async_logger')

    @patch('utils.logger.logger.logging.getLogger')
    def test_log_info(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        self.logger._AsyncLogger__logger = mock_logger
        self.logger.log('INFO', 'test info')
        mock_logger.info.assert_called_with('test info', stacklevel=2)

    @patch('utils.logger.logger.logging.getLogger')
    def test_log_error(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        self.logger._AsyncLogger__logger = mock_logger
        self.logger.log('ERROR', 'test error')
        mock_logger.error.assert_called_with('test error', stacklevel=2)

    def test_log_time_exec_sync(self):
        calls = []
        def dummy():
            calls.append('called')
        wrapped = self.logger.log_time_exec(dummy)
        wrapped()
        self.assertIn('called', calls)

    def test_log_time_exec_async(self):
        calls = []
        async def dummy():
            calls.append('called')
        wrapped = self.logger.log_time_exec(dummy)
        asyncio.run(wrapped())
        self.assertIn('called', calls)

    @patch('utils.logger.logger.logging.getLogger')
    @patch('asyncio.get_event_loop_policy')
    def test_log_with_invalid_level(self, mock_get_event_loop_policy, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        # Force sync path
        mock_loop = MagicMock()
        mock_loop.is_running.return_value = False
        mock_get_event_loop_policy.return_value.get_event_loop.return_value = mock_loop

        self.logger._AsyncLogger__logger = mock_logger
        self.logger.log('INVALID', 'test')
        mock_logger.info.assert_called_with('test', stacklevel=2)

if __name__ == '__main__':
    unittest.main()