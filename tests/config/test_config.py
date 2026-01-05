import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import unittest
from config.config import Settings

class TestSettings(unittest.TestCase):

    def test_settings_initialization(self):
        """Test that Settings can be initialized"""
        settings = Settings()
        self.assertIsInstance(settings.db_host, str)
        self.assertIsInstance(settings.db_port, str)
        self.assertIsInstance(settings.db_name, str)
        self.assertIsInstance(settings.db_username, str)
        self.assertIsInstance(settings.db_password, str)

    def test_settings_from_env(self):
        """Test that Settings reads from environment variables"""
        import os
        os.environ['DB_HOST'] = 'test_host'
        os.environ['DB_PORT'] = '5433'
        os.environ['DB_NAME'] = 'test_db'
        os.environ['DB_USERNAME'] = 'test_user'
        os.environ['DB_PASSWORD'] = 'test_pass'
        
        settings = Settings()
        self.assertEqual(settings.db_host, 'test_host')
        self.assertEqual(settings.db_port, '5433')
        self.assertEqual(settings.db_name, 'test_db')
        self.assertEqual(settings.db_username, 'test_user')
        self.assertEqual(settings.db_password, 'test_pass')
        
        # Cleanup
        del os.environ['DB_HOST']
        del os.environ['DB_PORT']
        del os.environ['DB_NAME']
        del os.environ['DB_USERNAME']
        del os.environ['DB_PASSWORD']
