import unittest

from config.config import Settings

class TestSettings(unittest.TestCase):

    def test_load_dev_env(self):
        settings = Settings(env="dev")
        self.assertIsInstance(settings.db_host, str)
        self.assertIsInstance(settings.db_port, str)
        self.assertIsInstance(settings.db_name, str)
        self.assertIsInstance(settings.db_username, str)
        self.assertIsInstance(settings.db_password, str)

        self.assertEqual(settings.db_host, "localhost")
        self.assertEqual(settings.db_port, "5432")
        self.assertEqual(settings.db_name, "postgres")
        self.assertEqual(settings.db_username, "postgres")
        self.assertEqual(settings.db_password, "admin")

    def test_load_prod_env(self):
        settings = Settings(env="prod")
        self.assertIsInstance(settings.db_host, str)
        self.assertIsInstance(settings.db_port, str)
        self.assertIsInstance(settings.db_name, str)
        self.assertIsInstance(settings.db_username, str)
        self.assertIsInstance(settings.db_password, str)

        self.assertEqual(settings.db_host, "localhost")
        self.assertEqual(settings.db_port, "5432")
        self.assertEqual(settings.db_name, "postgres")
        self.assertEqual(settings.db_username, "postgres")
        self.assertEqual(settings.db_password, "admin")
