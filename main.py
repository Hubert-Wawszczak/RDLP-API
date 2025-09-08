import asyncio
from utils.logger.logger import AsyncLogger
from services.api_client import APIClient
from services.loader import DataLoader
from pathlib import Path
import os
import argparse
from abc import ABC, abstractmethod


class DataProcessTemplate(ABC):
    def __init__(self, data_dir):
        self.data_dir = data_dir

    async def run(self):
        await self.init_logger()
        await self.api_client_init()
        await self.data_fetch()
        await self.load_to_db()
        await self.end_process()


    @abstractmethod
    async def init_logger(self):
        pass

    @abstractmethod
    async def api_client_init(self):
        pass

    @abstractmethod
    async def data_fetch(self):
        pass

    @abstractmethod
    async def load_to_db(self):
        pass

    @abstractmethod
    async def end_process(self):
        pass



class MainProcess(DataProcessTemplate):
    def __init__(self, data_dir, root):
        super().__init__(data_dir)
        self.api_client = None
        self.logger = None
        self.project_root = root

    async def init_logger(self):
        print("Initializing logger")
        self.logger = AsyncLogger(self.data_dir)

    async def api_client_init(self):
        self.logger.log("INFO", "Starting data fetch and load process.")
        self.api_client = APIClient(self.data_dir)

    async def data_fetch(self):
        await self.api_client.fetch_data(["all"])
        self.logger.log("INFO", "Finished data fetch and load process.")

    async def load_to_db(self):
        self.logger.log("INFO", "Starting data loading.")
        db_loader = DataLoader(self.data_dir)
        await db_loader.insert_data()
        self.logger.log("INFO", "Finished data loading.")

    async def end_process(self):
        self.logger.log("INFO", "Process ended.")


def run_tests():
    import unittest

    project_root = Path(__file__).parent
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=str(project_root / 'tests'), pattern='test_*.py', top_level_dir=str(project_root))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    if not result.wasSuccessful():
        exit(1)


async def main():
    project_root = Path(__file__).parent
    data_dir = project_root / 'api_data'

    process = MainProcess(data_dir, project_root)
    await process.run()

# asyncio.run(main())

if __name__ == "__main__":
    if os.getenv("RUN_TESTS") == "1":
        print("Running tests")
        run_tests()
    else:
        print("Running main process")
        asyncio.run(main())