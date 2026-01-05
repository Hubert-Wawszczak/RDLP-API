import asyncio
from utils.logger.logger import AsyncLogger
from services.api_client import APIClient
from services.loader import DataLoader
from pathlib import Path
import os
import yaml
import aiofiles
from abc import ABC, abstractmethod


class DataProcessTemplate(ABC):
    def __init__(self, data_dir):
        self.data_dir = data_dir

    async def run(self):
        await self.init_logger()
        await self.load_config()
        await self.init_api_client()
        await self.api_data_fetch()
        await self.load_to_db()
        await self.end_process()


    @abstractmethod
    async def init_logger(self):
        pass

    @abstractmethod
    async def load_config(self):
        pass

    @abstractmethod
    async def init_api_client(self):
        pass

    @abstractmethod
    async def api_data_fetch(self):
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
        self.config = None

    async def init_logger(self):
        print("Initializing logger")
        self.logger = AsyncLogger(self.data_dir)

    async def load_config(self):
        async with aiofiles.open(Path(__file__).parent / 'config.yaml', 'r') as f:
            self.logger.log("INFO", "Loading configuration file.")
            self.config = yaml.safe_load(await f.read())
            self.logger.log("INFO", f"Data loaded from config file: {self.config}")

    async def init_api_client(self):
        self.logger.log("INFO", "Starting data fetch and load process.")
        self.api_client = APIClient(self.data_dir)

    async def api_data_fetch(self):
        endpoints = self.config.get("endpoints")
        batch_size = self.config.get("batch_size")
        await self.api_client.fetch_data(endpoints, batch_size) # should be 'all' "RDLP_Krakow_wydzielenia"
        self.logger.log("INFO", "Finished data fetch and load process.")

    async def load_to_db(self):
        self.logger.log("INFO", "Starting data loading.")
        db_loader = DataLoader(self.data_dir)
        await db_loader.insert_data()
        self.logger.log("INFO", "Finished data loading.")

    async def end_process(self):
        self.logger.log("INFO", "Process ended.")


def run_tests():
    """Run tests using the test runner script"""
    import subprocess
    import sys
    from pathlib import Path
    
    project_root = Path(__file__).parent
    test_script = project_root / 'run_tests.py'
    
    result = subprocess.run([sys.executable, str(test_script)], cwd=str(project_root))
    sys.exit(result.returncode)


async def main():
    project_root = Path(__file__).parent
    data_dir = project_root / 'api_data'

    process = MainProcess(data_dir, project_root)
    await process.run()


if __name__ == "__main__":
    if os.getenv("RUN_TESTS") == "1":
        print("Running tests")
        run_tests()
    else:
        print("Running main process")
        asyncio.run(main())