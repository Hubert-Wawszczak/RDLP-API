
import asyncpg
import asyncio
import logging
import time
from config.config import Settings
from utils.logger.logger import AsyncLogger
from utils.singleton import singleton
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log


logger = AsyncLogger("G:\\PilarzOPS\\RDLP-API\\utils\\logger\\logger.yaml")


@singleton
class DBConnection:

    def __init__(self, mode: str = "dev"):
        self.__cfg = Settings(mode)
        self.__connection = None
        self.__pool = None


    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((
                asyncpg.ConnectionDoesNotExistError,
                asyncpg.ConnectionFailureError,
                ConnectionRefusedError,
                OSError
        )),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.WARNING)
    )
    async def connect(self) -> bool:
        try:
            if await self.is_connected():
                logger.log("INFO", "Already connected to the database.")
                return True

            self.__connection = await asyncpg.connect(
                user=self.__cfg.db_username,
                password=self.__cfg.db_password,
                database=self.__cfg.db_name,
                host=self.__cfg.db_host,
                port=self.__cfg.db_port
            )
            if await self.__connection.fetchval("SELECT 1;"):
                return True
            else:
                return False
        except Exception as e:
            logger.log("ERROR", str(e))
            self.__connection = None
            raise asyncpg.exceptions.PostgresConnectionError(str(e))

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((
                asyncpg.ConnectionDoesNotExistError,
                asyncpg.ConnectionFailureError,
                ConnectionRefusedError,
                OSError
        )),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.WARNING)
    )
    async def create_pool(self, min_size: int = 1, max_size: int = 10):
        try:
            if self.__pool is not None:
                logger.log("INFO", "Connection pool already exists.")
                return self.__pool
            self.__pool = await asyncpg.create_pool(
                user=self.__cfg.db_username,
                password=self.__cfg.db_password,
                database=self.__cfg.db_name,
                host=self.__cfg.db_host,
                port=self.__cfg.db_port,
                min_size=min_size,
                max_size=max_size
            )
            logger.log("INFO", "Connection pool created successfully.")
            return self.__pool
        except Exception as e:
            logger.log("ERROR", f"Failed to create connection pool: {str(e)}")
            self.__pool = None
            raise asyncpg.exceptions.PostgresConnectionError(str(e))

    async def close(self):
        if self.__connection and not self.__connection.is_closed():
            try:
                await self.__connection.close()
            except Exception as e:
                logger.log("ERROR", f"Failed to close the connection: {str(e)}")
            finally:
                self.__connection = None
        else:
            logger.log("INFO", "Connection is already closed or was never established.")

    async def is_connected(self) -> bool:
        return self.__connection is not None and not await self.__connection.is_closed()

    # FETCH queries
    async def execute_query(self, query: str, *args):
        if not await self.is_connected():
            raise RuntimeError("Not connected to the database.")
        try:
            return await self.__connection.fetch(query, *args)
        except Exception as e:
            logger.log("ERROR", f"Query execution failed: {str(e)}")
            raise

    # INSERT/UPDATE/DELETE queries
    async def execute_command(self, command: str, *args):
        try:
            if not await self.is_connected():
                raise RuntimeError("Not connected to the database.")

            return await self.__connection.execute(command, *args)
        except RuntimeError as e:
            logger.log("ERROR", f"Runtime error: {str(e)}")
            raise
        except AttributeError as e:
            logger.log("ERROR", f"Attribute error: {str(e)}")
            raise
        except Exception as e:
            logger.log("ERROR", f"Command execution failed: {str(e)}")
            raise

if __name__ == "__main__":
    logger.log("INFO", "Starting DB connection.")
    async def baza_test():
        q1 = DBConnection("dev")
        await q1.connect()
        await asyncio.sleep(3)

        asdf = await q1.execute_query("SELECT * FROM pg_tables WHERE schemaname = 'rdlp';")
        for i in asdf:
            print(str(time.time())+ str(i))
        #await q1.close()

    async def baza2():
        qq1 = DBConnection("dev")
        await qq1.connect()

        awd = await qq1.execute_query("SELECT * FROM pg_tables;")
        for i in awd:
            print(str(time.time())+ str(i))



    async def main():
        await asyncio.gather(
            baza2(),
            baza_test()
            )

    asyncio.run(main())
