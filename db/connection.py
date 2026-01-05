
import asyncpg
import asyncio
import logging
import time
from config.config import Settings
from utils.logger.logger import AsyncLogger
from utils.singleton import singleton
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log


logger = AsyncLogger()


@singleton
class DBConnection:
    """
    Singleton class for managing asynchronous PostgreSQL connections and connection pools.
    """

    def __init__(self):
        self.__cfg = Settings()
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
        """
        Establish a connection to the PostgreSQL database with retry logic.

        Returns:
            bool: True if connection is successful, False otherwise.

        Raises:
            asyncpg.exceptions.PostgresConnectionError: If connection fails.
        """
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
        """
        Create a connection pool for PostgreSQL with retry logic.

        Args:
            min_size (int): Minimum number of connections in the pool.
            max_size (int): Maximum number of connections in the pool.

        Returns:
            asyncpg.pool.Pool: The created connection pool.

        Raises:
            asyncpg.exceptions.PostgresConnectionError: If pool creation fails.
        """
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
            logger.log("INFO", f"Credentials used: {self.__cfg.model_dump()}")
            self.__pool = None
            raise asyncpg.exceptions.PostgresConnectionError(str(e))

    async def close(self):
        """
        Close the active database connection if it exists.
        """
        if self.__connection:
            try:
                if not await self.__connection.is_closed():
                    await self.__connection.close()
            except Exception as e:
                logger.log("ERROR", f"Failed to close the connection: {str(e)}")
            finally:
                self.__connection = None
        else:
            logger.log("INFO", "Connection is already closed or was never established.")

    async def is_connected(self) -> bool:
        """
        Check if the database connection is active.

        Returns:
            bool: True if connected, False otherwise.
        """
        return self.__connection is not None and not await self.__connection.is_closed()

    # FETCH queries
    async def execute_query(self, query: str, *args):
        """
        Execute a SQL SELECT query and return the results.

        Args:
            query (str): The SQL query to execute.
            *args: Query parameters.

        Returns:
            list: Query results.

        Raises:
            RuntimeError: If not connected.
            Exception: If query execution fails.
        """
        if not await self.is_connected():
            raise RuntimeError("Not connected to the database.")
        try:
            return await self.__connection.fetch(query, *args)
        except Exception as e:
            logger.log("ERROR", f"Query execution failed: {str(e)}")
            raise

    # INSERT/UPDATE/DELETE queries
    async def execute_command(self, command: str, *args):
        """
        Execute a SQL command (INSERT, UPDATE, DELETE).

        Args:
            command (str): The SQL command to execute.
            *args: Command parameters.

        Returns:
            str: Command execution status.

        Raises:
            RuntimeError: If not connected.
            Exception: If command execution fails.
        """
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

        asdf = await q1.execute_query("SELECT * FROM pg_tables WHERE schemaname = 'public';")
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
