



# Logger powinien wspierać logi na poziomach DEBUG / INFO / ERROR z czytelnym formatem.

# opracować w klase, dostęp do loggera przez metodę(poziom, wiadomość)
# __  <-atrybut prywatny na obiekt loggera


import time
import yaml
import asyncio
import logging
import functools
import logging.config
from datetime import datetime
from typing import Any, Callable, Optional, TypeVar, cast
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from utils.singleton import singleton

# Type variable for function decorators
F = TypeVar('F', bound=Callable[..., Any])





@singleton
class AsyncLogger:
    """
    Asynchronous logger that implements the Singleton pattern.
    Provides logging capabilities with support for asynchronous operations.
    """

    def __init__(self, config_path: Path = Path(__file__).parent / 'logger.yaml'):
        """
        Initialize the AsyncLogger instance.

        Args:
            config_path: Path to the YAML configuration file (optional)
        """
        self.__logger = None
        self.__loop = None
        self.__executor = ThreadPoolExecutor(max_workers=2)
        self.__initialize_logger(config_path)
        self.valid_levels = {'debug', 'info', 'warning', 'error', 'critical'}

    def __initialize_logger(self, config_path: Path = Path(__file__).parent / 'logger.yaml') -> None:
        """
        Initialize the logger with either a provided configuration file or default settings.

        Args:
            config_path: Path to the YAML configuration file (optional)
        """
        try:
            # Create logs directory if it doesn't exist
            logs_dir = Path(Path(__file__).parent.parent.parent / 'logs')
            logs_dir.mkdir(exist_ok=True)

            # Generate unique log filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"app_{timestamp}.log"
            log_filepath = logs_dir / log_filename

            if config_path:
                self.__configure_from_yaml(config_path, log_filepath)
            else:
                self.__configure_defaults(log_filepath)

            self.__logger = logging.getLogger('async_logger')
            try:
                self.__loop = asyncio.get_event_loop()
            except RuntimeError:
                self.__loop = None
                #asyncio.set_event_loop(self.__loop)

        except FileNotFoundError:
            raise RuntimeError(f"Configuration file not found: {config_path}")

        except Exception as e:
            # Fallback to basic console logging if initialization fails
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            self.__logger = logging.getLogger('async_logger_fallback')
            self.__logger.error(f"Failed to initialize logger: {str(e)}")



    def __configure_from_yaml(self, config_path: Path, log_filepath: Path) -> None:
        """
        Configure logger using a YAML configuration file.

        Args:
            config_path: Path to the YAML configuration file
            log_filepath: Path to the log file
        """
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)

            # Update log file path in the configuration
            for handler in config.get('handlers', {}).values():
                if handler.get('filename'):
                    handler['filename'] = str(log_filepath)

            logging.config.dictConfig(config)
        except Exception as e:
            raise RuntimeError(f"Failed to load logger configuration from {config_path}: {str(e)}")

    def __configure_defaults(self, log_filepath: Path) -> None:
        """
        Configure logger with default settings if no configuration file is provided.

        Args:
            log_filepath: Path to the log file
        """
        DEFAULT_CONFIG = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'standard': {
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                },
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'level': 'INFO',
                    'formatter': 'standard',
                    'stream': 'ext://sys.stdout',
                },
                'file': {
                    'class': 'logging.FileHandler',
                    'level': 'DEBUG',
                    'formatter': 'standard',
                    'filename': str(log_filepath),
                    'encoding': 'utf8'
                },
            },
            'loggers': {
                'async_logger': {
                    'handlers': ['console', 'file'],
                    'level': 'DEBUG',
                    'propagate': True
                }
            }
        }

        logging.config.dictConfig(DEFAULT_CONFIG)

    async def __async_log(self, level: str, message: str) -> None:
        """
        Asynchronously log a message at the specified level.

        Args:
            level: The log level (debug, info, warning, error, critical)
            message: The message to log
        """
        if not self.__logger:
            return

        log_func = getattr(self.__logger, level.lower(), self.__logger.info)

        # If we have a running event loop, use it to schedule the logging
        if self.__loop and self.__loop.is_running():
            await asyncio.get_event_loop().run_in_executor(
                self.__executor,
                lambda: log_func(message, stacklevel=9)
            )
        else:
            # Fallback to synchronous logging if no event loop is running
            log_func(message, stacklevel=9)

    def log(self, level: str, message: str) -> None:
        """
        Log a message at the specified level. This method handles both
        synchronous and asynchronous contexts automatically.

        Args:
            level: The log level (debug, info, warning, error, critical)
            message: The message to log
        """
        if not self.__logger:
            return
        normalized_level = level.lower() if level.lower() in self.valid_levels else 'info'
        try:
            # Check if we're in an async context
            if asyncio.get_event_loop_policy().get_event_loop().is_running():
                # Create task to avoid blocking
                asyncio.create_task(self.__async_log(level, message))
            else:
                # Synchronously log if not in async context
                log_func = getattr(self.__logger, normalized_level, self.__logger.info)
                log_func(message, stacklevel=2)
        except RuntimeError:
            # If we can't get an event loop, log synchronously
            log_func = getattr(self.__logger, level.lower(), self.__logger.info)
            log_func(message, stacklevel=2)

    def log_time_exec(self, func: F) -> F:
        """
        Decorator to measure and log the execution time of a function.
        Works with both synchronous and asynchronous functions.

        Args:
            func: The function to be decorated

        Returns:
            The wrapped function
        """

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                return await func(*args, **kwargs)
            finally:
                execution_time = time.time() - start_time
                self.log('info', f"Function '{func.__name__}' executed in {execution_time:.4f} seconds")

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                return func(*args, **kwargs)
            finally:
                execution_time = time.time() - start_time
                self.log('info', f"Function '{func.__name__}' executed in {execution_time:.4f} seconds")

        # Determine if the decorated function is a coroutine function
        if asyncio.iscoroutinefunction(func):
            return cast(F, async_wrapper)
        return cast(F, sync_wrapper)


if __name__ == "__main__":

    new_logger = AsyncLogger()
    new_logger.log("ERROR", "asdfxd")
    @new_logger.log_time_exec
    def asdf():
        time.sleep(1)

    asdf()

    async def task_sleep_5():
        new_logger.log("INFO","Task 5s start")
        await asyncio.sleep(2)
        new_logger.log("INFO","Task 5s end")

    async def task_sleep_10():
        new_logger.log("INFO","Task 10s start")
        await asyncio.sleep(3)
        new_logger.log("INFO","Task 10s end")
        
    async def main():
        new_logger.log("INFO", "start")
        await asyncio.gather(
            task_sleep_5(),
            task_sleep_10()
            )
        new_logger.log("INFO", "end")


    asyncio.run(main())

