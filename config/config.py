import os

from dotenv import load_dotenv
from path import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from utils.logger.logger import AsyncLogger
from typing import ClassVar

class Settings(BaseSettings):

    db_host: str = Field(default_factory=lambda:  Settings.__get_db_host())
    db_port: str = Field(default_factory=lambda:  Settings.__read_secrets("/run/secrets/db_port", "DB_PORT") or "5432")
    db_name: str = Field(default_factory=lambda:  Settings.__read_secrets("/run/secrets/db_name", "DB_NAME") or "postgres")
    db_username: str = Field(default_factory=lambda:  Settings.__read_secrets("/run/secrets/db_username", "DB_USERNAME") or "postgres")
    db_password: str = Field(default_factory=lambda:  Settings.__read_secrets("/run/secrets/db_password", "DB_PASSWORD") or "")
    logger: ClassVar[AsyncLogger] = AsyncLogger()

    def __init__(cls, **kwargs):
        env_file: Path = Path(__file__).parent.parent / "config.dev.env"
        load_dotenv(dotenv_path=env_file)
        super().__init__(**kwargs)

    @classmethod
    def __read_secrets(cls, secret_path: str, env_var: str) -> str:
        """
        Reads a value from a Docker secret file if it exists,
        otherwise falls back to an environment variable, then to a default.
        """
        try:
            file_path = Path(secret_path)
            if file_path.exists():
                return file_path.read_text().strip()
        except Exception as e:
            cls.logger.log("ERROR", f"Error reading secret file {secret_path}: {e}")
        result = os.getenv(env_var)
        return result if result is not None else ""

    @classmethod
    def __get_db_host(cls) -> str:
        """
        Determines the correct host:
        - Inside Docker: host.docker.internal
        - Local: localhost
        Can also read from secret/env.
        """
        # Try Docker secret first
        host = cls.__read_secrets("/run/secrets/db_host", "DB_HOST")
        if host:
            cls.logger.log("INFO", f"Using host: {host}")
            return host
        cls.logger.log("INFO", "Using default host: localhost")
        # Check if inside Docker
        # if Path("/.dockerenv").exists():
        #     return "host.docker.internal"  # points to host machine from container

        # Fallback to local environment
        return os.getenv("DB_HOST", "localhost")

    class Config:
        env_file = ".env"
        model_config = SettingsConfigDict(env_file_encoding='utf-8')

if __name__ == "__main__":
    asdf = Settings()
    print("settings", asdf.model_dump())
    print(asdf.db_host)