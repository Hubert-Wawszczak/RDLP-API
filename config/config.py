import sys

from dotenv import load_dotenv
from path import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from utils.logger.logger import AsyncLogger
from typing import ClassVar

class Settings(BaseSettings):

    db_host: str = Field(...)
    db_port: str = Field(...)
    db_name: str = Field(...)
    db_username: str = Field(...)
    db_password: str = Field(...)
    logger: ClassVar[AsyncLogger] = AsyncLogger()

    def __init__(cls, env: str = 'dev', **kwargs):
        try:
            env_file: Path = Path(__file__).parent.parent / f"config.{env}.env"
            if not env_file.exists():
                raise FileNotFoundError("Environment file not found")
            load_dotenv(env_file)
            super().__init__(**kwargs)
        except FileNotFoundError:
            cls.logger.log("ERROR", f"Environment file not found: {env_file}")
            return
        except OSError:
            cls.logger.log("ERROR", f"Environment file not found: {env_file}")
            return
        except Exception as e:
            cls.ogger.log("ERROR", f"Unexpected error: {e}")
            sys.exit()

    class Config:
        model_config = SettingsConfigDict(env_file_encoding='utf-8')

if __name__ == "__main__":
    asdf = Settings(env="prod")
    print("settings", asdf.model_dump())