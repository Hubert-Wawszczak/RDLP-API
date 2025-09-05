import sys

from dotenv import load_dotenv
from path import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from utils.logger.logger import AsyncLogger


class Settings(BaseSettings):

    db_host: str = Field(...)
    db_port: str = Field(...)
    db_name: str = Field(...)
    db_username: str = Field(...)
    db_password: str = Field(...)

logger = AsyncLogger()
    def __init__(self, env: str = 'dev', **kwargs):
        try:
            env_file: Path = Path(f"G:\\PilarzOPS\\RDLP-API\\config.{env}.env")
            if not env_file.exists():
                raise FileNotFoundError("Environment file not found")
            load_dotenv(env_file)
            super().__init__(**kwargs)
        except FileNotFoundError:
            logger.log("ERROR", f"Environment file not found: {env_file}")
            return
        except OSError:
            logger.log("ERROR", f"Environment file not found: {env_file}")
            return
        except Exception as e:
            logger.log("ERROR", f"Unexpected error: {e}")
            sys.exit()

    class Config:
        model_config = SettingsConfigDict(env_file_encoding='utf-8')

if __name__ == "__main__":
    asdf = Settings(env="prod")
    print("settings", asdf.model_dump())