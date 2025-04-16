import sys

from dotenv import load_dotenv
from path import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

# TODO: proper logging instead of print

class Settings(BaseSettings):

    db_host: str = Field(...)
    db_port: str = Field(...)
    db_name: str = Field(...)
    db_username: str = Field(...)
    db_password: str = Field(...)


    def __init__(self, env: str = 'dev', **kwargs):
        try:
            env_file: Path = Path(f"config.{env}.env")
            if not env_file.exists():
                raise FileNotFoundError("Environment file not found")
            load_dotenv(env_file)
            super().__init__(**kwargs)
        except FileNotFoundError:
            print("file not found")
            return
        except OSError:
            print("could not open/read file")
            return
        except Exception as e:
            print(f"An error occurred: {e}")
            sys.exit()

    class Config:
        model_config = SettingsConfigDict(env_file_encoding='utf-8')


asdf = Settings(env="prod")
print("settings", asdf.model_dump())