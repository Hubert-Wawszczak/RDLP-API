import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

class Config:
    def __init__(self, env_file: str = "config.env"):
        load_dotenv(env_file)


class Settings(BaseSettings):
    Config()
    database_url: str = os.getenv("DB_HOST")
    database_port: str = os.getenv("DB_PORT")
    database_name: str = os.getenv("DB_NAME")
    database_username: str = os.getenv("DB_USERNAME")
    database_password: str = os.getenv("DB_PASSWORD")


