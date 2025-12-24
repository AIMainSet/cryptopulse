from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List # Не забудь импорт

class Settings(BaseSettings):
    BOT_TOKEN: str
    ADMIN_IDS: List[int] # Было ADMIN_ID: int, стало списком

    CRYPTOBOT_TOKEN: str
    BYBIT_API_KEY: str
    BYBIT_API_SECRET: str
    DB_URL: str = "sqlite+aiosqlite:///./database.db"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

config = Settings()