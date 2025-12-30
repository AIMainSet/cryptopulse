from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List
import sentry_sdk
from sentry_sdk.integrations.asyncio import AsyncioIntegration
from sentry_sdk.integrations.logging import LoggingIntegration

def init_sentry():
    if config.SENTRY_DSN:
        sentry_sdk.init(
            dsn=config.SENTRY_DSN,
            integrations=[
                AsyncioIntegration(),
                LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)
            ],
            traces_sample_rate=1.0,
            environment="production" if not config.DEBUG else "development",
        )
class Settings(BaseSettings):
    BOT_TOKEN: str
    ADMIN_IDS: List[int]

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
