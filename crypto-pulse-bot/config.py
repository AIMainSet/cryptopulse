import logging
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Optional
import sentry_sdk
from sentry_sdk.integrations.asyncio import AsyncioIntegration
from sentry_sdk.integrations.logging import LoggingIntegration

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

    # Риск-менеджмент
    DEFAULT_DAILY_RISK_LIMIT: float = 5.0  # 5%
    DEFAULT_RISK_PER_TRADE: float = 1.0  # 1%
    DEFAULT_MAX_POSITIONS: int = 5

    # Кэширование
    CACHE_TTL_OHLCV: int = 60  # 1 минута
    CACHE_TTL_INDICATORS: int = 120  # 2 минуты

    # Performance
    BATCH_SIZE: int = 50  # Размер пачки для рассылки
    PARALLEL_PAIRS: int = 10  # Сколько пар анализировать параллельно

    # Мониторинг
    SENTRY_DSN: Optional[str] = None
    LOG_LEVEL: str = "INFO"

config = Settings()

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
