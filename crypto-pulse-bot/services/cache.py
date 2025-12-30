"""
cache.py - система кэширования для производительности
"""

from typing import Any, Optional
import asyncio
from datetime import datetime, timedelta
import hashlib
import json


class DataCache:
    """
    Простой in-memory кэш для свечных данных и результатов индикаторов.
    Для тысяч пользователей лучше использовать Redis.
    """

    def __init__(self):
        self._cache = {}
        self._lock = asyncio.Lock()

    def _make_key(self, exchange: str, symbol: str, timeframe: str) -> str:
        """Создает ключ для кэша"""
        data = f"{exchange}:{symbol}:{timeframe}"
        return hashlib.md5(data.encode()).hexdigest()

    async def get_ohlcv(self, exchange: str, symbol: str, timeframe: str) -> Optional[list]:
        """Получает OHLCV данные из кэша"""
        key = self._make_key(exchange, symbol, timeframe)
        async with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                # Проверяем TTL (1 минута для OHLCV)
                if datetime.now() - entry['timestamp'] < timedelta(minutes=1):
                    return entry['data']
        return None

    async def set_ohlcv(self, exchange: str, symbol: str, timeframe: str, data: list):
        """Сохраняет OHLCV данные в кэш"""
        key = self._make_key(exchange, symbol, timeframe)
        async with self._lock:
            self._cache[key] = {
                'data': data,
                'timestamp': datetime.now()
            }

        # Очищаем старые записи (TTL 5 минут)
        await self._cleanup_old(timedelta(minutes=5))

    async def _cleanup_old(self, max_age: timedelta):
        """Очищает старые записи из кэша"""
        now = datetime.now()
        keys_to_delete = []

        for key, entry in self._cache.items():
            if now - entry['timestamp'] > max_age:
                keys_to_delete.append(key)

        for key in keys_to_delete:
            del self._cache[key]
