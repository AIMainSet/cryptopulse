from typing import Any, Awaitable, Callable, Dict
from aiogram import BaseMiddleware
from aiogram.types import Message
from database import is_user_banned


class BanCheckMiddleware(BaseMiddleware):
    async def __call__(
            self,
            handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
            event: Message,
            data: Dict[str, Any]
    ) -> Any:
        # Проверяем только сообщения (Message)
        if isinstance(event, Message):
            if await is_user_banned(event.from_user.id):
                return  # Просто игнорируем пользователя

        return await handler(event, data)