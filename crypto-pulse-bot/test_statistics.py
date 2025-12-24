import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import db


async def test():
    print("🔍 Тестирование статистики...")

    # Подключаемся к БД
    await db.connect()
    print("✅ БД подключена")

    # Получаем первого пользователя
    users = await db.get_all_users()
    if users:
        user = users[0]
        print(f"👤 Тестовый пользователь: {user['username']} (ID: {user['id']})")

        # Тестируем метод статистики
        stats = await db.get_user_stats(user['id'])
        print(f"📊 Статистика: {stats}")

        # Проверяем сигналы в БД
        signals = await db.get_recent_signals()
        print(f"📈 Сигналов в БД: {len(signals)}")
    else:
        print("❌ Нет пользователей в БД")

    print("✅ Тест завершен")


if __name__ == "__main__":
    asyncio.run(test())