import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BotCommand
from config import config
from handlers import user_handlers
from core.market_worker import MarketWorker
from database import init_db

# Настройка команд в меню возле поля ввода
async def set_main_menu(bot: Bot):
    commands = [
        BotCommand(command='/menu', description='Главное меню'),
        BotCommand(command='/signals', description='Активные сигналы'),
        BotCommand(command='/settings', description='Настройки'),
        BotCommand(command='/help', description='Помощь')
    ]
    await bot.set_my_commands(commands)

async def main():
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )

    # 1. Инициализация базы данных
    await init_db()

    # 2. Создание бота и диспетчера
    bot = Bot(token=config.BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    # 3. Инициализация воркера анализа рынка
    worker = MarketWorker(bot)

    # 4. Регистрация роутеров
    from handlers import admin_handlers
    dp.include_router(admin_handlers.router)
    dp.include_router(user_handlers.router)

    # 5. Установка команд
    await set_main_menu(bot)

    logging.info("🚀 Рокет-старт: Бот и Воркер запущены!")

    # 6. Запуск фонового воркера
    asyncio.create_task(worker.start())

    # 7. Запуск поллинга (передаем воркер как зависимость)
    try:
        await dp.start_polling(bot, market_worker=worker)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот остановлен")