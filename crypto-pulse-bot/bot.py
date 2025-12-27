import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BotCommand
from config import config
from handlers import user_handlers
from core.market_worker import MarketWorker
from database import init_db
import ccxt.async_support as ccxt

# Настройка команд в меню возле поля ввода
async def set_main_menu(bot: Bot):
    commands = [
        BotCommand(command='/start', description='Перезапустить бота'),
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

    exchange = ccxt.bybit({
        'enableRateLimit': True,
        'apiKey': config.BYBIT_API_KEY,
        'secret': config.BYBIT_API_SECRET,
        'options': {'defaultType': 'spot'}
    })

    # 3. Инициализация воркера анализа рынка (ИЗМЕНИТЕ ЭТУ СТРОКУ)
    worker = MarketWorker(bot, exchange)

    # 4. Регистрация роутеров
    from handlers import admin_handlers
    dp.include_router(admin_handlers.router)
    dp.include_router(user_handlers.router)

    # 5. Установка команд
    await set_main_menu(bot)

    logging.info("🚀 Рокет-старт: Бот и Воркер запущены!")

    async def worker_supervisor(worker_instance):
        """Следит за воркером и перезапускает при падении"""
        while True:
            try:
                await worker_instance.start()
            except Exception as e:
                logging.error(f"Воркер упал: {e}. Перезапуск через 10 секунд...")
                await asyncio.sleep(10)
    # 6. Запуск фонового воркера
    background_task = asyncio.create_task(worker_supervisor(worker))
    background_task.add_done_callback(lambda t: logging.info("Фоновый воркер завершен"))

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
