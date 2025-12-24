#!/usr/bin/env python3
"""
Запуск всех компонентов бота:
1. Основной бот
2. Market Worker
3. Payment Checker
"""

import logging
import sys
from multiprocessing import Process

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_bot():
    """Запуск основного бота"""
    import asyncio
    from bot import main as bot_main

    logger.info("🤖 Запускаю основного бота...")
    asyncio.run(bot_main())


def run_market_worker():
    """Запуск Market Worker"""
    import asyncio
    from core.market_worker import main as mw_main

    logger.info("📈 Запускаю Market Worker...")
    asyncio.run(mw_main())


def run_webhook_server():
    """Запуск Webhook сервера (опционально)"""
    from webhook_server import app
    from aiohttp import web

    logger.info("🌐 Запускаю Webhook сервер...")
    web.run_app(app, port=8000)


def main():
    """Основная функция запуска всех компонентов"""
    logger.info("🚀 Запуск CryptoPulse системы...")

    processes = []

    try:
        # Запускаем основной бот
        p1 = Process(target=run_bot)
        p1.start()
        processes.append(p1)
        logger.info("✅ Основной бот запущен")

        # Ждем немного перед запуском следующего компонента
        import time
        time.sleep(3)

        # Запускаем Market Worker
        p2 = Process(target=run_market_worker)
        p2.start()
        processes.append(p2)
        logger.info("✅ Market Worker запущен")

        # Webhook сервер (закомментируйте если не нужен)
        # p3 = Process(target=run_webhook_server)
        # p3.start()
        # processes.append(p3)
        # logger.info("✅ Webhook сервер запущен")

        logger.info("🎯 Все компоненты запущены! Система работает.")

        # Ожидаем завершения всех процессов
        for p in processes:
            p.join()

    except KeyboardInterrupt:
        logger.info("🛑 Остановка системы...")
        for p in processes:
            p.terminate()
        logger.info("✅ Система остановлена")
    except Exception as e:
        logger.error(f"❌ Ошибка запуска системы: {e}")
        for p in processes:
            p.terminate()
        sys.exit(1)


if __name__ == "__main__":
    main()