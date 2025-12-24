import asyncio
import logging
from database import async_session, User
from sqlalchemy import select
from database import close_signal_in_db, save_new_signal

class SignalTracker:

    def __init__(self, bot):
        self.bot = bot
        self.active_signals = []  # Список живых сделок

    async def add_signal(self, signal):
        """Добавляет сигнал в мониторинг"""
        # Проверяем, нет ли уже такого символа в работе, чтобы не дублировать
        if any(s['symbol'] == signal['symbol'] for s in self.active_signals):
            return

        await save_new_signal(signal)
        self.active_signals.append(signal)
        logging.info(f"✅ Сигнал {signal['symbol']} сохранен в БД и трекер")

    async def start_monitoring(self, exchange_instance):
        """Бесконечный цикл проверки цен для всех активных сигналов"""
        while True:
            if not self.active_signals:
                await asyncio.sleep(30)
                continue

            try:
                # Получаем текущие цены для всех пар сразу (оптимизация)
                tickers = await exchange_instance.fetch_tickers([s['symbol'] for s in self.active_signals])

                for sig in self.active_signals[:]:  # Итерируемся по копии списка
                    symbol = sig['symbol']
                    current_price = tickers[symbol]['last']

                    is_closed = False
                    result_text = ""

                    # Проверка Take Profit
                    if (sig['side'] == 'buy' and current_price >= sig['tp']) or \
                            (sig['side'] == 'sell' and current_price <= sig['tp']):
                        result_text = f"🎯 **TAKE PROFIT** по {symbol}!\nЦена достигла {current_price}"
                        is_closed = True

                    # Проверка Stop Loss
                    elif (sig['side'] == 'buy' and current_price <= sig['sl']) or \
                            (sig['side'] == 'sell' and current_price >= sig['sl']):
                        result_text = f"🛑 **STOP LOSS** по {symbol}.\nЦена: {current_price}"
                        is_closed = True

                    if is_closed:
                        # Обновляем в БД
                        await close_signal_in_db(symbol, current_price, "TP" if "TAKE" in result_text else "SL")
                        # Рассылаем уведомление
                        await self.notify_all_premium(result_text)
                        self.active_signals.remove(sig)

            except Exception as e:
                logging.error(f"Ошибка в трекере сигналов: {e}")

            await asyncio.sleep(20)  # Проверяем цену каждые 20 секунд

    async def notify_all_premium(self, text):
        """Отправка уведомления о закрытии сделки всем премиумам"""
        async with async_session() as session:
            result = await session.execute(select(User).where(User.status == "PREMIUM"))
            users = result.scalars().all()
            for user in users:
                try:
                    await self.bot.send_message(user.user_id, text, parse_mode="Markdown")
                except Exception as e:
                    logging.error(f"Ошибка, перезапустите бота: {e}")
                    pass