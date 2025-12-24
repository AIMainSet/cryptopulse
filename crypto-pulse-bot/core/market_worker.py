import asyncio
import logging
import os
import pandas as pd
import pandas_ta as ta
from datetime import datetime
from sqlalchemy import select
from aiogram import Bot
from aiogram.types import FSInputFile

# Внутренние модули
from core.advanced_signal_generator import AdvancedSignalGenerator
from analytics.signal_tracker import SignalTracker
from database import async_session, User, check_and_expire_subscriptions
from core.chart_gen import create_signal_chart
from core.formatter import EnhancedSignalFormatter
import config


# --- 1. ГЛОБАЛЬНЫЕ ФУНКЦИИ (Должны быть в начале файла) ---

def calculate_position_size(deposit, risk_pct, entry, sl):
    """Профессиональный расчет объема позиции с защитой от ошибок"""
    try:
        d = float(deposit or 0)
        r = float(risk_pct or 0)
        e = float(entry or 0)
        s = float(sl or 0)

        if d <= 0 or r <= 0 or e <= 0:
            return 0

        risk_money = d * (r / 100)
        stop_dist = abs(e - s) / e

        if stop_dist <= 0:
            return 0

        return round(risk_money / stop_dist, 2)
    except Exception as err:
        logging.error(f"⚠️ Ошибка в расчете объема: {err}")
        return 0


# --- 2. КЛАСС ВОРКЕРА ---

class MarketWorker:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.gen = AdvancedSignalGenerator()
        self.tracker = SignalTracker(bot)
        self.formatter = EnhancedSignalFormatter()
        self._tasks = []

    async def start(self):
        logging.info("🚀 Запуск фоновых задач воркера...")
        try:
            self._tasks = []
            monitor_task = asyncio.create_task(
                self.tracker.start_monitoring(self.gen.exchange),
                name="SignalTracker"
            )
            sub_check_task = asyncio.create_task(
                self.subscription_checker(),
                name="SubChecker"
            )
            self._tasks.extend([monitor_task, sub_check_task])

            for task in self._tasks:
                task.add_done_callback(self._on_task_completed)

            logging.info("🕵️ Воркер и задачи мониторинга успешно инициализированы.")
        except Exception as e:
            logging.error(f"❌ Критическая ошибка инициализации: {e}")
            raise

        while True:
            try:
                # 1. СИНХРОНИЗАЦИЯ С БД
                async with async_session() as session:
                    result = await session.execute(
                        select(User.selected_pairs, User.user_id)
                        .where(User.status == "PREMIUM")
                    )
                    users_data = result.all()

                dynamic_symbols = {
                    p.strip().upper()
                    for row in users_data if row[0]
                    for p in row[0].split(",")
                }

                if not dynamic_symbols:
                    dynamic_symbols = set(getattr(config, 'DEFAULT_SYMBOLS', ["BTC/USDT"]))

                # Обновляем список в генераторе
                self.gen.update_symbols(list(dynamic_symbols))

                start_scan = datetime.now()
                logging.info(f"[{start_scan.strftime('%H:%M:%S')}] 🔍 Сканирование {len(self.gen.symbols)} пар...")

                # 2. АНАЛИЗ
                new_sigs = await self.gen.run_analysis_cycle()

                # 3. РАССЫЛКА С ОТЧЕТОМ
                total_sent = 0
                if new_sigs:
                    for s in new_sigs:
                        await self.tracker.add_signal(s)
                        sent_count = await self.broadcast_signal(s)
                        total_sent += sent_count

                    logging.info(f"✅ Цикл завершен. Найдено сигналов: {len(new_sigs)} | Доставлено: {total_sent}")
                else:
                    logging.info(f"⚖️ Цикл завершен. Сигналов нет. Активных Premium-юзеров: {len(users_data)}")

            except Exception as e:
                logging.error(f"❌ Ошибка в цикле воркера: {e}", exc_info=True)
                await asyncio.sleep(60)
                continue

            await asyncio.sleep(300)

    def _on_task_completed(self, task):
        """Обработка падения фоновых задач"""
        try:
            task.result()
        except Exception as e:
            logging.error(f"⚠️ Фоновая задача {task.get_name()} внезапно завершилась: {e}")

    async def subscription_checker(self):
        """Проверка истечения подписок"""
        while True:
            try:
                expired_user_ids = await check_and_expire_subscriptions()
                for uid in expired_user_ids:
                    try:
                        await self.bot.send_message(
                            uid,
                            "⚠️ *Срок действия вашей PREMIUM подписки истек*",
                            parse_mode="MarkdownV2"
                        )
                    except:
                        pass
            except Exception as e:
                logging.error(f"Ошибка в subscription_checker: {e}")
            await asyncio.sleep(3600)

    async def broadcast_signal(self, signal):
        """Рассылка сигнала конкретным пользователям"""
        symbol = signal['symbol']
        chart_path = None
        sent_success = 0

        # Попытка создать график
        try:
            ohlcv = await self.gen.exchange.fetch_ohlcv(symbol, timeframe='1h', limit=150)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            df['ema_50'] = ta.ema(df['Close'], length=50)
            df['ema_200'] = ta.ema(df['Close'], length=200)
            df_final = df.dropna().tail(100)

            if not df_final.empty:
                chart_path = create_signal_chart(
                    df=df_final, symbol=symbol, entry=signal['entry'],
                    tp=signal.get('tp1', signal['entry']),
                    sl=signal['sl'], side=signal['side']
                )
        except Exception as e:
            logging.error(f"📈 Ошибка графика {symbol}: {e}")

        # Формируем данные для форматтера
        signal_payload = {
            'symbol': symbol,
            'direction': 'LONG' if signal['side'].upper() in ['BUY', 'LONG'] else 'SHORT',
            'entry': signal['entry'],
            'tp1': signal.get('tp1'), 'tp2': signal.get('tp2'), 'tp3': signal.get('tp3'),
            'sl': signal['sl'], 'risk': 'Medium', 'leverage': '10x',
            'reason': signal.get('reason', 'Технический анализ'),
            'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        rating_data = {
            'emoji': '💎' if signal.get('status') == 'ULTRA' else '🔥',
            'status': signal.get('status', 'ULTRA'),
            'confidence': signal.get('confidence', 0.94)
        }

        # Рассылка по подписчикам конкретной монеты
        async with async_session() as session:
            result = await session.execute(select(User).where(User.status == "PREMIUM"))
            users = result.scalars().all()

            for user in users:
                user_pairs = [p.strip().upper() for p in user.selected_pairs.split(",")] if user.selected_pairs else []
                if symbol.upper() not in user_pairs:
                    continue

                # Расчет объема (теперь функция доступна здесь)
                pos_size = calculate_position_size(user.deposit, user.risk_per_trade, signal['entry'], signal['sl'])

                premium_text = self.formatter.format_signal_with_rating(signal_payload, rating_data)
                esc_pos = EnhancedSignalFormatter.escape_md(str(pos_size))
                esc_risk = EnhancedSignalFormatter.escape_md(str(user.risk_per_trade))

                final_text = (
                    f"{premium_text}\n\n"
                    f"💰 *ВАШ ИНДИВИДУАЛЬНЫЙ РАСЧЕТ:*\n"
                    f"└ Объем сделки: `{esc_pos}` USDT \\(риск {esc_risk}%\\)"
                )

                try:
                    if chart_path and os.path.exists(chart_path):
                        await self.bot.send_photo(user.user_id, photo=FSInputFile(chart_path), caption=final_text,
                                                  parse_mode="MarkdownV2")
                    else:
                        await self.bot.send_message(user.user_id, final_text, parse_mode="MarkdownV2")
                    sent_success += 1
                except Exception as e:
                    logging.error(f"🚨 Ошибка отправки юзеру {user.user_id}: {e}")

        # Удаляем график после рассылки
        if chart_path and os.path.exists(chart_path):
            try:
                os.remove(chart_path)
            except:
                pass

        return sent_success