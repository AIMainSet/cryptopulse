import asyncio
import logging
import os
import pandas as pd
import pandas_ta as ta
from datetime import datetime

from aiogram.exceptions import TelegramBadRequest, TelegramNotFound, TelegramForbiddenError
from sqlalchemy import select
from aiogram import Bot
from aiogram.types import FSInputFile
from core.advanced_signal_generator import AdvancedSignalGenerator
from analytics.signal_tracker import SignalTracker
from database import async_session, User, check_and_expire_subscriptions
from core.chart_gen import create_signal_chart
from core.formatter import EnhancedSignalFormatter
import config
from services.risk_manager import RiskManager

# --- ОСНОВНОЙ КЛАСС ВОРКЕРА ---

class MarketWorker:
    def __init__(self, bot: Bot, exchange):
        self.bot = bot
        self.exchange = exchange  # Готовый экземпляр биржи
        self.gen = AdvancedSignalGenerator(exchange=self.exchange, symbols=[])
        self.tracker = SignalTracker(bot)
        self.formatter = EnhancedSignalFormatter()
        self.risk_manager = RiskManager()
        self._tasks = []

    async def start(self):
        """Запуск основного цикла мониторинга рынка"""
        logging.info("🚀 Запуск фоновых задач воркера...")

        # Инициализация фоновых задач (трекер PNL и проверка подписок)
        try:
            self._tasks = []

            await self.risk_manager.start_daily_reset_scheduler()

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
                task.add_done_callback(MarketWorker._on_task_completed)

            logging.info("🕵️ Воркер и задачи мониторинга успешно инициализированы.")
        except Exception as e:
            logging.error(f"❌ Критическая ошибка инициализации воркера: {e}")
            raise

        # ГЛАВНЫЙ БЕСКОНЕЧНЫЙ ЦИКЛ
        while True:
            try:
                # 1. Синхронизация с БД: получаем список пар, за которыми следят Premium-юзеры
                async with async_session() as session:
                    result = await session.execute(
                        select(User.selected_pairs, User.user_id)
                        .where(User.status == "PREMIUM")
                    )
                    users_data = result.all()

                # Собираем уникальные пары для анализа
                dynamic_symbols = {
                    p.strip().upper()
                    for row in users_data if row[0]
                    for p in row[0].split(",")
                }

                # Если список пуст, берем дефолтные (чтобы бот не скучал)
                if not dynamic_symbols:
                    dynamic_symbols = set(getattr(config, 'DEFAULT_SYMBOLS', ["BTC/USDT", "ETH/USDT"]))

                # Обновляем список в генераторе
                self.gen.update_symbols(list(dynamic_symbols))

                # Логируем начало сканирования
                start_scan = datetime.now()
                logging.info(f"[{start_scan.strftime('%H:%M:%S')}] 🔍 Сканирование {len(self.gen.symbols)} пар...")

                # 2. ЗАПУСК АНАЛИЗА
                new_sigs = await self.gen.run_analysis_cycle()

                # 3. ОБРАБОТКА РЕЗУЛЬТАТОВ
                if new_sigs:
                    total_delivered = 0
                    for s in new_sigs:
                        # Добавляем в трекер для отслеживания PNL
                        await self.tracker.add_signal(s)
                        # Рассылаем пользователям
                        sent_count = await self.broadcast_signal(s)
                        total_delivered += sent_count

                    logging.info(
                        f"✅ Цикл завершен. Найдено сигналов: {len(new_sigs)} | Доставлено сообщений: {total_delivered}")
                else:
                    # Тихий лог, если ничего не найдено
                    pass
                    # Можно раскомментировать для отладки:
                    # logging.info("⚖️ Цикл завершен. Сигналов нет.")

            except Exception as e:
                logging.error(f"❌ Ошибка в главном цикле воркера: {e}", exc_info=True)
                await asyncio.sleep(60)  # Пауза при ошибке, чтобы не спамить логами
                continue

            # Пауза между циклами сканирования (300 сек = 5 минут)
            # Для тестов можно уменьшить до 60
            await asyncio.sleep(300)

    @staticmethod
    def _on_task_completed(task):
        """Перезапуск упавших фоновых задач"""
        try:
            task.result()
        except Exception as e:
            logging.error(f"⚠️ Фоновая задача {task.get_name()} упала: {e}")

    async def subscription_checker(self):
        """Фоновая проверка истекших подписок"""
        while True:
            try:
                expired_user_ids = await check_and_expire_subscriptions()
                for uid in expired_user_ids:
                    try:
                        await self.bot.send_message(
                            uid,
                            "⚠️ *Срок действия вашей PREMIUM подписки истек*\nПродлите доступ, чтобы не пропускать сигналы.",
                            parse_mode="MarkdownV2"
                        )
                    except (TelegramBadRequest, TelegramNotFound, TelegramForbiddenError) as ex:
                        # Пользователь заблокировал бота или удален
                        logging.debug(f"Не удалось отправить уведомление пользователю {uid}: {ex}")
                    except Exception as ex:
                        # Другие ошибки
                        logging.warning(f"Ошибка отправки уведомления пользователю {uid}: {ex}")
            except (asyncio.CancelledError, KeyboardInterrupt):
                    raise
            except Exception as e:
                    logging.error(f"Ошибка в subscription_checker: {e}", exc_info=True)
            await asyncio.sleep(3600)  # Проверка раз в час

    async def broadcast_signal(self, signal):
        """
        Оптимизированная рассылка сигналов (Batch Processing).
        Отправляет сигналы пачками, чтобы не блокировать бота.
        """
        symbol = signal['symbol']
        chart_path = None
        sent_success = 0

        # --- А. Генерация графика ---
        try:
            # Тянем чуть больше свечей для красоты графика
            ohlcv = await self.gen.exchange.fetch_ohlcv(symbol, timeframe='1h', limit=250)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])

            # Добавляем EMA для визуализации
            df['ema_50'] = ta.ema(df['Close'], length=50)
            df['ema_200'] = ta.ema(df['Close'], length=200)

            # Обрезаем последние 100 свечей для картинки
            df_final = df.dropna().tail(100)

            if not df_final.empty:
                # Передаем TP и SL, которые рассчитал генератор
                chart_path = create_signal_chart(
                    df=df_final,
                    symbol=symbol,
                    entry=signal['entry'],
                    tp=signal.get('tp1', signal.get('tp')),  # Поддержка обоих вариантов
                    sl=signal['sl'],
                    side=signal['side']
                )
        except Exception as e:
            logging.error(f"📈 Не удалось создать график для {symbol}: {e}")

        # --- Б. Подготовка текстов ---
        signal_data = {
            'symbol': symbol,
            'signal_type': 'BUY' if signal['side'].upper() in ['BUY', 'LONG'] else 'SELL',
            'confidence': signal.get('confidence', 0.94),
            'entry_price': signal['entry'],
            'stop_loss': signal['sl'],
            'take_profit_1': signal.get('tp1'),
            'take_profit_2': signal.get('tp2') or signal.get('tp3'),
            'timestamp': datetime.now(),  # Обязательно объект datetime, а не строка
            'quality_report': {
                'strength': signal.get('status', 'MEDIUM', 'HIGH', 'ULTRA'),
                'percentage': signal.get('confidence', 0.94) * 100,
                'recommendation': 'Торговать' if signal.get('confidence', 0.94) > 0.7 else 'Осторожно',
                'factors': {}  # Оставьте пустым или заполните при наличии данных
            }
        }

        base_text = self.formatter.format_signal_with_rating(signal_data)

        # --- В. Сбор получателей ---
        async with async_session() as session:
            result = await session.execute(select(User).where(User.status == "PREMIUM"))
            users = result.scalars().all()

        # --- Г. Функция отправки одному юзеру (внутренняя) ---
        async def send_to_one_user(user_obj):
            # 1. Проверка фильтра пар
            user_pairs = [p.strip().upper() for p in
                          user_obj.selected_pairs.split(",")] if user_obj.selected_pairs else []
            if symbol.upper() not in user_pairs:
                return False

            # 2. Расчет процента риска сигнала (стоп-лосс в %)
            entry_price = signal['entry']
            stop_loss = signal['sl']
            signal_risk_pct = abs(entry_price - stop_loss) / entry_price * 100

            # 3. ПРОВЕРКА ЛИМИТОВ РИСК-МЕНЕДЖМЕНТА
            risk_check = await self.risk_manager.check_user_limits(
                user_id=user_obj.id,  # ID из БД (первичный ключ), не путать с user_obj.user_id
                signal_risk=signal_risk_pct
            )

            if not risk_check['allowed']:
                logging.info(f"⛔ Пропуск сигнала для {user_obj.user_id}: {risk_check['reason']}")
                return False

            # 4. Используем размер позиции от риск-менеджера, а не от calculate_position_size
            pos_size = risk_check['position_size']
            esc_pos = EnhancedSignalFormatter.escape_md(str(pos_size))
            esc_risk = EnhancedSignalFormatter.escape_md(str(user_obj.risk_per_trade))

            # 5. Добавляем персонализацию к тексту
            final_text = (
                f"{base_text}\n\n"
                f"💰 *ВАШ ИНДИВИДУАЛЬНЫЙ РАСЧЕТ:*\n"
                f"└ Объем сделки: `{esc_pos}` USDT \\(риск {esc_risk}%\\)"
                f"\n└ Дневной риск использован: `{user_obj.daily_risk_used or 0:.2f}%` из `{user_obj.daily_risk_limit or 2.0}%`"
            )

            try:
                if chart_path and os.path.exists(chart_path):
                    await self.bot.send_photo(user_obj.user_id, photo=FSInputFile(chart_path),
                                              caption=final_text, parse_mode="MarkdownV2")
                else:
                    await self.bot.send_message(user_obj.user_id, final_text, parse_mode="MarkdownV2")

                # 6. ОБНОВЛЯЕМ ИСПОЛЬЗОВАННЫЙ ДНЕВНОЙ РИСК
                await self.risk_manager.update_daily_risk(
                    user_id=user_obj.id,
                    risk_amount=signal_risk_pct * (user_obj.risk_per_trade / 100)
                )

                logging.info(f"✅ Сигнал отправлен {user_obj.user_id}, объем: {pos_size} USDT")
                return True
            except Exception as exc:
                logging.error(f"🚨 Ошибка отправки юзеру {user_obj.user_id}: {exc}")
                return False

        # --- Пакетная отправка (Batching) ---
        # Отправляем пачками по 20 штук, чтобы было быстро, но не убило API
        tasks = []
        for user in users:
            tasks.append(send_to_one_user(user))

            if len(tasks) >= 20:
                results = await asyncio.gather(*tasks)
                sent_success += sum(results)
                tasks = []
                await asyncio.sleep(0.5)  # Микро-пауза для вежливости к API

        # Доотправляем остатки
        if tasks:
            results = await asyncio.gather(*tasks)
            sent_success += sum(results)

        # Удаляем график после рассылки
        if chart_path and os.path.exists(chart_path):
            try:
                os.remove(chart_path)
            except OSError as ex:
                # Конкретная ошибка файловой системы
                logging.debug(f"Не удалось удалить файл {chart_path}: {ex}")
            except Exception as ex:
                # Другие ошибки (маловероятно, но на всякий случай)
                logging.debug(f"Неожиданная ошибка при удалении файла {chart_path}: {ex}")

        return sent_success
