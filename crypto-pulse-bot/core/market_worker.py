import asyncio
import logging
import os
import pandas as pd
import pandas_ta as ta
from datetime import datetime
from sqlalchemy import select
from aiogram import Bot
from aiogram.types import FSInputFile

# Внутренние модули (проверь, что пути совпадают с твоей структурой)
from core.advanced_signal_generator import AdvancedSignalGenerator
from analytics.signal_tracker import SignalTracker
from database import async_session, User, check_and_expire_subscriptions
from core.chart_gen import create_signal_chart
from core.formatter import EnhancedSignalFormatter
import config


# --- 1. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (МАТЕМАТИКА) ---

def calculate_position_size(deposit, risk_pct, entry, sl):
    """
    Рассчитывает объем позиции исходя из риска на сделку.
    Возвращает объем в USDT.
    """
    try:
        d = float(deposit or 0)
        r = float(risk_pct or 0)
        e = float(entry or 0)
        s = float(sl or 0)

        if d <= 0 or r <= 0 or e <= 0:
            return 0

        # Риск в долларах (например, 10$ при депозите 1000 и риске 1%)
        risk_money = d * (r / 100)

        # Дистанция стопа в % (0.02 = 2%)
        stop_dist = abs(e - s) / e

        if stop_dist <= 0:
            return 0

        # Объем позиции = Риск / Дистанция стопа
        position_size = risk_money / stop_dist

        return round(position_size, 2)
    except Exception as err:
        logging.error(f"⚠️ Ошибка расчета позиции: {err}")
        return 0


# --- 2. ОСНОВНОЙ КЛАСС ВОРКЕРА ---

class MarketWorker:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.gen = AdvancedSignalGenerator()
        self.tracker = SignalTracker(bot)
        self.formatter = EnhancedSignalFormatter()
        self._tasks = []

    async def start(self):
        """Запуск основного цикла мониторинга рынка"""
        logging.info("🚀 Запуск фоновых задач воркера...")

        # Инициализация фоновых задач (трекер PNL и проверка подписок)
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

    def _on_task_completed(self, task):
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
                    except:
                        pass
            except Exception as e:
                logging.error(f"Ошибка в subscription_checker: {e}")
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
        signal_payload = {
            'symbol': symbol,
            'direction': 'LONG' if signal['side'].upper() in ['BUY', 'LONG'] else 'SHORT',
            'entry': signal['entry'],
            'tp1': signal.get('tp1'),
            'tp2': signal.get('tp2'),
            'tp3': signal.get('tp3'),
            'sl': signal['sl'],
            'risk': 'Medium',  # Можно сделать динамическим от ATR
            'leverage': 'Isolated 5x-10x',
            'reason': signal.get('reason', 'Технический анализ'),
            'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        rating_data = {
            'emoji': '💎' if signal.get('status') == 'ULTRA' else '🔥',
            'status': signal.get('status', 'ULTRA'),
            'confidence': signal.get('confidence', 0.94)
        }

        base_text = self.formatter.format_signal_with_rating(signal_payload, rating_data)

        # --- В. Сбор получателей ---
        async with async_session() as session:
            result = await session.execute(select(User).where(User.status == "PREMIUM"))
            users = result.scalars().all()

        # --- Г. Функция отправки одному юзеру (внутренняя) ---
        async def send_to_one_user(user):
            # Проверка фильтра пар
            user_pairs = [p.strip().upper() for p in user.selected_pairs.split(",")] if user.selected_pairs else []
            if symbol.upper() not in user_pairs:
                return False

            # Индивидуальный расчет позиции
            pos_size = calculate_position_size(user.deposit, user.risk_per_trade, signal['entry'], signal['sl'])
            esc_pos = EnhancedSignalFormatter.escape_md(str(pos_size))
            esc_risk = EnhancedSignalFormatter.escape_md(str(user.risk_per_trade))

            # Добавляем персонализацию к тексту
            final_text = (
                f"{base_text}\n\n"
                f"💰 *ВАШ ИНДИВИДУАЛЬНЫЙ РАСЧЕТ:*\n"
                f"└ Объем сделки: `{esc_pos}` USDT \\(риск {esc_risk}%\\)"
            )

            try:
                if chart_path and os.path.exists(chart_path):
                    await self.bot.send_photo(user.user_id, photo=FSInputFile(chart_path), caption=final_text,
                                              parse_mode="MarkdownV2")
                else:
                    await self.bot.send_message(user.user_id, final_text, parse_mode="MarkdownV2")
                return True
            except Exception as e:
                logging.error(f"🚨 Ошибка отправки юзеру {user.user_id}: {e}")
                return False

        # --- Д. Пакетная отправка (Batching) ---
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
            except:
                pass

        return sent_success
