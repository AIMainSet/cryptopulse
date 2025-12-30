import asyncio
import logging
from database import async_session, User, SignalHistory
from sqlalchemy import select
from database import close_signal_in_db, save_new_signal

logger = logging.getLogger(__name__)

class SignalTracker:
    def __init__(self, bot):
        self.bot = bot
        self.active_signals = []  # Список живых сделок

        asyncio.create_task(self.load_active_signals_from_db())

    async def load_active_signals_from_db(self):
        """Загружает активные сигналы из базы после перезапуска"""
        try:
            async with async_session() as session:
                result = await session.execute(
                    select(SignalHistory).where(SignalHistory.status == "OPEN")
                )
                signals = result.scalars().all()

                for sig in signals:
                    # Здесь проблема: в базе нет полей tp и sl!
                    # Пока создаем заглушку
                    self.active_signals.append({
                        'symbol': sig.symbol,
                        'side': sig.side,
                        'entry': sig.entry_price,
                        'tp': sig.tp1,  # Берем tp1 из базы
                        'sl': sig.sl  # Берем sl из базы
                    })
                logger.info(f"✅ Загружено {len(signals)} активных сигналов из БД")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки сигналов из БД: {e}")

    async def add_signal(self, signal):
        """Добавляет сигнал в мониторинг"""
        # Проверяем, нет ли уже такого символа в работе, чтобы не дублировать
        if any(s['symbol'] == signal['symbol'] for s in self.active_signals):
            return

        if 'tp' not in signal and 'tp1' in signal:
            signal['tp'] = signal['tp1']

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

                    if 'tp' not in sig:
                        if 'tp1' in sig:
                            sig['tp'] = sig['tp1']  # Используем tp1 как основной TP
                        else:
                            logger.error(f"Сигнал {symbol} не содержит ни 'tp', ни 'tp1'")
                            continue

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


# analytics/signal_tracker.py - добавить методы
class EnhancedSignalTracker:
    async def track_signal_result(self, signal_data: Dict):
        """Отслеживает результат сигнала и сохраняет статистику"""
        try:
            signal_id = self._generate_signal_id(signal_data)

            async with self.session() as session:
                # Сохраняем начальные данные
                stat = SignalStatistic(
                    signal_id=signal_id,
                    symbol=signal_data['symbol'],
                    direction=signal_data.get('side', signal_data.get('direction', 'unknown')),
                    entry_price=signal_data['entry_price'] if 'entry_price' in signal_data else signal_data['entry'],
                    stop_loss=signal_data['stop_loss'] if 'stop_loss' in signal_data else signal_data['sl'],
                    take_profit_1=signal_data.get('take_profit_1', signal_data.get('tp1', 0)),
                    take_profit_2=signal_data.get('take_profit_2', signal_data.get('tp2', 0)),
                    take_profit_3=signal_data.get('take_profit_3', signal_data.get('tp3', 0)),
                    quality_rating=signal_data.get('quality_rating', 'UNKNOWN'),
                    quality_score=signal_data.get('quality_score', 0),
                    generated_at=datetime.now(timezone.utc),
                    result='OPEN',

                    # Сохраняем факторы качества для анализа
                    mtf_consensus_score=signal_data.get('quality_report', {}).get('factors', {}).get('mtf_consensus',
                                                                                                     {}).get('score',
                                                                                                             0),
                    trend_strength_score=signal_data.get('quality_report', {}).get('factors', {}).get('trend_strength',
                                                                                                      {}).get('score',
                                                                                                              0),
                    volume_score=signal_data.get('quality_report', {}).get('factors', {}).get('volume', {}).get('score',
                                                                                                                0),
                    rsi_score=signal_data.get('quality_report', {}).get('factors', {}).get('rsi', {}).get('score', 0),
                    risk_reward_score=signal_data.get('quality_report', {}).get('factors', {}).get('risk_reward',
                                                                                                   {}).get('score', 0),
                )

                session.add(stat)
                await session.commit()

                logger.info(f"📊 Статистика: сохранен сигнал {signal_id} для {signal_data['symbol']}")

        except Exception as e:
            logger.error(f"Ошибка сохранения статистики: {e}")

    async def update_signal_result(self, signal_id: str, current_price: float, original_signal: Dict):
        """Обновляет результат сигнала при достижении TP/SL"""
        try:
            async with self.session() as session:
                stat = await session.execute(
                    select(SignalStatistic).where(SignalStatistic.signal_id == signal_id)
                )
                stat = stat.scalar_one_or_none()

                if not stat:
                    return

                entry = stat.entry_price
                direction = stat.direction

                # Определяем результат
                if direction == 'buy':
                    if current_price >= stat.take_profit_3:
                        result = 'TP3'
                        profit = (stat.take_profit_3 - entry) / entry * 100
                    elif current_price >= stat.take_profit_2:
                        result = 'TP2'
                        profit = (stat.take_profit_2 - entry) / entry * 100
                    elif current_price >= stat.take_profit_1:
                        result = 'TP1'
                        profit = (stat.take_profit_1 - entry) / entry * 100
                    elif current_price <= stat.stop_loss:
                        result = 'SL'
                        profit = (stat.stop_loss - entry) / entry * 100  # отрицательный
                    else:
                        return  # Сигнал еще открыт
                else:  # sell
                    if current_price <= stat.take_profit_3:
                        result = 'TP3'
                        profit = (entry - stat.take_profit_3) / entry * 100
                    elif current_price <= stat.take_profit_2:
                        result = 'TP2'
                        profit = (entry - stat.take_profit_2) / entry * 100
                    elif current_price <= stat.take_profit_1:
                        result = 'TP1'
                        profit = (entry - stat.take_profit_1) / entry * 100
                    elif current_price >= stat.stop_loss:
                        result = 'SL'
                        profit = (entry - stat.stop_loss) / entry * 100  # отрицательный
                    else:
                        return

                # Обновляем запись
                stat.result = result
                stat.closed_at = datetime.now(timezone.utc)
                stat.profit_loss_percent = profit

                await session.commit()

                logger.info(f"📊 Статистика: сигнал {signal_id} закрыт как {result} ({profit:+.2f}%)")

        except Exception as e:
            logger.error(f"Ошибка обновления статистики: {e}")

    def _generate_signal_id(self, signal_data: Dict) -> str:
        """Генерирует уникальный ID сигнала"""
        timestamp = int(datetime.now(timezone.utc).timestamp())
        symbol_clean = signal_data['symbol'].replace('/', '').lower()
        return f"{symbol_clean}_{timestamp}_{random.randint(1000, 9999)}"
