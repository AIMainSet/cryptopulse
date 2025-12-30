import pandas as pd
import pandas_ta as ta
import asyncio
import logging
from typing import Optional, Dict, List
from services.signal_quality import SignalQualityRater
from datetime import datetime, timezone  # Добавляем timezone
from analytics.multi_timeframe_analyzer import MultiTimeframeAnalyzer

logger = logging.getLogger(__name__)


class AdvancedSignalGenerator:
    def __init__(self, exchange, symbols: List[str] = None):
        self.btc_trend = "NEUTRAL"
        """Принимаем готовую биржу вместо конфигурации"""
        self.exchange = exchange

        self.symbols = symbols or []
        self.signal_rater = SignalQualityRater()

        # Конфигурируемые параметры
        self.MIN_VOLUME = 1_000_000  # Минимальный суточный объем (USDT). Только ликвидные пары.
        self.LOOKBACK_BARS = 100  # Количество свечей для анализа уровней поддержки/сопротивления
        self.ATR_SL_MULT = 2.0  # Множитель для стоп-лосса (1.5 = стоп в 1.5 * ATR от цены входа)
        self.MIN_RR = 2.0  # Минимальное соотношение риск/прибыль (1.0 = риск равен прибыли)
        self.MIN_SCORE = 40  # Минимальный балл для генерации сигнала

        self.mtf_analyzer = MultiTimeframeAnalyzer(self.exchange)

    @staticmethod
    def _create_dataframe(ohlcv) -> pd.DataFrame:
        """Создаёт DataFrame из OHLCV данных"""
        df = pd.DataFrame(
            ohlcv,
            columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
        )
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df

    async def _get_historical_volumes(self, symbol: str, timeframe: str = '1h', limit: int = 50) -> List[float]:
        """Получает исторические объемы для анализа"""
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            return df['volume'].tolist()
        except Exception as e:
            logger.error(f"Ошибка получения исторических объемов для {symbol}: {e}")
            return []

    async def analyze_pair(self, symbol: str):
        """Основной метод анализа с multi-timeframe"""
        try:
            # 1. Получаем multi-timeframe анализ
            mtf_result = await self.mtf_analyzer.analyze_all_timeframes(symbol)

            # 2. Проверяем, есть ли сильный сигнал
            if mtf_result['final_signal'] == 'none':
                return None

            if mtf_result['confidence'] < 0.6:  # Минимальная уверенность
                return None

            # 3. Получаем детализированные данные для основного ТФ (1h)
            ohlcv = await self.exchange.fetch_ohlcv(symbol, '1h', limit=200)
            df = self._create_dataframe(ohlcv)

            # Используем упрощенный расчет индикаторов для этого метода
            df = self._calculate_basic_indicators(df)

            # 4. Получаем текущую цену и объем
            ticker = await self.exchange.fetch_ticker(symbol)
            current_price = ticker['last']
            volume = ticker['baseVolume']

            # 5. Рассчитываем уровни TP/SL на основе ATR
            atr = df['atr'].iloc[-1]
            if mtf_result['final_signal'] == 'BUY':
                entry_price = current_price
                stop_loss = entry_price - (atr * 1.5)
                take_profit_1 = entry_price + (atr * 1.0)
                take_profit_2 = entry_price + (atr * 2.0)
            else:  # SELL
                entry_price = current_price
                stop_loss = entry_price + (atr * 1.5)
                take_profit_1 = entry_price - (atr * 1.0)
                take_profit_2 = entry_price - (atr * 2.0)

            # 6. Собираем сигнал
            signal = {
                'symbol': symbol,
                'signal_type': mtf_result['final_signal'],
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'take_profit_1': take_profit_1,
                'take_profit_2': take_profit_2,
                'timestamp': datetime.now(timezone.utc),  # Исправлено на timezone-aware
                'confidence': mtf_result['confidence'],
                'timeframe_analysis': mtf_result['timeframe_results'],
                'volume': volume,
                'atr': atr,
                'rsi': df['rsi'].iloc[-1]
            }

            # 7. Получаем исторические данные для анализа объема
            historical_volumes = await self._get_historical_volumes(symbol, '1h', 50)
            historical_data = {'volumes': historical_volumes}

            # 8. Оцениваем качество сигнала (используем self.signal_rater)
            quality_report = self.signal_rater.rate_signal(signal, historical_data)

            # 9. Добавляем оценку качества в сигнал
            signal['quality_report'] = quality_report
            signal['quality_rating'] = quality_report['strength']
            signal['quality_score'] = quality_report['percentage']

            # 10. Фильтруем слабые сигналы
            if quality_report['strength'] in ['WEAK', 'LOW']:
                logger.info(f"Слабый сигнал для {symbol}: {quality_report['strength']}")
                return None

            return signal

        except Exception as e:
            logger.error(f"Ошибка анализа {symbol}: {e}", exc_info=True)
            return None

    async def update_btc_trend(self, change_1h_percent):
        """
        Вызывай этот метод раз в минуту, передавая изменение BTC за час.
        """
        if change_1h_percent < -0.5:
            self.btc_trend = "DOWN"
        elif change_1h_percent > 0.5:
            self.btc_trend = "UP"
        else:
            self.btc_trend = "NEUTRAL"

    def validate_signal(self, signal_type, indicators):
        """
        Возвращает кортеж (bool is_valid, str reason).
        True = сигнал хороший, можно торговать.
        False = сигнал плохой, игнорируем.
        """

        # --- ФИЛЬТР 1: БИТКОИН (ГЛАВНЫЙ) ---
        # Если Биткоин падает, запрещаем покупать альткоины
        if signal_type == 'LONG' and self.btc_trend == 'DOWN':
            return False, "BTC DUMPING (Risk High)"

        # Если Биткоин растет, опасно шортить
        if signal_type == 'SHORT' and self.btc_trend == 'UP':
            return False, "BTC PUMPING (Risk High)"

        # --- ФИЛЬТР 2: ОБЪЕМ (ЛИКВИДНОСТЬ) ---
        # Нет объема = нет настоящего движения
        vol = indicators.get('volume', 0)
        avg_vol = indicators.get('volume_mean_20', 1)

        if vol < (avg_vol * 1.2):
            return False, f"LOW VOLUME (Cur: {vol:.0f} < Req: {avg_vol * 1.2:.0f})"

        # --- ФИЛЬТР 3: СИЛА ТРЕНДА (ADX) ---
        # ADX < 20 означает флэт (боковик). Индикаторы врут.
        adx = indicators.get('adx', 0)
        if adx < 20:
            return False, f"WEAK TREND (ADX {adx:.1f} < 20)"

        # --- ФИЛЬТР 4: EMA 200 (ГЛОБАЛЬНЫЙ ТРЕНД) ---
        # Торгуем только по тренду
        close = indicators.get('close', 0)
        ema_200 = indicators.get('ema_200', 0)

        if ema_200 > 0:
            if signal_type == 'LONG' and close < ema_200:
                return False, "PRICE BELOW EMA 200 (Don't Long downtrend)"
            if signal_type == 'SHORT' and close > ema_200:
                return False, "PRICE ABOVE EMA 200 (Don't Short uptrend)"

        return True, "VALID_SIGNAL"

    def update_symbols(self, new_symbols: List[str]):
        """Безопасное обновление списка пар"""
        if not new_symbols:
            return

        cleaned = []
        for s in new_symbols:
            if isinstance(s, str) and '/' in s:
                cleaned.append(s.strip().upper())

        self.symbols = list(set(cleaned))
        if self.symbols:
            logger.info(f"📋 Обновлен список пар: {self.symbols[:5]}..." +
                        (f" и еще {len(self.symbols) - 5}" if len(self.symbols) > 5 else ""))

    @staticmethod
    def _calculate_basic_indicators(df: pd.DataFrame) -> pd.DataFrame:
        """Упрощенный расчет индикаторов для analyze_pair"""
        try:
            # Базовые индикаторы
            df['rsi'] = ta.rsi(df['close'], length=14)
            df['ema_20'] = ta.ema(df['close'], length=20)
            df['ema_50'] = ta.ema(df['close'], length=50)
            df['ema_200'] = ta.ema(df['close'], length=200)
            df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)
            return df
        except Exception as e:
            logger.error(f"Ошибка расчета индикаторов: {e}")
            return df

    @staticmethod
    def _calculate_indicators(df: pd.DataFrame, symbol: str) -> Optional[pd.DataFrame]:
        """Безопасный расчет всех индикаторов"""
        try:
            # Базовые индикаторы
            df['rsi'] = ta.rsi(df['close'], length=14)
            df['ema_20'] = ta.ema(df['close'], length=20)
            df['ema_50'] = ta.ema(df['close'], length=50)
            df['ema_200'] = ta.ema(df['close'], length=200)
            df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)

            # ADX
            adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
            df['adx'] = adx_df['ADX_14']

            # MACD
            macd_df = ta.macd(df['close'], fast=12, slow=26, signal=9)
            df['macd'] = macd_df['MACD_12_26_9']
            df['macd_signal'] = macd_df['MACDs_12_26_9']
            df['macd_hist'] = macd_df['MACDh_12_26_9']

            # Bollinger Bands
            bb_df = ta.bbands(df['close'], length=20, std=2)  # type: ignore
            # Ищем правильные имена колонок
            for suffix in ['_20_2.0', '_20_2']:
                upper_col = f'BBU{suffix}'
                middle_col = f'BBM{suffix}'
                lower_col = f'BBL{suffix}'

                if upper_col in bb_df.columns:
                    df['bb_upper'] = bb_df[upper_col]
                    df['bb_middle'] = bb_df[middle_col]
                    df['bb_lower'] = bb_df[lower_col]
                    break
            else:
                # Fallback
                df['bb_middle'] = df['close'].rolling(20).mean()
                std = df['close'].rolling(20).std()
                df['bb_upper'] = df['bb_middle'] + (std * 2)
                df['bb_lower'] = df['bb_middle'] - (std * 2)

            df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['bb_middle']

            # Stochastic
            stoch_df = ta.stoch(df['high'], df['low'], df['close'], k=14, d=3)
            df['stoch_k'] = stoch_df['STOCHk_14_3_3']
            df['stoch_d'] = stoch_df['STOCHd_14_3_3']

            logger.info(
                f"📈 {symbol}: RSI последнее значение = {df['rsi'].iloc[-1] if not df['rsi'].isnull().all() else 'N/A'}")

            return df

        except Exception as e:
            logger.error(f"Ошибка расчета индикаторов для {symbol}: {e}")
            return None

    def _evaluate_signal_conditions(self, last, prev) -> Dict:
        """Оценка условий для сигнала с системой баллов"""
        buy_score = 0
        buy_reasons = []
        sell_score = 0
        sell_reasons = []

        logger.info(f"🔍 Начало оценки условий...")

        # Трендовые условия
        if last['close'] > last['ema_200']:
            buy_score += 10
            buy_reasons.append("Цена выше EMA200")
        else:
            sell_score += 10
            sell_reasons.append("Цена ниже EMA200")

        if last['ema_20'] > last['ema_50']:
            buy_score += 10
            buy_reasons.append("EMA20 > EMA50")
        else:
            sell_score += 10
            sell_reasons.append("EMA20 < EMA50")

        if last['adx'] > 25:
            buy_score += 5
            sell_score += 5
            buy_reasons.append(f"Сильный тренд (ADX {last['adx']:.1f})")
            sell_reasons.append(f"Сильный тренд (ADX {last['adx']:.1f})")

        # Моментные условия
        if 40 < last['rsi'] < 65:
            buy_score += 10
            buy_reasons.append(f"RSI {last['rsi']:.1f}")
        elif 35 < last['rsi'] < 60:
            sell_score += 10
            sell_reasons.append(f"RSI {last['rsi']:.1f}")

        if last['macd'] > last['macd_signal']:
            buy_score += 10
            buy_reasons.append("MACD бычий")
        else:
            sell_score += 10
            sell_reasons.append("MACD медвежий")

        # Паттерны
        if prev['close'] <= prev['ema_20'] and last['close'] > last['ema_20']:
            buy_score += 15
            buy_reasons.append("Пробой EMA20 снизу")

        if prev['close'] >= prev['ema_20'] and last['close'] < last['ema_20']:
            sell_score += 15
            sell_reasons.append("Пробой EMA20 сверху")

        # Bollinger Bands
        bb_position = (last['close'] - last['bb_lower']) / (last['bb_upper'] - last['bb_lower']) * 100
        if bb_position < 30:
            buy_score += 10
            buy_reasons.append(f"У нижней границы BB ({bb_position:.1f}%)")
        elif bb_position > 70:
            sell_score += 10
            sell_reasons.append(f"У верхней границы BB ({bb_position:.1f}%)")

        logger.info(f"📊 Итоговые баллы: buy={buy_score}, sell={sell_score}, MIN_SCORE={self.MIN_SCORE}")

        return {
            'buy_score': buy_score,
            'buy_reasons': buy_reasons,
            'sell_score': sell_score,
            'sell_reasons': sell_reasons,
            'bb_position': bb_position
        }

    async def get_data_and_analyze(self, symbol: str) -> Optional[Dict]:
        """Основной метод анализа"""
        try:
            # 1. Проверка объема
            ticker = await self.exchange.fetch_ticker(symbol)
            daily_volume = float(ticker.get('quoteVolume', 0))

            logger.info(f"📊 {symbol}: объем = {daily_volume:,.0f} USDT, нужно {self.MIN_VOLUME:,.0f}")

            if daily_volume < self.MIN_VOLUME:
                logger.debug(f"⏭ {symbol}: низкий объем ({daily_volume:,.0f} USDT)")
                return None

            # 2. Получение свечей
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe='1h', limit=250)
            if len(ohlcv) < 200:
                return None

            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

            # 3. Расчет индикаторов
            df = self._calculate_indicators(df, symbol)
            if df is None:
                return None

            last = df.iloc[-1]
            prev = df.iloc[-2]

            # 4. Оценка условий
            eval_result = self._evaluate_signal_conditions(last, prev)

            logger.info(f"🎯 {symbol}: Проверка условий завершена")

            if eval_result['buy_score'] >= eval_result['sell_score'] and eval_result['buy_score'] >= self.MIN_SCORE:
                direction = "buy"
                confidence = min(eval_result['buy_score'] / 100, 0.95)
                reasons = eval_result['buy_reasons']
            elif eval_result['sell_score'] >= self.MIN_SCORE:
                direction = "sell"
                confidence = min(eval_result['sell_score'] / 100, 0.95)
                reasons = eval_result['sell_reasons']
            else:
                logger.debug(
                    f"⏭ {symbol}: недостаточно баллов (buy: {eval_result['buy_score']}, sell: {eval_result['sell_score']})")
                return None

            # 6. Расчет уровней (используем вашу улучшенную логику с унифицированными периодами)
            entry = float(last['close'])
            atr_val = float(last['atr'])

            if direction == "buy":
                # Ваш расчет уровней с LOOKBACK_BARS = 20
                support_levels = df['low'].tail(self.LOOKBACK_BARS).nsmallest(3).values
                local_min = min(support_levels) if len(support_levels) > 0 else entry - (atr_val * 2)
                sl = min(local_min, entry - (atr_val * self.ATR_SL_MULT))
                risk = entry - sl

                resistance_levels = df['high'].tail(self.LOOKBACK_BARS).nlargest(3).values
                if len(resistance_levels) >= 3:
                    sorted_res = sorted(resistance_levels)
                    tp1, tp2, tp3 = sorted_res[0], sorted_res[1], sorted_res[2]
                    if tp1 < entry + risk:
                        tp1 = entry + risk
                else:
                    tp1, tp2, tp3 = entry + risk, entry + (risk * 2), entry + (risk * 3)

            else:  # sell
                resistance_levels = df['high'].tail(self.LOOKBACK_BARS).nlargest(3).values
                local_max = max(resistance_levels) if len(resistance_levels) > 0 else entry + (atr_val * 2)
                sl = max(local_max, entry + (atr_val * self.ATR_SL_MULT))
                risk = sl - entry

                support_levels = df['low'].tail(self.LOOKBACK_BARS).nsmallest(3).values
                if len(support_levels) >= 3:
                    valid_supports = [s for s in support_levels if s < entry]
                    if len(valid_supports) >= 3:
                        sorted_sup = sorted(valid_supports, reverse=True)  # От ближайшего к дальнему
                        tp1, tp2, tp3 = sorted_sup[0], sorted_sup[1], sorted_sup[2]
                    else:
                        # Если не нашли 3 уровня, используем ATR
                        tp1 = entry - (atr_val * self.MIN_RR)
                        tp2 = entry - (atr_val * self.MIN_RR * 1.5)
                        tp3 = entry - (atr_val * self.MIN_RR * 2)
                else:
                    # Fallback: используем ATR для расчета TP
                    tp1 = entry - (atr_val * self.MIN_RR)
                    tp2 = entry - (atr_val * self.MIN_RR * 1.5)
                    tp3 = entry - (atr_val * self.MIN_RR * 2)

                if tp1 >= entry:
                    tp1 = entry - (atr_val * self.MIN_RR)

            # Проверка минимального RR
            current_rr = (tp1 - entry) / risk if direction == "buy" else (entry - tp1) / risk
            if current_rr < self.MIN_RR:
                logger.warning(f"⚠️ {symbol}: RR {current_rr:.2f} < {self.MIN_RR}, корректируем")
                if direction == "buy":
                    tp1 = entry + (risk * self.MIN_RR)
                else:
                    tp1 = entry - (risk * self.MIN_RR)

            # 7. Подготовка данных для оценки качества (ПОЛНАЯ!)
            volatility_pct = df['close'].pct_change().std() * 100
            risk_reward = (tp1 - entry) / risk if direction == "buy" else (entry - tp1) / risk

            prepared_for_rating = {
                'symbol': symbol,
                'direction': direction.upper(),
                'risk_reward': round(risk_reward, 2),
                'confidence': round(confidence, 2),
                'volatility': f"{volatility_pct:.2f}%",
                'timeframes_analyzed': ['1h'],
                'adx_value': float(last['adx']),
                'rsi_value': float(last['rsi']),
                'volume': daily_volume,
                'macd_value': float(last['macd']),
                'macd_signal': float(last['macd_signal']),
                'stoch_k': float(last['stoch_k']),
                'stoch_d': float(last['stoch_d']),
                'bb_width': float(last['bb_width']),
                'bb_position': 'lower' if eval_result['bb_position'] < 20 else
                'upper' if eval_result['bb_position'] > 80 else 'middle',
                'bb_position_pct': eval_result['bb_position'],
                'score': eval_result['buy_score'] if direction == "buy" else eval_result['sell_score']
            }

            # 8. Оценка качества
            quality_result = self.signal_rater.rate_signal(prepared_for_rating)

            # 9. Фильтрация по качеству (используем новые ключи)
            if quality_result['strength'] in ['WEAK', 'LOW']:
                logger.info(f"⏭ {symbol} отфильтрован: {quality_result['strength']}")
                return None

            # 10. Формирование финального сигнала (обновленные ключи)
            signal = {
                'symbol': symbol,
                'side': direction,
                'entry': entry,
                'tp1': tp1, 'tp2': tp2, 'tp3': tp3,
                'sl': sl,
                'confidence': confidence,
                'reason': " | ".join(reasons),
                'volume_24h': daily_volume,
                # Обновленные ключи из нового SignalQualityRater
                'quality_rating': quality_result['strength'],
                'quality_score': quality_result['percentage'],
                'quality_factors': quality_result.get('factors', {}),
                'quality_recommendation': quality_result.get('recommendation', ''),
                'risk_reward_ratio': risk_reward,
                'timestamp': datetime.now(timezone.utc).isoformat()  # Исправлено
            }

            logger.info(f"✅ {quality_result['strength']} сигнал {symbol}: {direction.upper()} "
                        f"(score: {eval_result['buy_score'] if direction == 'buy' else eval_result['sell_score']}, "
                        f"rating: {quality_result['percentage']:.1f}%)")

            return signal

        except Exception as e:
            logger.error(f"Ошибка анализа {symbol}: {e}", exc_info=True)
            return None

    async def run_analysis_cycle(self) -> List[Dict]:
        """Запуск анализа всех пар"""
        if not self.symbols:
            return []

        signals = []
        for symbol in self.symbols:
            try:
                signal = await self.get_data_and_analyze(symbol)
                if signal:
                    signals.append(signal)
                await asyncio.sleep(0.3)  # Rate limiting
            except Exception as e:
                logger.error(f"Ошибка при анализе {symbol}: {e}")
                continue

        logger.info(f"📊 Цикл завершен: {len(signals)} сигналов из {len(self.symbols)} пар")
        return signals

    async def close(self):
        """Корректное закрытие соединений"""
        if hasattr(self, 'exchange'):
            await self.exchange.close()
