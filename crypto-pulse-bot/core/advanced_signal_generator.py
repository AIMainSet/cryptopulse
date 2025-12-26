import pandas as pd
import pandas_ta as ta
import ccxt.async_support as ccxt
import asyncio
import logging
from typing import Optional, Dict, List
from services.signal_quality import SignalQualityRater
from datetime import datetime

logger = logging.getLogger(__name__)


class AdvancedSignalGenerator:
    def __init__(self, exchange_config: Dict, symbols: List[str] = None):
        """Инициализация с конфигурацией, а не жесткими значениями"""
        exchange_name = exchange_config.get('exchange', 'bybit')
        self.exchange = getattr(ccxt, exchange_name)({
            'enableRateLimit': True,
            'apiKey': exchange_config.get('api_key', ''),
            'secret': exchange_config.get('api_secret', ''),
            'options': {'defaultType': 'spot'}
        })

        self.symbols = symbols or []
        self.signal_rater = SignalQualityRater()

        # Конфигурируемые параметры
        self.MIN_VOLUME = 5_000_000
        self.LOOKBACK_BARS = 20
        self.ATR_SL_MULT = 1.5
        self.MIN_RR = 1.5
        self.MIN_SCORE = 60

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

    async def _calculate_indicators(self, df: pd.DataFrame, symbol: str) -> Optional[pd.DataFrame]:
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
            bb_df = ta.bbands(df['close'], length=20, std=2)
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

            if daily_volume < self.MIN_VOLUME:
                logger.debug(f"⏭ {symbol}: низкий объем ({daily_volume:,.0f} USDT)")
                return None

            # 2. Получение свечей
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe='1h', limit=250)
            if len(ohlcv) < 200:
                return None

            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

            # 3. Расчет индикаторов
            df = await self._calculate_indicators(df, symbol)
            if df is None:
                return None

            last = df.iloc[-1]
            prev = df.iloc[-2]

            # 4. Оценка условий
            eval_result = self._evaluate_signal_conditions(last, prev)

            # 5. Определение направления
            direction = None
            confidence = 0.5

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
                    sorted_sup = sorted(support_levels, reverse=True)
                    tp1, tp2, tp3 = sorted_sup[0], sorted_sup[1], sorted_sup[2]
                    if tp1 > entry - risk:
                        tp1 = entry - risk
                else:
                    tp1, tp2, tp3 = entry - risk, entry - (risk * 2), entry - (risk * 3)

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
            quality_result = await self.signal_rater.rate_signal(prepared_for_rating)

            if quality_result['signal_level'] in ['WEAK', 'LOW']:
                logger.info(f"⏭ {symbol} отфильтрован: {quality_result['signal_level']}")
                return None

            # 9. Формирование финального сигнала
            signal = {
                'symbol': symbol,
                'side': direction,
                'entry': entry,
                'tp1': tp1, 'tp2': tp2, 'tp3': tp3,
                'sl': sl,
                'confidence': confidence,
                'reason': " | ".join(reasons),
                'volume_24h': daily_volume,
                'quality_rating': quality_result['total_rating'],
                'signal_level': quality_result['signal_level'],
                'quality_emoji': quality_result['emoji'],
                'quality_report': self.signal_rater.generate_quality_report(prepared_for_rating, quality_result),
                'risk_reward_ratio': risk_reward,
                'timestamp': datetime.now().isoformat()
            }

            logger.info(f"✅ {quality_result['signal_level']} сигнал {symbol}: {direction.upper()} "
                        f"(score: {eval_result['buy_score'] if direction == 'buy' else eval_result['sell_score']}, "
                        f"rating: {quality_result['total_rating']:.1%})")

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
