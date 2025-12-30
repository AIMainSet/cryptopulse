"""
Мультитаймфреймовый анализатор для подтверждения сигналов
"""

from typing import Dict, List, Tuple
import pandas as pd
import pandas_ta as ta
import numpy as np


class MultiTimeframeAnalyzer:
    """Анализатор сигналов на нескольких таймфреймах"""

    def __init__(self, exchange, timeframes: List[str] = None):
        self.exchange = exchange
        self.timeframes = timeframes or ['15m', '1h', '4h']
        self.weights = {
            '15m': 0.2,  # Младший ТФ - меньше вес
            '1h': 0.3,  # Основной ТФ
            '4h': 0.5  # Старший ТФ - больше вес
        }

    async def analyze_all_timeframes(self, symbol: str) -> Dict:
        """
        Анализирует пару на всех таймфреймах
        Возвращает консенсусный сигнал
        """
        results = {}

        for tf in self.timeframes:
            try:
                # Загружаем данные для каждого ТФ
                ohlcv = await self.exchange.fetch_ohlcv(symbol, tf, limit=300)
                df = self._create_dataframe(ohlcv)
                df = self._calculate_indicators(df)

                # Определяем тренд и сигнал для этого ТФ
                signal = self._analyze_timeframe(df, tf)
                results[tf] = signal

            except Exception as e:
                print(f"Ошибка анализа {symbol} на {tf}: {e}")
                results[tf] = {'trend': 'neutral', 'signal': 'none', 'strength': 0}

        # Рассчитываем консенсус
        consensus = self._calculate_consensus(results)
        return {
            'timeframe_results': results,
            'consensus': consensus,
            'final_signal': consensus['final_signal'],
            'confidence': consensus['confidence']
        }

    def _create_dataframe(self, ohlcv) -> pd.DataFrame:
        """Создаёт DataFrame из OHLCV данных"""
        df = pd.DataFrame(
            ohlcv,
            columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
        )
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df

    def _calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Рассчитывает индикаторы для таймфрейма"""
        # Трендовые индикаторы
        df['ema_20'] = ta.ema(df['close'], length=20)
        df['ema_50'] = ta.ema(df['close'], length=50)
        df['ema_200'] = ta.ema(df['close'], length=200)

        # Моментум
        df['rsi'] = ta.rsi(df['close'], length=14)

        # Волатильность
        df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)

        # MACD
        macd = ta.macd(df['close'])
        df['macd'] = macd['MACD_12_26_9']
        df['macd_signal'] = macd['MACDs_12_26_9']
        df['macd_hist'] = macd['MACDh_12_26_9']

        return df

    def _analyze_timeframe(self, df: pd.DataFrame, timeframe: str) -> Dict:
        """Анализирует конкретный таймфрейм"""
        latest = df.iloc[-1]
        prev = df.iloc[-2]

        # Определяем тренд
        trend = self._determine_trend(df)

        # Определяем сигнал
        signal, strength = self._determine_signal(df, timeframe)

        return {
            'trend': trend,
            'signal': signal,
            'strength': strength,
            'price': float(latest['close']),
            'rsi': float(latest['rsi']),
            'ema_trend': self._get_ema_trend(df)
        }

    def _determine_trend(self, df: pd.DataFrame) -> str:
        """Определяет тренд по EMA и MACD"""
        latest = df.iloc[-1]

        # Проверка по EMA
        ema_bullish = latest['ema_20'] > latest['ema_50'] > latest['ema_200']
        ema_bearish = latest['ema_20'] < latest['ema_50'] < latest['ema_200']

        # Проверка по MACD
        macd_bullish = latest['macd'] > latest['macd_signal']
        macd_bearish = latest['macd'] < latest['macd_signal']

        if ema_bullish and macd_bullish:
            return 'bullish'
        elif ema_bearish and macd_bearish:
            return 'bearish'
        else:
            return 'neutral'

    def _determine_signal(self, df: pd.DataFrame, timeframe: str) -> Tuple[str, float]:
        """Определяет торговый сигнал для таймфрейма"""
        latest = df.iloc[-1]
        prev = df.iloc[-2]

        signals = []
        strength = 0

        # 1. Сигналы по RSI
        if latest['rsi'] < 30 and prev['rsi'] >= 30:
            signals.append(('BUY', 'rsi_oversold'))
            strength += 0.3
        elif latest['rsi'] > 70 and prev['rsi'] <= 70:
            signals.append(('SELL', 'rsi_overbought'))
            strength += 0.3

        # 2. Пересечение EMA
        if latest['ema_20'] > latest['ema_50'] and prev['ema_20'] <= prev['ema_50']:
            signals.append(('BUY', 'ema_crossover'))
            strength += 0.4
        elif latest['ema_20'] < latest['ema_50'] and prev['ema_20'] >= prev['ema_50']:
            signals.append(('SELL', 'ema_crossover'))
            strength += 0.4

        # 3. MACD гистограмма меняет направление
        if latest['macd_hist'] > 0 and prev['macd_hist'] < 0:
            signals.append(('BUY', 'macd_hist_turn'))
            strength += 0.3
        elif latest['macd_hist'] < 0 and prev['macd_hist'] > 0:
            signals.append(('SELL', 'macd_hist_turn'))
            strength += 0.3

        # Определяем финальный сигнал
        if not signals:
            return 'none', 0

        # Группируем по направлению
        buy_signals = [s for s in signals if s[0] == 'BUY']
        sell_signals = [s for s in signals if s[0] == 'SELL']

        if len(buy_signals) > len(sell_signals):
            final_strength = sum(s[2] for s in buy_signals) / len(buy_signals)
            return 'BUY', min(final_strength, 1.0)
        elif len(sell_signals) > len(buy_signals):
            final_strength = sum(s[2] for s in sell_signals) / len(sell_signals)
            return 'SELL', min(final_strength, 1.0)
        else:
            return 'none', 0

    def _calculate_consensus(self, results: Dict) -> Dict:
        """Рассчитывает консенсус между таймфреймами"""
        signals = []
        total_weight = 0
        weighted_strength = 0

        for tf, result in results.items():
            if result['signal'] != 'none':
                weight = self.weights.get(tf, 0.1)
                signals.append((result['signal'], weight))
                weighted_strength += result['strength'] * weight
                total_weight += weight

        if not signals:
            return {'final_signal': 'none', 'confidence': 0}

        # Подсчитываем голоса с весами
        vote_count = {'BUY': 0, 'SELL': 0}
        for signal, weight in signals:
            vote_count[signal] += weight

        # Определяем победителя
        if vote_count['BUY'] > vote_count['SELL']:
            final_signal = 'BUY'
            confidence = vote_count['BUY'] / total_weight
        elif vote_count['SELL'] > vote_count['BUY']:
            final_signal = 'SELL'
            confidence = vote_count['SELL'] / total_weight
        else:
            final_signal = 'none'
            confidence = 0

        # Учитываем силу сигнала
        confidence = (confidence + weighted_strength) / 2

        return {
            'final_signal': final_signal,
            'confidence': confidence,
            'vote_count': vote_count
        }
