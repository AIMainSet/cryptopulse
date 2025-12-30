"""
Система оценки качества сигналов
"""
from datetime import datetime, timezone
from typing import Dict, Tuple
from enum import Enum
import numpy as np

class SignalStrength(Enum):
    WEAK = "WEAK"       # 0-3 балла
    LOW = "LOW"         # 4-6 баллов
    MEDIUM = "MEDIUM"   # 7-8 баллов
    HIGH = "HIGH"       # 9-10 баллов
    STRONG = "STRONG"   # 11-12 баллов


class EnhancedSignalQualityRater:
    """Расширенный оценщик качества сигналов"""

    def __init__(self):
        self.volume_period = 20  # Период для анализа объема

    def rate_signal(self, signal_data: Dict, historical_data: Dict = None) -> Dict:
        """
        Оценивает качество сигнала по 10 факторам
        Возвращает детализированный отчет
        """
        if not signal_data:
            return self._create_empty_rating()

        factors = {}
        total_score = 0
        max_possible = 0

        # 1. Multi-timeframe консенсус (0-2 балла)
        mtf_score, mtf_details = self._rate_mtf_consensus(signal_data)
        factors['mtf_consensus'] = {
            'score': mtf_score,
            'max': 2,
            'details': mtf_details
        }
        total_score += mtf_score
        max_possible += 2

        # 2. Сила тренда (0-2 балла)
        trend_score, trend_details = self._rate_trend_strength(signal_data)
        factors['trend_strength'] = {
            'score': trend_score,
            'max': 2,
            'details': trend_details
        }
        total_score += trend_score
        max_possible += 2

        # 3. Объем и ликвидность (0-2 балла)
        volume_score, volume_details = self._rate_volume(signal_data, historical_data)
        factors['volume'] = {
            'score': volume_score,
            'max': 2,
            'details': volume_details
        }
        total_score += volume_score
        max_possible += 2

        # 4. Уровни RSI (0-1 балл)
        rsi_score, rsi_details = self._rate_rsi(signal_data)
        factors['rsi'] = {
            'score': rsi_score,
            'max': 1,
            'details': rsi_details
        }
        total_score += rsi_score
        max_possible += 1

        # 5. Соотношение риск/прибыль (0-1 балл)
        rr_score, rr_details = self._rate_risk_reward(signal_data)
        factors['risk_reward'] = {
            'score': rr_score,
            'max': 1,
            'details': rr_details
        }
        total_score += rr_score
        max_possible += 1

        # 6. Волатильность (ATR) (0-1 балл)
        atr_score, atr_details = self._rate_volatility(signal_data)
        factors['volatility'] = {
            'score': atr_score,
            'max': 1,
            'details': atr_details
        }
        total_score += atr_score
        max_possible += 1

        # 7. Дивергенция (если есть) (0-1 балл)
        divergence_score, div_details = self._check_divergence(historical_data)
        factors['divergence'] = {
            'score': divergence_score,
            'max': 1,
            'details': div_details
        }
        total_score += divergence_score
        max_possible += 1

        # 8. Поддержка/сопротивление (0-1 балл)
        sr_score, sr_details = self._check_support_resistance(signal_data, historical_data)
        factors['support_resistance'] = {
            'score': sr_score,
            'max': 1,
            'details': sr_details
        }
        total_score += sr_score
        max_possible += 1

        # 9. Время суток (0-1 балл)
        time_score, time_details = self._rate_time_of_day()
        factors['market_hours'] = {
            'score': time_score,
            'max': 1,
            'details': time_details
        }
        total_score += time_score
        max_possible += 1

        # 10. Новостной фон (0-1 балл) - можно интегрировать позже
        news_score, news_details = self._rate_news_background(signal_data['symbol'])
        factors['news'] = {
            'score': news_score,
            'max': 1,
            'details': news_details
        }
        total_score += news_score
        max_possible += 1

        # Итоговый рейтинг
        strength = self._calculate_strength(total_score, max_possible)

        return {
            'strength': strength.value,
            'total_score': total_score,
            'max_score': max_possible,
            'percentage': (total_score / max_possible) * 100 if max_possible > 0 else 0,
            'factors': factors,
            'recommendation': self._get_recommendation(strength),
            'timestamp': datetime.now(timezone.utc)
        }

    @staticmethod
    def _rate_mtf_consensus(signal_data: Dict) -> Tuple[int, Dict]:
        """Оценивает консенсус между таймфреймами"""
        if 'timeframe_analysis' not in signal_data:
            return 0, {'reason': 'No MTF data available'}

        tf_results = signal_data['timeframe_analysis']
        signals = []

        for tf, result in tf_results.items():
            if result['signal'] != 'none':
                signals.append((tf, result['signal'], result['strength']))

        if len(signals) < 2:
            return 0, {'reason': 'Less than 2 timeframes confirm'}

        # Проверяем согласованность
        buy_count = sum(1 for s in signals if s[1] == 'BUY')
        sell_count = sum(1 for s in signals if s[1] == 'SELL')

        if buy_count == len(signals):  # Все ТФ показывают покупку
            return 2, {'reason': f'All {len(signals)} timeframes confirm BUY'}
        elif sell_count == len(signals):  # Все ТФ показывают продажу
            return 2, {'reason': f'All {len(signals)} timeframes confirm SELL'}
        elif abs(buy_count - sell_count) >= 2:  # Явное большинство
            return 1, {'reason': f'Strong majority ({max(buy_count, sell_count)}/{len(signals)})'}
        else:
            return 0, {'reason': 'No clear consensus'}

    @staticmethod
    def _rate_trend_strength(signal_data: Dict) -> Tuple[int, Dict]:
        """Оценивает силу тренда"""
        if 'timeframe_analysis' not in signal_data:
            return 0, {'reason': 'No trend data'}

        tf_results = signal_data['timeframe_analysis']
        trends = []

        for tf, result in tf_results.items():
            if 'ema_trend' in result:
                trends.append(result['ema_trend'])

        # Проверяем согласованность трендов
        if len(trends) < 2:
            return 0, {'reason': 'Insufficient trend data'}

        bullish_count = sum(1 for t in trends if t == 'bullish')
        bearish_count = sum(1 for t in trends if t == 'bearish')

        # Тренд должен быть в направлении сигнала
        signal_type = signal_data.get('signal_type', '')

        if signal_type == 'BUY' and bullish_count >= 2:
            return 2, {'reason': f'Strong bullish trend ({bullish_count}/{len(trends)} timeframes)'}
        elif signal_type == 'SELL' and bearish_count >= 2:
            return 2, {'reason': f'Strong bearish trend ({bearish_count}/{len(trends)} timeframes)'}
        elif (signal_type == 'BUY' and bullish_count == 1) or (signal_type == 'SELL' and bearish_count == 1):
            return 1, {'reason': 'Weak trend alignment'}
        else:
            return 0, {'reason': 'Trend contradicts signal'}

    def _rate_volume(self, signal_data: Dict, historical_data: Dict) -> Tuple[int, Dict]:
        """Оценивает объем"""
        if not historical_data or 'volumes' not in historical_data:
            return 0, {'reason': 'No volume data available'}

        current_volume = signal_data.get('volume', 0)
        volumes = historical_data['volumes']

        if len(volumes) < self.volume_period:
            return 0, {'reason': 'Insufficient historical volume data'}

        # Вычисляем средний объем
        avg_volume = np.mean(volumes[-self.volume_period:])

        if current_volume == 0 or avg_volume == 0:
            return 0, {'reason': 'Invalid volume data'}

        volume_ratio = current_volume / avg_volume

        if volume_ratio > 2.0:
            return 2, {'reason': f'Volume spike: {volume_ratio:.2f}x average'}
        elif volume_ratio > 1.5:
            return 1, {'reason': f'Above average volume: {volume_ratio:.2f}x'}
        elif volume_ratio < 0.5:
            return 0, {'reason': f'Low volume: {volume_ratio:.2f}x average (caution)'}
        else:
            return 0, {'reason': f'Normal volume: {volume_ratio:.2f}x average'}

    @staticmethod
    def _rate_rsi(signal_data: Dict) -> Tuple[int, Dict]:
        """Оценивает уровни RSI"""
        rsi = signal_data.get('rsi')
        if not rsi:
            return 0, {'reason': 'No RSI data'}

        signal_type = signal_data.get('signal_type', '')

        if signal_type == 'BUY':
            if rsi < 30:
                return 1, {'reason': f'RSI oversold: {rsi:.1f}'}
            elif rsi < 35:
                return 0, {'reason': f'RSI near oversold: {rsi:.1f}'}
            else:
                return 0, {'reason': f'RSI not in buy zone: {rsi:.1f}'}
        elif signal_type == 'SELL':
            if rsi > 70:
                return 1, {'reason': f'RSI overbought: {rsi:.1f}'}
            elif rsi > 65:
                return 0, {'reason': f'RSI near overbought: {rsi:.1f}'}
            else:
                return 0, {'reason': f'RSI not in sell zone: {rsi:.1f}'}
        else:
            return 0, {'reason': f'RSI neutral: {rsi:.1f}'}

    @staticmethod
    def _rate_risk_reward(signal_data: Dict) -> Tuple[int, Dict]:
        """Оценивает соотношение риск/прибыль"""
        try:
            entry = float(signal_data.get('entry_price', 0))
            sl = float(signal_data.get('stop_loss', 0))
            tp1 = float(signal_data.get('take_profit_1', 0))

            if entry == 0 or sl == 0 or tp1 == 0:
                return 0, {'reason': 'Invalid price levels'}

            risk = abs(entry - sl)
            reward = abs(tp1 - entry)

            if risk == 0:
                return 0, {'reason': 'Zero risk'}

            rr_ratio = reward / risk

            if rr_ratio >= 2.0:
                return 1, {'reason': f'Excellent R/R: 1:{rr_ratio:.2f}'}
            elif rr_ratio >= 1.5:
                return 0, {'reason': f'Good R/R: 1:{rr_ratio:.2f}'}
            else:
                return 0, {'reason': f'Poor R/R: 1:{rr_ratio:.2f}'}

        except Exception as e:
            return 0, {'reason': f'Error calculating R/R: {str(e)}'}

    @staticmethod
    def _rate_volatility(signal_data: Dict) -> Tuple[int, Dict]:
        """Оценивает волатильность на основе ATR"""
        atr = signal_data.get('atr')
        current_price = signal_data.get('entry_price', 0)

        if not atr or current_price == 0:
            return 0, {'reason': 'No volatility data'}

        atr_percent = (atr / current_price) * 100

        # Идеальная волатильность для торговли: 1-3%
        if 1.0 <= atr_percent <= 3.0:
            return 1, {'reason': f'Ideal volatility: {atr_percent:.2f}%'}
        elif atr_percent < 0.5:
            return 0, {'reason': f'Low volatility: {atr_percent:.2f}% (may be false breakout)'}
        elif atr_percent > 5.0:
            return 0, {'reason': f'High volatility: {atr_percent:.2f}% (increased risk)'}
        else:
            return 0, {'reason': f'Normal volatility: {atr_percent:.2f}%'}

    @staticmethod
    def _check_divergence(_historical_data: Dict) -> Tuple[int, Dict]:
        """Проверяет наличие дивергенции"""
        # Заглушка - нужно реализовать логику обнаружения дивергенции
        return 0, {'reason': 'Divergence check not implemented yet'}

    @staticmethod
    def _check_support_resistance(_signal_data: Dict, _historical_data: Dict) -> Tuple[int, Dict]:
        """Проверяет уровни поддержки/сопротивления"""
        # Заглушка - нужно реализовать определение уровней S/R
        return 0, {'reason': 'S/R analysis not implemented yet'}

    @staticmethod
    def _rate_time_of_day() -> Tuple[int, Dict]:
        """Оценивает время суток (торговые сессии)"""
        hour = datetime.now(timezone.utc).hour

        # Лондонская сессия: 8:00-16:00 UTC
        # Нью-Йоркская сессия: 13:00-21:00 UTC
        # Азиатская сессия: 00:00-08:00 UTC

        if 8 <= hour < 16:  # Лондон
            return 1, {'reason': f'London session ({hour}:00 UTC)'}
        elif 13 <= hour < 21:  # Нью-Йорк
            return 1, {'reason': f'New York session ({hour}:00 UTC)'}
        elif 0 <= hour < 8:  # Азия
            return 0, {'reason': f'Asian session ({hour}:00 UTC) - lower liquidity'}
        else:
            return 0, {'reason': f'Between sessions ({hour}:00 UTC)'}

    @staticmethod
    def _rate_news_background(_symbol: str) -> Tuple[int, Dict]:
        """Оценивает новостной фон"""
        # Заглушка - можно интегрировать с CryptoPanic или другим API
        return 0, {'reason': 'News analysis not implemented yet'}

    @staticmethod
    def _calculate_strength(score: int, max_score: int) -> SignalStrength:
        """Определяет силу сигнала на основе баллов"""
        percentage = (score / max_score) * 100 if max_score > 0 else 0

        if percentage >= 90:
            return SignalStrength.STRONG
        elif percentage >= 75:
            return SignalStrength.HIGH
        elif percentage >= 60:
            return SignalStrength.MEDIUM
        elif percentage >= 40:
            return SignalStrength.LOW
        else:
            return SignalStrength.WEAK

    @staticmethod
    def _get_recommendation(strength: SignalStrength) -> str:
        """Возвращает рекомендацию на основе силы сигнала"""
        recommendations = {
            SignalStrength.STRONG: "STRONG BUY/SELL - High confidence",
            SignalStrength.HIGH: "BUY/SELL - Good setup",
            SignalStrength.MEDIUM: "Consider BUY/SELL - Moderate confidence",
            SignalStrength.LOW: "Watch for confirmation - Low confidence",
            SignalStrength.WEAK: "Avoid - Weak setup"
        }
        return recommendations.get(strength, "No recommendation")

    @staticmethod
    def _create_empty_rating() -> Dict:
        """Создает пустой рейтинг"""
        return {
            'strength': SignalStrength.WEAK.value,
            'total_score': 0,
            'max_score': 12,
            'percentage': 0,
            'factors': {},
            'recommendation': 'No signal data',
            'timestamp': datetime.now(timezone.utc)
        }

SignalQualityRater = EnhancedSignalQualityRater
