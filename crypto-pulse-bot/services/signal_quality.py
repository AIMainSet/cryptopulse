"""
Система оценки качества сигналов
"""
import logging
from typing import Dict
from datetime import datetime

logger = logging.getLogger(__name__)


class SignalQualityRater:
    def __init__(self):
        self.rating_factors = {
            'timeframe_consensus': 0.25,  # Согласованность таймфреймов
            'volume_confirmation': 0.20,  # Подтверждение объемами
            'risk_reward_ratio': 0.15,  # Соотношение риск/прибыль
            'market_structure': 0.15,  # Рыночная структура
            'volatility_score': 0.10,  # Волатильность
            'confidence_score': 0.15  # Общая уверенность
        }

        self.rating_thresholds = {
            'HIGH': 0.75,
            'MEDIUM': 0.60,
            'LOW': 0.45,
            'WEAK': 0.0
        }

    async def rate_signal(self, signal: Dict, market_data: Dict = None) -> Dict:
        """Оценка качества сигнала"""
        try:
            ratings = {}

            # 1. Оценка согласованности таймфреймов
            ratings['timeframe_consensus'] = await self.rate_timeframe_consensus(
                signal.get('timeframes_analyzed', []),
                signal.get('direction')
            )

            # 2. Оценка подтверждения объемами
            ratings['volume_confirmation'] = await self.rate_volume_confirmation(
                signal.get('symbol'),
                signal.get('direction')
            )

            # 3. Оценка соотношения риск/прибыль
            ratings['risk_reward_ratio'] = self.rate_risk_reward(
                signal.get('risk_reward', 1)
            )

            # 4. Оценка рыночной структуры
            ratings['market_structure'] = await self.rate_market_structure(
                signal.get('symbol'),
                signal.get('direction')
            )

            # 5. Оценка волатильности
            ratings['volatility_score'] = self.rate_volatility(
                signal.get('volatility', '0%')
            )

            # 6. Оценка уверенности
            ratings['confidence_score'] = signal.get('confidence', 0.5)

            # Итоговый рейтинг
            total_rating = sum(
                rating * self.rating_factors[factor]
                for factor, rating in ratings.items()
            )

            # Определяем уровень сигнала
            signal_level = self.determine_signal_level(total_rating)

            return {
                'total_rating': total_rating,
                'signal_level': signal_level,
                'emoji': self.get_level_emoji(signal_level),
                'ratings': ratings,
                'recommendation': self.get_recommendation(signal_level, signal),
                'is_premium': signal_level in ['HIGH', 'MEDIUM'],
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

        except Exception as e:
            logger.error(f"Ошибка оценки сигнала: {e}")
            return {
                'total_rating': 0.5,
                'signal_level': 'STANDARD',
                'emoji': '⭐',
                'error': str(e)
            }

    async def rate_timeframe_consensus(self, timeframes: list, direction: str) -> float:
        """Оценка согласованности таймфреймов"""
        if not timeframes:
            return 0.5

        # Чем больше таймфреймов проанализировано, тем лучше
        base_score = min(len(timeframes) / 3, 1.0)

        # Бонус за наличие 4h таймфрейма (более надежный)
        if '4h' in timeframes:
            base_score += 0.2

        return min(base_score, 1.0)

    async def rate_volume_confirmation(self, symbol: str, direction: str) -> float:
        """Оценка подтверждения объемами"""
        # Здесь должна быть логика проверки объемов
        # Пока возвращаем базовый score
        return 0.7

    def rate_risk_reward(self, risk_reward: float) -> float:
        """Оценка соотношения риск/прибыль"""
        if risk_reward >= 3:
            return 1.0
        elif risk_reward >= 2:
            return 0.8
        elif risk_reward >= 1.5:
            return 0.6
        elif risk_reward >= 1:
            return 0.4
        else:
            return 0.2

    async def rate_market_structure(self, symbol: str, direction: str) -> float:
        """Оценка рыночной структуры"""
        # Проверка тренда, уровней поддержки/сопротивления и т.д.
        # Пока возвращаем базовый score
        return 0.6

    def rate_volatility(self, volatility_str: str) -> float:
        """Оценка волатильности"""
        try:
            volatility = float(volatility_str.strip('%')) / 100

            # Оптимальная волатильность для торговли: 2-5%
            if 0.02 <= volatility <= 0.05:
                return 0.9
            elif 0.01 <= volatility < 0.02 or 0.05 < volatility <= 0.08:
                return 0.7
            elif volatility < 0.01:  # Слишком низкая волатильность
                return 0.4
            else:  # Слишком высокая волатильность
                return 0.3
        except:
            return 0.5

    def determine_signal_level(self, rating: float) -> str:
        """Определение уровня сигнала по рейтингу"""
        if rating >= self.rating_thresholds['HIGH']:
            return 'HIGH'
        elif rating >= self.rating_thresholds['MEDIUM']:
            return 'MEDIUM'
        elif rating >= self.rating_thresholds['LOW']:
            return 'LOW'
        else:
            return 'WEAK'

    def get_level_emoji(self, level: str) -> str:
        """Получение emoji для уровня сигнала"""
        emojis = {
            'HIGH': '🔥',
            'MEDIUM': '✅',
            'LOW': '⚠️',
            'WEAK': '❌',
            'STANDARD': '⭐'
        }
        return emojis.get(level, '⭐')

    def get_recommendation(self, level: str, signal: Dict) -> str:
        """Получение рекомендации по сигналу"""
        recommendations = {
            'HIGH': f"Сильный сигнал! Рекомендуется открывать позицию по {signal['symbol']}",
            'MEDIUM': f"Хороший сигнал. Можно рассматривать сделку по {signal['symbol']}",
            'LOW': f"Сигнал требует осторожности. Уменьшите размер позиции по {signal['symbol']}",
            'WEAK': f"Слабый сигнал. Рекомендуется пропустить сделку по {signal['symbol']}",
            'STANDARD': f"Стандартный сигнал по {signal['symbol']}"
        }
        return recommendations.get(level, "Сигнал требует дополнительного анализа")

    def generate_quality_report(self, signal: Dict, rating: Dict) -> str:
        """Генерация отчета о качестве сигнала"""
        report = f"""
📊 <b>ОТЧЕТ О КАЧЕСТВЕ СИГНАЛА</b>

<b>Основные метрики:</b>
• Общий рейтинг: {rating['total_rating']:.2%}
• Уровень сигнала: {rating['signal_level']} {rating['emoji']}
• Премиум-сигнал: {'✅ Да' if rating['is_premium'] else '❌ Нет'}

<b>Детальная оценка:</b>
• Согласованность таймфреймов: {rating['ratings']['timeframe_consensus']:.2%}
• Подтверждение объемами: {rating['ratings']['volume_confirmation']:.2%}
• Риск/прибыль: {rating['ratings']['risk_reward_ratio']:.2%}
• Рыночная структура: {rating['ratings']['market_structure']:.2%}
• Волатильность: {rating['ratings']['volatility_score']:.2%}
• Уверенность: {rating['ratings']['confidence_score']:.2%}

<b>Рекомендация:</b>
{rating['recommendation']}

<i>Отчет сгенерирован: {rating['timestamp']}</i>
"""
        return report