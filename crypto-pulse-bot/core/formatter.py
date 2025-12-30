import re
from typing import Dict


class EnhancedSignalFormatter:

    @staticmethod
    def escape_md(text) -> str:
        """Экранирование для MarkdownV2"""
        if text is None:
            return ""
        text = str(text)
        # Экранируем спецсимволы, кроме тех, что используем для разметки
        escape_chars = r'_*[]()~`>#+-=|{}.!'
        return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

    @staticmethod
    def format_signal_with_rating(signal_data: Dict) -> str:
        """Форматирует сигнал с расширенной информацией о качестве"""
        quality = signal_data.get('quality_report', {})

        # Эмодзи для силы сигнала
        strength_emojis = {
            'STRONG': '🔥🔥🔥',
            'HIGH': '🔥🔥',
            'MEDIUM': '🔥',
            'LOW': '⚠️',
            'WEAK': '🚫'
        }

        strength = quality.get('strength', 'WEAK')
        emoji = strength_emojis.get(strength, '')

        message = f"""
    {emoji} *{strength} SIGNAL* {emoji}

    *Пара:* `{signal_data['symbol']}`
    *Направление:* {'🟢 LONG' if signal_data['signal_type'] == 'BUY' else '🔴 SHORT'}
    *Уверенность:* {signal_data.get('confidence', 0) * 100:.1f}%

    *Цена входа:* `{signal_data['entry_price']:.8f}`
    *Stop Loss:* `{signal_data['stop_loss']:.8f}` ({abs(signal_data['entry_price'] - signal_data['stop_loss']) / signal_data['entry_price'] * 100:.2f}%)
    *Take Profit 1:* `{signal_data['take_profit_1']:.8f}`
    *Take Profit 2:* `{signal_data['take_profit_2']:.8f}`

    *Качество сигнала:* {quality.get('percentage', 0):.1f}%
    *Рекомендация:* {quality.get('recommendation', 'N/A')}

    *Multi-Timeframe анализ:*
    """

        # Добавляем информацию по каждому ТФ
        if 'timeframe_analysis' in signal_data:
            for tf, analysis in signal_data['timeframe_analysis'].items():
                if analysis['signal'] != 'none':
                    message += f"  • {tf}: {analysis['signal']} (сила: {analysis['strength']:.2f})\n"

        message += f"\n*Ключевые факторы:*\n"

        # Добавляем топ-3 фактора качества
        if 'factors' in quality:
            factors = quality['factors']
            sorted_factors = sorted(
                factors.items(),
                key=lambda x: x[1].get('score', 0),
                reverse=True
            )[:3]

            for name, data in sorted_factors:
                score = data.get('score', 0)
                max_score = data.get('max', 1)
                details = data.get('details', {}).get('reason', '')
                message += f"  • {name}: {score}/{max_score} - {details}\n"

        message += f"\n_Сгенерировано: {signal_data['timestamp'].strftime('%Y-%m-%d %H:%M:%S UTC')}_"

        return message

    @staticmethod
    def calculate_percentage(entry: float, target: float) -> float:
        """Расчет процента изменения относительно точки входа."""
        if not entry or entry == 0:
            return 0.0
        return ((target - entry) / entry) * 100
