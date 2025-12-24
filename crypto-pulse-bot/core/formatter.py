import re
from typing import Dict, Optional


class EnhancedSignalFormatter:

    @staticmethod
    def escape_md(text) -> str:
        """Экранирование для MarkdownV2"""
        if text is None: return ""
        text = str(text)
        # Экранируем спецсимволы, кроме тех, что используем для разметки
        escape_chars = r'_*[]()~`>#+-=|{}.!'
        return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

    @staticmethod
    def format_signal_with_rating(signal: Dict, rating: Optional[Dict] = None) -> str:
        # 1. Данные и Направление
        is_long = str(signal.get('direction', 'LONG')).upper() == 'LONG'
        side_color = "🟢" if is_long else "🔴"
        direction_text = r"LONG \(ПОКУПКА\)" if is_long else r"SHORT \(ПРОДАЖА\)"
        arrow = "📈" if is_long else "📉"

        # 2. Рейтинг и Прогресс-бар
        rating_emoji = rating.get('emoji', '⭐') if rating else "⭐"
        status = EnhancedSignalFormatter.escape_md(rating.get('status', 'ULTRA').upper())
        conf_val = (rating.get('confidence', 0.5) * 100) if rating else 50.0
        filled = int(conf_val // 10)
        bar = "▰" * filled + "▱" * (10 - filled)
        conf_bar = f"{bar} {conf_val:.1f}%"

        # 3. Чистка чисел (Округление)
        symbol = EnhancedSignalFormatter.escape_md(signal.get('symbol', '').upper())
        entry = f"{float(signal.get('entry', 0)):.2f}"
        sl = f"{float(signal.get('sl', 0)):.2f}"

        # 4. Формирование Тейков (с округлением)
        tp_lines = []
        for i in range(1, 4):
            tp_val = signal.get(f'tp{i}')
            if tp_val:
                entry_val = float(signal.get('entry', 1))
                percent = ((float(tp_val) - entry_val) / entry_val) * 100

                val_esc = EnhancedSignalFormatter.escape_md(f"{float(tp_val):.2f}")
                perc_esc = EnhancedSignalFormatter.escape_md(f"{percent:+.2f}%")

                icon = "🎯" if i < 3 else "🏁"
                prefix = "┣" if i < 3 else "┗"
                tp_lines.append(f"{prefix} {icon} Цель {i}: `{val_esc}` — *{perc_esc}*")

        tp_text = "\n".join(tp_lines)
        divider = EnhancedSignalFormatter.escape_md("────────────────────")

        # 5. Сборка сообщения (Используем чистые перенос строк \n)
        message = (
            fr"{side_color} *{rating_emoji} {status} SIGNAL: {symbol}* {side_color}\n"
            f"{divider}\n"
            fr"💰 *Пара:* `{symbol}`\n"
            fr"🎯 *Тип:* {direction_text} {arrow}\n"
            fr"📥 *ВХОД:* `{EnhancedSignalFormatter.escape_md(entry)}`\n"
            fr"📊 *Уверенность:* `{EnhancedSignalFormatter.escape_md(conf_bar)}`\n"
            f"{divider}\n"
            fr"🎯 *ЦЕЛИ ТЕЙК\-ПРОФИТА:*\n"
            f"{tp_text}\n\n"
            fr"🛡 *STOP LOSS:* `{EnhancedSignalFormatter.escape_md(sl)}`\n"
            f"{divider}\n"
            fr"⚖️ *РИСК\-МЕНЕДЖМЕНТ:*\n"
            fr"• Риск: {EnhancedSignalFormatter.escape_md(signal.get('risk', 'Medium'))}\n"
            fr"• Плечо: {EnhancedSignalFormatter.escape_md(signal.get('leverage', '10x'))}\n"
            fr"• Позиция: 2\-5% от депозита\n\n"
            fr"📈 *ОБОСНОВАНИЕ:*\n"
            fr"_{EnhancedSignalFormatter.escape_md(signal.get('reason', 'Технический анализ'))}_\n\n"
            fr"🕒 _{EnhancedSignalFormatter.escape_md(signal.get('created_at', 'N/A'))}_"
        )
        return message

    @staticmethod
    def calculate_percentage(entry: float, target: float) -> float:
        """Расчет процента изменения относительно точки входа."""
        if not entry or entry == 0:
            return 0.0
        return ((target - entry) / entry) * 100

    @staticmethod
    def escape(self, text):
        if text is None: return ""
        special_chars = r"\_*[]()~`>#+-=|{}.!"
        res = str(text)
        for char in special_chars:
            res = res.replace(char, f"\\{char}")
        return res