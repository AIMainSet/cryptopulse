from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder


# --- ГЛАВНОЕ РЕПЛАЙ-МЕНЮ (Нижние кнопки) ---
def get_main_menu(status: str = "FREE") -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()

    # Первый ряд: Основные функции
    builder.row(KeyboardButton(text="📈 Алерты"), KeyboardButton(text="📊 Сигналы"))

    # Второй ряд: Зависит от статуса
    if status == "PREMIUM":
        builder.row(KeyboardButton(text="⚙️ Настройки"), KeyboardButton(text="💎 Подписка"))
    else:
        builder.row(KeyboardButton(text="💎 Подписка"), KeyboardButton(text="⚙️ Настройки"))

    # Третий ряд: Инфо
    builder.row(KeyboardButton(text="📊 Статистика"), KeyboardButton(text="ℹ️ Информация"))

    return builder.as_markup(resize_keyboard=True, input_field_placeholder="Выберите раздел...")


# --- ИНЛАЙН-МЕНЮ НАСТРОЕК ---
def get_settings_inline_menu(has_pairs: bool = False) -> InlineKeyboardMarkup:
    status_icon = "🟢" if has_pairs else "🔴"
    status_text = "ВКЛ" if has_pairs else "ВЫКЛ"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎯 Выбор торговых пар", callback_data="settings_pairs")],
        [InlineKeyboardButton(text=f"🔔 Уведомления: {status_icon} {status_text}",
                              callback_data="toggle_notifications")],
        [InlineKeyboardButton(text="💰 Риск-менеджмент", callback_data="settings_risk")],
        [InlineKeyboardButton(text="⬅️ Назад в главное меню", callback_data="back_to_main_menu")]
    ])
    return keyboard


# --- МЕНЮ ВЫБОРА ПАР ---
def get_pairs_menu(pairs_list: list, user_pairs_str: str) -> InlineKeyboardMarkup:
    # Преобразуем строку из базы в список для сравнения
    selected_list = [p.strip() for p in user_pairs_str.split(",")] if user_pairs_str else []

    builder = InlineKeyboardBuilder()
    for pair in pairs_list:
        icon = "✅" if pair in selected_list else "⬜"
        builder.button(text=f"{icon} {pair}", callback_data=f"toggle_pair:{pair}")

    builder.adjust(2)  # По 2 пары в ряд
    builder.row(InlineKeyboardButton(text="💾 Сохранить и вернуться", callback_data="back_to_settings"))
    return builder.as_markup()


# --- МЕНЮ ОПЛАТЫ ---
def get_payment_keyboard(url: str, invoice_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="💳 Оплатить 15.00 USDT", url=url))
    builder.row(InlineKeyboardButton(text="🔄 Проверить транзакцию", callback_data=f"check_pay:{invoice_id}"))
    builder.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main_menu"))
    return builder.as_markup()


# --- КНОПКА НАЗАД (Универсальная) ---
def get_back_inline(to_main: bool = False) -> InlineKeyboardMarkup:
    target = "back_to_main_menu" if to_main else "back_to_settings"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=target)]
    ])