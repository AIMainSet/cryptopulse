import logging
from datetime import datetime
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramBadRequest
from sqlalchemy import select, func, update

import keyboards as kb
from database import (
    async_session,
    get_or_create_user,
    update_user_pairs,
    set_user_premium,
    SignalHistory,
    User
)
from core.market_worker import MarketWorker
from payments import create_invoice, check_invoice_status

router = Router()

# Популярные пары по умолчанию
DEFAULT_PAIRS = "BTC/USDT,ETH/USDT,SOL/USDT"
AVAILABLE_PAIRS = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "ADA/USDT", "XRP/USDT", "DOT/USDT"]


class SettingsStates(StatesGroup):
    waiting_for_deposit = State()
    waiting_for_risk = State()


# --- ВСПОМОГАТЕЛЬНАЯ ЛОГИКА ОФОРМЛЕНИЯ ---

async def get_profile_text(user: User, first_name: str) -> str:
    """Генерирует красивый текст главного меню"""
    status_emoji = "💎" if user.status == "PREMIUM" else "👤"
    status_line = f"{status_emoji} **Тариф:** `{user.status}`"

    if user.status == "PREMIUM" and user.subscribed_until:
        end_date = user.subscribed_until.strftime("%d.%m.%Y")
        status_line += f" (до {end_date})"

    # Красивое отображение пар
    if user.selected_pairs:
        pairs_display = "\n".join([f"  ▫️ {p.strip()}" for p in user.selected_pairs.split(",")])
    else:
        pairs_display = "  ▫️ _не выбраны (уведомления выкл)_"

    return (
        f"🏠 **ЛИЧНЫЙ КАБИНЕТ**\n"
        f"────────────────────\n"
        f"👤 **Трейдер:** {first_name}\n"
        f"{status_line}\n\n"
        f"🎯 **Ваши пары в работе:**\n"
        f"{pairs_display}\n"
        f"────────────────────\n"
        f"🚀 _Бот сканирует рынок 24/7_"
    )


# --- ОСНОВНЫЕ КОМАНДЫ ---

@router.message(StateFilter(None), CommandStart())
@router.message(StateFilter(None), Command("menu"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user = await get_or_create_user(message.from_user.id, message.from_user.username)

    # Если у нового пользователя пусто в парах - ставим дефолт (но уведомления не шлем пока не подтвердит)
    if not user.selected_pairs:
        await update_user_pairs(user.user_id, DEFAULT_PAIRS)
        user.selected_pairs = DEFAULT_PAIRS

    text = await get_profile_text(user, message.from_user.first_name)
    await message.answer(text, reply_markup=kb.get_main_menu(user.status), parse_mode="Markdown")


@router.message(StateFilter(None), F.text == "⚙️ Настройки")
@router.message(StateFilter(None), Command("settings"))
async def show_settings(message: Message):
    user = await get_or_create_user(message.from_user.id)
    text = (
        "⚙️ **НАСТРОЙКИ ПРОФИЛЯ**\n"
        f"────────────────────\n"
        "Здесь вы можете выбрать монеты для алертов и настроить параметры риск-менеджмента для расчёта объёма сделки."
    )
    await message.answer(
        text,
        reply_markup=kb.get_settings_inline_menu(has_pairs=bool(user.selected_pairs)),
        parse_mode="Markdown"
    )


# --- CALLBACK ОБРАБОТЧИКИ (НАВИГАЦИЯ) ---

@router.callback_query(F.data == "back_to_main_menu")
async def back_to_main_menu(callback: CallbackQuery):
    user = await get_or_create_user(callback.from_user.id)
    text = await get_profile_text(user, callback.from_user.first_name)
    try:
        # Редактируем старое сообщение, превращая его в главное меню
        await callback.message.edit_text(text, reply_markup=None, parse_mode="Markdown")
    except TelegramBadRequest:
        pass
    await callback.answer()


@router.callback_query(F.data == "back_to_settings")
async def back_to_settings(callback: CallbackQuery):
    user = await get_or_create_user(callback.from_user.id)
    text = "⚙️ **НАСТРОЙКИ ПРОФИЛЯ**\n────────────────────"
    try:
        await callback.message.edit_text(
            text,
            reply_markup=kb.get_settings_inline_menu(has_pairs=bool(user.selected_pairs)),
            parse_mode="Markdown"
        )
    except TelegramBadRequest:
        pass
    await callback.answer()


# --- УПРАВЛЕНИЕ ПАРАМИ ---

@router.callback_query(F.data == "settings_pairs")
async def settings_pairs_menu(callback: CallbackQuery):
    user = await get_or_create_user(callback.from_user.id)
    await callback.message.edit_text(
        "🎯 **ВЫБОР ТОРГОВЫХ ПАР**\n\nОтметьте пары, по которым хотите получать уведомления в личку:",
        reply_markup=kb.get_pairs_menu(AVAILABLE_PAIRS, user.selected_pairs or ""),
        parse_mode="Markdown"
    )


@router.callback_query(F.data.startswith("toggle_pair:"))
async def toggle_pair(callback: CallbackQuery):
    pair = callback.data.split(":")[1]
    user = await get_or_create_user(callback.from_user.id)

    current_pairs = [p.strip() for p in user.selected_pairs.split(",")] if user.selected_pairs else []

    if pair in current_pairs:
        current_pairs.remove(pair)
    else:
        current_pairs.append(pair)

    new_pairs_str = ",".join(current_pairs)
    await update_user_pairs(user.user_id, new_pairs_str)

    # Обновляем те же кнопки
    try:
        await callback.message.edit_reply_markup(
            reply_markup=kb.get_pairs_menu(AVAILABLE_PAIRS, new_pairs_str)
        )
    except TelegramBadRequest:
        pass
    await callback.answer()


@router.callback_query(F.data == "toggle_notifications")
async def toggle_notifications(callback: CallbackQuery):
    user = await get_or_create_user(callback.from_user.id)

    if user.selected_pairs:
        # Если были включены - выключаем (очищаем)
        await update_user_pairs(user.user_id, "")
        await callback.answer("🔕 Уведомления полностью выключены", show_alert=True)
    else:
        # Если были выключены - ставим дефолт
        await update_user_pairs(user.user_id, DEFAULT_PAIRS)
        await callback.answer("🔔 Уведомления включены (BTC, ETH, SOL)", show_alert=True)

    # Обновляем меню настроек
    user = await get_or_create_user(callback.from_user.id)
    try:
        await callback.message.edit_reply_markup(
            reply_markup=kb.get_settings_inline_menu(has_pairs=bool(user.selected_pairs))
        )
    except TelegramBadRequest:
        pass


# --- ОСТАЛЬНЫЕ РАЗДЕЛЫ ---

@router.message(F.text == "📈 Алерты")
async def alerts_info(message: Message):
    user = await get_or_create_user(message.from_user.id)
    if user.status != "PREMIUM":
        return await message.answer(
            "❌ **Доступ ограничен**\n\nФункция алертов доступна только для PREMIUM пользователей.")

    pairs = user.selected_pairs.replace(",", ", ") if user.selected_pairs else "❌ Выключены"
    text = (
        "📈 **МОНИТОРИНГ СИГНАЛОВ**\n"
        f"────────────────────\n"
        f"📡 **Статус:** `Активен`\n"
        f"🎯 **Пары:** `{pairs}`\n\n"
        "Бот пришлет уведомление сразу, как только индикаторы RSI и EMA дадут сигнал на вход."
    )
    await message.answer(text, reply_markup=kb.get_back_inline(to_main=True), parse_mode="Markdown")


@router.message(F.text == "📊 Статистика")
async def show_stats(message: Message):
    async with async_session() as session:
        # Пример заглушки, пока база наполняется
        total = 42;
        tps = 32;
        profit = 124.5
        winrate = (tps / total * 100) if total > 0 else 0

        text = (
            f"📊 **ОБЩАЯ СТАТИСТИКА**\n"
            f"────────────────────\n"
            f"✅ Закрыто в плюс: `{tps}`\n"
            f"❌ Закрыто в минус: `{total - tps}`\n"
            f"📈 Winrate: `{round(winrate, 1)}%`\n"
            f"💰 Профит за месяц: `+{profit}%`"
        )
        await message.answer(text, reply_markup=kb.get_back_inline(to_main=True), parse_mode="Markdown")


@router.message(F.text == "ℹ️ Информация")
async def show_help(message: Message):
    text = (
        "ℹ️ **ИНФОРМАЦИЯ О БОТЕ**\n"
        f"────────────────────\n"
        "**Crypto Pulse Bot** — это аналитический инструмент для поиска торговых сигналов.\n\n"
        "🔹 **Стратегия:** RSI (14) + EMA (50/200).\n"
        "🔹 **Таймфрейм:** H1 (1 час).\n"
        "🔹 **Обновление:** каждые 15 минут.\n\n"
        "👨‍💻 Техподдержка: @Woolfin"
    )
    await message.answer(text, reply_markup=kb.get_back_inline(to_main=True), parse_mode="Markdown")


# --- РИСК-МЕНЕДЖМЕНТ (Расчет позиции) ---

@router.callback_query(F.data == "settings_risk")
async def settings_risk_start(callback: CallbackQuery, state: FSMContext):
    # Используем edit_text для плавного перехода
    await callback.message.edit_text(
        "💰 **РИСК-МЕНЕДЖМЕНТ**\n\n"
        "Введите ваш текущий баланс в USDT (например: `1000`):\n\n"
        "💡 _Это нужно для автоматического расчета объема сделки в сигналах._",
        reply_markup=kb.get_back_inline(),
        parse_mode="Markdown"
    )
    await state.set_state(SettingsStates.waiting_for_deposit)
    await callback.answer()


@router.message(SettingsStates.waiting_for_deposit)
async def process_deposit(message: Message, state: FSMContext):
    # Проверка на ввод числа
    val = message.text.replace(",", ".")
    if not val.replace(".", "", 1).isdigit():
        return await message.answer("❌ **Ошибка:** Введите числовое значение (например: 500)")

    await state.update_data(deposit=float(val))
    await message.answer(
        "✅ **Депозит сохранен.**\n\nТеперь введите риск на одну сделку в % (рекомендуется `1-2`%):",
        reply_markup=kb.get_back_inline()
    )
    await state.set_state(SettingsStates.waiting_for_risk)


@router.message(SettingsStates.waiting_for_risk)
async def process_risk(message: Message, state: FSMContext):
    val = message.text.replace(",", ".")
    if not val.replace(".", "", 1).isdigit():
        return await message.answer("❌ **Ошибка:** Введите число (например: 1.5)")

    risk_val = float(val)
    data = await state.get_data()

    async with async_session() as session:
        await session.execute(
            update(User).where(User.user_id == message.from_user.id)
            .values(deposit=data['deposit'], risk_per_trade=risk_val)
        )
        await session.commit()

    await state.clear()
    await message.answer(
        f"✅ **Настройки сохранены!**\n\n"
        f"💰 Депозит: `${data['deposit']}`\n"
        f"⚠️ Риск: `{risk_val}%` на сделку.",
        reply_markup=kb.get_main_menu("PREMIUM")  # Обновляем меню
    )


# --- АКТИВНЫЕ СИГНАЛЫ ---

@router.message(F.text == "📊 Сигналы")
@router.message(Command("signals"))
async def show_active_signals(message: Message, market_worker: MarketWorker):
    # Берем сигналы из трекера в реальном времени
    active_sigs = market_worker.tracker.active_signals

    if not active_sigs:
        return await message.answer(
            "🔎 **АКТИВНЫЕ СИГНАЛЫ**\n"
            "────────────────────\n"
            "В данный момент открытых позиций нет. Бот ожидает подтверждения условий стратегии.",
            parse_mode="Markdown"
        )

    text = "🚀 **АКТИВНЫЕ СИГНАЛЫ**\n────────────────────\n"
    for sig in active_sigs:
        side_emoji = "🟢 LONG" if sig['side'].upper() == "BUY" else "🔴 SHORT"
        text += (
            f"🔹 **{sig['symbol']}** | {side_emoji}\n"
            f"📥 Вход: `{sig['entry']}`\n"
            f"🎯 Цель: `{sig['tp']}`\n"
            f"🛡 Стоп: `{sig['sl']}`\n"
            f"───────────────\n"
        )

    await message.answer(text, parse_mode="Markdown")


# --- ОПЛАТА И ПОДПИСКА ---

@router.message(F.text == "💎 Подписка")
async def process_subscription(message: Message):
    user = await get_or_create_user(message.from_user.id)

    if user.status == "PREMIUM":
        return await message.answer(
            "🌟 **У вас уже есть PREMIUM!**\n\nБлагодарим за поддержку. Все функции бота разблокированы."
        )

    try:
        # Сумма 15 USDT. Можно менять.
        url, inv_id = await create_invoice(amount=15.0, user_id=message.from_user.id)

        text = (
            "💎 **ПРЕИМУЩЕСТВА PREMIUM**\n"
            "────────────────────\n"
            "• Безлимитные алерты по всем парам\n"
            "• Расширенная аналитика RSI/EMA\n"
            "• Доступ в закрытый чат трейдеров\n\n"
            "💳 **Стоимость:** `15.00 USDT`"
        )
        await message.answer(text, reply_markup=kb.get_payment_keyboard(url, inv_id), parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Invoice error: {e}")
        await message.answer("⚠️ Сервис оплаты временно недоступен. Попробуйте позже.")


@router.callback_query(F.data.startswith("check_pay:"))
async def verify_payment(callback: CallbackQuery):
    invoice_id = int(callback.data.split(":")[1])

    # Проверка статуса через API CryptoBot
    status = await check_invoice_status(invoice_id)

    if status:
        await set_user_premium(callback.from_user.id)
        await callback.message.edit_text(
            "✅ **ОПЛАТА ПОДТВЕРЖДЕНА!**\n\n"
            "Добро пожаловать в PREMIUM. Вам открыт доступ ко всем функциям и уведомлениям.",
            parse_mode="Markdown"
        )
    else:
        await callback.answer("⏳ Транзакция еще не подтверждена. Попробуйте через минуту.", show_alert=True)