from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from config import config

admin_router = Router()


# Простой фильтр для проверки, является ли пользователь админом
def is_admin(message: Message):
    return message.from_user.id in config.ADMIN_IDS


@admin_router.message(Command("admin"), F.from_user.id.in_(config.ADMIN_IDS))
async def admin_main_menu(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="⚙️ Настройки сигналов", callback_data="admin_settings")]
    ])
    await message.answer("👑 Панель администратора:", reply_markup=kb)


@admin_router.callback_query(F.data == "admin_stats")
async def show_stats(callback: CallbackQuery):
    # Здесь мы будем запрашивать данные из нашей новой database.py
    total_users = 100  # Заглушка: await db.get_users_count()
    active_signals = 5  # Заглушка

    await callback.message.edit_text(
        f"📈 **Статистика бота:**\n\n"
        f"Пользователей: {total_users}\n"
        f"Активных сигналов: {active_signals}",
        reply_markup=callback.message.reply_markup
    )
    await callback.answer()