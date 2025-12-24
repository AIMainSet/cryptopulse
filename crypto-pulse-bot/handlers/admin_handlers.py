from aiogram import Router, F, Bot
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from database import get_all_users, get_total_users_count, set_user_premium, set_user_ban
from config import config
import logging
import asyncio

router = Router()


# Состояния для админки
class AdminStates(StatesGroup):
    waiting_for_broadcast = State()


# --- ПАНЕЛЬ УПРАВЛЕНИЯ ---

@router.message(Command("admin"), F.from_user.id.in_(config.ADMIN_IDS))
async def admin_panel(message: Message):
    count = await get_total_users_count()
    await message.answer(
        f"👑 **Админ-панель**\n\n"
        f"👥 Всего пользователей в базе: {count}\n\n"
        f"**Доступные команды:**\n"
        f"📢 /broadcast — отправить сообщение всем\n"
        f"💎 `/give_premium ID` — выдать подписку"
    )


# --- РАССЫЛКА ---

@router.message(Command("broadcast"), F.from_user.id.in_(config.ADMIN_IDS))
async def broadcast_start(message: Message, state: FSMContext):
    await message.answer("📝 Введите текст для рассылки всем пользователям:\n\nДля отмены напишите /cancel")
    await state.set_state(AdminStates.waiting_for_broadcast)


@router.message(AdminStates.waiting_for_broadcast, F.from_user.id.in_(config.ADMIN_IDS))
async def broadcast_process(message: Message, state: FSMContext, bot: Bot):
    # Если админ нажал кнопку меню или команду во время ввода
    if message.text.startswith('/'):
        if message.text == '/cancel':
            await state.clear()
            return await message.answer("🚫 Рассылка отменена.")

    users = await get_all_users()
    await message.answer(f"📢 Начинаю рассылку на {len(users)} пользователей...")

    count = 0
    for user in users:
        try:
            # Отправляем сообщение
            await bot.send_message(user.user_id, message.text)
            count += 1
            # Небольшая пауза, чтобы Telegram не заблокировал за спам
            await asyncio.sleep(0.05)
        except Exception as e:
            logging.error(f"Не удалось отправить сообщение {user.user_id}: {e}")

    await state.clear()
    await message.answer(f"✅ Рассылка завершена!\n📊 Доставлено: {count} из {len(users)}")


# --- ВЫДАЧА ПРЕМИУМА ---

@router.message(Command("give_premium"), F.from_user.id.in_(config.ADMIN_IDS))
async def give_premium_cmd(message: Message):
    try:
        # Разбиваем команду "/give_premium 12345" на части
        parts = message.text.split()
        if len(parts) < 2:
            return await message.answer("⚠️ Формат: `/give_premium 12345678` (ID пользователя)")

        user_id = int(parts[1])
        await set_user_premium(user_id)

        await message.answer(f"💎 **Premium успешно выдан!**\n👤 ID: `{user_id}`")

        # Опционально: уведомляем пользователя
        try:
            from aiogram import Bot
            # Мы можем попробовать отправить пользователю радостную весть
            await message.bot.send_message(user_id, "🎉 Поздравляем! Администратор выдал вам **PREMIUM статус**.")
        except Exception as e:
            logging.error(f"Ошибка, перезапустите бота: {e}")
            pass

    except ValueError:
        await message.answer("❌ Ошибка: ID должен быть числом.")
    except Exception as e:
        logging.error(f"Ошибка в give_premium: {e}")
        await message.answer("❌ Произошла ошибка при выдаче Premium.")

# --- БАНЫ ---
@router.message(Command("ban"), F.from_user.id.in_(config.ADMIN_IDS))
async def ban_user_cmd(message: Message):
    try:
        parts = message.text.split()
        if len(parts) < 2:
            return await message.answer("⚠️ Формат: `/ban 12345678`")

        user_id = int(parts[1])
        await set_user_ban(user_id, True)
        await message.answer(f"🚫 Пользователь `{user_id}` заблокирован.")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


@router.message(Command("unban"), F.from_user.id.in_(config.ADMIN_IDS))
async def unban_user_cmd(message: Message):
    try:
        parts = message.text.split()
        if len(parts) < 2:
            return await message.answer("⚠️ Формат: `/unban 12345678`")

        user_id = int(parts[1])
        await set_user_ban(user_id, False)
        await message.answer(f"✅ Пользователь `{user_id}` разблокирован.")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")