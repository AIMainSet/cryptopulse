import asyncio
import logging
from datetime import datetime, timedelta
from aiogram import Bot
from database import db
from payments import cryptopay

logger = logging.getLogger(__name__)


class PaymentChecker:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.check_interval = 30  # секунд
        self.expiration_check_interval = 3600  # 1 час

    async def check_pending_payments(self):
        """Проверяет все pending платежи"""
        if not cryptopay or not cryptopay.api:
            logger.warning("CryptoPay не инициализирован, пропускаем проверку платежей")
            return

        try:
            # Получаем pending платежи из БД
            pending_payments = await db.get_pending_payments()

            if not pending_payments:
                return

            logger.info(f"🔍 Проверяю {len(pending_payments)} pending платежей...")

            for payment in pending_payments:
                try:
                    # Проверяем статус счета в CryptoBot
                    invoice = await cryptopay.check_invoice(payment['invoice_id'])

                    if not invoice:
                        logger.warning(f"Не удалось получить статус счета {payment['invoice_id']}")
                        continue

                    if invoice['status'] == 'paid':
                        # Обновляем статус платежа в БД
                        await db.update_payment_status(payment['invoice_id'], "PAID")

                        # Получаем пользователя
                        user = await db.get_user_by_id(payment['user_id'])
                        if not user:
                            logger.error(f"Пользователь {payment['user_id']} не найден")
                            continue

                        # Обновляем статус пользователя
                        subscription_end = datetime.now() + timedelta(days=30)
                        await db.update_user_status(
                            user['telegram_id'],
                            payment['tariff'],
                            subscription_end
                        )

                        # Отправляем уведомление пользователю
                        try:
                            await self.bot.send_message(
                                user['telegram_id'],
                                f"✅ *Оплата подтверждена!*\n\n"
                                f"Ваш статус изменен на *{payment['tariff']}*\n"
                                f"Подписка активна до: {subscription_end.strftime('%d.%m.%Y %H:%M')}\n\n"
                                f"Спасибо за покупку! 🎉",
                                parse_mode="Markdown"
                            )
                            logger.info(f"✅ Уведомление отправлено пользователю {user['telegram_id']}")
                        except Exception as e:
                            logger.error(f"Ошибка отправки уведомления пользователю {user['telegram_id']}: {e}")

                    elif invoice['status'] == 'expired':
                        # Помечаем платеж как просроченный
                        await db.update_payment_status(payment['invoice_id'], "EXPIRED")
                        logger.info(f"❌ Счет {payment['invoice_id']} просрочен")

                except Exception as e:
                    logger.error(f"Ошибка обработки платежа {payment.get('invoice_id')}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Ошибка в check_pending_payments: {e}")

    async def check_subscription_expiration(self):
        """Проверяет окончание подписок"""
        try:
            # Проверяем только активные пользователи (не FREE)
            cursor = db.conn.cursor()
            cursor.execute("""
                SELECT * FROM users 
                WHERE status IN ('PREMIUM', 'VIP') 
                AND subscription_end IS NOT NULL 
                AND subscription_end < datetime('now')
            """)

            expired_users = cursor.fetchall()

            if not expired_users:
                return

            logger.info(f"⚠️ Найдено {len(expired_users)} пользователей с истекшей подпиской")

            for user_row in expired_users:
                try:
                    user = dict(user_row)

                    # Обновляем статус на FREE
                    await db.update_user_status(user['telegram_id'], 'FREE', None)

                    # Отправляем уведомление
                    await self.bot.send_message(
                        user['telegram_id'],
                        "⚠️ *Ваша подписка истекла*\n\n"
                        "Ваш статус изменен на FREE. Для возобновления доступа оформите подписку заново.\n\n"
                        "Доступные функции ограничены:\n"
                        "• Задержка в уведомлениях\n"
                        "• Ограниченное количество пар\n"
                        "• Базовая статистика",
                        parse_mode="Markdown"
                    )

                    logger.info(f"✅ Статус пользователя {user['telegram_id']} изменен на FREE")

                except Exception as e:
                    logger.error(f"Ошибка обработки истекшей подписки пользователя {user.get('telegram_id')}: {e}")

        except Exception as e:
            logger.error(f"Ошибка в check_subscription_expiration: {e}")

    async def run(self):
        """Основной цикл проверки"""
        logger.info("🔄 Запущен PaymentChecker")

        while True:
            try:
                await self.check_pending_payments()
                # Проверяем истечение подписок реже
                if datetime.now().minute % 5 == 0:  # Каждые 5 минут
                    await self.check_subscription_expiration()
            except Exception as e:
                logger.error(f"Ошибка в основном цикле PaymentChecker: {e}")

            await asyncio.sleep(self.check_interval)