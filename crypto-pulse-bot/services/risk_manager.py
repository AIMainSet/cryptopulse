"""
risk_manager.py - система управления рисками
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional
from sqlalchemy import select
from database import Session, User, SignalHistory
from logging import getLogger

logger = getLogger(__name__)


class RiskManager:
    def __init__(self):
        self.daily_reset_task = None

    async def check_user_limits(self, user_id: int, signal_risk: float) -> Dict[str, bool | str]:
        """
        Проверяет лимиты пользователя перед отправкой сигнала

        Возвращает: {
            'allowed': bool,
            'reason': str (если not allowed),
            'position_size': float (если allowed)
        }
        """
        async with Session() as session:
            user = await session.get(User, user_id)
            if not user:
                return {'allowed': False, 'reason': 'Пользователь не найден'}

            # 1. Проверка дневного риска (5% по умолчанию)
            current_time = datetime.utcnow()
            if user.risk_reset_time and user.risk_reset_time < current_time:
                # Сброс дневного риска
                user.daily_risk_used = 0.0
                user.risk_reset_time = current_time + timedelta(days=1)
                await session.commit()

            daily_risk_limit = user.daily_risk_limit or 5.0  # 5% по умолчанию
            if user.daily_risk_used + signal_risk > daily_risk_limit:
                return {
                    'allowed': False,
                    'reason': f'Достигнут дневной лимит риска ({daily_risk_limit}%)'
                }

            # 2. Проверка максимального количества открытых позиций
            open_positions_query = select(SignalHistory).where(
                SignalHistory.user_id == user_id,
                SignalHistory.status.in_(['OPEN', 'PARTIAL'])
            )
            open_positions = await session.execute(open_positions_query)
            open_count = len(open_positions.scalars().all())

            max_positions = user.max_open_positions or 5  # По умолчанию 5
            if open_count >= max_positions:
                return {
                    'allowed': False,
                    'reason': f'Достигнут лимит открытых позиций ({max_positions})'
                }

            # 3. Расчет размера позиции
            position_size = await self.calculate_position_size(
                user_id,
                user.deposit or 1000,  # Если депозит не указан, берем 1000 USDT
                signal_risk,
                user.risk_per_trade or 1.0  # 1% на сделку по умолчанию
            )

            return {
                'allowed': True,
                'position_size': position_size,
                'reason': 'Лимиты в порядке'
            }

    async def calculate_position_size(
            self,
            user_id: int,
            deposit: float,
            signal_risk: float,
            risk_per_trade: float
    ) -> float:
        """
        Рассчитывает размер позиции на основе:
        - Депозита пользователя
        - Риска на сделку (в процентах)
        - Стоп-лосса сигнала
        """
        # Формула: (Депозит * Риск на сделку%) / (Стоп-лосс в %)
        # Например: 1000$ * 1% / 2% = 500$ позиция
        position_size = (deposit * risk_per_trade / 100) / (signal_risk / 100)
        return round(position_size, 2)

    async def update_daily_risk(self, user_id: int, risk_amount: float):
        """Обновляет использованный дневной риск"""
        async with Session() as session:
            user = await session.get(User, user_id)
            if user:
                user.daily_risk_used = (user.daily_risk_used or 0) + risk_amount
                await session.commit()
                logger.info(f"Обновлен дневной риск для {user_id}: {user.daily_risk_used}%")

    async def start_daily_reset_scheduler(self):
        """Запускает задачу для ежедневного сброса рисков"""

        async def reset_loop():
            while True:
                try:
                    await asyncio.sleep(3600)  # Проверяем каждый час
                    async with Session() as session:
                        users = await session.execute(select(User))
                        for user in users.scalars():
                            if user.risk_reset_time and user.risk_reset_time < datetime.utcnow():
                                user.daily_risk_used = 0.0
                                user.risk_reset_time = datetime.utcnow() + timedelta(days=1)
                        await session.commit()
                except Exception as e:
                    logger.error(f"Ошибка в reset_loop: {e}")

        self.daily_reset_task = asyncio.create_task(reset_loop())
