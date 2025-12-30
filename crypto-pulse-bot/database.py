from sqlalchemy import update
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import select, String, BigInteger, DateTime, Float, func, Boolean, Column, Integer, ForeignKey
from datetime import datetime, timedelta
from typing import Optional


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, unique=True)  # ID из Телеграм
    username: Mapped[Optional[str]] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(20), default="FREE")  # FREE, PREMIUM
    subscribed_until: Mapped[Optional[datetime]] = mapped_column(DateTime)
    selected_pairs: Mapped[str] = mapped_column(String, default="BTC/USDT,ETH/USDT")  # Храним через запятую
    daily_risk_limit: Mapped[float] = mapped_column(Float, default=2.0)  # 2% дневного лимита
    daily_risk_used: Mapped[float] = mapped_column(Float, default=0.0)
    max_open_positions: Mapped[int] = mapped_column(Integer, default=5)
    risk_reset_time: Mapped[Optional[datetime]] = mapped_column(DateTime)
    deposit: Mapped[float] = mapped_column(Float, default=1000.0)
    risk_per_trade: Mapped[float] = mapped_column(Float, default=1.0)  # в процентах
    is_banned: Mapped[bool] = mapped_column(Boolean, default=False)


class SignalHistory(Base):
    __tablename__ = "signals_history"

    id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[str] = mapped_column(String(20))
    side: Mapped[str] = mapped_column(String(10))  # buy/sell
    entry_price: Mapped[float] = mapped_column(Float)
    # Цели тейк-профита
    tp1: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    tp2: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    tp3: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    # Стоп-лосс
    sl: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    exit_price: Mapped[Optional[float]] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(20), default="OPEN")  # OPEN, TP, SL
    profit_pct: Mapped[Optional[float]] = mapped_column(Float)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)

    user_id: Mapped[int] = mapped_column(Integer, ForeignKey('users.id'), nullable=True)


class SignalStatistic(Base):
    __tablename__ = 'signal_statistics'

    id = Column(Integer, primary_key=True)
    signal_id = Column(String, unique=True)  # Уникальный ID сигнала
    symbol = Column(String)
    direction = Column(String)  # 'buy'/'sell'
    entry_price = Column(Float)
    stop_loss = Column(Float)
    take_profit_1 = Column(Float)
    take_profit_2 = Column(Float)
    take_profit_3 = Column(Float)
    quality_rating = Column(String)  # 'STRONG', 'HIGH', etc
    quality_score = Column(Float)  # процент
    generated_at = Column(DateTime)
    result = Column(String)  # 'TP1', 'TP2', 'TP3', 'SL', 'OPEN', 'EXPIRED'
    closed_at = Column(DateTime, nullable=True)
    profit_loss_percent = Column(Float, nullable=True)  # +5.2 или -3.1

    # Метрики качества из SignalQualityRater
    mtf_consensus_score = Column(Integer, default=0)
    trend_strength_score = Column(Integer, default=0)
    volume_score = Column(Integer, default=0)
    rsi_score = Column(Integer, default=0)
    risk_reward_score = Column(Integer, default=0)

    def __repr__(self):
        return f"<SignalStatistic(symbol={self.symbol}, result={self.result})>"


# Создаем движок (SQLite — просто и надежно для начала)
engine = create_async_engine("sqlite+aiosqlite:///database.db")
async_session = async_sessionmaker(engine, expire_on_commit=False)


async def check_and_expire_subscriptions():
    """Сбрасывает статус PREMIUM, если срок подписки истек"""
    async with async_session() as session:
        now = datetime.now()

        # Находим всех, у кого статус PREMIUM, но дата окончания уже прошла
        stmt = select(User).where(
            User.status == "PREMIUM",
            User.subscribed_until < now
        )
        result = await session.execute(stmt)
        expired_users = result.scalars().all()

        expired_ids = []
        for user in expired_users:
            expired_ids.append(user.user_id)
            user.status = "FREE"
            # Опционально: можно очистить дату, чтобы не смущала
            user.subscribed_until = None

        await session.commit()
        return expired_ids  # Возвращаем список ID, чтобы уведомить их


# Функция для сохранения нового сигнала
async def save_new_signal(sig_data: dict):
    async with async_session() as session:
        new_sig = SignalHistory(
            symbol=sig_data['symbol'],
            side=sig_data['side'],
            entry_price=sig_data['entry'],
            tp1=sig_data.get('tp1'),
            tp2=sig_data.get('tp2'),
            tp3=sig_data.get('tp3'),
            sl=sig_data.get('sl'),
            status="OPEN",
            user_id=sig_data.get('user_id', 0)
        )
        session.add(new_sig)
        await session.commit()
        return new_sig.id


# Функция для закрытия сигнала в базе
async def close_signal_in_db(symbol: str, exit_price: float, status: str):
    async with async_session() as session:
        # Ищем последний открытый сигнал по этой паре
        stmt = select(SignalHistory).where(
            SignalHistory.symbol == symbol,
            SignalHistory.status == "OPEN"
        ).order_by(SignalHistory.timestamp.desc())

        result = await session.execute(stmt)
        sig = result.scalar_one_or_none()

        if sig:
            sig.exit_price = float(exit_price)
            sig.status = status

            entry = float(sig.entry_price)
            exit_p = float(exit_price)

            if sig.side == "buy":
                sig.profit_pct = ((exit_p - entry) / entry) * 100
            else:
                sig.profit_pct = ((entry - exit_p) / entry) * 100
            await session.commit()


# Функция инициализации БД (создает файл и таблицы)
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# --- Функции для работы с пользователем ---

async def get_or_create_user(user_id: int, username: str = None):
    async with async_session() as session:
        result = await session.execute(select(User).where(User.user_id == user_id))
        user = result.scalar_one_or_none()

        if not user:
            user = User(user_id=user_id, username=username)
            session.add(user)
            await session.commit()
            await session.refresh(user)
        return user


async def update_user_pairs(user_id: int, pairs_str: str):
    async with async_session() as session:
        result = await session.execute(select(User).where(User.user_id == user_id))
        user = result.scalar_one_or_none()
        if user:
            user.selected_pairs = pairs_str
            await session.commit()


async def set_user_premium(user_id: int):
    async with async_session() as session:
        # Устанавливаем дату окончания: текущее время + 30 дней
        expire_date = datetime.now() + timedelta(days=30)

        stmt = update(User).where(User.user_id == user_id).values(
            status="PREMIUM",
            subscribed_until=expire_date
        )
        await session.execute(stmt)
        await session.commit()


async def get_all_users():
    """Возвращает всех пользователей из базы для рассылки"""
    async with async_session() as session:
        result = await session.execute(select(User))
        return result.scalars().all()


async def get_total_users_count():
    """Возвращает общее количество пользователей"""
    async with async_session() as session:
        result = await session.execute(select(func.count(User.id)))
        return result.scalar() or 0


async def set_user_ban(user_id: int, status: bool):
    """Установить или снять бан"""
    async with async_session() as session:
        await session.execute(
            update(User).where(User.user_id == user_id).values(is_banned=status)
        )
        await session.commit()


async def is_user_banned(user_id: int) -> bool:
    """Проверить, забанен ли пользователь"""
    async with async_session() as session:
        result = await session.execute(select(User.is_banned).where(User.user_id == user_id))
        return result.scalar() or False
