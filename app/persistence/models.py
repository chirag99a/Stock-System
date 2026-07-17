from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Trade(Base):
    __tablename__ = "trades"

    id: Mapped[str] = mapped_column(String, primary_key=True)  # UUID as string
    instrument: Mapped[str] = mapped_column(String, index=True)
    strike: Mapped[int] = mapped_column(Integer, index=True)
    option_type: Mapped[str] = mapped_column(String, index=True)  # CE/PE
    side: Mapped[str] = mapped_column(String, index=True)  # LONG/SHORT

    entry_price: Mapped[float] = mapped_column(Float)
    pnl: Mapped[float] = mapped_column(Float)

    signal_reason: Mapped[str] = mapped_column(String)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    notification_status: Mapped[str] = mapped_column(String, default="pending", index=True)
    notification_dedup_key: Mapped[str] = mapped_column(String, unique=True, index=True)
