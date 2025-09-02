from decimal import (
    Decimal,
)

from sqlalchemy import (
    Column,
    BigInteger,
    Boolean,
    Index,
    Numeric,
    PrimaryKeyConstraint,
    Text
)
from sqlalchemy.ext.asyncio import (
    AsyncAttrs,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    # mapped_column,
)


class Base(AsyncAttrs, DeclarativeBase):
    pass


class OKXTradeData(Base):
    __tablename__ = "okx_trade_data"
    __table_args__ = (
        PrimaryKeyConstraint(  # Explicitly define composite primary key
            'symbol_name',
            'trade_id',
        ),
    )

    # Primary key fields

    symbol_name: Mapped[str] = Column(Text)
    trade_id: Mapped[int] = Column(BigInteger)

    # Attribute fields

    is_buy: Mapped[bool] = Column(Boolean)

    price: Mapped[Decimal] = Column(Numeric)
    quantity: Mapped[Decimal] = Column(Numeric)

    timestamp_ms: Mapped[int] = Column(BigInteger)

    # Indices

    timestamp_ms_idx = Index(
        'timestamp_ms_idx',
        "symbol_name",
        'timestamp_ms'
    ),
