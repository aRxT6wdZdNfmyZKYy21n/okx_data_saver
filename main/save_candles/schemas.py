from decimal import (
    Decimal,
)

from sqlalchemy import (
    Column,
    BigInteger,
    Boolean,
    Numeric,
    JSON,
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


class OKXCandleData15m(Base):
    __tablename__ = "okx_candle_data_15m"
    __table_args__ = (
        PrimaryKeyConstraint(  # Explicitly define composite primary key
            'symbol_name',
            'start_timestamp_ms'
        ),
    )

    # Primary key fields

    symbol_name: Mapped[str] = Column(Text)
    start_timestamp_ms: Mapped[int] = Column(BigInteger)

    # Attribute fields

    is_closed: Mapped[bool] = Column(Boolean)

    close_price: Mapped[Decimal] = Column(Numeric)
    high_price: Mapped[Decimal] = Column(Numeric)
    low_price: Mapped[Decimal] = Column(Numeric)
    open_price: Mapped[Decimal] = Column(Numeric)

    volume_contracts_count: Mapped[Decimal] = Column(Numeric)
    volume_base_currency: Mapped[Decimal] = Column(Numeric)
    volume_quote_currency: Mapped[Decimal] = Column(Numeric)


class OKXCandleData1H(Base):
    __tablename__ = "okx_candle_data_1H"
    __table_args__ = (
        PrimaryKeyConstraint(  # Explicitly define composite primary key
            'symbol_name',
            'start_timestamp_ms'
        ),
    )

    # Primary key fields

    symbol_name: Mapped[str] = Column(Text)
    start_timestamp_ms: Mapped[int] = Column(BigInteger)

    # Attribute fields

    is_closed: Mapped[bool] = Column(Boolean)

    close_price: Mapped[Decimal] = Column(Numeric)
    high_price: Mapped[Decimal] = Column(Numeric)
    low_price: Mapped[Decimal] = Column(Numeric)
    open_price: Mapped[Decimal] = Column(Numeric)

    volume_contracts_count: Mapped[Decimal] = Column(Numeric)
    volume_base_currency: Mapped[Decimal] = Column(Numeric)
    volume_quote_currency: Mapped[Decimal] = Column(Numeric)
