from decimal import (
    Decimal,
)

from sqlalchemy import (
    BigInteger,
    Column,
    Integer,
    PrimaryKeyConstraint,
    Numeric,
)
from sqlalchemy.ext.asyncio import (
    AsyncAttrs,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
)
from sqlalchemy.types import (
    Enum,
)

from enumerations import (
    SymbolId,
)


class Base(AsyncAttrs, DeclarativeBase):
    pass


class OKXDataSetRecordData_2(Base):
    __tablename__ = 'okx_data_set_record_data_2'

    __table_args__ = (
        PrimaryKeyConstraint(  # Explicitly define composite primary key
            'symbol_id',
            'start_trade_id',
        ),
    )

    # Primary key fields

    symbol_id: Mapped[SymbolId] = Column(
        Enum(
            SymbolId,
        ),
    )

    start_trade_id: Mapped[int] = Column(
        Integer,
    )

    # Attribute fields

    buy_quantity: Mapped[Decimal] = Column(
        Numeric,
    )

    buy_trades_count: Mapped[int] = Column(
        BigInteger,
    )

    buy_volume: Mapped[Decimal] = Column(
        Numeric,
    )

    # Computed fields:
    # - buy_quantity_percent = buy_quantity / total_quantity
    # - buy_trades_count_percent = buy_trades_count / total_trades_count
    # - buy_volume_percent = buy_volume / total_volume
    # - buy_volume_by_max_percent = buy_volume / max_volume

    close_price: Mapped[Decimal] = Column(
        Numeric,
    )

    # Computed fields:
    # - close_price_delta = close_price - open_price
    # - close_price_delta_percent = close_price_delta / open_price

    end_timestamp_ms: Mapped[int] = Column(
        BigInteger,
    )

    end_trade_id: Mapped[int] = Column(
        BigInteger,
    )

    high_price: Mapped[Decimal] = Column(
        Numeric,
    )

    # Computed fields:
    # - high_price_delta = high_price - open_price
    # - high_price_delta_percent = high_price_delta / open_price

    low_price: Mapped[Decimal] = Column(
        Numeric,
    )

    # Computed fields:
    # - low_price_delta = low_price - open_price
    # - low_price_delta_percent = 1 + low_price_delta / open_price

    open_price: Mapped[Decimal] = Column(
        Numeric,
    )

    # Computed fields:
    # - sell_quantity = total_quantity - buy_quantity
    # - sell_quantity_percent = sell_quantity / total_quantity
    # - sell_trades_count = total_trades_count - buy_trades_count
    # - sell_trades_count_percent = sell_trades_count / total_trades_count
    # - sell_volume = total_volume - buy_volume
    # - sell_volume_percent = sell_volume / total_volume
    # - sell_volume_by_max_percent = sell_volume / max_volume
    # - sell_price_average = sell_volume / sell_quantity

    start_timestamp_ms: Mapped[int] = Column(
        BigInteger,
    )

    total_quantity: Mapped[Decimal] = Column(
        Numeric,
    )

    total_trades_count: Mapped[int] = Column(
        BigInteger,
    )

    total_volume: Mapped[Decimal] = Column(
        Numeric,
    )

    # Computed fields:
    # - total_price_average = total_volume / total_quantity
    # - total_volume_by_max_percent = total_volume / max_volume
