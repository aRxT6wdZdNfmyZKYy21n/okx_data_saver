from decimal import (
    Decimal,
)

from sqlalchemy import (
    BigInteger,
    Column,
    Integer,
    JSON,
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


class OKXDataSetRecordData(Base):
    __tablename__ = 'okx_data_set_record_data'

    __table_args__ = (
        PrimaryKeyConstraint(  # Explicitly define composite primary key
            'symbol_name',
            'data_set_idx',
            'record_idx',
        ),
    )

    # Primary key fields

    symbol_id: Mapped[SymbolId] = Column(
        Enum(
            SymbolId,
        ),
    )

    data_set_idx: Mapped[int] = Column(
        Integer,
    )

    record_idx: Mapped[int] = Column(
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

    close_price_delta: Mapped[Decimal] = Column(  # = close_price - open_price
        Numeric,
    )

    # Computed fields:
    # - close_price = open_price + close_price_delta
    # - close_price_delta_percent = 1 + close_price_delta / open_price

    # delta_ask_quantity_raw_by_price_raw_map: dict[str, str] = Column(
    #     JSON,
    # )

    # Computed fields:
    # - TODO

    # delta_bid_quantity_raw_by_price_raw_map: dict[str, str] = Column(
    #     JSON,
    # )

    # Computed fields:
    # - TODO

    high_price_delta: Mapped[Decimal] = Column(  # = high_price - open_price
        Numeric,
    )

    # Computed fields:
    # - high_price = open_price + high_price_delta
    # - high_price_delta_percent = 1 + high_price_delta / open_price

    # initial_ask_quantity_raw_by_price_raw_map: dict[str, str] = Column(
    #     JSON,
    # )

    initial_asks_total_quantity: Mapped[Decimal] = Column( # = sum(initial_ask_quantity_i)
        Numeric,
    )

    initial_asks_total_volume: Mapped[Decimal] = Column(  # = sum(initial_ask_price_i * initial_ask_quantity_i)
        Numeric,
    )

    max_initial_ask_price: Mapped[Decimal] = Column(  # = max(initial_ask_price_i)
        Numeric,
    )

    max_initial_ask_quantity: Mapped[Decimal] = Column(  # = max(initial_ask_quantity_i)
        Numeric,
    )

    max_initial_ask_volume: Mapped[Decimal] = Column(  # = max(initial_ask_price_i * initial_ask_quantity_i)
        Numeric,
    )

    # TODO
    # - min_initial_ask_price = min(initial_ask_price_i)
    # - min_initial_ask_quantity = min(initial_ask_quantity_i)
    # - min_initial_ask_volume = min(initial_ask_price_i * initial_ask_quantity_i)

    # Computed fields:
    # - initial_asks_total_volume_by_max_percent = initial_asks_total_volume / max_volume
    # - max_initial_ask_price_delta = max_initial_ask_price - open_price
    # - max_initial_ask_price_delta_percent = 1 + max_initial_ask_price_delta / open_price
    # - max_initial_ask_volume_by_max_percent = max_initial_ask_volume / max_volume
    # - min_initial_ask_price_delta = min_initial_ask_price - open_price
    # - min_initial_ask_price_delta_percent = 1 + min_initial_ask_price_delta / open_price
    # - min_initial_ask_volume_by_max_percent = min_initial_ask_volume / max_volume

    # initial_bid_quantity_raw_by_price_raw_map: dict[str, str] = Column(
    #     JSON,
    # )

    # Computed fields:  # TODO: uncomment
    # - initial_bids_total_quantity = sum(initial_bid_quantity_i)
    # - initial_bids_total_volume = sum(initial_bid_price_i * initial_bid_quantity_i)
    # - initial_bids_total_volume_by_max_percent = initial_bids_total_volume / max_volume
    # - max_initial_bid_price = max(initial_bid_price_i)
    # - max_initial_bid_price_delta = max_initial_bid_price - open_price
    # - max_initial_bid_price_delta_percent = 1 + max_initial_bid_price_delta / open_price
    # - max_initial_bid_quantity = max(initial_bid_quantity_i)
    # - max_initial_bid_volume = max(initial_bid_price_i * initial_bid_quantity_i)
    # - max_initial_bid_volume_by_max_percent = max_initial_bid_volume / max_volume
    # - min_initial_bid_price = min(initial_bid_price_i)
    # - min_initial_bid_price_delta = min_initial_bid_price - open_price
    # - min_initial_bid_price_delta_percent = 1 + min_initial_bid_price_delta / open_price
    # - min_initial_bid_quantity = min(initial_bid_quantity_i)
    # - min_initial_bid_volume = min(initial_bid_price_i * initial_bid_quantity_i)
    # - min_initial_bid_volume_by_max_percent = min_initial_bid_volume / max_volume

    # - initial_spread = min_initial_ask_price - max_initial_bid_price
    # - initial_spread_percent = initial_spread / max_initial_bid_price
    # - mid_initial_price = max_initial_bid_price + initial_spread / 2

    low_price_delta: Mapped[Decimal] = Column(  # = low_price - open_price
        Numeric,
    )

    # Computed fields:
    # - low_price = open_price + low_price_delta
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
