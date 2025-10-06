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


class OKXDataSetRecordData(Base):
    __tablename__ = 'okx_data_set_record_data'

    __table_args__ = (
        PrimaryKeyConstraint(  # Explicitly define composite primary key
            'symbol_id',
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

    close_price: Mapped[Decimal | None] = Column(
        Numeric,
    )

    # Computed fields:
    # - close_price_delta = close_price - open_price
    # - close_price_delta_percent = 1 + close_price_delta / open_price

    # delta_ask_quantity_raw_by_price_raw_map: dict[str, str] = Column(
    #     JSON,
    # )

    end_asks_total_quantity: Mapped[Decimal] = Column(  # = sum(end_ask_quantity_i)
        Numeric,
    )

    end_asks_total_volume: Mapped[Decimal] = Column(  # = sum(end_ask_price_i * end_ask_quantity_i)
        Numeric,
    )

    max_end_ask_price: Mapped[Decimal] = Column(  # = max(end_ask_price_i)
        Numeric,
    )

    max_end_ask_quantity: Mapped[Decimal] = Column(  # = max(end_ask_quantity_i)
        Numeric,
    )

    max_end_ask_volume: Mapped[Decimal] = Column(  # = max(end_ask_price_i * end_ask_quantity_i)
        Numeric,
    )

    min_end_ask_price: Mapped[Decimal] = Column(  # = min(end_ask_price_i)
        Numeric,
    )

    min_end_ask_quantity: Mapped[Decimal] = Column(  # = min(end_ask_quantity_i)
        Numeric,
    )

    min_end_ask_volume: Mapped[Decimal] = Column(  # = min(end_ask_price_i * end_ask_quantity_i)
        Numeric,
    )

    # Computed fields:
    # - end_asks_total_quantity_delta = end_asks_total_quantity - start_asks_total_quantity
    # - end_asks_total_quantity_delta_percent = 1 + end_asks_total_quantity_delta / start_asks_total_quantity
    # - end_asks_total_volume_by_max_percent = end_asks_total_volume / max_volume
    # - end_asks_total_volume_delta = end_asks_total_volume - start_asks_total_volume
    # - end_asks_total_volume_delta_percent = 1 + end_asks_total_volume_delta / start_asks_total_volume
    # - max_end_ask_price_delta_1 = max_end_ask_price - close_price
    # - max_end_ask_price_delta_percent_1 = 1 + max_end_ask_price_delta_1 / close_price
    # - max_end_ask_price_delta_2 = max_end_ask_price - max_start_ask_price
    # - max_end_ask_price_delta_percent_2 = 1 + max_end_ask_price_delta_2 / max_start_ask_price
    # - max_end_ask_quantity_delta = max_end_ask_quantity - max_start_ask_quantity
    # - max_end_ask_quantity_delta_percent = 1 + max_end_ask_quantity_delta / max_start_ask_quantity
    # - max_end_ask_volume_by_max_percent = max_end_ask_volume / max_volume
    # - max_end_ask_volume_delta = max_end_ask_volume - max_start_ask_volume
    # - max_end_ask_volume_delta_percent = 1 + max_end_ask_volume_delta / max_start_ask_volume
    # - min_end_ask_price_delta_1 = min_end_ask_price - close_price
    # - min_end_ask_price_delta_percent_1 = 1 + min_end_ask_price_delta_1 / close_price
    # - min_end_ask_price_delta_2 = min_end_ask_price - min_start_ask_price
    # - min_end_ask_price_delta_percent_2 = 1 + min_end_ask_price_delta_2 / min_start_ask_price
    # - min_end_ask_quantity_delta = min_end_ask_quantity - min_start_ask_quantity
    # - min_end_ask_quantity_delta_percent = 1 + min_end_ask_quantity_delta / min_start_ask_quantity
    # - min_end_ask_volume_by_max_percent = min_end_ask_volume / max_volume
    # - min_end_ask_volume_delta = min_end_ask_volume - min_start_ask_volume
    # - min_end_ask_volume_delta_percent = 1 + min_end_ask_volume_delta / min_start_ask_volume

    # delta_bid_quantity_raw_by_price_raw_map: dict[str, str] = Column(
    #     JSON,
    # )

    end_bids_total_quantity: Mapped[Decimal] = Column(  # = sum(end_bid_quantity_i)
        Numeric,
    )

    end_bids_total_volume: Mapped[Decimal] = Column(  # = sum(end_bid_price_i * end_bid_quantity_i)
        Numeric,
    )

    max_end_bid_price: Mapped[Decimal] = Column(  # = max(end_bid_price_i)
        Numeric,
    )

    max_end_bid_quantity: Mapped[Decimal] = Column(  # = max(end_bid_quantity_i)
        Numeric,
    )

    max_end_bid_volume: Mapped[Decimal] = Column(  # = max(end_bid_price_i * end_bid_quantity_i)
        Numeric,
    )

    min_end_bid_price: Mapped[Decimal] = Column(  # = min(end_bid_price_i)
        Numeric,
    )

    min_end_bid_quantity: Mapped[Decimal] = Column(  # = min(end_bid_quantity_i)
        Numeric,
    )

    min_end_bid_volume: Mapped[Decimal] = Column(  # = min(end_bid_price_i * end_bid_quantity_i)
        Numeric,
    )

    # Computed fields:
    # - end_bids_total_quantity_delta = end_bids_total_quantity - start_bids_total_quantity
    # - end_bids_total_quantity_delta_percent = 1 + end_bids_total_quantity_delta / start_bids_total_quantity
    # - end_bids_total_volume_by_max_percent = end_bids_total_volume / max_volume
    # - end_bids_total_volume_delta = end_bids_total_volume - start_bids_total_volume
    # - end_bids_total_volume_delta_percent = 1 + end_bids_total_volume_delta / start_bids_total_volume
    # - max_end_bid_price_delta_1 = max_end_bid_price - close_price
    # - max_end_bid_price_delta_percent_1 = 1 + max_end_bid_price_delta_1 / close_price
    # - max_end_bid_price_delta_2 = max_end_bid_price - max_start_bid_price
    # - max_end_bid_price_delta_percent_2 = 1 + max_end_bid_price_delta_2 / max_start_bid_price
    # - max_end_bid_quantity_delta = max_end_bid_quantity - max_start_bid_quantity
    # - max_end_bid_quantity_delta_percent = 1 + max_end_bid_quantity_delta / max_start_bid_quantity
    # - max_end_bid_volume_by_max_percent = max_end_bid_volume / max_volume
    # - max_end_bid_volume_delta = max_end_bid_volume - max_start_bid_volume
    # - max_end_bid_volume_delta_percent = 1 + max_end_bid_volume_delta / max_start_bid_volume
    # - min_end_bid_price_delta_1 = min_end_bid_price - close_price
    # - min_end_bid_price_delta_percent_1 = 1 + min_end_bid_price_delta_1 / close_price
    # - min_end_bid_price_delta_2 = min_end_bid_price - min_start_bid_price
    # - min_end_bid_price_delta_percent_2 = 1 + min_end_bid_price_delta_2 / min_start_bid_price
    # - min_end_bid_quantity_delta = min_end_bid_quantity - min_start_bid_quantity
    # - min_end_bid_quantity_delta_percent = 1 + min_end_bid_quantity_delta / min_start_bid_quantity
    # - min_end_bid_volume_by_max_percent = min_end_bid_volume / max_volume
    # - min_end_bid_volume_delta = min_end_bid_volume - min_start_bid_volume
    # - min_end_bid_volume_delta_percent = 1 + min_end_bid_volume_delta / min_start_bid_volume
    # - end_spread = min_end_ask_price - max_end_bid_price
    # - end_spread_delta = end_spread - start_spread
    # - end_spread_delta_percent = 1 + end_spread_delta / start_spread
    # - end_spread_percent = end_spread / max_end_bid_price
    # - end_spread_percent_delta = end_spread_percent - start_spread_percent
    # - end_spread_percent_delta_percent = 1 + end_spread_percent / start_spread_percent
    # - mid_end_price = max_end_bid_price + end_spread / 2
    # - mid_end_price_delta_1 = mid_end_price - close_price
    # - mid_end_price_delta_percent_1 = 1 + mid_end_price_delta_1 / close_price
    # - mid_end_price_delta_2 = mid_end_price - mid_start_price
    # - mid_end_price_delta_percent_2 = 1 + mid_end_price_delta_2 / mid_start_price

    end_timestamp_ms: Mapped[int] = Column(
        BigInteger,
    )

    end_trade_id: Mapped[int | None] = Column(
        BigInteger,
    )

    high_price: Mapped[Decimal] = Column(
        Numeric,
    )

    # Computed fields:
    # - high_price_delta = high_price - open_price
    # - high_price_delta_percent = 1 + high_price_delta / open_price

    # start_ask_quantity_raw_by_price_raw_map: dict[str, str] = Column(
    #     JSON,
    # )

    start_asks_total_quantity: Mapped[Decimal] = Column( # = sum(start_ask_quantity_i)
        Numeric,
    )

    start_asks_total_volume: Mapped[Decimal] = Column(  # = sum(start_ask_price_i * start_ask_quantity_i)
        Numeric,
    )

    max_start_ask_price: Mapped[Decimal] = Column(  # = max(start_ask_price_i)
        Numeric,
    )

    max_start_ask_quantity: Mapped[Decimal] = Column(  # = max(start_ask_quantity_i)
        Numeric,
    )

    max_start_ask_volume: Mapped[Decimal] = Column(  # = max(start_ask_price_i * start_ask_quantity_i)
        Numeric,
    )

    min_start_ask_price: Mapped[Decimal] = Column(  # = min(start_ask_price_i)
        Numeric,
    )

    min_start_ask_quantity: Mapped[Decimal] = Column(  # = min(start_ask_quantity_i)
        Numeric,
    )

    min_start_ask_volume: Mapped[Decimal] = Column(  # = min(start_ask_price_i * start_ask_quantity_i)
        Numeric,
    )

    # Computed fields:
    # - start_asks_total_volume_by_max_percent = start_asks_total_volume / max_volume
    # - max_start_ask_price_delta = max_start_ask_price - open_price
    # - max_start_ask_price_delta_percent = 1 + max_start_ask_price_delta / open_price
    # - max_start_ask_volume_by_max_percent = max_start_ask_volume / max_volume
    # - min_start_ask_price_delta = min_start_ask_price - open_price
    # - min_start_ask_price_delta_percent = 1 + min_start_ask_price_delta / open_price
    # - min_start_ask_volume_by_max_percent = min_start_ask_volume / max_volume

    # start_bid_quantity_raw_by_price_raw_map: dict[str, str] = Column(
    #     JSON,
    # )

    start_bids_total_quantity: Mapped[Decimal] = Column( # = sum(start_bid_quantity_i)
        Numeric,
    )

    start_bids_total_volume: Mapped[Decimal] = Column(  # = sum(start_bid_price_i * start_bid_quantity_i)
        Numeric,
    )

    max_start_bid_price: Mapped[Decimal] = Column(  # = max(start_bid_price_i)
        Numeric,
    )

    max_start_bid_quantity: Mapped[Decimal] = Column(  # = max(start_bid_quantity_i)
        Numeric,
    )

    max_start_bid_volume: Mapped[Decimal] = Column(  # = max(start_bid_price_i * start_bid_quantity_i)
        Numeric,
    )

    min_start_bid_price: Mapped[Decimal] = Column(  # = min(start_bid_price_i)
        Numeric,
    )

    min_start_bid_quantity: Mapped[Decimal] = Column(  # = min(start_bid_quantity_i)
        Numeric,
    )

    min_start_bid_volume: Mapped[Decimal] = Column(  # = min(start_bid_price_i * start_bid_quantity_i)
        Numeric,
    )

    # Computed fields:
    # - start_bids_total_volume_by_max_percent = start_bids_total_volume / max_volume
    # - max_start_bid_price_delta = max_start_bid_price - open_price
    # - max_start_bid_price_delta_percent = 1 + max_start_bid_price_delta / open_price
    # - max_start_bid_volume_by_max_percent = max_start_bid_volume / max_volume
    # - min_start_bid_price_delta = min_start_bid_price - open_price
    # - min_start_bid_price_delta_percent = 1 + min_start_bid_price_delta / open_price
    # - min_start_bid_volume_by_max_percent = min_start_bid_volume / max_volume
    # - start_spread = min_start_ask_price - max_start_bid_price
    # - start_spread_percent = start_spread / max_start_bid_price
    # - mid_start_price = max_start_bid_price + start_spread / 2
    # - mid_start_price_delta = mid_start_price - open_price
    # - mid_start_price_delta_percent = 1 + mid_start_price_delta / open_price

    low_price: Mapped[Decimal | None] = Column(
        Numeric,
    )

    # Computed fields:
    # - low_price_delta = low_price - open_price
    # - low_price_delta_percent = 1 + low_price_delta / open_price

    open_price: Mapped[Decimal | None] = Column(
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

    start_trade_id: Mapped[int | None] = Column(
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
