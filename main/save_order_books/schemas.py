from sqlalchemy import (
    Column,
    BigInteger,
    JSON,
    PrimaryKeyConstraint,
    Text,
)
from sqlalchemy.ext.asyncio import (
    AsyncAttrs,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    # mapped_column,
)
from sqlalchemy.types import (
    Enum,
)

from enumerations import (
    OKXOrderBookActionId,
    SymbolId,
)


class Base(AsyncAttrs, DeclarativeBase):
    pass


class OKXOrderBookData(Base):
    __tablename__ = 'okx_order_book_data'

    __table_args__ = (
        PrimaryKeyConstraint(  # Explicitly define composite primary key
            'symbol_name',
            'timestamp_ms',
        ),
    )

    # Primary key fields

    symbol_name: Mapped[str] = Column(Text)
    timestamp_ms: Mapped[int] = Column(BigInteger)

    # Attribute fields

    action: Mapped[str] = Column(Text)

    asks: list[list[str, str, str, str]] = Column(JSON)
    bids: list[list[str, str, str, str]] = Column(JSON)


class OKXOrderBookData2(Base):
    __tablename__ = 'okx_order_book_data_2'

    __table_args__ = (
        PrimaryKeyConstraint(  # Explicitly define composite primary key
            'symbol_id',
            'timestamp_ms',
        ),
    )

    # Primary key fields

    symbol_id: Mapped[SymbolId] = Column(
        Enum(
            SymbolId,
        ),
    )

    timestamp_ms: Mapped[int] = Column(BigInteger)

    # Attribute fields

    action_id: OKXOrderBookActionId = Column(
        Enum(
            OKXOrderBookActionId,
        ),
    )

    asks: list[list[str, str, str, str]] = Column(JSON)
    bids: list[list[str, str, str, str]] = Column(JSON)
