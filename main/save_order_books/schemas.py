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
