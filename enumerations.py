from enum import (
    auto,
    IntEnum,
    StrEnum,
)


class OKXOrderBookActionId(IntEnum):
    Snapshot = auto()
    Update = auto()


class SymbolId(IntEnum):
    BTC_USDT = auto()
    ETH_USDT = auto()
    SOL_USDT = auto()


class TradingDirection(StrEnum):
    Bear = 'Bear'
    Bull = 'Bull'
    Cross = 'Cross'
