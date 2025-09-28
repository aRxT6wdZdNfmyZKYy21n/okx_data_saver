"""
Константы для работы с Redis.
"""

from typing import Final

from enumerations import (
    SymbolId,
)

# Префиксы ключей Redis
TRADES_PREFIX: Final[str] = 'trades'
BOLLINGER_PREFIX: Final[str] = 'bollinger'
CANDLES_PREFIX: Final[str] = 'candles'
RSI_PREFIX: Final[str] = 'rsi'
SMOOTHED_PREFIX: Final[str] = 'smoothed'
LINES_PREFIX: Final[str] = 'lines'
EXTREME_LINES_PREFIX: Final[str] = 'extreme_lines'
ORDER_BOOK_VOLUMES_PREFIX: Final[str] = 'order_book_volumes'
VELOCITY_PREFIX: Final[str] = 'velocity'
METADATA_PREFIX: Final[str] = 'metadata'

# Максимальный размер части данных (500 МБ для безопасности)
MAX_PART_SIZE_BYTES: Final[int] = 500_000_000

# Время жизни ключей в Redis (в секундах)
# 1 час для основных данных
DEFAULT_TTL: Final[int] = 3600
# 1 час для метаданных
METADATA_TTL: Final[int] = 3600

# Версии схем данных
SCHEMA_VERSION: Final[str] = '1.0'


def get_trades_data_key(symbol_id: SymbolId) -> str:
    """Получение ключа для основных данных о сделках."""
    return f'{TRADES_PREFIX}:{symbol_id}:data'


def get_bollinger_key(symbol_id: SymbolId) -> str:
    """Получение ключа для полос Боллинджера."""
    return f'{TRADES_PREFIX}:{symbol_id}:{BOLLINGER_PREFIX}'


def get_candles_key(symbol_id: SymbolId, interval: str) -> str:
    """Получение ключа для свечных данных по интервалу."""
    return f'{TRADES_PREFIX}:{symbol_id}:{CANDLES_PREFIX}:{interval}'


def get_rsi_key(symbol_id: SymbolId) -> str:
    """Получение ключа для RSI данных."""
    return f'{TRADES_PREFIX}:{symbol_id}:{RSI_PREFIX}'


def get_smoothed_key(symbol_id: SymbolId, level: str) -> str:
    """Получение ключа для сглаженных данных по уровню."""
    return f'{TRADES_PREFIX}:{symbol_id}:{SMOOTHED_PREFIX}:{level}'


def get_lines_key(symbol_id: SymbolId, level: str) -> str:
    """Получение ключа для линий по уровню."""
    return f'{TRADES_PREFIX}:{symbol_id}:{LINES_PREFIX}:{level}'


def get_extreme_lines_key(symbol_id: SymbolId) -> str:
    """Получение ключа для экстремальных линий."""
    return f'{TRADES_PREFIX}:{symbol_id}:{EXTREME_LINES_PREFIX}'


def get_order_book_volumes_key(symbol_id: SymbolId) -> str:
    """Получение ключа для объемов стакана."""
    return f'{TRADES_PREFIX}:{symbol_id}:{ORDER_BOOK_VOLUMES_PREFIX}'


def get_velocity_key(symbol_id: SymbolId) -> str:
    """Получение ключа для данных скорости."""
    return f'{TRADES_PREFIX}:{symbol_id}:{VELOCITY_PREFIX}'


def get_metadata_key(symbol_id: SymbolId) -> str:
    """Получение ключа для метаданных символа."""
    return f'{TRADES_PREFIX}:{symbol_id}:{METADATA_PREFIX}'


def get_available_symbols_key() -> str:
    """Получение ключа для списка доступных символов."""
    return 'available_symbols'
