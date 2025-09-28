"""
Схемы данных для обработки и хранения в Redis.
"""

from datetime import datetime

from pydantic import BaseModel, Field

from enumerations import (
    SymbolId,
)


class DataFrameMetadata(BaseModel):
    """Метаданные для DataFrame в Redis."""

    compression: str = Field(..., description='Алгоритм сжатия')
    compression_ratio: float = Field(..., description='Коэффициент сжатия')
    parts_count: int = Field(..., description='Количество частей')
    total_size: int = Field(..., description='Общий размер в байтах')
    last_updated: datetime = Field(..., description='Время последнего обновления')
    schema_version: str = Field(default='1.0', description='Версия схемы')


class TradesDataMetadata(DataFrameMetadata):
    """Метаданные для данных о сделках."""

    min_trade_id: int = Field(..., description='Минимальный ID сделки')
    max_trade_id: int = Field(..., description='Максимальный ID сделки')
    min_price: float = Field(..., description='Минимальная цена')
    max_price: float = Field(..., description='Максимальная цена')
    symbol_id: SymbolId = Field(..., description='ID символа')


class BollingerMetadata(DataFrameMetadata):
    """Метаданные для полос Боллинджера."""

    symbol_id: SymbolId = Field(..., description='ID символа')
    timeperiod: int = Field(default=20, description='Период для расчета')


class CandlesMetadata(DataFrameMetadata):
    """Метаданные для свечных данных."""

    symbol_id: SymbolId = Field(..., description='ID символа')
    interval: str = Field(..., description='Интервал свечей')
    min_trade_id: int = Field(..., description='Минимальный ID сделки')
    max_trade_id: int = Field(..., description='Максимальный ID сделки')


class RSIMetadata(DataFrameMetadata):
    """Метаданные для RSI данных."""

    symbol_id: SymbolId = Field(..., description='ID символа')
    interval: str = Field(..., description='Интервал для расчета RSI')
    timeperiod: int = Field(default=14, description='Период для расчета RSI')


class SmoothedMetadata(DataFrameMetadata):
    """Метаданные для сглаженных данных."""

    symbol_id: SymbolId = Field(..., description='ID символа')
    level: str = Field(..., description='Уровень сглаживания')
    min_trade_id: int = Field(..., description='Минимальный ID сделки')
    max_trade_id: int = Field(..., description='Максимальный ID сделки')


class LinesMetadata(DataFrameMetadata):
    """Метаданные для линий."""

    symbol_id: SymbolId = Field(..., description='ID символа')
    level: str = Field(..., description='Уровень сглаживания')
    min_trade_id: int = Field(..., description='Минимальный ID сделки')
    max_trade_id: int = Field(..., description='Максимальный ID сделки')


class ExtremeLinesMetadata(DataFrameMetadata):
    """Метаданные для экстремальных линий."""

    symbol_id: SymbolId = Field(..., description='ID символа')
    width: int = Field(..., description='Ширина массива')
    height: int = Field(..., description='Высота массива')
    scale: float = Field(..., description='Масштаб')
    min_trade_id: int = Field(..., description='Минимальный ID сделки')
    min_price: float = Field(..., description='Минимальная цена')


class OrderBookVolumesMetadata(DataFrameMetadata):
    """Метаданные для объемов стакана."""

    symbol_id: SymbolId = Field(..., description='ID символа')
    width: int = Field(..., description='Ширина массива')
    height: int = Field(..., description='Высота массива')
    scale: float = Field(..., description='Масштаб')
    min_trade_id: int = Field(..., description='Минимальный ID сделки')
    min_price: float = Field(..., description='Минимальная цена')


class VelocityMetadata(DataFrameMetadata):
    """Метаданные для данных скорости."""

    symbol_id: SymbolId = Field(..., description='ID символа')
    interval: str = Field(..., description='Интервал для расчета скорости')


class SymbolMetadata(BaseModel):
    """Общие метаданные символа."""

    symbol_id: SymbolId = Field(..., description='ID символа')
    symbol_name: str = Field(..., description='Название символа')
    last_updated: datetime = Field(..., description='Время последнего обновления')
    has_trades_data: bool = Field(default=False, description='Есть ли данные о сделках')
    has_bollinger: bool = Field(default=False, description='Есть ли полосы Боллинджера')
    has_candles: bool = Field(default=False, description='Есть ли свечные данные')
    has_rsi: bool = Field(default=False, description='Есть ли RSI данные')
    has_smoothed: bool = Field(default=False, description='Есть ли сглаженные данные')
    has_extreme_lines: bool = Field(
        default=False, description='Есть ли экстремальные линии'
    )
    has_order_book_volumes: bool = Field(
        default=False, description='Есть ли объемы стакана'
    )
    has_velocity: bool = Field(default=False, description='Есть ли данные скорости')


class ProcessingStatus(BaseModel):
    """Статус обработки данных."""

    symbol_id: SymbolId = Field(..., description='ID символа')
    status: str = Field(..., description='Статус обработки')
    last_processed: datetime | None = Field(
        None, description='Время последней обработки'
    )
    error_message: str | None = Field(None, description='Сообщение об ошибке')
    processing_time_seconds: float | None = Field(
        None, description='Время обработки в секундах'
    )
