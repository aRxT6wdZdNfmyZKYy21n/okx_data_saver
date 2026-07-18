"""
Сервис данных для веб-GUI: загрузка последних N баров из БД, вычисляемые поля, агрегация по масштабу.
"""

from __future__ import annotations

import asyncio
import logging
import traceback

import polars

from enumerations import SymbolId
from main.save_final_data_set_3.schemas import OKXDataSetRecordData_3
from main.web_gui.aggregation import aggregate_bars
from main.web_gui.async_data_runtime import run_async_data
from main.web_gui.bars_cache_service import fetch_last_bars_with_redis_cache
from main.web_gui.constants import scale_to_multiplier
from settings import settings

logger = logging.getLogger(__name__)


def _db_uri() -> str:
    return (
        'postgresql'
        '://'
        f'{settings.POSTGRES_DB_USER_NAME}'
        ':'
        f'{settings.POSTGRES_DB_PASSWORD.get_secret_value()}'
        '@'
        f'{settings.POSTGRES_DB_HOST_NAME}'
        ':'
        f'{settings.POSTGRES_DB_PORT}'
        '/'
        f'{settings.POSTGRES_DB_NAME}'
    )


def _cast_bars_dataframe_types(df: polars.DataFrame) -> polars.DataFrame:
    return df.with_columns([
        polars.col('open_price').cast(polars.Float64),
        polars.col('high_price').cast(polars.Float64),
        polars.col('low_price').cast(polars.Float64),
        polars.col('close_price').cast(polars.Float64),
        polars.col('total_volume').cast(polars.Float64),
        polars.col('buy_volume').cast(polars.Float64),
    ])


def _fetch_last_bars_from_db_sync(
    symbol_id: SymbolId,
    limit: int,
    offset: int,
) -> polars.DataFrame | None:
    total = limit + offset
    logger.info(
        'DB read start: fetch_last_bars symbol=%s limit=%d offset=%d',
        symbol_id.name,
        limit,
        offset,
    )
    query = f"""
    SELECT
        symbol_id, start_trade_id, end_trade_id,
        start_timestamp_ms, end_timestamp_ms,
        open_price, high_price, low_price, close_price,
        total_volume, buy_volume, total_quantity, buy_quantity,
        total_trades_count, buy_trades_count
    FROM (
        SELECT *
        FROM {OKXDataSetRecordData_3.__tablename__}
        WHERE symbol_id = '{symbol_id.name}'
        ORDER BY start_trade_id DESC
        LIMIT {total}
    ) AS sub
    ORDER BY start_trade_id ASC
    OFFSET {offset}
    LIMIT {limit}
    """
    try:
        df = polars.read_database_uri(engine='connectorx', query=query, uri=_db_uri())
    except Exception as exception:
        logger.error(
            'Failed to fetch bars: %s',
            ''.join(traceback.format_exception(exception)),
        )
        return None

    if df.height == 0:
        logger.info(
            'DB read done: fetch_last_bars symbol=%s rows=0',
            symbol_id.name,
        )
        return None

    logger.info(
        'DB read done: fetch_last_bars symbol=%s rows=%d',
        symbol_id.name,
        int(df.height),
    )
    return _cast_bars_dataframe_types(df)


async def _fetch_last_bars_from_db(
    symbol_id: SymbolId,
    limit: int,
    offset: int,
) -> polars.DataFrame | None:
    return await asyncio.to_thread(
        _fetch_last_bars_from_db_sync,
        symbol_id,
        limit,
        offset,
    )


async def fetch_last_bars(
    symbol_id: SymbolId,
    limit: int,
    offset: int,
) -> polars.DataFrame | None:
    """
    Загружает последние `limit` баров для символа (по start_trade_id DESC), возвращает в порядке ASC.
    Пагинация: offset — сколько последних баров пропустить (0 = самые свежие).
    """
    return await fetch_last_bars_with_redis_cache(
        symbol_id=symbol_id,
        limit=limit,
        offset=offset,
        load_from_db=lambda: _fetch_last_bars_from_db(
            symbol_id=symbol_id,
            limit=limit,
            offset=offset,
        ),
    )


def fetch_last_bars_sync(
    symbol_id: SymbolId,
    limit: int,
    offset: int,
) -> polars.DataFrame | None:
    return run_async_data(
        lambda: fetch_last_bars(
            symbol_id=symbol_id,
            limit=limit,
            offset=offset,
        ),
    )


def add_computed_columns(df: polars.DataFrame) -> polars.DataFrame:
    """Добавляет buy_volume_percent, sell_volume_percent, total_volume_log2."""
    assert (df['total_volume'] > 0).all(), 'total_volume must be strictly positive (data error if 0)'
    total = polars.col('total_volume')
    buy_pct = polars.col('buy_volume') / total
    return df.with_columns([
        buy_pct.alias('buy_volume_percent'),
        (1 - buy_pct).alias('sell_volume_percent'),
        total.log(2).alias('total_volume_log2'),
    ])


async def _count_x1_bars_since_entry_from_db(
    symbol_id: SymbolId,
    entry_start_trade_id: int,
) -> int | None:
    return await asyncio.to_thread(
        _count_x1_bars_since_entry_from_db_sync,
        symbol_id,
        entry_start_trade_id,
    )


def _count_x1_bars_since_entry_from_db_sync(
    symbol_id: SymbolId,
    entry_start_trade_id: int,
) -> int | None:
    logger.info(
        'DB read start: count_x1_bars_since_entry symbol=%s entry_start_trade_id=%d',
        symbol_id.name,
        entry_start_trade_id,
    )
    query = f"""
    SELECT COUNT(*) AS bar_count
    FROM {OKXDataSetRecordData_3.__tablename__}
    WHERE symbol_id = '{symbol_id.name}'
      AND start_trade_id >= {entry_start_trade_id}
    """
    try:
        df = polars.read_database_uri(engine='connectorx', query=query, uri=_db_uri())
    except Exception as exception:
        logger.error(
            'Failed to count bars since entry: %s',
            ''.join(traceback.format_exception(exception)),
        )
        return None
    if df.height == 0:
        logger.info(
            'DB read done: count_x1_bars_since_entry symbol=%s bar_count=0',
            symbol_id.name,
        )
        return None
    bar_count = int(df['bar_count'][0])
    logger.info(
        'DB read done: count_x1_bars_since_entry symbol=%s bar_count=%d',
        symbol_id.name,
        bar_count,
    )
    return bar_count


async def count_x1_bars_since_entry(
    symbol_id: SymbolId,
    entry_start_trade_id: int,
) -> int | None:
    return await _count_x1_bars_since_entry_from_db(
        symbol_id=symbol_id,
        entry_start_trade_id=entry_start_trade_id,
    )


def count_x1_bars_since_entry_sync(
    symbol_id: SymbolId,
    entry_start_trade_id: int,
) -> int | None:
    return run_async_data(
        lambda: count_x1_bars_since_entry(
            symbol_id=symbol_id,
            entry_start_trade_id=entry_start_trade_id,
        ),
    )


async def get_bars_for_api(
    symbol_id: SymbolId,
    limit: int,
    offset: int,
    scale: str,
) -> polars.DataFrame | None:
    """
    Возвращает DataFrame для сериализации в API: последние бары с вычисляемыми полями и агрегацией.
    """
    raw = await fetch_last_bars(symbol_id=symbol_id, limit=limit, offset=offset)
    if raw is None:
        return None

    mult = scale_to_multiplier(scale)
    if mult > 1:
        raw = aggregate_bars(raw, mult)

    return add_computed_columns(raw)


def get_bars_for_api_sync(
    symbol_id: SymbolId,
    limit: int,
    offset: int,
    scale: str,
) -> polars.DataFrame | None:
    return run_async_data(
        lambda: get_bars_for_api(
            symbol_id=symbol_id,
            limit=limit,
            offset=offset,
            scale=scale,
        ),
    )
