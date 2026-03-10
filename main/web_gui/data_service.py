"""
Сервис данных для веб-GUI: загрузка последних N баров из БД, вычисляемые поля, агрегация по масштабу.
"""

import logging

import polars

from enumerations import SymbolId
from main.save_final_data_set_2.schemas import OKXDataSetRecordData_2
from main.web_gui.aggregation import aggregate_bars
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


def fetch_last_bars(
    symbol_id: SymbolId,
    limit: int,
    offset: int = 0,
) -> polars.DataFrame | None:
    """
    Загружает последние `limit` баров для символа (по start_trade_id DESC), возвращает в порядке ASC.
    Пагинация: offset — сколько последних баров пропустить (0 = самые свежие).
    """
    # Подзапрос: последние (offset+limit) баров DESC; внешний запрос — ASC, пропуск offset, взять limit
    total = limit + offset
    query = f"""
    SELECT
        symbol_id, start_trade_id, end_trade_id,
        start_timestamp_ms, end_timestamp_ms,
        open_price, high_price, low_price, close_price,
        total_volume, buy_volume, total_quantity, buy_quantity,
        total_trades_count, buy_trades_count
    FROM (
        SELECT *
        FROM {OKXDataSetRecordData_2.__tablename__}
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
            ''.join(__import__('traceback').format_exception(exception)),
        )
        return None

    if df.height == 0:
        return None

    # Приводим типы
    df = df.with_columns([
        polars.col('open_price').cast(polars.Float64),
        polars.col('high_price').cast(polars.Float64),
        polars.col('low_price').cast(polars.Float64),
        polars.col('close_price').cast(polars.Float64),
        polars.col('total_volume').cast(polars.Float64),
        polars.col('buy_volume').cast(polars.Float64),
    ])
    return df


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


def get_bars_for_api(
    symbol_id: SymbolId,
    limit: int,
    offset: int = 0,
    scale: str = 'x1',
) -> polars.DataFrame | None:
    """
    Возвращает DataFrame для сериализации в API: последние бары с вычисляемыми полями и агрегацией.
    """
    raw = fetch_last_bars(symbol_id=symbol_id, limit=limit, offset=offset)
    if raw is None:
        return None

    mult = scale_to_multiplier(scale)
    if mult > 1:
        raw = aggregate_bars(raw, mult)

    return add_computed_columns(raw)
