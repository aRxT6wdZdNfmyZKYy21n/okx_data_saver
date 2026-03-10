"""
Агрегация баров по масштабу (x2, x4, ... x2048) для веб-GUI.

Правила как в trading_bot_2 CandleAggregator: OHLC first/last/max/min, volume/count sum.
"""

import logging

import polars

logger = logging.getLogger(__name__)

# Колонки для агрегации
OHLC_FIRST = {'open_price', 'start_timestamp_ms', 'start_trade_id'}
OHLC_LAST = {'close_price', 'end_timestamp_ms', 'end_trade_id'}
OHLC_MAX = {'high_price'}
OHLC_MIN = {'low_price'}
VOLUME_SUM = {'total_volume', 'buy_volume', 'total_quantity', 'buy_quantity', 'total_trades_count', 'buy_trades_count'}


def aggregate_bars(df: polars.DataFrame, multiplier: int) -> polars.DataFrame:
    """
    Агрегирует бары: каждые `multiplier` подряд идущих строк в одну.
    Группировка по index // multiplier. Ожидается порядок по start_trade_id ASC.
    """
    if multiplier <= 1:
        return df

    df = df.with_columns((polars.int_range(polars.len()) // multiplier).alias('_group_id'))

    first_cols = [c for c in OHLC_FIRST if c in df.columns]
    last_cols = [c for c in OHLC_LAST if c in df.columns]
    max_cols = [c for c in OHLC_MAX if c in df.columns]
    min_cols = [c for c in OHLC_MIN if c in df.columns]
    sum_cols = [c for c in VOLUME_SUM if c in df.columns]

    agg_exprs = (
        [polars.col(c).first() for c in first_cols]
        + [polars.col(c).last() for c in last_cols]
        + [polars.col(c).max() for c in max_cols]
        + [polars.col(c).min() for c in min_cols]
        + [polars.col(c).sum() for c in sum_cols]
    )
    if 'symbol_id' in df.columns:
        agg_exprs.append(polars.col('symbol_id').first())

    out = df.group_by('_group_id').agg(agg_exprs).sort('_group_id').drop('_group_id')
    return out
