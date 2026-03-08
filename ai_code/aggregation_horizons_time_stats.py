#!/usr/bin/env python3
"""
Скрипт подсчёта статистики по времени для агрегированных горизонтов.

Загружает сделки из okx_data_set_record_data_2 (Polars), для каждого горизонта
x2, x4, …, x1024 строит скользящие окна (шаг 1), вычисляет дельту времени окна
(end_timestamp_ms последней − start_timestamp_ms первой), по списку дельт считает
min, max, std, avg. Выводит статистику в мс, секундах и минутах.
"""

import argparse
import logging
import sys
import traceback

import polars

from enumerations import SymbolId
from main.save_final_data_set_2.schemas import OKXDataSetRecordData_2
from settings import settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

HORIZONS = [
    ('x2', 2),
    ('x4', 4),
    ('x8', 8),
    ('x16', 16),
    ('x32', 32),
    ('x64', 64),
    ('x128', 128),
    ('x256', 256),
    ('x512', 512),
    ('x1024', 1024),
]

MS_PER_SECOND = 1000
MS_PER_MINUTE = 60 * MS_PER_SECOND


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            'Compute time-delta statistics per aggregation horizon '
            'from okx_data_set_record_data_2 (symbol_id, offset, limit).'
        ),
    )
    parser.add_argument(
        'symbol_id',
        type=str,
        help='Symbol id, e.g. BTC_USDT',
    )
    parser.add_argument(
        'offset',
        type=int,
        help='OFFSET for SQL query (row count to skip)',
    )
    parser.add_argument(
        'limit',
        type=int,
        help='LIMIT for SQL query (max row count to fetch)',
    )
    return parser.parse_args()


def _load_trades(symbol_id: SymbolId, offset: int, limit: int) -> polars.DataFrame:
    uri = (
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
    query = (
        'SELECT'
        ' symbol_id'
        ', start_trade_id'
        ', start_timestamp_ms'
        ', end_timestamp_ms'
        f' FROM {OKXDataSetRecordData_2.__tablename__}'
        ' WHERE'
        f' symbol_id = {symbol_id.name!r}'
        ' ORDER BY'
        ' symbol_id ASC'
        ', start_trade_id ASC'
        f' OFFSET {offset}'
        f' LIMIT {limit}'
        ';'
    )
    logger.info('Fetching rows from database (connectorx)...')
    df = polars.read_database_uri(
        engine='connectorx',
        query=query,
        uri=uri,
    )
    logger.info(f'Loaded {df.height} rows')
    return df


def _compute_horizon_deltas(
    df: polars.DataFrame,
    window_size: int,
) -> polars.Series:
    shift = window_size - 1
    delta_ms = (
        polars.col('end_timestamp_ms')
        - polars.col('start_timestamp_ms').shift(shift)
    )
    with_delta = df.with_columns(delta_ms.alias('delta_ms'))
    return with_delta.filter(polars.col('delta_ms').is_not_null())['delta_ms']


def _stats_for_series(s: polars.Series) -> tuple[float, float, float, float]:
    return (
        float(s.min()),
        float(s.max()),
        float(s.std()),
        float(s.mean()),
    )


def _print_stats(
    horizon_name: str,
    min_ms: float,
    max_ms: float,
    std_ms: float,
    avg_ms: float,
    n: int,
) -> None:
    print(f'  n_windows = {n}')
    print('  ms:       min = {:.2f}, max = {:.2f}, std = {:.2f}, avg = {:.2f}'.format(
        min_ms, max_ms, std_ms, avg_ms,
    ))
    print('  seconds:  min = {:.4f}, max = {:.4f}, std = {:.4f}, avg = {:.4f}'.format(
        min_ms / MS_PER_SECOND,
        max_ms / MS_PER_SECOND,
        std_ms / MS_PER_SECOND,
        avg_ms / MS_PER_SECOND,
    ))
    print('  minutes:  min = {:.4f}, max = {:.4f}, std = {:.4f}, avg = {:.4f}'.format(
        min_ms / MS_PER_MINUTE,
        max_ms / MS_PER_MINUTE,
        std_ms / MS_PER_MINUTE,
        avg_ms / MS_PER_MINUTE,
    ))


def main() -> None:
    args = _parse_args()
    try:
        symbol_id = SymbolId[args.symbol_id]
    except KeyError:
        valid = [s.name for s in SymbolId]
        logger.error(f'Invalid symbol_id {args.symbol_id!r}. Valid: {valid}')
        sys.exit(1)
    if args.offset < 0:
        logger.error('offset must be >= 0')
        sys.exit(1)
    if args.limit <= 0:
        logger.error('limit must be > 0')
        sys.exit(1)

    df = _load_trades(symbol_id, args.offset, args.limit)
    if df.height == 0:
        logger.warning('No rows loaded, nothing to compute')
        return

    print(f'Aggregation horizons time statistics (symbol_id={symbol_id.name}, '
          f'offset={args.offset}, limit={args.limit}, rows={df.height})')
    print()

    for horizon_name, window_size in HORIZONS:
        if df.height < window_size:
            print(f'{horizon_name} (window_size={window_size}): skip (rows < window_size)')
            print()
            continue
        deltas = _compute_horizon_deltas(df, window_size)
        n = len(deltas)
        min_ms, max_ms, std_ms, avg_ms = _stats_for_series(deltas)
        print(f'{horizon_name} (window_size={window_size}):')
        _print_stats(horizon_name, min_ms, max_ms, std_ms, avg_ms, n)
        print()


if __name__ == '__main__':
    try:
        main()
    except Exception as exception:
        logger.error(
            f'Handled exception: {"".join(traceback.format_exception(exception))}'
        )
        sys.exit(1)
