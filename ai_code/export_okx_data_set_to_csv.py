#!/usr/bin/env python3
"""
Скрипт экспорта строк из таблицы okx_data_set_record_data_2 в CSV.

Читает строки с заданным symbol_id (OFFSET, LIMIT), сохраняет в файл
вида {symbol_prefix}_{offset_M}M_to_{end_M}M.csv (числа в миллионах).
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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Export rows from okx_data_set_record_data_2 to CSV by symbol_id, offset, limit.',
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
    args = parser.parse_args()
    return args


def _symbol_prefix(symbol_id: SymbolId) -> str:
    return symbol_id.name.split('_')[0].lower()


def _output_filename(symbol_id: SymbolId, offset: int, limit: int) -> str:
    prefix = _symbol_prefix(symbol_id)
    start_m = offset // 1_000_000
    end_m = (offset + limit) // 1_000_000
    return f'{prefix}_{start_m}M_to_{end_m}M.csv'


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
        ', buy_quantity'
        ', buy_trades_count'
        ', buy_volume'
        ', close_price'
        ', end_timestamp_ms'
        ', end_trade_id'
        ', high_price'
        ', low_price'
        ', open_price'
        ', start_timestamp_ms'
        ', total_quantity'
        ', total_trades_count'
        ', total_volume'
        f' FROM {OKXDataSetRecordData_2.__tablename__}'
        ' WHERE'
        f' symbol_id = {symbol_id.name!r}'
        ' ORDER BY'
        ' symbol_id ASC'
        ', start_trade_id ASC'
        f' OFFSET {args.offset}'
        f' LIMIT {args.limit}'
        ';'
    )
    logger.info('Fetching rows from database (connectorx)...')
    df = polars.read_database_uri(
        engine='connectorx',
        query=query,
        uri=uri,
    )
    out_path = _output_filename(symbol_id, args.offset, args.limit)
    logger.info(f'Writing {df.height} rows to {out_path}')
    df.write_csv(out_path)
    logger.info('Done.')


if __name__ == '__main__':
    try:
        main()
    except Exception as exception:
        logger.error(
            f'Handled exception: {"".join(traceback.format_exception(exception))}'
        )
        sys.exit(1)
