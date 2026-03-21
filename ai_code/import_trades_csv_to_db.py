#!/usr/bin/env python3
"""
Импорт трейдов из CSV-файлов в таблицу okx_trade_data_2.

Скрипт:
- читает файлы data/*.csv (только верхний уровень);
- ожидает имена формата <INSTRUMENT>-trades-YYYY-MM.csv;
- валидирует соответствие instrument_name из строки имени файла;
- вставляет записи батчами с ON CONFLICT DO NOTHING.
"""

import asyncio
import csv
import logging
import traceback
from decimal import (
    Decimal,
)
from pathlib import (
    Path,
)
from re import (
    fullmatch,
)

from sqlalchemy.dialects.postgresql import (
    insert,
)
from sqlalchemy.ext.asyncio import (
    async_sessionmaker,
    create_async_engine,
)

from enumerations import (
    SymbolId,
)
from main.save_trades.schemas import (
    OKXTradeData2,
)
from settings import (
    settings,
)

try:
    import uvloop
except ImportError:
    uvloop = asyncio

logger = logging.getLogger(__name__)

_DATA_DIR = Path('data')
_CSV_GLOB_PATTERN = '*.csv'
_BATCH_SIZE = 10_000

_SYMBOL_ID_BY_INSTRUMENT_NAME = {
    'BTC-USDT': SymbolId.BTC_USDT,
    'ETH-USDT': SymbolId.ETH_USDT,
}

_EXPECTED_CSV_COLUMNS = (
    'instrument_name',
    'trade_id',
    'side',
    'price',
    'size',
    'created_time',
)


def _build_database_url() -> str:
    return (
        'postgresql+asyncpg'
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


def _instrument_name_from_file_name(file_name: str) -> str:
    match = fullmatch(
        r'([A-Z]+-[A-Z]+)-trades-\d{4}-\d{2}\.csv',
        file_name,
    )
    assert match is not None, file_name
    return match.group(1)


def _parse_trade_row(
    row: dict[str, str],
    expected_instrument_name: str,
    file_name: str,
) -> dict[str, object]:
    instrument_name = row['instrument_name']
    assert instrument_name == expected_instrument_name, (
        file_name,
        expected_instrument_name,
        instrument_name,
    )

    symbol_id = _SYMBOL_ID_BY_INSTRUMENT_NAME[instrument_name]

    trade_id = int(
        row['trade_id'],
    )

    side = row['side']
    assert side in ('BUY', 'SELL'), (
        file_name,
        side,
    )

    is_buy = (side == 'BUY')

    price = Decimal(
        row['price'],
    )
    quantity = Decimal(
        row['size'],
    )
    timestamp_ms = int(
        row['created_time'],
    )

    return {
        'symbol_id': symbol_id,
        'trade_id': trade_id,
        'is_buy': is_buy,
        'price': price,
        'quantity': quantity,
        'timestamp_ms': timestamp_ms,
    }


async def _flush_batch(
    session_maker: async_sessionmaker,
    rows_batch: list[dict[str, object]],
) -> int:
    if not rows_batch:
        return 0

    statement = insert(
        OKXTradeData2,
    ).on_conflict_do_nothing(
        index_elements=[
            OKXTradeData2.symbol_id,
            OKXTradeData2.trade_id,
        ],
    )

    async with session_maker() as session:
        async with session.begin():
            result = await session.execute(
                statement,
                rows_batch,
            )

    return int(
        result.rowcount or 0,
    )


async def _import_file(
    session_maker: async_sessionmaker,
    csv_path: Path,
) -> tuple[int, int]:
    expected_instrument_name = _instrument_name_from_file_name(
        csv_path.name,
    )

    parsed_rows_count = 0
    inserted_rows_count = 0
    rows_batch: list[dict[str, object]] = []

    with csv_path.open(
        mode='r',
        encoding='utf-8',
        newline='',
    ) as csv_file:
        reader = csv.DictReader(
            csv_file,
        )

        assert reader.fieldnames is not None, csv_path
        assert tuple(reader.fieldnames) == _EXPECTED_CSV_COLUMNS, (
            csv_path,
            reader.fieldnames,
        )

        for row in reader:
            parsed_rows_count += 1
            rows_batch.append(
                _parse_trade_row(
                    row=row,
                    expected_instrument_name=expected_instrument_name,
                    file_name=csv_path.name,
                )
            )

            if len(rows_batch) >= _BATCH_SIZE:
                inserted_rows_count += await _flush_batch(
                    session_maker=session_maker,
                    rows_batch=rows_batch,
                )
                rows_batch.clear()

    if rows_batch:
        inserted_rows_count += await _flush_batch(
            session_maker=session_maker,
            rows_batch=rows_batch,
        )

    return parsed_rows_count, inserted_rows_count


async def main() -> None:
    logging.basicConfig(
        encoding='utf-8',
        format='[%(levelname)s][%(asctime)s][%(name)s]: %(message)s',
        level=logging.INFO,
    )

    assert _DATA_DIR.exists(), _DATA_DIR
    assert _DATA_DIR.is_dir(), _DATA_DIR

    csv_paths = sorted(
        _DATA_DIR.glob(
            _CSV_GLOB_PATTERN,
        )
    )

    logger.info(
        'Found %s CSV files in %s',
        len(csv_paths),
        _DATA_DIR,
    )

    database_url = _build_database_url()

    engine = create_async_engine(
        database_url,
        echo=False,
    )
    session_maker = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )

    total_parsed_rows = 0
    total_inserted_rows = 0

    try:
        for csv_path in csv_paths:
            logger.info('Processing file: %s', csv_path)

            file_parsed_rows, file_inserted_rows = await _import_file(
                session_maker=session_maker,
                csv_path=csv_path,
            )

            total_parsed_rows += file_parsed_rows
            total_inserted_rows += file_inserted_rows

            logger.info(
                'File done: %s (parsed=%s, inserted=%s)',
                csv_path.name,
                file_parsed_rows,
                file_inserted_rows,
            )
    finally:
        await engine.dispose()

    logger.info(
        'Import done: parsed=%s, inserted=%s, skipped_as_duplicates=%s',
        total_parsed_rows,
        total_inserted_rows,
        total_parsed_rows - total_inserted_rows,
    )


if __name__ == '__main__':
    try:
        uvloop.run(
            main(),
        )
    except Exception as exception:
        logger.error(
            'Import failed: %s',
            ''.join(traceback.format_exception(exception)),
        )
        raise
