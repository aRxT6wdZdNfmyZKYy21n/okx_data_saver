#!/usr/bin/env python3
"""
Скрипт миграции данных из таблицы okx_trade_data_2 в okx_trade_data_3.

Переносит только те записи, которые есть в okx_trade_data_2 и отсутствуют
в okx_trade_data_3 (по составному ключу symbol_id, trade_id).
Один процесс, последовательная миграция.
"""

import asyncio
import logging
import sys
import traceback

import numpy
from sqlalchemy import (
    and_,
    func,
    select,
)
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

try:
    import uvloop
except ImportError:
    uvloop = asyncio

from constants.symbol import (
    SymbolConstants,
)
from main.save_trades.schemas import (
    OKXTradeData2,
    OKXTradeData3,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)


_COMMIT_COUNT = 10_000
_YIELD_PER = 10_000


async def ensure_table_okx_trade_data_3(engine):
    """Создаёт таблицу okx_trade_data_3, если её ещё нет."""
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: OKXTradeData3.__table__.create(
                sync_conn,
                checkfirst=True,
            ),
        )
    logger.info('Таблица okx_trade_data_3 готова (создана или уже существовала)')


async def run_migration(database_url: str) -> int:
    """Выполняет миграцию okx_trade_data_2 -> okx_trade_data_3. Возвращает число перенесённых записей."""
    engine = create_async_engine(
        database_url,
        echo=True,
    )
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )

    await ensure_table_okx_trade_data_3(engine)

    total_migrated = 0
    total_skipped = 0
    total_errors = 0

    for symbol_id in SymbolConstants.NameById:
        logger.info('Обрабатываем symbol_id=%s...', symbol_id)

        async with session_factory() as session:
            total_result = await session.execute(
                select(func.count())
                .select_from(OKXTradeData2)
                .where(OKXTradeData2.symbol_id == symbol_id)
            )
            count_2 = total_result.scalar()
            total_result = await session.execute(
                select(func.count())
                .select_from(OKXTradeData3)
                .where(OKXTradeData3.symbol_id == symbol_id)
            )
            count_3 = total_result.scalar()

        trade_ids_2 = numpy.empty(shape=(count_2,), dtype=numpy.int64)
        idx = 0
        async with session_factory() as session:
            result = await session.stream(
                select(OKXTradeData2.trade_id)
                .where(OKXTradeData2.symbol_id == symbol_id)
                .execution_options(yield_per=_YIELD_PER)
            )
            async for row in result:
                trade_ids_2[idx] = row.trade_id
                idx += 1
        if idx < trade_ids_2.size:
            trade_ids_2.resize((idx,))

        trade_ids_3 = numpy.empty(shape=(count_3,), dtype=numpy.int64)
        idx = 0
        async with session_factory() as session:
            result = await session.stream(
                select(OKXTradeData3.trade_id)
                .where(OKXTradeData3.symbol_id == symbol_id)
                .execution_options(yield_per=_YIELD_PER)
            )
            async for row in result:
                trade_ids_3[idx] = row.trade_id
                idx += 1
        if idx < trade_ids_3.size:
            trade_ids_3.resize((idx,))

        to_migrate = numpy.setdiff1d(trade_ids_2, trade_ids_3)
        logger.info(
            'symbol_id=%s: в _2 записей=%s, в _3 записей=%s, к переносу=%s',
            symbol_id,
            trade_ids_2.size,
            trade_ids_3.size,
            to_migrate.size,
        )

        migrated = 0
        async with session_factory() as session:
            for trade_id in to_migrate:
                trade_id = int(trade_id)
                try:
                    result = await session.execute(
                        select(OKXTradeData2).where(
                            and_(
                                OKXTradeData2.symbol_id == symbol_id,
                                OKXTradeData2.trade_id == trade_id,
                            )
                        )
                    )
                    row_2 = result.scalar_one_or_none()
                    if row_2 is None:
                        total_skipped += 1
                        continue
                    session.add(
                        OKXTradeData3(
                            symbol_id=row_2.symbol_id,
                            trade_id=row_2.trade_id,
                            is_buy=row_2.is_buy,
                            price=row_2.price,
                            quantity=row_2.quantity,
                            timestamp_ms=row_2.timestamp_ms,
                        )
                    )
                    migrated += 1
                    total_migrated += 1
                    if migrated % _COMMIT_COUNT == 0:
                        await session.commit()
                        logger.info('Зафиксировано %s записей для symbol_id=%s', migrated, symbol_id)
                except Exception as exception:
                    total_errors += 1
                    logger.error(
                        'Ошибка при переносе trade_id=%s, symbol_id=%s: %s',
                        trade_id,
                        symbol_id,
                        ''.join(traceback.format_exception(exception)),
                    )
                    continue
            if migrated % _COMMIT_COUNT != 0 or migrated > 0:
                await session.commit()

    await engine.dispose()
    logger.info(
        'Миграция завершена. Перенесено: %s, пропущено: %s, ошибок: %s',
        total_migrated,
        total_skipped,
        total_errors,
    )
    return total_migrated


async def main():
    """Точка входа: читает настройки и запускает миграцию."""
    try:
        from settings import settings

        database_url = (
            f'postgresql+asyncpg://{settings.POSTGRES_DB_USER_NAME}:'
            f'{settings.POSTGRES_DB_PASSWORD.get_secret_value()}@'
            f'{settings.POSTGRES_DB_HOST_NAME}:'
            f'{settings.POSTGRES_DB_PORT}/'
            f'{settings.POSTGRES_DB_NAME}'
        )
        logger.info(
            'Подключение к БД: %s:%s/%s',
            settings.POSTGRES_DB_HOST_NAME,
            settings.POSTGRES_DB_PORT,
            settings.POSTGRES_DB_NAME,
        )
    except Exception as exception:
        logger.error(
            'Ошибка при получении настроек БД: %s',
            ''.join(traceback.format_exception(exception)),
        )
        logger.error(
            'Убедитесь, что в .env заданы: POSTGRES_DB_HOST_NAME, POSTGRES_DB_NAME, '
            'POSTGRES_DB_PORT, POSTGRES_DB_PASSWORD, POSTGRES_DB_USER_NAME'
        )
        sys.exit(1)

    await run_migration(database_url)


if __name__ == '__main__':
    uvloop.run(main())
