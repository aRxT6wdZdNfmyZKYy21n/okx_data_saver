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

from sqlalchemy import (
    select,
)
from sqlalchemy.ext.asyncio import (
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


_BATCH_SIZE = 10_000


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
    """Выполняет миграцию okx_trade_data_2 -> okx_trade_data_3 батчами по _BATCH_SIZE записей."""
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
    total_errors = 0

    for symbol_id in SymbolConstants.NameById:
        logger.info('Обрабатываем symbol_id=%s...', symbol_id)
        last_trade_id = None
        batch_num = 0

        while True:
            batch_num += 1
            query = (
                select(OKXTradeData2)
                .where(OKXTradeData2.symbol_id == symbol_id)
                .order_by(OKXTradeData2.trade_id)
                .limit(_BATCH_SIZE)
            )
            if last_trade_id is not None:
                query = query.where(OKXTradeData2.trade_id > last_trade_id)

            async with session_factory() as session:
                result = await session.execute(query)
                batch = result.scalars().all()

            if not batch:
                break

            batch_trade_ids = [row.trade_id for row in batch]
            last_trade_id = batch_trade_ids[-1]

            async with session_factory() as session:
                existing = await session.execute(
                    select(OKXTradeData3.trade_id).where(
                        OKXTradeData3.symbol_id == symbol_id,
                        OKXTradeData3.trade_id.in_(batch_trade_ids),
                    )
                )
                existing_ids = {row[0] for row in existing.all()}

            to_insert = [row for row in batch if row.trade_id not in existing_ids]
            if not to_insert:
                if len(batch) < _BATCH_SIZE:
                    break
                continue

            async with session_factory() as session:
                try:
                    for row in to_insert:
                        session.add(
                            OKXTradeData3(
                                symbol_id=row.symbol_id,
                                trade_id=row.trade_id,
                                is_buy=row.is_buy,
                                price=row.price,
                                quantity=row.quantity,
                                timestamp_ms=row.timestamp_ms,
                            )
                        )
                    await session.commit()
                    total_migrated += len(to_insert)
                    logger.info(
                        'symbol_id=%s, батч %s: перенесено %s из %s записей (всего по символу в этом запуске: см. итог)',
                        symbol_id,
                        batch_num,
                        len(to_insert),
                        len(batch),
                    )
                except Exception as exception:
                    total_errors += len(to_insert)
                    logger.error(
                        'Ошибка при записи батча symbol_id=%s, батч %s: %s',
                        symbol_id,
                        batch_num,
                        ''.join(traceback.format_exception(exception)),
                    )

            if len(batch) < _BATCH_SIZE:
                break

    await engine.dispose()
    logger.info(
        'Миграция завершена. Перенесено: %s, ошибок: %s',
        total_migrated,
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
