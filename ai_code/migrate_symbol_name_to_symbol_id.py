#!/usr/bin/env python3
"""
Скрипт миграции для изменения формата хранения данных
с symbol_name (строка) на symbol_id (целое число).

Создает новые таблицы с постфиксом _2, используя symbol_id вместо symbol_name.
"""

import asyncio
import logging
import multiprocessing as mp

import numpy
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor

from sqlalchemy import (
    and_,
    func,
    select,
)
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
)

try:
    import uvloop
except ImportError:
    uvloop = asyncio

from constants.okx import OKXConstants
from constants.symbol import (
    SymbolConstants,
)
from main.save_candles.schemas import (
    Base as BaseOKXCandleData,
    OKXCandleData15m,
    OKXCandleData15m2,
    OKXCandleData1H,
    OKXCandleData1H2,
)
from main.save_order_books.schemas import (
    Base as BaseOKXOrderBookData,
    OKXOrderBookData,
    OKXOrderBookData2,
)
from main.save_trades.schemas import (
    Base as BaseOKXTradeData,
    OKXTradeData,
    OKXTradeData2,
)


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)


_COMMIT_COUNT = 10_000
_YIELD_PER = 10_000
_MAX_WORKERS = mp.cpu_count() - 2
_BATCH_SIZE = 100_000  # Размер батча для каждого процесса


def migrate_trade_data_batch(args):
    """Миграция батча данных торгов в отдельном процессе."""
    database_url, = args
    
    async def _migrate_batch():
        engine = create_async_engine(
            database_url,
            echo=True,
        )

        session_factory = async_sessionmaker(
            engine,
            expire_on_commit=False,
        )
        
        migrated_count = 0
        skipped_count = 0
        error_count = 0
        session_read: AsyncSession
        session_write: AsyncSession

        for symbol_name in SymbolConstants.IdByName:
            print(f'[PID {os.getpid()}]: Обрабатываем записи с symbol name {symbol_name!r}...')

            symbol_id = SymbolConstants.IdByName[symbol_name]

            async with session_factory() as session_read, session_factory() as session_write:
                # Получаем общее количество записей в таблице okx_trade_data

                total_result = await session_read.execute(
                    select(
                        func.count(),
                    ).select_from(
                        OKXTradeData,
                    ).where(
                        OKXTradeData.symbol_name == symbol_name,
                    )
                )

                total_rows = total_result.scalar()

                okx_trade_data_existent_trade_id_array = numpy.empty(
                    dtype=numpy.int64,
                    shape=(
                        total_rows,
                    ),
                )

                okx_trade_data_existent_trade_id_array_idx = 0

                # Получаем общее количество записей в таблице okx_order_book_data

                total_result = await session_read.execute(
                    select(
                        func.count(),
                    ).select_from(
                        OKXTradeData2,
                    ).where(
                        OKXTradeData2.symbol_id == symbol_id,
                    )
                )

                total_rows = total_result.scalar()

                okx_trade_data_2_existent_trade_id_array = numpy.empty(
                    dtype=numpy.int64,
                    shape=(
                        total_rows,
                    ),
                )

                okx_trade_data_2_existent_trade_id_array_idx = 0

                result = await session_read.stream(
                    select(
                        OKXTradeData.trade_id
                    ).where(
                        OKXTradeData.symbol_name == symbol_name,
                    ).limit(
                        okx_trade_data_existent_trade_id_array.size,
                    ).execution_options(
                        yield_per=_YIELD_PER
                    )
                )

                async for trade_data in result:
                    if okx_trade_data_existent_trade_id_array_idx % 1000 == 0:
                        print(trade_data, okx_trade_data_existent_trade_id_array_idx)

                    trade_id = trade_data.trade_id

                    okx_trade_data_existent_trade_id_array[okx_trade_data_existent_trade_id_array_idx] = trade_id

                    okx_trade_data_existent_trade_id_array_idx += 1

                assert okx_trade_data_existent_trade_id_array_idx == okx_trade_data_existent_trade_id_array.size, (okx_trade_data_existent_trade_id_array_idx, okx_trade_data_existent_trade_id_array.size)

                result = await session_read.stream(
                    select(
                        OKXTradeData2.trade_id
                    ).where(
                        OKXTradeData2.symbol_id == symbol_id,
                    ).limit(
                        okx_trade_data_2_existent_trade_id_array.size,
                    ).execution_options(
                        yield_per=_YIELD_PER
                    )
                )

                async for trade_data in result:
                    if okx_trade_data_2_existent_trade_id_array_idx % 1000 == 0:
                        print(trade_data, okx_trade_data_2_existent_trade_id_array_idx)

                    trade_id = trade_data.trade_id

                    okx_trade_data_2_existent_trade_id_array[okx_trade_data_existent_trade_id_array_idx] = trade_id

                    okx_trade_data_2_existent_trade_id_array_idx += 1

                assert okx_trade_data_2_existent_trade_id_array_idx == okx_trade_data_2_existent_trade_id_array.size, (okx_trade_data_2_existent_trade_id_array_idx, okx_trade_data_2_existent_trade_id_array.size)

                okx_trade_data_existent_trade_id_array_diff = numpy.setdiff1d(
                    okx_trade_data_existent_trade_id_array,
                    okx_trade_data_2_existent_trade_id_array,
                )

                print(f'[PID {os.getpid()}] Начинаем обработку {okx_trade_data_existent_trade_id_array_diff.size} записей торгов...')

                for trade_id in okx_trade_data_existent_trade_id_array_diff:
                    try:
                        # Получаем батч записей
                        result = await session_read.execute(
                            select(
                                OKXTradeData,
                            ).where(
                                and_(
                                    OKXTradeData.symbol_name == symbol_name,
                                    OKXTradeData.trade_id == trade_id,
                                )
                            )
                        )

                        trade_records = result.scalars().all()

                        for trade_data in trade_records:
                            try:
                                # Проверяем, что symbol_name существует в константах
                                if trade_data.symbol_name not in SymbolConstants.IdByName:
                                    print(f'[PID {os.getpid()}] Пропускаем запись с неизвестным symbol_name: {trade_data.symbol_name!r}')
                                    skipped_count += 1

                                    continue

                                symbol_id = SymbolConstants.IdByName[trade_data.symbol_name]

                                # Создаем новую запись
                                new_trade_data = OKXTradeData2(
                                    symbol_id=symbol_id,
                                    trade_id=trade_data.trade_id,
                                    is_buy=trade_data.is_buy,
                                    price=trade_data.price,
                                    quantity=trade_data.quantity,
                                    timestamp_ms=trade_data.timestamp_ms,
                                )

                                session_write.add(
                                    new_trade_data,
                                )

                                migrated_count += 1

                                if migrated_count % _COMMIT_COUNT == 0:
                                    await session_write.commit()
                                    print(f'[PID {os.getpid()}] Зафиксировано {migrated_count} записей торгов...')
                            except Exception as e:
                                error_count += 1
                                print(f'[PID {os.getpid()}] Ошибка при обработке записи trade_id={getattr(trade_data, "trade_id", "unknown")}: {str(e)}')
                                continue

                        # Коммитим оставшиеся записи в батче
                        await session_write.commit()
                    except Exception as e:
                        print(f'[PID {os.getpid()}] Ошибка при получении trade_id={trade_id}: {str(e)}')
                        error_count += 1
                        continue
        
        await engine.dispose()
        
        print(f'[PID {os.getpid()}] Миграция торгов завершена. Мигрировано: {migrated_count}, пропущено: {skipped_count}, ошибок: {error_count}')
        return migrated_count
    
    return uvloop.run(
        _migrate_batch(),
    )


def migrate_order_book_data_batch(args):
    """Миграция батча данных order book в отдельном процессе."""
    database_url, = args
    
    async def _migrate_batch():
        engine = create_async_engine(
            database_url,
            echo=True,
        )

        session_factory = async_sessionmaker(
            engine,
            expire_on_commit=False,
        )
        
        migrated_count = 0
        skipped_count = 0
        error_count = 0
        session_read: AsyncSession
        session_write: AsyncSession

        for symbol_name in SymbolConstants.IdByName:
            print(f'[PID {os.getpid()}]: Обрабатываем записи с symbol name {symbol_name!r}...')

            symbol_id = SymbolConstants.IdByName[symbol_name]

            async with session_factory() as session_read, session_factory() as session_write:
                # Получаем общее количество записей в таблице okx_order_book_data

                total_result = await session_read.execute(
                    select(
                        func.count(),
                    ).select_from(
                        OKXOrderBookData,
                    ).where(
                        OKXOrderBookData.symbol_name == symbol_name,
                    )
                )

                total_rows = total_result.scalar()

                okx_order_book_data_existent_timestamp_ms_array = numpy.empty(
                    dtype=numpy.int64,
                    shape=(
                        total_rows,
                    ),
                )

                okx_order_book_data_existent_timestamp_ms_array_idx = 0

                # Получаем общее количество записей в таблице okx_order_book_data

                total_result = await session_read.execute(
                    select(
                        func.count(),
                    ).select_from(
                        OKXOrderBookData2,
                    ).where(
                        OKXOrderBookData2.symbol_id == symbol_id,
                    )
                )

                total_rows = total_result.scalar()

                okx_order_book_data_2_existent_timestamp_ms_array = numpy.empty(
                    dtype=numpy.int64,
                    shape=(
                        total_rows,
                    ),
                )

                okx_order_book_data_2_existent_timestamp_ms_array_idx = 0

                result = await session_read.stream(
                    select(
                        OKXOrderBookData.timestamp_ms
                    ).where(
                        OKXOrderBookData.symbol_name == symbol_name,
                    ).limit(
                        okx_order_book_data_existent_timestamp_ms_array.size,
                    ).execution_options(
                        yield_per=_YIELD_PER
                    )
                )

                async for order_book_data in result:
                    if okx_order_book_data_existent_timestamp_ms_array_idx % 1000 == 0:
                        print(order_book_data, okx_order_book_data_existent_timestamp_ms_array_idx)

                    timestamp_ms = order_book_data.timestamp_ms

                    okx_order_book_data_existent_timestamp_ms_array[okx_order_book_data_existent_timestamp_ms_array_idx] = timestamp_ms

                    okx_order_book_data_existent_timestamp_ms_array_idx += 1

                assert okx_order_book_data_existent_timestamp_ms_array_idx == okx_order_book_data_existent_timestamp_ms_array.size, (okx_order_book_data_existent_timestamp_ms_array_idx, okx_order_book_data_existent_timestamp_ms_array.size)

                result = await session_read.stream(
                    select(
                        OKXOrderBookData2.timestamp_ms
                    ).where(
                        OKXOrderBookData2.symbol_id == symbol_id,
                    ).limit(
                        okx_order_book_data_2_existent_timestamp_ms_array.size,
                    ).execution_options(
                        yield_per=_YIELD_PER
                    )
                )

                async for order_book_data in result:
                    if okx_order_book_data_2_existent_timestamp_ms_array_idx % 1000 == 0:
                        print(order_book_data, okx_order_book_data_2_existent_timestamp_ms_array_idx)

                    timestamp_ms = order_book_data.timestamp_ms

                    okx_order_book_data_2_existent_timestamp_ms_array[okx_order_book_data_2_existent_timestamp_ms_array_idx] = timestamp_ms

                    okx_order_book_data_2_existent_timestamp_ms_array_idx += 1

                assert okx_order_book_data_2_existent_timestamp_ms_array_idx == okx_order_book_data_2_existent_timestamp_ms_array.size, (okx_order_book_data_2_existent_timestamp_ms_array_idx, okx_order_book_data_2_existent_timestamp_ms_array.size)

                okx_order_book_data_existent_timestamp_ms_array_diff = numpy.setdiff1d(
                    okx_order_book_data_existent_timestamp_ms_array,
                    okx_order_book_data_2_existent_timestamp_ms_array,
                )

                print(f'[PID {os.getpid()}] Начинаем обработку {okx_order_book_data_existent_timestamp_ms_array_diff.size} записей торгов...')

                for timestamp_ms in okx_order_book_data_existent_timestamp_ms_array_diff:
                    try:
                        # Получаем батч записей
                        result = await session_read.execute(
                            select(
                                OKXOrderBookData,
                            ).where(
                                and_(
                                    OKXTradeData.symbol_name == symbol_name,
                                    OKXTradeData.timestamp_ms == timestamp_ms,
                                )
                            ),
                        )

                        order_book_records = result.scalars().all()

                        for order_book in order_book_records:
                            try:
                                # Проверяем, что action существует в константах
                                if order_book.action not in OKXConstants.OrderBookActionIdByName:
                                    print(f'[PID {os.getpid()}] Пропускаем запись с неизвестным action: {order_book.action}')
                                    skipped_count += 1
                                    continue

                                # Проверяем, что symbol_name существует в константах
                                if order_book.symbol_name not in SymbolConstants.IdByName:
                                    print(f'[PID {os.getpid()}] Пропускаем запись с неизвестным symbol_name: {order_book.symbol_name}')
                                    skipped_count += 1
                                    continue

                                action_id = OKXConstants.OrderBookActionIdByName[order_book.action]

                                new_order_book = OKXOrderBookData2(
                                    symbol_id=symbol_id,
                                    timestamp_ms=order_book.timestamp_ms,
                                    action_id=action_id,
                                    asks=order_book.asks,
                                    bids=order_book.bids,
                                )

                                session_write.add(
                                    new_order_book,
                                )
                                migrated_count += 1

                                if migrated_count % _COMMIT_COUNT == 0:
                                    await session_write.commit()
                                    print(f'[PID {os.getpid()}] Зафиксировано {migrated_count} записей order book...')
                            except Exception as e:
                                error_count += 1
                                print(f'[PID {os.getpid()}] Ошибка при обработке записи order book timestamp_ms={getattr(order_book, "timestamp_ms", "unknown")}: {str(e)}')
                                continue

                        # Коммитим оставшиеся записи в батче
                        await session_write.commit()

                    except Exception as e:
                        print(f'[PID {os.getpid()}] Ошибка при получении timestamp_ms={timestamp_ms}: {str(e)}')
                        error_count += 1
                        continue
        
        await engine.dispose()
        
        print(f'[PID {os.getpid()}] Миграция order book завершена. Мигрировано: {migrated_count}, пропущено: {skipped_count}, ошибок: {error_count}')
        return migrated_count
    
    return uvloop.run(
        _migrate_batch(),
    )


def migrate_candle_data_15m_batch(args):
    """Миграция батча данных свечей 15m в отдельном процессе."""
    database_url, offset, limit = args
    
    async def _migrate_batch():
        engine = create_async_engine(
            database_url,
            echo=True,
        )

        session_factory = async_sessionmaker(
            engine,
            expire_on_commit=False,
        )
        
        migrated_count = 0
        session_read: AsyncSession
        session_write: AsyncSession

        async with session_factory() as session_read, session_factory() as session_write:
            result = await session_read.stream(
                select(
                    OKXCandleData15m,
                ).offset(
                    offset,
                ).limit(
                    limit,
                ).execution_options(
                    yield_per=_YIELD_PER,
                )
            )
            
            async for candle in result.scalars():
                symbol_id = SymbolConstants.IdByName[candle.symbol_name]
                
                new_candle = OKXCandleData15m2(
                    symbol_id=symbol_id,
                    start_timestamp_ms=candle.start_timestamp_ms,
                    is_closed=candle.is_closed,
                    close_price=candle.close_price,
                    high_price=candle.high_price,
                    low_price=candle.low_price,
                    open_price=candle.open_price,
                    volume_contracts_count=candle.volume_contracts_count,
                    volume_base_currency=candle.volume_base_currency,
                    volume_quote_currency=candle.volume_quote_currency,
                )
                
                session_write.add(
                    new_candle,
                )

                migrated_count += 1
                
                if migrated_count % _COMMIT_COUNT == 0:
                    await session_write.commit()
                    print(f'[PID {os.getpid()}] Зафиксировано {migrated_count} записей свечей 15m...')
            
            await session_write.commit()
        
        await engine.dispose()

        return migrated_count
    
    return uvloop.run(
        _migrate_batch(),
    )


def migrate_candle_data_1h_batch(args):
    """Миграция батча данных свечей 1H в отдельном процессе."""
    database_url, offset, limit = args
    
    async def _migrate_batch():
        engine = create_async_engine(
            database_url,
            echo=True,
        )

        session_factory = async_sessionmaker(
            engine,
            expire_on_commit=False,
        )
        
        migrated_count = 0
        session_read: AsyncSession
        session_write: AsyncSession

        async with session_factory() as session_read, session_factory() as session_write:
            result = await session_read.stream(
                select(
                    OKXCandleData1H,
                ).offset(
                    offset,
                ).limit(
                    limit,
                ).execution_options(
                    yield_per=_YIELD_PER,
                )
            )
            
            async for candle in result.scalars():
                symbol_id = SymbolConstants.IdByName[candle.symbol_name]
                
                new_candle = OKXCandleData1H2(
                    symbol_id=symbol_id,
                    start_timestamp_ms=candle.start_timestamp_ms,
                    is_closed=candle.is_closed,
                    close_price=candle.close_price,
                    high_price=candle.high_price,
                    low_price=candle.low_price,
                    open_price=candle.open_price,
                    volume_contracts_count=candle.volume_contracts_count,
                    volume_base_currency=candle.volume_base_currency,
                    volume_quote_currency=candle.volume_quote_currency,
                )
                
                session_write.add(
                    new_candle,
                )

                migrated_count += 1
                
                if migrated_count % _COMMIT_COUNT == 0:
                    await session_write.commit()
                    print(f'[PID {os.getpid()}] Зафиксировано {migrated_count} записей свечей 1H...')
            
            await session_write.commit()
        
        await engine.dispose()

        return migrated_count
    
    return uvloop.run(
        _migrate_batch(),
    )


class DatabaseMigrator:
    """Класс для миграции базы данных."""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = None
        self.session_factory = None

    async def connect(self):
        """Подключение к базе данных."""
        try:
            self.engine = create_async_engine(
                self.database_url,
                echo=True,
            )

            self.session_factory = async_sessionmaker(
                self.engine, expire_on_commit=False
            )

            logger.info('Успешно подключились к базе данных')
        except Exception as exception:
            logger.error(
                'Ошибка подключения к базе данных'
                f': {"".join(traceback.format_exception(exception))}',
            )
            raise

    async def disconnect(self):
        """Отключение от базы данных."""
        if self.engine:
            await self.engine.dispose()
            logger.info('Отключились от базы данных')

    async def get_table_row_count(self, model_class) -> int:
        """Получение количества строк в таблице."""
        session: AsyncSession

        async with self.session_factory() as session:
            result = await session.execute(
                select(
                    func.count(),
                ).select_from(
                    model_class,
                )
            )

            return result.scalar()

    def _create_batches(self, total_rows: int) -> list[tuple[int, int]]:
        """Создание батчей для многопроцессной обработки."""
        batches = []
        for offset in range(0, total_rows, _BATCH_SIZE):
            limit = min(_BATCH_SIZE, total_rows - offset)
            batches.append((offset, limit))
        return batches

    async def create_new_tables(self):
        """Создание новых таблиц с symbol_id."""
        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(
                    BaseOKXCandleData.metadata.create_all,
                )

                await conn.run_sync(
                    BaseOKXOrderBookData.metadata.create_all,
                )

                await conn.run_sync(
                    BaseOKXTradeData.metadata.create_all,
                )

            logger.info('Созданы все новые таблицы с symbol_id')
        except Exception as exception:
            logger.error(
                'Ошибка создания таблиц'
                f': {"".join(traceback.format_exception(exception))}',
            )
            raise

    async def migrate_trade_data(self):
        """Миграция данных из okx_trade_data в okx_trade_data_2."""
        logger.info('Начинаем миграцию данных торгов...')

        try:
            # Получаем количество записей для миграции
            total_rows = await self.get_table_row_count(OKXTradeData)
            logger.info(f'Найдено {total_rows} записей в okx_trade_data')

            if total_rows == 0:
                logger.info('Таблица okx_trade_data пуста, пропускаем миграцию')
                return

            # Подготавливаем аргументы для процесса
            args_list = [(self.database_url,)]

            # Запускаем многопроцессную миграцию
            with ProcessPoolExecutor(max_workers=_MAX_WORKERS) as executor:
                results = list(
                    executor.map(
                        migrate_trade_data_batch,
                        args_list,
                    ),
                )

            total_migrated = sum(results)
            logger.info(f'Миграция торгов завершена. Всего мигрировано: {total_migrated} записей')
        except Exception as e:
            logger.error(f'Ошибка при миграции данных торгов: {str(e)}')
            raise

    async def migrate_order_book_data(self):
        """Миграция данных из okx_order_book_data в okx_order_book_data_2."""
        logger.info('Начинаем миграцию данных order book...')

        try:
            # Получаем количество записей для миграции
            total_rows = await self.get_table_row_count(OKXOrderBookData)
            logger.info(f'Найдено {total_rows} записей в okx_order_book_data')

            if total_rows == 0:
                logger.info('Таблица okx_order_book_data пуста, пропускаем миграцию')
                return

            # Подготавливаем аргументы для процесса
            args_list = [(self.database_url,)]

            # Запускаем многопроцессную миграцию
            with ProcessPoolExecutor(max_workers=_MAX_WORKERS) as executor:
                results = list(executor.map(migrate_order_book_data_batch, args_list))

            total_migrated = sum(results)
            logger.info(f'Миграция order book завершена. Всего мигрировано: {total_migrated} записей')
        except Exception as e:
            logger.error(f'Ошибка при миграции данных order book: {str(e)}')
            raise

    async def migrate_candle_data_15m(self):
        """Миграция данных из okx_candle_data_15m в okx_candle_data_15m_2."""
        logger.info('Начинаем миграцию данных свечей 15m...')

        # Получаем количество записей для миграции
        total_rows = await self.get_table_row_count(OKXCandleData15m)
        logger.info(f'Найдено {total_rows} записей в okx_candle_data_15m')

        if total_rows == 0:
            logger.info('Таблица okx_candle_data_15m пуста, пропускаем миграцию')
            return

        # Создаем батчи для многопроцессной обработки
        batches = self._create_batches(total_rows)
        logger.info(f'Создано {len(batches)} батчей для обработки в {_MAX_WORKERS} процессах')

        # Подготавливаем аргументы для каждого процесса
        args_list = [(self.database_url, offset, limit) for offset, limit in batches]

        # Запускаем многопроцессную миграцию
        with ProcessPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            results = list(executor.map(migrate_candle_data_15m_batch, args_list))

        total_migrated = sum(results)
        logger.info(f'Миграция свечей 15m завершена. Всего мигрировано: {total_migrated} записей')

    async def migrate_candle_data_1h(self):
        """Миграция данных из okx_candle_data_1H в okx_candle_data_1H_2."""
        logger.info('Начинаем миграцию данных свечей 1H...')

        # Получаем количество записей для миграции
        total_rows = await self.get_table_row_count(OKXCandleData1H)
        logger.info(f'Найдено {total_rows} записей в okx_candle_data_1H')

        if total_rows == 0:
            logger.info('Таблица okx_candle_data_1H пуста, пропускаем миграцию')
            return

        # Создаем батчи для многопроцессной обработки
        batches = self._create_batches(total_rows)
        logger.info(f'Создано {len(batches)} батчей для обработки в {_MAX_WORKERS} процессах')

        # Подготавливаем аргументы для каждого процесса
        args_list = [(self.database_url, offset, limit) for offset, limit in batches]

        # Запускаем многопроцессную миграцию
        with ProcessPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            results = list(executor.map(migrate_candle_data_1h_batch, args_list))

        total_migrated = sum(results)
        logger.info(f'Миграция свечей 1H завершена. Всего мигрировано: {total_migrated} записей')

    async def verify_migration(self):
        """Проверка результатов миграции."""
        logger.info('Проверка результатов миграции...')

        tables_to_verify = [
            (OKXTradeData, OKXTradeData2),
            (OKXOrderBookData, OKXOrderBookData2),
            (OKXCandleData15m, OKXCandleData15m2),
            (OKXCandleData1H, OKXCandleData1H2),
        ]

        for source_model, target_model in tables_to_verify:
            try:
                source_count = await self.get_table_row_count(source_model)
                target_count = await self.get_table_row_count(target_model)
                logger.info(
                    f'{source_model.__tablename__}: {source_count} -> {target_model.__tablename__}: {target_count}'
                )
            except Exception as exception:
                logger.warning(
                    f'Не удалось проверить таблицы {source_model.__tablename__}'
                    f': {"".join(traceback.format_exception(exception))}',
                )

    async def run_migration(self):
        """Запуск полной миграции."""
        try:
            await self.connect()

            logger.info('Начинаем миграцию...')
            logger.info('Создание новых таблиц...')
            await self.create_new_tables()

            logger.info('Миграция данных...')
            await asyncio.gather(
                # self.migrate_trade_data(),
                self.migrate_order_book_data(),
                # self.migrate_candle_data_15m(),
                # self.migrate_candle_data_1h(),
            )

            logger.info('Проверка результатов...')
            await self.verify_migration()

            logger.info('Миграция завершена успешно!')
        except Exception as exception:
            logger.error(
                'Ошибка во время миграции'
                f': {"".join(traceback.format_exception(exception))}',
            )
            raise
        finally:
            await self.disconnect()


async def main():
    """Главная функция."""
    try:
        # Импортируем settings только когда нужно
        from settings import settings

        # Формируем URL базы данных из настроек
        database_url = (
            f'postgresql+asyncpg://{settings.POSTGRES_DB_USER_NAME}:'
            f'{settings.POSTGRES_DB_PASSWORD.get_secret_value()}@'
            f'{settings.POSTGRES_DB_HOST_NAME}:'
            f'{settings.POSTGRES_DB_PORT}/'
            f'{settings.POSTGRES_DB_NAME}'
        )

        logger.info(
            f'Подключение к базе данных: {settings.POSTGRES_DB_HOST_NAME}:{settings.POSTGRES_DB_PORT}/{settings.POSTGRES_DB_NAME}'
        )
    except Exception as exception:
        logger.error(
            f'Ошибка при получении настроек базы данных'
            f': {"".join(traceback.format_exception(exception))}',
        )

        logger.error('Убедитесь, что файл .env содержит все необходимые переменные:')
        logger.error('- POSTGRES_DB_HOST_NAME')
        logger.error('- POSTGRES_DB_NAME')
        logger.error('- POSTGRES_DB_PORT')
        logger.error('- POSTGRES_DB_PASSWORD')
        logger.error('- POSTGRES_DB_USER_NAME')
        sys.exit(1)

    migrator = DatabaseMigrator(database_url)
    await migrator.run_migration()


if __name__ == '__main__':
    # Защита от многократного запуска процессов
    mp.set_start_method('spawn', force=True)
    uvloop.run(
        main(),
    )
