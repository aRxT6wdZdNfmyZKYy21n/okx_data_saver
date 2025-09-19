#!/usr/bin/env python3
"""
Скрипт миграции для изменения формата хранения данных
с symbol_name (строка) на symbol_id (целое число).

Создает новые таблицы с постфиксом _2, используя symbol_id вместо symbol_name.
"""

import asyncio
import logging
import sys
import traceback

from sqlalchemy import (
    func,
    select,
)
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker
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
    Base,
    OKXCandleData15m,
    OKXCandleData15m2,
    OKXCandleData1H,
    OKXCandleData1H2,
)
from main.save_order_books.schemas import (
    OKXOrderBookData,
    OKXOrderBookData2,
)
from main.save_trades.schemas import (
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
        async with self.session_factory() as session:
            result = await session.execute(
                select(
                    func.count(),
                ).select_from(
                    model_class,
                )
            )
            return result.scalar()

    async def create_new_tables(self):
        """Создание новых таблиц с symbol_id."""
        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

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

        # Получаем количество записей для миграции
        total_rows = await self.get_table_row_count(OKXTradeData)
        logger.info(f'Найдено {total_rows} записей в okx_trade_data')

        if total_rows == 0:
            logger.info('Таблица okx_trade_data пуста, пропускаем миграцию')
            return

        migrated_count = 0
        async with self.session_factory() as session:
            result = await session.stream(
                select(
                    OKXTradeData,
                ).execution_options(
                    yield_per=1000,
                ),
            )

            async for trade in result.scalars():
                # Находим соответствующий symbol_id
                symbol_id = SymbolConstants.IdByName[trade.symbol_name]

                # Создаем новую запись с symbol_id
                new_trade = OKXTradeData2(
                    symbol_id=symbol_id.value,
                    trade_id=trade.trade_id,
                    is_buy=trade.is_buy,
                    price=trade.price,
                    quantity=trade.quantity,
                    timestamp_ms=trade.timestamp_ms,
                )

                session.add(new_trade)
                migrated_count += 1

                logger.info(
                    f'Мигрирована запись: {trade.symbol_name} -> {symbol_id.value}, '
                    f'trade_id={trade.trade_id}, timestamp={trade.timestamp_ms}'
                )

                # Коммитим каждые 100 записей для производительности
                if migrated_count % 100 == 0:
                    await session.commit()
                    logger.info(f'Зафиксировано {migrated_count} записей...')

            # Коммитим оставшиеся записи
            await session.commit()

        logger.info(
            f'Миграция торгов завершена. Всего мигрировано: {migrated_count} записей'
        )

    async def migrate_order_book_data(self):
        """Миграция данных из okx_order_book_data в okx_order_book_data_2."""
        logger.info('Начинаем миграцию данных order book...')

        # Получаем количество записей для миграции
        total_rows = await self.get_table_row_count(OKXOrderBookData)
        logger.info(f'Найдено {total_rows} записей в okx_order_book_data')

        if total_rows == 0:
            logger.info('Таблица okx_order_book_data пуста, пропускаем миграцию')
            return

        migrated_count = 0
        async with self.session_factory() as session:
            result = await session.stream(
                select(
                    OKXOrderBookData,
                ).execution_options(
                    yield_per=1000,
                ),
            )

            async for order_book in result.scalars():
                action_id = OKXConstants.OrderBookActionIdByName[order_book.action]

                # Находим соответствующий symbol_id
                symbol_id = SymbolConstants.IdByName[order_book.symbol_name]

                # Создаем новую запись с symbol_id
                new_order_book = OKXOrderBookData2(
                    symbol_id=symbol_id.value,
                    timestamp_ms=order_book.timestamp_ms,
                    action=action_id,
                    asks=order_book.asks,
                    bids=order_book.bids,
                )

                session.add(new_order_book)
                migrated_count += 1

                logger.info(
                    f'Мигрирована запись: {order_book.symbol_name} -> {symbol_id.value}, '
                    f'timestamp={order_book.timestamp_ms}, action={order_book.action}'
                )

                # Коммитим каждые 100 записей для производительности
                if migrated_count % 100 == 0:
                    await session.commit()
                    logger.info(f'Зафиксировано {migrated_count} записей...')

            # Коммитим оставшиеся записи
            await session.commit()

        logger.info(
            f'Миграция order book завершена. Всего мигрировано: {migrated_count} записей'
        )

    async def migrate_candle_data_15m(self):
        """Миграция данных из okx_candle_data_15m в okx_candle_data_15m_2."""
        logger.info('Начинаем миграцию данных свечей 15m...')

        # Получаем количество записей для миграции
        total_rows = await self.get_table_row_count(OKXCandleData15m)
        logger.info(f'Найдено {total_rows} записей в okx_candle_data_15m')

        if total_rows == 0:
            logger.info('Таблица okx_candle_data_15m пуста, пропускаем миграцию')
            return

        migrated_count = 0
        async with self.session_factory() as session:
            result = await session.stream(
                select(
                    OKXCandleData15m,
                ).execution_options(
                    yield_per=1000,
                ),
            )

            async for candle in result.scalars():
                # Находим соответствующий symbol_id
                symbol_id = SymbolConstants.IdByName[candle.symbol_name]

                # Создаем новую запись с symbol_id
                new_candle = OKXCandleData15m2(
                    symbol_id=symbol_id.value,
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

                session.add(new_candle)
                migrated_count += 1

                logger.info(
                    f'Мигрирована запись: {candle.symbol_name} -> {symbol_id.value}, '
                    f'start_timestamp={candle.start_timestamp_ms}, '
                    f'is_closed={candle.is_closed}'
                )

                # Коммитим каждые 100 записей для производительности
                if migrated_count % 100 == 0:
                    await session.commit()
                    logger.info(f'Зафиксировано {migrated_count} записей...')

            # Коммитим оставшиеся записи
            await session.commit()

        logger.info(
            f'Миграция свечей 15m завершена. Всего мигрировано: {migrated_count} записей'
        )

    async def migrate_candle_data_1h(self):
        """Миграция данных из okx_candle_data_1H в okx_candle_data_1H_2."""
        logger.info('Начинаем миграцию данных свечей 1H...')

        # Получаем количество записей для миграции
        total_rows = await self.get_table_row_count(OKXCandleData1H)
        logger.info(f'Найдено {total_rows} записей в okx_candle_data_1H')

        if total_rows == 0:
            logger.info('Таблица okx_candle_data_1H пуста, пропускаем миграцию')
            return

        migrated_count = 0
        async with self.session_factory() as session:
            result = await session.stream(
                select(
                    OKXCandleData1H,
                ).execution_options(
                    yield_per=1000,
                ),
            )

            async for candle in result.scalars():
                # Находим соответствующий symbol_id
                symbol_id = SymbolConstants.IdByName[candle.symbol_name]

                # Создаем новую запись с symbol_id
                new_candle = OKXCandleData1H2(
                    symbol_id=symbol_id.value,
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

                session.add(new_candle)
                migrated_count += 1

                logger.info(
                    f'Мигрирована запись: {candle.symbol_name} -> {symbol_id.value}, '
                    f'start_timestamp={candle.start_timestamp_ms}, '
                    f'is_closed={candle.is_closed}'
                )

                # Коммитим каждые 100 записей для производительности
                if migrated_count % 100 == 0:
                    await session.commit()
                    logger.info(f'Зафиксировано {migrated_count} записей...')

            # Коммитим оставшиеся записи
            await session.commit()

        logger.info(
            f'Миграция свечей 1H завершена. Всего мигрировано: {migrated_count} записей'
        )

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
            await self.migrate_trade_data()
            await self.migrate_order_book_data()
            await self.migrate_candle_data_15m()
            await self.migrate_candle_data_1h()

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
    uvloop.run(
        main(),
    )
