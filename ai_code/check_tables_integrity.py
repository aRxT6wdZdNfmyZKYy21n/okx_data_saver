#!/usr/bin/env python3
"""
Скрипт проверки для изменения формата хранения данных
с symbol_name (строка) на symbol_id (целое число).

Создает новые таблицы с постфиксом _2, используя symbol_id вместо symbol_name.
"""

import asyncio
import logging
import multiprocessing as mp
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor
from decimal import (
    Decimal,
)

import orjson
import polars
from sqlalchemy import (
    func,
    select,
)
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from enumerations import (
    OKXOrderBookActionId,
    SymbolId,
)

try:
    import uvloop
except ImportError:
    uvloop = asyncio

from main.save_candles.schemas import (
    Base as BaseOKXCandleData,
)
from main.save_candles.schemas import (
    OKXCandleData1H,
    OKXCandleData1H2,
    OKXCandleData15m,
    OKXCandleData15m2,
)
from main.save_order_books.schemas import (
    Base as BaseOKXOrderBookData,
)
from main.save_order_books.schemas import (
    OKXOrderBookData,
    OKXOrderBookData2,
)
from main.save_trades.schemas import (
    Base as BaseOKXTradeData,
)
from main.save_trades.schemas import (
    OKXTradeData,
    OKXTradeData2,
)
from settings import (
    settings,
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
_MAX_WORKERS = 1  # mp.cpu_count() - 2
_BATCH_SIZE = 1_000_000  # Размер батча для каждого процесса


def check_trade_data_batch(args):
    """Проверка батча данных торгов в отдельном процессе."""
    database_url, offset, limit = args

    async def _check_batch():
        # Формируем URI для подключения к БД
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

        checked_count = 0

        try:
            # Загружаем батч данных из таблицы
            dataframe = polars.read_database_uri(
                query=(
                    'SELECT'
                    ' symbol_id'
                    ', trade_id'
                    ', is_buy'
                    ', price'
                    ', quantity'
                    ', timestamp_ms'
                    f' FROM {OKXTradeData2.__tablename__}'
                    ' ORDER BY'
                    ' symbol_id ASC'
                    ', trade_id ASC'
                    f' OFFSET {offset}'
                    f' LIMIT {limit}'
                    ';'
                ),
                engine='connectorx',
                uri=uri,
            )

            is_buy: bool
            price: Decimal
            quantity: Decimal
            symbol_id_raw: str
            timestamp_ms: int
            trade_id: int

            # Проверяем каждую строку из таблицы
            for (
                symbol_id_raw,
                trade_id,
                is_buy,
                price,
                quantity,
                timestamp_ms,
            ) in dataframe.iter_rows(
                named=False,
            ):
                assert type(symbol_id_raw) is str, (type(symbol_id_raw),)
                assert type(trade_id) is int, (type(trade_id),)
                assert type(is_buy) is bool, (type(is_buy),)
                assert type(price) is Decimal, (type(price),)
                assert type(quantity) is Decimal, (type(quantity),)
                assert type(timestamp_ms) is int, (type(timestamp_ms),)

                symbol_id: SymbolId | None = getattr(
                    SymbolId,
                    symbol_id_raw,
                    None,
                )

                if symbol_id is None:
                    print(
                        f'[check_trade_data_batch] found incorrect symbol ID (raw): {symbol_id_raw!r}'
                    )

                    continue

                if trade_id < 0:
                    print(
                        f'[check_trade_data_batch] found incorrect trade ID {trade_id}'
                        f' of row with symbol ID {symbol_id.name!r}'
                    )

                    continue

                if price < 0:
                    print(
                        f'[check_trade_data_batch] found incorrect price {price}'
                        f' of row with symbol ID {symbol_id.name!r} and trade ID {trade_id}'
                    )

                    continue

                if quantity < 0:
                    print(
                        f'[check_trade_data_batch] found incorrect quantity {quantity}'
                        f' of row with symbol ID {symbol_id.name!r} and trade ID {trade_id}'
                    )

                    continue

                if not (-10800000 <= timestamp_ms <= 1767214800000):
                    print(
                        f'[check_order_book_data_batch] found incorrect timestamp (ms): {timestamp_ms!r}'
                        f' of row with symbol ID {symbol_id.name!r} and trade ID {trade_id}'
                    )

                    continue

                checked_count += 1

                if checked_count % 10000 == 0:
                    print(
                        f'[check_trade_data_batch] checked {checked_count} rows',
                    )
        except Exception as exception:
            print(
                'Handled exception'
                f': {"".join(traceback.format_exception(exception))}',
            )

        return checked_count

    return uvloop.run(
        _check_batch(),
    )


def check_order_book_data_batch(args):
    """Проверка батча данных order book в отдельном процессе."""
    database_url, offset, limit = args

    async def _check_batch() -> int:
        checked_count = 0

        try:
            # Создаем engine и session factory
            engine = create_async_engine(
                database_url,
                echo=True,
            )

            session_factory = async_sessionmaker(
                engine,
                expire_on_commit=False,
            )

            async with session_factory() as session:
                # Создаем запрос для получения батча данных
                query = (
                    select(
                        OKXOrderBookData2.symbol_id,
                        OKXOrderBookData2.timestamp_ms,
                        OKXOrderBookData2.action_id,
                        OKXOrderBookData2.asks,
                        OKXOrderBookData2.bids,
                    )
                    .order_by(
                        OKXOrderBookData2.symbol_id.asc(),
                        OKXOrderBookData2.timestamp_ms.asc(),
                    )
                    .offset(
                        offset,
                    )
                    .limit(
                        limit,
                    )
                    .execution_options(
                        yield_per=_YIELD_PER,
                    )
                )

                result = await session.stream(
                    query,
                )

                # Используем stream для обработки данных по частям
                async for row_data in result:
                    symbol_id_raw = row_data.symbol_id
                    timestamp_ms = row_data.timestamp_ms
                    action_id_raw = row_data.action_id
                    asks_raw = row_data.asks
                    bids_raw = row_data.bids

                    assert type(symbol_id_raw) is str, (type(symbol_id_raw),)
                    assert type(timestamp_ms) is int, (type(timestamp_ms),)
                    assert type(action_id_raw) is str, (type(action_id_raw),)
                    assert type(asks_raw) is str, (type(asks_raw),)
                    assert type(bids_raw) is str, (type(bids_raw),)

                    symbol_id: SymbolId | None = getattr(
                        SymbolId,
                        symbol_id_raw,
                        None,
                    )

                    if symbol_id is None:
                        print(
                            f'[check_order_book_data_batch] found incorrect symbol ID (raw): {symbol_id_raw!r}'
                        )

                        continue

                    if not (-10800000 <= timestamp_ms <= 1767214800000):
                        print(
                            f'[check_order_book_data_batch] found incorrect timestamp (ms): {timestamp_ms!r}'
                            f' of row with symbol ID {symbol_id.name!r}'
                        )

                        continue

                    action_id: OKXOrderBookActionId | None = getattr(
                        OKXOrderBookActionId,
                        action_id_raw,
                        None,
                    )

                    if action_id is None:
                        print(
                            f'[check_order_book_data_batch] found incorrect action ID (raw): {action_id_raw!r}'
                            f' of row with symbol ID {symbol_id.name!r} and timestamp (ms) {timestamp_ms}'
                        )

                        continue

                    try:
                        asks = orjson.loads(
                            asks_raw,
                        )
                    except orjson.JSONDecodeError:
                        print(
                            f'[check_order_book_data_batch] found incorrect asks (raw) {asks_raw!r}'
                            f' of row with symbol ID {symbol_id.name!r} and timestamp (ms) {timestamp_ms}'
                        )

                        continue

                    try:
                        bids = orjson.loads(
                            bids_raw,
                        )
                    except orjson.JSONDecodeError:
                        print(
                            f'[check_order_book_data_batch] found incorrect bids (raw) {bids_raw!r}'
                            f' of row with symbol ID {symbol_id.name!r} and timestamp (ms) {timestamp_ms}'
                        )

                        continue

                    checked_count += 1

                    if checked_count % 10000 == 0:
                        print(
                            f'[check_order_book_data_batch] checked {checked_count} rows',
                        )

            # Закрываем engine
            await engine.dispose()
        except Exception as exception:
            print(
                'Handled exception'
                f': {"".join(traceback.format_exception(exception))}',
            )

        return checked_count

    return uvloop.run(
        _check_batch(),
    )


def check_candle_data_15m_batch(args):
    """Проверка батча данных свечей 15m в отдельном процессе."""
    database_url, offset, limit = args

    async def _check_batch() -> int:
        # Импортируем settings только когда нужно
        from settings import settings

        # Формируем URI для подключения к БД
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

        # Загружаем батч данных из таблицы
        dataframe = polars.read_database_uri(
            query=(
                'SELECT'
                ' symbol_id'
                ', start_timestamp_ms'
                ', is_closed'
                ', close_price'
                ', high_price'
                ', low_price'
                ', open_price'
                ', volume_contracts_count'
                ', volume_base_currency'
                ', volume_quote_currency'
                f' FROM {OKXCandleData15m2.__tablename__}'
                f' ORDER BY symbol_id ASC, start_timestamp_ms ASC'
                f' OFFSET {offset} LIMIT {limit}'
            ),
            engine='connectorx',
            uri=uri,
        )

        checked_count = 0

        # Проверяем каждую строку из таблицы
        for row in dataframe.iter_rows(named=False):
            # TODO: Здесь будет ваша логика проверки полей
            # row содержит: (symbol_id, start_timestamp_ms, is_closed, close_price, high_price, low_price, open_price, volume_contracts_count, volume_base_currency, volume_quote_currency)
            checked_count += 1

        return checked_count

    return uvloop.run(
        _check_batch(),
    )


def check_candle_data_1h_batch(args):
    """Проверка батча данных свечей 1H в отдельном процессе."""
    database_url, offset, limit = args

    async def _check_batch() -> int:
        # Импортируем settings только когда нужно
        from settings import settings

        # Формируем URI для подключения к БД
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

        # Загружаем батч данных из новой таблицы
        dataframe = polars.read_database_uri(
            query=(
                'SELECT'
                ' symbol_id'
                ', start_timestamp_ms'
                ', is_closed'
                ', close_price'
                ', high_price'
                ', low_price'
                ', open_price'
                ', volume_contracts_count'
                ', volume_base_currency'
                ', volume_quote_currency'
                f' FROM {OKXCandleData1H2.__tablename__}'
                f' ORDER BY symbol_id ASC, start_timestamp_ms ASC'
                f' OFFSET {offset} LIMIT {limit}'
            ),
            engine='connectorx',
            uri=uri,
        )

        checked_count = 0

        # Проверяем каждую строку из таблицы
        for row in dataframe.iter_rows(named=False):
            # TODO: Здесь будет ваша логика проверки полей
            # row содержит: (symbol_id, start_timestamp_ms, is_closed, close_price, high_price, low_price, open_price, volume_contracts_count, volume_base_currency, volume_quote_currency)
            checked_count += 1

        return checked_count

    return uvloop.run(
        _check_batch(),
    )


class DatabaseMigrator:
    """Класс для проверки базы данных."""

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

    async def check_trade_data(self):
        """Проверка данных в okx_trade_data_2."""
        logger.info('Начинаем проверку данных торгов...')

        try:
            # Получаем количество записей для проверки
            total_rows = await self.get_table_row_count(OKXTradeData2)
            logger.info(f'Найдено {total_rows} записей в okx_trade_data_2')

            if total_rows == 0:
                logger.info('Таблица okx_trade_data_2 пуста, пропускаем проверку')
                return

            # Создаем батчи для многопроцессной обработки
            batches = self._create_batches(total_rows)
            logger.info(
                f'Создано {len(batches)} батчей для обработки в {_MAX_WORKERS} процессах'
            )

            # Подготавливаем аргументы для каждого процесса
            args_list = [
                (self.database_url, offset, limit) for offset, limit in batches
            ]

            # Запускаем многопроцессную проверку
            with ProcessPoolExecutor(max_workers=_MAX_WORKERS) as executor:
                results = list(
                    executor.map(
                        check_trade_data_batch,
                        args_list,
                    ),
                )

            total_checked = sum(results)
            logger.info(
                f'Проверка торгов завершена. Всего проверено: {total_checked} записей'
            )
        except Exception as exception:
            logger.error(f'Ошибка при проверки данных торгов: {str(exception)}')
            raise

    async def check_order_book_data(self):
        """Проверка данных из okx_order_book_data_2."""
        logger.info('Начинаем проверку данных order book...')

        try:
            # Получаем количество записей для проверки
            total_rows = await self.get_table_row_count(OKXOrderBookData2)
            logger.info(f'Найдено {total_rows} записей в okx_order_book_data_2')

            if total_rows == 0:
                logger.info('Таблица okx_order_book_data_2 пуста, пропускаем проверку')
                return

            # Создаем батчи для многопроцессной обработки
            batches = self._create_batches(total_rows)
            logger.info(
                f'Создано {len(batches)} батчей для обработки в {_MAX_WORKERS} процессах'
            )

            # Подготавливаем аргументы для каждого процесса
            args_list = [
                (self.database_url, offset, limit) for offset, limit in batches
            ]

            # Запускаем многопроцессную проверку
            with ProcessPoolExecutor(max_workers=_MAX_WORKERS) as executor:
                results = list(executor.map(check_order_book_data_batch, args_list))

            total_checked = sum(results)
            logger.info(
                f'Проверка order book завершена. Всего проверено: {total_checked} записей'
            )
        except Exception as exception:
            logger.error(f'Ошибка при проверки данных order book: {str(exception)}')
            raise

    async def check_candle_data_15m(self):
        """Проверка данных из okx_candle_data_15m_2."""
        logger.info('Начинаем проверку данных свечей 15m...')

        # Получаем количество записей для проверки
        total_rows = await self.get_table_row_count(OKXCandleData15m2)
        logger.info(f'Найдено {total_rows} записей в okx_candle_data_15m_2')

        if total_rows == 0:
            logger.info('Таблица okx_candle_data_15m пуста, пропускаем проверку')
            return

        # Создаем батчи для многопроцессной обработки
        batches = self._create_batches(total_rows)
        logger.info(
            f'Создано {len(batches)} батчей для обработки в {_MAX_WORKERS} процессах'
        )

        # Подготавливаем аргументы для каждого процесса
        args_list = [(self.database_url, offset, limit) for offset, limit in batches]

        # Запускаем многопроцессную проверку
        with ProcessPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            results = list(executor.map(check_candle_data_15m_batch, args_list))

        total_checked = sum(results)
        logger.info(
            f'Проверка свечей 15m завершена. Всего проверено: {total_checked} записей'
        )

    async def check_candle_data_1h(self):
        """Проверка данных в okx_candle_data_1H_2."""
        logger.info('Начинаем проверку данных свечей 1H...')

        # Получаем количество записей для проверки
        total_rows = await self.get_table_row_count(OKXCandleData1H2)
        logger.info(f'Найдено {total_rows} записей в okx_candle_data_1H_2')

        if total_rows == 0:
            logger.info('Таблица okx_candle_data_1H пуста, пропускаем проверку')
            return

        # Создаем батчи для многопроцессной обработки
        batches = self._create_batches(total_rows)
        logger.info(
            f'Создано {len(batches)} батчей для обработки в {_MAX_WORKERS} процессах'
        )

        # Подготавливаем аргументы для каждого процесса
        args_list = [(self.database_url, offset, limit) for offset, limit in batches]

        # Запускаем многопроцессную проверку
        with ProcessPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            results = list(executor.map(check_candle_data_1h_batch, args_list))

        total_checked = sum(results)
        logger.info(
            f'Проверка свечей 1H завершена. Всего проверено: {total_checked} записей'
        )

    async def verify_migration(self):
        """Проверка результатов проверки."""
        logger.info('Проверка результатов проверки...')

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
        """Запуск полной проверки."""
        try:
            await self.connect()

            logger.info('Начинаем проверку...')
            # logger.info('Создание новых таблиц...')
            # await self.create_new_tables()

            logger.info('Проверка данных...')
            await asyncio.gather(
                # self.check_trade_data(),
                self.check_order_book_data(),
                # self.check_candle_data_15m(),
                # self.check_candle_data_1h(),
            )

            logger.info('Проверка результатов...')
            await self.verify_migration()

            logger.info('Проверка завершена успешно!')
        except Exception as exception:
            logger.error(
                'Ошибка во время проверки'
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
