import asyncio
import logging
from datetime import UTC, datetime

from polars import DataFrame
from sqlalchemy import text

from constants.symbol import (
    SymbolConstants,
)
from enumerations import (
    SymbolId,
)
from main.process_data.data_processor import g_data_processor_v2
from main.process_data.data_set_record_fetcher import g_data_set_record_fetcher
from main.process_data.monitoring import g_error_handler, g_system_monitor
from main.process_data.redis_service import g_redis_data_service
from main.process_data.schemas import ProcessingStatus, SymbolMetadata
from main.show_plot.globals import (
    g_globals,
)
from utils.redis import g_redis_manager

try:
    import uvloop
except ImportError:
    uvloop = asyncio


logger = logging.getLogger(
    __name__,
)


_SYMBOL_NAMES = [
    'BTC-USDT',
    'ETH-USDT',
]


class DataProcessingDaemonV2:
    __slots__ = ()

    async def start_update_loop(
        self,
    ) -> None:
        while True:
            try:
                # Запускаем проверки здоровья системы
                health_checks = await g_system_monitor.run_health_checks()

                if not health_checks.get('redis', False):
                    logger.error('Redis health check failed, skipping update')
                    await asyncio.sleep(60)  # Пауза при проблемах с Redis
                    continue

                await self.__update()
            except Exception as exception:
                # Обрабатываем ошибку через систему мониторинга
                g_error_handler.handle_error(
                    operation='update_loop',
                    error=exception,
                    context={'loop_iteration': True},
                )

            await asyncio.sleep(
                15.0  # s
            )

    async def __update(
        self,
    ) -> None:
        # Обновляем список доступных символов
        await self.__update_current_available_symbol_name_set()

        for symbol_name in _SYMBOL_NAMES:
            await self.__update_symbol(
                symbol_name,
            )

    async def __update_symbol(self, symbol_name: str) -> None:
        """Обновление данных для конкретного символа."""
        symbol_id = SymbolConstants.IdByName[symbol_name]
        start_time = datetime.now(UTC)

        # Обновляем статус обработки
        await g_redis_data_service.save_processing_status(
            ProcessingStatus(
                symbol_id=symbol_id,
                status='processing',
                last_processed=start_time,
                error_message=None,
                processing_time_seconds=None,
            )
        )

        try:
            # Получаем агрегированные данные
            data_set_df = await self.__fetch_data_set_records_dataframe(
                min_start_trade_id=0,  # TODO: получать из Redis
                symbol_id=symbol_id,
            )

            if data_set_df is not None and data_set_df.height > 0:
                # Обрабатываем все данные символа
                await self.__process_symbol_data(
                    symbol_id=symbol_id,
                    symbol_name=symbol_name,
                    data_set_df=data_set_df,
                )

                logger.info(
                    f'Processed data set records for {symbol_name}: {data_set_df.height} records',
                )

            # Обновляем статус обработки
            processing_time = (datetime.now(UTC) - start_time).total_seconds()
            await g_redis_data_service.save_processing_status(
                ProcessingStatus(
                    symbol_id=symbol_id,
                    status='completed',
                    last_processed=datetime.now(UTC),
                    error_message=None,
                    processing_time_seconds=processing_time,
                )
            )
        except Exception as exception:
            processing_time = (datetime.now(UTC) - start_time).total_seconds()

            # Обрабатываем ошибку через систему мониторинга
            g_error_handler.handle_error(
                operation=f'update_symbol_{symbol_name}',
                error=exception,
                context={
                    'symbol_id': symbol_id,
                    'symbol_name': symbol_name,
                    'processing_time': processing_time,
                },
            )

            await g_redis_data_service.save_processing_status(
                ProcessingStatus(
                    symbol_id=symbol_id,
                    status='error',
                    last_processed=datetime.now(UTC),
                    error_message=str(exception),
                    processing_time_seconds=processing_time,
                )
            )

    @staticmethod
    async def __process_symbol_data(
        symbol_id: SymbolId,
        symbol_name: str,
        data_set_df: DataFrame,
    ) -> None:
        """Полная обработка данных символа."""
        # Сохраняем основные агрегированные данные
        price_series = data_set_df.get_column('close_price')
        min_price = float(price_series.min())
        max_price = float(price_series.max())
        min_trade_id = int(data_set_df.get_column('start_trade_id').min())
        max_trade_id = int(data_set_df.get_column('end_trade_id').max())

        # Сохраняем агрегированные данные в Redis (аналогично trades_data)
        await g_redis_data_service.save_trades_data(
            symbol_id=symbol_id,
            trades_df=data_set_df,  # Передаем агрегированные данные
            min_trade_id=min_trade_id,
            max_trade_id=max_trade_id,
            min_price=min_price,
            max_price=max_price,
        )

        # Обрабатываем все производные данные
        await g_data_processor_v2.process_data_set_records(
            symbol_id=symbol_id,
            data_set_df=data_set_df,
        )

        # Обновляем метаданные символа
        symbol_metadata = SymbolMetadata(
            symbol_id=symbol_id,
            symbol_name=symbol_name,
            last_updated=datetime.now(UTC),
            has_trades_data=True,
            has_bollinger=True,
            has_candles=True,
            has_rsi=True,
            has_smoothed=True,
            has_extreme_lines=True,
            has_order_book_volumes=True,
            has_velocity=True,
        )

        await g_redis_data_service.save_symbol_metadata(
            symbol_metadata,
        )

    @staticmethod
    async def __fetch_data_set_records_dataframe(
        min_start_trade_id: int,
        symbol_id: SymbolId,
    ) -> DataFrame | None:
        """Получение агрегированных данных из базы данных."""
        return await g_data_set_record_fetcher.fetch_data_set_records_dataframe(
            min_start_trade_id=min_start_trade_id,
            symbol_id=symbol_id,
        )

    @staticmethod
    async def __update_current_available_symbol_name_set() -> None:
        """Обновление списка доступных символов."""
        current_available_symbol_name_set: set[str] | None = None

        postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

        async with postgres_db_session_maker() as session:
            recursive_cte_full_query = text(
                """
WITH RECURSIVE symbol_id_cte(symbol_id) AS 
(
  (
    SELECT okx_data_set_record_data_2.symbol_id AS symbol_id 
    FROM okx_data_set_record_data_2 ORDER BY okx_data_set_record_data_2.symbol_id ASC 
    LIMIT 1
  )
  UNION ALL
  SELECT (
    SELECT symbol_id
    FROM okx_data_set_record_data_2
    WHERE symbol_id > cte.symbol_id
    ORDER BY symbol_id ASC
    LIMIT 1
  )
  FROM symbol_id_cte AS cte
  WHERE cte.symbol_id IS NOT NULL
)
SELECT symbol_id
FROM symbol_id_cte
WHERE symbol_id IS NOT NULL;
                """
            )

            result = await session.execute(
                recursive_cte_full_query,
            )

            for row in result:
                symbol_id_raw: str = row.symbol_id

                symbol_id = getattr(
                    SymbolId,
                    symbol_id_raw,
                )

                symbol_name = SymbolConstants.NameById[symbol_id]

                if current_available_symbol_name_set is None:
                    current_available_symbol_name_set = set()

                current_available_symbol_name_set.add(
                    symbol_name,
                )

        symbol_names = sorted(
            current_available_symbol_name_set,
        )

        await g_redis_data_service.save_available_symbols(
            symbol_names,
        )

        logger.info(f'Updated available symbols: {len(symbol_names)} symbols')


async def main() -> None:
    # Set up logging
    logging.basicConfig(
        encoding='utf-8',
        format='[%(levelname)s][%(asctime)s][%(name)s]: %(message)s',
        level=(
            # logging.INFO
            logging.DEBUG
        ),
    )

    # Initialize Redis connection
    await g_redis_manager.connect()

    try:
        data_processing_daemon = DataProcessingDaemonV2()

        # Start loops
        await asyncio.gather(
            data_processing_daemon.start_update_loop(),
        )
    finally:
        # Close Redis connection
        await g_redis_manager.disconnect()


if __name__ == '__main__':
    uvloop.run(
        main(),
    )
