"""
Адаптер для загрузки данных из Redis в FinPlotChartProcessor.
"""

import logging
import traceback
from typing import Any

from polars import DataFrame, Series

from enumerations import SymbolId
from main.process_data.redis_service import g_redis_data_service

logger = logging.getLogger(__name__)


class RedisDataAdapter:
    """Адаптер для загрузки данных из Redis."""

    @staticmethod
    async def load_trades_dataframe(
        symbol_id: SymbolId,
    ) -> DataFrame | None:
        """Загрузка данных о сделках из Redis."""
        try:
            trades_df = await g_redis_data_service.load_trades_data(
                symbol_id,
            )

            if trades_df is None:
                logger.warning(
                    f'No trades data found in Redis for {symbol_id.name}',
                )

                return None

            logger.info(
                f'Loaded trades data from Redis for {symbol_id.name}: {trades_df.height} records',
            )

            return trades_df
        except Exception as exception:
            logger.error(
                f'Error loading trades data from Redis for {symbol_id.name}'
                f': {"".join(traceback.format_exception(exception))}'
            )

            return None

    @staticmethod
    async def load_bollinger_data(
        symbol_id: SymbolId,
    ) -> tuple[Series | None, Series | None, Series | None]:
        """Загрузка полос Боллинджера из Redis."""
        try:
            bollinger_df = await g_redis_data_service.load_bollinger_data(
                symbol_id,
            )

            if bollinger_df is None:
                logger.warning(
                    f'No bollinger data found in Redis for {symbol_id.name}',
                )

                return None, None, None

            upper_band = bollinger_df.get_column(
                'upper_band',
            )
            middle_band = bollinger_df.get_column(
                'middle_band',
            )
            lower_band = bollinger_df.get_column(
                'lower_band',
            )

            logger.info(
                f'Loaded bollinger data from Redis for {symbol_id.name}',
            )

            return upper_band, middle_band, lower_band

        except Exception as exception:
            logger.error(
                f'Error loading bollinger data from Redis for {symbol_id.name}'
                f': {"".join(traceback.format_exception(exception))}',
            )

            return None, None, None

    @staticmethod
    async def load_candle_dataframe(
        symbol_id: SymbolId,
        interval: str,
    ) -> DataFrame | None:
        """Загрузка свечных данных из Redis."""
        try:
            candles_df = await g_redis_data_service.load_candles_data(
                symbol_id,
                interval,
            )

            if candles_df is None:
                logger.warning(
                    f'No candles data found in Redis for {symbol_id.name}:{interval}',
                )

                return None

            logger.info(
                f'Loaded candles data from Redis for {symbol_id.name}:{interval}: {candles_df.height} records',
            )

            return candles_df

        except Exception as exception:
            logger.error(
                f'Error loading candles data from Redis for {symbol_id.name}:{interval}'
                f': {"".join(traceback.format_exception(exception))}',
            )

            return None

    @staticmethod
    async def load_rsi_data(
        symbol_id: SymbolId,
    ) -> Series | None:
        """Загрузка RSI данных из Redis."""
        try:
            rsi_df = await g_redis_data_service.load_rsi_data(
                symbol_id,
            )

            if rsi_df is None:
                logger.warning(
                    f'No RSI data found in Redis for {symbol_id.name}',
                )

                return None

            rsi_series = rsi_df.get_column(
                'rsi',
            )

            logger.info(
                f'Loaded RSI data from Redis for {symbol_id.name}',
            )

            return rsi_series
        except Exception as exception:
            logger.error(
                f'Error loading RSI data from Redis for {symbol_id.name}'
                f': {"".join(traceback.format_exception(exception))}'
            )

            return None

    @staticmethod
    async def load_smoothed_dataframe(
        symbol_id: SymbolId,
        level: str,
    ) -> DataFrame | None:
        """Загрузка сглаженных данных из Redis."""
        try:
            smoothed_df = await g_redis_data_service.load_smoothed_data(
                symbol_id,
                level,
            )

            if smoothed_df is None:
                logger.warning(
                    f'No smoothed data found in Redis for {symbol_id.name}:{level}',
                )
                return None

            logger.info(
                f'Loaded smoothed data from Redis for {symbol_id.name}:{level}: {smoothed_df.height} records',
            )

            return smoothed_df

        except Exception as exception:
            logger.error(
                f'Error loading smoothed data from Redis for {symbol_id.name}:{level}'
                f': {"".join(traceback.format_exception(exception))}',
            )

            return None

    @staticmethod
    async def load_extreme_lines_data(
        symbol_id: SymbolId,
    ) -> tuple[Any | None, tuple[float, float] | None, float | None]:
        """Загрузка экстремальных линий из Redis."""
        try:
            extreme_lines_array = await g_redis_data_service.load_extreme_lines_data(
                symbol_id,
            )

            if extreme_lines_array is None:
                logger.warning(
                    f'No extreme lines data found in Redis for {symbol_id.name}'
                )
                return None, None, None

            # Получаем метаданные для позиции и масштаба
            metadata = await g_redis_data_service.load_symbol_metadata(symbol_id)
            if metadata:
                # Для экстремальных линий нужны дополнительные метаданные
                # Пока возвращаем заглушки
                position = (0.0, 0.0)
                scale = 1.0
            else:
                position = None
                scale = None

            logger.info(f'Loaded extreme lines data from Redis for {symbol_id.name}')
            return extreme_lines_array, position, scale

        except Exception as exception:
            logger.error(
                f'Error loading extreme lines data from Redis for {symbol_id.name}'
                f': {"".join(traceback.format_exception(exception))}',
            )

            return None, None, None

    @staticmethod
    async def load_order_book_volumes_data(
        symbol_id: SymbolId,
    ) -> tuple[Any | None, Any | None, tuple[float, float] | None, float | None]:
        """Загрузка объемов стакана из Redis."""
        try:
            (
                asks_array,
                bids_array,
            ) = await g_redis_data_service.load_order_book_volumes_data(
                symbol_id,
            )

            if asks_array is None or bids_array is None:
                logger.warning(
                    f'No order book volumes data found in Redis for {symbol_id.name}',
                )

                return None, None, None, None

            # Получаем метаданные для позиции и масштаба
            metadata = await g_redis_data_service.load_symbol_metadata(
                symbol_id,
            )

            if metadata:
                # Для объемов стакана нужны дополнительные метаданные
                # Пока возвращаем заглушки
                position = (0.0, 0.0)
                scale = 1.0
            else:
                position = None
                scale = None

            logger.info(
                f'Loaded order book volumes data from Redis for {symbol_id.name}',
            )

            return asks_array, bids_array, position, scale

        except Exception as exception:
            logger.error(
                f'Error loading order book volumes data from Redis for {symbol_id.name}'
                f': {"".join(traceback.format_exception(exception))}',
            )

            return None, None, None, None

    @staticmethod
    async def load_velocity_data(
        symbol_id: SymbolId,
    ) -> Series | None:
        """Загрузка данных скорости из Redis."""
        try:
            velocity_df = await g_redis_data_service.load_velocity_data(
                symbol_id,
            )

            if velocity_df is None:
                logger.warning(
                    f'No velocity data found in Redis for {symbol_id.name}',
                )

                return None

            velocity_series = velocity_df.get_column(
                'velocity',
            )

            logger.info(
                f'Loaded velocity data from Redis for {symbol_id.name}',
            )

            return velocity_series

        except Exception as exception:
            logger.error(
                f'Error loading velocity data from Redis for {symbol_id.name}'
                f': {"".join(traceback.format_exception(exception))}'
            )

            return None

    @staticmethod
    async def load_available_symbols() -> list[str] | None:
        """Загрузка списка доступных символов из Redis."""
        try:
            symbol_names = await g_redis_data_service.load_available_symbols()

            if not symbol_names:
                logger.warning('No available symbols found in Redis')
                return None

            logger.info(
                f'Loaded available symbols from Redis: {len(symbol_names)} symbols'
            )

            return symbol_names

        except Exception as exception:
            logger.error(
                f'Error loading available symbols from Redis'
                f': {"".join(traceback.format_exception(exception))}',
            )

            return None

    async def get_symbol_metadata(
        self,
        symbol_id: SymbolId,
    ) -> Any | None:
        """Получение метаданных символа из Redis."""
        try:
            metadata = await g_redis_data_service.load_symbol_metadata(
                symbol_id,
            )

            if metadata is None:
                logger.warning(f'No metadata found in Redis for {symbol_id.name}')
                return None

            logger.info(f'Loaded symbol metadata from Redis for {symbol_id.name}')

            return metadata
        except Exception as exception:
            logger.error(
                f'Error loading symbol metadata from Redis for {symbol_id.name}'
                f': {"".join(traceback.format_exception(exception))}'
            )

            return None


# Глобальный экземпляр адаптера
g_redis_data_adapter = RedisDataAdapter()
