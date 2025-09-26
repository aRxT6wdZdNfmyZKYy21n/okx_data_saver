"""
Обновленный процессор для работы с данными из Redis.
"""

import asyncio
import logging
import traceback
from typing import Any

from polars import DataFrame, Series

from enumerations import (
    SymbolId,
)
from main.show_plot.globals import g_globals
from main.show_plot.redis_data_adapter import g_redis_data_adapter

logger = logging.getLogger(__name__)


class RedisFinPlotChartProcessor:
    """Процессор для работы с данными из Redis."""

    def __init__(self):
        self.redis_adapter = g_redis_data_adapter

    async def update_trades_dataframe(
        self,
        symbol_id: SymbolId,
        min_trade_id: int = 0,
    ) -> DataFrame | None:
        """Обновление данных о сделках из Redis."""
        logger.info(f'Updating trades dataframe for {symbol_id.name}')

        trades_df = await self.redis_adapter.load_trades_dataframe(
            symbol_id=symbol_id,
            min_trade_id=min_trade_id,
        )

        if trades_df is not None:
            # Обновляем глобальные переменные
            g_globals.trades_dataframe = trades_df
            logger.info(f'Updated trades dataframe: {trades_df.height} records')

        return trades_df

    async def update_bollinger_series(
        self,
        symbol_id: SymbolId,
    ) -> tuple[Series, Series, Series] | None:
        """Обновление полос Боллинджера из Redis."""
        logger.info(f'Updating bollinger series for {symbol_id.name}')

        bollinger_data = await self.redis_adapter.load_bollinger_bands(symbol_id)

        if bollinger_data is not None:
            upper_band, middle_band, lower_band = bollinger_data

            # Обновляем глобальные переменные
            g_globals.bollinger_upper_band_series = upper_band
            g_globals.bollinger_middle_band_series = middle_band
            g_globals.bollinger_lower_band_series = lower_band

            logger.info(f'Updated bollinger series for {symbol_id.name}')

        return bollinger_data

    async def update_candle_dataframe_by_interval_name_map(
        self,
        symbol_id: SymbolId,
    ) -> dict[str, DataFrame]:
        """Обновление свечных данных по интервалам из Redis."""
        logger.info(f'Updating candle dataframes for {symbol_id.name}')

        candle_dataframe_by_interval_name_map: dict[str, DataFrame] = {}

        # Загружаем данные для каждого интервала
        for interval_name in g_globals.interval_names:
            candles_df = await self.redis_adapter.load_candles_dataframe(
                symbol_id=symbol_id,
                interval=interval_name,
            )

            if candles_df is not None:
                candle_dataframe_by_interval_name_map[interval_name] = candles_df
                logger.info(
                    f'Loaded candles for {interval_name}: {candles_df.height} records'
                )

        # Обновляем глобальные переменные
        g_globals.candle_dataframe_by_interval_name_map = (
            candle_dataframe_by_interval_name_map
        )

        logger.info(
            f'Updated candle dataframes: {len(candle_dataframe_by_interval_name_map)} intervals'
        )
        return candle_dataframe_by_interval_name_map

    async def update_rsi_series(
        self,
        symbol_id: SymbolId,
    ) -> Series | None:
        """Обновление RSI данных из Redis."""
        logger.info(f'Updating RSI series for {symbol_id.name}')

        rsi_series = await self.redis_adapter.load_rsi_series(symbol_id)

        if rsi_series is not None:
            # Обновляем глобальные переменные
            g_globals.rsi_series = rsi_series
            logger.info(f'Updated RSI series for {symbol_id.name}')

        return rsi_series

    async def update_trades_smoothed_dataframe_by_level_map(
        self,
        symbol_id: SymbolId,
    ) -> dict[str, DataFrame]:
        """Обновление сглаженных данных по уровням из Redis."""
        logger.info(f'Updating smoothed dataframes for {symbol_id.name}')

        trades_smoothed_dataframe_by_level_map: dict[str, DataFrame] = {}

        # Загружаем данные для каждого уровня сглаживания
        for level in g_globals.trades_smoothing_levels:
            if level != 'Raw (0)':
                smoothed_df = await self.redis_adapter.load_smoothed_dataframe(
                    symbol_id=symbol_id,
                    level=level,
                )

                if smoothed_df is not None:
                    trades_smoothed_dataframe_by_level_map[level] = smoothed_df
                    logger.info(
                        f'Loaded smoothed data for {level}: {smoothed_df.height} records'
                    )

        # Обновляем глобальные переменные
        g_globals.trades_smoothed_dataframe_by_level_map = (
            trades_smoothed_dataframe_by_level_map
        )

        logger.info(
            f'Updated smoothed dataframes: {len(trades_smoothed_dataframe_by_level_map)} levels'
        )
        return trades_smoothed_dataframe_by_level_map

    async def update_extreme_lines(
        self,
        symbol_id: SymbolId,
    ) -> Any | None:  # numpy.ndarray | None
        """Обновление экстремальных линий из Redis."""
        logger.info(f'Updating extreme lines for {symbol_id.name}')

        extreme_lines_array = await self.redis_adapter.load_extreme_lines_array(
            symbol_id,
        )

        if extreme_lines_array is not None:
            # Обновляем глобальные переменные
            g_globals.extreme_lines_array = extreme_lines_array
            logger.info(f'Updated extreme lines for {symbol_id.name}')

        return extreme_lines_array

    async def update_order_book_volumes(
        self,
        symbol_id: SymbolId,
    ) -> tuple[
        Any | None, Any | None
    ]:  # tuple[numpy.ndarray | None, numpy.ndarray | None]
        """Обновление объемов стакана из Redis."""
        logger.info(f'Updating order book volumes for {symbol_id.name}')

        (
            asks_array,
            bids_array,
        ) = await self.redis_adapter.load_order_book_volumes_arrays(
            symbol_id,
        )

        if asks_array is not None and bids_array is not None:
            # Обновляем глобальные переменные
            g_globals.asks_array = asks_array
            g_globals.bids_array = bids_array
            logger.info(f'Updated order book volumes for {symbol_id.name}')

        return asks_array, bids_array

    async def update_velocity_series(
        self,
        symbol_id: SymbolId,
    ) -> Series | None:
        """Обновление данных скорости из Redis."""
        logger.info(f'Updating velocity series for {symbol_id.name}')

        velocity_series = await self.redis_adapter.load_velocity_series(symbol_id)

        if velocity_series is not None:
            # Обновляем глобальные переменные
            g_globals.velocity_series = velocity_series
            logger.info(f'Updated velocity series for {symbol_id.name}')

        return velocity_series

    async def update_current_available_symbol_name_set(
        self,
    ) -> set[str]:
        """Обновление списка доступных символов из Redis."""
        logger.info('Updating available symbols')

        symbol_names = await self.redis_adapter.load_available_symbols()
        current_available_symbol_name_set = set(symbol_names)

        # Обновляем глобальные переменные
        g_globals.current_available_symbol_name_set = current_available_symbol_name_set

        logger.info(
            f'Updated available symbols: {len(current_available_symbol_name_set)} symbols'
        )
        return current_available_symbol_name_set

    async def load_all_data_for_symbol(
        self,
        symbol_id: SymbolId,
        min_trade_id: int = 0,
    ) -> bool:
        """Загрузка всех данных для символа из Redis."""
        logger.info(f'Loading all data for {symbol_id.name}')

        try:
            # Загружаем все типы данных параллельно
            await asyncio.gather(
                self.update_trades_dataframe(
                    symbol_id,
                    min_trade_id,
                ),
                self.update_bollinger_series(
                    symbol_id,
                ),
                self.update_candle_dataframe_by_interval_name_map(
                    symbol_id,
                ),
                self.update_rsi_series(
                    symbol_id,
                ),
                self.update_trades_smoothed_dataframe_by_level_map(
                    symbol_id,
                ),
                self.update_extreme_lines(
                    symbol_id,
                ),
                self.update_order_book_volumes(
                    symbol_id,
                ),
                self.update_velocity_series(
                    symbol_id,
                ),
            )

            logger.info(f'Successfully loaded all data for {symbol_id.name}')
            return True

        except Exception as exception:
            logger.error(
                f'Error loading data for {symbol_id.name}'
                f': {"".join(traceback.format_exception(exception))}',
            )
            return False


# Глобальный экземпляр процессора
g_redis_processor = RedisFinPlotChartProcessor()
