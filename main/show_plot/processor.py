"""
Обновленный процессор для работы с данными из Redis.
"""

import asyncio
import logging
import traceback
from typing import Any

from chrono import (
    Timer,
)
from polars import (
    DataFrame,
    Series,
)

from constants.plot import (
    PlotConstants,
)
from constants.symbol import (
    SymbolConstants,
)
from enumerations import (
    SymbolId,
)
from main.show_plot.gui.window import FinPlotChartWindow
from main.show_plot.redis_data_adapter import g_redis_data_adapter
from utils.redis import g_redis_manager

logger = logging.getLogger(__name__)


class RedisChartProcessor:
    """Процессор для работы с данными из Redis."""

    __slots__ = (
        '__bollinger_base_line_series',
        '__bollinger_lower_band_series',
        '__bollinger_upper_band_series',
        '__candle_dataframe_by_interval_name_map',
        '__current_available_symbol_name_set',
        '__current_rsi_interval_name',
        '__current_symbol_name',
        '__current_trades_smoothing_level',
        '__extreme_lines_array',
        '__extreme_lines_position',
        '__extreme_lines_scale',
        '__max_trade_price',
        '__min_trade_price',
        '__order_book_volumes_asks_array',
        '__order_book_volumes_bids_array',
        '__order_book_volumes_position',
        '__order_book_volumes_scale',
        '__rsi_series',
        '__trades_dataframe',
        '__trades_smoothed_dataframe_by_level_map',
        '__velocity_series',
        '__window',
    )

    def __init__(self) -> None:
        super().__init__()

        self.__bollinger_base_line_series: Series | None = None
        self.__bollinger_lower_band_series: Series | None = None
        self.__bollinger_upper_band_series: Series | None = None
        self.__candle_dataframe_by_interval_name_map: dict[str, DataFrame] = {}
        self.__current_available_symbol_name_set: set[str] | None = None
        self.__current_rsi_interval_name: str | None = None
        self.__current_symbol_name: str | None = None
        self.__current_trades_smoothing_level: str | None = None
        self.__extreme_lines_array: Any | None = None
        self.__extreme_lines_position: tuple[float, float] | None = None
        self.__extreme_lines_scale: float | None = None
        self.__max_trade_price: float | None = None
        self.__min_trade_price: float | None = None
        self.__order_book_volumes_asks_array: Any | None = None
        self.__order_book_volumes_bids_array: Any | None = None
        self.__order_book_volumes_position: tuple[float, float] | None = None
        self.__order_book_volumes_scale: float | None = None
        self.__rsi_series: Series | None = None
        self.__trades_dataframe: DataFrame | None = None
        self.__trades_smoothed_dataframe_by_level_map: dict[str, DataFrame] = {}
        self.__velocity_series: Series | None = None
        self.__window: FinPlotChartWindow | None = None

    def get_bollinger_base_line_series(
        self,
    ) -> Series | None:
        return self.__bollinger_base_line_series

    def get_bollinger_lower_band_series(
        self,
    ) -> Series | None:
        return self.__bollinger_lower_band_series

    def get_bollinger_upper_band_series(
        self,
    ) -> Series | None:
        return self.__bollinger_upper_band_series

    def get_candle_dataframe_by_interval_name_map(
        self,
    ) -> dict[str, DataFrame]:
        return self.__candle_dataframe_by_interval_name_map

    async def get_current_available_symbol_names(
        self,
    ) -> list[str] | None:
        current_available_symbol_name_set = self.__current_available_symbol_name_set

        if current_available_symbol_name_set is None:
            return None

        return sorted(
            current_available_symbol_name_set,
        )

    def get_current_rsi_interval_name(
        self,
    ) -> str | None:
        return self.__current_rsi_interval_name

    def get_current_symbol_name(
        self,
    ) -> str | None:
        return self.__current_symbol_name

    def get_current_trades_smoothing_level(
        self,
    ) -> str | None:
        return self.__current_trades_smoothing_level

    def get_extreme_lines_array(
        self,
    ) -> Any | None:
        return self.__extreme_lines_array

    def get_extreme_lines_position(
        self,
    ) -> tuple[float, float] | None:
        return self.__extreme_lines_position

    def get_extreme_lines_scale(
        self,
    ) -> float | None:
        return self.__extreme_lines_scale

    def get_max_trade_price(
        self,
    ) -> float | None:
        return self.__max_trade_price

    def get_min_trade_price(
        self,
    ) -> float | None:
        return self.__min_trade_price

    def get_order_book_volumes_asks_array(
        self,
    ) -> Any | None:
        return self.__order_book_volumes_asks_array

    def get_order_book_volumes_bids_array(
        self,
    ) -> Any | None:
        return self.__order_book_volumes_bids_array

    def get_order_book_volumes_position(
        self,
    ) -> tuple[float, float] | None:
        return self.__order_book_volumes_position

    def get_order_book_volumes_scale(
        self,
    ) -> float | None:
        return self.__order_book_volumes_scale

    def get_rsi_series(
        self,
    ) -> Series | None:
        return self.__rsi_series

    def get_trades_dataframe(
        self,
    ) -> DataFrame | None:
        return self.__trades_dataframe

    def get_smoothed_dataframe(
        self,
    ) -> DataFrame | None:
        level = self.__current_trades_smoothing_level

        if level == 'Raw (0)':
            return self.__trades_dataframe

        return self.__trades_smoothed_dataframe_by_level_map.get(
            level,
        )

    def get_velocity_series(
        self,
    ) -> Series | None:
        return self.__velocity_series

    async def init(
        self,
    ) -> None:
        # init Redis

        await g_redis_manager.connect()

        # init the window

        window = FinPlotChartWindow(
            processor=self,
        )

        self.__window = window

        # show the window

        window.show()

        await window.plot(
            is_need_run_once=True,
        )

    async def update_current_rsi_interval_name(
        self,
        value: str,
    ) -> bool:
        if value == self.__current_rsi_interval_name:
            return False

        self.__current_rsi_interval_name = value

        self.__rsi_series = None

        # Обновляем RSI данные для текущего символа
        current_symbol_name = self.__current_symbol_name
        if current_symbol_name is not None:
            current_symbol_id = SymbolConstants.IdByName[current_symbol_name]

            await self.__update_rsi_series(
                current_symbol_id,
            )

        window = self.__window

        await window.plot(
            is_need_run_once=True,
        )

        return True

    async def update_current_symbol_name(
        self,
        value: str,
    ) -> bool:
        current_available_symbol_name_set = self.__current_available_symbol_name_set

        if current_available_symbol_name_set is None:
            return False

        if value not in current_available_symbol_name_set:
            return False

        if value == self.__current_symbol_name:
            return False

        self.__current_symbol_name = value

        # Очищаем все данные при смене символа
        self.__bollinger_base_line_series = None
        self.__bollinger_lower_band_series = None
        self.__bollinger_upper_band_series = None
        self.__candle_dataframe_by_interval_name_map.clear()
        self.__extreme_lines_array = None
        self.__extreme_lines_position = None
        self.__extreme_lines_scale = None
        self.__max_trade_price = None
        self.__min_trade_price = None
        self.__order_book_volumes_asks_array = None
        self.__order_book_volumes_bids_array = None
        self.__order_book_volumes_position = None
        self.__order_book_volumes_scale = None
        self.__rsi_series = None
        self.__trades_dataframe = None
        self.__trades_smoothed_dataframe_by_level_map.clear()
        self.__velocity_series = None

        # Загружаем данные для нового символа

        current_symbol_id = SymbolConstants.IdByName[value]
        await self.__load_all_data_for_symbol(current_symbol_id)

        window = self.__window

        await window.plot(
            is_need_run_once=True,
        )

        window.auto_range_price_plot()

        return True

    async def update_current_trades_smoothing_level(
        self,
        value: str,
    ) -> bool:
        if value == self.__current_trades_smoothing_level:
            return False

        self.__current_trades_smoothing_level = value

        window = self.__window

        await window.plot(
            is_need_run_once=True,
        )

        return True

    async def start_updating_loop(
        self,
    ) -> None:
        while True:
            try:
                with Timer() as timer:
                    await self.__update()

                logger.info(
                    f'Processor was updated by {timer.elapsed:.3f}s',
                )
            except Exception as exception:
                logger.error(
                    'Could not update processor'
                    ': handled exception'
                    f': {"".join(traceback.format_exception(exception))}'
                )

            await asyncio.sleep(
                # 1.0  # s
                # 60.0  # s
                60.0  # s
            )

    async def __update(
        self,
    ) -> None:
        await self.__update_current_available_symbol_name_set()

        # Обновляем данные только если выбран символ
        current_symbol_name = self.__current_symbol_name
        if current_symbol_name:
            current_symbol_id = SymbolConstants.IdByName[current_symbol_name]
            await self.__update_trades_dataframe(current_symbol_id)

    async def __update_trades_dataframe(
        self,
        symbol_id: SymbolId,
    ) -> DataFrame | None:
        """Обновление данных о сделках из Redis."""
        logger.info(f'Updating trades dataframe for {symbol_id.name}')

        trades_df = await g_redis_data_adapter.load_trades_dataframe(
            symbol_id=symbol_id,
        )

        if trades_df is not None:
            # Обновляем атрибуты процессора
            self.__trades_dataframe = trades_df
            logger.info(f'Updated trades dataframe: {trades_df.height} records')

        return trades_df

    async def __update_bollinger_series(
        self,
        symbol_id: SymbolId,
    ) -> tuple[Series, Series, Series] | None:
        """Обновление полос Боллинджера из Redis."""
        logger.info(f'Updating bollinger series for {symbol_id.name}')

        bollinger_data = await g_redis_data_adapter.load_bollinger_data(symbol_id)

        if bollinger_data is not None:
            upper_band, middle_band, lower_band = bollinger_data

            # Обновляем атрибуты процессора
            self.__bollinger_upper_band_series = upper_band
            self.__bollinger_base_line_series = middle_band
            self.__bollinger_lower_band_series = lower_band

            logger.info(f'Updated bollinger series for {symbol_id.name}')

        return bollinger_data

    async def __update_candle_dataframe_by_interval_name_map(
        self,
        symbol_id: SymbolId,
    ) -> None:
        """Обновление свечных данных по интервалам из Redis."""
        logger.info(f'Updating candle dataframes for {symbol_id.name}')

        candle_dataframe_by_interval_name_map: dict[str, DataFrame] = {}

        # Загружаем данные для каждого интервала
        for interval_name in PlotConstants.IntervalNames:
            candles_df = await g_redis_data_adapter.load_candle_dataframe(
                symbol_id=symbol_id,
                interval=interval_name,
            )

            if candles_df is not None:
                candle_dataframe_by_interval_name_map[interval_name] = candles_df
                logger.info(
                    f'Loaded candles for {interval_name}: {candles_df.height} records'
                )

        # Обновляем атрибуты процессора
        self.__candle_dataframe_by_interval_name_map = (
            candle_dataframe_by_interval_name_map
        )

        logger.info(
            f'Updated candle dataframes: {len(candle_dataframe_by_interval_name_map)} intervals'
        )

    async def __update_rsi_series(
        self,
        symbol_id: SymbolId,
    ) -> Series | None:
        """Обновление RSI данных из Redis."""
        logger.info(f'Updating RSI series for {symbol_id.name}')

        rsi_series = await g_redis_data_adapter.load_rsi_data(symbol_id)

        if rsi_series is not None:
            # Обновляем атрибуты процессора
            self.__rsi_series = rsi_series
            logger.info(f'Updated RSI series for {symbol_id.name}')

        return rsi_series

    async def __update_trades_smoothed_dataframe_by_level_map(
        self,
        symbol_id: SymbolId,
    ) -> None:
        """Обновление сглаженных данных по уровням из Redis."""
        logger.info(f'Updating smoothed dataframes for {symbol_id.name}')

        trades_smoothed_dataframe_by_level_map: dict[str, DataFrame] = {}

        # Загружаем данные для каждого уровня сглаживания
        for level in PlotConstants.TradesSmoothingLevels:
            if level != 'Raw (0)':
                smoothed_df = await g_redis_data_adapter.load_smoothed_dataframe(
                    symbol_id=symbol_id,
                    level=level,
                )

                if smoothed_df is not None:
                    trades_smoothed_dataframe_by_level_map[level] = smoothed_df
                    logger.info(
                        f'Loaded smoothed data for {level}: {smoothed_df.height} records'
                    )

        # Обновляем атрибуты процессора
        self.__trades_smoothed_dataframe_by_level_map = (
            trades_smoothed_dataframe_by_level_map
        )

        logger.info(
            f'Updated smoothed dataframes: {len(trades_smoothed_dataframe_by_level_map)} levels'
        )

    async def __update_extreme_lines(
        self,
        symbol_id: SymbolId,
    ) -> Any | None:  # numpy.ndarray | None
        """Обновление экстремальных линий из Redis."""
        logger.info(f'Updating extreme lines for {symbol_id.name}')

        extreme_lines_data = await g_redis_data_adapter.load_extreme_lines_data(
            symbol_id,
        )

        if extreme_lines_data is not None:
            extreme_lines_array, position, scale = extreme_lines_data

            print(
                f'position: {position}, scale: {scale}',
            )

            # Обновляем атрибуты процессора
            self.__extreme_lines_array = extreme_lines_array
            self.__extreme_lines_position = position
            self.__extreme_lines_scale = scale

            logger.info(f'Updated extreme lines for {symbol_id.name}')

        return extreme_lines_data

    async def __update_order_book_volumes(
        self,
        symbol_id: SymbolId,
    ) -> None:
        """Обновление объемов стакана из Redis."""
        logger.info(f'Updating order book volumes for {symbol_id.name}')

        order_book_data = await g_redis_data_adapter.load_order_book_volumes_data(
            symbol_id,
        )

        if order_book_data is not None:
            asks_array, bids_array, position, scale = order_book_data

            # Обновляем атрибуты процессора
            self.__order_book_volumes_asks_array = asks_array
            self.__order_book_volumes_bids_array = bids_array
            self.__order_book_volumes_position = position
            self.__order_book_volumes_scale = scale
            logger.info(f'Updated order book volumes for {symbol_id.name}')

    async def __update_velocity_series(
        self,
        symbol_id: SymbolId,
    ) -> Series | None:
        """Обновление данных скорости из Redis."""
        logger.info(f'Updating velocity series for {symbol_id.name}')

        velocity_series = await g_redis_data_adapter.load_velocity_data(symbol_id)

        if velocity_series is not None:
            # Обновляем атрибуты процессора
            self.__velocity_series = velocity_series
            logger.info(f'Updated velocity series for {symbol_id.name}')

        return velocity_series

    async def __update_current_available_symbol_name_set(
        self,
    ) -> None:
        """Обновление списка доступных символов из Redis."""
        logger.info('Updating available symbols')

        symbol_names = await g_redis_data_adapter.load_available_symbols()
        if symbol_names is None:
            symbol_names = []

        current_available_symbol_name_set = set(symbol_names)

        # Обновляем атрибуты процессора
        self.__current_available_symbol_name_set = current_available_symbol_name_set

        logger.info(
            f'Updated available symbols: {len(current_available_symbol_name_set)} symbols'
        )

        await self.__window.plot(
            is_need_run_once=True,
        )

    async def __load_all_data_for_symbol(
        self,
        symbol_id: SymbolId,
    ) -> bool:
        """Загрузка всех данных для символа из Redis."""
        logger.info(f'Loading all data for {symbol_id.name}')

        try:
            # Загружаем все типы данных параллельно
            await asyncio.gather(
                self.__update_trades_dataframe(
                    symbol_id,
                ),
                self.__update_bollinger_series(
                    symbol_id,
                ),
                self.__update_candle_dataframe_by_interval_name_map(
                    symbol_id,
                ),
                self.__update_rsi_series(
                    symbol_id,
                ),
                self.__update_trades_smoothed_dataframe_by_level_map(
                    symbol_id,
                ),
                self.__update_extreme_lines(
                    symbol_id,
                ),
                self.__update_order_book_volumes(
                    symbol_id,
                ),
                self.__update_velocity_series(
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
g_redis_processor = RedisChartProcessor()
