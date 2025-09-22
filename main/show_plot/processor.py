import asyncio
import logging
import traceback
import typing
from datetime import (
    timedelta,
)
from decimal import (
    Decimal,
)

from functools import (
    partial,
)

import numpy
import pandas
import talib

from pandas import (
    DataFrame,
    Series,
)

from chrono import (
    Timer,
)
from sqlalchemy import (
    and_,
    select,
    text,
)
from sqlalchemy.orm import (
    Session,
)

from constants.common import (
    CommonConstants,
)
from constants.plot import (
    PlotConstants,
)
from constants.symbol import SymbolConstants
from enumerations import (
    TradingDirection, SymbolId,
)
from main.save_trades.schemas import (
    OKXTradeData2,
)
from main.show_plot.globals import (
    g_globals,
)

from main.show_plot.gui.window import (
    FinPlotChartWindow,
)
from utils.trading import TradingUtils

logger = logging.getLogger(
    __name__,
)

_DEBUG_SMOOTHING_LEVEL = None
# _DEBUG_SMOOTHING_LEVEL = 'Smoothed (2)'


class FinPlotChartProcessor(object):
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
        '__line_dataframe_by_level_map',
        '__max_trade_price',
        '__min_trade_price',
        '__order_book_volumes_array',
        '__order_book_volumes_position',
        '__order_book_volumes_scale',
        '__rsi_series',
        '__test_analytics_raw_data_list',
        '__test_series',
        '__trades_dataframe',
        '__trades_dataframe_update_lock',
        '__trades_smoothed_dataframe_by_level_map',
        '__velocity_series',
        '__window',
    )

    def __init__(
        self,
    ) -> None:
        super().__init__()

        self.__bollinger_base_line_series: Series | None = None
        self.__bollinger_lower_band_series: Series | None = None
        self.__bollinger_upper_band_series: Series | None = None
        self.__candle_dataframe_by_interval_name_map: dict[str, pandas.DataFrame] = {}
        self.__current_available_symbol_name_set: set[str] | None = None
        self.__current_rsi_interval_name: str | None = None
        self.__current_symbol_name: str | None = None
        self.__current_trades_smoothing_level: str | None = None
        self.__extreme_lines_array: numpy.ndarray | None = None
        self.__extreme_lines_position: tuple[float, float] | None = None
        self.__extreme_lines_scale: float | None = None
        self.__line_dataframe_by_level_map: DataFrame | None = {}
        self.__max_trade_price: Decimal | None = None
        self.__min_trade_price: Decimal | None = None
        self.__order_book_volumes_array: numpy.ndarray | None = None
        self.__order_book_volumes_position: tuple[float, float] | None = None
        self.__order_book_volumes_scale: float | None = None
        self.__rsi_series: Series | None = None
        self.__test_analytics_raw_data_list: list[dict[str, typing.Any]] | None = None
        self.__test_series: Series | None = None
        self.__trades_dataframe: DataFrame | None = None
        self.__trades_dataframe_update_lock = asyncio.Lock()
        self.__trades_smoothed_dataframe_by_level_map: dict[str, DataFrame | None] = {}
        self.__velocity_series: Series | None = None
        self.__window: FinPlotChartWindow | None = None

    # async def fini(
    #         self,
    # ) -> None:
    #     # TODO: fini the window
    #
    #     await super().fini()

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
    ) -> dict[str, pandas.DataFrame]:
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

    def get_line_dataframe(
        self,
    ) -> DataFrame | None:
        return self.__line_dataframe_by_level_map.get(
            self.__current_trades_smoothing_level,
        )

    def get_max_trade_price(
        self,
    ) -> Decimal | None:
        return self.__max_trade_price

    def get_min_trade_price(
        self,
    ) -> Decimal | None:
        return self.__min_trade_price

    def get_extreme_lines_array(
            self,
    ) -> numpy.ndarray | None:
        return self.__extreme_lines_array

    def get_extreme_lines_position(
            self,
    ) -> tuple[float, float] | None:
        return self.__extreme_lines_position

    def get_extreme_lines_scale(
            self,
    ) -> float | None:
        return self.__extreme_lines_scale

    def get_order_book_volumes_array(
            self,
    ) -> numpy.ndarray | None:
        return self.__order_book_volumes_array

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

    def get_test_analytics_raw_data_list(
        self,
    ) -> list[dict[str, typing.Any]] | None:
        return self.__test_analytics_raw_data_list

    def get_test_series(
        self,
    ) -> Series | None:
        return self.__test_series

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
                60.0  # s
            )

    async def update_current_rsi_interval_name(
        self,
        value: str,
    ) -> bool:
        if value == self.__current_rsi_interval_name:
            return False

        self.__current_rsi_interval_name = value

        self.__rsi_series = None

        self.__update_rsi_series()

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

        self.__bollinger_base_line_series = None
        self.__bollinger_lower_band_series = None
        self.__bollinger_upper_band_series = None
        self.__candle_dataframe_by_interval_name_map.clear()
        self.__extreme_lines_array = None
        self.__extreme_lines_position = None
        self.__extreme_lines_scale = None
        self.__line_dataframe_by_level_map.clear()
        self.__max_trade_price = None
        self.__min_trade_price = None
        self.__order_book_volumes_array = None
        self.__order_book_volumes_position = None
        self.__order_book_volumes_scale = None
        self.__rsi_series = None
        self.__test_analytics_raw_data_list = None
        self.__test_series = None
        self.__trades_dataframe = None
        self.__trades_smoothed_dataframe_by_level_map.clear()
        self.__velocity_series = None

        await self.__update_trades_dataframe()

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

    async def __update(
        self,
    ) -> None:
        await self.__update_current_available_symbol_name_set()
        await self.__update_trades_dataframe()

    def __update_bollinger_series(
        self,
    ) -> None:
        trades_dataframe = self.__trades_dataframe

        assert trades_dataframe is not None, None

        (
            bollinger_upper_band_series,
            bollinger_base_line_series,
            bollinger_lower_band_series,
        ) = talib.BBANDS(  # noqa
            trades_dataframe.price,
            matype=(
                talib.MA_Type.SMA  # noqa
            ),
            timeperiod=20,
        )

        if not bollinger_base_line_series.size:
            assert not bollinger_lower_band_series.size, None
            assert not bollinger_upper_band_series.size, None

            return

        assert bollinger_lower_band_series.size, None
        assert bollinger_upper_band_series.size, None

        self.__bollinger_base_line_series = bollinger_base_line_series
        self.__bollinger_lower_band_series = bollinger_lower_band_series
        self.__bollinger_upper_band_series = bollinger_upper_band_series

    def __update_candle_dataframe_by_interval_name_map(
        self,
    ) -> None:
        trades_dataframe = self.__trades_dataframe

        assert trades_dataframe is not None, None

        candle_dataframe_by_interval_name_map = (
            self.__candle_dataframe_by_interval_name_map
        )

        for interval_name in PlotConstants.IntervalNames:
            candle_raw_data_list: list[dict[str, typing.Any]] = []

            interval_duration = CommonConstants.IntervalDurationByNameMap[interval_name]

            interval_duration_ms = int(
                interval_duration.total_seconds() * 1000,  # ms
            )

            last_candle_raw_data: dict[str, typing.Any] | None = None

            old_candle_dataframe = candle_dataframe_by_interval_name_map.get(
                interval_name,
            )

            min_trade_id: int

            if old_candle_dataframe is not None:
                min_pandas_trade_id = old_candle_dataframe.index.max()

                min_trade_id = int(
                    min_pandas_trade_id,
                )
            else:
                min_trade_id = 0

            for trade_data in trades_dataframe.loc[
                trades_dataframe.index >= min_trade_id
            ].itertuples():
                trade_id: int = trade_data.Index

                # if trade_id < min_trade_id:
                #     continue

                price: float = trade_data.price
                quantity: float = trade_data.quantity
                volume = price * quantity

                timestamp: pandas.Timestamp = trade_data.timestamp_ms

                timestamp_ms = timestamp.value // 10**6

                candle_start_timestamp_ms = timestamp_ms - (
                    timestamp_ms % interval_duration_ms
                )

                candle_end_timestamp_ms = (
                    candle_start_timestamp_ms + interval_duration_ms
                )

                if last_candle_raw_data is not None:
                    if (
                        candle_start_timestamp_ms
                        == last_candle_raw_data['start_timestamp_ms']
                    ):
                        # Update candle raw data

                        if price > last_candle_raw_data['high_price']:
                            last_candle_raw_data['high_price'] = price

                        if price < last_candle_raw_data['low_price']:
                            last_candle_raw_data['low_price'] = price

                        last_candle_raw_data.update(
                            {
                                'trades_count': (
                                    last_candle_raw_data['trades_count'] + 1
                                ),
                                'end_trade_id': trade_id,
                                'close_price': price,
                                'volume': last_candle_raw_data['volume'] + volume,
                            }
                        )
                    else:
                        # Flush candle raw data

                        candle_raw_data_list.append(
                            last_candle_raw_data,
                        )

                        last_candle_raw_data = None

                if last_candle_raw_data is None:
                    last_candle_raw_data = {
                        'close_price': price,
                        'end_timestamp_ms': candle_end_timestamp_ms,
                        'end_trade_id': trade_id + 1,
                        'high_price': price,
                        'low_price': price,
                        'open_price': price,
                        'start_timestamp_ms': candle_start_timestamp_ms,
                        'start_trade_id': trade_id,
                        'trades_count': 1,
                        'volume': volume,
                    }

            if last_candle_raw_data is not None:
                # Flush candle raw data

                candle_raw_data_list.append(
                    last_candle_raw_data,
                )

                last_candle_raw_data = None  # noqa

            new_candle_dataframe = DataFrame.from_records(
                candle_raw_data_list,
                columns=[
                    'close_price',
                    'end_timestamp_ms',
                    'end_trade_id',
                    'high_price',
                    'low_price',
                    'open_price',
                    'start_timestamp_ms',
                    'start_trade_id',
                    'trades_count',
                    'volume',
                ],
            )

            assert new_candle_dataframe.size, (
                min_trade_id,
                old_candle_dataframe,
            )

            new_candle_dataframe.end_timestamp_ms = pandas.to_datetime(
                new_candle_dataframe.end_timestamp_ms,
                unit='ms',
            )

            new_candle_dataframe.start_timestamp_ms = pandas.to_datetime(
                new_candle_dataframe.start_timestamp_ms,
                unit='ms',
            )

            new_candle_dataframe.set_index(
                'start_trade_id',
                inplace=True,
            )

            new_candle_dataframe.sort_values(
                'start_trade_id',
                inplace=True,
            )

            candle_dataframe: pandas.DataFrame

            if old_candle_dataframe is not None:
                old_candle_dataframe.update(
                    new_candle_dataframe,
                )

                candle_dataframe = old_candle_dataframe.combine_first(
                    new_candle_dataframe,
                )
            else:
                candle_dataframe = new_candle_dataframe

            candle_dataframe_by_interval_name_map[interval_name] = candle_dataframe

    async def __update_trades_dataframe(
        self,
    ) -> None:
        current_symbol_name = self.__current_symbol_name

        if current_symbol_name is None:
            return

        current_symbol_id = SymbolConstants.IdByName[current_symbol_name]

        async with self.__trades_dataframe_update_lock:
            trades_dataframe = self.__trades_dataframe

            min_trade_id: int  # TODO: min_trade_id

            if trades_dataframe is not None:
                min_pandas_trade_id = trades_dataframe.index.max()

                min_trade_id = int(
                    min_pandas_trade_id,
                )
            else:
                min_trade_id = 0

            postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

            async with postgres_db_session_maker() as session:
                async with session.begin():
                    new_trades_dataframe: DataFrame = await session.run_sync(
                        partial(
                            self.__fetch_trades_dataframe,
                            min_trade_id=min_trade_id,
                            symbol_id=current_symbol_id,
                        )
                    )

            if not new_trades_dataframe.size:
                return

            price_series: Series = new_trades_dataframe.price

            new_max_trade_price = price_series.max()

            if new_max_trade_price is not None:
                max_trade_price = self.__max_trade_price

                if max_trade_price is None or (max_trade_price < new_max_trade_price):
                    self.__max_trade_price = (
                        max_trade_price  # noqa
                    ) = new_max_trade_price

            new_min_trade_price = price_series.min()

            if new_min_trade_price is not None:
                min_trade_price = self.__min_trade_price

                if min_trade_price is None or (min_trade_price > new_min_trade_price):
                    self.__min_trade_price = (
                        min_trade_price  # noqa
                    ) = new_min_trade_price

            # new_trades_dataframe = DataFrame.from_records(
            #     # [
            #     #     {
            #     #         'price': 100000.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533919,
            #     #         'trade_id': 1,
            #     #     },
            #     #     {
            #     #         'price': 99900.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533920,
            #     #         'trade_id': 2,
            #     #     },
            #     #     {
            #     #         'price': 99900.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533921,
            #     #         'trade_id': 3,
            #     #     },
            #     #     {
            #     #         'price': 99800.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533922,
            #     #         'trade_id': 4,
            #     #     },
            #     #     {
            #     #         'price': 99800.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533923,
            #     #         'trade_id': 5,
            #     #     },
            #     #     {
            #     #         'price': 99800.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533924,
            #     #         'trade_id': 6,
            #     #     },
            #     #     {
            #     #         'price': 99700.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533925,
            #     #         'trade_id': 7,
            #     #     },
            #     #     {
            #     #         'price': 99700.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533926,
            #     #         'trade_id': 8,
            #     #     },
            #     #     {
            #     #         'price': 99600.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533927,
            #     #         'trade_id': 9,
            #     #     },
            #     #     {
            #     #         'price': 99600.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533928,
            #     #         'trade_id': 10,
            #     #     },
            #     #     {
            #     #         'price': 99500.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533929,
            #     #         'trade_id': 11,
            #     #     },
            #     #     {
            #     #         'price': 99600.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533930,
            #     #         'trade_id': 12,
            #     #     },
            #     # ],
            #     # [
            #     #     {
            #     #         'price': 115924.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533919,
            #     #         'trade_id': 1,
            #     #     },
            #     #     {
            #     #         'price': 115922.33,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533920,
            #     #         'trade_id': 2,
            #     #     },
            #     #     {
            #     #         'price': 115922.33,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533921,
            #     #         'trade_id': 3,
            #     #     },
            #     #     {
            #     #         'price': 115922.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533922,
            #     #         'trade_id': 4,
            #     #     },
            #     #     {
            #     #         'price': 115922.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533923,
            #     #         'trade_id': 5,
            #     #     },
            #     #     {
            #     #         'price': 115940.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533924,
            #     #         'trade_id': 6,
            #     #     },
            #     #     {
            #     #         'price': 115940.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533925,
            #     #         'trade_id': 7,
            #     #     },
            #     #     {
            #     #         'price': 115944.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533926,
            #     #         'trade_id': 8,
            #     #     },
            #     #     {
            #     #         'price': 115944.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533927,
            #     #         'trade_id': 9,
            #     #     },
            #     #     {
            #     #         'price': 115948.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533928,
            #     #         'trade_id': 10,
            #     #     },
            #     #     {
            #     #         'price': 115948.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533929,
            #     #         'trade_id': 11,
            #     #     },
            #     #     {
            #     #         'price': 115952.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533930,
            #     #         'trade_id': 12,
            #     #     },
            #     #     {
            #     #         'price': 115952.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533931,
            #     #         'trade_id': 13,
            #     #     },
            #     #     {
            #     #         'price': 115954.9,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533932,
            #     #         'trade_id': 14,
            #     #     },
            #     #     {
            #     #         'price': 115952.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533933,
            #     #         'trade_id': 15,
            #     #     },
            #     # ],
            #     # [
            #     #     {
            #     #         'price': 115430.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533919,
            #     #         'trade_id': 1,
            #     #     },
            #     #     {
            #     #         'price': 116150.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533920,
            #     #         'trade_id': 2,
            #     #     },
            #     #     {
            #     #         'price': 115800.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533921,
            #     #         'trade_id': 3,
            #     #     },
            #     #     {
            #     #         'price': 116300.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533922,
            #     #         'trade_id': 4,
            #     #     },
            #     #     {
            #     #         'price': 115000.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533923,
            #     #         'trade_id': 5,
            #     #     },
            #     #     {
            #     #         'price': 115600.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533924,
            #     #         'trade_id': 6,
            #     #     },
            #     #     {
            #     #         'price': 115050.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533925,
            #     #         'trade_id': 7,
            #     #     },
            #     #     {
            #     #         'price': 115700.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533926,
            #     #         'trade_id': 8,
            #     #     },
            #     #     {
            #     #         'price': 115500.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533927,
            #     #         'trade_id': 9,
            #     #     },
            #     #     {
            #     #         'price': 115735.0,
            #     #         'quantity': 1.0,
            #     #         'timestamp_ms': 1758136533928,
            #     #         'trade_id': 10,
            #     #     },
            #     # ],
            #     columns=[
            #         'price',
            #         'quantity',
            #         'timestamp_ms',
            #         'trade_id',
            #     ],
            # )

            # new_trades_dataframe = pandas.read_csv(
            #     'data/1.csv',
            # )

            # assert new_trades_dataframe.size, None
            #
            # new_trades_dataframe.timestamp_ms = pandas.to_datetime(
            #     new_trades_dataframe.timestamp_ms,
            #     unit='ms',
            # )
            #
            # new_trades_dataframe.set_index(
            #     'trade_id',
            #     inplace=True,
            # )
            #
            # new_trades_dataframe.sort_values(
            #     'trade_id',
            #     inplace=True,
            # )

            if trades_dataframe is not None:
                trades_dataframe.update(
                    new_trades_dataframe,
                )

                trades_dataframe = trades_dataframe.combine_first(
                    new_trades_dataframe,
                )
            else:
                trades_dataframe = new_trades_dataframe

            self.__trades_dataframe = trades_dataframe

        with Timer() as timer:
            self.__update_bollinger_series()

        logger.info(
            f'Bollinger series were updated by {timer.elapsed:.3f}s',
        )

        with Timer() as timer:
            self.__update_candle_dataframe_by_interval_name_map()

        logger.info(
            f'Candle dataframe by interval name map was updated by {timer.elapsed:.3f}s'
        )

        with Timer() as timer:
            self.__update_rsi_series()

        logger.info(
            f'RSI series were updated by {timer.elapsed:.3f}s',
        )

        with Timer() as timer:
            self.__update_test_series()

        logger.info(
            f'Test series were updated by {timer.elapsed:.3f}s',
        )

        with Timer() as timer:
            self.__update_trades_smoothed_dataframe_by_level_map()

        logger.info(
            f'Trades smoothed dataframe by level map was updated by {timer.elapsed:.3f}s'
        )

        with Timer() as timer:
            self.__update_extreme_lines()

        logger.info(
            f'Extreme lines were updated by {timer.elapsed:.3f}s'
        )

        with Timer() as timer:
            self.__update_order_book_volumess()

        logger.info(
            f'Order book volumes were updated by {timer.elapsed:.3f}s'
        )

        with Timer() as timer:
            self.__update_velocity_series()

        logger.info(
            f'Velocity series were updated by {timer.elapsed:.3f}s',
        )

        await self.__window.plot(
            is_need_run_once=True,
        )

    @staticmethod
    def __fetch_trades_dataframe(
            session: Session,

            min_trade_id: int,
            symbol_id: SymbolId,
    ) -> DataFrame | None:
        connection = session.connection()

        trades_dataframe_chunks: list[DataFrame] = []

        for trades_dataframe_chunk in pandas.read_sql_query(
            select(
                OKXTradeData2,
            )
            .where(
                and_(
                    OKXTradeData2.symbol_id == symbol_id,
                    OKXTradeData2.trade_id >= min_trade_id,
                )
            )
            .order_by(
                OKXTradeData2.symbol_id.asc(),
                OKXTradeData2.trade_id.desc(),
            )
            .limit(
                2_000_000,
                # 1_000_000,
                # 500_000,
                # 50_000,
                # 10_000
                # 1_000
                # 50
            ),
            con=connection,
            chunksize=10_000,
        ):
            trades_dataframe_chunk.timestamp_ms = pandas.to_datetime(
                trades_dataframe_chunk.timestamp_ms,
                unit='ms',
            )

            trades_dataframe_chunk.set_index(
                'trade_id',
                inplace=True,
            )

            trades_dataframe_chunks.append(
                trades_dataframe_chunk,
            )

            print(
                f'Fetched {len(trades_dataframe_chunks)} chunks'
            )

        trades_dataframe = pandas.concat(
            trades_dataframe_chunks
        )

        trades_dataframe.sort_values(
            'trade_id',
            inplace=True,
        )

        return trades_dataframe

    async def __update_current_available_symbol_name_set(
        self,
    ) -> None:
        current_available_symbol_name_set: set[str] | None = None

        postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

        async with postgres_db_session_maker() as session:
            recursive_cte_full_query = text(
                """
WITH RECURSIVE symbol_id_cte(symbol_id) AS 
(
  (
    SELECT okx_trade_data_2.symbol_id AS symbol_id 
    FROM okx_trade_data_2 ORDER BY okx_trade_data_2.symbol_id ASC 
    LIMIT 1
  )
  UNION ALL
  SELECT (
    SELECT symbol_id
    FROM okx_trade_data_2
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

        self.__current_available_symbol_name_set = current_available_symbol_name_set

        window = self.__window

        await window.plot(
            is_need_run_once=True,
        )

        # window.auto_range_price_plot()

    def __update_extreme_lines(
            self,
    ) -> None:
        trades_dataframe = self.__trades_dataframe

        assert trades_dataframe is not None, None

        price_series = trades_dataframe.price
        trade_id_series = trades_dataframe.index

        max_price = float(
            price_series.max()
        )

        max_trade_id = int(
            trade_id_series.max()
        )

        min_price = float(
            price_series.min()
        )

        min_trade_id = int(
            trade_id_series.min()
        )

        delta_price = max_price - min_price

        if not delta_price:
            return

        delta_trade_id = max_trade_id - min_trade_id

        if not delta_trade_id:
            return

        logger.info(
            f'delta_price: {delta_price}, delta_trade_id: {delta_trade_id})'
        )

        # delta_price / delta_trade_id = height / width;
        # width = height * (delta_trade_id / delta_price);

        aspect_ratio = delta_trade_id / delta_price

        height = int(
            100  # delta_price / 10,
        )

        extreme_lines_scale = (
            delta_price /
            height
        )

        width = int(
            height * aspect_ratio,
            # delta_trade_id / 10,
        )

        logger.info(
            f'Creating extreme line array ({width} x {height}, scale {extreme_lines_scale}, aspect ratio {aspect_ratio})'
        )

        extreme_lines_array = numpy.zeros((
            width,
            height,
        ))

        logger.info(
            'Filling extreme line array...'
        )

        line_dataframe_by_level_map = self.__line_dataframe_by_level_map

        first_line_dataframe: DataFrame = line_dataframe_by_level_map['Smoothed (1)']

        active_extreme_line_raw_data_by_price_map: dict[float, dict[str, typing.Any]] = {}
        extreme_line_raw_data_list: list[dict[str, typing.Any]] = []

        for line_data in first_line_dataframe.itertuples():
            end_trade_id = int(
                line_data.end_trade_id
            )

            start_trade_id = int(
                line_data.Index
            )

            end_price = line_data.end_price
            start_price = line_data.start_price

            left_price = min(
                end_price,
                start_price,
            )

            right_price = max(
                end_price,
                start_price,
            )

            for price, extreme_line_raw_data in tuple(
                    active_extreme_line_raw_data_by_price_map.items(),
            ):
                if not (left_price <= price <= right_price):
                    continue

                active_extreme_line_raw_data_by_price_map.pop(
                    price,
                )

                extreme_line_raw_data.update({
                    'end_trade_id': start_trade_id,  # end_trade_id,
                    'price': price,
                })

                extreme_line_raw_data_list.append(
                    extreme_line_raw_data,
                )

            assert end_price not in active_extreme_line_raw_data_by_price_map, (end_price,)
            assert start_price not in active_extreme_line_raw_data_by_price_map, (start_price,)

            active_extreme_line_raw_data_by_price_map.update({
                end_price: {
                    'end_trade_id': None,
                    'start_trade_id': end_trade_id,
                },

                start_price: {
                    'end_trade_id': None,
                    'start_trade_id': start_trade_id,
                },
            })

        for price, extreme_line_raw_data in (
                active_extreme_line_raw_data_by_price_map.items()
        ):
            extreme_line_raw_data.update({
                'end_trade_id': max_trade_id,
                'price': price,
            })

            extreme_line_raw_data_list.append(
                extreme_line_raw_data,
            )

        active_extreme_line_raw_data_by_price_map.clear()

        for extreme_line_raw_data in extreme_line_raw_data_list:
            end_trade_id: int = extreme_line_raw_data['end_trade_id']
            price: float = extreme_line_raw_data['price']
            start_trade_id: int = extreme_line_raw_data['start_trade_id']

            end_x = int(
                (
                    end_trade_id - min_trade_id
                ) /
                extreme_lines_scale
            )

            start_x = int(
                (
                    start_trade_id - min_trade_id
                ) /
                extreme_lines_scale
            )

            y = min(
                int(
                    (
                        price - min_price
                    ) /
                    extreme_lines_scale
                ),
                height - 1
            )

            logger.info(
                f'Price: {price}, start_trade_id: {start_trade_id}, end_trade_id: {end_trade_id}, start_x: {start_x}, end_x: {end_x}, y: {y}'
            )

            # for x in range(start_x, end_x):
            #     extreme_lines_array[x, y] = x - start_x

            extreme_lines_array[start_x: end_x, y] = numpy.arange(
                end_x - start_x,
            )

        self.__extreme_lines_array = extreme_lines_array

        self.__extreme_lines_position = (
            float(
                min_trade_id,
            ),
            float(
                min_price,
            )
        )

        self.__extreme_lines_scale = extreme_lines_scale

    def __update_order_book_volumes(
            self,
    ) -> None:
        trades_dataframe = self.__trades_dataframe

        assert trades_dataframe is not None, None

        price_series = trades_dataframe.price
        trade_id_series = trades_dataframe.index

        max_price = float(
            price_series.max()
        )

        max_trade_id = int(
            trade_id_series.max()
        )

        min_price = float(
            price_series.min()
        )

        min_trade_id = int(
            trade_id_series.min()
        )

        delta_price = max_price - min_price

        if not delta_price:
            return

        delta_trade_id = max_trade_id - min_trade_id

        if not delta_trade_id:
            return

        logger.info(
            f'delta_price: {delta_price}, delta_trade_id: {delta_trade_id})'
        )

        # delta_price / delta_trade_id = height / width;
        # width = height * (delta_trade_id / delta_price);

        aspect_ratio = delta_trade_id / delta_price

        height = int(
            100  # delta_price / 10,
        )

        order_book_volumes_scale = (
            delta_price /
            height
        )

        width = int(
            height * aspect_ratio,
            # delta_trade_id / 10,
        )

        logger.info(
            f'Creating order book volume array ({width} x {height}, scale {order_book_volumes_scale}, aspect ratio {aspect_ratio})'
        )

        order_book_volumes_array = numpy.zeros((
            width,
            height,
        ))

        logger.info(
            'Filling order book volumes array...'
        )

        # TODO

        self.__order_book_volumes_array = order_book_volumes_array

        self.__order_book_volumes_position = (
            float(
                min_trade_id,
            ),
            float(
                min_price,
            )
        )

        self.__order_book_volumes_scale = order_book_volumes_scale

    def __update_rsi_series(
        self,
    ) -> None:
        current_rsi_interval_name = self.__current_rsi_interval_name

        if current_rsi_interval_name is None:
            return

        candle_dataframe_by_interval_name_map = (
            self.__candle_dataframe_by_interval_name_map
        )

        candle_dataframe = candle_dataframe_by_interval_name_map.get(
            current_rsi_interval_name,
        )

        if candle_dataframe is None:
            return

        rsi_series = talib.RSI(  # noqa
            candle_dataframe.close_price,
            timeperiod=14,  # 6
        )

        if not rsi_series.size:
            return

        self.__rsi_series = rsi_series

    def __update_test_series(
        self,
    ) -> None:
        trades_dataframe = self.__trades_dataframe

        assert trades_dataframe is not None, None

        return  # TODO

        prices: list[float] = []
        start_timestamps: list[pandas.Timestamp] = []

        for trade_data in trades_dataframe.itertuples():
            start_timestamp: pandas.Timestamp = trade_data.Index

            trade_close_price = trade_data.close
            trade_high_price = trade_data.high
            trade_low_price = trade_data.low
            trade_open_price = trade_data.open

            is_bull = trade_close_price >= trade_open_price

            second_price: float
            third_price: float

            if is_bull:
                second_price = trade_low_price
                third_price = trade_high_price
            else:
                second_price = trade_high_price
                third_price = trade_low_price

            prices.append(
                trade_open_price,
            )

            start_timestamps.append(
                start_timestamp,
            )

            prices.append(
                second_price,
            )

            start_timestamps.append(
                start_timestamp
                + timedelta(
                    minutes=3,
                    seconds=45,
                ),
            )

            prices.append(
                third_price,
            )

            start_timestamps.append(
                start_timestamp
                + timedelta(
                    minutes=7,
                    seconds=30,
                ),
            )

            prices.append(
                trade_close_price,
            )

            start_timestamps.append(
                start_timestamp
                + timedelta(
                    minutes=11,
                    seconds=15,
                ),
            )

        prices_2: list[float] = []
        start_timestamps_2: list[pandas.Timestamp] = []

        for _ in range(10):
            self.__process_1(
                first_item_idx=0,
                prices=prices,
                prices_2=prices_2,
                start_timestamps=start_timestamps,
                start_timestamps_2=start_timestamps_2,
            )

            prices.clear()
            start_timestamps.clear()

            self.__process_1(
                first_item_idx=1,
                prices=prices_2,
                prices_2=prices,
                start_timestamps=start_timestamps_2,
                start_timestamps_2=start_timestamps,
            )

            prices_2.clear()
            start_timestamps_2.clear()

        prices_3: list[float] = []
        start_timestamps_3: list[pandas.Timestamp] = []

        for _ in range(0):
            self.__process_2(
                first_item_idx=0,
                prices=prices,
                prices_2=prices_2,
                start_timestamps=start_timestamps,
                start_timestamps_2=start_timestamps_2,
            )

            prices.clear()
            start_timestamps.clear()

            self.__process_2(
                first_item_idx=1,
                prices=prices_2,
                prices_2=prices_3,
                start_timestamps=start_timestamps_2,
                start_timestamps_2=start_timestamps_3,
            )

            prices_2.clear()
            start_timestamps_2.clear()

            self.__process_2(
                first_item_idx=2,
                prices=prices_3,
                prices_2=prices,
                start_timestamps=start_timestamps_3,
                start_timestamps_2=start_timestamps,
            )

            prices_3.clear()
            start_timestamps_3.clear()

        if not start_timestamps:
            return

        test_analytics_raw_data_list: list[dict[str, typing.Any]] = []

        for first_item_idx in range(len(start_timestamps) - 1):
            second_item_idx = first_item_idx + 1

            first_price = prices[first_item_idx]
            first_start_timestamp = start_timestamps[first_item_idx]

            second_price = prices[second_item_idx]
            second_start_timestamp = start_timestamps[second_item_idx]

            is_bull = second_price >= first_price

            test_analytics_raw_data_list.append(
                {
                    'is_bull': is_bull,
                    'first_price': first_price,
                    'first_start_timestamp': first_start_timestamp,
                    'second_price': second_price,
                    'second_start_timestamp': second_start_timestamp,
                }
            )

        self.__test_analytics_raw_data_list = test_analytics_raw_data_list

        price_by_start_timestamp_map = {
            timestamp: price
            for timestamp, price in zip(
                start_timestamps,
                prices,
            )
        }

        prices_final: list[float] = []
        start_timestamps_final: list[pandas.Timestamp] = []

        start_timestamp = start_timestamps[0]
        end_timestamp = start_timestamps[-1]

        old_price: float | None = None
        old_start_timestamp: pandas.Timestamp | None = None

        while start_timestamp <= end_timestamp:
            price = price_by_start_timestamp_map.get(
                start_timestamp,
            )

            if price is None:
                assert old_price is not None, None
                assert old_start_timestamp is not None, None

                next_price: float | None = None
                next_timestamp = start_timestamp

                while next_timestamp <= end_timestamp:
                    next_price = price_by_start_timestamp_map.get(
                        next_timestamp,
                    )

                    if next_price is not None:
                        break

                    next_timestamp += timedelta(
                        minutes=3,
                        seconds=45,
                    )

                if next_price is not None:
                    coefficient = (
                        start_timestamp.timestamp() - old_start_timestamp.timestamp()
                    ) / (next_timestamp.timestamp() - old_start_timestamp.timestamp())

                    price = old_price + coefficient * (next_price - old_price)
                else:
                    price = old_price

                price_by_start_timestamp_map[start_timestamp] = price
            else:
                old_price = price
                old_start_timestamp = start_timestamp

            prices_final.append(
                price,
            )

            start_timestamps_final.append(
                start_timestamp,
            )

            start_timestamp += timedelta(
                minutes=3,
                seconds=45,
            )

        test_series = Series(
            prices_final,
            start_timestamps_final,
        )

        if not test_series.size:
            return

        self.__test_series = test_series

    def __update_trades_smoothed_dataframe_by_level_map(self) -> None:
        line_dataframe_by_level_map = self.__line_dataframe_by_level_map

        trades_dataframe = self.__trades_dataframe

        assert trades_dataframe is not None, None

        trades_smoothed_dataframe_by_level_map = (
            self.__trades_smoothed_dataframe_by_level_map
        )

        previous_line_dataframe: DataFrame | None = None

        for smoothing_level in PlotConstants.TradesSmoothingLevels:
            is_raw_level = smoothing_level == 'Raw (0)'

            if is_raw_level:
                continue

            is_first_level = smoothing_level == 'Smoothed (1)'

            line_dataframe: DataFrame
            line_raw_data: dict[str, typing.Any] | None = None
            line_raw_data_list: list[dict[str, typing.Any]] = []

            old_line_dataframe: DataFrame | None

            if is_first_level:
                old_line_dataframe = line_dataframe_by_level_map.get(
                    smoothing_level,
                )

                min_start_trade_id: int

                if old_line_dataframe is not None:
                    min_pandas_start_trade_id = old_line_dataframe.index.max()

                    min_start_trade_id = int(
                        min_pandas_start_trade_id,
                    )
                else:
                    min_start_trade_id = 0

                for trade_data in trades_dataframe.loc[
                    trades_dataframe.index >= min_start_trade_id
                ].itertuples():
                    trade_id: int = trade_data.Index

                    # if trade_id < min_trade_id:
                    #     continue

                    price: float = trade_data.price
                    quantity: float = trade_data.quantity
                    volume = price * quantity

                    timestamp: pandas.Timestamp = trade_data.timestamp_ms

                    # timestamp_ms = timestamp.value // 10 ** 6

                    if line_raw_data is None:
                        line_raw_data = {
                            'start_timestamp': timestamp,
                            'start_trade_id': trade_id,
                            'start_price': price,
                            'quantity': quantity,
                            'trading_direction': None,
                            'volume': volume,
                        }
                    else:
                        old_trading_direction: TradingDirection | None = line_raw_data[
                            'trading_direction'
                        ]

                        if old_trading_direction is None:
                            start_price: float = line_raw_data['start_price']
                            end_price = price

                            new_trading_direction = TradingUtils.get_direction(
                                start_price,
                                end_price,
                            )

                            line_raw_data.update(
                                {
                                    'end_price': end_price,
                                    'end_timestamp': timestamp,
                                    'end_trade_id': trade_id,
                                    'quantity': (line_raw_data['quantity'] + quantity),
                                    'trading_direction': new_trading_direction,
                                    'volume': (line_raw_data['volume'] + volume),
                                },
                            )
                        else:
                            new_end_price = price
                            old_end_price: float = line_raw_data['end_price']

                            new_trading_direction = TradingUtils.get_direction(
                                old_end_price,
                                new_end_price,
                            )

                            if (
                                old_trading_direction == new_trading_direction
                                or new_trading_direction == TradingDirection.Cross
                            ):
                                line_raw_data.update(
                                    {
                                        'quantity': (
                                            line_raw_data['quantity'] + quantity
                                        ),
                                        'volume': line_raw_data['volume'] + volume,
                                    },
                                )
                            else:
                                # Flush

                                line_raw_data_list.append(
                                    line_raw_data,
                                )

                                line_raw_data = {
                                    'start_timestamp': line_raw_data['end_timestamp'],
                                    'start_trade_id': line_raw_data['end_trade_id'],
                                    'start_price': old_end_price,
                                    'quantity': quantity,
                                    'trading_direction': new_trading_direction,
                                    'volume': volume,
                                }

                            line_raw_data.update(
                                {
                                    'end_price': new_end_price,
                                    'end_timestamp': timestamp,
                                    'end_trade_id': trade_id,
                                }
                            )

                if line_raw_data is not None:
                    trading_direction: TradingDirection | None = line_raw_data[
                        'trading_direction'
                    ]

                    if trading_direction is not None:
                        # Flush

                        line_raw_data_list.append(
                            line_raw_data,
                        )

                        line_raw_data = None  # noqa
            else:
                old_line_dataframe = None

                assert previous_line_dataframe is not None, None

                line_raw_data_1: dict[str, typing.Any] | None = None
                line_raw_data_list_1: list[dict[str, typing.Any]] = []

                first_line_data: typing.NamedTuple | None = None
                second_line_data: typing.NamedTuple | None = None

                # for line_data in previous_line_dataframe.loc[
                #     previous_line_dataframe.index >= min_start_trade_id
                # ].itertuples():
                for line_data in previous_line_dataframe.itertuples():
                    high_price: float
                    trading_direction: TradingDirection
                    low_price: float

                    if line_raw_data_1 is None:
                        end_price: float = line_data.end_price
                        high_price = end_price
                        start_price: float = line_data.start_price
                        low_price: float = start_price

                        trading_direction: TradingDirection = (
                            line_data.trading_direction
                        )

                        line_raw_data_1 = {
                            'end_timestamp': line_data.end_timestamp,
                            'end_trade_id': line_data.end_trade_id,
                            'end_price': end_price,
                            'high_price': high_price,
                            'low_price': low_price,
                            'start_timestamp': line_data.start_timestamp,
                            'start_trade_id': line_data.Index,
                            'start_price': start_price,
                            'quantity': line_data.quantity,
                            'trading_direction': trading_direction,
                            'volume': line_data.volume,
                        }

                        if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                            print('Line raw data init', line_raw_data_1)

                        # if trading_direction == TradingDirection.Cross:
                        #     # Flush
                        #
                        #     if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        #         print(
                        #             'Flushing 1',
                        #             line_raw_data_1,
                        #         )
                        #
                        #     line_raw_data_list_1.append(
                        #         line_raw_data_1,
                        #     )
                        #
                        #     line_raw_data_1 = None

                        continue

                    if line_data.trading_direction == TradingDirection.Cross:
                        # Combine

                        line_raw_data_1.update(
                            {
                                'end_timestamp': line_data.end_timestamp,
                                'end_trade_id': line_data.end_trade_id,
                                'end_price': line_data.end_price,
                                # 'high_price': high_price,
                                # 'low_price': low_price,
                                'quantity': line_raw_data_1['quantity']
                                + line_data.quantity,
                                'volume': line_raw_data_1['volume'] + line_data.volume,
                            },
                        )

                        continue

                    if first_line_data is None:
                        first_line_data = line_data

                        continue
                    elif second_line_data is None:
                        second_line_data = line_data

                    high_price: float = line_raw_data_1['high_price']
                    low_price: float = line_raw_data_1['low_price']

                    trading_direction: TradingDirection = line_raw_data_1[
                        'trading_direction'
                    ]

                    if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        print('Line data:', line_data)

                    assert first_line_data.trading_direction != trading_direction, (
                        first_line_data,
                        line_raw_data_1,
                        second_line_data,
                        smoothing_level,
                    )

                    if second_line_data.trading_direction != trading_direction:
                        # Flush

                        if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                            print('Flushing 2', line_raw_data_1)

                        line_raw_data_list_1.append(
                            line_raw_data_1,
                        )

                        end_price: float = first_line_data.end_price
                        high_price = end_price
                        start_price: float = first_line_data.start_price
                        low_price: float = start_price

                        trading_direction: TradingDirection = (
                            first_line_data.trading_direction
                        )

                        line_raw_data_1 = {
                            'end_timestamp': first_line_data.end_timestamp,
                            'end_trade_id': first_line_data.end_trade_id,
                            'end_price': end_price,
                            'high_price': high_price,
                            'low_price': low_price,
                            'start_timestamp': first_line_data.start_timestamp,
                            'start_trade_id': first_line_data.Index,
                            'start_price': start_price,
                            'quantity': first_line_data.quantity,
                            'trading_direction': trading_direction,
                            'volume': first_line_data.volume,
                        }

                        # if trading_direction == TradingDirection.Cross:
                        #     # Flush
                        #
                        #     if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        #         print(
                        #             'Flushing 3',
                        #             line_raw_data_1,
                        #         )
                        #
                        #     line_raw_data_list_1.append(
                        #         line_raw_data_1,
                        #     )
                        #
                        #     line_raw_data_1 = None

                        first_line_data = second_line_data
                        second_line_data = None

                        continue

                    # Check low price update direction

                    new_low_price = first_line_data.end_price

                    low_price_direction = TradingUtils.get_direction(
                        low_price,
                        new_low_price,
                    )

                    if (
                        low_price_direction != trading_direction
                        and low_price_direction != TradingDirection.Cross
                    ):
                        # Flush

                        if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                            print('Flushing 4', line_raw_data_1)

                        line_raw_data_list_1.append(
                            line_raw_data_1,
                        )

                        end_price: float = first_line_data.end_price
                        high_price = end_price
                        start_price: float = first_line_data.start_price
                        low_price: float = start_price

                        trading_direction: TradingDirection = (
                            first_line_data.trading_direction
                        )

                        line_raw_data_1 = {
                            'end_timestamp': first_line_data.end_timestamp,
                            'end_trade_id': first_line_data.end_trade_id,
                            'end_price': end_price,
                            'high_price': high_price,
                            'low_price': low_price,
                            'start_timestamp': first_line_data.start_timestamp,
                            'start_trade_id': first_line_data.Index,
                            'start_price': start_price,
                            'quantity': first_line_data.quantity,
                            'trading_direction': trading_direction,
                            'volume': first_line_data.volume,
                        }

                        # if trading_direction == TradingDirection.Cross:
                        #     # Flush
                        #
                        #     if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        #         print(
                        #             'Flushing 5',
                        #             line_raw_data_1,
                        #         )
                        #
                        #     line_raw_data_list_1.append(
                        #         line_raw_data_1,
                        #     )
                        #
                        #     line_raw_data_1 = None

                        first_line_data = second_line_data
                        second_line_data = None

                        continue

                    # Check high price update direction

                    new_high_price = second_line_data.end_price

                    high_price_direction = TradingUtils.get_direction(
                        high_price,
                        new_high_price,
                    )

                    if (
                        high_price_direction != trading_direction
                        and high_price_direction != TradingDirection.Cross
                    ):
                        # Flush

                        if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                            print('Flushing 6', line_raw_data_1)

                        line_raw_data_list_1.append(
                            line_raw_data_1,
                        )

                        end_price: float = first_line_data.end_price
                        high_price = end_price
                        start_price: float = first_line_data.start_price
                        low_price: float = start_price

                        trading_direction: TradingDirection = (
                            first_line_data.trading_direction
                        )

                        line_raw_data_1 = {
                            'end_timestamp': first_line_data.end_timestamp,
                            'end_trade_id': first_line_data.end_trade_id,
                            'end_price': end_price,
                            'high_price': high_price,
                            'low_price': low_price,
                            'start_timestamp': first_line_data.start_timestamp,
                            'start_trade_id': first_line_data.Index,
                            'start_price': start_price,
                            'quantity': first_line_data.quantity,
                            'trading_direction': trading_direction,
                            'volume': first_line_data.volume,
                        }

                        # if trading_direction == TradingDirection.Cross:
                        #     # Flush
                        #
                        #     if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        #         print(
                        #             'Flushing 7',
                        #             line_raw_data_1,
                        #         )
                        #
                        #     line_raw_data_list_1.append(
                        #         line_raw_data_1,
                        #     )
                        #
                        #     line_raw_data_1 = None

                        first_line_data = second_line_data
                        second_line_data = None

                        continue

                    # Update low price

                    if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        print('Update low price to', new_low_price)

                    line_raw_data_1.update(
                        {
                            'low_price': new_low_price,
                        }
                    )

                    # Combine line data

                    line_raw_data_1.update(
                        {
                            'end_timestamp': first_line_data.end_timestamp,
                            'end_trade_id': first_line_data.end_trade_id,
                            'end_price': first_line_data.end_price,
                            'quantity': line_raw_data_1['quantity']
                            + first_line_data.quantity,
                            'volume': line_raw_data_1['quantity']
                            + first_line_data.volume,
                        }
                    )

                    # Update high price

                    if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        print('Update high price to', new_high_price)

                    line_raw_data_1.update(
                        {
                            'high_price': new_high_price,
                        }
                    )

                    # Combine line data

                    line_raw_data_1.update(
                        {
                            'end_timestamp': second_line_data.end_timestamp,
                            'end_trade_id': second_line_data.end_trade_id,
                            'end_price': second_line_data.end_price,
                            'quantity': line_raw_data_1['quantity']
                            + second_line_data.quantity,
                            'volume': line_raw_data_1['quantity']
                            + second_line_data.volume,
                        }
                    )

                    first_line_data = None
                    second_line_data = None

                if line_raw_data_1 is not None:
                    # Flush

                    if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        print('Flushing 8', line_raw_data_1)

                    line_raw_data_list_1.append(
                        line_raw_data_1,
                    )

                    line_raw_data_1 = None  # noqa

                if first_line_data is not None:
                    end_price: float = first_line_data.end_price
                    high_price = end_price
                    start_price: float = first_line_data.start_price
                    low_price: float = start_price

                    trading_direction: TradingDirection = (
                        first_line_data.trading_direction
                    )

                    line_raw_data_1 = {
                        'end_timestamp': first_line_data.end_timestamp,
                        'end_trade_id': first_line_data.end_trade_id,
                        'end_price': end_price,
                        'high_price': high_price,
                        'low_price': low_price,
                        'start_timestamp': first_line_data.start_timestamp,
                        'start_trade_id': first_line_data.Index,
                        'start_price': start_price,
                        'quantity': first_line_data.quantity,
                        'trading_direction': trading_direction,
                        'volume': first_line_data.volume,
                    }

                    # Flush

                    if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        print('Flushing 9', line_raw_data_1)

                    line_raw_data_list_1.append(
                        line_raw_data_1,
                    )

                    first_line_data = None  # noqa
                    line_raw_data_1 = None  # noqa

                for line_raw_data_1 in line_raw_data_list_1:
                    if line_raw_data is None:
                        line_raw_data = line_raw_data_1

                        continue

                    old_trading_direction: TradingDirection = line_raw_data[
                        'trading_direction'
                    ]

                    new_trading_direction: TradingDirection = line_raw_data_1[
                        'trading_direction'
                    ]

                    if old_trading_direction == new_trading_direction:
                        line_raw_data.update(
                            {
                                'end_timestamp': line_raw_data_1['end_timestamp'],
                                'end_trade_id': line_raw_data_1['end_trade_id'],
                                'end_price': line_raw_data_1['end_price'],
                                'quantity': (
                                    line_raw_data['quantity'] + line_raw_data_1['quantity']
                                ),
                                'volume': (
                                    line_raw_data['volume'] + line_raw_data_1['volume']
                                ),
                            }
                        )
                    else:
                        # Flush

                        line_raw_data_list.append(
                            line_raw_data,
                        )

                        line_raw_data = line_raw_data_1

                if line_raw_data is not None:
                    # Flush

                    line_raw_data_list.append(line_raw_data)

                    line_raw_data = None  # noqa

            if line_raw_data_list:
                new_line_dataframe = DataFrame.from_records(
                    line_raw_data_list,
                    columns=[
                        'start_timestamp',
                        'start_trade_id',
                        'start_price',
                        'end_timestamp',
                        'end_trade_id',
                        'end_price',
                        'quantity',
                        'trading_direction',
                        'volume',
                    ],
                )

                assert new_line_dataframe.size, None

                new_line_dataframe.set_index(
                    'start_trade_id',
                    inplace=True,
                )

                new_line_dataframe.sort_values(
                    'start_trade_id',
                    inplace=True,
                )

                if old_line_dataframe is not None:
                    old_line_dataframe.update(
                        new_line_dataframe,
                    )

                    line_dataframe = old_line_dataframe.combine_first(
                        new_line_dataframe,
                    )
                else:
                    line_dataframe = new_line_dataframe

                line_dataframe_by_level_map[smoothing_level] = line_dataframe
            else:
                line_dataframe = line_dataframe_by_level_map[smoothing_level]

            previous_line_dataframe = line_dataframe

            # Smoothed dataframe updating

            old_smoothed_dataframe = trades_smoothed_dataframe_by_level_map.get(
                smoothing_level,
            )

            # min_trade_id: int
            #
            # if old_line_dataframe is not None:
            #     min_pandas_trade_id = old_smoothed_dataframe.index.max()
            #
            #     min_trade_id = int(
            #         min_pandas_trade_id,
            #     )
            # else:
            #     min_trade_id = 0

            trades_smoothed_raw_data_list: list[dict[str, typing.Any]] = []

            last_line_data: typing.NamedTuple | None = None

            for line_data in line_dataframe.loc[
                line_dataframe.index >= min_start_trade_id  # min_trade_id
            ].itertuples():  # TODO
                trades_smoothed_raw_data_list.append(
                    {
                        'price': line_data.start_price,
                        'timestamp': line_data.start_timestamp,
                        'trade_id': line_data.Index,
                    }
                )

                last_line_data = line_data

            if last_line_data is not None:
                trades_smoothed_raw_data_list.append(
                    {
                        'price': last_line_data.end_price,
                        'timestamp': last_line_data.end_timestamp,
                        'trade_id': last_line_data.end_trade_id,
                    },
                )

            if trades_smoothed_raw_data_list:
                new_smoothed_dataframe = DataFrame.from_records(
                    trades_smoothed_raw_data_list,
                    columns=[
                        'price',
                        # 'quantity',
                        'timestamp',
                        'trade_id',
                        # 'volume',
                    ],
                )

                assert new_smoothed_dataframe.size, None

                new_smoothed_dataframe.set_index(
                    'trade_id',
                    inplace=True,
                )

                new_smoothed_dataframe.sort_values(
                    'trade_id',
                    inplace=True,
                )

                smoothed_dataframe: DataFrame

                if old_smoothed_dataframe is not None:
                    old_smoothed_dataframe.update(
                        new_smoothed_dataframe,
                    )

                    smoothed_dataframe = old_smoothed_dataframe.combine_first(
                        new_smoothed_dataframe,
                    )
                else:
                    smoothed_dataframe = new_smoothed_dataframe

                trades_smoothed_dataframe_by_level_map[smoothing_level] = (
                    smoothed_dataframe
                )

    def __update_velocity_series(
        self,
    ) -> None:
        candle_dataframe_by_interval_name_map = (
            self.__candle_dataframe_by_interval_name_map
        )

        candle_dataframe_1m = candle_dataframe_by_interval_name_map.get(
            PlotConstants.VelocityIntervalName,
        )

        if candle_dataframe_1m is None:
            return

        self.__velocity_series = candle_dataframe_1m.trades_count

    @staticmethod
    def __process_1(
        first_item_idx: int,
        prices: list[float],
        prices_2: list[float],
        start_timestamps: list[pandas.Timestamp],
        start_timestamps_2: list[pandas.Timestamp],
    ) -> None:
        while first_item_idx < len(start_timestamps) - 2:
            second_item_idx = first_item_idx + 1
            third_item_idx = second_item_idx + 1

            first_price = prices[first_item_idx]
            first_start_timestamp = start_timestamps[first_item_idx]

            prices_2.append(
                first_price,
            )

            start_timestamps_2.append(
                first_start_timestamp,
            )

            second_price = prices[second_item_idx]
            second_start_timestamp = start_timestamps[second_item_idx]

            third_price = prices[third_item_idx]
            third_start_timestamp = start_timestamps[third_item_idx]

            is_bull_1 = second_price > first_price
            is_bear_1 = second_price < first_price
            is_cross_1 = second_price == first_price

            is_bull_2 = third_price > second_price
            is_bear_2 = third_price < second_price
            is_cross_2 = third_price == second_price

            if not (
                is_bull_1 == is_bull_2
                or is_bear_1 == is_bear_2
                or is_cross_1
                or is_cross_2
            ):
                # second_price = (
                #     (
                #         first_price +
                #         third_price
                #     ) /
                #
                #     2.0
                # )

                prices_2.append(
                    second_price,
                )

                start_timestamps_2.append(
                    second_start_timestamp,
                )

            first_item_idx += 2

    @staticmethod
    def __process_2(
        first_item_idx: int,
        prices: list[float],
        prices_2: list[float],
        start_timestamps: list[pandas.Timestamp],
        start_timestamps_2: list[pandas.Timestamp],
    ) -> None:
        while first_item_idx < len(start_timestamps) - 3:
            second_item_idx = first_item_idx + 1
            third_item_idx = second_item_idx + 1
            fourth_item_idx = third_item_idx + 1

            first_price = prices[first_item_idx]
            first_start_timestamp = start_timestamps[first_item_idx]

            prices_2.append(
                first_price,
            )

            start_timestamps_2.append(
                first_start_timestamp,
            )

            second_price = prices[second_item_idx]
            second_start_timestamp = start_timestamps[second_item_idx]

            third_price = prices[third_item_idx]
            third_start_timestamp = start_timestamps[third_item_idx]

            fourth_price = prices[fourth_item_idx]
            fourth_start_timestamp = start_timestamps[fourth_item_idx]

            if not (
                (
                    first_price <= second_price <= fourth_price
                    and first_price <= third_price <= fourth_price
                )
                or (
                    fourth_price <= second_price <= first_price
                    and fourth_price <= third_price <= first_price
                )
            ):
                prices_2.append(
                    second_price,
                )

                start_timestamps_2.append(
                    second_start_timestamp,
                )

                prices_2.append(
                    third_price,
                )

                start_timestamps_2.append(
                    third_start_timestamp,
                )

            first_item_idx += 3
