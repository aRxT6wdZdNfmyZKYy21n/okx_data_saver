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

from constants.common import CommonConstants
from constants.plot import PlotConstants
from main.save_trades.schemas import (
    OKXTradeData,
)
from main.show_plot.globals import (
    g_globals,
)

from main.show_plot.gui.window import (
    FinPlotChartWindow,
)


logger = logging.getLogger(
    __name__,
)


class FinPlotChartProcessor(object):
    __slots__ = (
        '__bollinger_base_line_series',
        '__bollinger_lower_band_series',
        '__bollinger_upper_band_series',
        '__candle_dataframe_by_interval_name_map',
        '__current_available_symbol_name_set',
        '__current_rsi_interval_name',
        '__current_symbol_name',
        '__max_price',
        '__max_trade_price',
        '__min_price',
        '__min_trade_price',
        '__rsi_series',
        '__test_analytics_raw_data_list',
        '__test_series',
        '__trades_dataframe',
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
        self.__max_price: Decimal | None = None
        self.__max_trade_price: Decimal | None = None
        self.__min_price: Decimal | None = None
        self.__min_trade_price: Decimal | None = None
        self.__rsi_series: Series | None = None
        self.__test_analytics_raw_data_list: list[dict[str, typing.Any]] | None = None
        self.__test_series: Series | None = None
        self.__trades_dataframe: DataFrame | None = None
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
        return await self.__get_current_available_symbol_names()

    def get_current_rsi_interval_name(
            self,
    ) -> str | None:
        return self.__current_rsi_interval_name

    def get_current_symbol_name(
        self,
    ) -> str | None:
        return self.__current_symbol_name

    def get_max_trade_price(
        self,
    ) -> Decimal | None:
        return self.__max_trade_price

    def get_max_price(
        self,
    ) -> Decimal | None:
        return self.__max_price

    def get_min_trade_price(
        self,
    ) -> Decimal | None:
        return self.__min_trade_price

    def get_min_price(
        self,
    ) -> Decimal | None:
        return self.__min_price

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
                    f'Processor was updated by {timer.elapsed:.3f}s'
                )
            except Exception as exception:
                logger.error(
                    'Could not update processor'
                    ': handled exception'
                    f': {"".join(traceback.format_exception(exception))}'
                )

            await asyncio.sleep(
                1.0  # s
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

        window.auto_range_price_plot()

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
        self.__max_price = None
        self.__max_trade_price = None
        self.__min_price = None
        self.__min_trade_price = None
        self.__rsi_series = None
        self.__test_analytics_raw_data_list = None
        self.__test_series = None
        self.__trades_dataframe = None
        self.__velocity_series = None

        await self.__update_trades_dataframe()

        window = self.__window

        await window.plot(
            is_need_run_once=True,
        )

        window.auto_range_price_plot()

        return True

    async def __get_current_available_symbol_names(
        self,
    ) -> list[str] | None:
        current_available_symbol_name_set = self.__current_available_symbol_name_set

        if current_available_symbol_name_set is None:
            return None

        return sorted(
            current_available_symbol_name_set,
        )

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

            for row in trades_dataframe.loc[
                trades_dataframe.index >= min_trade_id
            ].itertuples():
                trade_id: int = row.Index

                # if trade_id < min_trade_id:
                #     continue

                price: float = row.price
                quantity: float = row.quantity
                volume = price * quantity

                timestamp: pandas.Timestamp = row.timestamp_ms

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
                                'trades_count': last_candle_raw_data['trades_count'] + 1,
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

        trades_dataframe = self.__trades_dataframe

        min_trade_id: int  # TODO: min_trade_id

        if trades_dataframe is not None:
            min_pandas_trade_id = trades_dataframe.index.max()

            min_trade_id = int(
                min_pandas_trade_id,
            )
        else:
            min_trade_id = 0

        new_trade_raw_data_list: list[dict] | None = None
        new_max_trade_price: Decimal | None = None
        new_min_trade_price: Decimal | None = None

        postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

        async with postgres_db_session_maker() as session:
            result = await session.execute(
                select(
                    OKXTradeData,
                )
                .where(
                    and_(
                        OKXTradeData.symbol_name == current_symbol_name,
                        OKXTradeData.trade_id >= min_trade_id,
                    )
                )
                .order_by(
                    OKXTradeData.symbol_name.asc(),
                    OKXTradeData.trade_id.desc(),
                )
                .limit(
                    # 1_000_000,
                    500_000,
                    # 50_000,
                    # 10_000
                    # 1_000
                    # 50
                )
            )

            for row in result:
                new_trade_data: OKXTradeData = row[0]

                new_trade_price = new_trade_data.price

                if new_max_trade_price is None or (
                    new_max_trade_price < new_trade_price
                ):
                    new_max_trade_price = new_trade_price

                if new_min_trade_price is None or (
                    new_min_trade_price > new_trade_price
                ):
                    new_min_trade_price = new_trade_price

                new_trade_raw_data = {
                    'price': float(
                        new_trade_price,
                    ),
                    'quantity': float(
                        new_trade_data.quantity,
                    ),
                    'timestamp_ms': new_trade_data.timestamp_ms,
                    'trade_id': new_trade_data.trade_id,
                }

                if new_trade_raw_data_list is None:
                    new_trade_raw_data_list = []

                new_trade_raw_data_list.append(
                    new_trade_raw_data,
                )

        if new_max_trade_price is not None:
            max_trade_price = self.__max_trade_price

            if max_trade_price is None or (max_trade_price < new_max_trade_price):
                self.__max_trade_price = (
                    max_trade_price  # noqa
                ) = new_max_trade_price

                self.__update_max_price()

        if new_min_trade_price is not None:
            min_trade_price = self.__min_trade_price

            if min_trade_price is None or (min_trade_price > new_min_trade_price):
                self.__min_trade_price = (
                    min_trade_price  # noqa
                ) = new_min_trade_price

                self.__update_min_price()

        if new_trade_raw_data_list is None:
            return

        new_trades_dataframe = DataFrame.from_records(
            new_trade_raw_data_list,
            columns=[
                'price',
                'quantity',
                'timestamp_ms',
                'trade_id',
            ],
        )

        assert new_trades_dataframe.size, None

        new_trades_dataframe.timestamp_ms = pandas.to_datetime(
            new_trades_dataframe.timestamp_ms,
            unit='ms',
        )

        new_trades_dataframe.set_index(
            'trade_id',
            inplace=True,
        )

        new_trades_dataframe.sort_values(
            'trade_id',
            inplace=True,
        )

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
            f'Bollinger series were updated by {timer.elapsed:.3f}s'
        )

        with Timer() as timer:
            self.__update_candle_dataframe_by_interval_name_map()

        logger.info(
            f'Candle dataframe by interval name map was updated by {timer.elapsed:.3f}s'
        )

        with Timer() as timer:
            self.__update_rsi_series()

        logger.info(
            f'RSI series were updated by {timer.elapsed:.3f}s'
        )

        with Timer() as timer:
            self.__update_test_series()

        logger.info(
            f'Test series were updated by {timer.elapsed:.3f}s'
        )

        with Timer() as timer:
            self.__update_velocity_series()

        logger.info(
            f'Velocity series were updated by {timer.elapsed:.3f}s'
        )

        await self.__window.plot(
            is_need_run_once=True,
        )

    async def __update_current_available_symbol_name_set(self) -> None:
        current_available_symbol_name_set: set[str] | None = None

        postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

        async with postgres_db_session_maker() as session:
            recursive_cte_full_query = text(
                """
WITH RECURSIVE symbol_name_cte(symbol_name) AS 
(
  (
    SELECT okx_trade_data.symbol_name AS symbol_name 
    FROM okx_trade_data ORDER BY okx_trade_data.symbol_name ASC 
    LIMIT 1
  )
  UNION ALL
  SELECT (
    SELECT symbol_name
    FROM okx_trade_data
    WHERE symbol_name > cte.symbol_name
    ORDER BY symbol_name ASC 
    LIMIT 1
  )
  FROM symbol_name_cte AS cte
  WHERE cte.symbol_name IS NOT NULL
)
SELECT symbol_name
FROM symbol_name_cte
WHERE symbol_name IS NOT NULL;
                """
            )

            result = await session.execute(
                recursive_cte_full_query,
            )

            for row in result:
                (symbol_name,) = row

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

    def __update_max_price(
        self,
    ) -> None:
        self.__max_price = self.__max_trade_price

    def __update_min_price(
        self,
    ) -> None:
        self.__min_price = self.__min_trade_price

    def __update_rsi_series(
        self,
    ) -> None:
        current_rsi_interval_name = self.__current_rsi_interval_name

        if current_rsi_interval_name is None:
            return

        candle_dataframe_by_interval_name_map = self.__candle_dataframe_by_interval_name_map

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

    def __update_test_series(self) -> None:
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

    def __update_velocity_series(self) -> None:
        candle_dataframe_by_interval_name_map = self.__candle_dataframe_by_interval_name_map

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
