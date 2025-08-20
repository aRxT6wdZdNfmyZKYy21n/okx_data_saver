import asyncio
import logging
import traceback
import typing

from datetime import (
    timedelta,
)

from decimal import (
    Decimal
)

import pandas
import talib

from pandas import (
    DataFrame,
    Series,
)

from sqlalchemy import (
    select,
    and_,
)

from main.save_candles import (
    schemas
)
from main.save_candles.schemas import OKXCandleData15m, OKXCandleData1H
from main.show_plot.globals import g_globals

from main.show_plot.window import (
    FinPlotChartWindow
)


logger = logging.getLogger(
    __name__
)


_CANDLE_INTERVAL_NAMES: list[str] = [  # TODO: RU
    '15m',
    '1H',
]


class FinPlotChartProcessor(object):
    __slots__ = (
        '__bollinger_base_line_series',
        '__bollinger_lower_band_series',
        '__bollinger_upper_band_series',
        '__current_available_symbol_name_set',
        '__current_symbol_name',
        '__current_interval_name',
        '__candles_dataframe',
        '__max_candle_price',
        '__max_price',
        '__min_candle_price',
        '__min_price',
        '__rsi_series',
        '__test_analytics_raw_data_list',
        '__test_series',
        '__window'
    )

    def __init__(
            self
    ) -> None:
        super(
            FinPlotChartProcessor,
            self
        ).__init__()

        self.__bollinger_base_line_series: (
            Series | None
        ) = None

        self.__bollinger_lower_band_series: (
            Series | None
        ) = None

        self.__bollinger_upper_band_series: (
            Series | None
        ) = None

        self.__current_available_symbol_name_set: (
            set[str] | None
        ) = None

        self.__current_symbol_name: (
            str | None
        ) = None

        self.__current_interval_name: (
            str | None
        ) = None

        self.__candles_dataframe: (
            DataFrame | None
        ) = None

        self.__max_candle_price: (
            Decimal | None
        ) = None

        self.__max_price: (
            Decimal | None
        ) = None

        self.__min_candle_price: (
            Decimal | None
        ) = None

        self.__min_price: (
            Decimal | None
        ) = None

        self.__rsi_series: (
            Series | None
        ) = None

        self.__test_analytics_raw_data_list: list[dict[str, typing.Any]] | None = None

        self.__test_series: (
            Series | None
        ) = None

        self.__window: (
            FinPlotChartWindow | None
        ) = None

    # async def fini(
    #         self,
    # ) -> None:
    #     # TODO: fini the window
    #
    #     await (
    #         super(
    #             FinPlotChartProcessor,
    #             self
    #         ).fini()
    #     )

    def get_bollinger_base_line_series(
            self,
    ) -> (
            Series | None
    ):
        return self.__bollinger_base_line_series

    def get_bollinger_lower_band_series(
            self,
    ) -> (
            Series | None
    ):
        return self.__bollinger_lower_band_series

    def get_bollinger_upper_band_series(
            self,
    ) -> (
            Series | None
    ):
        return self.__bollinger_upper_band_series

    @staticmethod
    def get_current_available_interval_names() -> (
            list[str] | None
    ):
        return _CANDLE_INTERVAL_NAMES

    async def get_current_available_symbol_names(
            self,
    ) -> (
            list[str] | None
    ):
        return await self.__get_current_available_symbol_names()

    def get_current_interval_name(
            self,
    ) -> (
            str | None
    ):
        return self.__current_interval_name

    def get_current_symbol_name(
            self,
    ) -> (
            str | None
    ):
        return self.__current_symbol_name

    def get_candles_dataframe(
            self,
    ) -> (
            DataFrame | None
    ):
        return self.__candles_dataframe

    def get_max_candle_price(
            self,
    ) -> (
            Decimal | None
    ):
        return self.__max_candle_price

    def get_max_price(
            self,
    ) -> (
            Decimal | None
    ):
        return self.__max_price

    def get_min_candle_price(
            self,
    ) -> (
            Decimal | None
    ):
        return self.__min_candle_price

    def get_min_price(
            self,
    ) -> (
            Decimal | None
    ):
        return self.__min_price

    def get_rsi_series(
            self,
    ) -> (
            Series | None
    ):
        return self.__rsi_series

    def get_test_analytics_raw_data_list(
            self,
    ) -> list[dict[str, typing.Any]] | None:
        return self.__test_analytics_raw_data_list

    def get_test_series(
            self,
    ) -> (
            Series | None
    ):
        return self.__test_series

    async def init(
            self,
    ) -> None:
        # init the window

        window = FinPlotChartWindow(
            processor=self
        )

        self.__window = window

        # show the window

        window.show()

        await window.plot(
            is_need_run_once=(
                True
            )
        )

    async def start_updating_loop(
            self,
    ) -> None:
        while True:
            try:
                await self.__update()
            except Exception as exception:
                logger.error(
                    'Could not update processor'
                    ': handled exception'
                    f': {"".join(traceback.format_exception(exception))}'
                )

            await asyncio.sleep(
                5.0  # s
            )

    async def update_current_interval_name(
            self,

            value: (
                str
            ),
    ) -> bool:
        if (
                value not in
                _CANDLE_INTERVAL_NAMES
        ):
            return False

        if (
                value ==
                self.__current_interval_name
        ):
            return False

        self.__current_interval_name = (
            value
        )

        self.__bollinger_base_line_series = (
            None
        )

        self.__bollinger_lower_band_series = (
            None
        )

        self.__bollinger_upper_band_series = (
            None
        )

        self.__candles_dataframe = (
            None
        )

        self.__max_candle_price = (
            None
        )

        self.__max_price = (
            None
        )

        self.__min_candle_price = (
            None
        )

        self.__min_price = (
            None
        )

        self.__rsi_series = (
            None
        )

        self.__test_analytics_raw_data_list = None

        self.__test_series = (
            None
        )

        await (
            self.__update_current_available_symbol_name_set()
        )

        await (
            self.__update_candles_dataframe()
        )

        await self.__window.plot(
            is_need_run_once=(
                True
            )
        )

        return True

    async def update_current_symbol_name(
            self,

            value: str
    ) -> bool:
        current_available_symbol_name_set = (
            self.__current_available_symbol_name_set
        )

        if current_available_symbol_name_set is None:
            return False

        if (
                value not in
                current_available_symbol_name_set
        ):
            return False

        if (
                value ==
                self.__current_symbol_name
        ):
            return False

        self.__current_symbol_name = (
            value
        )

        self.__bollinger_base_line_series = (
            None
        )

        self.__bollinger_lower_band_series = (
            None
        )

        self.__bollinger_upper_band_series = (
            None
        )

        self.__candles_dataframe = (
            None
        )

        self.__max_candle_price = (
            None
        )

        self.__max_price = (
            None
        )

        self.__min_candle_price = (
            None
        )

        self.__min_price = (
            None
        )

        self.__rsi_series = (
            None
        )

        self.__test_analytics_raw_data_list = None

        self.__test_series = (
            None
        )

        await (
            self.__update_candles_dataframe()
        )

        await self.__window.plot(
            is_need_run_once=(
                True
            )
        )

        return True

    async def __get_current_available_symbol_names(
            self
    ) -> (
            typing.Optional[
                typing.List[
                    str
                ]
            ]
    ):
        current_available_symbol_name_set = (
            self.__current_available_symbol_name_set
        )

        if current_available_symbol_name_set is None:
            return None

        return (
            sorted(
                current_available_symbol_name_set
            )
        )

    async def __update(
            self
    ) -> None:
        await self.__update_candles_dataframe()

    def __update_bollinger_series(
            self
    ) -> None:
        candles_dataframe = (
            self.__candles_dataframe
        )

        assert (
            candles_dataframe is not None
        ), None

        (
            bollinger_upper_band_series,
            bollinger_base_line_series,
            bollinger_lower_band_series
        ) = (
            talib.BBANDS(  # noqa
                candles_dataframe.close,

                matype=(
                    talib.MA_Type.SMA  # noqa
                ),

                timeperiod=(
                    20
                )
            )
        )

        if not bollinger_base_line_series.size:
            assert (
                not bollinger_lower_band_series.size
            ), None

            assert (
                not bollinger_upper_band_series.size
            ), None

            return

        assert (
            bollinger_lower_band_series.size
        ), None

        assert (
            bollinger_upper_band_series.size
        ), None

        self.__bollinger_base_line_series = (
            bollinger_base_line_series
        )

        self.__bollinger_lower_band_series = (
            bollinger_lower_band_series
        )

        self.__bollinger_upper_band_series = (
            bollinger_upper_band_series
        )

    async def __update_candles_dataframe(
            self
    ) -> None:
        current_symbol_name = (
            self.__current_symbol_name
        )

        if current_symbol_name is None:
            return

        current_interval_name = (
            self.__current_interval_name
        )

        if current_interval_name is None:
            print(
                'current_interval_name is None'
            )

            return

        candles_dataframe = (
            self.__candles_dataframe
        )

        min_start_timestamp_ms: int

        if candles_dataframe is not None:
            min_pandas_start_timestamp: (
                pandas.Timestamp
            ) = candles_dataframe.index.max()

            min_start_timestamp_ms = int(
                min_pandas_start_timestamp.timestamp() *
                1000  # ms
            )
        else:
            min_start_timestamp_ms = 0

        new_candle_raw_data_list: list[dict] | None = None
        new_max_candle_price: Decimal | None = None
        new_min_candle_price: Decimal | None = None

        db_schema: type[OKXCandleData15m] | type[OKXCandleData1H] = getattr(
            schemas,
            f'OKXCandleData{current_interval_name}'
        )

        postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

        async with postgres_db_session_maker() as session:
            result = await session.execute(
                select(
                    db_schema,
                ).where(
                    and_(
                        (
                            db_schema.symbol_name ==
                            current_symbol_name
                        ),

                        (
                            db_schema.start_timestamp_ms >=
                            min_start_timestamp_ms
                        )
                    )
                ).order_by(
                    db_schema.symbol_name.asc(),
                    db_schema.start_timestamp_ms.desc(),
                ).limit(
                    # 10000
                    1000
                    # 50
                )
            )

            for row in result:
                new_candle_data: OKXCandleData15m | OKXCandleData1H = row[0]

                new_candle_high_price = Decimal(
                    new_candle_data.high_price
                )

                if (
                        new_max_candle_price is None or

                        (
                            new_max_candle_price <
                            new_candle_high_price
                        )
                ):
                    new_max_candle_price = (
                        new_candle_high_price
                    )

                new_candle_low_price = Decimal(
                    new_candle_data.low_price
                )

                if (
                        new_min_candle_price is None or

                        (
                            new_min_candle_price >
                            new_candle_low_price
                        )
                ):
                    new_min_candle_price = (
                        new_candle_low_price
                    )

                new_candle_raw_data = {
                    'close_price': float(
                        new_candle_data.close_price  # noqa
                    ),
                    'high_price': float(
                        new_candle_high_price
                    ),
                    'low_price': float(
                        new_candle_low_price
                    ),
                    'open_price': float(
                        new_candle_data.open_price  # noqa
                    ),
                    'start_timestamp_ms': int(
                        new_candle_data.start_timestamp_ms  # noqa
                    ),
                    'volume': float(
                        new_candle_data.volume_quote_currency  # noqa
                    ),
                }

                if new_candle_raw_data_list is None:
                    new_candle_raw_data_list = []

                new_candle_raw_data_list.append(
                    new_candle_raw_data
                )

        if new_max_candle_price is not None:
            max_candle_price = (
                self.__max_candle_price
            )

            if (
                    max_candle_price is None or

                    (
                        max_candle_price <
                        new_max_candle_price
                    )
            ):
                self.__max_candle_price = (
                    max_candle_price  # noqa
                ) = new_max_candle_price

                self.__update_max_price()

        if new_min_candle_price is not None:
            min_candle_price = (
                self.__min_candle_price
            )

            if (
                    min_candle_price is None or

                    (
                        min_candle_price >
                        new_min_candle_price
                    )
            ):
                self.__min_candle_price = (
                    min_candle_price  # noqa
                ) = new_min_candle_price

                self.__update_min_price()

        if new_candle_raw_data_list is None:
            return

        new_candles_dataframe = (
            DataFrame.from_records(
                new_candle_raw_data_list,

                columns=[
                    'close_price',
                    'high_price',
                    'low_price',
                    'open_price',
                    'start_timestamp_ms',
                    'volume'
                ]
            )
        )

        assert (
            new_candles_dataframe.size
        ), None

        new_candles_dataframe.rename(
            columns={
                'close_price': (
                    'close'
                ),

                'high_price': (
                    'high'
                ),

                'low_price': (
                    'low'
                ),

                'open_price': (
                    'open'
                ),

                'start_timestamp_ms': (
                    'time'
                )
            },

            inplace=(
                True
            )
        )

        new_candles_dataframe.time = (
            pandas.to_datetime(
                new_candles_dataframe.time,

                unit=(
                    'ms'
                )
            )
        )

        new_candles_dataframe.set_index(
            'time',
            inplace=True
        )

        new_candles_dataframe.sort_values(
            'time',
            inplace=True
        )

        if candles_dataframe is not None:
            candles_dataframe.update(  # TODO: check thread-safety
                new_candles_dataframe
            )

            candles_dataframe = (
                candles_dataframe.combine_first(
                    new_candles_dataframe
                )
            )
        else:
            candles_dataframe = (
                new_candles_dataframe
            )

        self.__candles_dataframe = (
            candles_dataframe
        )

        self.__update_bollinger_series()
        self.__update_rsi_series()
        self.__update_test_series()

        await self.__window.plot(
            is_need_run_once=(
                True
            )
        )

    async def __update_current_available_symbol_name_set(
            self
    ) -> None:
        current_interval_name = self.__current_interval_name

        if current_interval_name is None:
            print(
                'current_interval_name is None'
            )

            self.__current_available_symbol_name_set = None

            return

        current_available_symbol_name_set: set[str] | None = None

        db_schema: type[OKXCandleData15m] | type[OKXCandleData1H] = getattr(
            schemas,
            f'OKXCandleData{current_interval_name}'
        )

        postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

        async with postgres_db_session_maker() as session:
            recursive_cte_initial_query = select(
                db_schema.symbol_name
            ).order_by(
                db_schema.symbol_name.asc()
            ).limit(
                1
            ).cte(
                name='symbol_name_cte',
                recursive=True,
            )

            recursive_cte_sub_query = select(
                db_schema.symbol_name
            ).where(
                db_schema.symbol_name >
                recursive_cte_initial_query.c.symbol_name
            ).order_by(
                db_schema.symbol_name.asc()
            ).limit(
                1
            )

            recursive_cte_full_query = recursive_cte_initial_query.union_all(
                recursive_cte_sub_query
            )

            # full_cte = initial_cte.union_all(
            #     recursive_cte_initial_query,
            # )
            #
            result = await session.execute(
                select(
                    recursive_cte_full_query
                ),
            )

            for row in result:
                symbol_name, = row

                if current_available_symbol_name_set is None:
                    current_available_symbol_name_set = set()

                current_available_symbol_name_set.add(
                    symbol_name
                )

        self.__current_available_symbol_name_set = (
            current_available_symbol_name_set
        )

    def __update_max_price(
            self
    ) -> None:
        self.__max_price = (
            self.__max_candle_price
        )

    def __update_min_price(
            self
    ) -> None:
        self.__min_price = (
            self.__min_candle_price
        )

    def __update_rsi_series(
            self
    ) -> None:
        candles_dataframe = (
            self.__candles_dataframe
        )

        assert (
            candles_dataframe is not None
        ), None

        rsi_series = (
            talib.RSI(  # noqa
                candles_dataframe.close,
                timeperiod=14  # 6
            )
        )

        if not rsi_series.size:
            return

        self.__rsi_series = (
            rsi_series
        )

    def __update_test_series(
            self
    ) -> None:
        current_interval_name = self.__current_interval_name

        if current_interval_name is None:
            return

        candles_dataframe: DataFrame = (
            self.__candles_dataframe
        )

        assert (
            candles_dataframe is not None
        ), None

        prices: list[float] = []
        start_timestamps: list[pandas.Timestamp] = []

        start_timestamp: pandas.Timestamp

        for start_timestamp, candle_data in candles_dataframe.iterrows():
            candle_close_price = candle_data.close
            candle_high_price = candle_data.high
            candle_low_price = candle_data.low
            candle_open_price = candle_data.open

            is_bull = candle_close_price >= candle_open_price

            second_price: float
            third_price: float

            if is_bull:
                second_price = candle_low_price
                third_price = candle_high_price
            else:
                second_price = candle_high_price
                third_price = candle_low_price

            prices.append(
                candle_open_price
            )

            start_timestamps.append(
                start_timestamp
            )

            prices.append(
                second_price
            )

            start_timestamps.append(
                start_timestamp + timedelta(minutes=3, seconds=45)
            )

            prices.append(
                third_price
            )

            start_timestamps.append(
                start_timestamp + timedelta(minutes=7, seconds=30)
            )

            prices.append(
                candle_close_price
            )

            start_timestamps.append(
                start_timestamp + timedelta(minutes=11, seconds=15)
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
                start_timestamp
            )

            if price is None:
                assert old_price is not None, None
                assert old_start_timestamp is not None, None

                next_price: float | None = None
                next_timestamp = start_timestamp

                while next_timestamp <= end_timestamp:
                    next_price = price_by_start_timestamp_map.get(
                        next_timestamp
                    )

                    if next_price is not None:
                        break

                    next_timestamp += timedelta(
                        minutes=3,
                        seconds=45,
                    )

                if next_price is not None:
                    coefficient = (
                        (
                            start_timestamp.timestamp() -
                            old_start_timestamp.timestamp()
                        ) /

                        (
                            next_timestamp.timestamp() -
                            old_start_timestamp.timestamp()
                        )
                    )

                    price = (
                        old_price +

                        coefficient * (
                            next_price -
                            old_price
                        )
                    )
                else:
                    price = old_price

                price_by_start_timestamp_map[
                    start_timestamp
                ] = price
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

        self.__test_series = (
            test_series
        )

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
                first_price
            )

            start_timestamps_2.append(
                first_start_timestamp
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
                    is_bull_1 == is_bull_2 or
                    is_bear_1 == is_bear_2 or
                    is_cross_1 or
                    is_cross_2
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
                    second_price
                )

                start_timestamps_2.append(
                    second_start_timestamp
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
                first_price
            )

            start_timestamps_2.append(
                first_start_timestamp
            )

            second_price = prices[second_item_idx]
            second_start_timestamp = start_timestamps[second_item_idx]

            third_price = prices[third_item_idx]
            third_start_timestamp = start_timestamps[third_item_idx]

            fourth_price = prices[fourth_item_idx]
            fourth_start_timestamp = start_timestamps[fourth_item_idx]

            if not (
                    (
                        first_price <= second_price <= fourth_price and
                        first_price <= third_price <= fourth_price
                    ) or

                    (
                        fourth_price <= second_price <= first_price and
                        fourth_price <= third_price <= first_price
                    )
            ):
                prices_2.append(
                    second_price
                )

                start_timestamps_2.append(
                    second_start_timestamp
                )

                prices_2.append(
                    third_price
                )

                start_timestamps_2.append(
                    third_start_timestamp
                )

            first_item_idx += 3
