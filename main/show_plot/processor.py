import asyncio
import logging
import traceback
import typing

from collections import (
    defaultdict
)

from decimal import (
    Decimal
)

import pandas
import talib

from pandas import (
    DataFrame,
    Series
)

from main.show_plot.window import (
    FinPlotChartWindow
)


logger = logging.getLogger(
    __name__
)


_CANDLE_INTERVAL_NAMES: list[str] = [  # TODO: RU
    '15m',
    '1h',
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

        self.__window: (
            FinPlotChartWindow | None
        ) = None

    # async def fini(
    #         self
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
            self
    ) -> (
            typing.Optional[
                Series
            ]
    ):
        return (
            self.__bollinger_base_line_series
        )

    def get_bollinger_lower_band_series(
            self
    ) -> (
            typing.Optional[
                Series
            ]
    ):
        return (
            self.__bollinger_lower_band_series
        )

    def get_bollinger_upper_band_series(
            self
    ) -> (
            typing.Optional[
                Series
            ]
    ):
        return (
            self.__bollinger_upper_band_series
        )

    @staticmethod
    def get_current_available_interval_names() -> (
            typing.Optional[
                typing.List[
                    str
                ]
            ]
    ):
        return (
            _CANDLE_INTERVAL_NAMES
        )

    async def get_current_available_symbol_names(
            self
    ) -> (
            typing.Optional[
                typing.List[
                    str
                ]
            ]
    ):
        return (
            await self.__get_current_available_symbol_names()
        )

    def get_current_interval_name(
            self
    ) -> (
            typing.Optional[
                str
            ]
    ):
        return (
            self.__current_interval_name
        )

    def get_current_symbol_name(
            self
    ) -> (
            typing.Optional[
                str
            ]
    ):
        return (
            self.__current_symbol_name
        )

    def get_candles_dataframe(
            self
    ) -> (
            typing.Optional[
                DataFrame
            ]
    ):
        return (
            self.__candles_dataframe
        )

    def get_max_candle_price(
            self
    ) -> (
            typing.Optional[
                Decimal
            ]
    ):
        return (
            self.__max_candle_price
        )

    def get_max_price(
            self
    ) -> (
            typing.Optional[
                Decimal
            ]
    ):
        return (
            self.__max_price
        )

    def get_min_candle_price(
            self
    ) -> (
            typing.Optional[
                Decimal
            ]
    ):
        return (
            self.__min_candle_price
        )

    def get_min_price(
            self
    ) -> (
            typing.Optional[
                Decimal
            ]
    ):
        return (
            self.__min_price
        )

    def get_rsi_series(
            self
    ) -> (
            typing.Optional[
                Series
            ]
    ):
        return (
                self.__rsi_series
        )

    async def init(
            self
    ) -> None:
        # init the window

        window = (
            FinPlotChartWindow(
                processor=self
            )
        )

        self.__window = (
            window
        )

        # show the window

        window.show()

        await (
            window.plot(
                is_need_run_once=(
                    True
                )
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
                15.0  # s
            )

    async def update_current_interval_name(
            self,

            value: (
                str
            )
    ) -> bool:
        return (
            await self.__update_current_interval_name(
                value
            )
        )

    async def update_current_symbol_name(
            self,

            value: str
    ) -> bool:
        return (
            await self.__update_current_symbol_name(
                value
            )
        )

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
        pass  # TODO

        """
        candle_data_table_name_raw = (
            Constants.BingX.CandleDataDatabaseTableNameFormat.format(
                candle_interval_name_raw=(
                    BingxCandleIntervalName.OneMinute.value  # one minute candles are primary
                )
            )
        )

        db_storage = (
            g_common_globals.get_postgres_db_storage()
        )

        async with (
                available_symbol_name_set_by_exchange_place_map_update_lock
        ):
            async with (
                    await (
                        db_storage.get_connection()
                    )
            ) as connection:
                async for row_data in (
                        connection.select_distinct_optimized(
                            column=(
                                'exchange_place'
                            ),

                            schema_name=(
                                'bingx_market'
                            ),

                            table_name=(
                                candle_data_table_name_raw
                            )
                        )
                ):
                    exchange_place_id: int = (
                        row_data.pop(
                            'exchange_place'
                        )
                    )

                    exchange_place = (
                        ExchangePlace(
                            exchange_place_id
                        )
                    )

                    if available_symbol_name_set_by_exchange_place_map is None:
                        self.__available_symbol_name_set_by_exchange_place_map = (
                            available_symbol_name_set_by_exchange_place_map
                        ) = (
                            defaultdict(
                                set
                            )
                        )

                    available_symbol_name_set = (
                        available_symbol_name_set_by_exchange_place_map[
                            exchange_place
                        ]
                    )

                    async for row_data_2 in (
                            connection.select_distinct_optimized(
                                column=(
                                    'symbol_name'
                                ),

                                schema_name=(
                                    'bingx_market'
                                ),

                                table_name=(
                                    candle_data_table_name_raw
                                ),

                                where_condition_or_separators=[
                                    SqlCondition(
                                        left_operand_name=(
                                            '__target_table__.exchange_place'
                                        ),

                                        operator_sql_code=(
                                            '='
                                        ),

                                        right_operand_data_type=(
                                            SqlDataType.Integer
                                        ),

                                        right_operand_sql_code_fmt=(
                                            None
                                        ),

                                        right_operand_value=(
                                            exchange_place_id
                                        )
                                    )
                                ]
                            )
                    ):
                        symbol_name: str = (
                            row_data_2.pop(
                                'symbol_name'
                            )
                        )

                        available_symbol_name_set.add(
                            symbol_name
                        )

        if available_symbol_name_set_by_exchange_place_map is None:
            print(
                'available_symbol_name_set_by_exchange_place_map is None'
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

            self.__current_available_symbol_name_set = (
                None
            )

            self.__current_exchange_place = (
                None
            )

            self.__current_symbol_name = (
                None
            )

            self.__current_interval_name = (
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

            # TODO: plot

            return

        # TODO: remove deleted symbol names && exchange places

        current_exchange_place = (
            self.__current_exchange_place
        )

        async with (
                available_symbol_name_set_by_exchange_place_map_update_lock
        ):
            if current_exchange_place is None:
                print(
                    'current_exchange_place is None'
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

                self.__current_available_symbol_name_set = (
                    None
                )

                self.__current_symbol_name = (
                    None
                )

                self.__current_interval_name = (
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

                # TODO: plot

                return

            self.__current_available_symbol_name_set = (
                current_available_symbol_name_set
            ) = (
                available_symbol_name_set_by_exchange_place_map.get(
                    current_exchange_place
                )
            )

            if current_available_symbol_name_set is None:
                print(
                    'current_available_symbol_name_set is None'
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

                self.__current_symbol_name = (
                    None
                )

                self.__current_interval_name = (
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

                # TODO: plot

                return

            current_symbol_name = (
                self.__current_symbol_name
            )

            if (
                    current_symbol_name is None or

                    (
                        current_symbol_name not in
                        current_available_symbol_name_set
                    )
            ):
                print(
                    f'current_symbol_name ({current_symbol_name!r}) is None or'
                    ''
                    ' ('
                    '    current_symbol_name not in'
                    '    current_available_symbol_name_set'
                    ')'
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

                self.__current_interval_name = (
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

                # TODO: plot

                return

        await (
            self.__update_candles_dataframe()
        )
        """

    async def __update_current_interval_name(
            self,

            value: (
                str
            )
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

        await (
            self.__update_candles_dataframe()
        )

        return True

    async def __update_current_symbol_name(
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

        await (
            self.__update_candles_dataframe()
        )

        return True

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


        # TODO

        """
        db_storage = (
            g_common_globals.get_postgres_db_storage()
        )

        min_start_timestamp_ms: (
            typing.Optional[
                int
            ]
        )

        if candles_dataframe is not None:
            min_pandas_start_timestamp: (
                pandas.Timestamp
            ) = (
                candles_dataframe.index.max()
            )

            min_start_timestamp_ms = (
                min_pandas_start_timestamp.value //

                (
                    10 **
                    6
                )
            )
        else:
            min_start_timestamp_ms = (
                None
            )

        new_candle_raw_data_list: (
            typing.Optional[
                typing.List[
                    typing.Dict
                ]
            ]
        ) = None

        new_max_candle_price: (
            typing.Optional[
                Decimal
            ]
        ) = None

        new_min_candle_price: (
            typing.Optional[
                Decimal
            ]
        ) = None

        async with (
                await (
                    db_storage.get_connection()
                )
        ) as connection:
            async for new_candle_raw_data in (
                    connection.iter_bingx_candle_raw_data(
                        column_and_sql_order_type_pairs=[
                            (
                                'symbol_name',
                                SqlOrderType.Ascending
                            ),

                            (
                                'start_timestamp_ms',
                                SqlOrderType.Descending
                            )
                        ],

                        interval_name=(
                            current_interval_name
                        ),

                        # Filtering expressions

                        min_start_timestamp_ms=(
                            min_start_timestamp_ms
                        ),

                        symbol_name=(
                            current_symbol_name
                        ),

                        limit=(
                            10000  # 1000  # TODO
                        )
                    )
            ):
                if new_candle_raw_data_list is None:
                    new_candle_raw_data_list = []

                new_candle_high_price: Decimal = (
                    new_candle_raw_data[
                        'high_price'
                    ]
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

                new_candle_low_price: Decimal = (
                    new_candle_raw_data[
                        'low_price'
                    ]
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

                # Cast Decimals to floats

                for key, value in (
                        new_candle_raw_data.items()
                ):
                    if (
                            type(
                                value
                            ) is

                            Decimal
                    ):
                        (
                            new_candle_raw_data[
                                key
                            ]
                        ) = (
                            float(
                                value
                            )
                        )

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
                ) = (
                    new_max_candle_price
                )

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
                ) = (
                    new_min_candle_price
                )

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
        """

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
                timeperiod=6
            )
        )

        if not rsi_series.size:
            return

        self.__rsi_series = (
            rsi_series
        )
