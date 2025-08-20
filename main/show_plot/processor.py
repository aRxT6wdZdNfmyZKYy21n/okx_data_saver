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
                15.0  # s
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
                    10000
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
                timeperiod=6
            )
        )

        if not rsi_series.size:
            return

        self.__rsi_series = (
            rsi_series
        )
