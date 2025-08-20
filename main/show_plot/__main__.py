import asyncio
import logging
import sys
import traceback
import typing
from decimal import Decimal

import httpx
import orjson
import uvloop
from PyQt6.QtWidgets import QApplication
from qasync import QEventLoop

from sqlalchemy import (
    select,
    update
)

from sqlalchemy.dialects.postgresql import (
    insert,
)

from main.save_candles import (
    schemas
)

from main.save_candles.globals import (
    g_globals
)
from main.show_plot.processor import FinPlotChartProcessor

from utils.time import (
    TimeUtils
)


logger = (
    logging.getLogger(
        __name__
    )
)


_CANDLES_COUNT_PER_REQUEST = 300


async def init_db_models():
    postgres_db_engine = (
        g_globals.get_postgres_db_engine()
    )

    async with postgres_db_engine.begin() as connection:
        # await connection.run_sync(
        #     models.Base.metadata.drop_all
        # )

        await connection.run_sync(
            schemas.Base.metadata.create_all
        )


async def save_candles(
        api_session: httpx.AsyncClient,
) -> None:
    for symbol_name in (
            'BTC-USDT',
            'ETH-USDT'
    ):
        for interval_name, interval_duration_ms in (
                (
                    '15m',

                    (
                        1000 *  # ms
                        60 *  # s
                        15  # m
                    )
                ),
                (
                    '1H',

                    (
                        1000 *  # ms
                        60 *  # s
                        60 *  # m
                        1     # h
                    )
                )
        ):
            # Get last candle data

            postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

            db_schema: type[schemas.OKXCandleData15m] | type[schemas.OKXCandleData1H] = (
                getattr(
                    schemas,
                    f'OKXCandleData{interval_name}'
                )
            )

            async with postgres_db_session_maker() as session:
                result = await session.execute(
                    select(
                        db_schema,
                    ).where(
                        db_schema.symbol_name ==
                        symbol_name
                    ).order_by(
                        db_schema.start_timestamp_ms.desc(),
                    ).limit(
                        1
                    )
                )

                row_data = result.fetchone()

            is_last_candle_exists = (
                row_data is not None
            )

            now_timestamp_ms = TimeUtils.get_aware_current_timestamp_ms()

            if is_last_candle_exists:
                last_candle_data: schemas.OKXCandleData15m | schemas.OKXCandleData1H

                last_candle_data, = row_data

                last_candle_timestamp_ms = (
                    last_candle_data.start_timestamp_ms
                )
            else:
                last_candle_timestamp_ms = (
                    now_timestamp_ms -

                    (
                        1000 *  # ms
                        60   *  # s
                        60   *  # m
                        24   *  # h
                        365  *  # d
                        7      # y
                    )
                )

            logger.info(
                f'Saving {interval_name} candles for {symbol_name!r}'
                f' ({(now_timestamp_ms - last_candle_timestamp_ms) // (1000 * 60 * 60 * 1)} hours left)'
            )

            response = await api_session.get(
                url='/api/v5/market/history-candles',
                params={
                    'instId': symbol_name,
                    'bar': interval_name,
                    'after': (
                        last_candle_timestamp_ms +

                        (
                            interval_duration_ms *  # ms
                            _CANDLES_COUNT_PER_REQUEST
                        )
                    ),
                    'before': last_candle_timestamp_ms - 1,
                    'limit': _CANDLES_COUNT_PER_REQUEST,
                }
            )

            response.raise_for_status()

            response_body = await response.aread()

            response_raw_data = orjson.loads(
                response_body,
            )

            code: str = response_raw_data.pop(
                'code',
            )

            assert code == '0', (
                code,
                response_raw_data,
            )

            candle_tuples: (
                list[tuple[str, str, str, str, str, str, str, str, str]]
            ) = response_raw_data.pop(
                'data',
            )

            candle_raw_data_list_to_insert: list[dict[str, typing.Any]] = []
            candle_raw_data_to_update: dict[str, typing.Any] | None = None

            for candle_tuple in (
                    candle_tuples
            ):
                (
                    candle_start_timestamp_ms_raw,
                    candle_open_price_raw,
                    candle_high_price_raw,
                    candle_low_price_raw,
                    candle_close_price_raw,
                    candle_volume_contracts_count_raw,
                    candle_volume_base_currency_raw,
                    candle_volume_quote_currency_raw,
                    is_candle_closed_raw
                ) = candle_tuple

                candle_start_timestamp_ms = int(
                    candle_start_timestamp_ms_raw
                )

                candle_open_price = Decimal(
                    candle_open_price_raw
                )

                candle_high_price = Decimal(
                    candle_high_price_raw
                )

                candle_low_price = Decimal(
                    candle_low_price_raw
                )

                candle_close_price = Decimal(
                    candle_close_price_raw
                )

                candle_volume_contracts_count = Decimal(
                    candle_volume_contracts_count_raw
                )

                candle_volume_base_currency = Decimal(
                    candle_volume_base_currency_raw
                )

                candle_volume_quote_currency = Decimal(
                    candle_volume_quote_currency_raw
                )

                is_candle_closed: bool

                if is_candle_closed_raw == '0':
                    is_candle_closed = False
                elif is_candle_closed_raw == '1':
                    is_candle_closed = True
                else:
                    raise NotImplementedError(
                        is_candle_closed_raw,
                    )

                candle_raw_data = dict(
                    # Primary key fields

                    symbol_name=(
                        symbol_name
                    ),

                    start_timestamp_ms=(
                        candle_start_timestamp_ms
                    ),

                    # Attribute fields

                    is_closed=(
                        is_candle_closed
                    ),

                    close_price=(
                        candle_close_price
                    ),

                    high_price=(
                        candle_high_price
                    ),

                    open_price=(
                        candle_open_price
                    ),

                    low_price=(
                        candle_low_price
                    ),

                    volume_contracts_count=(
                        candle_volume_contracts_count
                    ),

                    volume_base_currency=(
                        candle_volume_base_currency
                    ),

                    volume_quote_currency=(
                        candle_volume_quote_currency
                    ),
                )

                if (
                        last_candle_timestamp_ms is not None and

                        (
                            candle_start_timestamp_ms ==
                            last_candle_timestamp_ms
                        )
                ):
                    candle_raw_data_to_update = candle_raw_data

                    # Remove primary key

                    candle_raw_data_to_update.pop(
                        'symbol_name',
                    )

                    candle_raw_data_to_update.pop(
                        'start_timestamp_ms',
                    )
                else:
                    candle_raw_data_list_to_insert.append(
                        candle_raw_data
                    )

            if is_last_candle_exists:
                assert candle_raw_data_to_update is not None, (
                    symbol_name,
                    interval_name,
                )

            if not (
                    candle_raw_data_to_update is not None or
                    candle_raw_data_list_to_insert
            ):
                logger.info(
                    'Nothing to update'
                )

                continue

            async with postgres_db_session_maker() as session:
                async with session.begin():
                    if candle_raw_data_to_update is not None:
                        await session.execute(
                            update(
                                db_schema
                            ).values(
                                candle_raw_data_to_update
                            )
                        )

                    if candle_raw_data_list_to_insert:
                        await session.execute(
                            insert(
                                db_schema
                            ),

                            candle_raw_data_list_to_insert
                        )

            logger.info(
                f'Added {len(candle_tuples)} {interval_name} candles'
            )

            await asyncio.sleep(
                1.0  # s
            )


async def start_processor_updating_loop() -> None:
    # Prepare DB

    await init_db_models()

    processor = FinPlotChartProcessor()

    await processor.init()

    await processor.start_updating_loop()


def main() -> None:
    # Set up logging

    logging.basicConfig(
        encoding=(
            'utf-8'
        ),

        format=(
            '[%(levelname)s]'
            '[%(asctime)s]'
            '[%(name)s]'
            ': %(message)s'
        ),

        level=(
            # logging.INFO
            logging.DEBUG
        )
    )

    # create PyQt6 application

    application = QApplication(
        sys.argv,
    )

    py_qt_event_loop = QEventLoop(
        application,
    )

    # py_qt_event_loop.set_debug(
    #     True,
    # )

    asyncio.set_event_loop(
        py_qt_event_loop,
    )

    # Start loops

    py_qt_event_loop.run_until_complete(
        start_processor_updating_loop()
    )


if (
        __name__ ==
        '__main__'
):
    main()

