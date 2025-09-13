import asyncio
import logging
import traceback
import typing

from decimal import (
    Decimal,
)

import httpx
import orjson
import uvloop

from sqlalchemy import (
    and_,
    select,
    update
)

from sqlalchemy.dialects.postgresql import (
    insert,
)

from main.save_trades import (
    schemas
)

from main.save_trades.globals import (
    g_globals
)


logger = (
    logging.getLogger(
        __name__
    )
)

_INITIAL_TRADE_ID_BY_SYMBOL_NAME_MAP = {
    'BTC-USDT': 744536971,
    'ETH-USDT': 600257838,
}

_SYMBOL_NAMES = [
    'BTC-USDT',
    'ETH-USDT',
]

_TRADES_COUNT_PER_UPDATE = (
    100
)


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


async def start_db_loop() -> None:
    postgres_db_task_queue = (
        g_globals.get_postgres_db_task_queue()
    )

    while True:
        task = await postgres_db_task_queue.get()

        try:
            await task
        except Exception as exception:
            logger.error(
                'Handled exception while awaiting DB task'
                f': {"".join(traceback.format_exception(exception))}',
            )


async def save_trades(
        api_session: httpx.AsyncClient,
) -> None:
    for symbol_name in _SYMBOL_NAMES:
        # Get last trade data

        postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

        db_schema = schemas.OKXTradeData

        async with postgres_db_session_maker() as session:
            result = await session.execute(
                select(
                    db_schema,
                ).where(
                    db_schema.symbol_name ==
                    symbol_name
                ).order_by(
                    db_schema.trade_id.desc(),
                ).limit(
                    1
                )
            )

            row_data = result.fetchone()

        is_last_trade_exists = (
            row_data is not None
        )

        last_trade_id: int

        if is_last_trade_exists:
            last_trade_data: schemas.OKXTradeData

            last_trade_data, = row_data

            last_trade_id = (
                last_trade_data.trade_id
            )
        else:
            last_trade_id = _INITIAL_TRADE_ID_BY_SYMBOL_NAME_MAP[
                symbol_name
            ]

        logger.info(
            f'Saving trades for {symbol_name!r}'
            f' (from {last_trade_id} trade ID)'
        )

        response = await api_session.get(
            url='/api/v5/market/history-trades',
            params={
                'instId': symbol_name,
                'type': '1',  # pagination type is 'trade_id'
                'after': (
                    last_trade_id +
                    _TRADES_COUNT_PER_UPDATE
                ),
                'before': last_trade_id - 1,
                'limit': 100,  # max
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

        trade_raw_data_list: (
            list[dict[str, typing.Any]]
        ) = response_raw_data.pop(
            'data',
        )

        trade_raw_data_list_to_insert: list[dict[str, typing.Any]] = []
        trade_raw_data_to_update: dict[str, typing.Any] | None = None

        for trade_raw_data in (
                trade_raw_data_list
        ):
            trade_id_raw: str = trade_raw_data.pop(
                'tradeId',
            )

            trade_id = int(
                trade_id_raw,
            )

            trade_symbol_name: str = trade_raw_data.pop(
                'instId',
            )

            assert trade_symbol_name == symbol_name, (
                symbol_name,
                trade_symbol_name,
            )

            trade_timestamp_ms_raw: str = trade_raw_data.pop(
                'ts',
            )

            trade_timestamp_ms = int(
                trade_timestamp_ms_raw,
            )

            trade_price_raw = trade_raw_data.pop(
                'px',
            )

            trade_price = Decimal(
                trade_price_raw,
            )

            trade_quantity_raw = trade_raw_data.pop(
                'sz',
            )

            trade_quantity = Decimal(
                trade_quantity_raw,
            )

            trade_side_taw = trade_raw_data.pop(
                'side',
            )

            is_buy: bool

            if trade_side_taw == 'buy':
                is_buy = True
            elif trade_side_taw == 'sell':
                is_buy = False
            else:
                raise NotImplementedError(
                    trade_side_taw,
                )

            trade_raw_data = dict(
                # Primary key fields

                symbol_name=(
                    symbol_name
                ),

                trade_id=(
                    trade_id
                ),

                # Attribute fields

                is_buy=(
                    is_buy
                ),

                price=(
                    trade_price
                ),

                quantity=(
                    trade_quantity
                ),

                timestamp_ms=(
                    trade_timestamp_ms
                ),
            )

            if (
                    last_trade_id is not None and

                    (
                        trade_id ==
                        last_trade_id
                    )
            ):
                trade_raw_data_to_update = trade_raw_data
            else:
                trade_raw_data_list_to_insert.append(
                    trade_raw_data
                )

        if is_last_trade_exists:
            assert trade_raw_data_to_update is not None, (
                symbol_name,
            )

        if not (
                trade_raw_data_to_update is not None or
                trade_raw_data_list_to_insert
        ):
            logger.info(
                'Nothing to update'
            )

            continue

        async with postgres_db_session_maker() as session:
            async with session.begin():
                if trade_raw_data_to_update is not None:
                    # Remove primary key

                    trade_raw_data_to_update.pop(
                        'symbol_name',
                    )

                    trade_id: int = trade_raw_data_to_update.pop(
                        'trade_id',
                    )

                    await session.execute(
                        update(
                            db_schema
                        ).values(
                            trade_raw_data_to_update
                        ).where(
                            and_(
                                (
                                    db_schema.symbol_name ==
                                    symbol_name
                                ),

                                (
                                    db_schema.trade_id ==
                                    trade_id
                                )
                            )
                        )
                    )

                if trade_raw_data_list_to_insert:
                    await session.execute(
                        insert(
                            db_schema
                        ),

                        trade_raw_data_list_to_insert
                    )

        logger.info(
            f'Added {len(trade_raw_data_list)} trades'
        )

        await asyncio.sleep(
            0.5  # s
        )


async def start_trades_saving_loop() -> None:
    async with httpx.AsyncClient(
            base_url='https://www.okx.com'
    ) as api_session:
        while True:
            try:
                await save_trades(
                    api_session,
                )
            except Exception as exception:
                logger.error(
                    'Could not save trades'
                    ': handled exception'
                    f': {"".join(traceback.format_exception(exception))}'
                )

            await asyncio.sleep(
                0.5  # s
            )


async def main() -> None:
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

    # Prepare DB

    await init_db_models()

    # Start loops

    await asyncio.gather(
        start_db_loop(),
        start_trades_saving_loop(),
    )


if (
        __name__ ==
        '__main__'
):
    uvloop.run(
        main()
    )
