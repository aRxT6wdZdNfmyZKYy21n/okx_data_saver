import asyncio
import logging
import traceback

import uvloop

from main.save_order_books import (
    schemas
)

from main.save_order_books.globals import (
    g_globals
)


logger = (
    logging.getLogger(
        __name__
    )
)


_SYMBOL_NAMES = [
    'BTC-USDT',
    'ETH-USDT',
]


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


async def on_new_order_book_data(
        action: str,
        asks: list[list[str, str, str, str]],
        bids: list[list[str, str, str, str]],
        symbol_name: str,
        timestamp_ms: int,
) -> None:
    logger.info(
        'Got new order book data'
        f': action {action!r}, asks {len(asks)}, bids {len(bids)}'
        f', symbol name {symbol_name!r}, timestamp (ms) {timestamp_ms}'
    )

    postgres_db_task_queue = (
        g_globals.get_postgres_db_task_queue()
    )

    postgres_db_task_queue.put_nowait(
        save_order_book_data(
            action,
            asks,
            bids,
            symbol_name,
            timestamp_ms,
        ),
    )


async def save_order_book_data(
        action: str,
        asks: list[list[str, str, str, str]],
        bids: list[list[str, str, str, str]],
        symbol_name: str,
        timestamp_ms: int
) -> None:
    postgres_db_session_maker = (
        g_globals.get_postgres_db_session_maker()
    )

    async with postgres_db_session_maker() as session:
        async with session.begin():
            session.add(
                schemas.OKXOrderBookData(
                    # Primary key fields

                    symbol_name=(
                        symbol_name
                    ),

                    timestamp_ms=(
                        timestamp_ms
                    ),

                    # Attribute fields

                    action=(
                        action
                    ),

                    asks=asks,
                    bids=bids,
                ),
            )

    logger.info(
        'Order book data was saved'
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


async def start_web_socket_connection_manager_loops() -> None:
    okx_web_socket_connection_manager = (
        g_globals.get_okx_web_socket_connection_manager()
    )

    okx_web_socket_connection_manager_on_new_order_book_data_event = (
        okx_web_socket_connection_manager.get_on_new_order_book_data_event()
    )

    okx_web_socket_connection_manager_on_new_order_book_data_event += (
        on_new_order_book_data
    )

    for symbol_name in _SYMBOL_NAMES:
        await okx_web_socket_connection_manager.subscribe(
            symbol_name=symbol_name
        )

    await asyncio.gather(
        okx_web_socket_connection_manager.start_loop(),
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
        start_web_socket_connection_manager_loops(),
    )


if (
        __name__ ==
        '__main__'
):
    uvloop.run(
        main()
    )
