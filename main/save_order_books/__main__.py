import asyncio
import logging

import uvloop

from main.save_order_books import (
    models
)

from main.save_order_books.globals import (
    g_globals
)
from utils.time import TimeUtils

logger = (
    logging.getLogger(
        __name__
    )
)


async def init_db_models():
    postgres_db_engine = (
        g_globals.get_postgres_db_engine()
    )

    async with postgres_db_engine.begin() as connection:
        await connection.run_sync(
            models.Base.metadata.drop_all
        )

        await connection.run_sync(
            models.Base.metadata.create_all
        )


async def on_new_order_book_data(
        action: str,
        asks: list[list[str, str, str, str]],
        bids: list[list[str, str, str, str]],
        symbol_name: str,
) -> None:
    logger.info(
        'Got new order book data'
        f': action {action!r}, asks {len(asks)}, bids {len(bids)}, symbol name {symbol_name!r}'
    )

    postgres_db_session_maker = (
        g_globals.get_postgres_db_session_maker()
    )

    async with postgres_db_session_maker() as session:
        async with session.begin():
            session.add(
                models.OKXOrderBookData(
                    # Primary key fields

                    symbol_name=(
                        symbol_name
                    ),

                    timestamp_ms=(
                        TimeUtils.get_aware_current_timestamp_ms()
                    ),

                    # Attribute fields

                    action=(
                        action
                    ),

                    asks=asks,
                    bids=bids,
                ),
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

    await okx_web_socket_connection_manager.subscribe(
        symbol_name='BTC-USDT'
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

    await start_web_socket_connection_manager_loops(),


if (
        __name__ ==
        '__main__'
):
    uvloop.run(
        main()
    )
