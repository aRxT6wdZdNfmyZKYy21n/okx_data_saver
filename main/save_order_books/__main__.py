import asyncio
import logging

from main.save_order_books.globals import (
    g_globals
)


logger = (
    logging.getLogger(
        __name__
    )
)


async def start_web_socket_connection_manager_loops() -> None:
    okx_web_socket_connection_manager = (
        g_globals.get_okx_web_socket_connection_manager()
    )

    await okx_web_socket_connection_manager.subscribe(
        symbol_name='BTC-USDT'
    )

    await asyncio.gather(
        okx_web_socket_connection_manager.start_loop(),
    )


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

    asyncio.run(
        start_web_socket_connection_manager_loops(),
    )


if (
        __name__ ==
        '__main__'
):
    main()
