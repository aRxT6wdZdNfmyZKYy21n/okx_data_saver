import asyncio
import logging
import typing

from main.save_order_books.globals import (
    g_globals
)


logger = (
    logging.getLogger(
        __name__
    )
)


async def on_new_order_book_data(
        action: str,
        asks: list[list[str, str, str, str]],
        bids: list[list[str, str, str, str]],
        symbol_name: str,
) -> None:
    print(f'{action!r} asks {len(asks)} bids {len(bids)} {symbol_name!r}')


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
