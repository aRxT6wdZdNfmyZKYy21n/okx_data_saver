import asyncio
import logging
import traceback

from constants.symbol import (
    SymbolConstants,
)

try:
    import uvloop
except ImportError:
    uvloop = asyncio


logger = logging.getLogger(
    __name__,
)


_SYMBOL_NAMES = [
    'BTC-USDT',
    'ETH-USDT',
]

class DataProcessingDaemon(object):
    __slots__ = (
        '__trades_'
    )

    async def start_update_loop(
            self,
    ) -> None:
        while True:
            try:
                await self.__update()
            except Exception as exception:
                logger.error(
                    'Could not update'
                    ': handled exception'
                    f': {"".join(traceback.format_exception(exception))}'
                )

            await asyncio.sleep(
                15.0  # s
            )

    async def __update(
            self,
    ) -> None:
        async with asyncio.TaskGroup() as task_group:
            for symbol_name in _SYMBOL_NAMES:
                task_group.create_task(
                    self.__update_symbol(
                        symbol_name,
                    )
                )

    async def __update_symbol(
            self,
            symbol_name: str
    ) -> None:
        symbol_id = SymbolConstants.IdByName[symbol_name]

        # TODO

async def main() -> None:
    # Set up logging

    logging.basicConfig(
        encoding='utf-8',
        format='[%(levelname)s][%(asctime)s][%(name)s]: %(message)s',
        level=(
            # logging.INFO
            logging.DEBUG
        ),
    )

    data_processing_daemon = DataProcessingDaemon()

    # Start loops

    await asyncio.gather(
        data_processing_daemon.start_update_loop(),
    )


if __name__ == '__main__':
    uvloop.run(
        main(),
    )
