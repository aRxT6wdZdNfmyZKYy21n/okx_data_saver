import asyncio
import logging
import traceback

import uvloop

from main.save_candles import (
    schemas
)

from main.save_candles.globals import (
    g_globals
)

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
                'Handled exception while awaiting DB tasl'
                f': {"".join(traceback.format_exception(exception))}',
            )


async def start_candles_saving_loop() -> None:
    pass  # TODO


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
        start_candles_saving_loop(),
    )


if (
        __name__ ==
        '__main__'
):
    uvloop.run(
        main()
    )
