import asyncio
import logging
import traceback

from sqlalchemy import (
    and_,
    select,
)

try:
    import uvloop
except ImportError:
    uvloop = asyncio

import main.save_order_books.schemas
import main.save_trades.schemas
import main.save_final_data_set.schemas
from enumerations import SymbolId, OKXOrderBookActionId
from main.save_final_data_set.globals import (
    g_globals,
)

logger = logging.getLogger(
    __name__,
)


_SYMBOL_IDS = [
    SymbolId.BTC_USDT,
    SymbolId.ETH_USDT,
]


async def init_db_models():
    postgres_db_engine = g_globals.get_postgres_db_engine()

    async with postgres_db_engine.begin() as connection:
        # await connection.run_sync(
        #     models.Base.metadata.drop_all
        # )

        await connection.run_sync(
            main.save_final_data_set.schemas.Base.metadata.create_all,
        )


async def save_final_data_set(
        symbol_id: SymbolId,
) -> None:
    final_data_set_record_db_schema = (
        main.save_final_data_set.schemas.OKXDataSetRecordData
    )

    order_book_data_db_schema = (
        main.save_order_books.schemas.OKXOrderBookData2
    )

    postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

    async with postgres_db_session_maker() as session:
        # Step 1: get last final data set record data

        async with session.begin():
            result = await session.execute(
                select(
                    final_data_set_record_db_schema
                ).where(
                    final_data_set_record_db_schema.symbol_id == symbol_id,
                ).order_by(
                    final_data_set_record_db_schema.symbol_id.asc(),
                    final_data_set_record_db_schema.data_set_idx.desc(),
                    final_data_set_record_db_schema.record_idx.desc(),
                ).limit(
                    1,
                ),
            )

            last_final_data_set_record_data: (
                main.save_final_data_set.schemas.OKXDataSetRecordData | None
            ) = result.scalar_one_or_none()

        min_timestamp_ms: int

        if last_final_data_set_record_data is not None:
            min_timestamp_ms = last_final_data_set_record_data.end_timestamp_ms
        else:
            min_timestamp_ms = 0

        # Step 2: get 2 OKXOrderBookData2 Snapshots after last final data set snapshot

        async with session.begin():
            result = await session.execute(
                select(
                    order_book_data_db_schema
                ).where(
                    and_(
                        order_book_data_db_schema.symbol_id == symbol_id,
                        order_book_data_db_schema.timestamp_ms >= min_timestamp_ms,
                        order_book_data_db_schema.action_id == OKXOrderBookActionId.Snapshot,
                    )
                ).order_by(
                    order_book_data_db_schema.symbol_id.asc(),
                    order_book_data_db_schema.timestamp_ms.asc(),
                ).limit(
                    2,
                ),
            )

            order_book_snapshots: (
                list[main.save_order_books.schemas.OKXOrderBookData2]
            ) = result.scalars().all()

        if len(order_book_snapshots) < 2:
            logger.info(
                f'There are only {len(order_book_snapshots)} order book snapshots'
                '; skipping final data set saving.'
            )

            return

        (
            start_order_book_snapshot_data,
            end_order_book_snapshot_data,
        ) = order_book_snapshots

        logger.info(
            'Start order book snapshot timestamp (ms)'
            f': {start_order_book_snapshot_data.timestamp_ms}'
            '; end order book snapshot timestamp (ms)'
            f': {end_order_book_snapshot_data.timestamp_ms}'
        )

        # TODO:
        # Step 3: fetch all order book updates between two snapshots
        # Step 4: fetch all trades between two snapshots
        # Step 5: save final data set records into DB


async def start_save_final_data_sets_loop() -> None:
    while True:
        for symbol_id in _SYMBOL_IDS:
            try:
                await save_final_data_set(
                    symbol_id,
                )
            except Exception as exception:
                logger.error(
                    'Handled exception while saving final data set'
                    f' of symbol with ID {symbol_id.name}'
                    f': {"".join(traceback.format_exception(exception))}',
                )

            await asyncio.sleep(
                1.0  # s
            )


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

    # Prepare DB

    await init_db_models()

    # Start saving final data sets



if __name__ == '__main__':
    uvloop.run(
        main(),
    )
