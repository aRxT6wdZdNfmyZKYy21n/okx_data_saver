import asyncio
import logging
import traceback
from decimal import (
    Decimal,
)

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
import main.save_final_data_set_2.schemas
from enumerations import (
    OKXOrderBookActionId,
    SymbolId,
)
from main.save_final_data_set_2.globals import (
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
            main.save_final_data_set_2.schemas.Base.metadata.create_all,
        )


async def save_final_data_set_2(
        symbol_id: SymbolId,
) -> None:
    logger.info(
        f'Saving final data set 2 for symbol with ID {symbol_id.name}'
    )

    final_data_set_record_db_schema = (
        main.save_final_data_set_2.schemas.OKXDataSetRecordData_2
    )

    postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

    async with (
        postgres_db_session_maker() as session_read_1,
        postgres_db_session_maker() as session_read_2,
        postgres_db_session_maker() as session_write,
    ):
        # Step 1: get last final data set 2 record data

        async with session_read_1.begin():
            result = await session_read_1.execute(
                select(
                    final_data_set_record_db_schema
                ).where(
                    final_data_set_record_db_schema.symbol_id == symbol_id,
                ).order_by(
                    final_data_set_record_db_schema.symbol_id.asc(),
                    final_data_set_record_db_schema.start_trade_id.desc(),
                ).limit(
                    1,
                ),
            )

            last_final_data_set_record_data: (
                main.save_final_data_set_2.schemas.OKXDataSetRecordData_2 | None
            ) = result.scalar_one_or_none()

        # Buy

        buy_quantity: Decimal
        buy_trades_count: int
        buy_volume: Decimal

        # Prices

        close_price: Decimal | None
        high_price: Decimal | None
        low_price: Decimal | None
        open_price: Decimal | None
        start_trade_id: int | None
        end_trade_id: int | None

        # Timestamp

        end_timestamp_ms: int | None
        start_timestamp_ms: int | None

        # Total

        total_quantity: Decimal
        total_trades_count: int
        total_volume: Decimal

        if last_final_data_set_record_data is not None:
            # Buy

            buy_quantity = last_final_data_set_record_data.buy_quantity
            buy_trades_count = last_final_data_set_record_data.buy_trades_count
            buy_volume = last_final_data_set_record_data.buy_volume

            # Prices

            close_price = last_final_data_set_record_data.close_price
            high_price = last_final_data_set_record_data.high_price
            low_price = last_final_data_set_record_data.low_price
            open_price = last_final_data_set_record_data.open_price
            start_trade_id = last_final_data_set_record_data.start_trade_id
            end_trade_id = last_final_data_set_record_data.end_trade_id

            # Timestamp

            end_timestamp_ms = last_final_data_set_record_data.end_timestamp_ms
            start_timestamp_ms = last_final_data_set_record_data.start_timestamp_ms

            total_quantity = last_final_data_set_record_data.total_quantity
            total_trades_count = last_final_data_set_record_data.total_trades_count
            total_volume = last_final_data_set_record_data.total_volume
        else:
            buy_quantity = Decimal(0)
            buy_trades_count = 0
            buy_volume = Decimal(0)

            close_price = None
            high_price = None
            low_price = None
            open_price = None
            start_trade_id = None
            end_trade_id = None

            end_timestamp_ms = None
            start_timestamp_ms = None

            total_quantity = Decimal(0)
            total_trades_count = 0
            total_volume = Decimal(0)

        logger.info(
            'start_timestamp_ms'
            f': {start_timestamp_ms}'
        )

        # Step 2:
        # - Fetch 10000 trades

        trade_data_db_schema = (
            main.save_trades.schemas.OKXTradeData2
        )

        async with session_read_1.begin():
            if start_timestamp_ms is not None:
                where_and_expression_1 = and_(
                    trade_data_db_schema.symbol_id == symbol_id,
                    trade_data_db_schema.timestamp_ms >= start_timestamp_ms,
                )
            else:
                where_and_expression_1 = and_(
                    trade_data_db_schema.symbol_id == symbol_id,
                )

            result_1 = await session_read_1.execute(
                select(
                    trade_data_db_schema,
                ).where(
                    where_and_expression_1,
                ).order_by(
                    trade_data_db_schema.symbol_id.asc(),
                    trade_data_db_schema.trade_id.asc(),
                ).limit(
                    10000,  # TODO: move to constants
                )
            )

            trades: list[main.save_trades.schemas.OKXTradeData2] = result_1.scalars().all()

        trades_count = len(
            trades,
        )

        logger.info(
            f'Fetched {trades_count} trades',
        )

        if not trades_count:
            return

        async with session_read_1.begin():
            # Processing trades  # TODO

            for trade_data in trades:
                trade_id = trade_data.trade_id

                if start_trade_id is None:
                    start_trade_id = trade_id

                trade_timestamp_ms = trade_data.timestamp_ms

                end_trade_id = trade_id

                trade_price = trade_data.price

                if high_price is None or trade_price > high_price:
                    high_price = trade_price

                if low_price is None or trade_price < low_price:
                    low_price = trade_price

                if open_price is None:
                    open_price = trade_price

                close_price = trade_price

                trade_quantity = trade_data.quantity

                trade_volume = (
                    trade_price
                    * trade_quantity
                )

                if trade_data.is_buy:
                    buy_quantity += trade_quantity
                    buy_trades_count += 1
                    buy_volume += trade_volume

                total_quantity += trade_quantity
                total_trades_count += 1
                total_volume += trade_volume

            if not total_trades_count:
                continue

            assert open_price is not None, None
            assert close_price is not None, None
            assert high_price is not None, None
            assert low_price is not None, None

            result_3 = await session_read_2.execute(
                select(
                    data_set_record_data_db_schema,
                ).where(
                    and_(
                        data_set_record_data_db_schema.symbol_id == symbol_id,
                        data_set_record_data_db_schema.data_set_idx == new_final_data_set_idx,
                        data_set_record_data_db_schema.record_idx == record_idx,
                    )
                ).limit(
                    1,
                )
            )

            final_data_set_record_data = result_3.scalar_one_or_none()

            if final_data_set_record_data is None:
                final_data_set_record_data = (
                    data_set_record_data_db_schema(
                        # Primary key fields
                        symbol_id=symbol_id,
                        data_set_idx=new_final_data_set_idx,
                        record_idx=record_idx,
                        # Attribute fields
                        buy_quantity=buy_quantity,
                        buy_trades_count=buy_trades_count,
                        buy_volume=buy_volume,
                        close_price=close_price,
                        end_asks_total_quantity=end_asks_total_quantity,
                        end_asks_total_volume=end_asks_total_volume,
                        max_end_ask_price=max_end_ask_price,
                        max_end_ask_quantity=max_end_ask_quantity,
                        max_end_ask_volume=max_end_ask_volume,
                        min_end_ask_price=min_end_ask_price,
                        min_end_ask_quantity=min_end_ask_quantity,
                        min_end_ask_volume=min_end_ask_volume,
                        end_bids_total_quantity=end_bids_total_quantity,
                        end_bids_total_volume=end_bids_total_volume,
                        max_end_bid_price=max_end_bid_price,
                        max_end_bid_quantity=max_end_bid_quantity,
                        max_end_bid_volume=max_end_bid_volume,
                        min_end_bid_price=min_end_bid_price,
                        min_end_bid_quantity=min_end_bid_quantity,
                        min_end_bid_volume=min_end_bid_volume,
                        end_timestamp_ms=end_timestamp_ms,
                        end_trade_id=end_trade_id,
                        high_price=high_price,
                        start_asks_total_quantity=start_asks_total_quantity,
                        start_asks_total_volume=start_asks_total_volume,
                        max_start_ask_price=max_start_ask_price,
                        max_start_ask_quantity=max_start_ask_quantity,
                        max_start_ask_volume=max_start_ask_volume,
                        min_start_ask_price=min_start_ask_price,
                        min_start_ask_quantity=min_start_ask_quantity,
                        min_start_ask_volume=min_start_ask_volume,
                        start_bids_total_quantity=start_bids_total_quantity,
                        start_bids_total_volume=start_bids_total_volume,
                        max_start_bid_price=max_start_bid_price,
                        max_start_bid_quantity=max_start_bid_quantity,
                        max_start_bid_volume=max_start_bid_volume,
                        min_start_bid_price=min_start_bid_price,
                        min_start_bid_quantity=min_start_bid_quantity,
                        min_start_bid_volume=min_start_bid_volume,
                        low_price=low_price,
                        open_price=open_price,
                        start_timestamp_ms=start_timestamp_ms,
                        start_trade_id=start_trade_id,
                        total_quantity=total_quantity,
                        total_trades_count=total_trades_count,
                        total_volume=total_volume,
                    )
                )

                session_write.add(
                    final_data_set_record_data,
                )

            record_idx += 1

            if record_idx % 1000 == 0:
                logger.info(
                    f'Processed {record_idx} records. Committing...',
                )

                await session_write.commit()

        await session_write.commit()

    logger.info(
        'Final data set 2 records were saved!',
    )


async def start_save_final_data_sets_loop() -> None:
    while True:
        for symbol_id in _SYMBOL_IDS:
            try:
                await save_final_data_set_2 (
                    symbol_id,
                )
            except Exception as exception:
                logger.error(
                    'Handled exception while saving final data set 2'
                    f' of symbol with ID {symbol_id.name}'
                    f': {"".join(traceback.format_exception(exception))}',
                )

            await asyncio.sleep(
                1.0  # s
            )


async def main_() -> None:
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

    # Start final data sets saving loop

    await start_save_final_data_sets_loop()


if __name__ == '__main__':
    uvloop.run(
        main_(),
    )
