import asyncio
import logging
import traceback
from decimal import (
    Decimal,
)

from sqlalchemy import (
    and_,
    select,
    update,
)

from main.save_final_data_set_2.schemas import OKXDataSetRecordData_2

try:
    import uvloop
except ImportError:
    uvloop = asyncio

import main.save_order_books.schemas
import main.save_trades.schemas
import main.save_final_data_set_2.schemas
from enumerations import (
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

    final_data_set_record_2_db_schema = (
        main.save_final_data_set_2.schemas.OKXDataSetRecordData_2
    )

    postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

    async with (
        postgres_db_session_maker() as session_read_1,
        postgres_db_session_maker() as session_write,
    ):
        # Step 1: get last final data set 2 record data

        async with session_read_1.begin():
            result = await session_read_1.execute(
                select(
                    final_data_set_record_2_db_schema
                ).where(
                    final_data_set_record_2_db_schema.symbol_id == symbol_id,
                ).order_by(
                    final_data_set_record_2_db_schema.symbol_id.asc(),
                    final_data_set_record_2_db_schema.start_trade_id.desc(),
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
        is_buy: bool | None

        # Prices

        close_price: Decimal | None
        high_price: Decimal | None
        low_price: Decimal | None
        open_price: Decimal | None
        start_trade_id: int | None
        end_trade_id: int | None

        # Timestamp

        end_timestamp_ms: int | None
        last_final_data_set_record_start_timestamp_ms: int | None

        # Total

        total_quantity: Decimal
        total_trades_count: int
        total_volume: Decimal

        if last_final_data_set_record_data is not None:
            # Buy

            buy_quantity = last_final_data_set_record_data.buy_quantity
            buy_trades_count = last_final_data_set_record_data.buy_trades_count
            buy_volume = last_final_data_set_record_data.buy_volume
            is_buy = buy_trades_count > 0  # Only for first (initial) level

            # Prices

            close_price = last_final_data_set_record_data.close_price
            high_price = last_final_data_set_record_data.high_price
            low_price = last_final_data_set_record_data.low_price
            open_price = last_final_data_set_record_data.open_price
            start_trade_id = last_final_data_set_record_data.start_trade_id
            end_trade_id = last_final_data_set_record_data.end_trade_id

            # Timestamp

            end_timestamp_ms = last_final_data_set_record_data.end_timestamp_ms
            last_final_data_set_record_start_timestamp_ms = last_final_data_set_record_data.start_timestamp_ms

            total_quantity = last_final_data_set_record_data.total_quantity
            total_trades_count = last_final_data_set_record_data.total_trades_count
            total_volume = last_final_data_set_record_data.total_volume
        else:
            buy_quantity = Decimal(0)
            buy_trades_count = 0
            buy_volume = Decimal(0)
            is_buy = None

            close_price = None
            high_price = None
            low_price = None
            open_price = None
            start_trade_id = None
            end_trade_id = None

            end_timestamp_ms = None
            last_final_data_set_record_start_timestamp_ms = None

            total_quantity = Decimal(0)
            total_trades_count = 0
            total_volume = Decimal(0)

        start_timestamp_ms = last_final_data_set_record_start_timestamp_ms

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

        # Processing trades  # TODO

        data_set_records_2: list[OKXDataSetRecordData_2] = []

        for trade_idx, trade_data in enumerate(
                trades,
        ):
            if trade_idx % 1000 == 0:
                logger.info(
                    f'Processed {trade_idx} records. Committing...',
                )

            if is_buy is None:
                is_buy = trade_data.is_buy
            elif is_buy != trade_data.is_buy:
                # Flush

                data_set_records_2.append(
                    OKXDataSetRecordData_2(
                        # Primary key fields
                        symbol_id=symbol_id,
                        start_trade_id=start_trade_id,
                        # Attribute fields
                        buy_quantity=buy_quantity,
                        buy_trades_count=buy_trades_count,
                        buy_volume=buy_volume,
                        close_price=close_price,
                        end_timestamp_ms=end_timestamp_ms,
                        end_trade_id=end_trade_id,
                        high_price=high_price,
                        low_price=low_price,
                        open_price=open_price,
                        start_timestamp_ms=start_timestamp_ms,
                        total_quantity=total_quantity,
                        total_trades_count=total_trades_count,
                        total_volume=total_volume,
                    )
                )

                buy_quantity = Decimal(0)
                buy_trades_count = 0
                buy_volume = Decimal(0)
                is_buy = trade_data.is_buy

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

            trade_id = trade_data.trade_id

            if start_trade_id is None:
                start_trade_id = trade_id

            end_trade_id = trade_id

            trade_timestamp_ms = trade_data.timestamp_ms

            if start_timestamp_ms is None:
                start_timestamp_ms = trade_timestamp_ms

            end_timestamp_ms = trade_timestamp_ms

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

        if total_trades_count:
            assert is_buy is not None, None
            assert open_price is not None, None
            assert close_price is not None, None
            assert high_price is not None, None
            assert low_price is not None, None

            assert start_trade_id is not None, None
            assert end_trade_id is not None, None

            assert end_timestamp_ms is not None, None
            assert start_timestamp_ms is not None, None

            assert total_quantity, None
            assert total_volume, None

            # Flush

            data_set_records_2.append(
                OKXDataSetRecordData_2(
                    # Primary key fields
                    symbol_id=symbol_id,
                    start_trade_id=start_trade_id,
                    # Attribute fields
                    buy_quantity=buy_quantity,
                    buy_trades_count=buy_trades_count,
                    buy_volume=buy_volume,
                    close_price=close_price,
                    end_timestamp_ms=end_timestamp_ms,
                    end_trade_id=end_trade_id,
                    high_price=high_price,
                    low_price=low_price,
                    open_price=open_price,
                    start_timestamp_ms=start_timestamp_ms,
                    total_quantity=total_quantity,
                    total_trades_count=total_trades_count,
                    total_volume=total_volume,
                )
            )

            buy_quantity = Decimal(0)
            buy_trades_count = 0
            buy_volume = Decimal(0)
            is_buy = None

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

        async with (
            session_read_1.begin(),
            session_write.begin()
        ):
            for data_set_record_2 in data_set_records_2:
                if last_final_data_set_record_start_timestamp_ms is not None and (
                    data_set_record_2.start_timestamp_ms
                    == last_final_data_set_record_start_timestamp_ms
                ):
                    # Update

                    await session_write.execute(
                        update(
                            final_data_set_record_2_db_schema,
                        )
                        .values(
                            # Attribute fields
                            buy_quantity=data_set_record_2.buy_quantity,
                            buy_trades_count=data_set_record_2.buy_trades_count,
                            buy_volume=data_set_record_2.buy_volume,
                            close_price=data_set_record_2.close_price,
                            end_timestamp_ms=data_set_record_2.end_timestamp_ms,
                            end_trade_id=data_set_record_2.end_trade_id,
                            high_price=data_set_record_2.high_price,
                            low_price=data_set_record_2.low_price,
                            open_price=data_set_record_2.open_price,
                            start_timestamp_ms=data_set_record_2.start_timestamp_ms,
                            total_quantity=data_set_record_2.total_quantity,
                            total_trades_count=data_set_record_2.total_trades_count,
                            total_volume=data_set_record_2.total_volume,
                        )
                        .where(
                            and_(
                                final_data_set_record_2_db_schema.symbol_id == symbol_id,
                                final_data_set_record_2_db_schema.start_trade_id == start_trade_id,
                            ),
                        )
                    )
                else:
                    session_write.add(
                        data_set_record_2,
                    )

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
