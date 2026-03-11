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

from main.save_final_data_set_3.schemas import OKXDataSetRecordData_3

try:
    import uvloop
except ImportError:
    uvloop = asyncio

import main.save_final_data_set_2.schemas
from enumerations import (
    SymbolId, TradingDirection,
)
from main.save_final_data_set_3.globals import (
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
            main.save_final_data_set_3.schemas.Base.metadata.create_all,
        )


def get_direction(
        open_price: float | Decimal,
        close_price: float | Decimal
) -> TradingDirection:
    if close_price > open_price:
        return TradingDirection.Bull
    elif close_price < open_price:
        return TradingDirection.Bear
    else:
        return TradingDirection.Cross


async def save_final_data_set_3(
        symbol_id: SymbolId,
) -> None:
    logger.info(
        f'Saving final data set 3 for symbol with ID {symbol_id.name}'
    )

    final_data_set_record_data_3_db_schema = (
        main.save_final_data_set_3.schemas.OKXDataSetRecordData_3
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
                    final_data_set_record_data_3_db_schema
                ).where(
                    final_data_set_record_data_3_db_schema.symbol_id == symbol_id,
                ).order_by(
                    final_data_set_record_data_3_db_schema.symbol_id.asc(),
                    final_data_set_record_data_3_db_schema.start_trade_id.desc(),
                ).limit(
                    1,
                ),
            )

            last_final_data_set_record_data: (
                main.save_final_data_set_3.schemas.OKXDataSetRecordData_3 | None
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

        direction: TradingDirection | None

        # Timestamp

        end_timestamp_ms: int | None
        last_final_data_set_record_start_trade_id: int | None

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
            last_final_data_set_record_start_trade_id = last_final_data_set_record_data.start_trade_id
            end_trade_id = last_final_data_set_record_data.end_trade_id

            direction = get_direction(
                open_price,
                close_price,
            )

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
            end_trade_id = None
            last_final_data_set_record_start_trade_id = None

            direction = None

            end_timestamp_ms = None
            start_timestamp_ms = None

            total_quantity = Decimal(0)
            total_trades_count = 0
            total_volume = Decimal(0)

        start_trade_id = last_final_data_set_record_start_trade_id

        logger.info(
            'start_trade_id'
            f': {start_trade_id}'
        )

        # Step 2:
        # - Fetch 100000 records

        final_data_set_record_data_2_db_schema = (
            main.save_final_data_set_2.schemas.OKXDataSetRecordData_2
        )

        async with session_read_1.begin():
            if start_trade_id is not None:
                where_and_expression_1 = and_(
                    final_data_set_record_data_2_db_schema.symbol_id == symbol_id,
                    final_data_set_record_data_2_db_schema.start_trade_id >= start_trade_id,
                )
            else:
                where_and_expression_1 = and_(
                    final_data_set_record_data_2_db_schema.symbol_id == symbol_id,
                )

            result_1 = await session_read_1.execute(
                select(
                    final_data_set_record_data_2_db_schema,
                ).where(
                    where_and_expression_1,
                ).order_by(
                    final_data_set_record_data_2_db_schema.symbol_id.asc(),
                    final_data_set_record_data_2_db_schema.start_trade_id.asc(),
                ).limit(
                    100000,  # TODO: move to constants
                )
            )

            final_data_set_records_2: list[main.save_final_data_set_2.schemas.OKXDataSetRecordData_2] = result_1.scalars().all()

        final_data_set_records_2_count = len(
            final_data_set_records_2,
        )

        logger.info(
            f'Fetched {final_data_set_records_2_count} records',
        )

        if not final_data_set_records_2_count:
            return

        # Processing records

        data_set_records_3: list[OKXDataSetRecordData_3] = []

        for record_idx, record_data in enumerate(
                final_data_set_records_2,
        ):
            if record_idx % 1000 == 0:
                logger.info(
                    f'Processed {record_idx} records. Committing...',
                )

            sell_volume = record_data.total_volume - record_data.buy_volume
            delta_volume = record_data.buy_volume - sell_volume

            new_direction: TradingDirection

            if delta_volume > 0:
                new_direction = TradingDirection.Bull
            elif delta_volume < 0:
                new_direction = TradingDirection.Bear
            else:
                new_direction = TradingDirection.Cross

            if record_data.open_price == record_data.close_price:
                continue

            if direction is None:
                direction = new_direction
            elif new_direction != direction:
                # Flush

                data_set_records_3.append(
                    OKXDataSetRecordData_3(
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

                close_price = None
                high_price = None
                low_price = None
                open_price = None
                start_trade_id = None
                end_trade_id = None

                direction = new_direction

                end_timestamp_ms = None
                start_timestamp_ms = None

                total_quantity = Decimal(0)
                total_trades_count = 0
                total_volume = Decimal(0)

            if start_trade_id is None:
                start_trade_id = record_data.start_trade_id

            end_trade_id = record_data.end_trade_id

            if start_timestamp_ms is None:
                start_timestamp_ms = record_data.start_timestamp_ms

            end_timestamp_ms = record_data.end_timestamp_ms

            new_high_price = record_data.high_price

            if high_price is None or new_high_price > high_price:
                high_price = new_high_price

            new_low_price = record_data.low_price

            if low_price is None or new_low_price < low_price:
                low_price = new_low_price

            if open_price is None:
                open_price = record_data.open_price

            close_price = record_data.close_price

            buy_quantity += record_data.buy_quantity
            buy_trades_count += record_data.buy_trades_count
            buy_volume += record_data.buy_volume

            total_quantity += record_data.total_quantity
            total_trades_count += record_data.total_trades_count
            total_volume += record_data.total_volume

        if total_trades_count:
            # assert direction is not None, None
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

            # # Flush
            # Don't flush

            # data_set_records_2.append(
            #     OKXDataSetRecordData_2(
            #         # Primary key fields
            #         symbol_id=symbol_id,
            #         start_trade_id=start_trade_id,
            #         # Attribute fields
            #         buy_quantity=buy_quantity,
            #         buy_trades_count=buy_trades_count,
            #         buy_volume=buy_volume,
            #         close_price=close_price,
            #         end_timestamp_ms=end_timestamp_ms,
            #         end_trade_id=end_trade_id,
            #         high_price=high_price,
            #         low_price=low_price,
            #         open_price=open_price,
            #         start_timestamp_ms=start_timestamp_ms,
            #         total_quantity=total_quantity,
            #         total_trades_count=total_trades_count,
            #         total_volume=total_volume,
            #     )
            # )

            buy_quantity = Decimal(0)
            buy_trades_count = 0
            buy_volume = Decimal(0)

            close_price = None
            high_price = None
            low_price = None
            open_price = None
            start_trade_id = None
            end_trade_id = None

            direction = None

            end_timestamp_ms = None
            start_timestamp_ms = None

            total_quantity = Decimal(0)
            total_trades_count = 0
            total_volume = Decimal(0)

        async with (
            session_read_1.begin(),
            session_write.begin()
        ):
            for data_set_record_3 in data_set_records_3:
                if last_final_data_set_record_start_trade_id is not None and (
                    data_set_record_3.start_trade_id
                    == last_final_data_set_record_start_trade_id
                ):
                    # Update

                    await session_write.execute(
                        update(
                            final_data_set_record_data_3_db_schema,
                        )
                        .values(
                            # Attribute fields
                            buy_quantity=data_set_record_3.buy_quantity,
                            buy_trades_count=data_set_record_3.buy_trades_count,
                            buy_volume=data_set_record_3.buy_volume,
                            close_price=data_set_record_3.close_price,
                            end_timestamp_ms=data_set_record_3.end_timestamp_ms,
                            end_trade_id=data_set_record_3.end_trade_id,
                            high_price=data_set_record_3.high_price,
                            low_price=data_set_record_3.low_price,
                            open_price=data_set_record_3.open_price,
                            start_timestamp_ms=data_set_record_3.start_timestamp_ms,
                            total_quantity=data_set_record_3.total_quantity,
                            total_trades_count=data_set_record_3.total_trades_count,
                            total_volume=data_set_record_3.total_volume,
                        )
                        .where(
                            and_(
                                final_data_set_record_data_3_db_schema.symbol_id == symbol_id,
                                final_data_set_record_data_3_db_schema.start_trade_id == data_set_record_3.start_trade_id,
                            ),
                        )
                    )
                else:
                    session_write.add(
                        data_set_record_3,
                    )

        await session_write.commit()

    logger.info(
        'Final data set 3 records were saved!',
    )


async def start_save_final_data_sets_loop() -> None:
    while True:
        for symbol_id in _SYMBOL_IDS:
            try:
                await save_final_data_set_3(
                    symbol_id,
                )
            except Exception as exception:
                logger.error(
                    'Handled exception while saving final data set 3'
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
