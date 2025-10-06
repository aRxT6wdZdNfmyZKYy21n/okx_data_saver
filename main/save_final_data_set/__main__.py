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
import main.save_final_data_set.schemas
from enumerations import (
    OKXOrderBookActionId,
    SymbolId,
)
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
        new_final_data_set_idx: int

        if last_final_data_set_record_data is not None:
            min_timestamp_ms = last_final_data_set_record_data.end_timestamp_ms
            new_final_data_set_idx = last_final_data_set_record_data.data_set_idx + 1
        else:
            min_timestamp_ms = 0
            new_final_data_set_idx = 0

        # Step 2: get 2 OKXOrderBookData2 Snapshots after last final data set snapshot

        order_book_data_db_schema = (
            main.save_order_books.schemas.OKXOrderBookData2
        )

        async with session.begin():
            result = await session.execute(
                select(
                    order_book_data_db_schema,
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

        end_order_book_snapshot_timestamp_ms = (
            end_order_book_snapshot_data.timestamp_ms
        )

        start_order_book_snapshot_timestamp_ms = (
            start_order_book_snapshot_data.timestamp_ms
        )

        logger.info(
            'Start order book snapshot timestamp (ms)'
            f': {start_order_book_snapshot_timestamp_ms}'
            '; end order book snapshot timestamp (ms)'
            f': {end_order_book_snapshot_timestamp_ms}'
        )

        # Step 3: fetch all order book updates between two snapshots

        async with session.begin():
            result = await session.execute(
                select(
                    order_book_data_db_schema,
                ).where(
                    and_(
                        order_book_data_db_schema.symbol_id == symbol_id,
                        order_book_data_db_schema.timestamp_ms >= start_order_book_snapshot_timestamp_ms,
                        order_book_data_db_schema.timestamp_ms < end_order_book_snapshot_timestamp_ms,
                        order_book_data_db_schema.action_id == OKXOrderBookActionId.Update,
                    )
                ).order_by(
                    order_book_data_db_schema.symbol_id.asc(),
                    order_book_data_db_schema.timestamp_ms.asc(),
                ),
            )

            order_book_updates: (
                list[main.save_order_books.schemas.OKXOrderBookData2]
            ) = result.scalars().all()

        logger.info(
            f'Fetched {len(order_book_updates)} order book updates',
        )

        # Step 4: fetch all trades between two snapshots

        trade_data_db_schema = (
            main.save_trades.schemas.OKXTradeData2
        )

        async with session.begin():
            result = await session.execute(
                select(
                    trade_data_db_schema,
                ).where(
                    and_(
                        trade_data_db_schema.symbol_id == symbol_id,
                        trade_data_db_schema.timestamp_ms >= start_order_book_snapshot_timestamp_ms,
                        trade_data_db_schema.timestamp_ms < end_order_book_snapshot_timestamp_ms,
                    )
                ).order_by(
                    order_book_data_db_schema.symbol_id.asc(),
                    order_book_data_db_schema.timestamp_ms.asc(),
                ),
            )

            trades: (
                list[main.save_trades.schemas.OKXTradeData2]
            ) = result.scalars().all()

        logger.info(
            f'Fetched {len(trades)} trades',
        )

        # Step 5: save final data set records into DB

        order_books = [
            start_order_book_snapshot_data,
        ]

        order_books.extend(
            order_book_updates,
        )

        order_books_count = len(
            order_books,
        )

        ask_quantity_by_price_map: dict[Decimal, Decimal] | None = None
        bid_quantity_by_price_map: dict[Decimal, Decimal] | None = None

        async with session.begin():
            for current_order_book_idx in range(
                order_books_count - 1
            ):
                if current_order_book_idx % 1000 == 0:
                    logger.info(
                        f'Processed {current_order_book_idx} / {order_books_count} order books',
                    )

                current_order_book_data = order_books[current_order_book_idx]
                current_order_book_timestamp_ms = current_order_book_data.timestamp_ms

                next_order_book_idx = current_order_book_idx + 1

                next_order_book_data = order_books[next_order_book_idx]
                next_order_book_timestamp_ms = next_order_book_data.timestamp_ms

                record_idx = current_order_book_idx

                # Processing trades

                buy_quantity = 0
                buy_trades_count = 0
                buy_volume = 0

                close_price: Decimal | None = None
                high_price: Decimal | None = None
                low_price: Decimal | None = None
                open_price: Decimal | None = None
                start_trade_id: int | None = None
                end_trade_id: int | None = None

                total_quantity = 0
                total_trades_count = 0
                total_volume = 0

                for trade_data in trades:
                    trade_timestamp_ms = trade_data.timestamp_ms

                    if trade_timestamp_ms < current_order_book_timestamp_ms:
                        continue
                    elif trade_timestamp_ms >= next_order_book_timestamp_ms:
                        break

                    trade_id = trade_data.trade_id

                    if start_trade_id is None:
                        start_trade_id = trade_id

                    end_trade_id = trade_id

                    trade_price = trade_data.price

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

                close_price_delta: Decimal | None
                high_price_delta: Decimal | None
                low_price_delta: Decimal | None

                if open_price is not None:
                    assert close_price is not None, None
                    assert high_price is not None, None
                    assert low_price is not None, None

                    close_price_delta = close_price - open_price
                    high_price_delta = high_price - open_price
                    low_price_delta = low_price - open_price
                else:
                    close_price_delta = None
                    high_price_delta = None
                    low_price_delta = None

                # Processing start order book

                if ask_quantity_by_price_map is None:
                    assert bid_quantity_by_price_map is None, None
                    assert current_order_book_data.action_id == OKXOrderBookActionId.Snapshot, None

                    ask_quantity_by_price_map = {}

                    price_raw: str
                    quantity_raw: str

                    for ask_list in current_order_book_data.asks:
                        (
                            price_raw,
                            quantity_raw,
                            _,
                            _
                        ) = ask_list

                        price = Decimal(
                            price_raw,
                        )

                        quantity = Decimal(
                            quantity_raw,
                        )

                        assert quantity, (
                            current_order_book_timestamp_ms,
                            ask_list,
                        )

                        ask_quantity_by_price_map[price] = quantity

                    bid_quantity_by_price_map = {}

                    for bid_list in current_order_book_data.bids:
                        (
                            price_raw,
                            quantity_raw,
                            _,
                            _
                        ) = bid_list

                        price = Decimal(
                            price_raw,
                        )

                        quantity = Decimal(
                            quantity_raw,
                        )

                        assert quantity, (
                            current_order_book_timestamp_ms,
                            bid_list,
                        )

                        bid_quantity_by_price_map[price] = quantity
                else:
                    assert bid_quantity_by_price_map is not None, None
                    assert current_order_book_data.action_id == OKXOrderBookActionId.Update, None

                start_asks_total_quantity = 0
                start_asks_total_volume = 0
                max_start_ask_price: Decimal | None = None
                max_start_ask_quantity: Decimal | None = None
                max_start_ask_volume: Decimal | None = None
                min_start_ask_price: Decimal | None = None
                min_start_ask_quantity: Decimal | None = None
                min_start_ask_volume: Decimal | None = None

                for ask_price, ask_quantity in ask_quantity_by_price_map.items():
                    if max_start_ask_price is None or ask_price > max_start_ask_price:
                        max_start_ask_price = ask_price

                    if min_start_ask_price is None or ask_price < min_start_ask_price:
                        min_start_ask_price = ask_price

                    start_asks_total_quantity += ask_quantity

                    if max_start_ask_quantity is None or ask_quantity > max_start_ask_quantity:
                        max_start_ask_quantity = ask_quantity

                    if min_start_ask_quantity is None or ask_quantity < min_start_ask_quantity:
                        min_start_ask_quantity = ask_quantity

                    ask_volume = (
                        ask_price
                        * ask_quantity
                    )

                    start_asks_total_volume += ask_volume

                    if max_start_ask_volume is None or ask_volume > max_start_ask_volume:
                        max_start_ask_volume = ask_volume

                    if min_start_ask_volume is None or ask_volume < min_start_ask_volume:
                        min_start_ask_volume = ask_volume

                assert max_start_ask_price is not None, None
                assert max_start_ask_quantity is not None, None
                assert max_start_ask_volume is not None, None
                assert min_start_ask_price is not None, None
                assert min_start_ask_quantity is not None, None
                assert min_start_ask_volume is not None, None

                start_bids_total_quantity = 0
                start_bids_total_volume = 0
                max_start_bid_price: Decimal | None = None
                max_start_bid_quantity: Decimal | None = None
                max_start_bid_volume: Decimal | None = None
                min_start_bid_price: Decimal | None = None
                min_start_bid_quantity: Decimal | None = None
                min_start_bid_volume: Decimal | None = None

                for bid_price, bid_quantity in bid_quantity_by_price_map.items():
                    if max_start_bid_price is None or bid_price > max_start_bid_price:
                        max_start_bid_price = bid_price

                    if min_start_bid_price is None or bid_price < min_start_bid_price:
                        min_start_bid_price = bid_price

                    start_bids_total_quantity += bid_quantity

                    if max_start_bid_quantity is None or bid_quantity > max_start_bid_quantity:
                        max_start_bid_quantity = bid_quantity

                    if min_start_bid_quantity is None or bid_quantity < min_start_bid_quantity:
                        min_start_bid_quantity = bid_quantity

                    bid_volume = (
                        bid_price
                        * bid_quantity
                    )

                    start_bids_total_volume += bid_volume

                    if max_start_bid_volume is None or bid_volume > max_start_bid_volume:
                        max_start_bid_volume = bid_volume

                    if min_start_bid_volume is None or bid_volume < min_start_bid_volume:
                        min_start_bid_volume = bid_volume

                assert max_start_bid_price is not None, None
                assert max_start_bid_quantity is not None, None
                assert max_start_bid_volume is not None, None
                assert min_start_bid_price is not None, None
                assert min_start_bid_quantity is not None, None
                assert min_start_bid_volume is not None, None

                # Processing end order book

                assert next_order_book_data.action_id == OKXOrderBookActionId.Update, None

                price_raw: str
                quantity_raw: str

                for ask_list in next_order_book_data.asks:
                    (
                        price_raw,
                        quantity_raw,
                        _,
                        _
                    ) = ask_list

                    price = Decimal(
                        price_raw,
                    )

                    quantity = Decimal(
                        quantity_raw,
                    )

                    if quantity:
                        ask_quantity_by_price_map[price] = quantity
                    else:
                        ask_quantity_by_price_map.pop(
                            price,
                            None,
                        )

                for bid_list in next_order_book_data.bids:
                    (
                        price_raw,
                        quantity_raw,
                        _,
                        _
                    ) = bid_list

                    price = Decimal(
                        price_raw,
                    )

                    quantity = Decimal(
                        quantity_raw,
                    )

                    if quantity:
                        bid_quantity_by_price_map[price] = quantity
                    else:
                        bid_quantity_by_price_map.pop(
                            price,
                            None,
                        )

                end_asks_total_quantity = 0
                end_asks_total_volume = 0
                max_end_ask_price: Decimal | None = None
                max_end_ask_quantity: Decimal | None = None
                max_end_ask_volume: Decimal | None = None
                min_end_ask_price: Decimal | None = None
                min_end_ask_quantity: Decimal | None = None
                min_end_ask_volume: Decimal | None = None

                for ask_price, ask_quantity in ask_quantity_by_price_map.items():
                    if max_end_ask_price is None or ask_price > max_end_ask_price:
                        max_end_ask_price = ask_price

                    if min_end_ask_price is None or ask_price < min_end_ask_price:
                        min_end_ask_price = ask_price

                    end_asks_total_quantity += ask_quantity

                    if max_end_ask_quantity is None or ask_quantity > max_end_ask_quantity:
                        max_end_ask_quantity = ask_quantity

                    if min_end_ask_quantity is None or ask_quantity < min_end_ask_quantity:
                        min_end_ask_quantity = ask_quantity

                    ask_volume = (
                        ask_price
                        * ask_quantity
                    )

                    end_asks_total_volume += ask_volume

                    if max_end_ask_volume is None or ask_volume > max_end_ask_volume:
                        max_end_ask_volume = ask_volume

                    if min_end_ask_volume is None or ask_volume < min_end_ask_volume:
                        min_end_ask_volume = ask_volume

                assert max_end_ask_price is not None, None
                assert max_end_ask_quantity is not None, None
                assert max_end_ask_volume is not None, None
                assert min_end_ask_price is not None, None
                assert min_end_ask_quantity is not None, None
                assert min_end_ask_volume is not None, None

                end_bids_total_quantity = 0
                end_bids_total_volume = 0
                max_end_bid_price: Decimal | None = None
                max_end_bid_quantity: Decimal | None = None
                max_end_bid_volume: Decimal | None = None
                min_end_bid_price: Decimal | None = None
                min_end_bid_quantity: Decimal | None = None
                min_end_bid_volume: Decimal | None = None

                for bid_price, bid_quantity in bid_quantity_by_price_map.items():
                    if max_end_bid_price is None or bid_price > max_end_bid_price:
                        max_end_bid_price = bid_price

                    if min_end_bid_price is None or bid_price < min_end_bid_price:
                        min_end_bid_price = bid_price

                    end_bids_total_quantity += bid_quantity

                    if max_end_bid_quantity is None or bid_quantity > max_end_bid_quantity:
                        max_end_bid_quantity = bid_quantity

                    if min_end_bid_quantity is None or bid_quantity < min_end_bid_quantity:
                        min_end_bid_quantity = bid_quantity

                    bid_volume = (
                        bid_price
                        * bid_quantity
                    )

                    end_bids_total_volume += bid_volume

                    if max_end_bid_volume is None or bid_volume > max_end_bid_volume:
                        max_end_bid_volume = bid_volume

                    if min_end_bid_volume is None or bid_volume < min_end_bid_volume:
                        min_end_bid_volume = bid_volume

                assert max_end_bid_price is not None, None
                assert max_end_bid_quantity is not None, None
                assert max_end_bid_volume is not None, None
                assert min_end_bid_price is not None, None
                assert min_end_bid_quantity is not None, None
                assert min_end_bid_volume is not None, None

                final_data_set_record_data = (
                    main.save_final_data_set.schemas.OKXDataSetRecordData(
                        # Primary key fields
                        symbol_id=symbol_id,
                        data_set_idx=new_final_data_set_idx,
                        record_idx=record_idx,
                        # Attribute fields
                        buy_quantity=buy_quantity,
                        buy_trades_count=buy_trades_count,
                        buy_volume=buy_volume,
                        close_price_delta=close_price_delta,
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
                        end_timestamp_ms=next_order_book_timestamp_ms,
                        end_trade_id=end_trade_id,
                        high_price_delta=high_price_delta,
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
                        low_price_delta=low_price_delta,
                        open_price=open_price,
                        start_timestamp_ms=current_order_book_timestamp_ms,
                        start_trade_id=start_trade_id,
                        total_quantity=total_quantity,
                        total_trades_count=total_trades_count,
                        total_volume=total_volume,
                    )
                )

                session.add(
                    final_data_set_record_data,
                )


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
