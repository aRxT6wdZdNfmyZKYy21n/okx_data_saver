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


_IS_RESCUE_MODE_ENABLED = False

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
    logger.info(
        f'Saving final data set for symbol with ID {symbol_id.name}'
    )

    final_data_set_record_db_schema = (
        main.save_final_data_set.schemas.OKXDataSetRecordData
    )

    postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

    async with (
        postgres_db_session_maker() as session_read_1,
        postgres_db_session_maker() as session_read_2,
        postgres_db_session_maker() as session_read_3,
        postgres_db_session_maker() as session_write,
    ):
        # Step 1: get last final data set record data

        async with session_read_1.begin():
            result = await session_read_1.execute(
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

        last_final_data_set_record_end_timestamp_ms: int | None
        new_final_data_set_idx: int

        if last_final_data_set_record_data is not None:
            last_final_data_set_record_end_timestamp_ms = last_final_data_set_record_data.end_timestamp_ms
            new_final_data_set_idx = last_final_data_set_record_data.data_set_idx

            if not _IS_RESCUE_MODE_ENABLED:
                new_final_data_set_idx += 1
        else:
            last_final_data_set_record_end_timestamp_ms = None
            new_final_data_set_idx = 0

        logger.info(
            'last_final_data_set_record_end_timestamp_ms'
            f': {last_final_data_set_record_end_timestamp_ms}'
        )

        # Step 2: get start OKXOrderBookData2 Snapshot

        order_book_data_db_schema = (
            main.save_order_books.schemas.OKXOrderBookData2
        )

        async with session_read_1.begin():
            order_by_expressions = [
                order_book_data_db_schema.symbol_id.asc(),
            ]

            if last_final_data_set_record_end_timestamp_ms is not None:
                if _IS_RESCUE_MODE_ENABLED:
                    order_by_expressions.append(
                        order_book_data_db_schema.timestamp_ms.desc(),
                    )

                    where_and_expression = and_(
                        order_book_data_db_schema.symbol_id == symbol_id,
                        order_book_data_db_schema.action_id == OKXOrderBookActionId.Snapshot,
                        order_book_data_db_schema.timestamp_ms < last_final_data_set_record_end_timestamp_ms,
                    )
                else:
                    order_by_expressions.append(
                        order_book_data_db_schema.timestamp_ms.asc(),
                    )

                    where_and_expression = and_(
                        order_book_data_db_schema.symbol_id == symbol_id,
                        order_book_data_db_schema.action_id == OKXOrderBookActionId.Snapshot,
                        order_book_data_db_schema.timestamp_ms >= last_final_data_set_record_end_timestamp_ms,
                    )
            else:
                order_by_expressions.append(
                    order_book_data_db_schema.timestamp_ms.asc(),
                )

                where_and_expression = and_(
                    order_book_data_db_schema.symbol_id == symbol_id,
                    order_book_data_db_schema.action_id == OKXOrderBookActionId.Snapshot,
                    order_book_data_db_schema.timestamp_ms >= 0,
                )

            result = await session_read_1.execute(
                select(
                    order_book_data_db_schema,
                ).where(
                    where_and_expression,
                ).order_by(
                    *order_by_expressions,
                ).limit(
                    1,
                ),
            )

            start_order_book_snapshot_data: (
                main.save_order_books.schemas.OKXOrderBookData2 | None
            ) = result.scalar_one_or_none()

        if start_order_book_snapshot_data is None:
            logger.info(
                f'There are no order book snapshots'
                '; skipping final data set saving.'
            )

            return

        start_order_book_snapshot_timestamp_ms = (
            start_order_book_snapshot_data.timestamp_ms
        )

        logger.info(
            'Start order book snapshot timestamp (ms)'
            f': {start_order_book_snapshot_timestamp_ms}'
        )

        # Step 3: get end OKXOrderBookData2 Snapshot (if exists)

        async with session_read_1.begin():
            result = await session_read_1.execute(
                select(
                    order_book_data_db_schema,
                ).where(
                    and_(
                        order_book_data_db_schema.symbol_id == symbol_id,
                        order_book_data_db_schema.action_id == OKXOrderBookActionId.Snapshot,
                        order_book_data_db_schema.timestamp_ms > start_order_book_snapshot_timestamp_ms,
                    ),
                ).order_by(
                    order_book_data_db_schema.symbol_id.asc(),
                    order_book_data_db_schema.timestamp_ms.asc(),
                ).limit(
                    1,
                ),
            )

            end_order_book_snapshot_data: (
                main.save_order_books.schemas.OKXOrderBookData2 | None
            ) = result.scalar_one_or_none()

        end_order_book_snapshot_timestamp_ms: int | None

        if end_order_book_snapshot_data is not None:
            end_order_book_snapshot_timestamp_ms = end_order_book_snapshot_data.timestamp_ms
        else:
            end_order_book_snapshot_timestamp_ms = None

        logger.info(
            'End order book snapshot timestamp (ms)'
            f': {end_order_book_snapshot_timestamp_ms}'
        )

        # Step 4:
        # - Fetch all order books after start snapshot

        ask_quantity_by_price_map: dict[Decimal, Decimal] | None = None
        bid_quantity_by_price_map: dict[Decimal, Decimal] | None = None

        current_order_book_data: (
            main.save_order_books.schemas.OKXOrderBookData2 | None
        ) = start_order_book_snapshot_data

        data_set_record_data_db_schema = (
            main.save_final_data_set.schemas.OKXDataSetRecordData
        )

        next_order_book_data: (
            main.save_order_books.schemas.OKXOrderBookData2 | None
        ) = None

        record_idx = 0

        trade_data_db_schema = (
            main.save_trades.schemas.OKXTradeData2
        )

        async with (
            session_read_1.begin(),
            session_read_3.begin(),
        ):
            if end_order_book_snapshot_timestamp_ms is not None:
                where_and_expression_1 = and_(
                    order_book_data_db_schema.symbol_id == symbol_id,
                    order_book_data_db_schema.action_id == OKXOrderBookActionId.Update,
                    order_book_data_db_schema.timestamp_ms > start_order_book_snapshot_timestamp_ms,
                    order_book_data_db_schema.timestamp_ms < end_order_book_snapshot_timestamp_ms,
                )

                where_and_expression_2 = and_(
                    trade_data_db_schema.symbol_id == symbol_id,
                    trade_data_db_schema.timestamp_ms >= start_order_book_snapshot_timestamp_ms,
                    trade_data_db_schema.timestamp_ms < end_order_book_snapshot_timestamp_ms,
                )
            else:
                return  # Not supported for now

                # where_and_expression_1 = and_(
                #     order_book_data_db_schema.symbol_id == symbol_id,
                #     order_book_data_db_schema.timestamp_ms > start_order_book_snapshot_timestamp_ms,
                # )

                # where_and_expression_2 = and_(
                #     trade_data_db_schema.symbol_id == symbol_id,
                #     trade_data_db_schema.timestamp_ms >= start_order_book_snapshot_timestamp_ms,
                # )

            # Fetch all trades between two snapshots

            async with session_read_2.begin():
                result_2 = await session_read_2.execute(
                    select(
                        trade_data_db_schema,
                    ).where(
                        where_and_expression_2,
                    ).order_by(
                        trade_data_db_schema.symbol_id.asc(),
                        trade_data_db_schema.trade_id.asc(),
                    )
                )

                trades: list[main.save_trades.schemas.OKXTradeData2] = result_2.scalars().all()

            trades_count = len(
                trades,
            )

            logger.info(
                f'Fetched {trades_count} trades',
            )

            if not trades_count:
                raise NotImplementedError

            order_book_data: (
                main.save_order_books.schemas.OKXOrderBookData2 | None
            )

            order_book_idx = 0

            result_1 = await session_read_1.stream(
                select(
                    order_book_data_db_schema,
                ).where(
                    where_and_expression_1,
                ).order_by(
                    order_book_data_db_schema.symbol_id.asc(),
                    order_book_data_db_schema.timestamp_ms.asc(),
                ).execution_options(
                    yield_per=1000,
                ),
            )

            start_trade_idx = 0  # For optimization purposes

            async for order_book_data in result_1.scalars():
                order_book_idx += 1

                if order_book_idx % 1000 == 0:
                    logger.info(
                        f'Processed {order_book_idx} order books',
                    )

                if current_order_book_data is None:
                    current_order_book_data = order_book_data

                    continue
                elif next_order_book_data is None:
                    next_order_book_data = order_book_data

                start_timestamp_ms = current_order_book_data.timestamp_ms
                end_timestamp_ms = next_order_book_data.timestamp_ms

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

                        try:
                            price = Decimal(
                                price_raw,
                            )
                        except Exception as exception:
                            logger.error(
                                f'Could not convert ask price {price_raw!r} to decimal'
                                f' (timestamp_ms: {current_order_book_data.timestamp_ms})'
                                ': handled exception'
                                f': {"".join(traceback.format_exception(exception))}',
                            )

                            continue

                        try:
                            quantity = Decimal(
                                quantity_raw,
                            )
                        except Exception as exception:
                            logger.error(
                                f'Could not convert ask quantity {quantity_raw!r} to decimal'
                                f' (timestamp_ms: {current_order_book_data.timestamp_ms})'
                                ': handled exception'
                                f': {"".join(traceback.format_exception(exception))}',
                            )

                            continue

                        assert quantity, (
                            start_timestamp_ms,
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

                        try:
                            price = Decimal(
                                price_raw,
                            )
                        except Exception as exception:
                            logger.error(
                                f'Could not convert bid price {price_raw!r} to decimal'
                                f' (timestamp_ms: {current_order_book_data.timestamp_ms})'
                                ': handled exception'
                                f': {"".join(traceback.format_exception(exception))}',
                            )

                            continue

                        try:
                            quantity = Decimal(
                                quantity_raw,
                            )
                        except Exception as exception:
                            logger.error(
                                f'Could not convert bid quantity {quantity_raw!r} to decimal'
                                f' (timestamp_ms: {current_order_book_data.timestamp_ms})'
                                ': handled exception'
                                f': {"".join(traceback.format_exception(exception))}',
                            )

                            continue

                        assert quantity, (
                            start_timestamp_ms,
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

                if not start_asks_total_volume:
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

                assert start_asks_total_quantity, None
                assert start_asks_total_volume, None
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

                if not start_bids_total_volume:
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

                assert start_bids_total_quantity, None
                assert start_bids_total_volume, None
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

                    try:
                        price = Decimal(
                            price_raw,
                        )
                    except Exception as exception:
                        logger.error(
                            f'Could not convert ask price {price_raw!r} to decimal'
                            f' (timestamp_ms: {next_order_book_data.timestamp_ms})'
                            ': handled exception'
                            f': {"".join(traceback.format_exception(exception))}',
                        )

                        continue

                    try:
                        quantity = Decimal(
                            quantity_raw,
                        )
                    except Exception as exception:
                        logger.error(
                            f'Could not convert ask quantity {quantity_raw!r} to decimal'
                            f' (timestamp_ms: {next_order_book_data.timestamp_ms})'
                            ': handled exception'
                            f': {"".join(traceback.format_exception(exception))}',
                        )

                        continue

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

                    try:
                        price = Decimal(
                            price_raw,
                        )
                    except Exception as exception:
                        logger.error(
                            f'Could not convert bid price {price_raw!r} to decimal'
                            f' (timestamp_ms: {next_order_book_data.timestamp_ms})'
                            ': handled exception'
                            f': {"".join(traceback.format_exception(exception))}',
                        )

                        continue

                    try:
                        quantity = Decimal(
                            quantity_raw,
                        )
                    except Exception as exception:
                        logger.error(
                            f'Could not convert bid quantity {quantity_raw!r} to decimal'
                            f' (timestamp_ms: {next_order_book_data.timestamp_ms})'
                            ': handled exception'
                            f': {"".join(traceback.format_exception(exception))}',
                        )

                        continue

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

                assert end_asks_total_quantity, None
                assert end_asks_total_volume, None
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

                assert end_bids_total_quantity, None
                assert end_bids_total_volume, None
                assert max_end_bid_price is not None, None
                assert max_end_bid_quantity is not None, None
                assert max_end_bid_volume is not None, None
                assert min_end_bid_price is not None, None
                assert min_end_bid_quantity is not None, None
                assert min_end_bid_volume is not None, None

                current_order_book_data = next_order_book_data
                next_order_book_data = None

                if current_order_book_data.action_id == OKXOrderBookActionId.Snapshot:
                    new_final_data_set_idx += 1

                    ask_quantity_by_price_map = None
                    bid_quantity_by_price_map = None

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

                for trade_idx in range(
                        start_trade_idx,
                        trades_count
                ):
                    trade_data = trades[trade_idx]

                    trade_timestamp_ms = trade_data.timestamp_ms

                    if trade_timestamp_ms < start_timestamp_ms:
                        continue
                    elif trade_timestamp_ms >= end_timestamp_ms:
                        start_trade_idx = trade_idx

                        break

                    trade_id = trade_data.trade_id

                    if start_trade_id is None:
                        start_trade_id = trade_id

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

                result_3 = await session_read_3.execute(
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
        'Final data set records were saved!',
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
