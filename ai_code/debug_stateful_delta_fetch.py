"""Debug stateful incremental fetch vs checkpoint tail (RFC 160)."""

from __future__ import annotations

import sys

from main.offline_inference.trading_bot_imports import ensure_trading_bot_on_path

ensure_trading_bot_on_path()

import polars as pl

from enumerations import SymbolId
from main.save_final_data_set_3.schemas import OKXDataSetRecordData_3
from main.web_gui.data_service import fetch_last_bars_sync, _db_uri
from trading_bot_dataset.src.stateful_inference_common import classify_x1_delta_action


def _inspect_since(since_start_trade_id: int, limit: int) -> None:
    symbol = SymbolId.BTC_USDT
    query = f"""
    SELECT
        start_trade_id,
        end_trade_id,
        start_timestamp_ms,
        end_timestamp_ms,
        close_price
    FROM {OKXDataSetRecordData_3.__tablename__}
    WHERE symbol_id = '{symbol.name}'
      AND start_trade_id >= {since_start_trade_id}
    ORDER BY start_trade_id ASC
    LIMIT {limit}
    """
    df = pl.read_database_uri(engine='connectorx', query=query, uri=_db_uri())
    print(f'\n=== since_start_trade_id={since_start_trade_id} limit={limit} rows={df.height} ===')
    print(df)
    return df


def main() -> None:
    since_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1041519501
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10_000

    _inspect_since(since_id, limit)

    delta = fetch_last_bars_sync(
        symbol_id=SymbolId.BTC_USDT,
        limit=limit,
        offset=0,
        since_start_trade_id=since_id,
    )
    if delta is None:
        print('fetch_last_bars_sync returned None')
        return

    print(f'\n=== fetch_last_bars_sync rows={delta.height} ===')
    preview_cols = [
        'start_trade_id',
        'end_trade_id',
        'start_timestamp_ms',
        'end_timestamp_ms',
        'close_price',
    ]
    print(delta.select(preview_cols))

    last_end_timestamp_ms = int(delta['end_timestamp_ms'][-1])
    if delta.height > 0:
        checkpoint_last = since_id
        try:
            action = classify_x1_delta_action(
                delta=delta,
                last_start_trade_id=checkpoint_last,
                last_end_timestamp_ms=last_end_timestamp_ms,
            )
            print(f'\nclassify_x1_delta_action(checkpoint_last={checkpoint_last}) -> {action!r}')
        except Exception as exception:
            print(f'\nclassify_x1_delta_action FAILED: {exception!r}')

    import trading_bot_dataset.src.stateful_inference_common as mod

    print(f'\nstateful_inference_common path: {mod.__file__}')


if __name__ == '__main__':
    main()
