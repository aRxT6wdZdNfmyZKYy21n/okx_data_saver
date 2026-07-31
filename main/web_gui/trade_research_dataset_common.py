"""Trade research dataset helpers (forward-target padding for train-mode export)."""

from __future__ import annotations

import logging

import polars

logger = logging.getLogger(__name__)

TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS = 65536


def append_forward_target_padding(
    raw_df: polars.DataFrame,
    forward_bars: int,
) -> tuple[polars.DataFrame, int]:
    real_bar_count = int(raw_df.height)
    if forward_bars <= 0:
        return raw_df, real_bar_count

    last_row = raw_df.row(real_bar_count - 1, named=True)
    start_timestamp_ms = float(last_row['start_timestamp_ms'])
    end_timestamp_ms = float(last_row['end_timestamp_ms'])
    bar_duration_ms = end_timestamp_ms - start_timestamp_ms
    if bar_duration_ms <= 0.0:
        bar_duration_ms = 60000.0

    last_start_trade_id = int(last_row['start_trade_id'])
    last_end_trade_id = int(last_row['end_trade_id'])
    trade_id_step = last_end_trade_id - last_start_trade_id
    if trade_id_step <= 0:
        trade_id_step = 1

    close_price = float(last_row['close_price'])
    symbol_id = last_row['symbol_id']

    padding_rows: list[dict[str, float | str]] = []
    previous_end_timestamp_ms = end_timestamp_ms
    previous_end_trade_id = last_end_trade_id

    for _padding_index in range(forward_bars):
        bar_start_timestamp_ms = previous_end_timestamp_ms
        bar_end_timestamp_ms = bar_start_timestamp_ms + bar_duration_ms
        bar_start_trade_id = previous_end_trade_id + 1
        bar_end_trade_id = bar_start_trade_id + trade_id_step
        padding_rows.append(
            {
                'symbol_id': symbol_id,
                'start_trade_id': float(bar_start_trade_id),
                'end_trade_id': float(bar_end_trade_id),
                'start_timestamp_ms': bar_start_timestamp_ms,
                'end_timestamp_ms': bar_end_timestamp_ms,
                'open_price': close_price,
                'high_price': close_price,
                'low_price': close_price,
                'close_price': close_price,
                'total_volume': 0.0,
                'buy_volume': 0.0,
                'total_quantity': 0.0,
                'buy_quantity': 0.0,
                'total_trades_count': 0.0,
                'buy_trades_count': 0.0,
            },
        )
        previous_end_timestamp_ms = bar_end_timestamp_ms
        previous_end_trade_id = bar_end_trade_id

    padding_df = polars.DataFrame(padding_rows)
    padded_df = polars.concat([raw_df, padding_df], how='vertical')
    logger.info(
        'Trade research forward-target padding: real_bars=%d padded_bars=%d total=%d',
        real_bar_count,
        forward_bars,
        int(padded_df.height),
    )
    return padded_df, real_bar_count


def prepare_trade_research_raw_dataframe(
    raw_df: polars.DataFrame,
) -> tuple[polars.DataFrame, int]:
    return append_forward_target_padding(
        raw_df=raw_df,
        forward_bars=TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS,
    )


def real_last_start_trade_id(raw_df: polars.DataFrame, real_bar_count: int) -> int:
    last_real_row = raw_df.row(real_bar_count - 1, named=True)
    return int(last_real_row['start_trade_id'])


def sample_exit_raw_index(
    sample_index: int,
    start_index: int,
    horizon_steps: int,
    level0_to_raw: list[int],
) -> int:
    exit_bar_index = start_index + sample_index + horizon_steps
    return int(level0_to_raw[exit_bar_index])


def sample_exit_on_real_bars(
    sample_index: int,
    start_index: int,
    horizon_steps: int,
    level0_to_raw: list[int],
    real_bar_count: int,
) -> bool:
    exit_raw_index = sample_exit_raw_index(
        sample_index=sample_index,
        start_index=start_index,
        horizon_steps=horizon_steps,
        level0_to_raw=level0_to_raw,
    )
    return exit_raw_index < real_bar_count
