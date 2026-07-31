"""Trade research dataset helpers (forward-target padding for train-mode export)."""

from __future__ import annotations

import logging

import polars

logger = logging.getLogger(__name__)

TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS = 65536
TRADE_RESEARCH_FORWARD_TARGET_PADDING_SITE = 'dataset_level0'
TRADE_RESEARCH_EXPORT_FORWARD_TARGET_PADDING_BARS = 0
TRADE_RESEARCH_PAYLOAD_MODE_MIXED = 'mixed'


def pnl_max_sample_index(dataset_length: int, horizon_steps: int) -> int:
    return dataset_length - 1 - horizon_steps


def sample_indices_for_display_grid(
    dataset_length: int,
    step_bars: int,
) -> tuple[list[int], str | None]:
    max_display_sample_index = dataset_length - 1
    if max_display_sample_index < 0:
        return [], 'dataset too short for display grid'

    sample_indices = list(range(0, max_display_sample_index + 1, step_bars))
    return sample_indices, None


def sample_indices_for_pnl_grid(
    dataset_length: int,
    step_bars: int,
    horizon_steps: int,
) -> tuple[list[int], str | None]:
    max_sample_index = pnl_max_sample_index(
        dataset_length=dataset_length,
        horizon_steps=horizon_steps,
    )
    if max_sample_index < 0:
        return [], 'dataset too short for eval horizon'

    sample_indices = list(range(0, max_sample_index + 1, step_bars))
    return sample_indices, None


def is_trade_research_entry_point_segment(
    sample_index: int,
    pnl_max_sample_index: int,
    start_index: int,
    horizon_steps: int,
    level0_height: int,
) -> bool:
    if sample_index > pnl_max_sample_index:
        return True
    exit_bar_index = start_index + sample_index + horizon_steps
    if exit_bar_index >= level0_height:
        return True
    return False


def inference_provenance_fields(
    inference_x1_timestamp_ms: int,
    inference_entry_close: float,
) -> dict[str, float | int]:
    return {
        'inference_x1_timestamp_ms': int(inference_x1_timestamp_ms),
        'inference_entry_close': float(inference_entry_close),
    }


def inference_entry_close_for_row(
    row_index: int,
    entry_close_by_row: object,
    pred_start_price_by_row: object | None,
) -> float:
    if pred_start_price_by_row is not None:
        return float(pred_start_price_by_row[row_index])
    return float(entry_close_by_row[row_index])


def last_grid_inference_provenance_summary(
    sample_index: int,
    inference_x1_timestamp_ms: int,
    inference_entry_close: float,
    segment_kind: str | None,
    action: str | None,
) -> dict[str, object]:
    summary: dict[str, object] = {
        'sample_index': int(sample_index),
        'inference_x1_timestamp_ms': int(inference_x1_timestamp_ms),
        'inference_entry_close': float(inference_entry_close),
    }
    if segment_kind is not None:
        summary['segment_kind'] = segment_kind
    if action is not None:
        summary['action'] = action
    return summary


def build_last_grid_inference_provenance(
    grid_sample_indices: list[int],
    segments_by_sample_index: dict[int, dict[str, object]],
    entry_timestamp_ms_by_row: object,
    entry_close_by_row: object,
    pred_start_price_by_row: object | None,
    row_for_sample: object,
) -> dict[str, object] | None:
    if len(grid_sample_indices) == 0:
        return None
    last_sample_index = max(grid_sample_indices)
    row_index = row_for_sample(last_sample_index)
    if row_index is None:
        return None
    segment_kind: str | None = None
    action: str | None = None
    if last_sample_index in segments_by_sample_index:
        last_segment = segments_by_sample_index[last_sample_index]
        if 'segment_kind' in last_segment:
            segment_kind = str(last_segment['segment_kind'])
        if 'action' in last_segment:
            action = str(last_segment['action'])
    return last_grid_inference_provenance_summary(
        sample_index=last_sample_index,
        inference_x1_timestamp_ms=int(entry_timestamp_ms_by_row[row_index]),
        inference_entry_close=inference_entry_close_for_row(
            row_index=row_index,
            entry_close_by_row=entry_close_by_row,
            pred_start_price_by_row=pred_start_price_by_row,
        ),
        segment_kind=segment_kind,
        action=action,
    )


def inference_tail_grid_sample_indices(
    grid_sample_indices: list[int],
    train_sample_index_by_inference_sample: dict[int, int],
) -> list[int]:
    return [
        sample_index
        for sample_index in grid_sample_indices
        if sample_index not in train_sample_index_by_inference_sample
    ]


def inference_tail_pnl_sample_indices(
    pnl_sample_indices: list[int],
    train_sample_index_by_inference_sample: dict[int, int],
) -> list[int]:
    return [
        sample_index
        for sample_index in pnl_sample_indices
        if sample_index not in train_sample_index_by_inference_sample
    ]


def inference_tail_selection_note(
    unmapped_grid_count: int,
    unmapped_pnl_count: int,
) -> str | None:
    if unmapped_grid_count <= 0 and unmapped_pnl_count <= 0:
        return None
    return (
        f'inference tail {unmapped_grid_count} grid + {unmapped_pnl_count} pnl samples '
        '(train_sample_index=-1)'
    )


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

    padding_rows: list[dict[str, object]] = []
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
                'start_trade_id': bar_start_trade_id,
                'end_trade_id': bar_end_trade_id,
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
    padding_df = padding_df.select(
        [
            polars.col(column_name).cast(raw_df.schema[column_name])
            for column_name in raw_df.columns
        ],
    )
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
    forward_bars: int,
) -> tuple[polars.DataFrame, int]:
    return append_forward_target_padding(
        raw_df=raw_df,
        forward_bars=forward_bars,
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


def last_real_sample_index(
    dataset_length: int,
    start_index: int,
    level0_to_raw: list[int],
    real_bar_count: int,
) -> int:
    for sample_index in range(dataset_length - 1, -1, -1):
        entry_bar_index = start_index + sample_index
        raw_entry_row = level0_to_raw[entry_bar_index]
        if raw_entry_row < real_bar_count:
            return sample_index
    raise RuntimeError('No dataset sample maps to a real x1 bar')
