"""
Online non-overlapping trade research @ eval horizon for Web GUI overlays.

Loads full x1 history (WEB_GUI_TRADE_RESEARCH_LIMIT) for tensor context, runs
batched inference on grid @ step_bars for line segments plus a denser stride
for sequential hybrid backtest PnL (capital-constrained re-entry after exit).
"""

from __future__ import annotations

import logging
import math
import traceback

import httpx
import polars
from fastapi import HTTPException

from enumerations import SymbolId
from main.web_gui.data_service import fetch_last_bars_sync
from main.web_gui.inference_service import (
    _build_dataset,
    _build_level0_to_raw_row_indices,
    _build_train_level0_context,
    _encode_payload,
    _prepare_payload_dict_from_train_sample,
    _train_sample_index_for_inference_sample,
    fetch_inference_metadata,
)
from settings import settings

logger = logging.getLogger(__name__)

DEFAULT_EVAL_HORIZON = 'x2048'
DEFAULT_STEP_BARS = 2048
BATCH_CHUNK_SIZE = 32
BATCH_HTTP_TIMEOUT_SEC = 600.0
# OKX USDT-margined perpetual, regular tier (taker 0.05% per side) — как в trading_bot backtest
OKX_ROUND_TRIP_TAKER_FEE_RATE = 0.0005 * 2.0


def horizon_steps_from_name(horizon_name: str) -> int:
    if not horizon_name.startswith('x'):
        raise ValueError(f'Invalid horizon name: {horizon_name!r}')
    return int(horizon_name[1:])


def _prediction_key_for_horizon(horizon_name: str) -> str:
    return f'target_close_return_signed_log2_{horizon_name}'


def _sample_indices_for_full_dataset(
    dataset_length: int,
    step_bars: int,
    horizon_steps: int,
) -> tuple[list[int], str | None]:
    max_sample_index = dataset_length - 1 - horizon_steps
    if max_sample_index < 0:
        return [], 'dataset too short for eval horizon'

    sample_indices = list(range(0, max_sample_index + 1, step_bars))
    return sample_indices, None


def _segment_visible_by_start_trade_id(
    entry_start_trade_id: int,
    visible_min_start_trade_id: int | None,
    visible_max_start_trade_id: int | None,
) -> bool:
    if visible_min_start_trade_id is not None:
        if entry_start_trade_id < visible_min_start_trade_id:
            return False
    if visible_max_start_trade_id is not None:
        if entry_start_trade_id > visible_max_start_trade_id:
            return False
    return True


def _call_inference_batch_api(
    samples: list[dict[str, object]],
    symbol_id: str,
) -> list[dict[str, object]]:
    if len(samples) == 0:
        return []
    encoded_payload = _encode_payload({'samples': samples})
    response = httpx.post(
        f'{settings.WEB_GUI_INFERENCE_API_BASE_URL}/inference/batch',
        content=encoded_payload,
        params={'symbol': symbol_id},
        headers={'Content-Type': 'application/octet-stream'},
        timeout=BATCH_HTTP_TIMEOUT_SEC,
    )
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail=response.text)
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    payload = response.json()
    if 'results' not in payload:
        raise RuntimeError('Batch inference response missing results')
    results = payload['results']
    if not isinstance(results, list):
        raise RuntimeError('Batch inference results must be a list')
    return results


def _recommended_entry_action(
    inference_result: dict[str, object],
) -> str | None:
    if 'entry_hint' not in inference_result:
        return None
    entry_hint = inference_result['entry_hint']
    if not isinstance(entry_hint, dict):
        return None
    if 'recommended_action' not in entry_hint:
        return None
    recommended_action = str(entry_hint['recommended_action'])
    if recommended_action not in ('long', 'short'):
        return None
    return recommended_action


def _pred_target_price(
    entry_price: float,
    pred_eval_log2: float,
) -> float:
    return float(entry_price * math.pow(2.0, pred_eval_log2))


def _realized_linear_return_train_aligned(
    inference_entry_bar_index: int,
    horizon_steps: int,
    inference_level0_to_raw: list[int],
    train_level0_df: polars.DataFrame,
    raw_to_train_level0_row: dict[int, int],
) -> float | None:
    raw_entry_row = inference_level0_to_raw[inference_entry_bar_index]
    if raw_entry_row not in raw_to_train_level0_row:
        return None
    train_entry_row = raw_to_train_level0_row[raw_entry_row]
    train_exit_row = train_entry_row + horizon_steps
    if train_exit_row >= int(train_level0_df.height):
        return None
    entry_log2 = float(train_level0_df['close_price_log2'][train_entry_row])
    exit_log2 = float(train_level0_df['close_price_log2'][train_exit_row])
    return math.pow(2.0, exit_log2 - entry_log2) - 1.0


def _direction_action_from_inference(
    inference_result: dict[str, object],
) -> str:
    policy = inference_result['policy']
    if not isinstance(policy, dict):
        raise RuntimeError('inference policy must be a dict')
    if 'probabilities' not in policy:
        raise RuntimeError('inference policy missing probabilities')
    probabilities = policy['probabilities']
    if not isinstance(probabilities, dict):
        raise RuntimeError('inference policy probabilities must be a dict')
    if 'long' not in probabilities:
        raise RuntimeError('inference policy probabilities missing long')
    if 'short' not in probabilities:
        raise RuntimeError('inference policy probabilities missing short')
    long_probability = float(probabilities['long'])
    short_probability = float(probabilities['short'])
    if long_probability >= short_probability:
        return 'long'
    return 'short'


def _hybrid_backtest_allows_entry(
    inference_result: dict[str, object],
) -> bool:
    if 'entry_hint' not in inference_result:
        return False
    entry_hint = inference_result['entry_hint']
    if not isinstance(entry_hint, dict):
        return False
    if 'hint_mode' not in entry_hint:
        return False
    hint_mode = str(entry_hint['hint_mode'])
    if hint_mode == 'snr_only':
        if 'snr_blocks_entry' not in entry_hint:
            return False
        return not bool(entry_hint['snr_blocks_entry'])
    if hint_mode == 'hybrid_gate_snr':
        if 'hybrid_blocks_entry' not in entry_hint:
            return False
        return not bool(entry_hint['hybrid_blocks_entry'])
    raise RuntimeError(f'Unknown entry_hint hint_mode: {hint_mode!r}')


def _trade_net_pnl_from_linear_return(
    action: str,
    realized_linear_return: float,
    round_trip_fee_rate: float,
) -> float:
    if action == 'long':
        return realized_linear_return - round_trip_fee_rate
    if action == 'short':
        return -realized_linear_return - round_trip_fee_rate
    raise RuntimeError(f'Unexpected action for PnL: {action!r}')


def _sample_indices_for_pnl_backtest(
    max_sample_index: int,
    stride: int,
) -> list[int]:
    if stride <= 0:
        raise ValueError(f'pnl stride must be positive: {stride}')
    return list(range(0, max_sample_index + 1, stride))


def _merge_sorted_sample_indices(
    first_indices: list[int],
    second_indices: list[int],
) -> list[int]:
    merged: list[int] = []
    seen: set[int] = set()
    for sample_index in first_indices + second_indices:
        if sample_index in seen:
            continue
        seen.add(sample_index)
        merged.append(sample_index)
    merged.sort()
    return merged


def _map_sample_indices_to_train(
    sample_indices: list[int],
    start_index: int,
    inference_level0_to_raw: list[int],
    raw_to_train_level0_row: dict[int, int],
    train_dataset_length: int,
) -> tuple[list[int], dict[int, int], int]:
    mapped_sample_indices: list[int] = []
    train_sample_index_by_inference_sample: dict[int, int] = {}
    skipped_count = 0
    for sample_index in sample_indices:
        train_sample_index = _train_sample_index_for_inference_sample(
            sample_index=sample_index,
            start_index=start_index,
            inference_level0_to_raw=inference_level0_to_raw,
            raw_to_train_level0_row=raw_to_train_level0_row,
        )
        if train_sample_index is None:
            skipped_count = skipped_count + 1
            continue
        if train_sample_index < 0 or train_sample_index >= train_dataset_length:
            skipped_count = skipped_count + 1
            continue
        mapped_sample_indices.append(sample_index)
        train_sample_index_by_inference_sample[sample_index] = train_sample_index
    return mapped_sample_indices, train_sample_index_by_inference_sample, skipped_count


def _run_inference_for_samples(
    sample_indices: list[int],
    train_sample_index_by_inference_sample: dict[int, int],
    train_dataset: object,
    symbol_id: str,
) -> dict[int, dict[str, object]]:
    inference_by_sample: dict[int, dict[str, object]] = {}
    if len(sample_indices) == 0:
        return inference_by_sample

    total_chunks = (len(sample_indices) + BATCH_CHUNK_SIZE - 1) // BATCH_CHUNK_SIZE
    for chunk_index, chunk_start in enumerate(
        range(0, len(sample_indices), BATCH_CHUNK_SIZE),
    ):
        chunk_sample_indices = sample_indices[
            chunk_start:chunk_start + BATCH_CHUNK_SIZE
        ]
        chunk_payloads = []
        for sample_index in chunk_sample_indices:
            train_sample_index = train_sample_index_by_inference_sample[sample_index]
            chunk_payloads.append(
                _prepare_payload_dict_from_train_sample(
                    train_dataset=train_dataset,
                    train_sample_index=train_sample_index,
                ),
            )
        chunk_results = _call_inference_batch_api(chunk_payloads, symbol_id)
        if len(chunk_results) != len(chunk_sample_indices):
            raise RuntimeError(
                'Batch inference result count mismatch: '
                f'{len(chunk_results)} != {len(chunk_sample_indices)}',
            )
        for sample_index, inference_result in zip(
            chunk_sample_indices,
            chunk_results,
            strict=True,
        ):
            inference_by_sample[sample_index] = inference_result
        if (chunk_index + 1) % 10 == 0 or (chunk_index + 1) == total_chunks:
            logger.info(
                'Trade research inference: %d/%d batches, %d/%d samples',
                chunk_index + 1,
                total_chunks,
                len(inference_by_sample),
                len(sample_indices),
            )
    return inference_by_sample


def _trade_pnl_for_sample(
    sample_index: int,
    inference_result: dict[str, object],
    horizon_steps: int,
    start_index: int,
    inference_level0_to_raw: list[int],
    train_level0_df: polars.DataFrame,
    raw_to_train_level0_row: dict[int, int],
) -> float | None:
    entry_bar_index = start_index + sample_index
    exit_bar_index = entry_bar_index + horizon_steps
    if exit_bar_index >= len(inference_level0_to_raw):
        return None

    realized_linear_return = _realized_linear_return_train_aligned(
        inference_entry_bar_index=entry_bar_index,
        horizon_steps=horizon_steps,
        inference_level0_to_raw=inference_level0_to_raw,
        train_level0_df=train_level0_df,
        raw_to_train_level0_row=raw_to_train_level0_row,
    )
    if realized_linear_return is None:
        return None

    backtest_direction = _direction_action_from_inference(inference_result)
    return _trade_net_pnl_from_linear_return(
        action=backtest_direction,
        realized_linear_return=realized_linear_return,
        round_trip_fee_rate=OKX_ROUND_TRIP_TAKER_FEE_RATE,
    )


def _next_cached_sample_index(
    sample_index: int,
    cached_sample_indices: list[int],
) -> int | None:
    left = 0
    right = len(cached_sample_indices)
    while left < right:
        middle = (left + right) // 2
        if cached_sample_indices[middle] < sample_index:
            left = middle + 1
        else:
            right = middle
    if left >= len(cached_sample_indices):
        return None
    return cached_sample_indices[left]


def _compute_grid_hybrid_backtest_sum(
    inference_by_sample: dict[int, dict[str, object]],
    grid_sample_indices: list[int],
    horizon_steps: int,
    start_index: int,
    inference_level0_to_raw: list[int],
    train_level0_df: polars.DataFrame,
    raw_to_train_level0_row: dict[int, int],
    visible_min_start_trade_id: int | None,
    visible_max_start_trade_id: int | None,
    raw_df: polars.DataFrame,
) -> tuple[float, int, float, int]:
    net_pnl_sum = 0.0
    trade_count = 0
    visible_net_pnl_sum = 0.0
    visible_trade_count = 0

    for sample_index in grid_sample_indices:
        if sample_index not in inference_by_sample:
            continue
        inference_result = inference_by_sample[sample_index]
        if not _hybrid_backtest_allows_entry(inference_result):
            continue

        trade_pnl = _trade_pnl_for_sample(
            sample_index=sample_index,
            inference_result=inference_result,
            horizon_steps=horizon_steps,
            start_index=start_index,
            inference_level0_to_raw=inference_level0_to_raw,
            train_level0_df=train_level0_df,
            raw_to_train_level0_row=raw_to_train_level0_row,
        )
        if trade_pnl is None:
            continue

        net_pnl_sum = net_pnl_sum + trade_pnl
        trade_count = trade_count + 1

        entry_bar_index = start_index + sample_index
        entry_raw_index = inference_level0_to_raw[entry_bar_index]
        entry_meta = _raw_bar_metadata(raw_df, entry_raw_index)
        entry_start_trade_id = int(entry_meta['start_trade_id'])
        if _segment_visible_by_start_trade_id(
            entry_start_trade_id=entry_start_trade_id,
            visible_min_start_trade_id=visible_min_start_trade_id,
            visible_max_start_trade_id=visible_max_start_trade_id,
        ):
            visible_net_pnl_sum = visible_net_pnl_sum + trade_pnl
            visible_trade_count = visible_trade_count + 1

    return net_pnl_sum, trade_count, visible_net_pnl_sum, visible_trade_count


def _compute_sequential_hybrid_backtest(
    inference_by_sample: dict[int, dict[str, object]],
    pnl_sample_indices: list[int],
    max_sample_index: int,
    horizon_steps: int,
    start_index: int,
    inference_level0_to_raw: list[int],
    train_level0_df: polars.DataFrame,
    raw_to_train_level0_row: dict[int, int],
    visible_min_start_trade_id: int | None,
    visible_max_start_trade_id: int | None,
    raw_df: polars.DataFrame,
) -> tuple[float, int, float, int]:
    cached_sample_indices = sorted(
        sample_index
        for sample_index in pnl_sample_indices
        if sample_index in inference_by_sample
    )

    net_pnl_sum = 0.0
    trade_count = 0
    visible_net_pnl_sum = 0.0
    visible_trade_count = 0

    sample_index = 0
    while sample_index <= max_sample_index:
        if sample_index not in inference_by_sample:
            next_cached = _next_cached_sample_index(
                sample_index=sample_index,
                cached_sample_indices=cached_sample_indices,
            )
            if next_cached is None:
                break
            sample_index = next_cached

        inference_result = inference_by_sample[sample_index]
        if not _hybrid_backtest_allows_entry(inference_result):
            sample_index = sample_index + 1
            continue

        if sample_index + horizon_steps > max_sample_index:
            break

        trade_pnl = _trade_pnl_for_sample(
            sample_index=sample_index,
            inference_result=inference_result,
            horizon_steps=horizon_steps,
            start_index=start_index,
            inference_level0_to_raw=inference_level0_to_raw,
            train_level0_df=train_level0_df,
            raw_to_train_level0_row=raw_to_train_level0_row,
        )
        if trade_pnl is None:
            sample_index = sample_index + 1
            continue

        net_pnl_sum = net_pnl_sum + trade_pnl
        trade_count = trade_count + 1

        entry_bar_index = start_index + sample_index
        entry_raw_index = inference_level0_to_raw[entry_bar_index]
        entry_meta = _raw_bar_metadata(raw_df, entry_raw_index)
        entry_start_trade_id = int(entry_meta['start_trade_id'])
        if _segment_visible_by_start_trade_id(
            entry_start_trade_id=entry_start_trade_id,
            visible_min_start_trade_id=visible_min_start_trade_id,
            visible_max_start_trade_id=visible_max_start_trade_id,
        ):
            visible_net_pnl_sum = visible_net_pnl_sum + trade_pnl
            visible_trade_count = visible_trade_count + 1

        sample_index = sample_index + horizon_steps

    return net_pnl_sum, trade_count, visible_net_pnl_sum, visible_trade_count


def _row_value(row: dict[str, object], column_name: str) -> object:
    if column_name not in row:
        raise RuntimeError(f'Dataframe row missing column {column_name!r}')
    return row[column_name]


def _raw_bar_metadata(
    raw_df: polars.DataFrame,
    raw_row_index: int,
) -> dict[str, float | int]:
    row = raw_df.row(raw_row_index, named=True)
    return {
        'start_trade_id': int(_row_value(row, 'start_trade_id')),
        'start_timestamp_ms': int(_row_value(row, 'start_timestamp_ms')),
        'open_price': float(_row_value(row, 'open_price')),
        'close_price': float(_row_value(row, 'close_price')),
    }


def run_trade_research(
    symbol_id: str,
    eval_horizon: str,
    step_bars: int,
    visible_min_start_trade_id: int | None,
    visible_max_start_trade_id: int | None,
) -> dict[str, object]:
    if not settings.WEB_GUI_INFERENCE_ENABLED:
        raise HTTPException(status_code=503, detail='Inference is disabled')

    horizon_steps = horizon_steps_from_name(eval_horizon)
    if step_bars != horizon_steps:
        raise HTTPException(
            status_code=422,
            detail=(
                f'Non-overlapping research requires step_bars={horizon_steps} '
                f'for eval_horizon={eval_horizon}, got {step_bars}'
            ),
        )

    symbol = SymbolId[symbol_id]
    research_limit = settings.WEB_GUI_TRADE_RESEARCH_LIMIT

    metadata = fetch_inference_metadata()
    required_rows = int(metadata['sequence_length']) * int(metadata['max_scale'])
    minimum_rows = required_rows + horizon_steps
    if research_limit < minimum_rows:
        raise HTTPException(
            status_code=422,
            detail=(
                'Trade research limit is below minimum x1 bars '
                f'(minimum {minimum_rows}, configured {research_limit})'
            ),
        )

    df = fetch_last_bars_sync(symbol_id=symbol, limit=research_limit, offset=0)
    if df is None:
        raise HTTPException(status_code=422, detail='Недостаточно данных для trade research')
    if df.height < minimum_rows:
        raise HTTPException(
            status_code=422,
            detail=(
                'Trade research: fetched fewer x1 bars than required '
                f'({df.height} < {minimum_rows})'
            ),
        )

    dataset = _build_dataset(df, metadata)
    train_dataset, train_level0_df, raw_to_train_level0_row = _build_train_level0_context(
        df=df,
        metadata=metadata,
    )
    start_index = int(dataset.dataset.start_index)
    dataset_length = len(dataset)
    level0_df = dataset.dataset.aggregated_data[0]
    level0_height = int(level0_df.height)
    level0_to_raw_row_indices = _build_level0_to_raw_row_indices(df, level0_df)

    max_sample_index = dataset_length - 1 - horizon_steps
    if max_sample_index < 0:
        raise HTTPException(
            status_code=422,
            detail='Trade research: dataset too short for one full horizon segment',
        )

    sample_indices, sample_selection_note = _sample_indices_for_full_dataset(
        dataset_length=dataset_length,
        step_bars=step_bars,
        horizon_steps=horizon_steps,
    )

    pnl_stride = settings.WEB_GUI_TRADE_RESEARCH_PNL_STRIDE
    pnl_sample_indices = _sample_indices_for_pnl_backtest(
        max_sample_index=max_sample_index,
        stride=pnl_stride,
    )
    inference_sample_indices = _merge_sorted_sample_indices(
        first_indices=sample_indices,
        second_indices=pnl_sample_indices,
    )

    mapped_inference_indices, train_sample_index_by_inference_sample, skipped_unmapped_samples = (
        _map_sample_indices_to_train(
            sample_indices=inference_sample_indices,
            start_index=start_index,
            inference_level0_to_raw=level0_to_raw_row_indices,
            raw_to_train_level0_row=raw_to_train_level0_row,
            train_dataset_length=len(train_dataset),
        )
    )
    mapped_grid_indices = [
        sample_index
        for sample_index in sample_indices
        if sample_index in train_sample_index_by_inference_sample
    ]
    mapped_pnl_indices = [
        sample_index
        for sample_index in pnl_sample_indices
        if sample_index in train_sample_index_by_inference_sample
    ]
    if skipped_unmapped_samples > 0:
        unmapped_note = (
            f'skipped {skipped_unmapped_samples} samples without train-mode alignment'
        )
        if sample_selection_note is None:
            sample_selection_note = unmapped_note
        else:
            sample_selection_note = f'{sample_selection_note}; {unmapped_note}'

    logger.info(
        'Trade research: symbol=%s grid_samples=%d pnl_samples=%d infer_samples=%d '
        'step=%d pnl_stride=%d horizon=%s research_limit=%d level0=%d raw_df=%d start=%d '
        'visible_trade_id=[%s,%s] note=%s',
        symbol_id,
        len(mapped_grid_indices),
        len(mapped_pnl_indices),
        len(mapped_inference_indices),
        step_bars,
        pnl_stride,
        eval_horizon,
        research_limit,
        level0_height,
        df.height,
        start_index,
        visible_min_start_trade_id,
        visible_max_start_trade_id,
        sample_selection_note,
    )

    segments: list[dict[str, object]] = []
    policy_trade_count = 0
    entry_allowed_count = 0
    grid_backtest_net_pnl_sum = 0.0
    grid_backtest_trade_count = 0
    grid_backtest_visible_net_pnl_sum = 0.0
    grid_backtest_visible_trade_count = 0
    sequential_backtest_net_pnl_sum = 0.0
    sequential_backtest_trade_count = 0
    sequential_backtest_visible_net_pnl_sum = 0.0
    sequential_backtest_visible_trade_count = 0

    if len(mapped_inference_indices) == 0:
        return {
            'symbol_id': symbol_id,
            'eval_horizon': eval_horizon,
            'step_bars': step_bars,
            'research_limit': research_limit,
            'pnl_stride': pnl_stride,
            'required_rows': required_rows,
            'bars_loaded': int(df.height),
            'level0_rows': level0_height,
            'dataset_length': dataset_length,
            'start_index': start_index,
            'visible_min_start_trade_id': visible_min_start_trade_id,
            'visible_max_start_trade_id': visible_max_start_trade_id,
            'sample_count': 0,
            'pnl_sample_count': 0,
            'trade_inference_count': 0,
            'entry_allowed_count': 0,
            'grid_backtest_net_pnl_sum': 0.0,
            'grid_backtest_trade_count': 0,
            'grid_backtest_visible_net_pnl_sum': 0.0,
            'grid_backtest_visible_trade_count': 0,
            'sequential_backtest_net_pnl_sum': 0.0,
            'sequential_backtest_trade_count': 0,
            'sequential_backtest_visible_net_pnl_sum': 0.0,
            'sequential_backtest_visible_trade_count': 0,
            'backtest_net_pnl_sum': 0.0,
            'backtest_trade_count': 0,
            'backtest_visible_net_pnl_sum': 0.0,
            'backtest_visible_trade_count': 0,
            'round_trip_fee_rate': OKX_ROUND_TRIP_TAKER_FEE_RATE,
            'segment_count': 0,
            'segments': segments,
            'sample_selection_note': sample_selection_note,
        }

    try:
        inference_by_sample = _run_inference_for_samples(
            sample_indices=mapped_inference_indices,
            train_sample_index_by_inference_sample=train_sample_index_by_inference_sample,
            train_dataset=train_dataset,
            symbol_id=symbol_id,
        )

        (
            grid_backtest_net_pnl_sum,
            grid_backtest_trade_count,
            grid_backtest_visible_net_pnl_sum,
            grid_backtest_visible_trade_count,
        ) = _compute_grid_hybrid_backtest_sum(
            inference_by_sample=inference_by_sample,
            grid_sample_indices=mapped_grid_indices,
            horizon_steps=horizon_steps,
            start_index=start_index,
            inference_level0_to_raw=level0_to_raw_row_indices,
            train_level0_df=train_level0_df,
            raw_to_train_level0_row=raw_to_train_level0_row,
            visible_min_start_trade_id=visible_min_start_trade_id,
            visible_max_start_trade_id=visible_max_start_trade_id,
            raw_df=df,
        )

        (
            sequential_backtest_net_pnl_sum,
            sequential_backtest_trade_count,
            sequential_backtest_visible_net_pnl_sum,
            sequential_backtest_visible_trade_count,
        ) = _compute_sequential_hybrid_backtest(
            inference_by_sample=inference_by_sample,
            pnl_sample_indices=mapped_pnl_indices,
            max_sample_index=max_sample_index,
            horizon_steps=horizon_steps,
            start_index=start_index,
            inference_level0_to_raw=level0_to_raw_row_indices,
            train_level0_df=train_level0_df,
            raw_to_train_level0_row=raw_to_train_level0_row,
            visible_min_start_trade_id=visible_min_start_trade_id,
            visible_max_start_trade_id=visible_max_start_trade_id,
            raw_df=df,
        )

        prediction_key = _prediction_key_for_horizon(eval_horizon)
        for sample_index in mapped_grid_indices:
            inference_result = inference_by_sample[sample_index]
            if 'policy' not in inference_result:
                continue
            policy = inference_result['policy']
            if 'action' not in policy:
                continue
            action = str(policy['action'])
            if action in ('long', 'short'):
                policy_trade_count = policy_trade_count + 1

            recommended_action = _recommended_entry_action(inference_result)
            if recommended_action is None:
                continue

            entry_allowed_count = entry_allowed_count + 1

            entry_bar_index = start_index + sample_index
            exit_bar_index = entry_bar_index + horizon_steps
            if exit_bar_index >= level0_height:
                continue

            entry_raw_index = level0_to_raw_row_indices[entry_bar_index]
            exit_raw_index = level0_to_raw_row_indices[exit_bar_index]
            entry_meta = _raw_bar_metadata(df, entry_raw_index)
            exit_meta = _raw_bar_metadata(df, exit_raw_index)
            entry_start_trade_id = int(entry_meta['start_trade_id'])

            segment_visible = _segment_visible_by_start_trade_id(
                entry_start_trade_id=entry_start_trade_id,
                visible_min_start_trade_id=visible_min_start_trade_id,
                visible_max_start_trade_id=visible_max_start_trade_id,
            )
            if not segment_visible:
                continue

            entry_close = float(entry_meta['close_price'])
            entry_open = float(entry_meta['open_price'])
            exit_close = float(exit_meta['close_price'])

            pred_eval_log2 = 0.0
            if 'predictions' in inference_result:
                predictions = inference_result['predictions']
                if prediction_key in predictions:
                    pred_eval_log2 = float(predictions[prediction_key])

            segments.append(
                {
                    'sample_index': int(sample_index),
                    'entry_bar_index': int(entry_bar_index),
                    'exit_bar_index': int(exit_bar_index),
                    'entry_start_trade_id': entry_start_trade_id,
                    'exit_start_trade_id': int(exit_meta['start_trade_id']),
                    'entry_timestamp_ms': int(entry_meta['start_timestamp_ms']),
                    'exit_timestamp_ms': int(exit_meta['start_timestamp_ms']),
                    'entry_open': entry_open,
                    'entry_close': entry_close,
                    'exit_close': exit_close,
                    'pred_target_open': _pred_target_price(
                        entry_price=entry_open,
                        pred_eval_log2=pred_eval_log2,
                    ),
                    'pred_target_close': _pred_target_price(
                        entry_price=entry_close,
                        pred_eval_log2=pred_eval_log2,
                    ),
                    'pred_eval_log2': pred_eval_log2,
                    'policy_action': action,
                    'action': recommended_action,
                },
            )
    except HTTPException:
        raise
    except Exception as exception:
        logger.error(
            'Trade research failed: %s',
            ''.join(traceback.format_exception(exception)),
        )
        raise HTTPException(status_code=500, detail='Trade research failed') from exception

    return {
        'symbol_id': symbol_id,
        'eval_horizon': eval_horizon,
        'step_bars': step_bars,
        'research_limit': research_limit,
        'pnl_stride': pnl_stride,
        'required_rows': required_rows,
        'bars_loaded': int(df.height),
        'level0_rows': level0_height,
        'dataset_length': dataset_length,
        'start_index': start_index,
        'visible_min_start_trade_id': visible_min_start_trade_id,
        'visible_max_start_trade_id': visible_max_start_trade_id,
        'sample_count': len(mapped_grid_indices),
        'pnl_sample_count': len(mapped_pnl_indices),
        'trade_inference_count': policy_trade_count,
        'entry_allowed_count': entry_allowed_count,
        'grid_backtest_net_pnl_sum': grid_backtest_net_pnl_sum,
        'grid_backtest_trade_count': grid_backtest_trade_count,
        'grid_backtest_visible_net_pnl_sum': grid_backtest_visible_net_pnl_sum,
        'grid_backtest_visible_trade_count': grid_backtest_visible_trade_count,
        'sequential_backtest_net_pnl_sum': sequential_backtest_net_pnl_sum,
        'sequential_backtest_trade_count': sequential_backtest_trade_count,
        'sequential_backtest_visible_net_pnl_sum': sequential_backtest_visible_net_pnl_sum,
        'sequential_backtest_visible_trade_count': sequential_backtest_visible_trade_count,
        'backtest_net_pnl_sum': sequential_backtest_net_pnl_sum,
        'backtest_trade_count': sequential_backtest_trade_count,
        'backtest_visible_net_pnl_sum': sequential_backtest_visible_net_pnl_sum,
        'backtest_visible_trade_count': sequential_backtest_visible_trade_count,
        'round_trip_fee_rate': OKX_ROUND_TRIP_TAKER_FEE_RATE,
        'segment_count': len(segments),
        'segments': segments,
        'sample_selection_note': sample_selection_note,
    }
