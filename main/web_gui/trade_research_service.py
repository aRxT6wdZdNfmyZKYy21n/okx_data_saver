"""
Online non-overlapping trade research @ eval horizon for Web GUI overlays.

Loads full x1 history (WEB_GUI_TRADE_RESEARCH_LIMIT) for tensor context, runs
batched inference on grid @ step_bars for line segments plus a denser stride
for sequential hybrid backtest PnL (capital-constrained re-entry after exit).
"""

from __future__ import annotations

import json
import logging
import math
import traceback
from typing import Any

import httpx
import numpy as np
import polars
from fastapi import HTTPException

from enumerations import SymbolId
from main.web_gui.data_service import fetch_last_bars_sync
from main.web_gui.trade_research_dataset_common import (
    inference_provenance_fields,
    inference_tail_grid_sample_indices,
    inference_tail_pnl_sample_indices,
    inference_tail_selection_note,
    is_trade_research_entry_point_segment,
    last_grid_inference_provenance_summary,
    pnl_max_sample_index,
    real_last_start_trade_id,
    sample_exit_on_real_bars,
    sample_indices_for_display_grid,
    sample_indices_for_pnl_grid,
)
from main.web_gui.inference_service import (
    _build_dataset,
    _build_level0_to_raw_row_indices,
    _build_train_level0_context,
    _encode_payload,
    _prepare_payload_dict_from_sample,
    _prepare_payload_dict_from_train_sample,
    _train_sample_index_for_inference_sample,
    fetch_inference_metadata,
)
from settings import settings

logger = logging.getLogger(__name__)

DEFAULT_EVAL_HORIZON = 'x32'
DEFAULT_STEP_BARS = 1536
BATCH_CHUNK_SIZE = 32
BATCH_HTTP_TIMEOUT_SEC = 600.0
# OKX USDT-margined perpetual, regular tier (taker 0.05% per side) — как в trading_bot backtest
OKX_ROUND_TRIP_TAKER_FEE_RATE = 0.0005 * 2.0

TRADE_RESEARCH_NPZ_INFERENCE_ROW_KEYS = [
    'policy_action',
    'policy_prob_hold',
    'policy_prob_long',
    'policy_prob_short',
    'entry_hint_json',
]

TRADE_RESEARCH_NPZ_PRICE_ROW_KEYS = [
    'pred_start_price',
    'pred_target_price',
]


def horizon_steps_from_name(horizon_name: str) -> int:
    if not horizon_name.startswith('x'):
        raise ValueError(f'Invalid horizon name: {horizon_name!r}')
    return int(horizon_name[1:])


def inference_stack_fingerprint(
    metadata: dict[str, object],
    symbol_id: str,
) -> dict[str, str]:
    checkpoint_path_by_symbol = metadata['checkpoint_path_by_symbol']
    if symbol_id not in checkpoint_path_by_symbol:
        raise RuntimeError(f'Metadata missing checkpoint for {symbol_id!r}')

    trade_research_stack_by_symbol = metadata['trade_research_stack_by_symbol']
    if symbol_id not in trade_research_stack_by_symbol:
        raise RuntimeError(
            f'Metadata missing trade_research_stack for {symbol_id!r}',
        )
    stack_entry = trade_research_stack_by_symbol[symbol_id]
    if not isinstance(stack_entry, dict):
        raise RuntimeError(
            f'Invalid trade_research_stack metadata for {symbol_id!r}',
        )
    for key in ('run_label', 'eval_horizon', 'policy_path'):
        if key not in stack_entry:
            raise RuntimeError(
                f'Metadata trade_research_stack missing {key!r} for {symbol_id!r}',
            )

    entry_hint_mode = 'hybrid'
    if 'entry_hint_mode_by_symbol' in metadata:
        entry_hint_mode_by_symbol = metadata['entry_hint_mode_by_symbol']
        if symbol_id in entry_hint_mode_by_symbol:
            entry_hint_mode = str(entry_hint_mode_by_symbol[symbol_id])

    return {
        'run_label': str(stack_entry['run_label']),
        'checkpoint_path': str(checkpoint_path_by_symbol[symbol_id]),
        'eval_horizon': str(stack_entry['eval_horizon']),
        'policy_path': str(stack_entry['policy_path']),
        'entry_hint_mode': entry_hint_mode,
    }


def eval_horizon_from_metadata(
    metadata: dict[str, object],
    symbol_id: str,
) -> str:
    return inference_stack_fingerprint(metadata, symbol_id)['eval_horizon']


def npz_stack_matches_fingerprint(
    existing: dict[str, Any],
    fingerprint: dict[str, str],
) -> bool:
    for key in ('run_label', 'checkpoint_path', 'eval_horizon', 'policy_path', 'entry_hint_mode'):
        if key not in existing:
            return False
        if str(existing[key][0]) != fingerprint[key]:
            return False
    return True


def inference_row_from_batch_result(
    inference_result: dict[str, object],
) -> dict[str, object]:
    if 'policy' not in inference_result:
        raise RuntimeError('Batch inference result missing policy')
    if 'entry_hint' not in inference_result:
        raise RuntimeError('Batch inference result missing entry_hint')
    policy = inference_result['policy']
    if not isinstance(policy, dict):
        raise RuntimeError('Batch inference policy must be a dict')
    if 'action' not in policy:
        raise RuntimeError('Batch inference policy missing action')
    if 'probabilities' not in policy:
        raise RuntimeError('Batch inference policy missing probabilities')
    probabilities = policy['probabilities']
    if not isinstance(probabilities, dict):
        raise RuntimeError('Batch inference policy probabilities must be a dict')
    for key in ('hold', 'long', 'short'):
        if key not in probabilities:
            raise RuntimeError(f'Batch inference policy probabilities missing {key!r}')

    entry_hint = inference_result['entry_hint']
    if not isinstance(entry_hint, dict):
        raise RuntimeError('Batch inference entry_hint must be a dict')

    return {
        'policy_action': str(policy['action']),
        'policy_prob_hold': float(probabilities['hold']),
        'policy_prob_long': float(probabilities['long']),
        'policy_prob_short': float(probabilities['short']),
        'entry_hint_json': json.dumps(entry_hint, ensure_ascii=False, separators=(',', ':')),
    }


def build_inference_by_sample_from_npz(
    npz_data: np.lib.npyio.NpzFile,
    horizon_names: list[str],
) -> dict[int, dict[str, object]]:
    for key in TRADE_RESEARCH_NPZ_INFERENCE_ROW_KEYS:
        if key not in npz_data.files:
            raise RuntimeError(
                'Trade research NPZ missing policy columns; re-run '
                'main.trade_research_export against the current inference_api config',
            )

    sample_index = npz_data['sample_index'].astype(np.int64)
    row_count = int(sample_index.shape[0])
    policy_action = npz_data['policy_action']
    policy_prob_hold = npz_data['policy_prob_hold'].astype(np.float64)
    policy_prob_long = npz_data['policy_prob_long'].astype(np.float64)
    policy_prob_short = npz_data['policy_prob_short'].astype(np.float64)
    entry_hint_json = npz_data['entry_hint_json']
    pred_by_horizon = {
        horizon_name: npz_data[f'pred_{horizon_name}'].astype(np.float64)
        for horizon_name in horizon_names
    }

    inference_by_sample: dict[int, dict[str, object]] = {}
    for row_index in range(row_count):
        sample_idx = int(sample_index[row_index])
        predictions: dict[str, float] = {}
        for horizon_name in horizon_names:
            prediction_key = _prediction_key_for_horizon(horizon_name)
            predictions[prediction_key] = float(pred_by_horizon[horizon_name][row_index])

        entry_hint_raw = entry_hint_json[row_index]
        if isinstance(entry_hint_raw, bytes):
            entry_hint_text = entry_hint_raw.decode('utf-8')
        else:
            entry_hint_text = str(entry_hint_raw)
        entry_hint = json.loads(entry_hint_text)
        if not isinstance(entry_hint, dict):
            raise RuntimeError(f'Invalid entry_hint JSON for sample {sample_idx}')

        inference_by_sample[sample_idx] = {
            'predictions': predictions,
            'policy': {
                'action': str(policy_action[row_index]),
                'probabilities': {
                    'hold': float(policy_prob_hold[row_index]),
                    'long': float(policy_prob_long[row_index]),
                    'short': float(policy_prob_short[row_index]),
                },
            },
            'entry_hint': entry_hint,
        }

    return inference_by_sample


def _prediction_key_for_horizon(horizon_name: str) -> str:
    return f'target_close_return_signed_log2_{horizon_name}'


def _pnl_max_sample_index(dataset_length: int, horizon_steps: int) -> int:
    return pnl_max_sample_index(
        dataset_length=dataset_length,
        horizon_steps=horizon_steps,
    )


def _sample_indices_for_display_grid(
    dataset_length: int,
    step_bars: int,
) -> tuple[list[int], str | None]:
    return sample_indices_for_display_grid(
        dataset_length=dataset_length,
        step_bars=step_bars,
    )


def _sample_indices_for_full_dataset(
    dataset_length: int,
    step_bars: int,
    horizon_steps: int,
) -> tuple[list[int], str | None]:
    return sample_indices_for_pnl_grid(
        dataset_length=dataset_length,
        step_bars=step_bars,
        horizon_steps=horizon_steps,
    )


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
    if hint_mode == 'policy_only':
        if 'recommended_action' not in entry_hint:
            return False
        recommended_action = str(entry_hint['recommended_action'])
        return recommended_action in ('long', 'short')
    if hint_mode == 'sign_fee_band':
        if 'recommended_action' not in entry_hint:
            return False
        recommended_action = str(entry_hint['recommended_action'])
        return recommended_action in ('long', 'short')
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


def compounded_return_from_trade_pnls(trade_pnls: list[float]) -> float:
    equity = 1.0
    for trade_pnl in trade_pnls:
        equity = equity * (1.0 + trade_pnl)
    return equity - 1.0


def summarize_trade_pnls(
    trade_pnls: list[float],
) -> dict[str, float | int | None]:
    trade_count = len(trade_pnls)
    if trade_count == 0:
        return {
            'net_pnl_sum': 0.0,
            'trade_count': 0,
            'avg_trade_pnl': None,
            'compounded_return': None,
        }
    net_pnl_sum = float(sum(trade_pnls))
    return {
        'net_pnl_sum': net_pnl_sum,
        'trade_count': trade_count,
        'avg_trade_pnl': net_pnl_sum / float(trade_count),
        'compounded_return': compounded_return_from_trade_pnls(trade_pnls),
    }


def backtest_metrics_response_prefix(
    prefix: str,
    metrics: dict[str, float | int | None],
) -> dict[str, float | int | None]:
    return {
        f'{prefix}_net_pnl_sum': metrics['net_pnl_sum'],
        f'{prefix}_trade_count': metrics['trade_count'],
        f'{prefix}_avg_trade_pnl': metrics['avg_trade_pnl'],
        f'{prefix}_compounded_return': metrics['compounded_return'],
    }

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


def _run_inference_for_tail_samples(
    sample_indices: list[int],
    inference_dataset: object,
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
        chunk_payloads = [
            _prepare_payload_dict_from_sample(
                dataset=inference_dataset,
                sample_index=sample_index,
            )
            for sample_index in chunk_sample_indices
        ]
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
                'Trade research inference tail: %d/%d batches, %d/%d samples',
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
    real_bar_count: int | None,
) -> float | None:
    entry_bar_index = start_index + sample_index
    exit_bar_index = entry_bar_index + horizon_steps
    if exit_bar_index >= len(inference_level0_to_raw):
        return None
    if real_bar_count is not None:
        if not sample_exit_on_real_bars(
            sample_index=sample_index,
            start_index=start_index,
            horizon_steps=horizon_steps,
            level0_to_raw=inference_level0_to_raw,
            real_bar_count=real_bar_count,
        ):
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
    real_bar_count: int | None,
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
            real_bar_count=real_bar_count,
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
    real_bar_count: int | None,
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
            real_bar_count=real_bar_count,
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

    real_bar_count = int(df.height)
    real_last_trade_id = real_last_start_trade_id(df, real_bar_count)

    dataset = _build_dataset(
        df,
        metadata,
    )
    train_dataset, train_level0_df, raw_to_train_level0_row = _build_train_level0_context(
        df=df,
        metadata=metadata,
    )
    start_index = int(dataset.dataset.start_index)
    dataset_length = len(dataset)
    level0_df = dataset.dataset.aggregated_data[0]
    level0_height = int(level0_df.height)
    level0_to_raw_row_indices = _build_level0_to_raw_row_indices(df, level0_df)

    max_sample_index = _pnl_max_sample_index(
        dataset_length=dataset_length,
        horizon_steps=horizon_steps,
    )
    if max_sample_index < 0:
        raise HTTPException(
            status_code=422,
            detail='Trade research: dataset too short for one full horizon segment',
        )

    sample_indices, sample_selection_note = _sample_indices_for_display_grid(
        dataset_length=dataset_length,
        step_bars=step_bars,
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
    unmapped_grid = inference_tail_grid_sample_indices(
        grid_sample_indices=sample_indices,
        train_sample_index_by_inference_sample=train_sample_index_by_inference_sample,
    )
    unmapped_pnl = inference_tail_pnl_sample_indices(
        pnl_sample_indices=pnl_sample_indices,
        train_sample_index_by_inference_sample=train_sample_index_by_inference_sample,
    )
    inference_tail_indices = _merge_sorted_sample_indices(
        first_indices=unmapped_grid,
        second_indices=unmapped_pnl,
    )
    tail_note = inference_tail_selection_note(
        unmapped_grid_count=len(unmapped_grid),
        unmapped_pnl_count=len(unmapped_pnl),
    )
    if tail_note is not None:
        if sample_selection_note is None:
            sample_selection_note = tail_note
        else:
            sample_selection_note = f'{sample_selection_note}; {tail_note}'
    if skipped_unmapped_samples != len(inference_tail_indices):
        raise RuntimeError(
            'Unmapped sample count mismatch: '
            f'skipped={skipped_unmapped_samples} tail={len(inference_tail_indices)}',
        )

    logger.info(
        'Trade research: symbol=%s grid_samples=%d pnl_samples=%d infer_samples=%d '
        'infer_tail=%d step=%d pnl_stride=%d horizon=%s research_limit=%d level0=%d raw_df=%d start=%d '
        'visible_trade_id=[%s,%s] note=%s',
        symbol_id,
        len(mapped_grid_indices) + len(unmapped_grid),
        len(mapped_pnl_indices) + len(unmapped_pnl),
        len(mapped_inference_indices) + len(inference_tail_indices),
        len(inference_tail_indices),
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

    if len(mapped_inference_indices) == 0 and len(inference_tail_indices) == 0:
        return {
            'symbol_id': symbol_id,
            'eval_horizon': eval_horizon,
            'step_bars': step_bars,
            'research_limit': research_limit,
            'pnl_stride': pnl_stride,
            'required_rows': required_rows,
            'bars_loaded': int(df.height),
            'real_bars_loaded': real_bar_count,
            'real_last_start_trade_id': real_last_trade_id,
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
            'last_grid_inference_provenance': None,
            'sample_selection_note': sample_selection_note,
        }

    last_grid_inference_provenance = None
    try:
        inference_by_sample = _run_inference_for_samples(
            sample_indices=mapped_inference_indices,
            train_sample_index_by_inference_sample=train_sample_index_by_inference_sample,
            train_dataset=train_dataset,
            symbol_id=symbol_id,
        )
        inference_by_sample.update(
            _run_inference_for_tail_samples(
                sample_indices=inference_tail_indices,
                inference_dataset=dataset,
                symbol_id=symbol_id,
            ),
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
            real_bar_count=real_bar_count,
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
            real_bar_count=real_bar_count,
        )

        prediction_key = _prediction_key_for_horizon(eval_horizon)
        for sample_index in sample_indices:
            if sample_index not in inference_by_sample:
                continue
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
            entry_raw_index = level0_to_raw_row_indices[entry_bar_index]
            entry_meta = _raw_bar_metadata(df, entry_raw_index)
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

            pred_eval_log2 = 0.0
            if 'predictions' in inference_result:
                predictions = inference_result['predictions']
                if prediction_key in predictions:
                    pred_eval_log2 = float(predictions[prediction_key])

            pred_target_close = _pred_target_price(
                entry_price=entry_close,
                pred_eval_log2=pred_eval_log2,
            )
            pred_target_open = _pred_target_price(
                entry_price=entry_open,
                pred_eval_log2=pred_eval_log2,
            )

            if is_trade_research_entry_point_segment(
                sample_index=sample_index,
                pnl_max_sample_index=max_sample_index,
                start_index=start_index,
                horizon_steps=horizon_steps,
                level0_height=level0_height,
            ):
                segments.append(
                    {
                        'sample_index': int(sample_index),
                        'segment_kind': 'entry_point',
                        'entry_bar_index': int(entry_bar_index),
                        'exit_bar_index': int(exit_bar_index),
                        'entry_start_trade_id': entry_start_trade_id,
                        'exit_start_trade_id': entry_start_trade_id,
                        'entry_timestamp_ms': int(entry_meta['start_timestamp_ms']),
                        'exit_timestamp_ms': int(entry_meta['start_timestamp_ms']),
                        'entry_open': entry_open,
                        'entry_close': entry_close,
                        'exit_close': entry_close,
                        'pred_start_price': entry_close,
                        'pred_target_price': pred_target_close,
                        'pred_target_open': pred_target_open,
                        'pred_target_close': pred_target_close,
                        'pred_eval_log2': pred_eval_log2,
                        'policy_action': action,
                        'action': recommended_action,
                        **inference_provenance_fields(
                            inference_x1_timestamp_ms=int(entry_meta['start_timestamp_ms']),
                            inference_entry_close=entry_close,
                        ),
                    },
                )
                continue

            exit_raw_index = level0_to_raw_row_indices[exit_bar_index]
            exit_meta = _raw_bar_metadata(df, exit_raw_index)
            exit_close = float(exit_meta['close_price'])

            segments.append(
                {
                    'sample_index': int(sample_index),
                    'segment_kind': 'full_segment',
                    'entry_bar_index': int(entry_bar_index),
                    'exit_bar_index': int(exit_bar_index),
                    'entry_start_trade_id': entry_start_trade_id,
                    'exit_start_trade_id': int(exit_meta['start_trade_id']),
                    'entry_timestamp_ms': int(entry_meta['start_timestamp_ms']),
                    'exit_timestamp_ms': int(exit_meta['start_timestamp_ms']),
                    'entry_open': entry_open,
                    'entry_close': entry_close,
                    'exit_close': exit_close,
                    'pred_start_price': entry_close,
                    'pred_target_price': pred_target_close,
                    'pred_target_open': pred_target_open,
                    'pred_target_close': pred_target_close,
                    'pred_eval_log2': pred_eval_log2,
                    'policy_action': action,
                    'action': recommended_action,
                    **inference_provenance_fields(
                        inference_x1_timestamp_ms=int(entry_meta['start_timestamp_ms']),
                        inference_entry_close=entry_close,
                    ),
                },
            )

        segments_by_sample_index = {
            int(segment['sample_index']): segment for segment in segments
        }
        if len(sample_indices) > 0:
            last_sample_index = max(sample_indices)
            if last_sample_index in inference_by_sample:
                last_entry_bar_index = start_index + last_sample_index
                last_entry_raw_index = level0_to_raw_row_indices[last_entry_bar_index]
                last_entry_meta = _raw_bar_metadata(df, last_entry_raw_index)
                last_segment_kind = None
                last_action = None
                if last_sample_index in segments_by_sample_index:
                    last_segment = segments_by_sample_index[last_sample_index]
                    if 'segment_kind' in last_segment:
                        last_segment_kind = str(last_segment['segment_kind'])
                    if 'action' in last_segment:
                        last_action = str(last_segment['action'])
                last_grid_inference_provenance = last_grid_inference_provenance_summary(
                    sample_index=last_sample_index,
                    inference_x1_timestamp_ms=int(last_entry_meta['start_timestamp_ms']),
                    inference_entry_close=float(last_entry_meta['close_price']),
                    segment_kind=last_segment_kind,
                    action=last_action,
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
        'real_bars_loaded': real_bar_count,
        'real_last_start_trade_id': real_last_trade_id,
        'level0_rows': level0_height,
        'dataset_length': dataset_length,
        'start_index': start_index,
        'visible_min_start_trade_id': visible_min_start_trade_id,
        'visible_max_start_trade_id': visible_max_start_trade_id,
        'sample_count': len(sample_indices),
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
        'last_grid_inference_provenance': last_grid_inference_provenance,
        'sample_selection_note': sample_selection_note,
    }
