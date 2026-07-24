from __future__ import annotations

import logging
from typing import Any

import numpy as np

from main.web_gui.trade_research_service import (
    OKX_ROUND_TRIP_TAKER_FEE_RATE,
    _direction_action_from_inference,
    _hybrid_backtest_allows_entry,
    _next_cached_sample_index,
    _pred_target_price,
    _recommended_entry_action,
    _sample_indices_for_full_dataset,
    _sample_indices_for_pnl_backtest,
    _segment_visible_by_start_trade_id,
    _trade_net_pnl_from_linear_return,
    build_inference_by_sample_from_npz,
    horizon_steps_from_name,
)

logger = logging.getLogger(__name__)


def _sample_index_to_row(sample_index: np.ndarray) -> dict[int, int]:
    return {
        int(sample_index[row_index]): row_index
        for row_index in range(int(sample_index.shape[0]))
    }


def _realized_linear_from_npz_row(
    row_index: int,
    eval_target_log2: np.ndarray,
) -> float:
    target_log2 = float(eval_target_log2[row_index])
    return float(2.0 ** target_log2 - 1.0)


def _trade_pnl_for_npz_sample(
    sample_index_value: int,
    inference_result: dict[str, object],
    sample_index_to_row: dict[int, int],
    eval_target_log2: np.ndarray,
) -> float | None:
    if sample_index_value not in sample_index_to_row:
        return None
    row_index = sample_index_to_row[sample_index_value]
    realized_linear_return = _realized_linear_from_npz_row(
        row_index=row_index,
        eval_target_log2=eval_target_log2,
    )
    backtest_direction = _direction_action_from_inference(inference_result)
    return _trade_net_pnl_from_linear_return(
        action=backtest_direction,
        realized_linear_return=realized_linear_return,
        round_trip_fee_rate=OKX_ROUND_TRIP_TAKER_FEE_RATE,
    )


def _compute_grid_backtest_from_npz(
    inference_by_sample: dict[int, dict[str, object]],
    grid_sample_indices: list[int],
    sample_index_to_row: dict[int, int],
    eval_target_log2: np.ndarray,
    entry_start_trade_id: np.ndarray,
    visible_min_start_trade_id: int | None,
    visible_max_start_trade_id: int | None,
) -> tuple[float, int, float, int]:
    net_pnl_sum = 0.0
    trade_count = 0
    visible_net_pnl_sum = 0.0
    visible_trade_count = 0

    for sample_index_value in grid_sample_indices:
        if sample_index_value not in inference_by_sample:
            continue
        inference_result = inference_by_sample[sample_index_value]
        if not _hybrid_backtest_allows_entry(inference_result):
            continue

        trade_pnl = _trade_pnl_for_npz_sample(
            sample_index_value=sample_index_value,
            inference_result=inference_result,
            sample_index_to_row=sample_index_to_row,
            eval_target_log2=eval_target_log2,
        )
        if trade_pnl is None:
            continue

        net_pnl_sum = net_pnl_sum + trade_pnl
        trade_count = trade_count + 1

        row_index = sample_index_to_row[sample_index_value]
        start_trade_id = int(entry_start_trade_id[row_index])
        if _segment_visible_by_start_trade_id(
            entry_start_trade_id=start_trade_id,
            visible_min_start_trade_id=visible_min_start_trade_id,
            visible_max_start_trade_id=visible_max_start_trade_id,
        ):
            visible_net_pnl_sum = visible_net_pnl_sum + trade_pnl
            visible_trade_count = visible_trade_count + 1

    return net_pnl_sum, trade_count, visible_net_pnl_sum, visible_trade_count


def _compute_sequential_backtest_from_npz(
    inference_by_sample: dict[int, dict[str, object]],
    pnl_sample_indices: list[int],
    max_sample_index: int,
    horizon_steps: int,
    sample_index_to_row: dict[int, int],
    eval_target_log2: np.ndarray,
    entry_start_trade_id: np.ndarray,
    visible_min_start_trade_id: int | None,
    visible_max_start_trade_id: int | None,
) -> tuple[float, int, float, int]:
    cached_sample_indices = sorted(
        sample_index_value
        for sample_index_value in pnl_sample_indices
        if sample_index_value in inference_by_sample
    )

    net_pnl_sum = 0.0
    trade_count = 0
    visible_net_pnl_sum = 0.0
    visible_trade_count = 0

    sample_index_value = 0
    while sample_index_value <= max_sample_index:
        if sample_index_value not in inference_by_sample:
            next_cached = _next_cached_sample_index(
                sample_index=sample_index_value,
                cached_sample_indices=cached_sample_indices,
            )
            if next_cached is None:
                break
            sample_index_value = next_cached

        inference_result = inference_by_sample[sample_index_value]
        if not _hybrid_backtest_allows_entry(inference_result):
            sample_index_value = sample_index_value + 1
            continue

        if sample_index_value + horizon_steps > max_sample_index:
            break

        trade_pnl = _trade_pnl_for_npz_sample(
            sample_index_value=sample_index_value,
            inference_result=inference_result,
            sample_index_to_row=sample_index_to_row,
            eval_target_log2=eval_target_log2,
        )
        if trade_pnl is None:
            sample_index_value = sample_index_value + 1
            continue

        net_pnl_sum = net_pnl_sum + trade_pnl
        trade_count = trade_count + 1

        row_index = sample_index_to_row[sample_index_value]
        start_trade_id = int(entry_start_trade_id[row_index])
        if _segment_visible_by_start_trade_id(
            entry_start_trade_id=start_trade_id,
            visible_min_start_trade_id=visible_min_start_trade_id,
            visible_max_start_trade_id=visible_max_start_trade_id,
        ):
            visible_net_pnl_sum = visible_net_pnl_sum + trade_pnl
            visible_trade_count = visible_trade_count + 1

        sample_index_value = sample_index_value + horizon_steps

    return net_pnl_sum, trade_count, visible_net_pnl_sum, visible_trade_count


def load_trade_research_response(
    symbol_id: str,
    eval_horizon: str,
    step_bars: int,
    visible_min_start_trade_id: int | None,
    visible_max_start_trade_id: int | None,
    meta: dict[str, Any],
    npz_path: str,
) -> dict[str, object]:
    npz_data = np.load(npz_path, allow_pickle=True)
    stored_eval_horizon = str(npz_data['eval_horizon'][0])
    if eval_horizon != stored_eval_horizon:
        logger.warning(
            'Trade research request eval_horizon=%s differs from artifact %s; using artifact',
            eval_horizon,
            stored_eval_horizon,
        )
        eval_horizon = stored_eval_horizon

    horizon_steps = horizon_steps_from_name(eval_horizon)
    if step_bars != horizon_steps:
        raise ValueError(
            f'Non-overlapping research requires step_bars={horizon_steps} '
            f'for eval_horizon={eval_horizon}, got {step_bars}',
        )

    horizon_names = [str(item) for item in npz_data['horizon_names']]
    inference_by_sample = build_inference_by_sample_from_npz(
        npz_data=npz_data,
        horizon_names=horizon_names,
    )

    dataset_length = int(npz_data['dataset_length'][0])
    start_index = int(npz_data['start_index'][0])
    pnl_stride = int(npz_data['pnl_stride'][0])
    sample_index = npz_data['sample_index'].astype(np.int64)
    eval_target_log2 = npz_data['eval_target_log2'].astype(np.float64)
    sample_index_to_row = _sample_index_to_row(sample_index)

    max_sample_index = dataset_length - 1 - horizon_steps
    grid_sample_indices, _grid_note = _sample_indices_for_full_dataset(
        dataset_length=dataset_length,
        step_bars=horizon_steps,
        horizon_steps=horizon_steps,
    )
    mapped_grid_indices = [
        sample_index_value
        for sample_index_value in grid_sample_indices
        if sample_index_value in inference_by_sample
    ]
    pnl_sample_indices = _sample_indices_for_pnl_backtest(
        max_sample_index=max_sample_index,
        stride=pnl_stride,
    )
    mapped_pnl_indices = [
        sample_index_value
        for sample_index_value in pnl_sample_indices
        if sample_index_value in inference_by_sample
    ]

    entry_start_trade_id = npz_data['entry_start_trade_id'].astype(np.int64)
    exit_start_trade_id = npz_data['exit_start_trade_id'].astype(np.int64)
    entry_timestamp_ms = npz_data['entry_timestamp_ms'].astype(np.int64)
    exit_timestamp_ms = npz_data['exit_timestamp_ms'].astype(np.int64)
    entry_open = npz_data['entry_open'].astype(np.float64)
    entry_close = npz_data['entry_close'].astype(np.float64)
    exit_close = npz_data['exit_close'].astype(np.float64)
    pred_eval_log2 = npz_data[f'pred_{eval_horizon}'].astype(np.float64)

    (
        grid_backtest_net_pnl_sum,
        grid_backtest_trade_count,
        grid_backtest_visible_net_pnl_sum,
        grid_backtest_visible_trade_count,
    ) = _compute_grid_backtest_from_npz(
        inference_by_sample=inference_by_sample,
        grid_sample_indices=mapped_grid_indices,
        sample_index_to_row=sample_index_to_row,
        eval_target_log2=eval_target_log2,
        entry_start_trade_id=entry_start_trade_id,
        visible_min_start_trade_id=visible_min_start_trade_id,
        visible_max_start_trade_id=visible_max_start_trade_id,
    )

    (
        sequential_backtest_net_pnl_sum,
        sequential_backtest_trade_count,
        sequential_backtest_visible_net_pnl_sum,
        sequential_backtest_visible_trade_count,
    ) = _compute_sequential_backtest_from_npz(
        inference_by_sample=inference_by_sample,
        pnl_sample_indices=mapped_pnl_indices,
        max_sample_index=max_sample_index,
        horizon_steps=horizon_steps,
        sample_index_to_row=sample_index_to_row,
        eval_target_log2=eval_target_log2,
        entry_start_trade_id=entry_start_trade_id,
        visible_min_start_trade_id=visible_min_start_trade_id,
        visible_max_start_trade_id=visible_max_start_trade_id,
    )

    segments: list[dict[str, object]] = []
    policy_trade_count = 0
    entry_allowed_count = 0

    for sample_index_value in mapped_grid_indices:
        inference_result = inference_by_sample[sample_index_value]
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

        row_index = sample_index_to_row[sample_index_value]
        entry_start = int(entry_start_trade_id[row_index])
        if not _segment_visible_by_start_trade_id(
            entry_start_trade_id=entry_start,
            visible_min_start_trade_id=visible_min_start_trade_id,
            visible_max_start_trade_id=visible_max_start_trade_id,
        ):
            continue

        entry_bar_index = start_index + sample_index_value
        exit_bar_index = entry_bar_index + horizon_steps
        pred_log2 = float(pred_eval_log2[row_index])

        segments.append(
            {
                'sample_index': int(sample_index_value),
                'entry_bar_index': int(entry_bar_index),
                'exit_bar_index': int(exit_bar_index),
                'entry_start_trade_id': entry_start,
                'exit_start_trade_id': int(exit_start_trade_id[row_index]),
                'entry_timestamp_ms': int(entry_timestamp_ms[row_index]),
                'exit_timestamp_ms': int(exit_timestamp_ms[row_index]),
                'entry_open': float(entry_open[row_index]),
                'entry_close': float(entry_close[row_index]),
                'exit_close': float(exit_close[row_index]),
                'pred_target_open': _pred_target_price(
                    entry_price=float(entry_open[row_index]),
                    pred_eval_log2=pred_log2,
                ),
                'pred_target_close': _pred_target_price(
                    entry_price=float(entry_close[row_index]),
                    pred_eval_log2=pred_log2,
                ),
                'pred_eval_log2': pred_log2,
                'policy_action': action,
                'action': recommended_action,
            },
        )

    return {
        'status': meta['status'],
        'symbol_id': symbol_id,
        'eval_horizon': eval_horizon,
        'step_bars': step_bars,
        'research_limit': int(meta['research_limit']),
        'pnl_stride': pnl_stride,
        'required_rows': int(meta['required_rows']),
        'bars_loaded': int(meta['bars_loaded']),
        'level0_rows': int(meta['level0_rows']),
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
        'sample_selection_note': meta['sample_selection_note']
        if 'sample_selection_note' in meta
        else None,
        'artifact_updated_at_ms': meta['updated_at_ms'],
        'run_label': meta['run_label'],
        'checkpoint_path': meta['checkpoint_path'],
    }
