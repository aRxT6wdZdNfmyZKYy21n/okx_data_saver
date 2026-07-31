from __future__ import annotations

import logging
import math
import time
from typing import Any

import numpy as np

from main.offline_inference.trade_research_npz_store import TradeResearchNpzStore
from main.web_gui.trade_research_service import (
    OKX_ROUND_TRIP_TAKER_FEE_RATE,
    _next_cached_sample_index,
    _pred_target_price,
    _sample_indices_for_full_dataset,
    _sample_indices_for_pnl_backtest,
    _segment_visible_by_start_trade_id,
    _trade_net_pnl_from_linear_return,
    backtest_metrics_response_prefix,
    horizon_steps_from_name,
    summarize_trade_pnls,
)

logger = logging.getLogger(__name__)


def _realized_linear_from_npz_row(
    row_index: int,
    eval_target_log2: np.ndarray,
) -> float | None:
    target_log2 = float(eval_target_log2[row_index])
    if math.isnan(target_log2):
        return None
    return float(2.0 ** target_log2 - 1.0)


def _trade_pnl_for_npz_row(
    row_index: int,
    direction_action: str,
    eval_target_log2: np.ndarray,
    exit_start_trade_id: np.ndarray | None,
    real_last_start_trade_id: int | None,
) -> float | None:
    if (
        exit_start_trade_id is not None
        and real_last_start_trade_id is not None
        and int(exit_start_trade_id[row_index]) > real_last_start_trade_id
    ):
        return None
    realized_linear_return = _realized_linear_from_npz_row(
        row_index=row_index,
        eval_target_log2=eval_target_log2,
    )
    return _trade_net_pnl_from_linear_return(
        action=direction_action,
        realized_linear_return=realized_linear_return,
        round_trip_fee_rate=OKX_ROUND_TRIP_TAKER_FEE_RATE,
    )


def _row_passes_entry_filter(
    store: TradeResearchNpzStore,
    row_index: int,
    entry_filter: str,
) -> bool:
    if entry_filter == 'hybrid':
        return store.allows_entry_at_row(row_index)
    if entry_filter == 'recommended':
        return store.recommended_action_at_row(row_index) is not None
    raise ValueError(f'Unknown entry_filter: {entry_filter!r}')


def _trade_action_for_row(
    store: TradeResearchNpzStore,
    row_index: int,
    entry_filter: str,
) -> str:
    if entry_filter == 'hybrid':
        return store.direction_action_at_row(row_index)
    if entry_filter == 'recommended':
        recommended_action = store.recommended_action_at_row(row_index)
        if recommended_action is None:
            raise RuntimeError('recommended entry_filter requires recommended_action')
        return recommended_action
    raise ValueError(f'Unknown entry_filter: {entry_filter!r}')


def _collect_grid_trade_pnls_from_npz(
    store: TradeResearchNpzStore,
    grid_sample_indices: list[int],
    eval_target_log2: np.ndarray,
    split: str,
    entry_filter: str,
    exit_start_trade_id: np.ndarray | None,
    real_last_start_trade_id: int | None,
) -> list[float]:
    trade_pnls: list[float] = []
    for sample_index_value in grid_sample_indices:
        row_index = store.row_for_sample(sample_index_value)
        if row_index is None:
            continue
        if not store.has_train_aligned_targets_at_row(row_index):
            continue
        if not store.row_matches_split(row_index, split):
            continue
        if not _row_passes_entry_filter(
            store=store,
            row_index=row_index,
            entry_filter=entry_filter,
        ):
            continue
        trade_pnl = _trade_pnl_for_npz_row(
            row_index=row_index,
            direction_action=_trade_action_for_row(
                store=store,
                row_index=row_index,
                entry_filter=entry_filter,
            ),
            eval_target_log2=eval_target_log2,
            exit_start_trade_id=exit_start_trade_id,
            real_last_start_trade_id=real_last_start_trade_id,
        )
        if trade_pnl is None:
            continue
        trade_pnls.append(trade_pnl)
    return trade_pnls


def _collect_sequential_trade_pnls_from_npz(
    store: TradeResearchNpzStore,
    cached_pnl_sample_indices: list[int],
    max_sample_index: int,
    horizon_steps: int,
    eval_target_log2: np.ndarray,
    split: str,
    entry_filter: str,
    entry_start_trade_id: np.ndarray | None,
    visible_min_start_trade_id: int | None,
    visible_max_start_trade_id: int | None,
    exit_start_trade_id: np.ndarray | None,
    real_last_start_trade_id: int | None,
) -> tuple[list[float], list[float]]:
    cached_sample_indices = sorted(cached_pnl_sample_indices)
    cached_sample_set = set(cached_sample_indices)
    trade_pnls: list[float] = []
    visible_trade_pnls: list[float] = []
    sample_index_value = 0
    while sample_index_value <= max_sample_index:
        if sample_index_value not in cached_sample_set:
            next_cached = _next_cached_sample_index(
                sample_index=sample_index_value,
                cached_sample_indices=cached_sample_indices,
            )
            if next_cached is None:
                break
            sample_index_value = next_cached

        row_index = store.row_for_sample(sample_index_value)
        if row_index is None:
            sample_index_value = sample_index_value + 1
            continue

        if not store.has_train_aligned_targets_at_row(row_index):
            if _row_passes_entry_filter(
                store=store,
                row_index=row_index,
                entry_filter=entry_filter,
            ):
                if sample_index_value + horizon_steps > max_sample_index:
                    break
                sample_index_value = sample_index_value + horizon_steps
            else:
                sample_index_value = sample_index_value + 1
            continue

        if not store.row_matches_split(row_index, split):
            if _row_passes_entry_filter(
                store=store,
                row_index=row_index,
                entry_filter=entry_filter,
            ):
                if sample_index_value + horizon_steps > max_sample_index:
                    break
                sample_index_value = sample_index_value + horizon_steps
            else:
                sample_index_value = sample_index_value + 1
            continue

        if not _row_passes_entry_filter(
            store=store,
            row_index=row_index,
            entry_filter=entry_filter,
        ):
            sample_index_value = sample_index_value + 1
            continue

        if sample_index_value + horizon_steps > max_sample_index:
            break

        trade_pnl = _trade_pnl_for_npz_row(
            row_index=row_index,
            direction_action=_trade_action_for_row(
                store=store,
                row_index=row_index,
                entry_filter=entry_filter,
            ),
            eval_target_log2=eval_target_log2,
            exit_start_trade_id=exit_start_trade_id,
            real_last_start_trade_id=real_last_start_trade_id,
        )
        if trade_pnl is None:
            sample_index_value = sample_index_value + horizon_steps
            continue
        trade_pnls.append(trade_pnl)
        if entry_start_trade_id is not None:
            start_trade_id = int(entry_start_trade_id[row_index])
            if _segment_visible_by_start_trade_id(
                entry_start_trade_id=start_trade_id,
                visible_min_start_trade_id=visible_min_start_trade_id,
                visible_max_start_trade_id=visible_max_start_trade_id,
            ):
                visible_trade_pnls.append(trade_pnl)

        sample_index_value = sample_index_value + horizon_steps

    return trade_pnls, visible_trade_pnls


def _visible_trade_pnls_from_grid(
    store: TradeResearchNpzStore,
    grid_sample_indices: list[int],
    eval_target_log2: np.ndarray,
    entry_start_trade_id: np.ndarray,
    visible_min_start_trade_id: int | None,
    visible_max_start_trade_id: int | None,
    split: str,
    entry_filter: str,
    exit_start_trade_id: np.ndarray | None,
    real_last_start_trade_id: int | None,
) -> list[float]:
    visible_pnls: list[float] = []
    for sample_index_value in grid_sample_indices:
        row_index = store.row_for_sample(sample_index_value)
        if row_index is None:
            continue
        if not store.has_train_aligned_targets_at_row(row_index):
            continue
        if not store.row_matches_split(row_index, split):
            continue
        if not _row_passes_entry_filter(
            store=store,
            row_index=row_index,
            entry_filter=entry_filter,
        ):
            continue
        start_trade_id = int(entry_start_trade_id[row_index])
        if not _segment_visible_by_start_trade_id(
            entry_start_trade_id=start_trade_id,
            visible_min_start_trade_id=visible_min_start_trade_id,
            visible_max_start_trade_id=visible_max_start_trade_id,
        ):
            continue
        trade_pnl = _trade_pnl_for_npz_row(
            row_index=row_index,
            direction_action=_trade_action_for_row(
                store=store,
                row_index=row_index,
                entry_filter=entry_filter,
            ),
            eval_target_log2=eval_target_log2,
            exit_start_trade_id=exit_start_trade_id,
            real_last_start_trade_id=real_last_start_trade_id,
        )
        if trade_pnl is None:
            continue
        visible_pnls.append(trade_pnl)
    return visible_pnls


def load_trade_research_response(
    symbol_id: str,
    eval_horizon: str,
    step_bars: int,
    visible_min_start_trade_id: int | None,
    visible_max_start_trade_id: int | None,
    meta: dict[str, Any],
    npz_path: str,
) -> dict[str, object]:
    started = time.monotonic()
    logger.info(
        'trade_research_loader start symbol=%s eval_horizon=%s step_bars=%d npz=%s',
        symbol_id,
        eval_horizon,
        step_bars,
        npz_path,
    )
    npz_data = np.load(npz_path, allow_pickle=True)
    logger.info(
        'trade_research_loader npz loaded symbol=%s duration_ms=%d',
        symbol_id,
        int((time.monotonic() - started) * 1000),
    )
    stored_eval_horizon = str(npz_data['eval_horizon'][0])
    requested_eval_horizon = eval_horizon
    if eval_horizon != stored_eval_horizon:
        raise RuntimeError(
            'Trade research artifact eval_horizon='
            f'{stored_eval_horizon} does not match request {eval_horizon}',
        )

    horizon_steps = horizon_steps_from_name(eval_horizon)
    if step_bars != horizon_steps:
        logger.warning(
            'Trade research step_bars=%s corrected to %s for eval_horizon=%s',
            step_bars,
            horizon_steps,
            eval_horizon,
        )
        step_bars = horizon_steps

    npz_store = TradeResearchNpzStore(npz_data=npz_data)
    logger.info(
        'trade_research_loader npz store ready symbol=%s rows=%d duration_ms=%d',
        symbol_id,
        npz_store.row_count(),
        int((time.monotonic() - started) * 1000),
    )

    dataset_length = int(npz_data['dataset_length'][0])
    start_index = int(npz_data['start_index'][0])
    pnl_stride = int(npz_data['pnl_stride'][0])
    eval_target_log2 = npz_data['eval_target_log2'].astype(np.float64)

    max_sample_index = dataset_length - 1 - horizon_steps
    grid_sample_indices, _grid_note = _sample_indices_for_full_dataset(
        dataset_length=dataset_length,
        step_bars=horizon_steps,
        horizon_steps=horizon_steps,
    )
    mapped_grid_indices = [
        sample_index_value
        for sample_index_value in grid_sample_indices
        if npz_store.has_sample(sample_index_value)
    ]
    pnl_sample_indices = _sample_indices_for_pnl_backtest(
        max_sample_index=max_sample_index,
        stride=pnl_stride,
    )
    mapped_pnl_indices = [
        sample_index_value
        for sample_index_value in pnl_sample_indices
        if npz_store.has_sample(sample_index_value)
    ]
    pnl_grid_indices = [
        sample_index_value
        for sample_index_value in grid_sample_indices
        if npz_store.has_sample(sample_index_value)
    ]

    entry_start_trade_id = npz_data['entry_start_trade_id'].astype(np.int64)
    exit_start_trade_id = npz_data['exit_start_trade_id'].astype(np.int64)
    entry_timestamp_ms = npz_data['entry_timestamp_ms'].astype(np.int64)
    exit_timestamp_ms = npz_data['exit_timestamp_ms'].astype(np.int64)
    entry_open = npz_data['entry_open'].astype(np.float64)
    entry_close = npz_data['entry_close'].astype(np.float64)
    exit_close = npz_data['exit_close'].astype(np.float64)
    pred_eval_log2 = npz_data[f'pred_{eval_horizon}'].astype(np.float64)
    has_pred_start_price = 'pred_start_price' in npz_data.files
    has_pred_target_price = 'pred_target_price' in npz_data.files
    if has_pred_start_price:
        pred_start_price = npz_data['pred_start_price'].astype(np.float64)
    if has_pred_target_price:
        pred_target_price = npz_data['pred_target_price'].astype(np.float64)
    npz_entry_start_trade_id = npz_data['entry_start_trade_id'].astype(np.int64)
    real_last_start_trade_id_value: int | None = None
    if 'real_last_start_trade_id' in npz_data.files:
        real_last_start_trade_id_value = int(npz_data['real_last_start_trade_id'][0])

    grid_trade_pnls = _collect_grid_trade_pnls_from_npz(
        store=npz_store,
        grid_sample_indices=pnl_grid_indices,
        eval_target_log2=eval_target_log2,
        split='all',
        entry_filter='hybrid',
        exit_start_trade_id=exit_start_trade_id,
        real_last_start_trade_id=real_last_start_trade_id_value,
    )
    grid_metrics = summarize_trade_pnls(grid_trade_pnls)
    grid_entry_ok_pnls = _collect_grid_trade_pnls_from_npz(
        store=npz_store,
        grid_sample_indices=pnl_grid_indices,
        eval_target_log2=eval_target_log2,
        split='all',
        entry_filter='recommended',
        exit_start_trade_id=exit_start_trade_id,
        real_last_start_trade_id=real_last_start_trade_id_value,
    )
    grid_entry_ok_metrics = summarize_trade_pnls(grid_entry_ok_pnls)
    visible_grid_pnls = _visible_trade_pnls_from_grid(
        store=npz_store,
        grid_sample_indices=pnl_grid_indices,
        eval_target_log2=eval_target_log2,
        entry_start_trade_id=npz_entry_start_trade_id,
        visible_min_start_trade_id=visible_min_start_trade_id,
        visible_max_start_trade_id=visible_max_start_trade_id,
        split='all',
        entry_filter='recommended',
        exit_start_trade_id=exit_start_trade_id,
        real_last_start_trade_id=real_last_start_trade_id_value,
    )
    visible_grid_metrics = summarize_trade_pnls(visible_grid_pnls)
    grid_backtest_net_pnl_sum = float(grid_metrics['net_pnl_sum'])
    grid_backtest_trade_count = int(grid_metrics['trade_count'])
    grid_backtest_visible_net_pnl_sum = float(visible_grid_metrics['net_pnl_sum'])
    grid_backtest_visible_trade_count = int(visible_grid_metrics['trade_count'])
    logger.info(
        'trade_research_loader grid backtest done symbol=%s hybrid_trades=%d entry_ok_trades=%d duration_ms=%d',
        symbol_id,
        grid_backtest_trade_count,
        int(grid_entry_ok_metrics['trade_count']),
        int((time.monotonic() - started) * 1000),
    )

    sequential_trade_pnls, sequential_visible_pnls = _collect_sequential_trade_pnls_from_npz(
        store=npz_store,
        cached_pnl_sample_indices=mapped_pnl_indices,
        max_sample_index=max_sample_index,
        horizon_steps=horizon_steps,
        eval_target_log2=eval_target_log2,
        split='all',
        entry_filter='hybrid',
        entry_start_trade_id=npz_entry_start_trade_id,
        visible_min_start_trade_id=visible_min_start_trade_id,
        visible_max_start_trade_id=visible_max_start_trade_id,
        exit_start_trade_id=exit_start_trade_id,
        real_last_start_trade_id=real_last_start_trade_id_value,
    )
    sequential_metrics = summarize_trade_pnls(sequential_trade_pnls)
    sequential_entry_ok_pnls, _sequential_entry_ok_visible_pnls = _collect_sequential_trade_pnls_from_npz(
        store=npz_store,
        cached_pnl_sample_indices=mapped_pnl_indices,
        max_sample_index=max_sample_index,
        horizon_steps=horizon_steps,
        eval_target_log2=eval_target_log2,
        split='all',
        entry_filter='recommended',
        entry_start_trade_id=npz_entry_start_trade_id,
        visible_min_start_trade_id=visible_min_start_trade_id,
        visible_max_start_trade_id=visible_max_start_trade_id,
        exit_start_trade_id=exit_start_trade_id,
        real_last_start_trade_id=real_last_start_trade_id_value,
    )
    sequential_entry_ok_metrics = summarize_trade_pnls(sequential_entry_ok_pnls)
    sequential_backtest_net_pnl_sum = float(sequential_metrics['net_pnl_sum'])
    sequential_backtest_trade_count = int(sequential_metrics['trade_count'])
    sequential_visible_metrics = summarize_trade_pnls(sequential_visible_pnls)
    sequential_backtest_visible_net_pnl_sum = float(sequential_visible_metrics['net_pnl_sum'])
    sequential_backtest_visible_trade_count = int(sequential_visible_metrics['trade_count'])
    logger.info(
        'trade_research_loader sequential backtest done symbol=%s hybrid_trades=%d entry_ok_trades=%d duration_ms=%d',
        symbol_id,
        sequential_backtest_trade_count,
        int(sequential_entry_ok_metrics['trade_count']),
        int((time.monotonic() - started) * 1000),
    )

    val_split_available = npz_store.val_split_available
    train_size_ratio = npz_store.train_size_ratio
    sequential_val_metrics: dict[str, float | int | None] = {
        'net_pnl_sum': None,
        'trade_count': None,
        'avg_trade_pnl': None,
        'compounded_return': None,
    }
    grid_val_metrics: dict[str, float | int | None] = {
        'net_pnl_sum': None,
        'trade_count': None,
        'avg_trade_pnl': None,
        'compounded_return': None,
    }
    if val_split_available:
        sequential_val_pnls, _sequential_val_visible_pnls = _collect_sequential_trade_pnls_from_npz(
            store=npz_store,
            cached_pnl_sample_indices=mapped_pnl_indices,
            max_sample_index=max_sample_index,
            horizon_steps=horizon_steps,
            eval_target_log2=eval_target_log2,
            split='val',
            entry_filter='hybrid',
            entry_start_trade_id=None,
            visible_min_start_trade_id=None,
            visible_max_start_trade_id=None,
            exit_start_trade_id=exit_start_trade_id,
            real_last_start_trade_id=real_last_start_trade_id_value,
        )
        sequential_val_metrics = summarize_trade_pnls(sequential_val_pnls)
        grid_val_pnls = _collect_grid_trade_pnls_from_npz(
            store=npz_store,
            grid_sample_indices=pnl_grid_indices,
            eval_target_log2=eval_target_log2,
            split='val',
            entry_filter='hybrid',
            exit_start_trade_id=exit_start_trade_id,
            real_last_start_trade_id=real_last_start_trade_id_value,
        )
        grid_val_metrics = summarize_trade_pnls(grid_val_pnls)
        logger.info(
            'trade_research_loader val split symbol=%s seq_trades=%s seq_linear=%s seq_comp=%s',
            symbol_id,
            sequential_val_metrics['trade_count'],
            sequential_val_metrics['net_pnl_sum'],
            sequential_val_metrics['compounded_return'],
        )

    segments: list[dict[str, object]] = []
    policy_trade_count = 0
    entry_allowed_count = 0

    for sample_index_value in mapped_grid_indices:
        row_index = npz_store.row_for_sample(sample_index_value)
        if row_index is None:
            continue

        action = npz_store.policy_action_at_row(row_index)
        if action in ('long', 'short'):
            policy_trade_count = policy_trade_count + 1

        recommended_action = npz_store.recommended_action_at_row(row_index)
        if recommended_action is None:
            continue
        entry_allowed_count = entry_allowed_count + 1

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
        if has_pred_start_price:
            segment_pred_start_price = float(pred_start_price[row_index])
        else:
            segment_pred_start_price = float(entry_close[row_index])
        if has_pred_target_price:
            segment_pred_target_close = float(pred_target_price[row_index])
        else:
            segment_pred_target_close = _pred_target_price(
                entry_price=segment_pred_start_price,
                pred_eval_log2=pred_log2,
            )

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
                'pred_start_price': segment_pred_start_price,
                'pred_target_price': segment_pred_target_close,
                'pred_target_open': _pred_target_price(
                    entry_price=float(entry_open[row_index]),
                    pred_eval_log2=pred_log2,
                ),
                'pred_target_close': segment_pred_target_close,
                'pred_eval_log2': pred_log2,
                'policy_action': action,
                'action': recommended_action,
            },
        )

    duration_ms = int((time.monotonic() - started) * 1000)
    logger.info(
        'trade_research_loader done symbol=%s segments=%d policy_trades=%d entry_allowed=%d duration_ms=%d',
        symbol_id,
        len(segments),
        policy_trade_count,
        entry_allowed_count,
        duration_ms,
    )
    return {
        'status': meta['status'],
        'symbol_id': symbol_id,
        'requested_eval_horizon': requested_eval_horizon,
        'eval_horizon': eval_horizon,
        'step_bars': step_bars,
        'research_limit': int(meta['research_limit']),
        'pnl_stride': pnl_stride,
        'entry_hint_mode': meta['entry_hint_mode']
        if 'entry_hint_mode' in meta
        else None,
        'required_rows': int(meta['required_rows']),
        'bars_loaded': int(meta['bars_loaded']),
        'real_bars_loaded': int(meta['real_bars_loaded'])
        if 'real_bars_loaded' in meta
        else int(meta['bars_loaded']),
        'forward_target_padding_bars': int(meta['forward_target_padding_bars'])
        if 'forward_target_padding_bars' in meta
        else 0,
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
        **backtest_metrics_response_prefix('grid_backtest', grid_metrics),
        **backtest_metrics_response_prefix('grid_entry_ok_backtest', grid_entry_ok_metrics),
        'sequential_backtest_net_pnl_sum': sequential_backtest_net_pnl_sum,
        'sequential_backtest_trade_count': sequential_backtest_trade_count,
        **backtest_metrics_response_prefix('sequential_backtest', sequential_metrics),
        **backtest_metrics_response_prefix(
            'sequential_entry_ok_backtest',
            sequential_entry_ok_metrics,
        ),
        'sequential_backtest_visible_net_pnl_sum': sequential_backtest_visible_net_pnl_sum,
        'sequential_backtest_visible_trade_count': sequential_backtest_visible_trade_count,
        'val_split_available': val_split_available,
        'train_size_ratio': train_size_ratio,
        **backtest_metrics_response_prefix('sequential_backtest_val', sequential_val_metrics),
        **backtest_metrics_response_prefix('grid_backtest_val', grid_val_metrics),
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
        'display_payload_mode': meta['payload_mode']
        if 'payload_mode' in meta
        else None,
        'display_sample_selection_note': meta['sample_selection_note']
        if 'sample_selection_note' in meta
        else None,
        'display_artifact_updated_at_ms': meta['updated_at_ms']
        if 'updated_at_ms' in meta
        else None,
        'artifact_updated_at_ms': meta['updated_at_ms'],
        'run_label': meta['run_label'],
        'checkpoint_path': meta['checkpoint_path'],
    }
