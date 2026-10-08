"""Deploy-aligned trade research backtest (entry hint + exit_stack: renew sign-only or fixed_h)."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np

from main.offline_inference.trade_research_npz_store import TradeResearchNpzStore
from main.web_gui.sign_only_renew_exit_common import evaluate_sign_only_checkpoint
from main.web_gui.trade_research_paired_pred_common import renew_pred_log2_at_row
from main.web_gui.trade_research_service import (
    OKX_ROUND_TRIP_TAKER_FEE_RATE,
    _next_cached_sample_index,
    _trade_net_pnl_from_linear_return,
    summarize_trade_pnls,
)


@dataclass(frozen=True)
class TradeResearchDeployStack:
    entry_hint_mode: str
    entry_direction_mode: str | None
    range_interval_resolution: str | None
    min_position_weight: float | None
    exit_stack_mode: str
    exit_eval_horizon: str
    exit_min_hold_steps: int
    exit_check_interval_steps: int


def _deploy_stack_from_exit_fields(
    entry_hint_mode: str,
    entry_direction_mode: str | None,
    range_interval_resolution: str | None,
    min_position_weight: float | None,
    exit_stack_mode: str,
    eval_horizon: str,
    min_hold_steps: int,
    check_interval_steps: int,
) -> TradeResearchDeployStack:
    if exit_stack_mode not in ('rolling_h_renew_sign_only', 'fixed_h'):
        raise RuntimeError(
            f'unsupported exit_stack_mode for trade research: {exit_stack_mode!r}',
        )
    return TradeResearchDeployStack(
        entry_hint_mode=entry_hint_mode,
        entry_direction_mode=entry_direction_mode,
        range_interval_resolution=range_interval_resolution,
        min_position_weight=min_position_weight,
        exit_stack_mode=exit_stack_mode,
        exit_eval_horizon=eval_horizon,
        exit_min_hold_steps=min_hold_steps,
        exit_check_interval_steps=check_interval_steps,
    )


def _range_interval_entry_fields_from_sources(
    hint_cfg: dict[str, object] | None,
    meta: dict[str, Any] | None,
) -> tuple[str | None, float | None]:
    range_interval_resolution: str | None = None
    min_position_weight: float | None = None
    if hint_cfg is not None:
        if 'resolution' in hint_cfg:
            range_interval_resolution = str(hint_cfg['resolution'])
        if 'min_position_weight' in hint_cfg:
            min_position_weight = float(hint_cfg['min_position_weight'])
    if meta is not None:
        if 'range_interval_resolution' in meta:
            range_interval_resolution = str(meta['range_interval_resolution'])
        if 'min_position_weight' in meta and meta['min_position_weight'] is not None:
            min_position_weight = float(meta['min_position_weight'])
    return range_interval_resolution, min_position_weight


def deploy_stack_from_meta(meta: dict[str, Any]) -> TradeResearchDeployStack | None:
    if 'exit_stack_mode' not in meta:
        entry_hint_mode = str(meta['entry_hint_mode']) if 'entry_hint_mode' in meta else ''
        if entry_hint_mode not in ('sign_fee_band', 'range_interval'):
            return None
        if 'eval_horizon' not in meta:
            return None
        eval_horizon = str(meta['eval_horizon'])
        if not eval_horizon.startswith('x'):
            return None
        min_hold_steps = int(eval_horizon[1:])
        entry_direction_mode = None
        if 'entry_direction_mode' in meta and meta['entry_direction_mode'] is not None:
            entry_direction_mode = str(meta['entry_direction_mode'])
        range_interval_resolution, min_position_weight = (
            _range_interval_entry_fields_from_sources(
                hint_cfg=None,
                meta=meta,
            )
        )
        return _deploy_stack_from_exit_fields(
            entry_hint_mode=entry_hint_mode,
            entry_direction_mode=entry_direction_mode,
            range_interval_resolution=range_interval_resolution,
            min_position_weight=min_position_weight,
            exit_stack_mode='rolling_h_renew_sign_only',
            eval_horizon=eval_horizon,
            min_hold_steps=min_hold_steps,
            check_interval_steps=min_hold_steps,
        )
    exit_stack_mode = str(meta['exit_stack_mode'])
    if exit_stack_mode not in ('rolling_h_renew_sign_only', 'fixed_h'):
        return None
    if 'exit_stack_eval_horizon' not in meta:
        raise RuntimeError('trade research meta missing exit_stack_eval_horizon')
    if 'exit_stack_min_hold_steps' not in meta:
        raise RuntimeError('trade research meta missing exit_stack_min_hold_steps')
    entry_hint_mode = 'hybrid'
    if 'entry_hint_mode' in meta:
        entry_hint_mode = str(meta['entry_hint_mode'])
    entry_direction_mode = None
    if 'entry_direction_mode' in meta and meta['entry_direction_mode'] is not None:
        entry_direction_mode = str(meta['entry_direction_mode'])
    eval_horizon = str(meta['exit_stack_eval_horizon'])
    min_hold_steps = int(meta['exit_stack_min_hold_steps'])
    check_interval_steps = min_hold_steps
    if 'exit_stack_check_interval_steps' in meta:
        check_interval_steps = int(meta['exit_stack_check_interval_steps'])
    range_interval_resolution, min_position_weight = (
        _range_interval_entry_fields_from_sources(
            hint_cfg=None,
            meta=meta,
        )
    )
    return _deploy_stack_from_exit_fields(
        entry_hint_mode=entry_hint_mode,
        entry_direction_mode=entry_direction_mode,
        range_interval_resolution=range_interval_resolution,
        min_position_weight=min_position_weight,
        exit_stack_mode=exit_stack_mode,
        eval_horizon=eval_horizon,
        min_hold_steps=min_hold_steps,
        check_interval_steps=check_interval_steps,
    )


def deploy_stack_from_inference_metadata(
    metadata: dict[str, object],
    symbol_id: str,
    eval_horizon: str,
) -> TradeResearchDeployStack | None:
    if 'exit_stack_by_symbol' not in metadata:
        return None
    exit_stack_by_symbol = metadata['exit_stack_by_symbol']
    if not isinstance(exit_stack_by_symbol, dict):
        raise RuntimeError('inference metadata exit_stack_by_symbol must be a dict')
    if symbol_id not in exit_stack_by_symbol:
        return None
    exit_stack = exit_stack_by_symbol[symbol_id]
    if not isinstance(exit_stack, dict):
        raise RuntimeError(f'invalid exit_stack for {symbol_id!r}')
    exit_stack_mode = str(exit_stack['mode'])
    if exit_stack_mode not in ('rolling_h_renew_sign_only', 'fixed_h'):
        return None
    if str(exit_stack['eval_horizon']) != eval_horizon:
        raise RuntimeError(
            'exit_stack eval_horizon '
            f'{exit_stack["eval_horizon"]!r} != trade research {eval_horizon!r}',
        )
    entry_hint_mode = 'hybrid'
    if 'entry_hint_mode_by_symbol' in metadata:
        modes = metadata['entry_hint_mode_by_symbol']
        if isinstance(modes, dict) and symbol_id in modes:
            entry_hint_mode = str(modes[symbol_id])
    entry_direction_mode = None
    hint_cfg: dict[str, object] | None = None
    if 'entry_hint_by_symbol' in metadata:
        hints = metadata['entry_hint_by_symbol']
        if isinstance(hints, dict) and symbol_id in hints:
            raw_hint_cfg = hints[symbol_id]
            if isinstance(raw_hint_cfg, dict):
                hint_cfg = raw_hint_cfg
                if 'direction_mode' in hint_cfg:
                    entry_direction_mode = str(hint_cfg['direction_mode'])
    range_interval_resolution, min_position_weight = (
        _range_interval_entry_fields_from_sources(
            hint_cfg=hint_cfg,
            meta=None,
        )
    )
    min_hold_steps = int(exit_stack['min_hold_steps'])
    return _deploy_stack_from_exit_fields(
        entry_hint_mode=entry_hint_mode,
        entry_direction_mode=entry_direction_mode,
        range_interval_resolution=range_interval_resolution,
        min_position_weight=min_position_weight,
        exit_stack_mode=exit_stack_mode,
        eval_horizon=eval_horizon,
        min_hold_steps=min_hold_steps,
        check_interval_steps=min_hold_steps,
    )


def deploy_stack_meta_payload(stack: TradeResearchDeployStack) -> dict[str, object]:
    payload: dict[str, object] = {
        'exit_stack_mode': stack.exit_stack_mode,
        'exit_stack_eval_horizon': stack.exit_eval_horizon,
        'exit_stack_min_hold_steps': stack.exit_min_hold_steps,
        'exit_stack_check_interval_steps': stack.exit_check_interval_steps,
        'entry_direction_mode': stack.entry_direction_mode,
    }
    if stack.range_interval_resolution is not None:
        payload['range_interval_resolution'] = stack.range_interval_resolution
    if stack.min_position_weight is not None:
        payload['min_position_weight'] = stack.min_position_weight
    return payload


def _linear_return_between_rows(
    entry_close: np.ndarray,
    entry_row: int,
    exit_row: int,
) -> float:
    entry_price = float(entry_close[entry_row])
    exit_price = float(entry_close[exit_row])
    if entry_price <= 0.0 or exit_price <= 0.0:
        raise RuntimeError('entry/exit close must be positive')
    return exit_price / entry_price - 1.0


def sign_only_exit_sample_index_for_entry(
    store: TradeResearchNpzStore,
    entry_sample_index: int,
    max_sample_index: int,
    pred_eval_log2: np.ndarray,
    deploy_stack: TradeResearchDeployStack,
    side: str,
    pred_short_log2: np.ndarray | None,
) -> tuple[int | None, str]:
    max_bars = max_sample_index - entry_sample_index
    if max_bars < deploy_stack.exit_min_hold_steps:
        return None, 'before_min_hold_window'

    min_hold_steps = deploy_stack.exit_min_hold_steps
    check_interval_steps = deploy_stack.exit_check_interval_steps
    last_renew_segment_evaluated = -1
    bars_held = min_hold_steps
    last_feasible_exit_sample: int | None = None

    while bars_held <= max_bars:
        checkpoint_sample = entry_sample_index + bars_held
        row_index = store.row_for_sample(checkpoint_sample)
        if row_index is None:
            bars_held = bars_held + check_interval_steps
            continue
        last_feasible_exit_sample = checkpoint_sample
        pred_log2 = renew_pred_log2_at_row(
            side=side,
            row_index=row_index,
            long_pred_array=pred_eval_log2,
            short_pred_array=pred_short_log2,
        )
        (
            suggest_close,
            exit_reason,
            _pred_linear,
            updated_last_renew,
            _current_segment,
            at_renew_checkpoint,
        ) = evaluate_sign_only_checkpoint(
            side=side,
            bars_held=bars_held,
            min_hold_steps=min_hold_steps,
            check_interval_steps=check_interval_steps,
            pred_log2=pred_log2,
            last_renew_segment_evaluated=last_renew_segment_evaluated,
        )
        if at_renew_checkpoint:
            last_renew_segment_evaluated = updated_last_renew
        if suggest_close:
            return checkpoint_sample, exit_reason
        bars_held = bars_held + check_interval_steps

    if last_feasible_exit_sample is not None:
        return last_feasible_exit_sample, 'data_end_last_checkpoint'
    return None, 'no_checkpoint_rows'


def fixed_h_exit_sample_index_for_entry(
    store: TradeResearchNpzStore,
    entry_sample_index: int,
    max_sample_index: int,
    deploy_stack: TradeResearchDeployStack,
) -> tuple[int | None, str]:
    if deploy_stack.exit_stack_mode != 'fixed_h':
        raise RuntimeError(
            f'fixed_h_exit_sample_index_for_entry requires fixed_h stack, '
            f'got {deploy_stack.exit_stack_mode!r}',
        )
    hold_steps = deploy_stack.exit_min_hold_steps
    if max_sample_index - entry_sample_index < hold_steps:
        return None, 'before_min_hold_window'
    exit_sample_index = entry_sample_index + hold_steps
    if exit_sample_index > max_sample_index:
        return None, 'exit_beyond_max_sample'
    exit_row_index = store.row_for_sample(exit_sample_index)
    if exit_row_index is None:
        return None, 'missing_exit_row'
    return exit_sample_index, 'fixed_h_hold_complete'


def deploy_exit_sample_index_for_entry(
    store: TradeResearchNpzStore,
    entry_sample_index: int,
    max_sample_index: int,
    pred_eval_log2: np.ndarray,
    deploy_stack: TradeResearchDeployStack,
    side: str,
    pred_short_log2: np.ndarray | None,
) -> tuple[int | None, str]:
    if deploy_stack.exit_stack_mode == 'fixed_h':
        return fixed_h_exit_sample_index_for_entry(
            store=store,
            entry_sample_index=entry_sample_index,
            max_sample_index=max_sample_index,
            deploy_stack=deploy_stack,
        )
    if deploy_stack.exit_stack_mode == 'rolling_h_renew_sign_only':
        return sign_only_exit_sample_index_for_entry(
            store=store,
            entry_sample_index=entry_sample_index,
            max_sample_index=max_sample_index,
            pred_eval_log2=pred_eval_log2,
            deploy_stack=deploy_stack,
            side=side,
            pred_short_log2=pred_short_log2,
        )
    raise RuntimeError(
        f'unsupported deploy exit_stack_mode: {deploy_stack.exit_stack_mode!r}',
    )


def trade_pnl_for_deploy_sign_only_exit(
    store: TradeResearchNpzStore,
    entry_row_index: int,
    entry_sample_index: int,
    max_sample_index: int,
    entry_close: np.ndarray,
    pred_eval_log2: np.ndarray,
    deploy_stack: TradeResearchDeployStack,
    side: str,
    exit_start_trade_id: np.ndarray | None,
    real_last_start_trade_id: int | None,
    pred_short_log2: np.ndarray | None,
) -> tuple[float | None, int | None, str | None]:
    exit_sample_index, exit_reason = deploy_exit_sample_index_for_entry(
        store=store,
        entry_sample_index=entry_sample_index,
        max_sample_index=max_sample_index,
        pred_eval_log2=pred_eval_log2,
        deploy_stack=deploy_stack,
        side=side,
        pred_short_log2=pred_short_log2,
    )
    if exit_sample_index is None:
        return None, None, exit_reason
    exit_row_index = store.row_for_sample(exit_sample_index)
    if exit_row_index is None:
        return None, None, 'missing_exit_row'
    if exit_start_trade_id is not None and real_last_start_trade_id is not None:
        if int(exit_start_trade_id[exit_row_index]) > real_last_start_trade_id:
            return None, exit_sample_index, 'exit_beyond_real_bars'
    realized_linear = _linear_return_between_rows(
        entry_close=entry_close,
        entry_row=entry_row_index,
        exit_row=exit_row_index,
    )
    trade_pnl = _trade_net_pnl_from_linear_return(
        action=side,
        realized_linear_return=realized_linear,
        round_trip_fee_rate=OKX_ROUND_TRIP_TAKER_FEE_RATE,
    )
    return trade_pnl, exit_sample_index, exit_reason


def _row_passes_deploy_entry(
    store: TradeResearchNpzStore,
    row_index: int,
) -> bool:
    recommended_action = store.recommended_action_at_row(row_index)
    if recommended_action is None:
        return False
    return recommended_action in ('long', 'short')


def _deploy_entry_side_at_row(
    store: TradeResearchNpzStore,
    row_index: int,
) -> str | None:
    recommended_action = store.recommended_action_at_row(row_index)
    if recommended_action is None:
        return None
    if recommended_action not in ('long', 'short'):
        return None
    return recommended_action


def collect_grid_deploy_trade_pnls_from_npz(
    store: TradeResearchNpzStore,
    grid_sample_indices: list[int],
    max_sample_index: int,
    entry_close: np.ndarray,
    pred_eval_log2: np.ndarray,
    pred_short_log2: np.ndarray | None,
    deploy_stack: TradeResearchDeployStack,
    split: str,
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
        if not _row_passes_deploy_entry(store=store, row_index=row_index):
            continue
        side = _deploy_entry_side_at_row(store=store, row_index=row_index)
        if side is None:
            continue
        trade_pnl, _exit_sample, _reason = trade_pnl_for_deploy_sign_only_exit(
            store=store,
            entry_row_index=row_index,
            entry_sample_index=sample_index_value,
            max_sample_index=max_sample_index,
            entry_close=entry_close,
            pred_eval_log2=pred_eval_log2,
            deploy_stack=deploy_stack,
            side=side,
            exit_start_trade_id=exit_start_trade_id,
            real_last_start_trade_id=real_last_start_trade_id,
            pred_short_log2=pred_short_log2,
        )
        if trade_pnl is None:
            continue
        position_weight = store.position_weight_at_row(row_index)
        trade_pnls.append(trade_pnl * position_weight)
    return trade_pnls


def collect_sequential_deploy_trade_pnls_from_npz(
    store: TradeResearchNpzStore,
    cached_pnl_sample_indices: list[int],
    max_sample_index: int,
    entry_close: np.ndarray,
    pred_eval_log2: np.ndarray,
    pred_short_log2: np.ndarray | None,
    deploy_stack: TradeResearchDeployStack,
    split: str,
    min_entry_spacing_samples: int,
    exit_start_trade_id: np.ndarray | None,
    real_last_start_trade_id: int | None,
) -> list[float]:
    cached_sample_indices = sorted(cached_pnl_sample_indices)
    cached_sample_set = set(cached_sample_indices)
    trade_pnls: list[float] = []
    sample_index_value = 0
    last_entry_sample_index: int | None = None

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
            sample_index_value = sample_index_value + 1
            continue
        if not store.row_matches_split(row_index, split):
            sample_index_value = sample_index_value + 1
            continue
        if not _row_passes_deploy_entry(store=store, row_index=row_index):
            sample_index_value = sample_index_value + 1
            continue
        side = _deploy_entry_side_at_row(store=store, row_index=row_index)
        if side is None:
            sample_index_value = sample_index_value + 1
            continue
        if last_entry_sample_index is not None:
            if sample_index_value - last_entry_sample_index < min_entry_spacing_samples:
                sample_index_value = sample_index_value + 1
                continue

        trade_pnl, exit_sample_index, _reason = trade_pnl_for_deploy_sign_only_exit(
            store=store,
            entry_row_index=row_index,
            entry_sample_index=sample_index_value,
            max_sample_index=max_sample_index,
            entry_close=entry_close,
            pred_eval_log2=pred_eval_log2,
            deploy_stack=deploy_stack,
            side=side,
            exit_start_trade_id=exit_start_trade_id,
            real_last_start_trade_id=real_last_start_trade_id,
            pred_short_log2=pred_short_log2,
        )
        if trade_pnl is None or exit_sample_index is None:
            sample_index_value = sample_index_value + 1
            continue

        position_weight = store.position_weight_at_row(row_index)
        trade_pnls.append(trade_pnl * position_weight)
        last_entry_sample_index = sample_index_value
        sample_index_value = exit_sample_index + 1

    return trade_pnls


def collect_sequential_deploy_entry_sample_indices_from_npz(
    store: TradeResearchNpzStore,
    cached_pnl_sample_indices: list[int],
    max_sample_index: int,
    entry_close: np.ndarray,
    pred_eval_log2: np.ndarray,
    pred_short_log2: np.ndarray | None,
    deploy_stack: TradeResearchDeployStack,
    split: str,
    min_entry_spacing_samples: int,
    exit_start_trade_id: np.ndarray | None,
    real_last_start_trade_id: int | None,
) -> list[int]:
    cached_sample_indices = sorted(cached_pnl_sample_indices)
    cached_sample_set = set(cached_sample_indices)
    entry_sample_indices: list[int] = []
    sample_index_value = 0
    last_entry_sample_index: int | None = None

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
            sample_index_value = sample_index_value + 1
            continue
        if not store.row_matches_split(row_index, split):
            sample_index_value = sample_index_value + 1
            continue
        if not _row_passes_deploy_entry(store=store, row_index=row_index):
            sample_index_value = sample_index_value + 1
            continue
        side = _deploy_entry_side_at_row(store=store, row_index=row_index)
        if side is None:
            sample_index_value = sample_index_value + 1
            continue
        if last_entry_sample_index is not None:
            if sample_index_value - last_entry_sample_index < min_entry_spacing_samples:
                sample_index_value = sample_index_value + 1
                continue

        trade_pnl, exit_sample_index, _reason = trade_pnl_for_deploy_sign_only_exit(
            store=store,
            entry_row_index=row_index,
            entry_sample_index=sample_index_value,
            max_sample_index=max_sample_index,
            entry_close=entry_close,
            pred_eval_log2=pred_eval_log2,
            deploy_stack=deploy_stack,
            side=side,
            exit_start_trade_id=exit_start_trade_id,
            real_last_start_trade_id=real_last_start_trade_id,
            pred_short_log2=pred_short_log2,
        )
        if trade_pnl is None or exit_sample_index is None:
            sample_index_value = sample_index_value + 1
            continue

        entry_sample_indices.append(int(sample_index_value))
        last_entry_sample_index = sample_index_value
        sample_index_value = exit_sample_index + 1

    return entry_sample_indices


@dataclass
class _OpenOverlapLeg:
    exit_sample_index: int


def collect_overlap_deploy_entry_sample_indices_from_npz(
    store: TradeResearchNpzStore,
    entry_candidate_sample_indices: list[int],
    max_sample_index: int,
    entry_close: np.ndarray,
    pred_eval_log2: np.ndarray,
    pred_short_log2: np.ndarray | None,
    deploy_stack: TradeResearchDeployStack,
    split: str,
    min_entry_spacing_samples: int,
    max_open_positions: int,
    exit_start_trade_id: np.ndarray | None,
    real_last_start_trade_id: int | None,
) -> list[int]:
    if min_entry_spacing_samples <= 0:
        raise RuntimeError(
            f'min_entry_spacing_samples must be positive, got: {min_entry_spacing_samples}',
        )
    if max_open_positions <= 0:
        raise RuntimeError(
            f'max_open_positions must be positive, got: {max_open_positions}',
        )

    entry_sample_indices: list[int] = []
    open_legs: list[_OpenOverlapLeg] = []
    last_entry_sample_index: int | None = None

    for sample_index_value in sorted(entry_candidate_sample_indices):
        if sample_index_value > max_sample_index:
            continue

        open_legs = [
            leg
            for leg in open_legs
            if leg.exit_sample_index > sample_index_value
        ]

        row_index = store.row_for_sample(sample_index_value)
        if row_index is None:
            continue
        if not store.has_train_aligned_targets_at_row(row_index):
            continue
        if not store.row_matches_split(row_index, split):
            continue
        if not _row_passes_deploy_entry(store=store, row_index=row_index):
            continue
        side = _deploy_entry_side_at_row(store=store, row_index=row_index)
        if side is None:
            continue
        if last_entry_sample_index is not None:
            if sample_index_value - last_entry_sample_index < min_entry_spacing_samples:
                continue
        if len(open_legs) >= max_open_positions:
            continue

        trade_pnl, exit_sample_index, _reason = trade_pnl_for_deploy_sign_only_exit(
            store=store,
            entry_row_index=row_index,
            entry_sample_index=sample_index_value,
            max_sample_index=max_sample_index,
            entry_close=entry_close,
            pred_eval_log2=pred_eval_log2,
            deploy_stack=deploy_stack,
            side=side,
            exit_start_trade_id=exit_start_trade_id,
            real_last_start_trade_id=real_last_start_trade_id,
            pred_short_log2=pred_short_log2,
        )
        if trade_pnl is None or exit_sample_index is None:
            continue

        entry_sample_indices.append(int(sample_index_value))
        open_legs.append(_OpenOverlapLeg(exit_sample_index=int(exit_sample_index)))
        last_entry_sample_index = sample_index_value

    return entry_sample_indices


def collect_overlap_deploy_trade_records_from_npz(
    store: TradeResearchNpzStore,
    entry_candidate_sample_indices: list[int],
    max_sample_index: int,
    entry_close: np.ndarray,
    pred_eval_log2: np.ndarray,
    pred_short_log2: np.ndarray | None,
    deploy_stack: TradeResearchDeployStack,
    split: str,
    min_entry_spacing_samples: int,
    max_open_positions: int,
    exit_start_trade_id: np.ndarray | None,
    real_last_start_trade_id: int | None,
    entry_start_trade_id: np.ndarray | None,
    entry_timestamp_ms: np.ndarray | None,
) -> list[dict[str, float | int | str]]:
    """
    Paper-style overlap: deploy exit per leg (fixed_h or renew), new entries every ``min_entry_spacing_samples``
    x1 bars since the last entry (not after prior exit). ``max_open_positions`` caps concurrency.

    Entry candidates are usually ``pnl_stride`` inference rows. With ``pnl_stride=32``, that matches
    live spacing-8 paper only at those bars (sub-stride entries need denser export, e.g. stride 8).
    """
    if min_entry_spacing_samples <= 0:
        raise RuntimeError(
            f'min_entry_spacing_samples must be positive, got: {min_entry_spacing_samples}',
        )
    if max_open_positions <= 0:
        raise RuntimeError(
            f'max_open_positions must be positive, got: {max_open_positions}',
        )

    trade_records: list[dict[str, float | int | str]] = []
    open_legs: list[_OpenOverlapLeg] = []
    last_entry_sample_index: int | None = None

    for sample_index_value in sorted(entry_candidate_sample_indices):
        if sample_index_value > max_sample_index:
            continue

        open_legs = [
            leg
            for leg in open_legs
            if leg.exit_sample_index > sample_index_value
        ]

        row_index = store.row_for_sample(sample_index_value)
        if row_index is None:
            continue
        if not store.has_train_aligned_targets_at_row(row_index):
            continue
        if not store.row_matches_split(row_index, split):
            continue
        if not _row_passes_deploy_entry(store=store, row_index=row_index):
            continue
        side = _deploy_entry_side_at_row(store=store, row_index=row_index)
        if side is None:
            continue
        if last_entry_sample_index is not None:
            if sample_index_value - last_entry_sample_index < min_entry_spacing_samples:
                continue
        if len(open_legs) >= max_open_positions:
            continue

        trade_pnl, exit_sample_index, _reason = trade_pnl_for_deploy_sign_only_exit(
            store=store,
            entry_row_index=row_index,
            entry_sample_index=sample_index_value,
            max_sample_index=max_sample_index,
            entry_close=entry_close,
            pred_eval_log2=pred_eval_log2,
            deploy_stack=deploy_stack,
            side=side,
            exit_start_trade_id=exit_start_trade_id,
            real_last_start_trade_id=real_last_start_trade_id,
            pred_short_log2=pred_short_log2,
        )
        if trade_pnl is None or exit_sample_index is None:
            continue

        position_weight = store.position_weight_at_row(row_index)
        weighted_pnl = trade_pnl * position_weight
        train_sample_index_value = -1
        if store._train_sample_index is not None:
            train_sample_index_value = int(store._train_sample_index[row_index])
        record: dict[str, float | int | str] = {
            'entry_sample_index': int(sample_index_value),
            'row_index': int(row_index),
            'side': side,
            'position_weight': float(position_weight),
            'pnl_linear_before_weight': float(trade_pnl),
            'weighted_pnl_linear': float(weighted_pnl),
            'train_sample_index': train_sample_index_value,
        }
        if entry_start_trade_id is not None:
            record['entry_start_trade_id'] = int(entry_start_trade_id[row_index])
        if entry_timestamp_ms is not None:
            record['entry_timestamp_ms'] = int(entry_timestamp_ms[row_index])
        trade_records.append(record)
        open_legs.append(_OpenOverlapLeg(exit_sample_index=int(exit_sample_index)))
        last_entry_sample_index = sample_index_value

    return trade_records


def collect_overlap_deploy_trade_pnls_from_npz(
    store: TradeResearchNpzStore,
    entry_candidate_sample_indices: list[int],
    max_sample_index: int,
    entry_close: np.ndarray,
    pred_eval_log2: np.ndarray,
    pred_short_log2: np.ndarray | None,
    deploy_stack: TradeResearchDeployStack,
    split: str,
    min_entry_spacing_samples: int,
    max_open_positions: int,
    exit_start_trade_id: np.ndarray | None,
    real_last_start_trade_id: int | None,
) -> list[float]:
    trade_records = collect_overlap_deploy_trade_records_from_npz(
        store=store,
        entry_candidate_sample_indices=entry_candidate_sample_indices,
        max_sample_index=max_sample_index,
        entry_close=entry_close,
        pred_eval_log2=pred_eval_log2,
        pred_short_log2=pred_short_log2,
        deploy_stack=deploy_stack,
        split=split,
        min_entry_spacing_samples=min_entry_spacing_samples,
        max_open_positions=max_open_positions,
        exit_start_trade_id=exit_start_trade_id,
        real_last_start_trade_id=real_last_start_trade_id,
        entry_start_trade_id=None,
        entry_timestamp_ms=None,
    )
    return [float(record['weighted_pnl_linear']) for record in trade_records]


def summarize_deploy_metrics(trade_pnls: list[float]) -> dict[str, float | int | None]:
    return summarize_trade_pnls(trade_pnls)


def pred_target_price_from_log2(
    entry_price: float,
    pred_log2: float,
    side: str,
) -> float:
    from main.web_gui.trade_research_paired_pred_common import pred_target_price_for_side

    return pred_target_price_for_side(
        entry_price=entry_price,
        pred_log2=pred_log2,
        side=side,
    )


def resolve_deploy_segment_exit_fields(
    store: TradeResearchNpzStore,
    entry_sample_index: int,
    start_index: int,
    max_sample_index: int,
    pred_eval_log2: np.ndarray,
    pred_short_log2: np.ndarray | None,
    deploy_stack: TradeResearchDeployStack,
    entry_timestamp_ms: np.ndarray,
    entry_close: np.ndarray,
    exit_close: np.ndarray,
    entry_start_trade_id: np.ndarray,
    exit_start_trade_id: np.ndarray,
    segment_pred_start_price: float,
    side: str,
) -> dict[str, object] | None:
    entry_row_index = store.row_for_sample(entry_sample_index)
    if entry_row_index is None:
        return None
    exit_sample_index, _exit_reason = deploy_exit_sample_index_for_entry(
        store=store,
        entry_sample_index=entry_sample_index,
        max_sample_index=max_sample_index,
        pred_eval_log2=pred_eval_log2,
        deploy_stack=deploy_stack,
        side=side,
        pred_short_log2=pred_short_log2,
    )
    if exit_sample_index is None:
        return None
    exit_row_index = store.row_for_sample(exit_sample_index)
    if exit_row_index is None:
        return None
    entry_row_index_for_pred = store.row_for_sample(entry_sample_index)
    if entry_row_index_for_pred is None:
        return None
    if deploy_stack.exit_stack_mode == 'fixed_h':
        exit_pred_log2 = renew_pred_log2_at_row(
            side=side,
            row_index=entry_row_index_for_pred,
            long_pred_array=pred_eval_log2,
            short_pred_array=pred_short_log2,
        )
    else:
        exit_pred_log2 = renew_pred_log2_at_row(
            side=side,
            row_index=exit_row_index,
            long_pred_array=pred_eval_log2,
            short_pred_array=pred_short_log2,
        )
    exit_bar_index = start_index + exit_sample_index
    return {
        'exit_bar_index': int(exit_bar_index),
        'exit_start_trade_id': int(exit_start_trade_id[exit_row_index]),
        'exit_timestamp_ms': int(entry_timestamp_ms[exit_row_index]),
        'exit_close': float(exit_close[exit_row_index]),
        'pred_target_price': pred_target_price_from_log2(
            entry_price=segment_pred_start_price,
            pred_log2=exit_pred_log2,
            side=side,
        ),
        'pred_target_close': pred_target_price_from_log2(
            entry_price=segment_pred_start_price,
            pred_log2=exit_pred_log2,
            side=side,
        ),
        'pred_eval_log2_at_exit': exit_pred_log2,
        'exit_sample_index': int(exit_sample_index),
        'entry_start_trade_id': int(entry_start_trade_id[entry_row_index]),
    }
