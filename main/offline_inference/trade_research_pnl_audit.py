"""
Сверка trade research PnL по NPZ-артефакту.

Запуск:
  python3 -m main.offline_inference.trade_research_pnl_audit BTC_USDT x2048
  python3 -m main.offline_inference.trade_research_pnl_audit BTC_USDT x1536
"""

from __future__ import annotations

import argparse
import sys

import numpy as np

from main.offline_inference.artifacts import list_trade_research_horizons, resolve_trade_research_artifact
from main.offline_inference.trade_research_loader import (
    _collect_grid_trade_pnls_from_npz,
    _collect_sequential_trade_pnls_from_npz,
)
from main.offline_inference.trade_research_npz_store import TradeResearchNpzStore
from main.web_gui.trade_research_service import (
    _sample_indices_for_full_dataset,
    _sample_indices_for_pnl_backtest,
    horizon_steps_from_name,
    summarize_trade_pnls,
)

_AUDIT_MODES: tuple[tuple[str, str], ...] = (
    ('grid hybrid (walk-forward ref)', 'hybrid'),
    ('grid entry-ok (= chart lines)', 'recommended'),
    ('seq hybrid (capital-constrained)', 'hybrid'),
    ('seq entry-ok (SNR bands, stride walk)', 'recommended'),
)


def _pct(linear_sum: float | int | None) -> str:
    if linear_sum is None:
        return '?'
    return f'{100.0 * float(linear_sum):+.2f}%'


def _collect_audit_rows(
    store: TradeResearchNpzStore,
    mapped_grid: list[int],
    mapped_pnl: list[int],
    max_sample_index: int,
    horizon_steps: int,
    eval_target_log2: np.ndarray,
    split: str,
    exit_start_trade_id: np.ndarray | None,
    real_last_start_trade_id: int | None,
) -> list[tuple[str, dict[str, float | int | None]]]:
    rows: list[tuple[str, dict[str, float | int | None]]] = []
    for label, entry_filter in _AUDIT_MODES:
        if label.startswith('grid'):
            pnls = _collect_grid_trade_pnls_from_npz(
                store=store,
                grid_sample_indices=mapped_grid,
                eval_target_log2=eval_target_log2,
                split=split,
                entry_filter=entry_filter,
                exit_start_trade_id=exit_start_trade_id,
                real_last_start_trade_id=real_last_start_trade_id,
            )
        else:
            pnls, _visible = _collect_sequential_trade_pnls_from_npz(
                store=store,
                cached_pnl_sample_indices=mapped_pnl,
                max_sample_index=max_sample_index,
                horizon_steps=horizon_steps,
                eval_target_log2=eval_target_log2,
                split=split,
                entry_filter=entry_filter,
                entry_start_trade_id=None,
                visible_min_start_trade_id=None,
                visible_max_start_trade_id=None,
                exit_start_trade_id=exit_start_trade_id,
                real_last_start_trade_id=real_last_start_trade_id,
            )
        rows.append((label, summarize_trade_pnls(pnls)))
    return rows


def _print_metrics_table(
    title: str,
    rows: list[tuple[str, dict[str, float | int | None]]],
) -> None:
    print('')
    print(title)
    print(f'{"mode":<40} {"trades":>7} {"linear":>12} {"avg/trade":>12}')
    print('-' * 75)
    for label, metrics in rows:
        print(
            f'{label:<40} {int(metrics["trade_count"]):>7} '
            f'{_pct(metrics["net_pnl_sum"]):>12} '
            f'{_pct(metrics["avg_trade_pnl"]):>12}',
        )


def audit_symbol(symbol_id: str, eval_horizon: str) -> int:
    resolved = resolve_trade_research_artifact(symbol_id=symbol_id, eval_horizon=eval_horizon)
    if resolved is None:
        available = list_trade_research_horizons(symbol_id=symbol_id)
        print(
            f'No trade research artifact for {symbol_id} @ {eval_horizon}. '
            f'Available: {", ".join(available) if available else "(none)"}',
            file=sys.stderr,
        )
        return 1

    npz_path, meta = resolved
    npz_data = np.load(npz_path, allow_pickle=True)
    store = TradeResearchNpzStore(npz_data=npz_data)

    artifact_horizon = str(npz_data['eval_horizon'][0])
    horizon_steps = horizon_steps_from_name(artifact_horizon)
    dataset_length = int(npz_data['dataset_length'][0])
    pnl_stride = int(npz_data['pnl_stride'][0])
    max_sample_index = dataset_length - 1 - horizon_steps
    eval_target_log2 = npz_data['eval_target_log2'].astype(np.float64)
    exit_start_trade_id = npz_data['exit_start_trade_id'].astype(np.int64)
    real_last_start_trade_id_value: int | None = None
    if 'real_last_start_trade_id' in npz_data.files:
        real_last_start_trade_id_value = int(npz_data['real_last_start_trade_id'][0])

    grid_indices, _ = _sample_indices_for_full_dataset(
        dataset_length=dataset_length,
        step_bars=horizon_steps,
        horizon_steps=horizon_steps,
    )
    mapped_grid = [
        sample_index_value
        for sample_index_value in grid_indices
        if store.has_sample(sample_index_value)
    ]
    mapped_pnl = [
        sample_index_value
        for sample_index_value in _sample_indices_for_pnl_backtest(
            max_sample_index=max_sample_index,
            stride=pnl_stride,
        )
        if store.has_sample(sample_index_value)
    ]

    print(f'=== Trade research PnL audit: {symbol_id} @ {artifact_horizon} ===')
    print(f'npz_path={npz_path}')
    print(
        f'pnl_stride={pnl_stride} entry_hint_mode='
        f'{meta["entry_hint_mode"] if "entry_hint_mode" in meta else "?"}',
    )
    print(
        f'npz_rows={store.row_count()} grid_points={len(mapped_grid)} '
        f'pnl_samples={len(mapped_pnl)} val_split={store.val_split_available}',
    )
    if store.val_split_available and store.train_size_ratio is not None:
        val_ratio_pct = round((1.0 - float(store.train_size_ratio)) * 100.0)
        print(
            f'train_size={store.train_size} train_size_ratio={store.train_size_ratio} '
            f'val_tail~{val_ratio_pct}%',
        )
    if 'forward_target_padding_bars' in meta:
        print(
            f'forward_target_padding_bars={meta["forward_target_padding_bars"]} '
            f'real_bars_loaded={meta["real_bars_loaded"] if "real_bars_loaded" in meta else "?"}',
        )
    if 'sample_selection_note' in meta:
        print(f'note: {meta["sample_selection_note"]}')

    full_rows = _collect_audit_rows(
        store=store,
        mapped_grid=mapped_grid,
        mapped_pnl=mapped_pnl,
        max_sample_index=max_sample_index,
        horizon_steps=horizon_steps,
        eval_target_log2=eval_target_log2,
        split='all',
        exit_start_trade_id=exit_start_trade_id,
        real_last_start_trade_id=real_last_start_trade_id_value,
    )
    _print_metrics_table('--- full history (all samples) ---', full_rows)

    if store.val_split_available:
        val_rows = _collect_audit_rows(
            store=store,
            mapped_grid=mapped_grid,
            mapped_pnl=mapped_pnl,
            max_sample_index=max_sample_index,
            horizon_steps=horizon_steps,
            eval_target_log2=eval_target_log2,
            split='val',
            exit_start_trade_id=exit_start_trade_id,
            real_last_start_trade_id=real_last_start_trade_id_value,
        )
        val_ratio_pct = 25
        if store.train_size_ratio is not None:
            val_ratio_pct = round((1.0 - float(store.train_size_ratio)) * 100.0)
        _print_metrics_table(
            f'--- val split (~{val_ratio_pct}% tail, walk-forward comparable) ---',
            val_rows,
        )
    else:
        print('')
        print('--- val split: unavailable (re-export NPZ with train_sample_index) ---')

    print('')
    available = list_trade_research_horizons(symbol_id=symbol_id)
    print(f'Available horizons: {", ".join(available) if available else "(none)"}')
    print('')
    print(
        'Calibration buckets (053 classes on sequential trades): '
        'trading_bot/docs/auto_ml/scripts/run_entry_calibration_buckets_l37.sh',
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description='Audit trade research NPZ PnL metrics')
    parser.add_argument('symbol_id', nargs='?', default='BTC_USDT')
    parser.add_argument('eval_horizon', nargs='?', default='x2048')
    args = parser.parse_args()
    raise SystemExit(audit_symbol(symbol_id=args.symbol_id, eval_horizon=args.eval_horizon))


if __name__ == '__main__':
    main()
