"""
Offline deploy overlap backtest on trade research NPZ — compare val split definitions.

Reference: doc 290 ds1 val @ grid 48, confidence min_weight=0, fee 0.1%% linear.

Usage:
  cd okx_data_saver && source .venv/bin/activate
  python3 -m ai_code.audit_trade_research_deploy_overlap_splits BTC_USDT x2048
"""

from __future__ import annotations

import argparse
import json
import sys

import numpy as np

from main.offline_inference.artifacts import resolve_trade_research_artifact
from main.offline_inference.trade_research_deploy_backtest_common import (
    collect_overlap_deploy_entry_sample_indices_from_npz,
    collect_overlap_deploy_trade_pnls_from_npz,
    deploy_stack_from_meta,
)
from main.offline_inference.trade_research_npz_store import TradeResearchNpzStore
from main.runtime_limits import apply_runtime_limits
from main.web_gui.trade_research_service import (
    _sample_indices_for_pnl_backtest,
    horizon_steps_from_name,
    summarize_trade_pnls,
)
from settings import settings

# SequentialTradeDataModule ds1 holdout (btc_0M_to_8.01M.csv, train_size=0.9)
CSV_DS1_VAL_TRAIN_SAMPLE_INDEX_START = 1705258
CSV_DS1_VAL_TRAIN_SAMPLE_INDEX_END = CSV_DS1_VAL_TRAIN_SAMPLE_INDEX_START + 189474


def _row_in_csv_ds1_val(store: TradeResearchNpzStore, row_index: int) -> bool:
    if store._train_sample_index is None:
        return False
    tsi = int(store._train_sample_index[row_index])
    if tsi < 0:
        return False
    return (
        tsi >= CSV_DS1_VAL_TRAIN_SAMPLE_INDEX_START
        and tsi < CSV_DS1_VAL_TRAIN_SAMPLE_INDEX_END
    )


def _filter_pnl_candidates_to_rows(
    store: TradeResearchNpzStore,
    mapped_pnl: list[int],
    row_predicate,
) -> list[int]:
    filtered: list[int] = []
    for sample_index in mapped_pnl:
        row_index = store.row_for_sample(sample_index)
        if row_index is None:
            continue
        if row_predicate(store, row_index):
            filtered.append(sample_index)
    return filtered


def _overlap_metrics(
    store: TradeResearchNpzStore,
    mapped_pnl: list[int],
    max_sample_index: int,
    entry_close: np.ndarray,
    pred_long: np.ndarray,
    pred_short: np.ndarray,
    deploy_stack: object,
    exit_start_trade_id: np.ndarray,
    real_last_start_trade_id: int,
    split_label: str,
    candidate_filter,
) -> dict[str, object]:
    candidates = mapped_pnl
    if candidate_filter is not None:
        candidates = _filter_pnl_candidates_to_rows(
            store=store,
            mapped_pnl=mapped_pnl,
            row_predicate=candidate_filter,
        )
    pnls = collect_overlap_deploy_trade_pnls_from_npz(
        store=store,
        entry_candidate_sample_indices=candidates,
        max_sample_index=max_sample_index,
        entry_close=entry_close,
        pred_eval_log2=pred_long,
        pred_short_log2=pred_short,
        deploy_stack=deploy_stack,
        split='all',
        min_entry_spacing_samples=int(settings.WEB_GUI_TRADING_ENTRY_GRID_STEPS),
        max_open_positions=int(settings.WEB_GUI_TRADING_MAX_OPEN_POSITIONS),
        exit_start_trade_id=exit_start_trade_id,
        real_last_start_trade_id=real_last_start_trade_id,
    )
    summary = summarize_trade_pnls(pnls)
    entry_indices = collect_overlap_deploy_entry_sample_indices_from_npz(
        store=store,
        entry_candidate_sample_indices=candidates,
        max_sample_index=max_sample_index,
        entry_close=entry_close,
        pred_eval_log2=pred_long,
        pred_short_log2=pred_short,
        deploy_stack=deploy_stack,
        split='all',
        min_entry_spacing_samples=int(settings.WEB_GUI_TRADING_ENTRY_GRID_STEPS),
        max_open_positions=int(settings.WEB_GUI_TRADING_MAX_OPEN_POSITIONS),
        exit_start_trade_id=exit_start_trade_id,
        real_last_start_trade_id=real_last_start_trade_id,
    )
    long_count = 0
    short_count = 0
    for sample_index in entry_indices:
        row_index = store.row_for_sample(sample_index)
        if row_index is None:
            continue
        action = store.recommended_action_at_row(row_index)
        if action == 'long':
            long_count = long_count + 1
        elif action == 'short':
            short_count = short_count + 1
    summary_out = {
        'split_label': split_label,
        'pnl_candidate_bars': len(candidates),
        'trade_count': int(summary['trade_count']),
        'net_pnl_sum_linear': float(summary['net_pnl_sum']),
        'net_pnl_sum_pct': float(summary['net_pnl_sum']) * 100.0,
        'avg_trade_pnl_linear': float(summary['avg_trade_pnl']),
        'compounded_return': summary['compounded_return'],
        'entry_long_count': long_count,
        'entry_short_count': short_count,
    }
    return summary_out


def _run_deploy_npz_reference(trading_bot_root: str) -> dict[str, object]:
    import os
    import sys
    from pathlib import Path

    tb_root = Path(trading_bot_root)
    if str(tb_root) not in sys.path:
        sys.path.insert(0, str(tb_root))
    os.chdir(tb_root)
    from src.tools.range_interval_confidence_common import (
        evaluate_confidence_sized_fixed_h_stack,
    )
    from src.tools.trading_policy_common import load_predictions_npz

    long_path = Path('docs/auto_ml/267_predictions/l267_e0_ds1_val_x2048.npz')
    short_path = Path('docs/auto_ml/281_predictions/l281_e0_ds1_val_x2048.npz')
    eval_horizon = 'x2048'
    _, _, _, _, long_preds, long_targets, _ = load_predictions_npz(long_path)
    _, _, _, _, short_preds, _, _ = load_predictions_npz(short_path)
    long_log2 = np.asarray(long_preds[eval_horizon], dtype=np.float64)
    short_log2 = np.asarray(short_preds[eval_horizon], dtype=np.float64)
    target_eval = np.asarray(long_targets[eval_horizon], dtype=np.float64)
    stack = evaluate_confidence_sized_fixed_h_stack(
        long_pred_log2=long_log2,
        short_pred_log2=short_log2,
        target_eval_log2=target_eval,
        eval_horizon=eval_horizon,
        interval_eps_log2=0.0,
        round_trip_fee_rate=0.001,
        entry_grid_steps=48,
        entry_grid_phase=0,
        min_abs_confidence=0.0,
        min_position_weight=0.0,
        require_pred_exceeds_fee=False,
        entry_scope='interval_union_full_single_side',
        notional_usd_per_leg=10.0,
        confidence_epsilon=1e-12,
        initial_capital_usd=None,
        sample_count_x1=int(long_log2.shape[0]),
        val_bars_per_day=8640.0,
        enforce_cash_constraint=False,
    )
    backtest = stack['backtest']
    return {
        'source': 'deploy_val_npz_doc290',
        'sample_count': int(long_log2.shape[0]),
        'trade_count': int(backtest['trade_count']),
        'net_pnl_sum_linear': float(backtest['net_pnl_sum']),
        'net_pnl_sum_pct': float(backtest['net_pnl_sum']) * 100.0,
        'long_count': int(backtest['long_count']),
        'short_count': int(backtest['short_count']),
        'entry_grid_steps': 48,
        'round_trip_fee_rate': 0.001,
        'note': 'CSV ds1 val NPZ dense bars, same as 290',
    }


def audit(symbol_id: str, eval_horizon: str, include_deploy_reference: bool) -> int:
    resolved = resolve_trade_research_artifact(
        symbol_id=symbol_id,
        eval_horizon=eval_horizon,
    )
    if resolved is None:
        print(f'No artifact for {symbol_id} @ {eval_horizon}', file=sys.stderr)
        return 1

    npz_path, meta = resolved
    npz_data = np.load(npz_path, allow_pickle=True)
    store = TradeResearchNpzStore(npz_data=npz_data)
    deploy_stack = deploy_stack_from_meta(meta)
    if deploy_stack is None:
        print('NPZ meta missing deploy stack (range_interval confidence fixed_h)', file=sys.stderr)
        return 1

    artifact_horizon = str(npz_data['eval_horizon'][0])
    horizon_steps = horizon_steps_from_name(artifact_horizon)
    dataset_length = int(npz_data['dataset_length'][0])
    max_sample_index = dataset_length - 1 - horizon_steps
    pnl_stride = int(npz_data['pnl_stride'][0])
    mapped_pnl = [
        si
        for si in _sample_indices_for_pnl_backtest(
            max_sample_index=max_sample_index,
            stride=pnl_stride,
        )
        if store.row_for_sample(si) is not None
    ]

    entry_close = npz_data['entry_close'].astype(np.float64)
    pred_long = npz_data[f'pred_{artifact_horizon}'].astype(np.float64)
    from main.web_gui.trade_research_paired_pred_common import npz_pred_range_min_key

    range_min_key = npz_pred_range_min_key(artifact_horizon)
    pred_short = npz_data[range_min_key].astype(np.float64)
    exit_start_trade_id = npz_data['exit_start_trade_id'].astype(np.int64)
    real_last: int | None = None
    if 'real_last_start_trade_id' in npz_data.files:
        real_last = int(npz_data['real_last_start_trade_id'][0])

    splits: list[tuple[str, object]] = [
        ('tr_npz_all_history', None),
        (
            'tr_npz_gui_val_tail',
            lambda st, ri: st.row_matches_split(ri, 'val'),
        ),
        (
            'tr_npz_csv_ds1_val_train_sample_index',
            _row_in_csv_ds1_val,
        ),
    ]

    results: list[dict[str, object]] = []
    for label, predicate in splits:
        results.append(
            _overlap_metrics(
                store=store,
                mapped_pnl=mapped_pnl,
                max_sample_index=max_sample_index,
                entry_close=entry_close,
                pred_long=pred_long,
                pred_short=pred_short,
                deploy_stack=deploy_stack,
                exit_start_trade_id=exit_start_trade_id,
                real_last_start_trade_id=real_last,
                split_label=label,
                candidate_filter=predicate,
            ),
        )

    report: dict[str, object] = {
        'symbol_id': symbol_id,
        'eval_horizon': eval_horizon,
        'npz_path': str(npz_path),
        'pnl_stride': pnl_stride,
        'entry_grid_steps': int(settings.WEB_GUI_TRADING_ENTRY_GRID_STEPS),
        'train_size_okx': store.train_size,
        'csv_ds1_val_train_sample_index_range': [
            CSV_DS1_VAL_TRAIN_SAMPLE_INDEX_START,
            CSV_DS1_VAL_TRAIN_SAMPLE_INDEX_END,
        ],
        'trade_research_overlap_splits': results,
    }

    if include_deploy_reference:
        okx_root = Path(__file__).resolve().parents[1]
        tb_root = okx_root.parent / 'trading_bot'
        if not (tb_root / 'main_v3.py').is_file():
            tb_root = Path('/mnt/hdd2/Repositories/trading_bot')
        report['deploy_val_reference'] = _run_deploy_npz_reference(str(tb_root))

    print(json.dumps(report, indent=2))
    return 0


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Deploy overlap PnL: TR NPZ splits vs CSV ds1 val reference',
    )
    parser.add_argument('symbol_id', type=str)
    parser.add_argument('eval_horizon', type=str)
    parser.add_argument(
        '--no-deploy-reference',
        action='store_true',
        help='Skip trading_bot deploy val NPZ (doc 290) reference row',
    )
    return parser.parse_args()


if __name__ == '__main__':
    from pathlib import Path

    apply_runtime_limits()
    args = parse_arguments()
    raise SystemExit(
        audit(
            symbol_id=args.symbol_id,
            eval_horizon=args.eval_horizon,
            include_deploy_reference=not args.no_deploy_reference,
        ),
    )
