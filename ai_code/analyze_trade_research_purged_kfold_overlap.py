"""
Deploy overlap PnL per purged walk-forward val window (train_sample_index).

  python3 -m ai_code.analyze_trade_research_purged_kfold_overlap BTC_USDT x2048 \\
    --fold-manifest /mnt/hdd2/Repositories/trading_bot/docs/auto_ml/291_reports/purged_kfold_folds_ds1_w131072_s131072.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

from main.offline_inference.artifacts import resolve_trade_research_artifact
from main.offline_inference.trade_research_deploy_backtest_common import (
    collect_overlap_deploy_trade_records_from_npz,
    deploy_stack_from_meta,
)
from main.offline_inference.trade_research_npz_store import TradeResearchNpzStore
from main.runtime_limits import apply_runtime_limits
from main.web_gui.trade_research_paired_pred_common import npz_pred_range_min_key
from main.web_gui.trade_research_service import (
    _sample_indices_for_pnl_backtest,
    horizon_steps_from_name,
    summarize_trade_pnls,
)
from settings import settings


def _load_fold_manifest(manifest_path: Path) -> list[dict[str, int]]:
    payload = json.loads(manifest_path.read_text(encoding='utf-8'))
    if 'folds' not in payload:
        raise RuntimeError(f'{manifest_path}: missing folds')
    folds_raw = payload['folds']
    if not isinstance(folds_raw, list):
        raise RuntimeError(f'{manifest_path}: folds must be a list')
    folds: list[dict[str, int]] = []
    for item in folds_raw:
        if not isinstance(item, dict):
            raise RuntimeError(f'{manifest_path}: invalid fold entry')
        folds.append(
            {
                'fold_index': int(item['fold_index']),
                'val_start_index': int(item['val_start_index']),
                'val_end_index': int(item['val_end_index']),
            },
        )
    return folds


def _row_in_val_window(
    store: TradeResearchNpzStore,
    row_index: int,
    val_start_index: int,
    val_end_index: int,
) -> bool:
    if store._train_sample_index is None:
        return False
    tsi = int(store._train_sample_index[row_index])
    if tsi < 0:
        return False
    return tsi >= val_start_index and tsi < val_end_index


def analyze(
    symbol_id: str,
    eval_horizon: str,
    fold_manifest_path: Path,
) -> dict[str, object]:
    folds = _load_fold_manifest(fold_manifest_path)
    resolved = resolve_trade_research_artifact(
        symbol_id=symbol_id,
        eval_horizon=eval_horizon,
    )
    if resolved is None:
        raise RuntimeError(f'No artifact for {symbol_id} @ {eval_horizon}')

    npz_path, meta = resolved
    npz_data = np.load(npz_path, allow_pickle=True)
    store = TradeResearchNpzStore(npz_data=npz_data)
    deploy_stack = deploy_stack_from_meta(meta)
    if deploy_stack is None:
        raise RuntimeError('NPZ missing deploy stack')

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
    range_min_key = npz_pred_range_min_key(artifact_horizon)
    pred_short = npz_data[range_min_key].astype(np.float64)
    exit_start_trade_id = npz_data['exit_start_trade_id'].astype(np.int64)
    entry_start_trade_id = npz_data['entry_start_trade_id'].astype(np.int64)
    entry_timestamp_ms = npz_data['entry_timestamp_ms'].astype(np.int64)
    real_last: int | None = None
    if 'real_last_start_trade_id' in npz_data.files:
        real_last = int(npz_data['real_last_start_trade_id'][0])

    fold_rows: list[dict[str, object]] = []
    for fold in folds:
        val_start = fold['val_start_index']
        val_end = fold['val_end_index']
        candidates: list[int] = []
        for sample_index in mapped_pnl:
            row_index = store.row_for_sample(sample_index)
            if row_index is None:
                continue
            if _row_in_val_window(
                store=store,
                row_index=row_index,
                val_start_index=val_start,
                val_end_index=val_end,
            ):
                candidates.append(sample_index)

        records = collect_overlap_deploy_trade_records_from_npz(
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
            real_last_start_trade_id=real_last,
            entry_start_trade_id=entry_start_trade_id,
            entry_timestamp_ms=entry_timestamp_ms,
        )
        pnls = [float(r['weighted_pnl_linear']) for r in records]
        summary = summarize_trade_pnls(pnls)
        long_count = 0
        short_count = 0
        for record in records:
            if record['side'] == 'long':
                long_count = long_count + 1
            elif record['side'] == 'short':
                short_count = short_count + 1
        time_min = None
        time_max = None
        if len(records) > 0:
            times = [int(r['entry_timestamp_ms']) for r in records if 'entry_timestamp_ms' in r]
            if len(times) > 0:
                from datetime import datetime, timezone

                time_min = datetime.fromtimestamp(
                    min(times) / 1000.0,
                    tz=timezone.utc,
                ).strftime('%Y-%m-%d')
                time_max = datetime.fromtimestamp(
                    max(times) / 1000.0,
                    tz=timezone.utc,
                ).strftime('%Y-%m-%d')
        fold_rows.append(
            {
                'fold_index': fold['fold_index'],
                'val_train_sample_index_range': [val_start, val_end],
                'pnl_candidate_bars': len(candidates),
                'trade_count': int(summary['trade_count']),
                'net_pnl_sum_linear': float(summary['net_pnl_sum']),
                'net_pnl_sum_pct': float(summary['net_pnl_sum']) * 100.0,
                'entry_long_count': long_count,
                'entry_short_count': short_count,
                'entry_time_utc_min': time_min,
                'entry_time_utc_max': time_max,
            },
        )

    net_sum = 0.0
    for row in fold_rows:
        net_sum = net_sum + float(row['net_pnl_sum_linear'])

    return {
        'symbol_id': symbol_id,
        'eval_horizon': artifact_horizon,
        'npz_path': str(npz_path),
        'fold_manifest_path': str(fold_manifest_path),
        'entry_grid_steps': int(settings.WEB_GUI_TRADING_ENTRY_GRID_STEPS),
        'aggregate_net_pnl_sum_linear': net_sum,
        'aggregate_net_pnl_sum_pct': net_sum * 100.0,
        'folds': fold_rows,
    }


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Purged walk-forward val windows: deploy overlap PnL per fold',
    )
    parser.add_argument('symbol_id', type=str)
    parser.add_argument('eval_horizon', type=str)
    parser.add_argument(
        '--fold-manifest',
        type=Path,
        required=True,
    )
    parser.add_argument('--output-json', type=Path, default='')
    return parser.parse_args()


def main() -> int:
    apply_runtime_limits()
    args = parse_arguments()
    report = analyze(
        symbol_id=args.symbol_id,
        eval_horizon=args.eval_horizon,
        fold_manifest_path=args.fold_manifest,
    )
    text = json.dumps(report, indent=2)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(text + '\n', encoding='utf-8')
    print(text)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
