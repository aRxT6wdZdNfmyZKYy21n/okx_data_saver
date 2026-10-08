"""
Deploy overlap PnL on val — chronological sub-period breakdown.

Usage:
  cd okx_data_saver && source .venv/bin/activate
  python3 -m ai_code.analyze_trade_research_deploy_val_subperiods BTC_USDT x2048
  python3 -m ai_code.analyze_trade_research_deploy_val_subperiods BTC_USDT x2048 --bins 10 --output-json /tmp/val_subperiods.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

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


def _filter_candidates(
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


def _ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime('%Y-%m-%d')


def _bin_trades_chronologically(
    records: list[dict[str, float | int | str]],
    bin_count: int,
) -> list[dict[str, object]]:
    if len(records) == 0:
        return []
    sorted_records = sorted(
        records,
        key=lambda record: (
            int(record['entry_start_trade_id'])
            if 'entry_start_trade_id' in record
            else int(record['entry_sample_index']),
        ),
    )
    chunks = np.array_split(np.arange(len(sorted_records)), bin_count)
    bins_out: list[dict[str, object]] = []
    cumulative_before = 0.0
    for bin_index, index_array in enumerate(chunks):
        if index_array.shape[0] == 0:
            continue
        slice_records = [sorted_records[int(i)] for i in index_array]
        pnls = [float(r['weighted_pnl_linear']) for r in slice_records]
        summary = summarize_trade_pnls(pnls)
        long_count = 0
        short_count = 0
        for record in slice_records:
            if record['side'] == 'long':
                long_count = long_count + 1
            elif record['side'] == 'short':
                short_count = short_count + 1
        first = slice_records[0]
        last = slice_records[-1]
        period_net = float(summary['net_pnl_sum'])
        cumulative_after = cumulative_before + period_net
        bin_payload: dict[str, object] = {
            'bin_index': bin_index,
            'bin_label': f'{bin_index + 1}/{bin_count}',
            'trade_count': int(summary['trade_count']),
            'net_pnl_linear': period_net,
            'net_pnl_pct': period_net * 100.0,
            'avg_trade_pnl_linear': float(summary['avg_trade_pnl']),
            'cumulative_pnl_linear_before': cumulative_before,
            'cumulative_pnl_linear_after': cumulative_after,
            'entry_long_count': long_count,
            'entry_short_count': short_count,
            'short_fraction': short_count / max(1, long_count + short_count),
        }
        if 'entry_start_trade_id' in first:
            bin_payload['entry_start_trade_id_min'] = int(first['entry_start_trade_id'])
            bin_payload['entry_start_trade_id_max'] = int(last['entry_start_trade_id'])
        if 'entry_timestamp_ms' in first and 'entry_timestamp_ms' in last:
            t0 = int(first['entry_timestamp_ms'])
            t1 = int(last['entry_timestamp_ms'])
            bin_payload['entry_time_utc_min'] = _ms_to_iso(t0)
            bin_payload['entry_time_utc_max'] = _ms_to_iso(t1)
        if 'train_sample_index' in first and 'train_sample_index' in last:
            bin_payload['train_sample_index_min'] = int(first['train_sample_index'])
            bin_payload['train_sample_index_max'] = int(last['train_sample_index'])
        bins_out.append(bin_payload)
        cumulative_before = cumulative_after
    return bins_out


def _drawdown_summary(bins: list[dict[str, object]]) -> dict[str, object]:
    if len(bins) == 0:
        return {}
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    max_drawdown_bin: int | None = None
    first_negative_bin: int | None = None
    worst_bin_index: int | None = None
    worst_bin_pnl = float('inf')
    for bin_row in bins:
        bin_index = int(bin_row['bin_index'])
        period_net = float(bin_row['net_pnl_linear'])
        if period_net < worst_bin_pnl:
            worst_bin_pnl = period_net
            worst_bin_index = bin_index
        cumulative = cumulative + period_net
        if cumulative < 0.0 and first_negative_bin is None:
            first_negative_bin = bin_index
        if cumulative > peak:
            peak = cumulative
        drawdown = peak - cumulative
        if drawdown > max_drawdown:
            max_drawdown = drawdown
            max_drawdown_bin = bin_index
    return {
        'worst_period_bin_index': worst_bin_index,
        'worst_period_net_pnl_linear': worst_bin_pnl if worst_bin_index is not None else None,
        'max_drawdown_linear': max_drawdown,
        'max_drawdown_bin_index': max_drawdown_bin,
        'first_cumulative_negative_bin_index': first_negative_bin,
        'final_cumulative_linear': float(bins[-1]['cumulative_pnl_linear_after']),
    }


def _analyze_val_slice(
    slice_label: str,
    store: TradeResearchNpzStore,
    candidates: list[int],
    max_sample_index: int,
    entry_close: np.ndarray,
    pred_long: np.ndarray,
    pred_short: np.ndarray,
    deploy_stack: object,
    exit_start_trade_id: np.ndarray,
    entry_start_trade_id: np.ndarray,
    entry_timestamp_ms: np.ndarray,
    real_last_start_trade_id: int | None,
    bin_count: int,
) -> dict[str, object]:
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
        real_last_start_trade_id=real_last_start_trade_id,
        entry_start_trade_id=entry_start_trade_id,
        entry_timestamp_ms=entry_timestamp_ms,
    )
    pnls = [float(r['weighted_pnl_linear']) for r in records]
    overall = summarize_trade_pnls(pnls)
    bins = _bin_trades_chronologically(records=records, bin_count=bin_count)
    return {
        'slice_label': slice_label,
        'trade_count': int(overall['trade_count']),
        'net_pnl_sum_linear': float(overall['net_pnl_sum']),
        'net_pnl_sum_pct': float(overall['net_pnl_sum']) * 100.0,
        'chronological_bins': bins,
        'drawdown_summary': _drawdown_summary(bins),
    }


def analyze(
    symbol_id: str,
    eval_horizon: str,
    bin_count: int,
) -> dict[str, object]:
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

    slices: list[tuple[str, object]] = [
        (
            'gui_val_okx_train_size_tail',
            lambda st, ri: st.row_matches_split(ri, 'val'),
        ),
        (
            'csv_ds1_val_train_sample_index',
            _row_in_csv_ds1_val,
        ),
    ]

    slice_reports: list[dict[str, object]] = []
    for label, predicate in slices:
        candidates = _filter_candidates(
            store=store,
            mapped_pnl=mapped_pnl,
            row_predicate=predicate,
        )
        slice_reports.append(
            _analyze_val_slice(
                slice_label=label,
                store=store,
                candidates=candidates,
                max_sample_index=max_sample_index,
                entry_close=entry_close,
                pred_long=pred_long,
                pred_short=pred_short,
                deploy_stack=deploy_stack,
                exit_start_trade_id=exit_start_trade_id,
                entry_start_trade_id=entry_start_trade_id,
                entry_timestamp_ms=entry_timestamp_ms,
                real_last_start_trade_id=real_last,
                bin_count=bin_count,
            ),
        )

    return {
        'symbol_id': symbol_id,
        'eval_horizon': artifact_horizon,
        'npz_path': str(npz_path),
        'bin_count': bin_count,
        'entry_grid_steps': int(settings.WEB_GUI_TRADING_ENTRY_GRID_STEPS),
        'pnl_stride': pnl_stride,
        'train_size_okx': store.train_size,
        'val_slices': slice_reports,
    }


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Chronological sub-period PnL on trade research val slices',
    )
    parser.add_argument('symbol_id', type=str)
    parser.add_argument('eval_horizon', type=str)
    parser.add_argument('--bins', type=int, default=8)
    parser.add_argument('--output-json', type=str, default='')
    return parser.parse_args()


def main() -> int:
    apply_runtime_limits()
    args = parse_arguments()
    if args.bins < 2:
        print('--bins must be >= 2', file=sys.stderr)
        return 1
    report = analyze(
        symbol_id=args.symbol_id,
        eval_horizon=args.eval_horizon,
        bin_count=args.bins,
    )
    text = json.dumps(report, indent=2)
    if args.output_json:
        with open(args.output_json, 'w', encoding='utf-8') as handle:
            handle.write(text)
            handle.write('\n')
    print(text)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
