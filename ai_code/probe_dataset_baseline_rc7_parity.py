"""
Compare train HybridTradeDataset: baseline (fccda8a) vs rc7 (targets-after-trim).

Usage:
  cd okx_data_saver && source .venv/bin/activate
  python3 -m ai_code.probe_dataset_baseline_rc7_parity [--bars 400000] [--samples 32]
"""

from __future__ import annotations

import argparse
import logging
import pickle
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

import numpy as np
import polars

from enumerations import SymbolId
from main.web_gui.data_service import fetch_last_bars_sync
from main.web_gui.inference_service import fetch_inference_metadata

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
BASELINE_PACKAGE_DIR = REPO_ROOT / 'trading_bot_dataset_baseline'
RC7_PACKAGE_DIR = REPO_ROOT / 'trading_bot_dataset'
WORKER_MODULE = 'ai_code.probe_dataset_baseline_rc7_worker'


def _run_worker(
    package_dir: Path,
    df_parquet: Path,
    metadata_pickle: Path,
    out_pickle: Path,
    detail_samples: int,
    detail_trade_ids_file: Path | None,
) -> None:
    command = [
        sys.executable,
        '-m',
        WORKER_MODULE,
        '--package-dir',
        str(package_dir),
        '--df-parquet',
        str(df_parquet),
        '--metadata-pickle',
        str(metadata_pickle),
        '--out-pickle',
        str(out_pickle),
        '--detail-samples',
        str(detail_samples),
    ]
    if detail_trade_ids_file is not None:
        command.extend(
            [
                '--detail-trade-ids-file',
                str(detail_trade_ids_file),
            ],
        )
    subprocess.run(command, check=True, cwd=str(REPO_ROOT))


def _compare_tensors(left: np.ndarray, right: np.ndarray) -> dict[str, object]:
    if left.shape != right.shape:
        return {
            'shape_match': False,
            'left_shape': left.shape,
            'right_shape': right.shape,
            'max_abs_diff': None,
            'exact_match': False,
        }
    if left.size == 0 and right.size == 0:
        return {
            'shape_match': True,
            'max_abs_diff': 0.0,
            'mean_abs_diff': 0.0,
            'exact_match': True,
        }
    diff = np.abs(left.astype(np.float64) - right.astype(np.float64))
    max_diff = float(np.max(diff))
    return {
        'shape_match': True,
        'max_abs_diff': max_diff,
        'mean_abs_diff': float(np.mean(diff)),
        'exact_match': bool(max_diff == 0.0),
    }


def _trade_id_maps(
    sample_trade_ids: list[tuple[int, int]],
) -> tuple[dict[int, int], dict[int, int]]:
    sample_by_trade_id: dict[int, int] = {}
    trade_id_by_sample: dict[int, int] = {}
    for sample_index, start_trade_id in sample_trade_ids:
        if start_trade_id in sample_by_trade_id:
            raise RuntimeError(
                f'duplicate start_trade_id in dataset mapping: {start_trade_id}',
            )
        sample_by_trade_id[start_trade_id] = sample_index
        trade_id_by_sample[sample_index] = start_trade_id
    return sample_by_trade_id, trade_id_by_sample


def _print_geometry(baseline: dict[str, object], rc7: dict[str, object]) -> None:
    print('\n--- dataset geometry ---')
    print(
        f'start_index: baseline={baseline["start_index"]} rc7={rc7["start_index"]} '
        f'delta={int(rc7["start_index"]) - int(baseline["start_index"])}',
    )
    print(
        f'dataset_length: baseline={baseline["dataset_length"]} rc7={rc7["dataset_length"]} '
        f'delta={int(rc7["dataset_length"]) - int(baseline["dataset_length"])}',
    )
    print(
        f'level0_rows: baseline={baseline["level0_rows"]} rc7={rc7["level0_rows"]} '
        f'delta={int(rc7["level0_rows"]) - int(baseline["level0_rows"])}',
    )


def _print_sample_index_alignment(
    baseline: dict[str, object],
    rc7: dict[str, object],
    overlap_trade_ids: list[int],
) -> None:
    baseline_map, _baseline_by_sample = _trade_id_maps(baseline['sample_trade_ids'])
    rc7_map, _rc7_by_sample = _trade_id_maps(rc7['sample_trade_ids'])

    sample_shifts: list[int] = []
    for start_trade_id in overlap_trade_ids:
        sample_shifts.append(rc7_map[start_trade_id] - baseline_map[start_trade_id])

    unique_shifts = sorted(set(sample_shifts))
    print('\n--- sample_index alignment (by start_trade_id) ---')
    print(f'overlap samples: {len(overlap_trade_ids)}')
    print(f'unique sample_index shifts (rc7 - baseline): {unique_shifts[:20]}'
          + (' ...' if len(unique_shifts) > 20 else ''))
    if len(unique_shifts) == 1:
        print(f'all overlapping samples shifted by: {unique_shifts[0]}')
    else:
        shift_counts: dict[int, int] = {}
        for shift in sample_shifts:
            shift_counts[shift] = shift_counts[shift] + 1
        print('shift histogram (top 10):')
        for shift, count in sorted(
            shift_counts.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:10]:
            print(f'  shift {shift:+d}: {count} samples')


def _print_tensor_parity_table(
    baseline: dict[str, object],
    rc7: dict[str, object],
) -> None:
    baseline_map, _ = _trade_id_maps(baseline['sample_trade_ids'])
    rc7_map, _ = _trade_id_maps(rc7['sample_trade_ids'])
    scale_features = list(baseline['scale_features'])

    rows: list[dict[str, object]] = []
    for start_trade_id in sorted(set(baseline_map.keys()) & set(rc7_map.keys())):
        baseline_sample = baseline_map[start_trade_id]
        rc7_sample = rc7_map[start_trade_id]
        if start_trade_id not in baseline['detail_trade_ids']:
            continue
        if baseline_sample not in baseline['detail_tensors']:
            continue
        if rc7_sample not in rc7['detail_tensors']:
            continue

        baseline_tensors = baseline['detail_tensors'][baseline_sample]
        rc7_tensors = rc7['detail_tensors'][rc7_sample]

        row: dict[str, object] = {
            'start_trade_id': start_trade_id,
            'baseline_sample': baseline_sample,
            'rc7_sample': rc7_sample,
            'sample_shift': rc7_sample - baseline_sample,
        }
        for scale_name in scale_features:
            scale_diff = _compare_tensors(
                baseline_tensors['x_seq'][scale_name],
                rc7_tensors['x_seq'][scale_name],
            )
            row[f'{scale_name}_max_diff'] = scale_diff['max_abs_diff']
            row[f'{scale_name}_ok'] = scale_diff['exact_match']
        target_diff = _compare_tensors(
            baseline_tensors['targets'],
            rc7_tensors['targets'],
        )
        row['targets_max_diff'] = target_diff['max_abs_diff']
        row['targets_ok'] = target_diff['exact_match']
        rows.append(row)

    if not rows:
        print('\n--- tensor parity ---')
        print('no detail samples in overlap (increase --samples)')
        return

    print('\n--- tensor parity (detail samples with shared start_trade_id) ---')
    header = (
        'start_trade_id',
        'b_idx',
        'r_idx',
        'shift',
        'x1_ok',
        'x1_max',
        'x1536_ok',
        'x1536_max',
        'tgt_ok',
        'tgt_max',
    )
    print(' | '.join(header))
    print('-' * 100)
    for row in rows:
        x1_ok = row['x1_ok'] if 'x1_ok' in row else '?'
        x1536_ok = row['x1536_ok'] if 'x1536_ok' in row else '?'
        x1_max = row['x1_max_diff'] if 'x1_max_diff' in row else None
        x1536_max = row['x1536_max_diff'] if 'x1536_max_diff' in row else None
        print(
            f'{row["start_trade_id"]} | {row["baseline_sample"]} | {row["rc7_sample"]} | '
            f'{row["sample_shift"]:+d} | {x1_ok} | {x1_max} | {x1536_ok} | {x1536_max} | '
            f'{row["targets_ok"]} | {row["targets_max_diff"]}',
        )

    x1_exact = sum(1 for row in rows if row['x1_ok'] is True)
    x1536_exact = sum(1 for row in rows if row['x1536_ok'] is True) if 'x1536_ok' in rows[0] else 0
    tgt_exact = sum(1 for row in rows if row['targets_ok'] is True)
    print(
        f'\nexact match counts: x1={x1_exact}/{len(rows)} '
        f'x1536={x1536_exact}/{len(rows)} targets={tgt_exact}/{len(rows)}',
    )

    same_idx_rows = [row for row in rows if row['sample_shift'] == 0]
    if same_idx_rows:
        same_idx_x1 = sum(1 for row in same_idx_rows if row['x1_ok'] is True)
        print(
            f'same sample_index (shift=0): {len(same_idx_rows)} samples, '
            f'x1 exact={same_idx_x1}/{len(same_idx_rows)}',
        )


def _print_same_sample_index_mismatch(
    baseline: dict[str, object],
    rc7: dict[str, object],
) -> None:
    overlap_length = min(int(baseline['dataset_length']), int(rc7['dataset_length']))
    if overlap_length <= 0:
        return

    baseline_ids = [trade_id for _idx, trade_id in baseline['sample_trade_ids'][:overlap_length]]
    rc7_ids = [trade_id for _idx, trade_id in rc7['sample_trade_ids'][:overlap_length]]
    mismatches = [
        (sample_index, baseline_ids[sample_index], rc7_ids[sample_index])
        for sample_index in range(overlap_length)
        if baseline_ids[sample_index] != rc7_ids[sample_index]
    ]
    print('\n--- same sample_index start_trade_id ---')
    print(f'compared sample indices: 0..{overlap_length - 1}')
    print(f'mismatched mappings: {len(mismatches)}')
    if mismatches:
        print('first 10 mismatches (sample_idx, baseline_id, rc7_id):')
        for sample_index, baseline_id, rc7_id in mismatches[:10]:
            print(f'  {sample_index}: {baseline_id} vs {rc7_id}')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--bars', type=int, default=400_000)
    parser.add_argument('--samples', type=int, default=32)
    parser.add_argument('--symbol', type=str, default='BTC_USDT')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    if not BASELINE_PACKAGE_DIR.is_dir():
        raise RuntimeError(
            f'Missing baseline worktree: {BASELINE_PACKAGE_DIR}. '
            'Run: git -C trading_bot_dataset worktree add ../trading_bot_dataset_baseline fccda8a',
        )
    if not RC7_PACKAGE_DIR.is_dir():
        raise RuntimeError(f'Missing rc7 package dir: {RC7_PACKAGE_DIR}')

    metadata = fetch_inference_metadata()
    sequence_length = int(metadata['sequence_length'])
    max_scale = int(metadata['max_scale'])
    required_rows = sequence_length * max_scale
    bars_limit = max(args.bars, required_rows + 10_000)

    symbol = SymbolId[args.symbol]
    raw_df = fetch_last_bars_sync(symbol_id=symbol, limit=bars_limit, offset=0, since_start_trade_id=None)
    if raw_df is None:
        raise RuntimeError('Failed to fetch bars')

    print('=' * 72)
    print('DATASET PARITY: baseline fccda8a vs rc7 targets-after-trim')
    print('=' * 72)
    print(f'raw bars: {raw_df.height} (limit={bars_limit})')
    print(f'baseline package: {BASELINE_PACKAGE_DIR}')
    print(f'rc7 package: {RC7_PACKAGE_DIR}')

    with tempfile.TemporaryDirectory(prefix='dataset_parity_') as temp_dir:
        temp_path = Path(temp_dir)
        df_parquet = temp_path / 'raw.parquet'
        metadata_pickle = temp_path / 'metadata.pkl'
        baseline_pickle = temp_path / 'baseline.pkl'
        rc7_pickle = temp_path / 'rc7.pkl'

        raw_df.write_parquet(df_parquet)
        with open(metadata_pickle, 'wb') as metadata_file:
            pickle.dump(metadata, metadata_file)

        print('\nPhase 1: geometry + sample mapping...')
        _run_worker(
            package_dir=BASELINE_PACKAGE_DIR,
            df_parquet=df_parquet,
            metadata_pickle=metadata_pickle,
            out_pickle=baseline_pickle,
            detail_samples=0,
            detail_trade_ids_file=None,
        )
        _run_worker(
            package_dir=RC7_PACKAGE_DIR,
            df_parquet=df_parquet,
            metadata_pickle=metadata_pickle,
            out_pickle=rc7_pickle,
            detail_samples=0,
            detail_trade_ids_file=None,
        )

        with open(baseline_pickle, 'rb') as baseline_file:
            baseline = pickle.load(baseline_file)
        with open(rc7_pickle, 'rb') as rc7_file:
            rc7 = pickle.load(rc7_file)

        baseline_map, _ = _trade_id_maps(baseline['sample_trade_ids'])
        rc7_map, _ = _trade_id_maps(rc7['sample_trade_ids'])
        overlap_trade_ids = sorted(set(baseline_map.keys()) & set(rc7_map.keys()))
        if args.samples > 0 and overlap_trade_ids:
            if args.samples >= len(overlap_trade_ids):
                detail_trade_ids = overlap_trade_ids
            else:
                step = max(len(overlap_trade_ids) // args.samples, 1)
                detail_trade_ids = overlap_trade_ids[::step][: args.samples]
        else:
            detail_trade_ids = []

        detail_trade_ids_file = temp_path / 'detail_trade_ids.txt'
        detail_trade_ids_file.write_text(
            '\n'.join(str(trade_id) for trade_id in detail_trade_ids),
            encoding='utf-8',
        )

        print(f'\nPhase 2: tensor dump for {len(detail_trade_ids)} shared start_trade_id(s)...')
        _run_worker(
            package_dir=BASELINE_PACKAGE_DIR,
            df_parquet=df_parquet,
            metadata_pickle=metadata_pickle,
            out_pickle=baseline_pickle,
            detail_samples=0,
            detail_trade_ids_file=detail_trade_ids_file,
        )
        _run_worker(
            package_dir=RC7_PACKAGE_DIR,
            df_parquet=df_parquet,
            metadata_pickle=metadata_pickle,
            out_pickle=rc7_pickle,
            detail_samples=0,
            detail_trade_ids_file=detail_trade_ids_file,
        )

        with open(baseline_pickle, 'rb') as baseline_file:
            baseline = pickle.load(baseline_file)
        with open(rc7_pickle, 'rb') as rc7_file:
            rc7 = pickle.load(rc7_file)

    _print_geometry(baseline, rc7)
    _print_same_sample_index_mismatch(baseline, rc7)

    baseline_map, _ = _trade_id_maps(baseline['sample_trade_ids'])
    rc7_map, _ = _trade_id_maps(rc7['sample_trade_ids'])
    overlap_trade_ids = sorted(set(baseline_map.keys()) & set(rc7_map.keys()))
    _print_sample_index_alignment(baseline, rc7, overlap_trade_ids)
    _print_tensor_parity_table(baseline, rc7)

    print('\n--- summary ---')
    if int(baseline['start_index']) == int(rc7['start_index']) and len(overlap_trade_ids) > 0:
        shift_values = sorted(
            {
                rc7_map[tid] - baseline_map[tid]
                for tid in overlap_trade_ids
            },
        )
        if len(shift_values) == 1 and shift_values[0] == 0:
            print('PASS candidate: constant zero shift on overlap; check tensor table for exact x_seq.')
        elif len(shift_values) == 1:
            print(
                f'ALIGN shift: all overlap samples shifted by {shift_values[0]} '
                '(same raw bar, different sample_idx)',
            )
        else:
            print('FAIL: non-uniform sample_index shift — trim anchor likely differs.')
            print(f'  unique shifts (first 10): {shift_values[:10]}')
    else:
        print('FAIL: start_index mismatch or empty overlap.')


if __name__ == '__main__':
    try:
        main()
    except Exception as exception:
        logger.error('probe failed: %s', ''.join(traceback.format_exception(exception)))
        raise
