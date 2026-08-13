"""
Compare HybridTradeDataset[idx] with vs without forward-target raw-x1 padding.

Usage:
  cd okx_data_saver && source .venv/bin/activate
  python3 -m ai_code.compare_padded_unpadded_dataset [--bars 400000] [--samples 32]
"""

from __future__ import annotations

import argparse
import logging
import sys
import traceback

import numpy as np
import polars
import torch
from omegaconf import OmegaConf

from enumerations import SymbolId
from main.web_gui.data_service import fetch_last_bars_sync
from main.web_gui.inference_service import (
    _build_level0_to_raw_row_indices_from_dataset,
    _build_train_dataset,
    fetch_inference_metadata,
)
from main.web_gui.trade_research_dataset_common import (
    TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS,
    append_forward_target_padding,
    last_real_sample_index,
)

logger = logging.getLogger(__name__)


def _tensor_stats(name: str, tensor: torch.Tensor) -> dict[str, float | int | tuple[int, ...]]:
    arr = tensor.detach().cpu().numpy().astype(np.float64)
    return {
        'name': name,
        'shape': tuple(tensor.shape),
        'min': float(np.min(arr)),
        'max': float(np.max(arr)),
        'mean': float(np.mean(arr)),
        'nan': int(np.isnan(arr).sum()),
        'inf': int(np.isinf(arr).sum()),
    }


def _compare_tensors(
    label: str,
    left: torch.Tensor,
    right: torch.Tensor,
) -> dict[str, object]:
    left_arr = left.detach().cpu().numpy().astype(np.float64)
    right_arr = right.detach().cpu().numpy().astype(np.float64)
    if left_arr.size == 0 and right_arr.size == 0:
        return {
            'label': label,
            'shape_match': True,
            'max_abs_diff': 0.0,
            'mean_abs_diff': 0.0,
            'exact_match': True,
        }
        return {
            'label': label,
            'shape_match': False,
            'left_shape': left_arr.shape,
            'right_shape': right_arr.shape,
        }
    diff = np.abs(left_arr - right_arr)
    return {
        'label': label,
        'shape_match': True,
        'max_abs_diff': float(np.max(diff)),
        'mean_abs_diff': float(np.mean(diff)),
        'exact_match': bool(np.max(diff) == 0.0),
    }


def _compare_sample(
    train_unpadded: object,
    train_padded: object,
    sample_index_unpadded: int,
    sample_index_padded: int,
    entry_start_trade_id: int,
) -> dict[str, object]:
    x_seq_u, x_static_u, targets_u = train_unpadded[sample_index_unpadded]
    x_seq_p, x_static_p, targets_p = train_padded[sample_index_padded]

    scale_diffs: dict[str, object] = {}
    for scale_name in x_seq_u:
        if scale_name not in x_seq_p:
            scale_diffs[scale_name] = {'error': 'missing in padded'}
            continue
        scale_diffs[scale_name] = _compare_tensors(
            scale_name,
            x_seq_u[scale_name],
            x_seq_p[scale_name],
        )

    return {
        'entry_start_trade_id': entry_start_trade_id,
        'sample_index_unpadded': sample_index_unpadded,
        'sample_index_padded': sample_index_padded,
        'x_static': _compare_tensors('x_static', x_static_u, x_static_p),
        'targets': _compare_tensors('targets', targets_u, targets_p),
        'targets_unpadded': float(targets_u.detach().cpu().numpy().reshape(-1)[0])
        if targets_u.numel() == 1
        else targets_u.detach().cpu().numpy().reshape(-1).tolist(),
        'targets_padded': float(targets_p.detach().cpu().numpy().reshape(-1)[0])
        if targets_p.numel() == 1
        else targets_p.detach().cpu().numpy().reshape(-1).tolist(),
        'x_seq': scale_diffs,
    }


def _build_train_from_df(df: polars.DataFrame, metadata: dict) -> object:
    return _build_train_dataset(df=df, metadata=metadata)


def _level0_start_trade_ids(
    train_dataset: object,
    level0_to_raw: list[int],
    raw_df: polars.DataFrame,
) -> list[int]:
    start_index = int(train_dataset.start_index)
    dataset_length = len(train_dataset)
    ids: list[int] = []
    for sample_index in range(dataset_length):
        entry_bar_index = start_index + sample_index
        raw_row = level0_to_raw[entry_bar_index]
        row = raw_df.row(int(raw_row), named=True)
        ids.append(int(row['start_trade_id']))
    return ids


def _find_sample_by_start_trade_id(
    start_trade_ids: list[int],
    target_id: int,
) -> int | None:
    matches = [idx for idx, sid in enumerate(start_trade_ids) if sid == target_id]
    if len(matches) == 0:
        return None
    if len(matches) > 1:
        raise RuntimeError(f'Duplicate start_trade_id {target_id}: {len(matches)} samples')
    return matches[0]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--bars', type=int, default=400_000)
    parser.add_argument('--samples', type=int, default=32)
    parser.add_argument('--symbol', type=str, default='BTC_USDT')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    metadata = fetch_inference_metadata()
    sequence_length = int(metadata['sequence_length'])
    max_scale = int(metadata['max_scale'])
    required_rows = sequence_length * max_scale
    bars_limit = max(args.bars, required_rows + TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS + 10_000)

    symbol = SymbolId[args.symbol]
    raw_df = fetch_last_bars_sync(symbol_id=symbol, limit=bars_limit, offset=0)
    if raw_df is None:
        raise RuntimeError('Failed to fetch bars')

    real_bar_count = int(raw_df.height)
    padded_df, _ = append_forward_target_padding(
        raw_df=raw_df,
        forward_bars=TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS,
    )

    print('=' * 72)
    print('DATASET BUILD COMPARISON: unpadded vs padded raw-x1')
    print('=' * 72)
    print(f'raw bars fetched: {real_bar_count}')
    print(f'padded bars: {int(padded_df.height)} (+{TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS})')
    print(f'required_rows (seq*max_scale): {required_rows}')

    print('\nBuilding unpadded train dataset...')
    train_unpadded = _build_train_from_df(raw_df, metadata)
    print('Building padded train dataset...')
    train_padded = _build_train_from_df(padded_df, metadata)

    level0_u_rows = train_unpadded.level0_row_count()
    level0_p_rows = train_padded.level0_row_count()
    print('\n--- dataset geometry ---')
    print(f'start_index: unpadded={train_unpadded.start_index} padded={train_padded.start_index}')
    print(f'dataset_length: unpadded={len(train_unpadded)} padded={len(train_padded)}')
    print(f'level0 rows: unpadded={level0_u_rows} padded={level0_p_rows}')
    print(f'static_cols: {train_unpadded.static_columns}')
    print(f'target_cols: {train_unpadded.target_cols}')
    print(f'level0 storage cols include targets? '
          f'{any(c.startswith("target_") for c in train_unpadded.level_storage_columns[0])}')
    print(f'level0 output cols include targets? '
          f'{any(c.startswith("target_") for c in train_unpadded.level_output_columns[0])}')

    l0_to_raw_u = _build_level0_to_raw_row_indices_from_dataset(raw_df, train_unpadded)
    l0_to_raw_p = _build_level0_to_raw_row_indices_from_dataset(padded_df, train_padded)

    last_u = last_real_sample_index(
        dataset_length=len(train_unpadded),
        start_index=int(train_unpadded.start_index),
        level0_to_raw=l0_to_raw_u,
        real_bar_count=real_bar_count,
    )
    last_p = last_real_sample_index(
        dataset_length=len(train_padded),
        start_index=int(train_padded.start_index),
        level0_to_raw=l0_to_raw_p,
        real_bar_count=real_bar_count,
    )
    print(f'last_real_sample_index: unpadded={last_u} padded={last_p}')

    ids_u = _level0_start_trade_ids(train_unpadded, l0_to_raw_u, raw_df)
    ids_p = _level0_start_trade_ids(train_padded, l0_to_raw_p, padded_df)

    common_ids = set(ids_u).intersection(ids_p)
    print(f'samples with same entry start_trade_id in both: {len(common_ids)}')

    # Pick comparison anchors: early/mid/late real samples
    compare_ids: list[int] = []
    padded_only_ids: list[int] = []
    for sample_index in range(len(ids_p)):
        sid = ids_p[sample_index]
        if sid not in common_ids:
            if len(padded_only_ids) < 5:
                padded_only_ids.append(sid)
            continue
        if sample_index == last_p:
            compare_ids.append(sid)
        elif sample_index == 0:
            compare_ids.append(sid)
        elif sample_index == len(ids_p) // 2:
            compare_ids.append(sid)

    stride = max(1, len(ids_p) // max(args.samples, 1))
    for sample_index in range(0, len(ids_p), stride):
        sid = ids_p[sample_index]
        if sid in common_ids and sid not in compare_ids:
            compare_ids.append(sid)
        if len(compare_ids) >= args.samples:
            break

    print(f'\n--- per-sample x_seq / targets diff ({len(compare_ids)} anchors) ---')
    mismatched_x_seq = 0
    mismatched_targets = 0
    for entry_id in compare_ids:
        idx_u = _find_sample_by_start_trade_id(ids_u, entry_id)
        idx_p = _find_sample_by_start_trade_id(ids_p, entry_id)
        if idx_u is None or idx_p is None:
            continue
        report = _compare_sample(
            train_unpadded=train_unpadded,
            train_padded=train_padded,
            sample_index_unpadded=idx_u,
            sample_index_padded=idx_p,
            entry_start_trade_id=entry_id,
        )
        x_seq_ok = all(
            scale_report.get('exact_match', False)
            for scale_report in report['x_seq'].values()
            if isinstance(scale_report, dict) and scale_report.get('shape_match', False)
        )
        targets_ok = report['targets'].get('exact_match', False)
        if not x_seq_ok:
            mismatched_x_seq += 1
        if not targets_ok:
            mismatched_targets += 1
        max_diffs = {
            scale: rep.get('max_abs_diff')
            for scale, rep in report['x_seq'].items()
            if isinstance(rep, dict) and 'max_abs_diff' in rep
        }
        print(
            f"entry_id={entry_id} idx_u={idx_u} idx_p={idx_p} "
            f"x_seq_match={x_seq_ok} targets_match={targets_ok} "
            f"target_u={report['targets_unpadded']} target_p={report['targets_padded']} "
            f"max_diff={max_diffs}",
        )

    print(f'\nx_seq mismatches: {mismatched_x_seq}/{len(compare_ids)}')
    print(f'target mismatches: {mismatched_targets}/{len(compare_ids)}')

    if padded_only_ids:
        print(f'\npadded-only sample entry ids (first few): {padded_only_ids}')

    # Tail target behavior on level0 raw rows near real end
    print('\n--- level0 target_x1536 near real tail (unpadded vs padded row alignment) ---')
    target_col = 'target_close_return_signed_log2_x1536'
    if train_unpadded.aggregated_data and 0 in train_unpadded.aggregated_data:
        level0_u = train_unpadded.aggregated_data[0]
        level0_p = train_padded.aggregated_data[0]
        if target_col in level0_u.columns and target_col in level0_p.columns:
            tail_rows = 5
            u_tail = level0_u.tail(tail_rows).select([target_col])
            p_tail = level0_p.tail(
                tail_rows + TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS // 1536 + 2,
            ).tail(tail_rows)
            print('unpadded last rows target:', u_tail[target_col].to_list())
            print('padded   last REAL-ish rows target (may include pad):', p_tail[target_col].to_list())
        else:
            print(f'skipped: {target_col} not in materialized level0 polars')
    else:
        print('skipped: level0 polars dropped after tensor materialization')

    # Check whether targets live in same tensor as static (should be empty static)
    print('\n--- model input separation ---')
    sample0_x_seq, sample0_static, sample0_targets = train_padded[0]
    print('x_static shape:', tuple(sample0_static.shape), '(empty static => model gets x_seq only)')
    print('targets shape:', tuple(sample0_targets.shape), 'NOT passed in inference payload x_static')
    for scale_name, tensor in sample0_x_seq.items():
        print(f'x_seq[{scale_name}] shape:', tuple(tensor.shape))


if __name__ == '__main__':
    try:
        main()
    except Exception as exception:
        logger.error('Failed: %s', ''.join(traceback.format_exception(exception)))
        sys.exit(1)
