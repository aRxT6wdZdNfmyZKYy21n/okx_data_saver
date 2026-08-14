"""
Live inference diff: same start_trade_id, unpadded vs padded dataset payloads.

Usage:
  cd okx_data_saver && source .venv/bin/activate
  python3 -m ai_code.compare_padded_unpadded_inference [--bars 400000] [--count 10]
"""

from __future__ import annotations

import argparse
import logging
import sys
import traceback

import numpy as np
import polars

from enumerations import SymbolId
from main.web_gui.data_service import fetch_last_bars_sync
from main.web_gui.inference_service import (
    _build_level0_to_raw_row_indices_from_dataset,
    _build_train_dataset,
    _prepare_payload_dict_from_train_sample,
    fetch_inference_metadata,
)
from main.web_gui.trade_research_dataset_common import append_forward_target_padding
from main.web_gui.trade_research_service import _call_inference_batch_api

logger = logging.getLogger(__name__)

PREDICTION_KEYS = [
    'target_close_return_signed_log2_x1536',
    'target_close_return_signed_log2_x1536_pred_long',
    'target_close_return_signed_log2_x1536_pred_short',
]


def _entry_start_trade_id(
    train_dataset: object,
    level0_to_raw: list[int],
    raw_df: polars.DataFrame,
    sample_index: int,
) -> int:
    actual_index = int(train_dataset.start_index) + sample_index
    raw_row = level0_to_raw[actual_index]
    row = raw_df.row(int(raw_row), named=True)
    return int(row['start_trade_id'])


def _build_id_to_sample(
    train_dataset: object,
    level0_to_raw: list[int],
    raw_df: polars.DataFrame,
) -> dict[int, int]:
    mapping: dict[int, int] = {}
    for sample_index in range(len(train_dataset)):
        entry_id = _entry_start_trade_id(
            train_dataset=train_dataset,
            level0_to_raw=level0_to_raw,
            raw_df=raw_df,
            sample_index=sample_index,
        )
        if entry_id not in mapping:
            mapping[entry_id] = sample_index
    return mapping


def _policy_action(result: dict[str, object]) -> str:
    policy = result['policy']
    if not isinstance(policy, dict):
        raise RuntimeError('policy must be a dict')
    return str(policy['action'])


def _recommended_action(result: dict[str, object]) -> str | None:
    if 'entry_hint' not in result:
        return None
    entry_hint = result['entry_hint']
    if not isinstance(entry_hint, dict):
        return None
    if 'recommended_action' not in entry_hint:
        return None
    return str(entry_hint['recommended_action'])


def _pred_float(result: dict[str, object], key: str) -> float | None:
    predictions = result['predictions']
    if not isinstance(predictions, dict):
        raise RuntimeError('predictions must be a dict')
    if key not in predictions:
        return None
    return float(predictions[key])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--bars', type=int, default=400_000)
    parser.add_argument('--count', type=int, default=10)
    parser.add_argument('--symbol', type=str, default='BTC_USDT')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    metadata = fetch_inference_metadata()
    symbol = SymbolId[args.symbol]

    raw_df = fetch_last_bars_sync(symbol_id=symbol, limit=args.bars, offset=0, since_start_trade_id=None)
    if raw_df is None:
        raise RuntimeError('Failed to fetch bars')

    padded_df, _real_bar_count = append_forward_target_padding(
        raw_df=raw_df,
        forward_bars=65536,
    )

    print('Building unpadded train dataset...')
    train_u = _build_train_dataset(df=raw_df, metadata=metadata)
    print('Building padded train dataset...')
    train_p = _build_train_dataset(df=padded_df, metadata=metadata)

    l2ru = _build_level0_to_raw_row_indices_from_dataset(raw_df, train_u)
    l2rp = _build_level0_to_raw_row_indices_from_dataset(padded_df, train_p)

    id_to_u = _build_id_to_sample(train_u, l2ru, raw_df)
    id_to_p = _build_id_to_sample(train_p, l2rp, padded_df)
    common_ids = sorted(set(id_to_u.keys()).intersection(id_to_p.keys()))

    mid = len(common_ids) // 2
    half = args.count // 2
    start = max(0, mid - half)
    selected_ids = common_ids[start:start + args.count]
    if len(selected_ids) < args.count:
        selected_ids = common_ids[: args.count]

    print(f'\nSelected {len(selected_ids)} aligned start_trade_id (mid-history window)')
    print('=' * 100)

    payloads_u: list[dict[str, object]] = []
    payloads_p: list[dict[str, object]] = []
    for entry_id in selected_ids:
        idx_u = id_to_u[entry_id]
        idx_p = id_to_p[entry_id]
        payloads_u.append(
            _prepare_payload_dict_from_train_sample(
                train_dataset=train_u,
                train_sample_index=idx_u,
            ),
        )
        payloads_p.append(
            _prepare_payload_dict_from_train_sample(
                train_dataset=train_p,
                train_sample_index=idx_p,
            ),
        )

    print('Calling inference batch (unpadded payloads)...')
    results_u = _call_inference_batch_api(payloads_u, args.symbol)
    print('Calling inference batch (padded payloads)...')
    results_p = _call_inference_batch_api(payloads_p, args.symbol)

    if len(results_u) != len(selected_ids) or len(results_p) != len(selected_ids):
        raise RuntimeError('Batch result count mismatch')

    pred_diffs: list[float] = []
    long_diffs: list[float] = []
    short_diffs: list[float] = []
    policy_mismatch = 0
    recommended_mismatch = 0

    for entry_id, res_u, res_p, idx_u, idx_p in zip(
        selected_ids,
        results_u,
        results_p,
        [id_to_u[i] for i in selected_ids],
        [id_to_p[i] for i in selected_ids],
        strict=True,
    ):
        pred_u = _pred_float(res_u, PREDICTION_KEYS[0])
        pred_p = _pred_float(res_p, PREDICTION_KEYS[0])
        long_u = _pred_float(res_u, PREDICTION_KEYS[1])
        long_p = _pred_float(res_p, PREDICTION_KEYS[1])
        short_u = _pred_float(res_u, PREDICTION_KEYS[2])
        short_p = _pred_float(res_p, PREDICTION_KEYS[2])

        pol_u = _policy_action(res_u)
        pol_p = _policy_action(res_p)
        rec_u = _recommended_action(res_u)
        rec_p = _recommended_action(res_p)

        if pred_u is not None and pred_p is not None:
            pred_diffs.append(abs(pred_u - pred_p))
        if long_u is not None and long_p is not None:
            long_diffs.append(abs(long_u - long_p))
        if short_u is not None and short_p is not None:
            short_diffs.append(abs(short_u - short_p))
        if pol_u != pol_p:
            policy_mismatch += 1
        if rec_u != rec_p:
            recommended_mismatch += 1

        print(
            f'entry_id={entry_id} idx_u={idx_u} idx_p={idx_p}\n'
            f'  pred_x1536: u={pred_u:.8f} p={pred_p:.8f} diff={abs(pred_u - pred_p):.8f}\n'
            f'  pred_long:  u={long_u:.8f} p={long_p:.8f} diff={abs(long_u - long_p):.8f}\n'
            f'  pred_short: u={short_u:.8f} p={short_p:.8f} diff={abs(short_u - short_p):.8f}\n'
            f'  policy: u={pol_u} p={pol_p}  recommended: u={rec_u} p={rec_p}',
        )
        print('-' * 100)

    print('\n=== SUMMARY ===')
    if pred_diffs:
        print(
            f'combined pred_x1536: max_diff={max(pred_diffs):.8f} '
            f'mean_diff={float(np.mean(pred_diffs)):.8f}',
        )
    if long_diffs:
        print(
            f'pred_long:           max_diff={max(long_diffs):.8f} '
            f'mean_diff={float(np.mean(long_diffs)):.8f}',
        )
    if short_diffs:
        print(
            f'pred_short:          max_diff={max(short_diffs):.8f} '
            f'mean_diff={float(np.mean(short_diffs)):.8f}',
        )
    print(f'policy mismatches:     {policy_mismatch}/{len(selected_ids)}')
    print(f'recommended mismatches: {recommended_mismatch}/{len(selected_ids)}')


if __name__ == '__main__':
    try:
        main()
    except Exception as exception:
        logger.error('Failed: %s', ''.join(traceback.format_exception(exception)))
        sys.exit(1)
