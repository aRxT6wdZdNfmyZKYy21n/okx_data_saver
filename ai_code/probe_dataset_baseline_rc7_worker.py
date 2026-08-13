"""Subprocess worker: build HybridTradeDataset(train) and dump parity artifacts."""

from __future__ import annotations

import argparse
import logging
import pickle
import sys
import traceback
import types
from pathlib import Path

import numpy as np
import polars
from omegaconf import OmegaConf

logger = logging.getLogger(__name__)


def _install_dataset_package(package_dir: Path) -> None:
    for module_name in list(sys.modules):
        if module_name == 'trading_bot_dataset' or module_name.startswith('trading_bot_dataset.'):
            del sys.modules[module_name]
    package = types.ModuleType('trading_bot_dataset')
    package.__path__ = [str(package_dir)]
    sys.modules['trading_bot_dataset'] = package


def _build_level0_to_raw_row_indices(
    raw_df: polars.DataFrame,
    level0_close_price_log2: np.ndarray,
) -> list[int]:
    level0_log2 = level0_close_price_log2.astype(np.float64)
    raw_log2 = raw_df['close_price'].log(base=2).to_numpy()

    raw_indices: list[int] = []
    raw_pos = 0
    raw_len = len(raw_log2)

    for level0_pos, target_log2 in enumerate(level0_log2):
        found = False
        while raw_pos < raw_len:
            if abs(raw_log2[raw_pos] - target_log2) <= 1e-4:
                raw_indices.append(raw_pos)
                raw_pos = raw_pos + 1
                found = True
                break
            raw_pos = raw_pos + 1
        if not found:
            raise RuntimeError(
                f'Failed to align level0 row {level0_pos} to raw dataframe',
            )

    return raw_indices


def _sample_start_trade_ids(
    raw_df: polars.DataFrame,
    start_index: int,
    dataset_length: int,
    level0_to_raw: list[int],
) -> list[tuple[int, int]]:
    rows: list[tuple[int, int]] = []
    for sample_index in range(dataset_length):
        entry_bar_index = start_index + sample_index
        raw_row = level0_to_raw[entry_bar_index]
        row = raw_df.row(int(raw_row), named=True)
        rows.append((sample_index, int(row['start_trade_id'])))
    return rows


def _dump_sample_tensors(
    dataset: object,
    sample_indices: list[int],
    scale_features: list[str],
) -> dict[int, dict[str, object]]:
    dumped: dict[int, dict[str, object]] = {}
    for sample_index in sample_indices:
        x_seq, x_static, targets = dataset[sample_index]
        scale_arrays: dict[str, object] = {}
        for scale_name in scale_features:
            scale_arrays[scale_name] = x_seq[scale_name].detach().cpu().numpy().copy()
        dumped[sample_index] = {
            'x_static': x_static.detach().cpu().numpy().copy(),
            'targets': targets.detach().cpu().numpy().copy(),
            'x_seq': scale_arrays,
        }
    return dumped


def _trade_id_map(
    sample_trade_ids: list[tuple[int, int]],
) -> dict[int, int]:
    sample_by_trade_id: dict[int, int] = {}
    for sample_index, start_trade_id in sample_trade_ids:
        if start_trade_id in sample_by_trade_id:
            raise RuntimeError(
                f'duplicate start_trade_id in dataset mapping: {start_trade_id}',
            )
        sample_by_trade_id[start_trade_id] = sample_index
    return sample_by_trade_id


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--package-dir', type=str, required=True)
    parser.add_argument('--df-parquet', type=str, required=True)
    parser.add_argument('--metadata-pickle', type=str, required=True)
    parser.add_argument('--out-pickle', type=str, required=True)
    parser.add_argument('--detail-samples', type=int, default=32)
    parser.add_argument('--detail-trade-ids-file', type=str, default='')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    package_dir = Path(args.package_dir).resolve()
    _install_dataset_package(package_dir)

    from trading_bot_dataset.src.dataset import HybridTradeDataset
    from trading_bot_dataset.src.volume_windows import extract_volume_windows_config

    with open(args.metadata_pickle, 'rb') as metadata_file:
        metadata = pickle.load(metadata_file)

    raw_df = polars.read_parquet(args.df_parquet)
    dataset_cfg = OmegaConf.create(metadata['dataset_config'])
    model_cfg = OmegaConf.create(
        {
            'params': {
                'scale_features': metadata['model_config']['scale_features'],
            },
        },
    )
    volume_windows_config = extract_volume_windows_config(dataset_cfg)

    train_dataset = HybridTradeDataset(
        dataframe=raw_df,
        sequence_length=int(metadata['sequence_length']),
        raw_columns=list(dataset_cfg['raw_cols']),
        static_columns=list(dataset_cfg['static_cols']),
        target_cols=list(dataset_cfg['target_cols']),
        aggregation_levels=list(dataset_cfg['aggregation_levels']),
        use_indicators=bool(dataset_cfg['use_indicators']),
        indicator_cols=list(dataset_cfg['indicator_cols']),
        model_config=model_cfg,
        inference_mode=False,
        volume_windows_config=volume_windows_config,
    )

    level0_to_raw = _build_level0_to_raw_row_indices(
        raw_df=raw_df,
        level0_close_price_log2=train_dataset.level0_close_price_log2_numpy(),
    )
    sample_trade_ids = _sample_start_trade_ids(
        raw_df=raw_df,
        start_index=int(train_dataset.start_index),
        dataset_length=len(train_dataset),
        level0_to_raw=level0_to_raw,
    )

    dataset_length = len(train_dataset)
    detail_sample_indices: list[int] = []
    sample_by_trade_id = _trade_id_map(sample_trade_ids)
    chosen_trade_ids: list[int] = []
    if args.detail_trade_ids_file:
        trade_ids_path = Path(args.detail_trade_ids_file)
        chosen_trade_ids = [
            int(line.strip())
            for line in trade_ids_path.read_text(encoding='utf-8').splitlines()
            if line.strip()
        ]
    elif args.detail_samples > 0 and sample_by_trade_id:
        all_trade_ids = sorted(sample_by_trade_id.keys())
        if args.detail_samples >= len(all_trade_ids):
            chosen_trade_ids = all_trade_ids
        else:
            step = max(len(all_trade_ids) // args.detail_samples, 1)
            chosen_trade_ids = all_trade_ids[::step][: args.detail_samples]

    for start_trade_id in chosen_trade_ids:
        if start_trade_id not in sample_by_trade_id:
            raise RuntimeError(
                f'start_trade_id {start_trade_id} missing from built dataset mapping',
            )
        detail_sample_indices.append(sample_by_trade_id[start_trade_id])

    scale_features = list(metadata['model_config']['scale_features'])
    detail_tensors = _dump_sample_tensors(
        dataset=train_dataset,
        sample_indices=detail_sample_indices,
        scale_features=scale_features,
    )

    artifact = {
        'package_dir': str(package_dir),
        'start_index': int(train_dataset.start_index),
        'dataset_length': dataset_length,
        'level0_rows': int(level0_df.height),
        'sample_trade_ids': sample_trade_ids,
        'detail_sample_indices': detail_sample_indices,
        'detail_trade_ids': chosen_trade_ids,
        'detail_tensors': detail_tensors,
        'target_cols': list(train_dataset.target_cols),
        'scale_features': scale_features,
    }

    out_path = Path(args.out_pickle)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'wb') as out_file:
        pickle.dump(artifact, out_file)

    logger.info(
        'worker done package=%s samples=%d level0=%d start_index=%d',
        package_dir.name,
        dataset_length,
        int(level0_df.height),
        int(train_dataset.start_index),
    )


if __name__ == '__main__':
    try:
        main()
    except Exception as exception:
        logger.error('worker failed: %s', ''.join(traceback.format_exception(exception)))
        raise
