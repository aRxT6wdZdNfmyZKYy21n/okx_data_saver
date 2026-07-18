from __future__ import annotations

import logging
import traceback
from typing import Any

import numpy as np
import polars

from enumerations import SymbolId
from main.offline_inference.artifacts import write_trade_research_meta
from main.offline_inference.atomic_io import atomic_write_npz
from main.offline_inference.paths import trade_research_npz_path
from main.web_gui.data_service import fetch_last_bars_sync
from main.web_gui.inference_service import (
    _build_dataset,
    _build_level0_to_raw_row_indices,
    _build_train_level0_context,
    _prepare_payload_dict_from_train_sample,
    _train_sample_index_for_inference_sample,
    fetch_inference_metadata,
)
from main.web_gui.trade_research_service import (
    _call_inference_batch_api,
    _merge_sorted_sample_indices,
    _map_sample_indices_to_train,
    _raw_bar_metadata,
    _row_value,
    _sample_indices_for_full_dataset,
    _sample_indices_for_pnl_backtest,
    horizon_steps_from_name,
)
from settings import settings

logger = logging.getLogger(__name__)

DEFAULT_EVAL_HORIZON = 'x2048'
BATCH_CHUNK_SIZE = 32


def _prediction_key_for_horizon(horizon_name: str) -> str:
    return f'target_close_return_signed_log2_{horizon_name}'


def _horizon_names_from_metadata(metadata: dict[str, object]) -> list[str]:
    if 'horizons' not in metadata:
        raise RuntimeError('Inference metadata missing horizons')
    horizon_names = [str(horizon_name) for horizon_name in metadata['horizons']]
    if len(horizon_names) == 0:
        raise RuntimeError('Inference metadata horizons list is empty')
    return horizon_names


def _target_log2_from_train_sample(
    train_dataset: object,
    train_sample_index: int,
    horizon_names: list[str],
) -> dict[str, float]:
    _x_seq, _x_static, targets = train_dataset[train_sample_index]
    target_values = targets.detach().cpu().numpy().reshape(-1)
    target_cols = list(train_dataset.target_cols)
    targets_by_horizon: dict[str, float] = {}
    for target_col in target_cols:
        prefix = 'target_close_return_signed_log2_'
        if not str(target_col).startswith(prefix):
            continue
        horizon_name = str(target_col)[len(prefix):]
        col_index = target_cols.index(target_col)
        targets_by_horizon[horizon_name] = float(target_values[col_index])
    for horizon_name in horizon_names:
        if horizon_name not in targets_by_horizon:
            raise RuntimeError(f'Missing target for horizon {horizon_name!r}')
    return targets_by_horizon


def _bar_metadata_for_sample(
    sample_index: int,
    start_index: int,
    horizon_steps: int,
    level0_to_raw: list[int],
    raw_df: polars.DataFrame,
    level0_height: int,
) -> dict[str, float | int]:
    entry_bar_index = start_index + sample_index
    exit_bar_index = entry_bar_index + horizon_steps
    if exit_bar_index >= level0_height:
        raise RuntimeError(
            f'Sample {sample_index} exit bar {exit_bar_index} >= level0 height {level0_height}',
        )
    entry_raw_index = level0_to_raw[entry_bar_index]
    exit_raw_index = level0_to_raw[exit_bar_index]
    entry_meta = _raw_bar_metadata(raw_df, entry_raw_index)
    exit_meta = _raw_bar_metadata(raw_df, exit_raw_index)
    return {
        'entry_start_trade_id': int(entry_meta['start_trade_id']),
        'exit_start_trade_id': int(exit_meta['start_trade_id']),
        'entry_timestamp_ms': int(entry_meta['start_timestamp_ms']),
        'exit_timestamp_ms': int(exit_meta['start_timestamp_ms']),
        'entry_open': float(entry_meta['open_price']),
        'entry_close': float(entry_meta['close_price']),
        'exit_close': float(exit_meta['close_price']),
    }


def _run_batch_predictions(
    sample_indices: list[int],
    train_sample_index_by_inference_sample: dict[int, int],
    train_dataset: object,
    symbol_id: str,
) -> dict[int, dict[str, float]]:
    predictions_by_sample: dict[int, dict[str, float]] = {}
    if len(sample_indices) == 0:
        return predictions_by_sample

    total_chunks = (len(sample_indices) + BATCH_CHUNK_SIZE - 1) // BATCH_CHUNK_SIZE
    for chunk_index, chunk_start in enumerate(
        range(0, len(sample_indices), BATCH_CHUNK_SIZE),
    ):
        chunk_sample_indices = sample_indices[
            chunk_start:chunk_start + BATCH_CHUNK_SIZE
        ]
        chunk_payloads = []
        for sample_index in chunk_sample_indices:
            train_sample_index = train_sample_index_by_inference_sample[sample_index]
            chunk_payloads.append(
                _prepare_payload_dict_from_train_sample(
                    train_dataset=train_dataset,
                    train_sample_index=train_sample_index,
                ),
            )
        chunk_results = _call_inference_batch_api(
            samples=chunk_payloads,
            symbol_id=symbol_id,
        )
        if len(chunk_results) != len(chunk_sample_indices):
            raise RuntimeError(
                'Batch inference result count mismatch: '
                f'{len(chunk_results)} != {len(chunk_sample_indices)}',
            )
        for sample_index, inference_result in zip(
            chunk_sample_indices,
            chunk_results,
            strict=True,
        ):
            if 'predictions' not in inference_result:
                raise RuntimeError('Batch inference result missing predictions')
            predictions = inference_result['predictions']
            if not isinstance(predictions, dict):
                raise RuntimeError('Batch inference predictions must be a dict')
            predictions_by_sample[sample_index] = {
                str(key): float(value)
                for key, value in predictions.items()
            }
        if (chunk_index + 1) % 10 == 0 or (chunk_index + 1) == total_chunks:
            logger.info(
                'Trade research export inference: %d/%d batches, %d/%d samples',
                chunk_index + 1,
                total_chunks,
                len(predictions_by_sample),
                len(sample_indices),
            )
    return predictions_by_sample


def _load_existing_npz(npz_path: str) -> dict[str, Any] | None:
    import os

    if not os.path.isfile(npz_path):
        return None
    data = np.load(npz_path, allow_pickle=True)
    return {key: data[key] for key in data.files}


def _checkpoint_matches_existing(
    existing: dict[str, Any],
    run_label: str,
    checkpoint_path: str,
) -> bool:
    existing_run_label = str(existing['run_label'][0])
    existing_checkpoint_path = str(existing['checkpoint_path'][0])
    return (
        existing_run_label == run_label
        and existing_checkpoint_path == checkpoint_path
    )


def _merge_npz_rows(
    existing: dict[str, Any] | None,
    new_rows: dict[str, Any],
    horizon_names: list[str],
) -> dict[str, Any]:
    if existing is None:
        return new_rows

    array_keys = [
        'eval_target_log2',
        'entry_start_trade_id',
        'exit_start_trade_id',
        'entry_timestamp_ms',
        'exit_timestamp_ms',
        'entry_open',
        'entry_close',
        'exit_close',
    ]
    for horizon_name in horizon_names:
        array_keys.append(f'pred_{horizon_name}')
        array_keys.append(f'target_{horizon_name}')

    row_by_sample: dict[int, dict[str, Any]] = {}

    def ingest(source: dict[str, Any]) -> None:
        sample_indices = source['sample_index'].astype(np.int64)
        for row_index, sample_index_value in enumerate(sample_indices.tolist()):
            row_values: dict[str, Any] = {}
            for key in array_keys:
                row_values[key] = source[key][row_index]
            row_by_sample[int(sample_index_value)] = row_values

    ingest(existing)
    ingest(new_rows)

    sorted_sample_indices = sorted(row_by_sample.keys())
    merged_rows: dict[str, list[Any]] = {
        'sample_index': [],
    }
    for key in array_keys:
        merged_rows[key] = []

    for sample_index_value in sorted_sample_indices:
        row_values = row_by_sample[sample_index_value]
        merged_rows['sample_index'].append(sample_index_value)
        for key in array_keys:
            merged_rows[key].append(row_values[key])

    scalar_keys = [
        'run_label',
        'checkpoint_path',
        'eval_horizon',
        'dataset_length',
        'start_index',
        'pnl_stride',
        'bars_loaded',
        'level0_rows',
        'last_bar_start_trade_id',
        'research_limit',
        'required_rows',
    ]
    metadata_fields = {
        key: new_rows[key]
        for key in scalar_keys
    }
    metadata_fields['horizon_names'] = new_rows['horizon_names']

    return _build_npz_payload(
        rows=merged_rows,
        horizon_names=horizon_names,
        metadata_fields=metadata_fields,
    )


def _build_npz_payload(
    rows: dict[str, list[Any]],
    horizon_names: list[str],
    metadata_fields: dict[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = dict(metadata_fields)
    payload['sample_index'] = np.array(rows['sample_index'], dtype=np.int64)
    payload['eval_target_log2'] = np.array(rows['eval_target_log2'], dtype=np.float64)
    payload['entry_start_trade_id'] = np.array(rows['entry_start_trade_id'], dtype=np.int64)
    payload['exit_start_trade_id'] = np.array(rows['exit_start_trade_id'], dtype=np.int64)
    payload['entry_timestamp_ms'] = np.array(rows['entry_timestamp_ms'], dtype=np.int64)
    payload['exit_timestamp_ms'] = np.array(rows['exit_timestamp_ms'], dtype=np.int64)
    payload['entry_open'] = np.array(rows['entry_open'], dtype=np.float64)
    payload['entry_close'] = np.array(rows['entry_close'], dtype=np.float64)
    payload['exit_close'] = np.array(rows['exit_close'], dtype=np.float64)
    payload['horizon_names'] = np.array(horizon_names, dtype=object)
    for horizon_name in horizon_names:
        payload[f'pred_{horizon_name}'] = np.array(
            rows[f'pred_{horizon_name}'],
            dtype=np.float64,
        )
        payload[f'target_{horizon_name}'] = np.array(
            rows[f'target_{horizon_name}'],
            dtype=np.float64,
        )
    return payload


def run_trade_research_export(symbol_id: str) -> None:
    eval_horizon = DEFAULT_EVAL_HORIZON
    horizon_steps = horizon_steps_from_name(eval_horizon)
    step_bars = horizon_steps
    research_limit = settings.WEB_GUI_TRADE_RESEARCH_LIMIT
    pnl_stride = settings.WEB_GUI_TRADE_RESEARCH_PNL_STRIDE
    npz_path = trade_research_npz_path(symbol_id)

    write_trade_research_meta(
        symbol_id=symbol_id,
        payload={
            'status': 'computing',
            'eval_horizon': eval_horizon,
            'pnl_stride': pnl_stride,
            'research_limit': research_limit,
        },
    )

    metadata = fetch_inference_metadata()
    required_rows = int(metadata['sequence_length']) * int(metadata['max_scale'])
    minimum_rows = required_rows + horizon_steps
    if research_limit < minimum_rows:
        raise RuntimeError(
            'Trade research limit is below minimum x1 bars '
            f'(minimum {minimum_rows}, configured {research_limit})',
        )

    checkpoint_path_by_symbol = metadata['checkpoint_path_by_symbol']
    if symbol_id not in checkpoint_path_by_symbol:
        raise RuntimeError(f'Metadata missing checkpoint for {symbol_id!r}')
    checkpoint_path = str(checkpoint_path_by_symbol[symbol_id])

    policy_by_symbol = metadata['policy_by_symbol']
    if symbol_id not in policy_by_symbol:
        raise RuntimeError(f'Metadata missing policy for {symbol_id!r}')
    run_label = str(policy_by_symbol[symbol_id]['run_label'])

    symbol = SymbolId[symbol_id]
    df = fetch_last_bars_sync(symbol_id=symbol, limit=research_limit, offset=0)
    if df is None:
        raise RuntimeError('Недостаточно данных для trade research export')
    if df.height < minimum_rows:
        raise RuntimeError(
            'Trade research export: fetched fewer x1 bars than required '
            f'({df.height} < {minimum_rows})',
        )

    horizon_names = _horizon_names_from_metadata(metadata)
    logger.info(
        'Dataset preparation start: trade research rows=%d sequence_length=%d',
        int(df.height),
        int(metadata['sequence_length']),
    )
    dataset = _build_dataset(df, metadata)
    train_dataset, train_level0_df, raw_to_train_level0_row = _build_train_level0_context(
        df=df,
        metadata=metadata,
    )
    start_index = int(dataset.dataset.start_index)
    dataset_length = len(dataset)
    logger.info(
        'Dataset preparation done: samples=%d start_index=%d level0_rows=%d',
        dataset_length,
        start_index,
        int(train_level0_df.height),
    )
    level0_df = dataset.dataset.aggregated_data[0]
    level0_height = int(level0_df.height)
    level0_to_raw_row_indices = _build_level0_to_raw_row_indices(df, level0_df)

    max_sample_index = dataset_length - 1 - horizon_steps
    if max_sample_index < 0:
        raise RuntimeError('Dataset too short for eval horizon')

    grid_sample_indices, sample_selection_note = _sample_indices_for_full_dataset(
        dataset_length=dataset_length,
        step_bars=step_bars,
        horizon_steps=horizon_steps,
    )
    pnl_sample_indices = _sample_indices_for_pnl_backtest(
        max_sample_index=max_sample_index,
        stride=pnl_stride,
    )
    inference_sample_indices = _merge_sorted_sample_indices(
        first_indices=grid_sample_indices,
        second_indices=pnl_sample_indices,
    )

    mapped_inference_indices, train_sample_index_by_inference_sample, skipped_unmapped = (
        _map_sample_indices_to_train(
            sample_indices=inference_sample_indices,
            start_index=start_index,
            inference_level0_to_raw=level0_to_raw_row_indices,
            raw_to_train_level0_row=raw_to_train_level0_row,
            train_dataset_length=len(train_dataset),
        )
    )
    if skipped_unmapped > 0:
        unmapped_note = (
            f'skipped {skipped_unmapped} samples without train-mode alignment'
        )
        if sample_selection_note is None:
            sample_selection_note = unmapped_note
        else:
            sample_selection_note = f'{sample_selection_note}; {unmapped_note}'

    existing_npz = _load_existing_npz(npz_path)
    if existing_npz is not None and not _checkpoint_matches_existing(
        existing=existing_npz,
        run_label=run_label,
        checkpoint_path=checkpoint_path,
    ):
        logger.info(
            'Checkpoint changed (%s -> %s); rebuilding NPZ from scratch',
            str(existing_npz['checkpoint_path'][0]),
            checkpoint_path,
        )
        existing_npz = None

    if existing_npz is not None:
        existing_start_index = int(existing_npz['start_index'][0])
        if existing_start_index != start_index:
            logger.info(
                'Dataset start_index shifted (%d -> %d); rebuilding NPZ from scratch',
                existing_start_index,
                start_index,
            )
            existing_npz = None

    existing_sample_set: set[int] = set()
    if existing_npz is not None:
        existing_sample_set = {
            int(value)
            for value in existing_npz['sample_index'].astype(np.int64).tolist()
        }

    samples_to_infer = [
        sample_index
        for sample_index in mapped_inference_indices
        if sample_index not in existing_sample_set
    ]

    logger.info(
        'Trade research export: symbol=%s total_samples=%d existing=%d new=%d',
        symbol_id,
        len(mapped_inference_indices),
        len(existing_sample_set),
        len(samples_to_infer),
    )

    predictions_by_sample = _run_batch_predictions(
        sample_indices=samples_to_infer,
        train_sample_index_by_inference_sample=train_sample_index_by_inference_sample,
        train_dataset=train_dataset,
        symbol_id=symbol_id,
    )

    rows: dict[str, list[Any]] = {
        'sample_index': [],
        'eval_target_log2': [],
        'entry_start_trade_id': [],
        'exit_start_trade_id': [],
        'entry_timestamp_ms': [],
        'exit_timestamp_ms': [],
        'entry_open': [],
        'entry_close': [],
        'exit_close': [],
    }
    for horizon_name in horizon_names:
        rows[f'pred_{horizon_name}'] = []
        rows[f'target_{horizon_name}'] = []

    for sample_index in samples_to_infer:
        train_sample_index = train_sample_index_by_inference_sample[sample_index]
        sample_predictions = predictions_by_sample[sample_index]
        targets_by_horizon = _target_log2_from_train_sample(
            train_dataset=train_dataset,
            train_sample_index=train_sample_index,
            horizon_names=horizon_names,
        )
        bar_metadata = _bar_metadata_for_sample(
            sample_index=sample_index,
            start_index=start_index,
            horizon_steps=horizon_steps,
            level0_to_raw=level0_to_raw_row_indices,
            raw_df=df,
            level0_height=level0_height,
        )

        rows['sample_index'].append(int(sample_index))
        rows['eval_target_log2'].append(float(targets_by_horizon[eval_horizon]))
        rows['entry_start_trade_id'].append(int(bar_metadata['entry_start_trade_id']))
        rows['exit_start_trade_id'].append(int(bar_metadata['exit_start_trade_id']))
        rows['entry_timestamp_ms'].append(int(bar_metadata['entry_timestamp_ms']))
        rows['exit_timestamp_ms'].append(int(bar_metadata['exit_timestamp_ms']))
        rows['entry_open'].append(float(bar_metadata['entry_open']))
        rows['entry_close'].append(float(bar_metadata['entry_close']))
        rows['exit_close'].append(float(bar_metadata['exit_close']))

        for horizon_name in horizon_names:
            prediction_key = _prediction_key_for_horizon(horizon_name)
            if prediction_key not in sample_predictions:
                raise RuntimeError(
                    f'Missing prediction {prediction_key!r} for sample {sample_index}',
                )
            rows[f'pred_{horizon_name}'].append(float(sample_predictions[prediction_key]))
            rows[f'target_{horizon_name}'].append(float(targets_by_horizon[horizon_name]))

    last_bar_row = df.row(df.height - 1, named=True)
    last_bar_start_trade_id = int(_row_value(last_bar_row, 'start_trade_id'))

    metadata_fields = {
        'run_label': np.array([run_label], dtype=object),
        'checkpoint_path': np.array([checkpoint_path], dtype=object),
        'eval_horizon': np.array([eval_horizon], dtype=object),
        'dataset_length': np.array([dataset_length], dtype=np.int64),
        'start_index': np.array([start_index], dtype=np.int64),
        'pnl_stride': np.array([pnl_stride], dtype=np.int64),
        'bars_loaded': np.array([int(df.height)], dtype=np.int64),
        'level0_rows': np.array([level0_height], dtype=np.int64),
        'last_bar_start_trade_id': np.array([last_bar_start_trade_id], dtype=np.int64),
        'research_limit': np.array([research_limit], dtype=np.int64),
        'required_rows': np.array([required_rows], dtype=np.int64),
    }

    if len(samples_to_infer) == 0 and existing_npz is not None:
        merged_payload = dict(existing_npz)
        for key, value in metadata_fields.items():
            merged_payload[key] = value
    else:
        new_payload = _build_npz_payload(
            rows=rows,
            horizon_names=horizon_names,
            metadata_fields=metadata_fields,
        )
        merged_payload = _merge_npz_rows(
            existing=existing_npz,
            new_rows=new_payload,
            horizon_names=horizon_names,
        )

    atomic_write_npz(npz_path, **merged_payload)

    sample_count = int(merged_payload['sample_index'].shape[0])
    last_sample_index = int(np.max(merged_payload['sample_index']))
    write_trade_research_meta(
        symbol_id=symbol_id,
        payload={
            'status': 'ok',
            'eval_horizon': eval_horizon,
            'pnl_stride': pnl_stride,
            'research_limit': research_limit,
            'required_rows': required_rows,
            'bars_loaded': int(df.height),
            'level0_rows': level0_height,
            'dataset_length': dataset_length,
            'start_index': start_index,
            'sample_count': sample_count,
            'last_sample_index': last_sample_index,
            'last_bar_start_trade_id': last_bar_start_trade_id,
            'run_label': run_label,
            'checkpoint_path': checkpoint_path,
            'sample_selection_note': sample_selection_note,
        },
    )


def run_trade_research_export_safe(symbol_id: str) -> None:
    try:
        run_trade_research_export(symbol_id=symbol_id)
    except Exception as exception:
        logger.error(
            'Trade research export failed: %s',
            ''.join(traceback.format_exception(exception)),
        )
        write_trade_research_meta(
            symbol_id=symbol_id,
            payload={
                'status': 'error',
                'error_message': str(exception),
            },
        )
        raise
