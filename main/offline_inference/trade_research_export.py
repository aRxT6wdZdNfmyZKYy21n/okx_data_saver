from __future__ import annotations

import logging
import os
import traceback
from typing import Any

import numpy as np
import polars

from enumerations import SymbolId
from main.offline_inference.artifacts import (
    write_trade_research_meta,
)
from main.offline_inference.atomic_io import atomic_write_npz
from main.offline_inference.paths import (
    trade_research_horizon_dir,
    trade_research_npz_path,
)
from main.web_gui.data_service import fetch_last_bars_sync
from main.web_gui.trade_research_dataset_common import (
    TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS,
    TRADE_RESEARCH_FORWARD_TARGET_PADDING_SITE,
    prepare_trade_research_raw_dataframe,
    real_last_start_trade_id,
)
from main.web_gui.inference_service import (
    _build_dataset,
    _build_level0_to_raw_row_indices,
    _build_train_level0_context,
    _prepare_payload_dict_from_train_sample,
    _train_sample_index_for_inference_sample,
    fetch_inference_metadata,
)
from main.web_gui.trade_research_service import (
    TRADE_RESEARCH_NPZ_INFERENCE_ROW_KEYS,
    TRADE_RESEARCH_NPZ_PRICE_ROW_KEYS,
    _call_inference_batch_api,
    _pred_target_price,
    _merge_sorted_sample_indices,
    _map_sample_indices_to_train,
    _raw_bar_metadata,
    _row_value,
    _sample_indices_for_full_dataset,
    _sample_indices_for_pnl_backtest,
    horizon_steps_from_name,
    inference_row_from_batch_result,
    inference_stack_fingerprint,
    npz_stack_matches_fingerprint,
)
from settings import settings

logger = logging.getLogger(__name__)

BATCH_CHUNK_SIZE = 32


def _train_size_ratio_for_export(metadata: dict[str, object]) -> float:
    dataset_cfg = metadata['dataset_config']
    if 'train_size' in dataset_cfg:
        return float(dataset_cfg['train_size'])
    return settings.WEB_GUI_TRADE_RESEARCH_TRAIN_SIZE_RATIO


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


def _run_batch_inference(
    sample_indices: list[int],
    train_sample_index_by_inference_sample: dict[int, int],
    train_dataset: object,
    symbol_id: str,
) -> dict[int, dict[str, object]]:
    inference_by_sample: dict[int, dict[str, object]] = {}
    if len(sample_indices) == 0:
        return inference_by_sample

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
            if not isinstance(inference_result, dict):
                raise RuntimeError('Batch inference result must be a dict')
            inference_by_sample[sample_index] = inference_result
        if (chunk_index + 1) % 10 == 0 or (chunk_index + 1) == total_chunks:
            logger.info(
                'Trade research export inference: %d/%d batches, %d/%d samples',
                chunk_index + 1,
                total_chunks,
                len(inference_by_sample),
                len(sample_indices),
            )
    return inference_by_sample


def _should_rebuild_existing_npz(
    existing_npz: dict[str, Any] | None,
    stack_fingerprint: dict[str, str],
    start_index: int,
    payload_mode: str,
) -> bool:
    if existing_npz is None:
        return False
    if not _checkpoint_matches_existing(
        existing=existing_npz,
        fingerprint=stack_fingerprint,
    ):
        logger.info(
            'Inference stack changed (%s / %s / %s); rebuilding NPZ from scratch',
            str(existing_npz['run_label'][0]),
            str(existing_npz['eval_horizon'][0]),
            str(existing_npz['policy_path'][0])
            if 'policy_path' in existing_npz
            else 'unknown policy',
        )
        return True
    for key in TRADE_RESEARCH_NPZ_INFERENCE_ROW_KEYS:
        if key not in existing_npz:
            logger.info(
                'Existing NPZ missing %s; rebuilding from scratch',
                key,
            )
            return True
    existing_start_index = int(existing_npz['start_index'][0])
    if existing_start_index != start_index:
        logger.info(
            'Dataset start_index shifted (%d -> %d); rebuilding NPZ from scratch',
            existing_start_index,
            start_index,
        )
        return True
    if 'payload_mode' not in existing_npz:
        logger.info('Existing NPZ missing payload_mode; rebuilding from scratch')
        return True
    existing_payload_mode = str(existing_npz['payload_mode'][0])
    if existing_payload_mode != payload_mode:
        logger.info(
            'Payload mode changed (%s -> %s); rebuilding NPZ from scratch',
            existing_payload_mode,
            payload_mode,
        )
        return True
    if 'forward_target_padding_bars' not in existing_npz:
        logger.info(
            'Existing NPZ missing forward_target_padding_bars; rebuilding from scratch',
        )
        return True
    existing_padding_bars = int(existing_npz['forward_target_padding_bars'][0])
    if existing_padding_bars != TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS:
        logger.info(
            'Forward target padding changed (%d -> %d); rebuilding NPZ from scratch',
            existing_padding_bars,
            TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS,
        )
        return True
    if 'forward_target_padding_site' not in existing_npz:
        logger.info(
            'Existing NPZ missing forward_target_padding_site; rebuilding from scratch',
        )
        return True
    existing_padding_site = str(existing_npz['forward_target_padding_site'][0])
    if existing_padding_site != TRADE_RESEARCH_FORWARD_TARGET_PADDING_SITE:
        logger.info(
            'Forward target padding site changed (%s -> %s); rebuilding NPZ from scratch',
            existing_padding_site,
            TRADE_RESEARCH_FORWARD_TARGET_PADDING_SITE,
        )
        return True
    return False


def _append_pred_price_fields(
    rows: dict[str, list[Any]],
    entry_close: float,
    pred_eval_log2: float,
) -> None:
    pred_start_price = float(entry_close)
    rows['pred_start_price'].append(pred_start_price)
    rows['pred_target_price'].append(
        _pred_target_price(
            entry_price=pred_start_price,
            pred_eval_log2=pred_eval_log2,
        ),
    )


def _load_existing_npz(npz_path: str) -> dict[str, Any] | None:
    import os

    if not os.path.isfile(npz_path):
        return None
    data = np.load(npz_path, allow_pickle=True)
    return {key: data[key] for key in data.files}


def _checkpoint_matches_existing(
    existing: dict[str, Any],
    fingerprint: dict[str, str],
) -> bool:
    return npz_stack_matches_fingerprint(existing, fingerprint)


def _merge_npz_rows(
    existing: dict[str, Any] | None,
    new_rows: dict[str, Any],
    horizon_names: list[str],
) -> dict[str, Any]:
    if existing is None:
        return new_rows

    array_keys = [
        'train_sample_index',
        'eval_target_log2',
        'entry_start_trade_id',
        'exit_start_trade_id',
        'entry_timestamp_ms',
        'exit_timestamp_ms',
        'entry_open',
        'entry_close',
        'exit_close',
        'pred_start_price',
        'pred_target_price',
    ]
    array_keys.extend(TRADE_RESEARCH_NPZ_INFERENCE_ROW_KEYS)
    for horizon_name in horizon_names:
        array_keys.append(f'pred_{horizon_name}')
        array_keys.append(f'target_{horizon_name}')

    row_by_sample: dict[int, dict[str, Any]] = {}

    def ingest(source: dict[str, Any]) -> None:
        sample_indices = source['sample_index'].astype(np.int64)
        for row_index, sample_index_value in enumerate(sample_indices.tolist()):
            row_values: dict[str, Any] = {}
            for key in array_keys:
                if key in source:
                    row_values[key] = source[key][row_index]
                elif key == 'train_sample_index':
                    row_values[key] = -1
                else:
                    raise RuntimeError(
                        f'Merged NPZ source missing required array key {key!r}',
                    )
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
        'policy_path',
        'entry_hint_mode',
        'eval_horizon',
        'dataset_length',
        'start_index',
        'pnl_stride',
        'bars_loaded',
        'level0_rows',
        'last_bar_start_trade_id',
        'research_limit',
        'required_rows',
        'train_size',
        'train_size_ratio',
        'payload_mode',
        'real_bars_loaded',
        'forward_target_padding_bars',
        'forward_target_padding_site',
        'real_last_start_trade_id',
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
    if 'train_sample_index' in rows:
        payload['train_sample_index'] = np.array(rows['train_sample_index'], dtype=np.int64)
    payload['eval_target_log2'] = np.array(rows['eval_target_log2'], dtype=np.float64)
    payload['entry_start_trade_id'] = np.array(rows['entry_start_trade_id'], dtype=np.int64)
    payload['exit_start_trade_id'] = np.array(rows['exit_start_trade_id'], dtype=np.int64)
    payload['entry_timestamp_ms'] = np.array(rows['entry_timestamp_ms'], dtype=np.int64)
    payload['exit_timestamp_ms'] = np.array(rows['exit_timestamp_ms'], dtype=np.int64)
    payload['entry_open'] = np.array(rows['entry_open'], dtype=np.float64)
    payload['entry_close'] = np.array(rows['entry_close'], dtype=np.float64)
    payload['exit_close'] = np.array(rows['exit_close'], dtype=np.float64)
    payload['pred_start_price'] = np.array(rows['pred_start_price'], dtype=np.float64)
    payload['pred_target_price'] = np.array(rows['pred_target_price'], dtype=np.float64)
    payload['horizon_names'] = np.array(horizon_names, dtype=object)
    for key in TRADE_RESEARCH_NPZ_INFERENCE_ROW_KEYS:
        if key == 'entry_hint_json':
            payload[key] = np.array(rows[key], dtype=object)
        elif key == 'policy_action':
            payload[key] = np.array(rows[key], dtype=object)
        else:
            payload[key] = np.array(rows[key], dtype=np.float64)
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


def _backfill_pred_price_columns(
    payload: dict[str, Any],
    eval_horizon: str,
) -> dict[str, Any]:
    if 'pred_start_price' in payload and 'pred_target_price' in payload:
        return payload

    entry_close = payload['entry_close'].astype(np.float64)
    pred_eval_log2 = payload[f'pred_{eval_horizon}'].astype(np.float64)
    updated_payload = dict(payload)
    updated_payload['pred_start_price'] = entry_close.copy()
    updated_payload['pred_target_price'] = entry_close * np.power(2.0, pred_eval_log2)
    logger.info(
        'Backfilled pred_start_price/pred_target_price for %d NPZ rows (eval_horizon=%s)',
        int(entry_close.shape[0]),
        eval_horizon,
    )
    return updated_payload


def _extend_train_sample_index_map(
    sample_indices: list[int],
    train_sample_index_by_inference_sample: dict[int, int],
    start_index: int,
    inference_level0_to_raw: list[int],
    raw_to_train_level0_row: dict[int, int],
) -> dict[int, int]:
    extended = dict(train_sample_index_by_inference_sample)
    for sample_index in sample_indices:
        if sample_index in extended:
            continue
        train_sample_index = _train_sample_index_for_inference_sample(
            sample_index=sample_index,
            start_index=start_index,
            inference_level0_to_raw=inference_level0_to_raw,
            raw_to_train_level0_row=raw_to_train_level0_row,
        )
        if train_sample_index is not None:
            extended[sample_index] = int(train_sample_index)
    return extended


def _backfill_train_split_fields(
    merged_payload: dict[str, Any],
    train_sample_index_by_inference_sample: dict[int, int],
    train_size: int,
    train_size_ratio: float,
) -> None:
    sample_indices = merged_payload['sample_index'].astype(np.int64).tolist()
    train_sample_index_values: list[int] = []
    for sample_index_value in sample_indices:
        if sample_index_value in train_sample_index_by_inference_sample:
            train_sample_index_values.append(
                int(train_sample_index_by_inference_sample[sample_index_value]),
            )
        else:
            train_sample_index_values.append(-1)
    merged_payload['train_sample_index'] = np.array(
        train_sample_index_values,
        dtype=np.int64,
    )
    merged_payload['train_size'] = np.array([train_size], dtype=np.int64)
    merged_payload['train_size_ratio'] = np.array([train_size_ratio], dtype=np.float64)


def run_trade_research_export(symbol_id: str) -> None:
    research_limit = settings.WEB_GUI_TRADE_RESEARCH_LIMIT
    pnl_stride = settings.WEB_GUI_TRADE_RESEARCH_PNL_STRIDE

    metadata = fetch_inference_metadata()
    stack_fingerprint = inference_stack_fingerprint(metadata, symbol_id)
    eval_horizon = stack_fingerprint['eval_horizon']
    npz_path = trade_research_npz_path(symbol_id, eval_horizon)
    os.makedirs(trade_research_horizon_dir(symbol_id, eval_horizon), exist_ok=True)
    run_label = stack_fingerprint['run_label']
    checkpoint_path = stack_fingerprint['checkpoint_path']
    policy_path = stack_fingerprint['policy_path']
    entry_hint_mode = stack_fingerprint['entry_hint_mode']
    horizon_steps = horizon_steps_from_name(eval_horizon)
    step_bars = horizon_steps

    write_trade_research_meta(
        symbol_id=symbol_id,
        eval_horizon=eval_horizon,
        payload={
            'status': 'computing',
            'eval_horizon': eval_horizon,
            'pnl_stride': pnl_stride,
            'research_limit': research_limit,
            'policy_path': policy_path,
            'entry_hint_mode': entry_hint_mode,
        },
    )

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

    symbol = SymbolId[symbol_id]
    df = fetch_last_bars_sync(symbol_id=symbol, limit=research_limit, offset=0)
    if df is None:
        raise RuntimeError('Недостаточно данных для trade research export')
    if df.height < minimum_rows:
        raise RuntimeError(
            'Trade research export: fetched fewer x1 bars than required '
            f'({df.height} < {minimum_rows})',
        )

    df, real_bar_count = prepare_trade_research_raw_dataframe(df)
    real_last_trade_id = real_last_start_trade_id(df, real_bar_count)

    horizon_names = _horizon_names_from_metadata(metadata)
    logger.info(
        'Dataset preparation start: trade research real_rows=%d total_rows=%d sequence_length=%d',
        real_bar_count,
        int(df.height),
        int(metadata['sequence_length']),
    )
    dataset = _build_dataset(
        df.head(real_bar_count),
        metadata,
    )
    train_dataset, train_level0_df, raw_to_train_level0_row = _build_train_level0_context(
        df=df,
        metadata=metadata,
    )
    train_size_ratio = _train_size_ratio_for_export(metadata)
    train_size = int(len(train_dataset) * train_size_ratio)
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
    if _should_rebuild_existing_npz(
        existing_npz=existing_npz,
        stack_fingerprint=stack_fingerprint,
        start_index=start_index,
        payload_mode='train',
    ):
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

    inference_by_sample = _run_batch_inference(
        sample_indices=samples_to_infer,
        train_sample_index_by_inference_sample=train_sample_index_by_inference_sample,
        train_dataset=train_dataset,
        symbol_id=symbol_id,
    )

    rows: dict[str, list[Any]] = {
        'sample_index': [],
        'train_sample_index': [],
        'eval_target_log2': [],
        'entry_start_trade_id': [],
        'exit_start_trade_id': [],
        'entry_timestamp_ms': [],
        'exit_timestamp_ms': [],
        'entry_open': [],
        'entry_close': [],
        'exit_close': [],
        'pred_start_price': [],
        'pred_target_price': [],
    }
    for horizon_name in horizon_names:
        rows[f'pred_{horizon_name}'] = []
        rows[f'target_{horizon_name}'] = []
    for key in TRADE_RESEARCH_NPZ_INFERENCE_ROW_KEYS:
        rows[key] = []

    eval_prediction_key = _prediction_key_for_horizon(eval_horizon)

    for sample_index in samples_to_infer:
        train_sample_index = train_sample_index_by_inference_sample[sample_index]
        inference_result = inference_by_sample[sample_index]
        if 'predictions' not in inference_result:
            raise RuntimeError('Batch inference result missing predictions')
        sample_predictions = inference_result['predictions']
        if not isinstance(sample_predictions, dict):
            raise RuntimeError('Batch inference predictions must be a dict')
        inference_row = inference_row_from_batch_result(inference_result)
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
        rows['train_sample_index'].append(int(train_sample_index))
        rows['eval_target_log2'].append(float(targets_by_horizon[eval_horizon]))
        rows['entry_start_trade_id'].append(int(bar_metadata['entry_start_trade_id']))
        rows['exit_start_trade_id'].append(int(bar_metadata['exit_start_trade_id']))
        rows['entry_timestamp_ms'].append(int(bar_metadata['entry_timestamp_ms']))
        rows['exit_timestamp_ms'].append(int(bar_metadata['exit_timestamp_ms']))
        rows['entry_open'].append(float(bar_metadata['entry_open']))
        rows['entry_close'].append(float(bar_metadata['entry_close']))
        rows['exit_close'].append(float(bar_metadata['exit_close']))

        if eval_prediction_key not in sample_predictions:
            raise RuntimeError(
                f'Missing prediction {eval_prediction_key!r} for sample {sample_index}',
            )
        pred_eval_log2 = float(sample_predictions[eval_prediction_key])

        for horizon_name in horizon_names:
            prediction_key = _prediction_key_for_horizon(horizon_name)
            if prediction_key not in sample_predictions:
                raise RuntimeError(
                    f'Missing prediction {prediction_key!r} for sample {sample_index}',
                )
            rows[f'pred_{horizon_name}'].append(float(sample_predictions[prediction_key]))
            rows[f'target_{horizon_name}'].append(float(targets_by_horizon[horizon_name]))

        _append_pred_price_fields(
            rows=rows,
            entry_close=float(bar_metadata['entry_close']),
            pred_eval_log2=pred_eval_log2,
        )

        for key in TRADE_RESEARCH_NPZ_INFERENCE_ROW_KEYS:
            rows[key].append(inference_row[key])

    last_bar_row = df.row(real_bar_count - 1, named=True)
    last_bar_start_trade_id = int(_row_value(last_bar_row, 'start_trade_id'))

    metadata_fields = {
        'run_label': np.array([run_label], dtype=object),
        'checkpoint_path': np.array([checkpoint_path], dtype=object),
        'policy_path': np.array([policy_path], dtype=object),
        'entry_hint_mode': np.array([entry_hint_mode], dtype=object),
        'eval_horizon': np.array([eval_horizon], dtype=object),
        'dataset_length': np.array([dataset_length], dtype=np.int64),
        'start_index': np.array([start_index], dtype=np.int64),
        'pnl_stride': np.array([pnl_stride], dtype=np.int64),
        'bars_loaded': np.array([int(df.height)], dtype=np.int64),
        'real_bars_loaded': np.array([real_bar_count], dtype=np.int64),
        'forward_target_padding_bars': np.array(
            [TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS],
            dtype=np.int64,
        ),
        'forward_target_padding_site': np.array([TRADE_RESEARCH_FORWARD_TARGET_PADDING_SITE], dtype=object),
        'real_last_start_trade_id': np.array([real_last_trade_id], dtype=np.int64),
        'level0_rows': np.array([level0_height], dtype=np.int64),
        'last_bar_start_trade_id': np.array([last_bar_start_trade_id], dtype=np.int64),
        'research_limit': np.array([research_limit], dtype=np.int64),
        'required_rows': np.array([required_rows], dtype=np.int64),
        'train_size': np.array([train_size], dtype=np.int64),
        'train_size_ratio': np.array([train_size_ratio], dtype=np.float64),
        'payload_mode': np.array(['train'], dtype=object),
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

    all_npz_sample_indices = merged_payload['sample_index'].astype(np.int64).tolist()
    extended_train_sample_index_map = _extend_train_sample_index_map(
        sample_indices=all_npz_sample_indices,
        train_sample_index_by_inference_sample=train_sample_index_by_inference_sample,
        start_index=start_index,
        inference_level0_to_raw=level0_to_raw_row_indices,
        raw_to_train_level0_row=raw_to_train_level0_row,
    )
    _backfill_train_split_fields(
        merged_payload=merged_payload,
        train_sample_index_by_inference_sample=extended_train_sample_index_map,
        train_size=train_size,
        train_size_ratio=train_size_ratio,
    )

    merged_payload = _backfill_pred_price_columns(
        payload=merged_payload,
        eval_horizon=eval_horizon,
    )

    atomic_write_npz(npz_path, **merged_payload)

    sample_count = int(merged_payload['sample_index'].shape[0])
    last_sample_index = int(np.max(merged_payload['sample_index']))
    write_trade_research_meta(
        symbol_id=symbol_id,
        eval_horizon=eval_horizon,
        payload={
            'status': 'ok',
            'eval_horizon': eval_horizon,
            'pnl_stride': pnl_stride,
            'research_limit': research_limit,
            'required_rows': required_rows,
            'bars_loaded': int(df.height),
            'real_bars_loaded': real_bar_count,
            'forward_target_padding_bars': TRADE_RESEARCH_FORWARD_TARGET_PADDING_BARS,
            'forward_target_padding_site': TRADE_RESEARCH_FORWARD_TARGET_PADDING_SITE,
            'real_last_start_trade_id': real_last_trade_id,
            'level0_rows': level0_height,
            'dataset_length': dataset_length,
            'start_index': start_index,
            'sample_count': sample_count,
            'last_sample_index': last_sample_index,
            'last_bar_start_trade_id': last_bar_start_trade_id,
            'run_label': run_label,
            'checkpoint_path': checkpoint_path,
            'policy_path': policy_path,
            'entry_hint_mode': entry_hint_mode,
            'train_size': train_size,
            'train_size_ratio': train_size_ratio,
            'sample_selection_note': sample_selection_note,
            'payload_mode': 'train',
        },
    )


def run_trade_research_export_safe(symbol_id: str) -> None:
    eval_horizon: str | None = None
    try:
        metadata = fetch_inference_metadata()
        eval_horizon = inference_stack_fingerprint(metadata, symbol_id)['eval_horizon']
        run_trade_research_export(symbol_id=symbol_id)
    except Exception as exception:
        logger.error(
            'Trade research export failed: %s',
            ''.join(traceback.format_exception(exception)),
        )
        if eval_horizon is not None:
            write_trade_research_meta(
                symbol_id=symbol_id,
                eval_horizon=eval_horizon,
                payload={
                    'status': 'error',
                    'error_message': str(exception),
                },
            )
        raise
