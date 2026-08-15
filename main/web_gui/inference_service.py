import logging
import pickle
import traceback

import httpx
import numpy as np
import polars
import torch
from fastapi import HTTPException
from omegaconf import OmegaConf

from enumerations import SymbolId
from main.web_gui.data_service import fetch_last_bars_sync
from main.web_gui.trade_research_dataset_common import (
    last_real_sample_index,
)
from settings import settings
from trading_bot_dataset.src.dataset import HybridTradeDataset, HybridTradeDatasetInference
from trading_bot_dataset.src.volume_windows import extract_volume_windows_config

logger = logging.getLogger(__name__)


def _ensure_trading_bot_root_on_path() -> None:
    from main.offline_inference.trading_bot_imports import ensure_trading_bot_on_path

    ensure_trading_bot_on_path()


def _volume_windows_config_from_metadata(metadata: dict) -> dict[str, object] | None:
    dataset_cfg = metadata['dataset_config']
    dataset_cfg_omega = OmegaConf.create(dataset_cfg)
    return extract_volume_windows_config(dataset_cfg_omega)


def _hybrid_dataset_kwargs_from_metadata(metadata: dict) -> dict[str, object]:
    sequence_length = int(metadata['sequence_length'])
    dataset_cfg = metadata['dataset_config']
    model_cfg = metadata['model_config']
    model_cfg_omega = OmegaConf.create(
        {
            'params': {
                'scale_features': model_cfg['scale_features'],
            },
        },
    )
    return {
        'sequence_length': sequence_length,
        'raw_columns': list(dataset_cfg['raw_cols']),
        'static_columns': list(dataset_cfg['static_cols']),
        'target_cols': list(dataset_cfg['target_cols']),
        'aggregation_levels': list(dataset_cfg['aggregation_levels']),
        'use_indicators': bool(dataset_cfg['use_indicators']),
        'indicator_cols': list(dataset_cfg['indicator_cols']),
        'model_config': model_cfg_omega,
        'volume_windows_config': _volume_windows_config_from_metadata(metadata),
        'range_hold_cap_multiplier': (
            int(dataset_cfg['range_hold_cap_multiplier'])
            if 'range_hold_cap_multiplier' in dataset_cfg
            and dataset_cfg['range_hold_cap_multiplier'] is not None
            else None
        ),
        'coarse_phase_mode': (
            str(dataset_cfg['coarse_phase_mode'])
            if 'coarse_phase_mode' in dataset_cfg
            else 'fixed'
        ),
        'coarse_phase_offset': (
            int(dataset_cfg['coarse_phase_offset'])
            if 'coarse_phase_offset' in dataset_cfg
            else 0
        ),
        'coarse_phase_offset_max': (
            int(dataset_cfg['coarse_phase_offset_max'])
            if 'coarse_phase_offset_max' in dataset_cfg
            and dataset_cfg['coarse_phase_offset_max'] is not None
            else None
        ),
    }


def fetch_inference_metadata() -> dict:
    response = httpx.get(
        f'{settings.WEB_GUI_INFERENCE_API_BASE_URL}/metadata',
        timeout=30.0,
    )
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()


def _build_dataset(
    df: polars.DataFrame,
    metadata: dict,
    inference_build_sample_index: int | None,
) -> HybridTradeDatasetInference:
    _ensure_trading_bot_root_on_path()
    dataset_kwargs = _hybrid_dataset_kwargs_from_metadata(metadata)
    return HybridTradeDatasetInference(
        dataframe=df,
        inference_build_sample_index=inference_build_sample_index,
        source_data_path=None,
        **dataset_kwargs,
    )


def _build_train_dataset(
    df: polars.DataFrame,
    metadata: dict,
) -> HybridTradeDataset:
    _ensure_trading_bot_root_on_path()
    dataset_kwargs = _hybrid_dataset_kwargs_from_metadata(metadata)
    return HybridTradeDataset(
        dataframe=df,
        inference_mode=False,
        inference_build_sample_index=None,
        source_data_path=None,
        **dataset_kwargs,
    )


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


def _build_level0_to_raw_row_indices_from_dataset(
    raw_df: polars.DataFrame,
    dataset: HybridTradeDataset,
) -> list[int]:
    return _build_level0_to_raw_row_indices(
        raw_df=raw_df,
        level0_close_price_log2=dataset.level0_close_price_log2_numpy(),
    )


def _build_train_level0_context(
    df: polars.DataFrame,
    metadata: dict,
) -> tuple[HybridTradeDataset, dict[int, int]]:
    train_dataset = _build_train_dataset(df=df, metadata=metadata)
    train_level0_to_raw = _build_level0_to_raw_row_indices_from_dataset(
        raw_df=df,
        dataset=train_dataset,
    )
    raw_to_train_level0_row: dict[int, int] = {}
    for train_row, raw_row in enumerate(train_level0_to_raw):
        raw_to_train_level0_row[raw_row] = train_row
    return train_dataset, raw_to_train_level0_row


def _train_sample_index_for_inference_sample(
    sample_index: int,
    start_index: int,
    inference_level0_to_raw: list[int],
    raw_to_train_level0_row: dict[int, int],
) -> int | None:
    inference_entry_bar_index = start_index + sample_index
    raw_entry_row = inference_level0_to_raw[inference_entry_bar_index]
    if raw_entry_row not in raw_to_train_level0_row:
        return None
    train_entry_row = raw_to_train_level0_row[raw_entry_row]
    return train_entry_row - start_index


def _prepare_payload_dict_from_sample(
    dataset: HybridTradeDatasetInference,
    sample_index: int,
) -> dict:
    x_seq, x_static = dataset[sample_index]
    normalized_x_seq: dict[str, object] = {}
    for scale_name, scale_tensor in x_seq.items():
        # Модель ожидает [batch, seq_len, features]; при инференсе из датасета обычно [seq_len, features].
        # Срезы из HybridTradeDataset — views на огромный level_tensor; pickle сериализует весь storage,
        # поэтому перед pickle нужен compact-клон, иначе размер пейлоада ~ O(число баров в датасете).
        normalized_x_seq[scale_name] = scale_tensor.unsqueeze(0).clone()

    return {
        'x_seq': normalized_x_seq,
        'x_static': x_static.unsqueeze(0).clone(),
    }


def _prepare_payload_dict_from_train_sample(
    train_dataset: HybridTradeDataset,
    train_sample_index: int,
) -> dict[str, object]:
    x_seq, x_static, _targets = train_dataset[train_sample_index]
    normalized_x_seq: dict[str, object] = {}
    for scale_name, scale_tensor in x_seq.items():
        normalized_x_seq[scale_name] = scale_tensor.unsqueeze(0).clone()
    return {
        'x_seq': normalized_x_seq,
        'x_static': x_static.unsqueeze(0).clone(),
    }


def _payload_dicts_from_batched_tensors(
    batched_x_seq: dict[str, object],
    batched_x_static: object,
) -> list[dict[str, object]]:
    if not isinstance(batched_x_static, torch.Tensor):
        raise RuntimeError('batched_x_static must be a tensor')
    batch_size = int(batched_x_static.shape[0])
    payloads: list[dict[str, object]] = []
    for batch_index in range(batch_size):
        normalized_x_seq: dict[str, object] = {}
        for scale_name, scale_tensor in batched_x_seq.items():
            if not isinstance(scale_tensor, torch.Tensor):
                raise RuntimeError('batched x_seq values must be tensors')
            normalized_x_seq[scale_name] = (
                scale_tensor[batch_index:batch_index + 1].clone()
            )
        payloads.append(
            {
                'x_seq': normalized_x_seq,
                'x_static': batched_x_static[batch_index:batch_index + 1].clone(),
            },
        )
    return payloads


def x_seq_2d_from_payload_dict(payload_dict: dict) -> dict[str, object]:
    x_seq_raw = payload_dict['x_seq']
    if not isinstance(x_seq_raw, dict):
        raise RuntimeError('Prepared payload x_seq must be a dict')
    x_seq_2d: dict[str, object] = {}
    for scale_name, scale_tensor in x_seq_raw.items():
        tensor_2d = scale_tensor.squeeze(0).clone()
        x_seq_2d[str(scale_name)] = tensor_2d
    return x_seq_2d


def _bar_metadata_from_df_row(
    df: polars.DataFrame,
    row_index: int,
) -> dict[str, int | float]:
    row = df.row(row_index, named=True)
    return {
        'bar_start_trade_id': int(row['start_trade_id']),
        'bar_timestamp_ms': int(row['start_timestamp_ms']),
        'bar_close_price': float(row['close_price']),
    }


def _prepare_inference_context_from_df(
    df: polars.DataFrame,
    metadata: dict,
) -> tuple[dict, dict[str, int | float]]:
    real_bar_count = int(df.height)
    logger.info(
        'Dataset preparation start: rows=%d sequence_length=%d',
        real_bar_count,
        int(metadata['sequence_length']),
    )
    inference_dataset = _build_dataset(
        df,
        metadata,
        inference_build_sample_index=-1,
    )
    return _prepare_inference_context_from_dataset(
        df=df,
        metadata=metadata,
        inference_dataset=inference_dataset,
    )


def _prepare_inference_context_from_dataset(
    df: polars.DataFrame,
    metadata: dict,
    inference_dataset: HybridTradeDatasetInference,
) -> tuple[dict, dict[str, int | float]]:
    real_bar_count = int(df.height)
    start_index = int(inference_dataset.dataset.start_index)
    logger.info(
        'Dataset preparation done: samples=%d start_index=%d',
        len(inference_dataset),
        start_index,
    )

    hybrid_dataset = inference_dataset.dataset
    level0_to_raw = _build_level0_to_raw_row_indices_from_dataset(
        raw_df=df,
        dataset=hybrid_dataset,
    )
    last_index = last_real_sample_index(
        dataset_length=len(inference_dataset),
        start_index=start_index,
        level0_to_raw=level0_to_raw,
        real_bar_count=real_bar_count,
    )
    raw_entry_row = level0_to_raw[start_index + last_index]
    db_latest_row_index = real_bar_count - 1
    entry_bar_metadata = _bar_metadata_from_df_row(df, raw_entry_row)
    db_latest_bar_metadata = _bar_metadata_from_df_row(df, db_latest_row_index)
    model_lag_bars = db_latest_row_index - raw_entry_row
    logger.info(
        'Last real-bar sample: index=%d raw_row=%d (real_bar_count=%d) '
        'model_lag_bars=%d level0_rows=%d',
        last_index,
        raw_entry_row,
        real_bar_count,
        model_lag_bars,
        hybrid_dataset.level0_row_count(),
    )

    payload_dict = _prepare_payload_dict_from_sample(
        dataset=inference_dataset,
        sample_index=last_index,
    )
    provenance: dict[str, int | float] = {
        'sample_index': last_index,
        'start_index': start_index,
        'raw_entry_row': raw_entry_row,
        'real_bar_count': real_bar_count,
        'level0_rows': hybrid_dataset.level0_row_count(),
        'model_lag_bars': model_lag_bars,
        'bar_start_trade_id': entry_bar_metadata['bar_start_trade_id'],
        'bar_timestamp_ms': entry_bar_metadata['bar_timestamp_ms'],
        'bar_close_price': entry_bar_metadata['bar_close_price'],
        'db_latest_start_trade_id': db_latest_bar_metadata['bar_start_trade_id'],
        'db_latest_timestamp_ms': db_latest_bar_metadata['bar_timestamp_ms'],
        'db_latest_close_price': db_latest_bar_metadata['bar_close_price'],
    }
    return payload_dict, provenance


def prepare_inference_payload_from_df(df: polars.DataFrame) -> dict:
    metadata = fetch_inference_metadata()
    required_rows = int(metadata['sequence_length']) * int(metadata['max_scale'])
    if df.height < required_rows:
        raise RuntimeError(
            'Инференс невозможен при таком количестве свечей x1 '
            f'(требуется минимум {required_rows}, получено {df.height})',
        )
    payload_dict, _provenance = _prepare_inference_context_from_df(df, metadata)
    return payload_dict


def prepare_x_seq_2d_from_df(df: polars.DataFrame) -> dict[str, object]:
    payload_dict = prepare_inference_payload_from_df(df)
    return x_seq_2d_from_payload_dict(payload_dict)


def _encode_payload(payload_dict: dict) -> bytes:
    logger.info('Encoding payload to Pickle...')
    return pickle.dumps(payload_dict)


def run_remote_inference(symbol_id: str, limit: int) -> dict[str, object]:
    if not settings.WEB_GUI_INFERENCE_ENABLED:
        raise HTTPException(status_code=503, detail='Inference is disabled')

    symbol = SymbolId[symbol_id]
    df = fetch_last_bars_sync(symbol_id=symbol, limit=limit, offset=0, since_start_trade_id=None)
    if df is None:
        raise HTTPException(status_code=422, detail='Недостаточно данных для инференса')

    try:
        return run_remote_inference_from_df(symbol_id=symbol_id, df=df)
    except HTTPException:
        raise
    except RuntimeError as exception:
        raise HTTPException(status_code=422, detail=str(exception)) from exception
    except Exception as exception:
        logger.error(
            'Inference request failed: %s',
            ''.join(traceback.format_exception(exception)),
        )
        raise HTTPException(status_code=500, detail='Inference request failed') from exception


def run_remote_inference_from_payload_dict(
    symbol_id: str,
    payload_dict: dict,
) -> dict[str, object]:
    if not settings.WEB_GUI_INFERENCE_ENABLED:
        raise RuntimeError('Inference is disabled')

    encoded_payload = _encode_payload(payload_dict)

    logger.info('Payload size: %d', len(encoded_payload))

    response = httpx.post(
        f'{settings.WEB_GUI_INFERENCE_API_BASE_URL}/inference',
        content=encoded_payload,
        params={'symbol': symbol_id},
        headers={'Content-Type': 'application/octet-stream'},
        timeout=60.0,
    )
    if response.status_code == 404:
        raise RuntimeError(response.text)
    if response.status_code >= 400:
        raise RuntimeError(response.text)
    return response.json()


def run_remote_inference_from_df(
    symbol_id: str,
    df: polars.DataFrame,
) -> dict[str, object]:
    payload_dict = prepare_inference_payload_from_df(df)
    return run_remote_inference_from_payload_dict(
        symbol_id=symbol_id,
        payload_dict=payload_dict,
    )


def run_remote_inference_and_x_seq_from_df(
    symbol_id: str,
    df: polars.DataFrame,
) -> tuple[dict[str, object], dict[str, object], dict[str, int | float]]:
    metadata = fetch_inference_metadata()
    required_rows = int(metadata['sequence_length']) * int(metadata['max_scale'])
    if df.height < required_rows:
        raise RuntimeError(
            'Инференс невозможен при таком количестве свечей x1 '
            f'(требуется минимум {required_rows}, получено {df.height})',
        )
    payload_dict, provenance = _prepare_inference_context_from_df(df, metadata)
    inference_result = run_remote_inference_from_payload_dict(
        symbol_id=symbol_id,
        payload_dict=payload_dict,
    )
    x_seq = x_seq_2d_from_payload_dict(payload_dict)
    return inference_result, x_seq, provenance
