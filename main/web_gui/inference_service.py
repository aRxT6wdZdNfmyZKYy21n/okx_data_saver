import logging
import pickle
import traceback

import httpx
import polars
from fastapi import HTTPException
from omegaconf import OmegaConf

from enumerations import SymbolId
from main.web_gui.data_service import fetch_last_bars_sync
from settings import settings
from trading_bot_dataset.src.dataset import HybridTradeDataset, HybridTradeDatasetInference

logger = logging.getLogger(__name__)


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
) -> HybridTradeDatasetInference:
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

    return HybridTradeDatasetInference(
        dataframe=df,
        sequence_length=sequence_length,
        raw_columns=list(dataset_cfg['raw_cols']),
        static_columns=list(dataset_cfg['static_cols']),
        target_cols=list(dataset_cfg['target_cols']),
        aggregation_levels=list(dataset_cfg['aggregation_levels']),
        use_indicators=bool(dataset_cfg['use_indicators']),
        indicator_cols=list(dataset_cfg['indicator_cols']),
        model_config=model_cfg_omega,
    )


def _build_train_dataset(
    df: polars.DataFrame,
    metadata: dict,
) -> HybridTradeDataset:
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
    return HybridTradeDataset(
        dataframe=df,
        sequence_length=sequence_length,
        raw_columns=list(dataset_cfg['raw_cols']),
        static_columns=list(dataset_cfg['static_cols']),
        target_cols=list(dataset_cfg['target_cols']),
        aggregation_levels=list(dataset_cfg['aggregation_levels']),
        use_indicators=bool(dataset_cfg['use_indicators']),
        indicator_cols=list(dataset_cfg['indicator_cols']),
        model_config=model_cfg_omega,
        inference_mode=False,
    )


def _build_level0_to_raw_row_indices(
    raw_df: polars.DataFrame,
    level0_df: polars.DataFrame,
) -> list[int]:
    level0_log2 = level0_df['close_price_log2'].to_numpy()
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


def _build_train_level0_context(
    df: polars.DataFrame,
    metadata: dict,
) -> tuple[HybridTradeDataset, polars.DataFrame, dict[int, int]]:
    train_dataset = _build_train_dataset(df=df, metadata=metadata)
    train_level0_df = train_dataset.aggregated_data[0]
    train_level0_to_raw = _build_level0_to_raw_row_indices(df, train_level0_df)
    raw_to_train_level0_row: dict[int, int] = {}
    for train_row, raw_row in enumerate(train_level0_to_raw):
        raw_to_train_level0_row[raw_row] = train_row
    return train_dataset, train_level0_df, raw_to_train_level0_row


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


def _prepare_train_aligned_payload_dict(
    df: polars.DataFrame,
    metadata: dict,
    inference_dataset: HybridTradeDatasetInference,
    sample_index: int,
) -> dict[str, object]:
    train_dataset, _train_level0_df, raw_to_train_level0_row = _build_train_level0_context(
        df=df,
        metadata=metadata,
    )
    start_index = int(inference_dataset.dataset.start_index)
    level0_df = inference_dataset.dataset.aggregated_data[0]
    inference_level0_to_raw = _build_level0_to_raw_row_indices(df, level0_df)
    train_sample_index = _train_sample_index_for_inference_sample(
        sample_index=sample_index,
        start_index=start_index,
        inference_level0_to_raw=inference_level0_to_raw,
        raw_to_train_level0_row=raw_to_train_level0_row,
    )
    if train_sample_index is not None:
        if train_sample_index >= 0 and train_sample_index < len(train_dataset):
            logger.info(
                'Train-mode payload: inference sample %d -> train sample %d',
                sample_index,
                train_sample_index,
            )
            return _prepare_payload_dict_from_train_sample(
                train_dataset=train_dataset,
                train_sample_index=train_sample_index,
            )
    logger.warning(
        'No train-mode alignment for inference sample %d; using inference-mode tensors',
        sample_index,
    )
    return _prepare_payload_dict_from_sample(
        dataset=inference_dataset,
        sample_index=sample_index,
    )


def _prepare_payload_dict_from_df(df: polars.DataFrame) -> dict:
    metadata = fetch_inference_metadata()
    logger.info(
        'Dataset preparation start: inference rows=%d sequence_length=%d',
        int(df.height),
        int(metadata['sequence_length']),
    )
    inference_dataset = _build_dataset(df, metadata)
    logger.info(
        'Dataset preparation done: samples=%d start_index=%d',
        len(inference_dataset),
        int(inference_dataset.dataset.start_index),
    )

    last_index = len(inference_dataset) - 1
    if last_index < 0:
        raise RuntimeError('No samples available after dataset preparation')

    logger.info('Last index: %d', last_index)
    return _prepare_train_aligned_payload_dict(
        df=df,
        metadata=metadata,
        inference_dataset=inference_dataset,
        sample_index=last_index,
    )


def prepare_x_seq_2d_from_df(df: polars.DataFrame) -> dict[str, object]:
    payload_dict = _prepare_payload_dict_from_df(df)
    x_seq_raw = payload_dict['x_seq']
    if not isinstance(x_seq_raw, dict):
        raise RuntimeError('Prepared payload x_seq must be a dict')
    x_seq_2d: dict[str, object] = {}
    for scale_name, scale_tensor in x_seq_raw.items():
        tensor_2d = scale_tensor.squeeze(0).clone()
        x_seq_2d[str(scale_name)] = tensor_2d
    return x_seq_2d


def _encode_payload(payload_dict: dict) -> bytes:
    logger.info('Encoding payload to Pickle...')
    return pickle.dumps(payload_dict)


def run_remote_inference(symbol_id: str, limit: int) -> dict[str, object]:
    if not settings.WEB_GUI_INFERENCE_ENABLED:
        raise HTTPException(status_code=503, detail='Inference is disabled')

    symbol = SymbolId[symbol_id]
    df = fetch_last_bars_sync(symbol_id=symbol, limit=limit, offset=0)
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


def run_remote_inference_from_df(
    symbol_id: str,
    df: polars.DataFrame,
) -> dict[str, object]:
    if not settings.WEB_GUI_INFERENCE_ENABLED:
        raise RuntimeError('Inference is disabled')

    metadata = fetch_inference_metadata()
    required_rows = int(metadata['sequence_length']) * int(metadata['max_scale'])
    if df.height < required_rows:
        raise RuntimeError(
            'Инференс невозможен при таком количестве свечей x1 '
            f'(требуется минимум {required_rows}, получено {df.height})',
        )

    payload_dict = _prepare_payload_dict_from_df(df)
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
