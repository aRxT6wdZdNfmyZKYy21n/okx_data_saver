import logging
import pickle
import traceback
from functools import lru_cache

import httpx
import polars
from fastapi import HTTPException
from omegaconf import OmegaConf

from enumerations import SymbolId
from main.web_gui.data_service import fetch_last_bars
from settings import settings
from trading_bot_dataset.src.dataset import HybridTradeDataset

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def fetch_inference_metadata() -> dict:
    response = httpx.get(
        f'{settings.WEB_GUI_INFERENCE_API_BASE_URL}/metadata',
        timeout=30.0,
    )
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()


def _prepare_payload_dict_from_df(df: polars.DataFrame) -> dict:
    metadata = fetch_inference_metadata()
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

    dataset = HybridTradeDataset(
        dataframe=df,
        sequence_length=sequence_length,
        raw_columns=list(dataset_cfg['raw_cols']),
        static_columns=list(dataset_cfg['static_cols']),
        target_cols=list(dataset_cfg['target_cols']),
        aggregation_levels=list(dataset_cfg['aggregation_levels']),
        use_indicators=bool(dataset_cfg['use_indicators']),
        indicator_cols=list(dataset_cfg['indicator_cols']),
        model_config=model_cfg_omega,
        inference_mode=True,
    )

    last_index = len(dataset) - 1
    if last_index < 0:
        raise RuntimeError('No samples available after dataset preparation')

    logger.info(f'Last index: {last_index}')

    x_seq, x_static = dataset[last_index]
    normalized_x_seq: dict[str, object] = {}
    for scale_name, scale_tensor in x_seq.items():
        # Модель ожидает [batch, seq_len, features]; при инференсе из датасета обычно [seq_len, features].
        normalized_x_seq[scale_name] = scale_tensor.unsqueeze(0)

    return {
        'x_seq': normalized_x_seq,
        'x_static': x_static.unsqueeze(0),
    }


def _encode_payload(payload_dict: dict) -> bytes:
    logger.info('Encoding payload to Pickle...')

    return pickle.dumps(payload_dict)


def run_remote_inference(symbol_id: str, limit: int) -> dict[str, float]:
    if not settings.WEB_GUI_INFERENCE_ENABLED:
        raise HTTPException(status_code=503, detail='Inference is disabled')

    symbol = SymbolId[symbol_id]
    df = fetch_last_bars(symbol_id=symbol, limit=limit, offset=0)
    if df is None:
        raise HTTPException(status_code=422, detail='Недостаточно данных для инференса')

    metadata = fetch_inference_metadata()
    required_rows = int(metadata['sequence_length']) * int(metadata['max_scale'])
    if df.height < required_rows:
        raise HTTPException(
            status_code=422,
            detail=(
                'Инференс невозможен при таком количестве свечей x1 '
                f'(требуется минимум {required_rows}, получено {df.height})'
            ),
        )

    try:
        payload_dict = _prepare_payload_dict_from_df(df)
        encoded_payload = _encode_payload(payload_dict)
        response = httpx.post(
            f'{settings.WEB_GUI_INFERENCE_API_BASE_URL}/inference',
            content=encoded_payload,
            params={'symbol': symbol_id},
            headers={'Content-Type': 'application/octet-stream'},
            timeout=60.0,
        )
        if response.status_code == 404:
            raise HTTPException(status_code=404, detail=response.text)
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        return response.json()
    except HTTPException:
        raise
    except Exception as exception:
        logger.error(
            'Inference request failed: %s',
            ''.join(traceback.format_exception(exception)),
        )
        raise HTTPException(status_code=500, detail='Inference request failed') from exception
