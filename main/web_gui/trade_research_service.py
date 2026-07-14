"""
Online non-overlapping trade research @ eval horizon for Web GUI overlays.

Runs batched inference every `step_bars` (default x2048) on loaded x1 history.
"""

from __future__ import annotations

import logging
import math
import traceback

import httpx
import polars
from fastapi import HTTPException
from omegaconf import OmegaConf

from enumerations import SymbolId
from main.web_gui.data_service import fetch_last_bars
from main.web_gui.inference_service import (
    _build_dataset,
    _encode_payload,
    _prepare_payload_dict_from_sample,
    fetch_inference_metadata,
)
from settings import settings
from trading_bot_dataset.src.dataset import HybridTradeDatasetInference

logger = logging.getLogger(__name__)

DEFAULT_EVAL_HORIZON = 'x2048'
DEFAULT_STEP_BARS = 2048
MAX_SEGMENTS = 64
BATCH_CHUNK_SIZE = 4
BATCH_HTTP_TIMEOUT_SEC = 180.0


def horizon_steps_from_name(horizon_name: str) -> int:
    if not horizon_name.startswith('x'):
        raise ValueError(f'Invalid horizon name: {horizon_name!r}')
    return int(horizon_name[1:])


def _prediction_key_for_horizon(horizon_name: str) -> str:
    return f'target_close_return_signed_log2_{horizon_name}'


def _call_inference_batch_api(
    samples: list[dict[str, object]],
    symbol_id: str,
) -> list[dict[str, object]]:
    if len(samples) == 0:
        return []
    encoded_payload = _encode_payload({'samples': samples})
    response = httpx.post(
        f'{settings.WEB_GUI_INFERENCE_API_BASE_URL}/inference/batch',
        content=encoded_payload,
        params={'symbol': symbol_id},
        headers={'Content-Type': 'application/octet-stream'},
        timeout=BATCH_HTTP_TIMEOUT_SEC,
    )
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail=response.text)
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    payload = response.json()
    if 'results' not in payload:
        raise RuntimeError('Batch inference response missing results')
    results = payload['results']
    if not isinstance(results, list):
        raise RuntimeError('Batch inference results must be a list')
    return results


def _pred_target_close(
    entry_close: float,
    pred_eval_log2: float,
    action: str,
) -> float:
    if action == 'long':
        signed_log2 = pred_eval_log2
    elif action == 'short':
        signed_log2 = -pred_eval_log2
    else:
        signed_log2 = pred_eval_log2
    return float(entry_close * math.pow(2.0, signed_log2))


def _row_value(row: dict[str, object], column_name: str) -> object:
    if column_name not in row:
        raise RuntimeError(f'Dataframe row missing column {column_name!r}')
    return row[column_name]


def run_trade_research(
    symbol_id: str,
    limit: int,
    eval_horizon: str,
    step_bars: int,
) -> dict[str, object]:
    if not settings.WEB_GUI_INFERENCE_ENABLED:
        raise HTTPException(status_code=503, detail='Inference is disabled')

    horizon_steps = horizon_steps_from_name(eval_horizon)
    if step_bars != horizon_steps:
        raise HTTPException(
            status_code=422,
            detail=(
                f'Non-overlapping research requires step_bars={horizon_steps} '
                f'for eval_horizon={eval_horizon}, got {step_bars}'
            ),
        )

    symbol = SymbolId[symbol_id]
    effective_limit = min(limit, settings.WEB_GUI_RECORDS_LIMIT)
    metadata = fetch_inference_metadata()
    required_rows = int(metadata['sequence_length']) * int(metadata['max_scale'])
    minimum_rows = required_rows + horizon_steps
    if effective_limit < minimum_rows:
        raise HTTPException(
            status_code=422,
            detail=(
                'Trade research requires more x1 bars '
                f'(minimum {minimum_rows}, requested {effective_limit})'
            ),
        )

    df = fetch_last_bars(symbol_id=symbol, limit=effective_limit, offset=0)
    if df is None:
        raise HTTPException(status_code=422, detail='Недостаточно данных для trade research')
    if df.height < minimum_rows:
        raise HTTPException(
            status_code=422,
            detail=(
                'Trade research: fetched fewer x1 bars than required '
                f'({df.height} < {minimum_rows})'
            ),
        )

    dataset = _build_dataset(df, metadata)
    start_index = int(dataset.dataset.start_index)
    max_sample_index = len(dataset) - 1 - horizon_steps
    if max_sample_index < 0:
        raise HTTPException(
            status_code=422,
            detail='Trade research: dataset too short for one full horizon segment',
        )

    sample_indices = list(range(0, max_sample_index + 1, step_bars))
    if len(sample_indices) > MAX_SEGMENTS:
        sample_indices = sample_indices[-MAX_SEGMENTS:]

    logger.info(
        'Trade research: symbol=%s samples=%d step=%d horizon=%s',
        symbol_id,
        len(sample_indices),
        step_bars,
        eval_horizon,
    )

    segments: list[dict[str, object]] = []
    inference_results: list[tuple[int, dict[str, object]]] = []

    try:
        for chunk_start in range(0, len(sample_indices), BATCH_CHUNK_SIZE):
            chunk_sample_indices = sample_indices[
                chunk_start:chunk_start + BATCH_CHUNK_SIZE
            ]
            chunk_payloads = [
                _prepare_payload_dict_from_sample(dataset, sample_index)
                for sample_index in chunk_sample_indices
            ]
            chunk_results = _call_inference_batch_api(chunk_payloads, symbol_id)
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
                inference_results.append((sample_index, inference_result))

        prediction_key = _prediction_key_for_horizon(eval_horizon)
        for sample_index, inference_result in inference_results:
            if 'policy' not in inference_result:
                continue
            policy = inference_result['policy']
            if 'action' not in policy:
                continue
            action = str(policy['action'])
            if action not in ('long', 'short'):
                continue

            entry_bar_index = start_index + sample_index
            exit_bar_index = entry_bar_index + horizon_steps
            if exit_bar_index >= df.height:
                continue

            entry_row = df.row(entry_bar_index, named=True)
            exit_row = df.row(exit_bar_index, named=True)
            entry_close = float(_row_value(entry_row, 'close_price'))
            exit_close = float(_row_value(exit_row, 'close_price'))

            pred_eval_log2 = 0.0
            if 'predictions' in inference_result:
                predictions = inference_result['predictions']
                if prediction_key in predictions:
                    pred_eval_log2 = float(predictions[prediction_key])

            segments.append(
                {
                    'sample_index': int(sample_index),
                    'entry_bar_index': int(entry_bar_index),
                    'exit_bar_index': int(exit_bar_index),
                    'entry_start_trade_id': int(_row_value(entry_row, 'start_trade_id')),
                    'exit_start_trade_id': int(_row_value(exit_row, 'start_trade_id')),
                    'entry_timestamp_ms': int(_row_value(entry_row, 'start_timestamp_ms')),
                    'exit_timestamp_ms': int(_row_value(exit_row, 'start_timestamp_ms')),
                    'entry_close': entry_close,
                    'exit_close': exit_close,
                    'pred_target_close': _pred_target_close(
                        entry_close=entry_close,
                        pred_eval_log2=pred_eval_log2,
                        action=action,
                    ),
                    'pred_eval_log2': pred_eval_log2,
                    'action': action,
                },
            )
    except HTTPException:
        raise
    except Exception as exception:
        logger.error(
            'Trade research failed: %s',
            ''.join(traceback.format_exception(exception)),
        )
        raise HTTPException(status_code=500, detail='Trade research failed') from exception

    return {
        'symbol_id': symbol_id,
        'eval_horizon': eval_horizon,
        'step_bars': step_bars,
        'required_rows': required_rows,
        'bars_loaded': int(df.height),
        'sample_count': len(sample_indices),
        'segment_count': len(segments),
        'segments': segments,
    }
