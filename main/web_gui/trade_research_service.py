"""
Online non-overlapping trade research @ eval horizon for Web GUI overlays.

Loads full x1 history (WEB_GUI_TRADE_RESEARCH_LIMIT) for tensor context, runs
batched inference on the entire non-overlapping grid @ step_bars, keeps only
segments with entry_hint.recommended_action in (long, short) after SNR + hybrid
gate, then returns those whose entry falls inside the visible chart window.
"""

from __future__ import annotations

import logging
import math
import traceback

import httpx
import polars
from fastapi import HTTPException

from enumerations import SymbolId
from main.web_gui.data_service import fetch_last_bars
from main.web_gui.inference_service import (
    _build_dataset,
    _encode_payload,
    _prepare_payload_dict_from_sample,
    fetch_inference_metadata,
)
from settings import settings

logger = logging.getLogger(__name__)

DEFAULT_EVAL_HORIZON = 'x2048'
DEFAULT_STEP_BARS = 2048
BATCH_CHUNK_SIZE = 32
BATCH_HTTP_TIMEOUT_SEC = 600.0


def horizon_steps_from_name(horizon_name: str) -> int:
    if not horizon_name.startswith('x'):
        raise ValueError(f'Invalid horizon name: {horizon_name!r}')
    return int(horizon_name[1:])


def _prediction_key_for_horizon(horizon_name: str) -> str:
    return f'target_close_return_signed_log2_{horizon_name}'


def _sample_indices_for_full_dataset(
    dataset_length: int,
    step_bars: int,
    horizon_steps: int,
) -> tuple[list[int], str | None]:
    max_sample_index = dataset_length - 1 - horizon_steps
    if max_sample_index < 0:
        return [], 'dataset too short for eval horizon'

    sample_indices = list(range(0, max_sample_index + 1, step_bars))
    return sample_indices, None


def _segment_visible_by_start_trade_id(
    entry_start_trade_id: int,
    visible_min_start_trade_id: int | None,
    visible_max_start_trade_id: int | None,
) -> bool:
    if visible_min_start_trade_id is not None:
        if entry_start_trade_id < visible_min_start_trade_id:
            return False
    if visible_max_start_trade_id is not None:
        if entry_start_trade_id > visible_max_start_trade_id:
            return False
    return True


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


def _recommended_entry_action(
    inference_result: dict[str, object],
) -> str | None:
    if 'entry_hint' not in inference_result:
        return None
    entry_hint = inference_result['entry_hint']
    if not isinstance(entry_hint, dict):
        return None
    if 'recommended_action' not in entry_hint:
        return None
    recommended_action = str(entry_hint['recommended_action'])
    if recommended_action not in ('long', 'short'):
        return None
    return recommended_action


def _pred_target_price(
    entry_price: float,
    pred_eval_log2: float,
) -> float:
    return float(entry_price * math.pow(2.0, pred_eval_log2))


def _row_value(row: dict[str, object], column_name: str) -> object:
    if column_name not in row:
        raise RuntimeError(f'Dataframe row missing column {column_name!r}')
    return row[column_name]


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


def _raw_bar_metadata(
    raw_df: polars.DataFrame,
    raw_row_index: int,
) -> dict[str, float | int]:
    row = raw_df.row(raw_row_index, named=True)
    return {
        'start_trade_id': int(_row_value(row, 'start_trade_id')),
        'start_timestamp_ms': int(_row_value(row, 'start_timestamp_ms')),
        'open_price': float(_row_value(row, 'open_price')),
        'close_price': float(_row_value(row, 'close_price')),
    }


def run_trade_research(
    symbol_id: str,
    eval_horizon: str,
    step_bars: int,
    visible_min_start_trade_id: int | None,
    visible_max_start_trade_id: int | None,
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
    research_limit = settings.WEB_GUI_TRADE_RESEARCH_LIMIT

    metadata = fetch_inference_metadata()
    required_rows = int(metadata['sequence_length']) * int(metadata['max_scale'])
    minimum_rows = required_rows + horizon_steps
    if research_limit < minimum_rows:
        raise HTTPException(
            status_code=422,
            detail=(
                'Trade research limit is below minimum x1 bars '
                f'(minimum {minimum_rows}, configured {research_limit})'
            ),
        )

    df = fetch_last_bars(symbol_id=symbol, limit=research_limit, offset=0)
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
    dataset_length = len(dataset)
    level0_df = dataset.dataset.aggregated_data[0]
    level0_height = int(level0_df.height)
    level0_to_raw_row_indices = _build_level0_to_raw_row_indices(df, level0_df)

    max_sample_index = dataset_length - 1 - horizon_steps
    if max_sample_index < 0:
        raise HTTPException(
            status_code=422,
            detail='Trade research: dataset too short for one full horizon segment',
        )

    sample_indices, sample_selection_note = _sample_indices_for_full_dataset(
        dataset_length=dataset_length,
        step_bars=step_bars,
        horizon_steps=horizon_steps,
    )

    logger.info(
        'Trade research: symbol=%s samples=%d step=%d horizon=%s '
        'research_limit=%d level0=%d raw_df=%d start=%d '
        'visible_trade_id=[%s,%s] note=%s',
        symbol_id,
        len(sample_indices),
        step_bars,
        eval_horizon,
        research_limit,
        level0_height,
        df.height,
        start_index,
        visible_min_start_trade_id,
        visible_max_start_trade_id,
        sample_selection_note,
    )

    segments: list[dict[str, object]] = []
    inference_results: list[tuple[int, dict[str, object]]] = []
    policy_trade_count = 0
    entry_allowed_count = 0

    if len(sample_indices) == 0:
        return {
            'symbol_id': symbol_id,
            'eval_horizon': eval_horizon,
            'step_bars': step_bars,
            'research_limit': research_limit,
            'required_rows': required_rows,
            'bars_loaded': int(df.height),
            'level0_rows': level0_height,
            'dataset_length': dataset_length,
            'start_index': start_index,
            'visible_min_start_trade_id': visible_min_start_trade_id,
            'visible_max_start_trade_id': visible_max_start_trade_id,
            'sample_count': 0,
            'trade_inference_count': 0,
            'entry_allowed_count': 0,
            'segment_count': 0,
            'segments': segments,
            'sample_selection_note': sample_selection_note,
        }

    try:
        total_chunks = (
            (len(sample_indices) + BATCH_CHUNK_SIZE - 1) // BATCH_CHUNK_SIZE
        )
        for chunk_index, chunk_start in enumerate(
            range(0, len(sample_indices), BATCH_CHUNK_SIZE),
        ):
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
            if (chunk_index + 1) % 10 == 0 or (chunk_index + 1) == total_chunks:
                logger.info(
                    'Trade research inference: %d/%d batches, %d/%d samples',
                    chunk_index + 1,
                    total_chunks,
                    len(inference_results),
                    len(sample_indices),
                )

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

            policy_trade_count = policy_trade_count + 1

            recommended_action = _recommended_entry_action(inference_result)
            if recommended_action is None:
                continue

            entry_allowed_count = entry_allowed_count + 1

            entry_bar_index = start_index + sample_index
            exit_bar_index = entry_bar_index + horizon_steps
            if exit_bar_index >= level0_height:
                continue

            entry_raw_index = level0_to_raw_row_indices[entry_bar_index]
            exit_raw_index = level0_to_raw_row_indices[exit_bar_index]
            entry_meta = _raw_bar_metadata(df, entry_raw_index)
            exit_meta = _raw_bar_metadata(df, exit_raw_index)
            entry_start_trade_id = int(entry_meta['start_trade_id'])

            if not _segment_visible_by_start_trade_id(
                entry_start_trade_id=entry_start_trade_id,
                visible_min_start_trade_id=visible_min_start_trade_id,
                visible_max_start_trade_id=visible_max_start_trade_id,
            ):
                continue

            entry_close = float(entry_meta['close_price'])
            entry_open = float(entry_meta['open_price'])
            exit_close = float(exit_meta['close_price'])

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
                    'entry_start_trade_id': entry_start_trade_id,
                    'exit_start_trade_id': int(exit_meta['start_trade_id']),
                    'entry_timestamp_ms': int(entry_meta['start_timestamp_ms']),
                    'exit_timestamp_ms': int(exit_meta['start_timestamp_ms']),
                    'entry_open': entry_open,
                    'entry_close': entry_close,
                    'exit_close': exit_close,
                    'pred_target_open': _pred_target_price(
                        entry_price=entry_open,
                        pred_eval_log2=pred_eval_log2,
                    ),
                    'pred_target_close': _pred_target_price(
                        entry_price=entry_close,
                        pred_eval_log2=pred_eval_log2,
                    ),
                    'pred_eval_log2': pred_eval_log2,
                    'policy_action': action,
                    'action': recommended_action,
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
        'research_limit': research_limit,
        'required_rows': required_rows,
        'bars_loaded': int(df.height),
        'level0_rows': level0_height,
        'dataset_length': dataset_length,
        'start_index': start_index,
        'visible_min_start_trade_id': visible_min_start_trade_id,
        'visible_max_start_trade_id': visible_max_start_trade_id,
        'sample_count': len(sample_indices),
        'trade_inference_count': policy_trade_count,
        'entry_allowed_count': entry_allowed_count,
        'segment_count': len(segments),
        'segments': segments,
        'sample_selection_note': sample_selection_note,
    }
