import logging
import pickle
import traceback

import httpx
from fastapi import HTTPException

from enumerations import SymbolId
from main.web_gui.data_service import fetch_last_bars
from main.web_gui.inference_service import (
    fetch_inference_metadata,
    prepare_x_seq_2d_from_df,
)
from settings import settings

logger = logging.getLogger(__name__)


def build_exit_transformer_disabled_response() -> dict[str, object]:
    return {
        'enabled': False,
        'action': 'hold',
        'suggest_close': False,
        'predicted_delta_pnl': None,
        'delta_pnl_threshold': None,
        'min_hold_steps': None,
        'bars_held': None,
        'run_label': None,
        'checkpoint_path': None,
        'eval_horizon': None,
    }


def run_remote_exit_transformer(payload: dict[str, object]) -> dict[str, object]:
    if not settings.WEB_GUI_EXIT_TRANSFORMER_ENABLED:
        return build_exit_transformer_disabled_response()
    if not settings.WEB_GUI_INFERENCE_ENABLED:
        raise HTTPException(status_code=503, detail='Inference is disabled')

    symbol_id = str(payload['symbol_id'])
    bars_limit = int(payload['bars_limit'])
    symbol = SymbolId[symbol_id]
    df = fetch_last_bars(symbol_id=symbol, limit=bars_limit, offset=0)
    if df is None:
        raise HTTPException(status_code=422, detail='Недостаточно данных для exit transformer')

    metadata = fetch_inference_metadata()
    required_rows = int(metadata['sequence_length']) * int(metadata['max_scale'])
    if df.height < required_rows:
        raise HTTPException(
            status_code=422,
            detail=(
                'Exit transformer невозможен при таком количестве свечей x1 '
                f'(требуется минимум {required_rows}, получено {df.height})'
            ),
        )

    x_seq = prepare_x_seq_2d_from_df(df)
    return run_remote_exit_transformer_with_x_seq(
        payload=payload,
        x_seq=x_seq,
    )


def run_remote_exit_transformer_with_x_seq(
    payload: dict[str, object],
    x_seq: dict[str, object],
) -> dict[str, object]:
    if not settings.WEB_GUI_EXIT_TRANSFORMER_ENABLED:
        return build_exit_transformer_disabled_response()
    if not settings.WEB_GUI_INFERENCE_ENABLED:
        raise RuntimeError('Inference is disabled')

    symbol_id = str(payload['symbol_id'])
    try:
        request_payload: dict[str, object] = {
            'side': payload['side'],
            'eval_horizon': payload['eval_horizon'],
            'bars_held': payload['bars_held'],
            'entry_predictions': payload['entry_predictions'],
            'current_predictions': payload['current_predictions'],
            'unrealized_linear': payload['unrealized_linear'],
            'mfe_linear': payload['mfe_linear'],
            'mae_linear': payload['mae_linear'],
            'giveback_linear': payload['giveback_linear'],
            'x_seq': x_seq,
        }
        encoded_payload = pickle.dumps(request_payload)
        logger.info('Exit transformer payload size: %d', len(encoded_payload))

        response = httpx.post(
            f'{settings.WEB_GUI_INFERENCE_API_BASE_URL}/exit-transformer',
            content=encoded_payload,
            params={'symbol': symbol_id},
            headers={'Content-Type': 'application/octet-stream'},
            timeout=60.0,
        )
        if response.status_code == 404:
            raise RuntimeError(response.text)
        if response.status_code >= 400:
            raise RuntimeError(response.text)
        result = response.json()
        result['enabled'] = True
        return result
    except HTTPException:
        raise
    except Exception as exception:
        logger.error(
            'Exit transformer request failed: %s',
            ''.join(traceback.format_exception(exception)),
        )
        raise RuntimeError('Exit transformer request failed') from exception
