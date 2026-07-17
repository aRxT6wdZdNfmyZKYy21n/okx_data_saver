import logging
import traceback

import httpx
from fastapi import HTTPException

from settings import settings

logger = logging.getLogger(__name__)


def build_exit_policy_disabled_response() -> dict[str, object]:
    return {
        'enabled': False,
        'action': 'hold',
        'suggest_close': False,
        'close_probability': None,
        'close_probability_threshold': None,
        'min_hold_steps': None,
        'bars_held': None,
        'run_label': None,
        'policy_path': None,
        'eval_horizon': None,
    }


def run_remote_exit_policy(payload: dict[str, object]) -> dict[str, object]:
    if not settings.WEB_GUI_EXIT_GBM_ENABLED:
        return build_exit_policy_disabled_response()
    if not settings.WEB_GUI_INFERENCE_ENABLED:
        raise HTTPException(status_code=503, detail='Inference is disabled')

    symbol_id = str(payload['symbol_id'])
    try:
        response = httpx.post(
            f'{settings.WEB_GUI_INFERENCE_API_BASE_URL}/exit-policy',
            json={
                'symbol': symbol_id,
                'side': payload['side'],
                'eval_horizon': payload['eval_horizon'],
                'bars_held': payload['bars_held'],
                'entry_predictions': payload['entry_predictions'],
                'current_predictions': payload['current_predictions'],
                'entry_policy': payload['entry_policy'],
                'current_policy': payload['current_policy'],
                'unrealized_linear': payload['unrealized_linear'],
                'mfe_linear': payload['mfe_linear'],
                'mae_linear': payload['mae_linear'],
                'giveback_linear': payload['giveback_linear'],
            },
            timeout=30.0,
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
            'Exit policy request failed: %s',
            ''.join(traceback.format_exception(exception)),
        )
        raise HTTPException(status_code=500, detail='Exit policy request failed') from exception
