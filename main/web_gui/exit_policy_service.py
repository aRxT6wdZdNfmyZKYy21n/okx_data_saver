import logging
import traceback

import httpx
from fastapi import HTTPException

from main.web_gui.trade_journal_service import apply_last_renew_segment_evaluated
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
    use_exit_stack = False
    if 'exit_stack_mode' in payload:
        use_exit_stack = bool(payload['exit_stack_mode'])
    if not settings.WEB_GUI_EXIT_GBM_ENABLED and not use_exit_stack:
        return build_exit_policy_disabled_response()
    if not settings.WEB_GUI_INFERENCE_ENABLED:
        raise HTTPException(status_code=503, detail='Inference is disabled')

    symbol_id = str(payload['symbol_id'])
    request_body: dict[str, object] = {
        'symbol': symbol_id,
        'side': payload['side'],
        'eval_horizon': payload['eval_horizon'],
        'bars_held': payload['bars_held'],
    }
    if 'current_predictions' in payload:
        request_body['current_predictions'] = payload['current_predictions']
    if 'last_renew_segment_evaluated' in payload:
        request_body['last_renew_segment_evaluated'] = payload[
            'last_renew_segment_evaluated'
        ]
    if use_exit_stack:
        if 'entry_predictions' in payload:
            request_body['entry_predictions'] = payload['entry_predictions']
        if 'entry_policy' in payload:
            request_body['entry_policy'] = payload['entry_policy']
        if 'current_policy' in payload:
            request_body['current_policy'] = payload['current_policy']
        if 'unrealized_linear' in payload:
            request_body['unrealized_linear'] = payload['unrealized_linear']
        if 'mfe_linear' in payload:
            request_body['mfe_linear'] = payload['mfe_linear']
        if 'mae_linear' in payload:
            request_body['mae_linear'] = payload['mae_linear']
        if 'giveback_linear' in payload:
            request_body['giveback_linear'] = payload['giveback_linear']
    else:
        request_body['entry_predictions'] = payload['entry_predictions']
        request_body['current_predictions'] = payload['current_predictions']
        request_body['entry_policy'] = payload['entry_policy']
        request_body['current_policy'] = payload['current_policy']
        request_body['unrealized_linear'] = payload['unrealized_linear']
        request_body['mfe_linear'] = payload['mfe_linear']
        request_body['mae_linear'] = payload['mae_linear']
        request_body['giveback_linear'] = payload['giveback_linear']
    try:
        response = httpx.post(
            f'{settings.WEB_GUI_INFERENCE_API_BASE_URL}/exit-policy',
            json=request_body,
            timeout=30.0,
        )
        if response.status_code == 404:
            raise HTTPException(status_code=404, detail=response.text)
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        result = response.json()
        if (
            use_exit_stack
            and 'last_renew_segment_evaluated' in result
        ):
            apply_last_renew_segment_evaluated(
                int(result['last_renew_segment_evaluated']),
            )
        return result
    except HTTPException:
        raise
    except Exception as exception:
        logger.error(
            'Exit policy request failed: %s',
            ''.join(traceback.format_exception(exception)),
        )
        raise HTTPException(status_code=500, detail='Exit policy request failed') from exception
