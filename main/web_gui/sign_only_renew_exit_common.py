"""Sign-only rolling-H renew exit evaluation for live/paper trading."""

from __future__ import annotations

import math
from typing import Any


def prediction_key_for_horizon(horizon_name: str) -> str:
    return f'target_close_return_signed_log2_{horizon_name}'


def log2_return_to_linear(pred_log2: float) -> float:
    return math.pow(2.0, pred_log2) - 1.0


def extract_pred_eval_log2_from_predictions(
    predictions: dict[str, float],
    eval_horizon: str,
) -> float:
    prediction_key = prediction_key_for_horizon(eval_horizon)
    if prediction_key not in predictions:
        raise ValueError(
            f'predictions missing {prediction_key!r} for sign_only renew exit',
        )
    return float(predictions[prediction_key])


def sign_still_valid_for_side(side: str, pred_linear: float) -> bool:
    side_normalized = side.lower()
    if side_normalized == 'long':
        return pred_linear > 0.0
    if side_normalized == 'short':
        return pred_linear < 0.0
    raise ValueError(f'side must be long or short, got: {side!r}')


def evaluate_sign_only_checkpoint(
    side: str,
    bars_held: int,
    min_hold_steps: int,
    check_interval_steps: int,
    pred_log2: float,
    last_renew_segment_evaluated: int,
) -> tuple[bool, str, float, int, int, bool]:
    pred_linear = log2_return_to_linear(pred_log2)
    current_renew_segment = bars_held // check_interval_steps
    pending_segment_eval = (
        bars_held >= min_hold_steps
        and current_renew_segment > last_renew_segment_evaluated
    )
    updated_last_renew_segment_evaluated = last_renew_segment_evaluated
    if pending_segment_eval:
        updated_last_renew_segment_evaluated = current_renew_segment

    if bars_held < min_hold_steps:
        return (
            False,
            'before_min_hold',
            pred_linear,
            last_renew_segment_evaluated,
            current_renew_segment,
            False,
        )
    if not pending_segment_eval:
        return (
            False,
            'between_renew_checkpoints',
            pred_linear,
            last_renew_segment_evaluated,
            current_renew_segment,
            False,
        )
    if sign_still_valid_for_side(side, pred_linear):
        return (
            False,
            'sign_valid_renewed',
            pred_linear,
            updated_last_renew_segment_evaluated,
            current_renew_segment,
            True,
        )
    return (
        True,
        'sign_flip_at_checkpoint',
        pred_linear,
        updated_last_renew_segment_evaluated,
        current_renew_segment,
        True,
    )


def build_sign_only_exit_policy_response(
    side: str,
    eval_horizon: str,
    min_hold_steps: int,
    check_interval_steps: int,
    bars_held: int,
    pred_log2: float,
    last_renew_segment_evaluated: int,
    eval_source: str,
) -> dict[str, Any]:
    (
        suggest_close,
        exit_reason,
        pred_linear,
        updated_last_renew_segment_evaluated,
        current_renew_segment,
        at_renew_checkpoint,
    ) = evaluate_sign_only_checkpoint(
        side=side,
        bars_held=bars_held,
        min_hold_steps=min_hold_steps,
        check_interval_steps=check_interval_steps,
        pred_log2=pred_log2,
        last_renew_segment_evaluated=last_renew_segment_evaluated,
    )
    sign_still_valid = sign_still_valid_for_side(side, pred_linear)
    segment_baseline = last_renew_segment_evaluated
    if segment_baseline < 0:
        segment_baseline = 0
    segments_crossed = 0
    if at_renew_checkpoint:
        segments_crossed = current_renew_segment - segment_baseline
    return {
        'mode': 'rolling_h_renew_sign_only',
        'configured_eval_horizon': eval_horizon,
        'request_eval_horizon': eval_horizon,
        'min_hold_steps': min_hold_steps,
        'check_interval_steps': check_interval_steps,
        'bars_held': bars_held,
        'current_renew_segment': current_renew_segment,
        'last_renew_segment_evaluated': updated_last_renew_segment_evaluated,
        'segments_crossed': segments_crossed,
        'at_renew_checkpoint': at_renew_checkpoint,
        'pred_eval_log2': pred_log2,
        'pred_eval_linear': pred_linear,
        'sign_still_valid': sign_still_valid,
        'exit_reason': exit_reason,
        'suggest_close': suggest_close,
        'action': 'close' if suggest_close else 'hold',
        'action_id': 1 if suggest_close else 0,
        'eval_source': eval_source,
        'note': (
            'rolling_h_renew_sign_only: evaluate pending renew segments using '
            'latest available predictions; close when pred sign flips'
        ),
        'run_label': 'rolling_h_renew_sign_only',
        'eval_horizon': eval_horizon,
        'policy_path': '',
        'close_probability': 1.0 if suggest_close else 0.0,
        'close_probability_threshold': 0.5,
    }
