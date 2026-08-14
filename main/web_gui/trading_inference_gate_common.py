"""Freshness gate for automated trading on inference artifacts."""

from __future__ import annotations

from typing import Any


def resolve_inference_completed_at_ms(
    artifact: dict[str, Any],
) -> int | None:
    if 'inference_completed_at_ms' in artifact:
        return int(artifact['inference_completed_at_ms'])
    if 'updated_at_ms' in artifact:
        return int(artifact['updated_at_ms'])
    return None


def resolve_prediction_age_ms(
    artifact: dict[str, Any],
    now_ms: int,
) -> int | None:
    inference_completed_at_ms = resolve_inference_completed_at_ms(artifact)
    if inference_completed_at_ms is None:
        return None
    age_ms = now_ms - inference_completed_at_ms
    if age_ms < 0:
        return 0
    return age_ms


def evaluate_trading_inference_gate(
    artifact: dict[str, Any] | None,
    max_prediction_age_ms: int,
    now_ms: int,
) -> tuple[bool, str, dict[str, Any]]:
    if max_prediction_age_ms <= 0:
        raise ValueError(
            f'max_prediction_age_ms must be positive, got: {max_prediction_age_ms}',
        )
    if artifact is None:
        return False, 'missing_artifact', {}

    details: dict[str, Any] = {}
    if 'status' in artifact:
        details['artifact_status'] = str(artifact['status'])
    inference_completed_at_ms = resolve_inference_completed_at_ms(artifact)
    if inference_completed_at_ms is not None:
        details['inference_completed_at_ms'] = inference_completed_at_ms
    prediction_age_ms = resolve_prediction_age_ms(
        artifact=artifact,
        now_ms=now_ms,
    )
    if prediction_age_ms is not None:
        details['prediction_age_ms'] = prediction_age_ms
        details['max_prediction_age_ms'] = max_prediction_age_ms

    if 'status' in artifact and str(artifact['status']) == 'error':
        if 'error_message' in artifact:
            details['error_message'] = str(artifact['error_message'])
        return False, 'inference_error', details

    if 'predictions' not in artifact or not isinstance(artifact['predictions'], dict):
        return False, 'missing_predictions', details

    if inference_completed_at_ms is None:
        return False, 'missing_inference_completed_at_ms', details

    if prediction_age_ms is None:
        return False, 'missing_inference_completed_at_ms', details

    if prediction_age_ms > max_prediction_age_ms:
        return False, 'predictions_stale', details

    return True, '', details
