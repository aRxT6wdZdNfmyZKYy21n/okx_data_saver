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


def resolve_bar_timestamp_ms(
    artifact: dict[str, Any],
) -> int | None:
    if 'bar_timestamp_ms' in artifact:
        return int(artifact['bar_timestamp_ms'])
    return None


def resolve_inference_age_ms(
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


def resolve_bar_age_ms(
    artifact: dict[str, Any],
    now_ms: int,
) -> int | None:
    bar_timestamp_ms = resolve_bar_timestamp_ms(artifact)
    if bar_timestamp_ms is None:
        return None
    age_ms = now_ms - bar_timestamp_ms
    if age_ms < 0:
        return 0
    return age_ms


def resolve_prediction_age_ms(
    artifact: dict[str, Any],
    now_ms: int,
) -> int | None:
    return resolve_inference_age_ms(
        artifact=artifact,
        now_ms=now_ms,
    )


def evaluate_trading_inference_gate(
    artifact: dict[str, Any] | None,
    max_inference_age_ms: int,
    max_bar_age_ms: int,
    now_ms: int,
) -> tuple[bool, str, dict[str, Any]]:
    if max_inference_age_ms <= 0:
        raise ValueError(
            f'max_inference_age_ms must be positive, got: {max_inference_age_ms}',
        )
    if max_bar_age_ms <= 0:
        raise ValueError(
            f'max_bar_age_ms must be positive, got: {max_bar_age_ms}',
        )
    if artifact is None:
        return False, 'missing_artifact', {}

    details: dict[str, Any] = {}
    if 'status' in artifact:
        details['artifact_status'] = str(artifact['status'])

    inference_completed_at_ms = resolve_inference_completed_at_ms(artifact)
    if inference_completed_at_ms is not None:
        details['inference_completed_at_ms'] = inference_completed_at_ms

    inference_age_ms = resolve_inference_age_ms(
        artifact=artifact,
        now_ms=now_ms,
    )
    if inference_age_ms is not None:
        details['inference_age_ms'] = inference_age_ms
        details['max_inference_age_ms'] = max_inference_age_ms

    bar_timestamp_ms = resolve_bar_timestamp_ms(artifact)
    if bar_timestamp_ms is not None:
        details['bar_timestamp_ms'] = bar_timestamp_ms

    bar_age_ms = resolve_bar_age_ms(
        artifact=artifact,
        now_ms=now_ms,
    )
    if bar_age_ms is not None:
        details['bar_age_ms'] = bar_age_ms
        details['max_bar_age_ms'] = max_bar_age_ms

    if 'status' in artifact and str(artifact['status']) == 'error':
        if 'error_message' in artifact:
            details['error_message'] = str(artifact['error_message'])
        return False, 'inference_error', details

    if 'predictions' not in artifact or not isinstance(artifact['predictions'], dict):
        return False, 'missing_predictions', details

    if inference_completed_at_ms is None:
        return False, 'missing_inference_completed_at_ms', details

    if inference_age_ms is None:
        return False, 'missing_inference_completed_at_ms', details

    if inference_age_ms > max_inference_age_ms:
        return False, 'inference_stale', details

    if bar_timestamp_ms is None:
        return False, 'missing_bar_timestamp_ms', details

    if bar_age_ms is None:
        return False, 'missing_bar_timestamp_ms', details

    if bar_age_ms > max_bar_age_ms:
        return False, 'bar_stale', details

    return True, '', details
