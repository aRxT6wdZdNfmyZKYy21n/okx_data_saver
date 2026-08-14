from __future__ import annotations

from main.web_gui.trading_inference_gate_common import (
    evaluate_trading_inference_gate,
    resolve_prediction_age_ms,
)


def _artifact(
    *,
    status: str,
    inference_completed_at_ms: int,
    with_predictions: bool,
) -> dict[str, object]:
    artifact: dict[str, object] = {
        'status': status,
        'inference_completed_at_ms': inference_completed_at_ms,
    }
    if with_predictions:
        artifact['predictions'] = {
            'target_close_return_signed_log2_x32': 0.001,
        }
    return artifact


def test_gate_allows_computing_when_predictions_fresh() -> None:
    now_ms = 1_000_000
    artifact = _artifact(
        status='computing',
        inference_completed_at_ms=now_ms - 120_000,
        with_predictions=True,
    )
    usable, reason, details = evaluate_trading_inference_gate(
        artifact=artifact,
        max_prediction_age_ms=600_000,
        now_ms=now_ms,
    )
    assert usable is True
    assert reason == ''
    assert details['prediction_age_ms'] == 120_000


def test_gate_blocks_stale_predictions_even_if_status_ok() -> None:
    now_ms = 1_000_000
    artifact = _artifact(
        status='ok',
        inference_completed_at_ms=now_ms - 700_000,
        with_predictions=True,
    )
    usable, reason, details = evaluate_trading_inference_gate(
        artifact=artifact,
        max_prediction_age_ms=600_000,
        now_ms=now_ms,
    )
    assert usable is False
    assert reason == 'predictions_stale'
    assert details['prediction_age_ms'] == 700_000


def test_gate_blocks_inference_error() -> None:
    usable, reason, _details = evaluate_trading_inference_gate(
        artifact={
            'status': 'error',
            'error_message': 'boom',
            'predictions': {},
            'inference_completed_at_ms': 1,
        },
        max_prediction_age_ms=600_000,
        now_ms=2,
    )
    assert usable is False
    assert reason == 'inference_error'


def test_prediction_age_ms_never_negative() -> None:
    artifact = _artifact(
        status='ok',
        inference_completed_at_ms=2_000,
        with_predictions=True,
    )
    assert resolve_prediction_age_ms(artifact=artifact, now_ms=1_000) == 0
