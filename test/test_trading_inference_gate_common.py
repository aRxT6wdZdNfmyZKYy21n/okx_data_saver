from __future__ import annotations

from main.web_gui.trading_inference_gate_common import (
    evaluate_trading_inference_gate,
    resolve_bar_age_ms,
    resolve_inference_age_ms,
)


def _artifact(
    *,
    status: str,
    inference_completed_at_ms: int,
    bar_timestamp_ms: int,
    with_predictions: bool,
) -> dict[str, object]:
    artifact: dict[str, object] = {
        'status': status,
        'inference_completed_at_ms': inference_completed_at_ms,
        'bar_timestamp_ms': bar_timestamp_ms,
    }
    if with_predictions:
        artifact['predictions'] = {
            'target_close_return_signed_log2_x32': 0.001,
        }
    return artifact


def test_gate_allows_fresh_inference_and_bar() -> None:
    now_ms = 1_000_000
    artifact = _artifact(
        status='computing',
        inference_completed_at_ms=now_ms - 30_000,
        bar_timestamp_ms=now_ms - 240_000,
        with_predictions=True,
    )
    usable, reason, details = evaluate_trading_inference_gate(
        artifact=artifact,
        max_inference_age_ms=60_000,
        max_bar_age_ms=300_000,
        now_ms=now_ms,
    )
    assert usable is True
    assert reason == ''
    assert details['inference_age_ms'] == 30_000
    assert details['bar_age_ms'] == 240_000


def test_gate_blocks_stale_inference() -> None:
    now_ms = 1_000_000
    artifact = _artifact(
        status='ok',
        inference_completed_at_ms=now_ms - 120_000,
        bar_timestamp_ms=now_ms - 120_000,
        with_predictions=True,
    )
    usable, reason, details = evaluate_trading_inference_gate(
        artifact=artifact,
        max_inference_age_ms=60_000,
        max_bar_age_ms=300_000,
        now_ms=now_ms,
    )
    assert usable is False
    assert reason == 'inference_stale'
    assert details['inference_age_ms'] == 120_000


def test_gate_blocks_stale_bar_even_if_inference_fresh() -> None:
    now_ms = 1_000_000
    artifact = _artifact(
        status='ok',
        inference_completed_at_ms=now_ms - 30_000,
        bar_timestamp_ms=now_ms - 400_000,
        with_predictions=True,
    )
    usable, reason, details = evaluate_trading_inference_gate(
        artifact=artifact,
        max_inference_age_ms=60_000,
        max_bar_age_ms=300_000,
        now_ms=now_ms,
    )
    assert usable is False
    assert reason == 'bar_stale'
    assert details['bar_age_ms'] == 400_000


def test_gate_blocks_inference_error() -> None:
    usable, reason, _details = evaluate_trading_inference_gate(
        artifact={
            'status': 'error',
            'error_message': 'boom',
            'predictions': {},
            'inference_completed_at_ms': 1,
            'bar_timestamp_ms': 1,
        },
        max_inference_age_ms=60_000,
        max_bar_age_ms=300_000,
        now_ms=2,
    )
    assert usable is False
    assert reason == 'inference_error'


def test_inference_age_ms_never_negative() -> None:
    artifact = _artifact(
        status='ok',
        inference_completed_at_ms=2_000,
        bar_timestamp_ms=2_000,
        with_predictions=True,
    )
    assert resolve_inference_age_ms(artifact=artifact, now_ms=1_000) == 0
    assert resolve_bar_age_ms(artifact=artifact, now_ms=1_000) == 0
