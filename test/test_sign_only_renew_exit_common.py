import math

from main.web_gui.sign_only_renew_exit_common import (
    build_sign_only_exit_policy_response,
    evaluate_sign_only_checkpoint,
)


def _pred_log2_for_linear(linear_return: float) -> float:
    return float(math.log2(1.0 + linear_return))


def test_sign_only_checkpoint_closes_on_flip_with_latest_predictions() -> None:
    (
        suggest_close,
        exit_reason,
        _pred_linear,
        updated_last_renew,
        current_segment,
        at_checkpoint,
    ) = evaluate_sign_only_checkpoint(
        side='short',
        bars_held=159,
        min_hold_steps=32,
        check_interval_steps=32,
        pred_log2=_pred_log2_for_linear(0.0006),
        last_renew_segment_evaluated=-1,
    )
    assert suggest_close is True
    assert exit_reason == 'sign_flip_at_checkpoint'
    assert updated_last_renew == 4
    assert current_segment == 4
    assert at_checkpoint is True


def test_sign_only_checkpoint_renews_without_waiting_for_new_inference() -> None:
    (
        suggest_close,
        exit_reason,
        _pred_linear,
        updated_last_renew,
        _current_segment,
        at_checkpoint,
    ) = evaluate_sign_only_checkpoint(
        side='short',
        bars_held=536,
        min_hold_steps=32,
        check_interval_steps=32,
        pred_log2=_pred_log2_for_linear(-0.0013),
        last_renew_segment_evaluated=15,
    )
    assert suggest_close is False
    assert exit_reason == 'sign_valid_renewed'
    assert updated_last_renew == 16
    assert at_checkpoint is True


def test_sign_only_exit_policy_response_marks_eval_source() -> None:
    response = build_sign_only_exit_policy_response(
        side='short',
        eval_horizon='x32',
        min_hold_steps=32,
        check_interval_steps=32,
        bars_held=536,
        pred_log2=_pred_log2_for_linear(0.0008),
        last_renew_segment_evaluated=15,
        eval_source='daemon_latest_predictions_at_checkpoint',
    )
    assert response['suggest_close'] is True
    assert response['exit_reason'] == 'sign_flip_at_checkpoint'
    assert response['eval_source'] == 'daemon_latest_predictions_at_checkpoint'
    assert response['at_renew_checkpoint'] is True
