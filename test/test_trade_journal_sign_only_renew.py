from main.web_gui.trade_journal_service import compute_sign_only_renew_metrics


def test_sign_only_renew_before_min_hold() -> None:
    metrics = compute_sign_only_renew_metrics(
        bars_elapsed=4,
        min_hold_steps=32,
        check_interval_steps=32,
        mark_price=101.0,
        side='long',
        entry_price=100.0,
        notional_usd=100.0,
        excursion=None,
    )
    assert metrics['segments_completed'] == 0
    assert metrics['segment_bars_elapsed'] == 4
    assert metrics['bars_until_checkpoint'] == 28
    assert metrics['at_renew_checkpoint'] is False


def test_sign_only_renew_at_checkpoint() -> None:
    metrics = compute_sign_only_renew_metrics(
        bars_elapsed=32,
        min_hold_steps=32,
        check_interval_steps=32,
        mark_price=101.0,
        side='long',
        entry_price=100.0,
        notional_usd=100.0,
        excursion=None,
    )
    assert metrics['segments_completed'] == 1
    assert metrics['segment_bars_elapsed'] == 32
    assert metrics['bars_until_checkpoint'] == 0
    assert metrics['at_renew_checkpoint'] is True


def test_sign_only_renew_mid_segment() -> None:
    metrics = compute_sign_only_renew_metrics(
        bars_elapsed=40,
        min_hold_steps=32,
        check_interval_steps=32,
        mark_price=101.0,
        side='long',
        entry_price=100.0,
        notional_usd=100.0,
        excursion=None,
    )
    assert metrics['segments_completed'] == 1
    assert metrics['segment_bars_elapsed'] == 8
    assert metrics['bars_until_checkpoint'] == 24
    assert metrics['at_renew_checkpoint'] is False
