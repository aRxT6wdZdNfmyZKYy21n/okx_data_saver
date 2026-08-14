from main.web_gui.trade_journal_service import (
    build_equity_curve,
    compute_cash_balance_usd,
    resolve_entry_side_from_hint,
)


def test_resolve_entry_side_sign_fee_band_long() -> None:
    side = resolve_entry_side_from_hint(
        {
            'recommended_action': 'long',
            'allow_long': True,
            'allow_short': False,
            'entry_blocked': False,
        },
    )
    assert side == 'long'


def test_resolve_entry_side_blocked() -> None:
    side = resolve_entry_side_from_hint(
        {
            'recommended_action': 'long',
            'entry_blocked': True,
        },
    )
    assert side is None


def test_cash_balance_after_closed_trades() -> None:
    closed_trades = [
        {'realized_pnl_usd': 10.0},
        {'realized_pnl_usd': -5.0},
    ]
    assert compute_cash_balance_usd(closed_trades) == 105.0
    curve = build_equity_curve(closed_trades)
    assert curve[0]['balance_usd'] == 100.0
    assert curve[-1]['balance_usd'] == 105.0
