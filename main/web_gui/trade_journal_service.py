"""
Ручной trade journal для micro live: открытая позиция + история закрытых сделок (JSON).
"""

import json
import logging
import os
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_JOURNAL_LOCK = threading.Lock()

DEFAULT_NOTIONAL_USD = 7.0
DEFAULT_EVAL_HORIZON = 'x2048'
ROUND_TRIP_FEE_RATE = 0.001
TAKER_FEE_RATE_PER_SIDE = 0.0005


def journal_path() -> str:
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    return os.path.join(repo_root, 'data', 'trade_journal.json')


def _empty_journal() -> dict[str, Any]:
    return {
        'open_position': None,
        'closed_trades': [],
    }


def _load_journal_unlocked() -> dict[str, Any]:
    path = journal_path()
    if not os.path.isfile(path):
        return _empty_journal()
    with open(path, encoding='utf-8') as journal_file:
        data = json.load(journal_file)
    if 'open_position' not in data:
        data['open_position'] = None
    if 'closed_trades' not in data:
        data['closed_trades'] = []
    return data


def _save_journal_unlocked(data: dict[str, Any]) -> None:
    path = journal_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as journal_file:
        json.dump(data, journal_file, ensure_ascii=False, indent=2)


def parse_eval_horizon_steps(eval_horizon: str) -> int:
    if not eval_horizon.startswith('x'):
        raise ValueError(f'eval_horizon must start with x, got: {eval_horizon}')
    steps = int(eval_horizon[1:])
    if steps <= 0:
        raise ValueError(f'eval_horizon must be positive, got: {eval_horizon}')
    return steps


def gross_return_pct(side: str, entry_price: float, mark_price: float) -> float:
    if side == 'long':
        return (mark_price - entry_price) / entry_price
    if side == 'short':
        return (entry_price - mark_price) / entry_price
    raise ValueError(f'Unknown side: {side}')


def net_return_pct(gross_return_pct_value: float, include_exit_fee: bool) -> float:
    if include_exit_fee:
        return gross_return_pct_value - ROUND_TRIP_FEE_RATE
    return gross_return_pct_value - TAKER_FEE_RATE_PER_SIDE


def compute_position_metrics(
    side: str,
    entry_price: float,
    notional_usd: float,
    eval_horizon_steps: int,
    bars_elapsed: int,
    mark_price: float,
) -> dict[str, float | int | bool]:
    gross_pct = gross_return_pct(side, entry_price, mark_price)
    unrealized_net_pct = net_return_pct(gross_pct, include_exit_fee=False)
    return {
        'bars_elapsed': bars_elapsed,
        'eval_horizon_steps': eval_horizon_steps,
        'bars_remaining': max(0, eval_horizon_steps - bars_elapsed),
        'progress_pct': min(100.0, 100.0 * bars_elapsed / eval_horizon_steps),
        'at_target_horizon': bars_elapsed >= eval_horizon_steps,
        'mark_price': mark_price,
        'gross_return_pct': gross_pct * 100.0,
        'unrealized_net_return_pct': unrealized_net_pct * 100.0,
        'unrealized_pnl_usd': notional_usd * unrealized_net_pct,
    }


def get_journal_state() -> dict[str, Any]:
    with _JOURNAL_LOCK:
        return _load_journal_unlocked()


def open_position(
    symbol_id: str,
    side: str,
    entry_price: float,
    entry_start_trade_id: int,
    entry_timestamp_ms: int,
    eval_horizon: str,
    notional_usd: float,
    policy_action: str | None,
    notes: str,
) -> dict[str, Any]:
    side_normalized = side.lower()
    if side_normalized not in ('long', 'short'):
        raise ValueError(f'side must be long or short, got: {side}')
    if entry_price <= 0:
        raise ValueError(f'entry_price must be positive, got: {entry_price}')
    if notional_usd <= 0:
        raise ValueError(f'notional_usd must be positive, got: {notional_usd}')

    eval_horizon_steps = parse_eval_horizon_steps(eval_horizon)
    now_iso = datetime.now(timezone.utc).isoformat()

    with _JOURNAL_LOCK:
        journal = _load_journal_unlocked()
        if journal['open_position'] is not None:
            raise ValueError('Open position already exists; close it before opening a new one')

        position = {
            'id': str(uuid.uuid4()),
            'symbol_id': symbol_id,
            'side': side_normalized,
            'entry_price': entry_price,
            'entry_start_trade_id': entry_start_trade_id,
            'entry_timestamp_ms': entry_timestamp_ms,
            'eval_horizon': eval_horizon,
            'eval_horizon_steps': eval_horizon_steps,
            'notional_usd': notional_usd,
            'policy_action': policy_action,
            'notes': notes,
            'opened_at_utc': now_iso,
        }
        journal['open_position'] = position
        _save_journal_unlocked(journal)
        return position


def close_position(
    exit_price: float,
    exit_start_trade_id: int,
    exit_timestamp_ms: int,
    notes: str,
) -> dict[str, Any]:
    if exit_price <= 0:
        raise ValueError(f'exit_price must be positive, got: {exit_price}')

    now_iso = datetime.now(timezone.utc).isoformat()

    with _JOURNAL_LOCK:
        journal = _load_journal_unlocked()
        open_position_data = journal['open_position']
        if open_position_data is None:
            raise ValueError('No open position to close')

        side = open_position_data['side']
        entry_price = float(open_position_data['entry_price'])
        notional_usd = float(open_position_data['notional_usd'])
        gross_pct = gross_return_pct(side, entry_price, exit_price)
        net_pct = net_return_pct(gross_pct, include_exit_fee=True)

        closed_trade = {
            'id': open_position_data['id'],
            'symbol_id': open_position_data['symbol_id'],
            'side': side,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'entry_start_trade_id': open_position_data['entry_start_trade_id'],
            'exit_start_trade_id': exit_start_trade_id,
            'entry_timestamp_ms': open_position_data['entry_timestamp_ms'],
            'exit_timestamp_ms': exit_timestamp_ms,
            'eval_horizon': open_position_data['eval_horizon'],
            'eval_horizon_steps': open_position_data['eval_horizon_steps'],
            'notional_usd': notional_usd,
            'policy_action': open_position_data['policy_action'],
            'entry_notes': open_position_data['notes'],
            'exit_notes': notes,
            'opened_at_utc': open_position_data['opened_at_utc'],
            'closed_at_utc': now_iso,
            'gross_return_pct': gross_pct * 100.0,
            'net_return_pct': net_pct * 100.0,
            'realized_pnl_usd': notional_usd * net_pct,
            'fee_model_round_trip_rate': ROUND_TRIP_FEE_RATE,
        }
        journal['closed_trades'].append(closed_trade)
        journal['open_position'] = None
        _save_journal_unlocked(journal)
        return closed_trade


def discard_open_position() -> None:
    with _JOURNAL_LOCK:
        journal = _load_journal_unlocked()
        journal['open_position'] = None
        _save_journal_unlocked(journal)


def enrich_open_position(
    open_position_data: dict[str, Any],
    bars_elapsed: int,
    mark_price: float,
) -> dict[str, Any]:
    metrics = compute_position_metrics(
        side=open_position_data['side'],
        entry_price=float(open_position_data['entry_price']),
        notional_usd=float(open_position_data['notional_usd']),
        eval_horizon_steps=int(open_position_data['eval_horizon_steps']),
        bars_elapsed=bars_elapsed,
        mark_price=mark_price,
    )
    enriched = dict(open_position_data)
    enriched['metrics'] = metrics
    return enriched


def build_journal_response(
    journal: dict[str, Any],
    bars_elapsed: int | None,
    mark_price: float | None,
) -> dict[str, Any]:
    open_position_data = journal['open_position']
    enriched_open = None
    if open_position_data is not None and bars_elapsed is not None and mark_price is not None:
        enriched_open = enrich_open_position(open_position_data, bars_elapsed, mark_price)

    closed = journal['closed_trades']
    recent_closed = closed[-20:][::-1]
    total_realized = sum(float(t['realized_pnl_usd']) for t in closed)

    return {
        'open_position': enriched_open if enriched_open is not None else open_position_data,
        'closed_trades': recent_closed,
        'closed_trades_count': len(closed),
        'total_realized_pnl_usd': total_realized,
        'defaults': {
            'notional_usd': DEFAULT_NOTIONAL_USD,
            'eval_horizon': DEFAULT_EVAL_HORIZON,
            'round_trip_fee_rate': ROUND_TRIP_FEE_RATE,
        },
    }
