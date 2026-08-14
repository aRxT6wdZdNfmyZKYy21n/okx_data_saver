"""
Automated paper-trading loop for web_gui: full-balance entries/exits via trade journal.
"""

import logging
import time
import traceback
from typing import Any

from enumerations import SymbolId
from main.offline_inference.artifacts import enrich_inference_artifact, read_latest_inference
from main.web_gui.data_service import count_x1_bars_since_entry_sync, fetch_last_bars_sync
from main.web_gui.inference_service import fetch_inference_metadata
from main.web_gui.sign_only_renew_exit_common import (
    build_sign_only_exit_policy_response,
    extract_pred_eval_log2_from_predictions,
)
from main.web_gui.trade_execution_log import append_execution_event
from main.web_gui.trade_journal_service import (
    apply_checkpoint_pending_since_ms,
    apply_daemon_last_exit_policy,
    apply_last_exit_eval_inference_completed_at_ms,
    apply_last_renew_segment_evaluated,
    apply_mark_price_to_open_position,
    close_position_automated,
    compute_cash_balance_usd,
    default_eval_horizon,
    ensure_automated_trading_initialized,
    enrich_open_position,
    get_journal_state,
    open_position_automated,
    parse_eval_horizon_steps,
    resolve_entry_side_from_hint,
    resolve_last_renew_segment_evaluated,
)
from settings import settings

logger = logging.getLogger(__name__)


def _resolve_inference_completed_at_ms(artifact: dict[str, Any]) -> int | None:
    if 'inference_completed_at_ms' in artifact:
        return int(artifact['inference_completed_at_ms'])
    if 'updated_at_ms' in artifact:
        return int(artifact['updated_at_ms'])
    return None


def _resolve_exit_stack_for_symbol(symbol_id: str) -> dict[str, Any] | None:
    metadata = fetch_inference_metadata()
    if 'exit_stack_by_symbol' not in metadata:
        return None
    exit_stack_by_symbol = metadata['exit_stack_by_symbol']
    if symbol_id not in exit_stack_by_symbol:
        return None
    exit_stack = exit_stack_by_symbol[symbol_id]
    if not isinstance(exit_stack, dict):
        raise RuntimeError(f'exit_stack for {symbol_id} must be a dict')
    return exit_stack


def _fetch_latest_x1_bar(symbol: SymbolId) -> dict[str, Any]:
    df = fetch_last_bars_sync(symbol_id=symbol, limit=1, offset=0)
    if df is None or df.height < 1:
        raise RuntimeError(f'No x1 bars available for {symbol.name}')
    row = df.row(df.height - 1, named=True)
    return {
        'close_price': float(row['close_price']),
        'start_trade_id': int(row['start_trade_id']),
        'start_timestamp_ms': int(row['start_timestamp_ms']),
    }


def _load_inference_artifact(symbol_id: str) -> dict[str, Any] | None:
    artifact = read_latest_inference(symbol_id=symbol_id)
    if artifact is None:
        return None
    return enrich_inference_artifact(artifact)


def _build_entry_policy_snapshot(
    policy: dict[str, Any] | None,
    entry_hint: dict[str, Any] | None,
    eval_horizon: str,
) -> dict[str, Any] | None:
    if policy is None:
        return None
    snapshot: dict[str, Any] = {
        'action': policy['action'] if 'action' in policy else None,
        'eval_horizon': policy['eval_horizon'] if 'eval_horizon' in policy else eval_horizon,
        'run_label': policy['run_label'] if 'run_label' in policy else None,
        'probabilities': policy['probabilities'] if 'probabilities' in policy else None,
        'entry_hint': entry_hint,
    }
    return snapshot


def _try_automated_entry(
    symbol_id: str,
    symbol: SymbolId,
    artifact: dict[str, Any],
    latest_bar: dict[str, Any],
    cash_balance_usd: float,
) -> None:
    if 'entry_hint' not in artifact or artifact['entry_hint'] is None:
        append_execution_event(
            'skip_entry',
            {
                'symbol_id': symbol_id,
                'reason': 'missing_entry_hint',
            },
        )
        return
    entry_hint = artifact['entry_hint']
    if not isinstance(entry_hint, dict):
        raise RuntimeError('artifact entry_hint must be a dict')
    side = resolve_entry_side_from_hint(entry_hint)
    if side is None:
        append_execution_event(
            'skip_entry',
            {
                'symbol_id': symbol_id,
                'reason': 'no_entry_signal',
                'recommended_action': entry_hint['recommended_action']
                if 'recommended_action' in entry_hint
                else None,
            },
        )
        return
    if cash_balance_usd <= 0.0:
        append_execution_event(
            'skip_entry',
            {
                'symbol_id': symbol_id,
                'reason': 'non_positive_balance',
                'cash_balance_usd': cash_balance_usd,
            },
        )
        return

    policy = artifact['policy'] if 'policy' in artifact else None
    if policy is not None and isinstance(policy, dict) and 'action' in policy:
        policy_action = str(policy['action']).lower()
        if policy_action == 'hold':
            append_execution_event(
                'skip_entry',
                {
                    'symbol_id': symbol_id,
                    'reason': 'policy_hold',
                    'recommended_action': side,
                },
            )
            return

    eval_horizon = default_eval_horizon()
    if policy is not None and isinstance(policy, dict) and 'eval_horizon' in policy:
        eval_horizon = str(policy['eval_horizon'])

    predictions = None
    if 'predictions' in artifact and isinstance(artifact['predictions'], dict):
        predictions = artifact['predictions']

    exit_stack = _resolve_exit_stack_for_symbol(symbol_id)
    exit_stack_mode = None
    exit_stack_eval_horizon = None
    exit_stack_min_hold_steps = None
    if exit_stack is not None:
        exit_stack_mode = str(exit_stack['mode'])
        exit_stack_eval_horizon = str(exit_stack['eval_horizon'])
        exit_stack_min_hold_steps = int(exit_stack['min_hold_steps'])

    policy_action_label = None
    if policy is not None and isinstance(policy, dict) and 'action' in policy:
        policy_action_label = str(policy['action'])

    balance_before = cash_balance_usd
    position = open_position_automated(
        symbol_id=symbol_id,
        side=side,
        entry_price=latest_bar['close_price'],
        entry_start_trade_id=latest_bar['start_trade_id'],
        entry_timestamp_ms=latest_bar['start_timestamp_ms'],
        eval_horizon=eval_horizon,
        notional_usd=cash_balance_usd,
        policy_action=policy_action_label,
        entry_policy=_build_entry_policy_snapshot(policy, entry_hint, eval_horizon),
        entry_predictions=predictions,
        exit_stack_mode=exit_stack_mode,
        exit_stack_eval_horizon=exit_stack_eval_horizon,
        exit_stack_min_hold_steps=exit_stack_min_hold_steps,
    )
    append_execution_event(
        'entry',
        {
            'symbol_id': symbol_id,
            'side': side,
            'entry_price': latest_bar['close_price'],
            'entry_start_trade_id': latest_bar['start_trade_id'],
            'notional_usd': cash_balance_usd,
            'balance_before_usd': balance_before,
            'balance_after_usd': balance_before,
            'entry_hint': entry_hint,
            'policy': policy,
            'inference_completed_at_ms': _resolve_inference_completed_at_ms(artifact),
            'position_id': position['id'],
        },
    )


def _resolve_sign_only_exit_params(
    open_position_data: dict[str, Any],
) -> tuple[str, int, int]:
    if 'exit_stack_eval_horizon' in open_position_data:
        deploy_eval_horizon = str(open_position_data['exit_stack_eval_horizon'])
    else:
        deploy_eval_horizon = str(open_position_data['eval_horizon'])
    min_hold_steps = int(open_position_data['exit_stack_min_hold_steps'])
    check_interval_steps = parse_eval_horizon_steps(deploy_eval_horizon)
    return deploy_eval_horizon, min_hold_steps, check_interval_steps


def _evaluate_sign_only_exit_policy_local(
    open_position_data: dict[str, Any],
    artifact: dict[str, Any],
    bars_elapsed: int,
    eval_source: str,
) -> dict[str, Any]:
    if 'predictions' not in artifact or not isinstance(artifact['predictions'], dict):
        raise ValueError('artifact predictions missing for sign_only exit eval')
    deploy_eval_horizon, min_hold_steps, check_interval_steps = (
        _resolve_sign_only_exit_params(open_position_data)
    )
    pred_log2 = extract_pred_eval_log2_from_predictions(
        predictions=artifact['predictions'],
        eval_horizon=deploy_eval_horizon,
    )
    return build_sign_only_exit_policy_response(
        side=str(open_position_data['side']),
        eval_horizon=deploy_eval_horizon,
        min_hold_steps=min_hold_steps,
        check_interval_steps=check_interval_steps,
        bars_held=bars_elapsed,
        pred_log2=pred_log2,
        last_renew_segment_evaluated=resolve_last_renew_segment_evaluated(
            open_position_data,
        ),
        eval_source=eval_source,
    )


def _apply_sign_only_exit_policy_state(
    exit_policy: dict[str, Any],
    inference_completed_at_ms: int | None,
    pending_segment_eval: bool,
) -> None:
    if (
        'last_renew_segment_evaluated' in exit_policy
        and exit_policy['last_renew_segment_evaluated'] is not None
    ):
        apply_last_renew_segment_evaluated(
            int(exit_policy['last_renew_segment_evaluated']),
        )
    if inference_completed_at_ms is not None and pending_segment_eval:
        apply_last_exit_eval_inference_completed_at_ms(inference_completed_at_ms)


def _manage_open_position(
    symbol_id: str,
    symbol: SymbolId,
    open_position_data: dict[str, Any],
    artifact: dict[str, Any],
    latest_bar: dict[str, Any],
) -> None:
    mark_price = latest_bar['close_price']
    apply_mark_price_to_open_position(mark_price=mark_price)
    journal = get_journal_state()
    open_position_data = journal['open_position']
    if open_position_data is None:
        raise RuntimeError('Open position disappeared during mark update')

    bars_elapsed = count_x1_bars_since_entry_sync(
        symbol_id=symbol,
        entry_start_trade_id=int(open_position_data['entry_start_trade_id']),
    )
    if bars_elapsed is None:
        append_execution_event(
            'tick',
            {
                'symbol_id': symbol_id,
                'reason': 'bars_elapsed_unavailable',
            },
        )
        return

    enriched = enrich_open_position(
        open_position_data=open_position_data,
        bars_elapsed=bars_elapsed,
        mark_price=mark_price,
        client_last_renew_segment_evaluated=None,
    )
    metrics = enriched['metrics']
    pending_segment_eval = bool(metrics['pending_segment_eval'])
    if pending_segment_eval:
        if 'checkpoint_pending_since_ms' not in open_position_data:
            checkpoint_pending_since_ms = int(time.time() * 1000.0)
            apply_checkpoint_pending_since_ms(checkpoint_pending_since_ms)
            append_execution_event(
                'checkpoint_crossed',
                {
                    'symbol_id': symbol_id,
                    'bars_elapsed': bars_elapsed,
                    'current_renew_segment': metrics['current_renew_segment'],
                    'checkpoint_pending_since_ms': checkpoint_pending_since_ms,
                },
            )
            journal = get_journal_state()
            open_position_data = journal['open_position']
            if open_position_data is None:
                return
    else:
        apply_checkpoint_pending_since_ms(None)

    inference_completed_at_ms = _resolve_inference_completed_at_ms(artifact)

    exit_stack_mode = None
    if 'exit_stack_mode' in open_position_data:
        exit_stack_mode = str(open_position_data['exit_stack_mode'])
    if exit_stack_mode != 'rolling_h_renew_sign_only':
        append_execution_event(
            'skip_exit_eval',
            {
                'symbol_id': symbol_id,
                'reason': 'unsupported_exit_stack_mode',
                'exit_stack_mode': exit_stack_mode,
            },
        )
        return

    if pending_segment_eval:
        if 'predictions' not in artifact or not isinstance(artifact['predictions'], dict):
            append_execution_event(
                'skip_exit_eval',
                {
                    'symbol_id': symbol_id,
                    'reason': 'missing_predictions_at_checkpoint',
                    'bars_elapsed': bars_elapsed,
                    'pending_segment_eval': pending_segment_eval,
                },
            )
            return
        exit_policy = _evaluate_sign_only_exit_policy_local(
            open_position_data=open_position_data,
            artifact=artifact,
            bars_elapsed=bars_elapsed,
            eval_source='daemon_latest_predictions_at_checkpoint',
        )
    else:
        if 'predictions' not in artifact or not isinstance(artifact['predictions'], dict):
            append_execution_event(
                'skip_exit_eval',
                {
                    'symbol_id': symbol_id,
                    'reason': 'missing_predictions',
                    'bars_elapsed': bars_elapsed,
                },
            )
            return
        exit_policy = _evaluate_sign_only_exit_policy_local(
            open_position_data=open_position_data,
            artifact=artifact,
            bars_elapsed=bars_elapsed,
            eval_source='daemon_latest_predictions',
        )

    apply_daemon_last_exit_policy(exit_policy)
    if exit_policy['at_renew_checkpoint']:
        _apply_sign_only_exit_policy_state(
            exit_policy=exit_policy,
            inference_completed_at_ms=inference_completed_at_ms,
            pending_segment_eval=pending_segment_eval,
        )
    append_execution_event(
        'exit_policy_eval',
        {
            'symbol_id': symbol_id,
            'bars_elapsed': bars_elapsed,
            'pending_segment_eval': pending_segment_eval,
            'inference_completed_at_ms': inference_completed_at_ms,
            'exit_policy': exit_policy,
            'metrics': metrics,
        },
    )

    if 'suggest_close' in exit_policy and bool(exit_policy['suggest_close']):
        if not pending_segment_eval and not exit_policy['at_renew_checkpoint']:
            append_execution_event(
                'skip_exit',
                {
                    'symbol_id': symbol_id,
                    'reason': 'suggest_close_without_pending_checkpoint',
                    'exit_policy': exit_policy,
                },
            )
            return
        balance_before = compute_cash_balance_usd(journal['closed_trades'])
        closed_trade = close_position_automated(
            exit_price=mark_price,
            exit_start_trade_id=latest_bar['start_trade_id'],
            exit_timestamp_ms=latest_bar['start_timestamp_ms'],
            exit_overlay={'exit_policy': exit_policy, 'metrics': metrics},
        )
        append_execution_event(
            'exit',
            {
                'symbol_id': symbol_id,
                'exit_price': mark_price,
                'exit_start_trade_id': latest_bar['start_trade_id'],
                'balance_before_usd': balance_before,
                'balance_after_usd': closed_trade['balance_after_close_usd']
                if 'balance_after_close_usd' in closed_trade
                else compute_cash_balance_usd(get_journal_state()['closed_trades']),
                'realized_pnl_usd': closed_trade['realized_pnl_usd'],
                'exit_policy': exit_policy,
            },
        )


def run_trading_tick(symbol_id: str) -> None:
    if not settings.WEB_GUI_TRADING_ENABLED:
        return
    ensure_automated_trading_initialized()

    symbol = SymbolId[symbol_id]
    latest_bar = _fetch_latest_x1_bar(symbol=symbol)
    artifact = _load_inference_artifact(symbol_id=symbol_id)
    journal = get_journal_state()
    cash_balance_usd = compute_cash_balance_usd(journal['closed_trades'])

    append_execution_event(
        'tick',
        {
            'symbol_id': symbol_id,
            'cash_balance_usd': cash_balance_usd,
            'has_open_position': journal['open_position'] is not None,
            'mark_price': latest_bar['close_price'],
            'artifact_status': artifact['status'] if artifact is not None and 'status' in artifact else None,
            'inference_completed_at_ms': _resolve_inference_completed_at_ms(artifact)
            if artifact is not None
            else None,
        },
    )

    if artifact is None or artifact['status'] != 'ok':
        return

    open_position_data = journal['open_position']
    if open_position_data is None:
        _try_automated_entry(
            symbol_id=symbol_id,
            symbol=symbol,
            artifact=artifact,
            latest_bar=latest_bar,
            cash_balance_usd=cash_balance_usd,
        )
        return

    if open_position_data['symbol_id'] != symbol_id:
        append_execution_event(
            'skip_tick',
            {
                'symbol_id': symbol_id,
                'reason': 'open_position_other_symbol',
                'open_symbol_id': open_position_data['symbol_id'],
            },
        )
        return

    _manage_open_position(
        symbol_id=symbol_id,
        symbol=symbol,
        open_position_data=open_position_data,
        artifact=artifact,
        latest_bar=latest_bar,
    )


def run_trading_tick_safe(symbol_id: str) -> None:
    try:
        run_trading_tick(symbol_id=symbol_id)
    except Exception as exception:
        logger.error(
            'Trading tick failed: %s',
            ''.join(traceback.format_exception(exception)),
        )
        append_execution_event(
            'error',
            {
                'symbol_id': symbol_id,
                'error': str(exception),
                'traceback': ''.join(traceback.format_exception(exception)),
            },
        )
