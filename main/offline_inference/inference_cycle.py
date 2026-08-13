import logging
import time
import traceback

from enumerations import SymbolId
from main.offline_inference.artifacts import (
    write_latest_inference_error,
    write_latest_inference_ok,
)
from main.web_gui.data_service import count_x1_bars_since_entry_sync, fetch_last_bars_sync
from main.web_gui.exit_policy_service import (
    build_exit_policy_disabled_response,
    run_remote_exit_policy,
)
from main.web_gui.exit_transformer_service import (
    build_exit_transformer_disabled_response,
    run_remote_exit_transformer_with_x_seq,
)
from main.web_gui.inference_service import (
    fetch_inference_metadata,
    run_remote_inference_and_x_seq_from_df,
)
from main.web_gui.trade_journal_service import (
    apply_mark_price_to_open_position,
    compute_position_metrics,
    compute_sign_only_renew_metrics,
    get_journal_state,
    parse_eval_horizon_steps,
)
from settings import settings

logger = logging.getLogger(__name__)


def _linear_metric_from_pct(value: float | int) -> float:
    numeric = float(value)
    if not numeric == numeric:
        return 0.0
    return numeric / 100.0


def _log_inference_db_fetch(symbol_id: str, df) -> None:
    now_ms = int(time.time() * 1000.0)
    last_row = df.row(df.height - 1, named=True)
    latest_timestamp_ms = int(last_row['start_timestamp_ms'])
    logger.info(
        'Inference daemon DB fetch: symbol=%s rows=%d latest_start_trade_id=%d '
        'latest_timestamp_ms=%d db_age_ms=%d',
        symbol_id,
        df.height,
        int(last_row['start_trade_id']),
        latest_timestamp_ms,
        now_ms - latest_timestamp_ms,
    )


def _log_inference_entry_provenance(
    symbol_id: str,
    provenance: dict[str, int | float],
) -> None:
    now_ms = int(time.time() * 1000.0)
    entry_timestamp_ms = int(provenance['bar_timestamp_ms'])
    db_latest_timestamp_ms = int(provenance['db_latest_timestamp_ms'])
    logger.info(
        'Inference daemon model entry: symbol=%s sample_index=%d raw_row=%d/%d '
        'level0_rows=%d model_lag_bars=%d entry_start_trade_id=%d entry_age_ms=%d '
        'db_latest_start_trade_id=%d db_latest_age_ms=%d',
        symbol_id,
        int(provenance['sample_index']),
        int(provenance['raw_entry_row']),
        int(provenance['real_bar_count']),
        int(provenance['level0_rows']),
        int(provenance['model_lag_bars']),
        int(provenance['bar_start_trade_id']),
        now_ms - entry_timestamp_ms,
        int(provenance['db_latest_start_trade_id']),
        now_ms - db_latest_timestamp_ms,
    )


def _build_exit_payloads(
    symbol_id: str,
    open_position_data: dict[str, object],
    bars_elapsed: int,
    mark_price: float,
    inference_result: dict[str, object],
    x_seq: dict[str, object],
) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    if 'predictions' not in inference_result:
        raise RuntimeError('Inference result missing predictions')
    if 'policy' not in inference_result:
        raise RuntimeError('Inference result missing policy')

    current_predictions = inference_result['predictions']
    current_policy = inference_result['policy']
    if not isinstance(current_predictions, dict):
        raise RuntimeError('Inference predictions must be a dict')
    if not isinstance(current_policy, dict):
        raise RuntimeError('Inference policy must be a dict')

    exit_stack_mode: str | None = None
    if 'exit_stack_mode' in open_position_data:
        exit_stack_mode = str(open_position_data['exit_stack_mode'])
    sign_only_exit = exit_stack_mode == 'rolling_h_renew_sign_only'

    entry_predictions: dict[str, object] | None = None
    entry_policy: dict[str, object] | None = None
    if not sign_only_exit:
        if 'entry_predictions' not in open_position_data:
            return None, None
        if 'entry_policy' not in open_position_data:
            return None, None
        entry_predictions_raw = open_position_data['entry_predictions']
        entry_policy_raw = open_position_data['entry_policy']
        if not isinstance(entry_predictions_raw, dict):
            return None, None
        if not isinstance(entry_policy_raw, dict):
            return None, None
        entry_predictions = entry_predictions_raw
        entry_policy = entry_policy_raw

    if sign_only_exit:
        min_hold_steps = int(open_position_data['exit_stack_min_hold_steps'])
        if 'exit_stack_eval_horizon' in open_position_data:
            check_interval_steps = parse_eval_horizon_steps(
                str(open_position_data['exit_stack_eval_horizon']),
            )
        else:
            check_interval_steps = int(open_position_data['eval_horizon_steps'])
        metrics = compute_sign_only_renew_metrics(
            bars_elapsed=bars_elapsed,
            min_hold_steps=min_hold_steps,
            check_interval_steps=check_interval_steps,
            mark_price=mark_price,
            side=str(open_position_data['side']),
            entry_price=float(open_position_data['entry_price']),
            notional_usd=float(open_position_data['notional_usd']),
            excursion=open_position_data['excursion']
            if 'excursion' in open_position_data
            else None,
        )
    else:
        metrics = compute_position_metrics(
            side=str(open_position_data['side']),
            entry_price=float(open_position_data['entry_price']),
            notional_usd=float(open_position_data['notional_usd']),
            eval_horizon_steps=int(open_position_data['eval_horizon_steps']),
            bars_elapsed=bars_elapsed,
            mark_price=mark_price,
            excursion=open_position_data['excursion']
            if 'excursion' in open_position_data
            else None,
        )

    common_payload: dict[str, object] | None = None
    if entry_predictions is not None and entry_policy is not None:
        common_payload = {
            'symbol_id': symbol_id,
            'side': open_position_data['side'],
            'eval_horizon': open_position_data['eval_horizon'],
            'bars_held': metrics['bars_elapsed'],
            'entry_predictions': entry_predictions,
            'current_predictions': current_predictions,
            'unrealized_linear': _linear_metric_from_pct(
                metrics['unrealized_net_return_pct'],
            ),
            'mfe_linear': _linear_metric_from_pct(metrics['mfe_net_return_pct']),
            'mae_linear': _linear_metric_from_pct(metrics['mae_net_return_pct']),
            'giveback_linear': _linear_metric_from_pct(
                metrics['giveback_net_return_pct'],
            ),
        }

    exit_policy_result: dict[str, object] | None = None
    if sign_only_exit:
        if 'exit_stack_eval_horizon' in open_position_data:
            deploy_eval_horizon = str(open_position_data['exit_stack_eval_horizon'])
        else:
            deploy_eval_horizon = str(open_position_data['eval_horizon'])
        exit_policy_payload = {
            'symbol_id': symbol_id,
            'side': open_position_data['side'],
            'eval_horizon': deploy_eval_horizon,
            'bars_held': metrics['bars_elapsed'],
            'current_predictions': current_predictions,
            'exit_stack_mode': exit_stack_mode,
        }
        exit_policy_result = run_remote_exit_policy(exit_policy_payload)
    elif settings.WEB_GUI_EXIT_GBM_ENABLED:
        if common_payload is None:
            return None, None
        if 'probabilities' not in current_policy:
            raise RuntimeError('Inference policy missing probabilities for exit GBM')
        if 'probabilities' not in entry_policy:
            raise RuntimeError('Entry policy missing probabilities for exit GBM')
        exit_policy_payload = dict(common_payload)
        exit_policy_payload['entry_policy'] = entry_policy
        exit_policy_payload['current_policy'] = {
            'action': current_policy['action'],
            'action_id': current_policy['action_id']
            if 'action_id' in current_policy
            else None,
            'probabilities': current_policy['probabilities'],
        }
        exit_policy_result = run_remote_exit_policy(exit_policy_payload)
    else:
        exit_policy_result = build_exit_policy_disabled_response()

    exit_transformer_result: dict[str, object] | None = None
    if settings.WEB_GUI_EXIT_TRANSFORMER_ENABLED:
        if common_payload is None:
            exit_transformer_result = build_exit_transformer_disabled_response()
        else:
            exit_transformer_payload = dict(common_payload)
            exit_transformer_result = run_remote_exit_transformer_with_x_seq(
                payload=exit_transformer_payload,
                x_seq=x_seq,
            )
    else:
        exit_transformer_result = build_exit_transformer_disabled_response()

    return exit_policy_result, exit_transformer_result


def run_inference_cycle(symbol_id: str) -> None:
    bars_limit = settings.INFERENCE_DAEMON_BARS_LIMIT
    symbol = SymbolId[symbol_id]
    df = fetch_last_bars_sync(symbol_id=symbol, limit=bars_limit, offset=0)
    if df is None:
        raise RuntimeError('Недостаточно данных для инференса')

    _log_inference_db_fetch(symbol_id=symbol_id, df=df)

    metadata = fetch_inference_metadata()
    required_rows = int(metadata['sequence_length']) * int(metadata['max_scale'])
    if df.height < required_rows:
        raise RuntimeError(
            'Инференс невозможен при таком количестве свечей x1 '
            f'(требуется минимум {required_rows}, получено {df.height})',
        )

    inference_result, x_seq, entry_provenance = run_remote_inference_and_x_seq_from_df(
        symbol_id=symbol_id,
        df=df,
    )
    _log_inference_entry_provenance(
        symbol_id=symbol_id,
        provenance=entry_provenance,
    )

    exit_policy_result: dict[str, object] | None = None
    exit_transformer_result: dict[str, object] | None = None

    journal = get_journal_state()
    open_position_data = journal['open_position']
    if (
        open_position_data is not None
        and open_position_data['symbol_id'] == symbol_id
    ):
        mark_price = float(df.row(df.height - 1, named=True)['close_price'])
        apply_mark_price_to_open_position(mark_price=mark_price)
        journal = get_journal_state()
        open_position_data = journal['open_position']
        if open_position_data is None:
            raise RuntimeError('Open journal position disappeared after mark price update')
        entry_start_trade_id = int(open_position_data['entry_start_trade_id'])
        bars_elapsed = count_x1_bars_since_entry_sync(
            symbol_id=symbol,
            entry_start_trade_id=entry_start_trade_id,
        )
        if bars_elapsed is None:
            logger.warning(
                'Could not count bars since entry for %s; skipping exit overlays',
                symbol_id,
            )
        else:
            exit_policy_result, exit_transformer_result = _build_exit_payloads(
                symbol_id=symbol_id,
                open_position_data=open_position_data,
                bars_elapsed=bars_elapsed,
                mark_price=mark_price,
                inference_result=inference_result,
                x_seq=x_seq,
            )

    payload: dict[str, object] = {
        'bar_start_trade_id': entry_provenance['bar_start_trade_id'],
        'bar_timestamp_ms': entry_provenance['bar_timestamp_ms'],
        'bar_close_price': entry_provenance['bar_close_price'],
        'predictions': inference_result['predictions'],
        'policy': inference_result['policy'] if 'policy' in inference_result else None,
        'entry_hint': inference_result['entry_hint']
        if 'entry_hint' in inference_result
        else None,
        'exit_policy': exit_policy_result,
        'exit_transformer': exit_transformer_result,
    }
    write_latest_inference_ok(symbol_id=symbol_id, payload=payload)


def run_inference_cycle_safe(symbol_id: str) -> None:
    try:
        run_inference_cycle(symbol_id=symbol_id)
    except Exception as exception:
        logger.error(
            'Inference cycle failed: %s',
            ''.join(traceback.format_exception(exception)),
        )
        write_latest_inference_error(
            symbol_id=symbol_id,
            error_message=str(exception),
        )
