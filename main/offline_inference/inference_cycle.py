import logging
import traceback

from enumerations import SymbolId
from main.offline_inference.artifacts import (
    write_latest_inference_error,
    write_latest_inference_ok,
)
from main.web_gui.data_service import count_x1_bars_since_entry, fetch_last_bars
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
    prepare_x_seq_2d_from_df,
    run_remote_inference_from_df,
)
from main.web_gui.trade_journal_service import (
    apply_mark_price_to_open_position,
    compute_position_metrics,
    get_journal_state,
)
from settings import settings

logger = logging.getLogger(__name__)


def _linear_metric_from_pct(value: float | int) -> float:
    numeric = float(value)
    if not numeric == numeric:
        return 0.0
    return numeric / 100.0


def _latest_bar_metadata(df) -> dict[str, int]:
    last_row = df.row(df.height - 1, named=True)
    return {
        'bar_start_trade_id': int(last_row['start_trade_id']),
        'bar_timestamp_ms': int(last_row['start_timestamp_ms']),
    }


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

    if 'entry_predictions' not in open_position_data:
        return None, None
    if 'entry_policy' not in open_position_data:
        return None, None

    entry_predictions = open_position_data['entry_predictions']
    entry_policy = open_position_data['entry_policy']
    if not isinstance(entry_predictions, dict):
        return None, None
    if not isinstance(entry_policy, dict):
        return None, None

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

    common_payload: dict[str, object] = {
        'symbol_id': symbol_id,
        'side': open_position_data['side'],
        'eval_horizon': open_position_data['eval_horizon'],
        'bars_held': metrics['bars_elapsed'],
        'entry_predictions': entry_predictions,
        'current_predictions': current_predictions,
        'unrealized_linear': _linear_metric_from_pct(metrics['unrealized_net_return_pct']),
        'mfe_linear': _linear_metric_from_pct(metrics['mfe_net_return_pct']),
        'mae_linear': _linear_metric_from_pct(metrics['mae_net_return_pct']),
        'giveback_linear': _linear_metric_from_pct(metrics['giveback_net_return_pct']),
    }

    exit_policy_result: dict[str, object] | None = None
    if settings.WEB_GUI_EXIT_GBM_ENABLED:
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
    df = fetch_last_bars(symbol_id=symbol, limit=bars_limit, offset=0)
    if df is None:
        raise RuntimeError('Недостаточно данных для инференса')

    metadata = fetch_inference_metadata()
    required_rows = int(metadata['sequence_length']) * int(metadata['max_scale'])
    if df.height < required_rows:
        raise RuntimeError(
            'Инференс невозможен при таком количестве свечей x1 '
            f'(требуется минимум {required_rows}, получено {df.height})',
        )

    inference_result = run_remote_inference_from_df(symbol_id=symbol_id, df=df)
    x_seq = prepare_x_seq_2d_from_df(df)
    bar_metadata = _latest_bar_metadata(df)

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
        bars_elapsed = count_x1_bars_since_entry(
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
        'bar_start_trade_id': bar_metadata['bar_start_trade_id'],
        'bar_timestamp_ms': bar_metadata['bar_timestamp_ms'],
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
