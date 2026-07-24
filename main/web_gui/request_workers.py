"""
Выполнение запросов с Polars в отдельном процессе (spawn), чтобы избежать утечки памяти.
heavy/light пулы в main.spawn_process (Polars только внутри дочернего процесса).
"""

import logging

from enumerations import SymbolId

from main.spawn_process import run_in_spawned_process
from main.web_gui.data_service import get_bars_for_api_sync, count_x1_bars_since_entry_sync
from main.web_gui.dow_service import get_dow_bars_for_api
from main.web_gui.exit_policy_service import run_remote_exit_policy
from main.web_gui.exit_transformer_service import run_remote_exit_transformer
from main.web_gui.inference_service import run_remote_inference
from main.web_gui.trade_research_artifact_service import run_trade_research_from_artifact
from main.web_gui.serialization import serialize_bar_row
from main.web_gui.trade_journal_service import (
    build_journal_response,
    build_trade_journal_api_response,
    close_position,
    discard_open_position,
    get_journal_state,
    open_position,
)
from main.web_gui.constants import CHART_SHOW_LIMIT
from settings import settings

logger = logging.getLogger(__name__)

BAR_COLS = [
    'start_trade_id', 'end_trade_id',
    'start_timestamp_ms', 'end_timestamp_ms',
    'open_price', 'high_price', 'low_price', 'close_price',
    'total_volume', 'buy_volume_percent', 'sell_volume_percent', 'total_volume_log2',
]


def _worker_bars(symbol_id_str: str, limit: int, offset: int, scale: str) -> list[dict] | None:
    """Вызывается в дочернем процессе. Возвращает список сериализованных баров или None."""
    symbol = SymbolId[symbol_id_str]
    effective_limit = min(limit, settings.WEB_GUI_RECORDS_LIMIT)
    df = get_bars_for_api_sync(symbol_id=symbol, limit=effective_limit, offset=offset, scale=scale)
    if df is None:
        return None
    available = [c for c in BAR_COLS if c in df.columns]
    rows = df.select(available).to_dicts()
    return [serialize_bar_row(r) for r in rows][-CHART_SHOW_LIMIT:]


def _worker_dow(symbol_id_str: str, limit: int, level: int) -> list[dict] | None:
    """Вызывается в дочернем процессе. Возвращает список сериализованных баров Доу или None."""
    symbol = SymbolId[symbol_id_str]
    effective_limit = min(limit, settings.WEB_GUI_RECORDS_LIMIT)
    bars = get_dow_bars_for_api(symbol_id=symbol, limit=effective_limit, level=level)
    if bars is None:
        return None
    return [serialize_bar_row(r) for r in bars][-CHART_SHOW_LIMIT:]


def _worker_inference(symbol_id_str: str, limit: int) -> dict[str, object]:
    """Вызывается в дочернем процессе. Возвращает словарь предсказаний."""
    return run_remote_inference(symbol_id=symbol_id_str, limit=limit)


def _worker_trade_research_from_artifact(
    symbol_id_str: str,
    eval_horizon: str,
    step_bars: int,
    visible_min_start_trade_id: int | None,
    visible_max_start_trade_id: int | None,
) -> dict[str, object]:
    return run_trade_research_from_artifact(
        symbol_id=symbol_id_str,
        eval_horizon=eval_horizon,
        step_bars=step_bars,
        visible_min_start_trade_id=visible_min_start_trade_id,
        visible_max_start_trade_id=visible_max_start_trade_id,
    )


def _worker_inference_cycle_safe(symbol_id_str: str) -> None:
    from main.offline_inference.inference_cycle import run_inference_cycle_safe

    run_inference_cycle_safe(symbol_id=symbol_id_str)


def _worker_trade_research_export_safe(symbol_id_str: str) -> None:
    from main.offline_inference.trade_research_export import run_trade_research_export_safe

    run_trade_research_export_safe(symbol_id=symbol_id_str)


def _worker_exit_policy(payload: dict) -> dict[str, object]:
    return run_remote_exit_policy(payload)


def _worker_exit_transformer(payload: dict) -> dict[str, object]:
    return run_remote_exit_transformer(payload)


def _resolve_bars_elapsed_from_db(
    symbol_id_str: str,
    mark_price: float | None,
) -> int | None:
    if mark_price is None:
        return None
    journal = get_journal_state()
    open_position_data = journal['open_position']
    if open_position_data is None:
        return None
    if open_position_data['symbol_id'] != symbol_id_str:
        return None
    symbol = SymbolId[symbol_id_str]
    entry_start_trade_id = int(open_position_data['entry_start_trade_id'])
    return count_x1_bars_since_entry_sync(
        symbol_id=symbol,
        entry_start_trade_id=entry_start_trade_id,
    )


def _build_trade_journal_api_response_with_db_bars_elapsed(
    symbol_id_str: str,
    mark_price: float | None,
) -> dict:
    bars_elapsed = _resolve_bars_elapsed_from_db(
        symbol_id_str=symbol_id_str,
        mark_price=mark_price,
    )
    return build_trade_journal_api_response(
        symbol_id_str=symbol_id_str,
        mark_price=mark_price,
        bars_elapsed=bars_elapsed,
        persist_mark_price=True,
    )


def _worker_trade_journal_bars_elapsed(
    symbol_id_str: str,
    entry_start_trade_id: int,
) -> int | None:
    """COUNT(*) x1 баров с entry — только в spawn-процессе (Polars)."""
    symbol = SymbolId[symbol_id_str]
    return count_x1_bars_since_entry_sync(
        symbol_id=symbol,
        entry_start_trade_id=entry_start_trade_id,
    )


def _worker_trade_journal_entry(payload: dict) -> dict:
    entry_policy = payload['entry_policy']
    entry_predictions = payload['entry_predictions']
    open_position(
        symbol_id=payload['symbol_id'],
        side=payload['side'],
        entry_price=float(payload['entry_price']),
        entry_start_trade_id=int(payload['entry_start_trade_id']),
        entry_timestamp_ms=int(payload['entry_timestamp_ms']),
        eval_horizon=payload['eval_horizon'],
        notional_usd=float(payload['notional_usd']),
        policy_action=payload['policy_action'],
        notes=payload['notes'],
        entry_policy=entry_policy,
        entry_predictions=entry_predictions,
    )
    return _build_trade_journal_api_response_with_db_bars_elapsed(
        symbol_id_str=payload['symbol_id'],
        mark_price=float(payload['entry_price']),
    )


def _worker_trade_journal_exit(payload: dict) -> dict:
    exit_overlay = None
    if 'exit_overlay' in payload:
        exit_overlay = payload['exit_overlay']
    closed_trade = close_position(
        exit_price=float(payload['exit_price']),
        exit_start_trade_id=int(payload['exit_start_trade_id']),
        exit_timestamp_ms=int(payload['exit_timestamp_ms']),
        notes=payload['notes'],
        exit_overlay=exit_overlay,
    )
    return _build_trade_journal_api_response_with_db_bars_elapsed(
        symbol_id_str=closed_trade['symbol_id'],
        mark_price=float(payload['exit_price']),
    )


def _worker_trade_journal_discard() -> dict:
    discard_open_position()
    return build_journal_response(get_journal_state(), None, None)
