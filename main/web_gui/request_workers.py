"""
Выполнение запросов с Polars в отдельном процессе (spawn), чтобы избежать утечки памяти.
heavy/light пулы в main.spawn_process (Polars только внутри дочернего процесса).
"""

import logging
import time

from enumerations import SymbolId

from main.spawn_process import run_in_spawned_process
from main.web_gui.data_service import get_bars_for_api_sync, count_x1_bars_since_entry_sync
from main.web_gui.dow_service import get_dow_bars_for_api
from main.web_gui.inference_service import run_remote_inference
from main.web_gui.trade_research_artifact_service import run_trade_research_from_artifact
from main.web_gui.serialization import serialize_bar_row
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
    started = time.monotonic()
    logger.info(
        'worker_bars start symbol=%s scale=%s limit=%d offset=%d',
        symbol_id_str,
        scale,
        limit,
        offset,
    )
    symbol = SymbolId[symbol_id_str]
    effective_limit = min(limit, settings.WEB_GUI_RECORDS_LIMIT)
    df = get_bars_for_api_sync(symbol_id=symbol, limit=effective_limit, offset=offset, scale=scale)
    if df is None:
        duration_ms = int((time.monotonic() - started) * 1000)
        logger.info(
            'worker_bars done symbol=%s scale=%s rows=0 duration_ms=%d',
            symbol_id_str,
            scale,
            duration_ms,
        )
        return None
    available = [c for c in BAR_COLS if c in df.columns]
    rows = df.select(available).to_dicts()
    result = [serialize_bar_row(r) for r in rows][-CHART_SHOW_LIMIT:]
    duration_ms = int((time.monotonic() - started) * 1000)
    logger.info(
        'worker_bars done symbol=%s scale=%s rows=%d returned=%d duration_ms=%d',
        symbol_id_str,
        scale,
        int(df.height),
        len(result),
        duration_ms,
    )
    return result


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
    started = time.monotonic()
    logger.info(
        'worker_trade_research start symbol=%s eval_horizon=%s step_bars=%d '
        'visible_min=%s visible_max=%s',
        symbol_id_str,
        eval_horizon,
        step_bars,
        visible_min_start_trade_id,
        visible_max_start_trade_id,
    )
    payload = run_trade_research_from_artifact(
        symbol_id=symbol_id_str,
        eval_horizon=eval_horizon,
        step_bars=step_bars,
        visible_min_start_trade_id=visible_min_start_trade_id,
        visible_max_start_trade_id=visible_max_start_trade_id,
    )
    duration_ms = int((time.monotonic() - started) * 1000)
    segment_count = payload['segment_count'] if 'segment_count' in payload else '?'
    logger.info(
        'worker_trade_research done symbol=%s segments=%s duration_ms=%d',
        symbol_id_str,
        segment_count,
        duration_ms,
    )
    return payload


def _worker_inference_cycle_safe(symbol_id_str: str) -> None:
    from main.offline_inference.inference_cycle import run_inference_cycle_safe

    run_inference_cycle_safe(symbol_id=symbol_id_str)


def _worker_trade_research_export_safe(
    symbol_id_str: str,
    num_workers: int,
    prefetch_factor: int,
) -> None:
    from main.offline_inference.trade_research_export import run_trade_research_export_safe

    run_trade_research_export_safe(
        symbol_id=symbol_id_str,
        num_workers=num_workers,
        prefetch_factor=prefetch_factor,
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
