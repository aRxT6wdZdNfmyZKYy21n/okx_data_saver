"""
Выполнение запросов с Polars в отдельном процессе (spawn), чтобы избежать утечки памяти.
"""

import logging
import multiprocessing
from collections.abc import Callable

from enumerations import SymbolId

from main.web_gui.data_service import get_bars_for_api
from main.web_gui.dow_service import get_dow_bars_for_api
from main.web_gui.serialization import serialize_bar_row
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
    df = get_bars_for_api(symbol_id=symbol, limit=effective_limit, offset=offset, scale=scale)
    if df is None:
        return None
    available = [c for c in BAR_COLS if c in df.columns]
    rows = df.select(available).to_dicts()
    return [serialize_bar_row(r) for r in rows]


def _worker_dow(symbol_id_str: str, limit: int, level: int) -> list[dict] | None:
    """Вызывается в дочернем процессе. Возвращает список сериализованных баров Доу или None."""
    symbol = SymbolId[symbol_id_str]
    effective_limit = min(limit, settings.WEB_GUI_RECORDS_LIMIT)
    bars = get_dow_bars_for_api(symbol_id=symbol, limit=effective_limit, level=level)
    if bars is None:
        return None
    return [serialize_bar_row(r) for r in bars]


def run_in_spawned_process(fn: Callable[..., list[dict] | None], *args) -> list[dict] | None:
    """
    Запускает fn(*args) в отдельном процессе (spawn). Возвращает результат из дочернего процесса.
    Используется для запросов с Polars, чтобы после завершения процесса память освобождалась.
    """
    ctx = multiprocessing.get_context('spawn')
    result_queue = ctx.Queue()

    def wrapper() -> None:
        try:
            result = fn(*args)
            result_queue.put(('ok', result))
        except Exception as e:
            logger.exception('Worker failed')
            result_queue.put(('error', str(e)))

    p = ctx.Process(target=wrapper)
    p.start()
    p.join()
    kind, payload = result_queue.get()
    if kind == 'error':
        raise RuntimeError(f'Worker error: {payload}')
    return payload
