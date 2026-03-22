"""
Выполнение запросов с Polars в отдельном процессе (spawn), чтобы избежать утечки памяти.
Одновременно выполняется не более одного такого процесса (ограничение по Lock).
"""

import logging
import multiprocessing
import sys
import threading
from collections.abc import Callable

from enumerations import SymbolId

from main.web_gui.data_service import get_bars_for_api
from main.web_gui.dow_service import get_dow_bars_for_api
from main.web_gui.inference_service import run_remote_inference
from main.web_gui.serialization import serialize_bar_row
from settings import settings

logger = logging.getLogger(__name__)

# Уровень логирования в spawn-процессах: выставляется из __main__ при старте с -v/--verbose.
VERBOSE = False

# Один тяжёлый процесс (бары / Доу) одновременно — чтобы не съедать ресурсы при миллионах сделок.
_process_lock = threading.Lock()

BAR_COLS = [
    'start_trade_id', 'end_trade_id',
    'start_timestamp_ms', 'end_timestamp_ms',
    'open_price', 'high_price', 'low_price', 'close_price',
    'total_volume', 'buy_volume_percent', 'sell_volume_percent', 'total_volume_log2',
]

_SHOW_LIMIT = 50000


def _worker_bars(symbol_id_str: str, limit: int, offset: int, scale: str) -> list[dict] | None:
    """Вызывается в дочернем процессе. Возвращает список сериализованных баров или None."""
    symbol = SymbolId[symbol_id_str]
    effective_limit = min(limit, settings.WEB_GUI_RECORDS_LIMIT)
    df = get_bars_for_api(symbol_id=symbol, limit=effective_limit, offset=offset, scale=scale)
    if df is None:
        return None
    available = [c for c in BAR_COLS if c in df.columns]
    rows = df.select(available).to_dicts()
    return [serialize_bar_row(r) for r in rows][-_SHOW_LIMIT:]


def _worker_dow(symbol_id_str: str, limit: int, level: int) -> list[dict] | None:
    """Вызывается в дочернем процессе. Возвращает список сериализованных баров Доу или None."""
    symbol = SymbolId[symbol_id_str]
    effective_limit = min(limit, settings.WEB_GUI_RECORDS_LIMIT)
    bars = get_dow_bars_for_api(symbol_id=symbol, limit=effective_limit, level=level)
    if bars is None:
        return None
    return [serialize_bar_row(r) for r in bars][-_SHOW_LIMIT:]


def _worker_inference(symbol_id_str: str, limit: int) -> dict[str, float]:
    """Вызывается в дочернем процессе. Возвращает словарь предсказаний."""
    return run_remote_inference(symbol_id=symbol_id_str, limit=limit)


def _run_worker(
    result_queue: multiprocessing.Queue,
    fn: Callable[..., object],
    verbose: bool,
    *args,
) -> None:
    """
    Точка входа в дочерний процесс (должна быть на уровне модуля, чтобы pickle при spawn).
    """
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
        stream=sys.stdout,
    )
    try:
        result = fn(*args)
        result_queue.put(('ok', result))
    except Exception as e:
        logger.exception('Worker failed')
        result_queue.put(('error', str(e)))


def run_in_spawned_process(fn: Callable[..., object], *args) -> object:
    """
    Запускает fn(*args) в отдельном процессе (spawn). Возвращает результат из дочернего процесса.
    Используется для запросов с Polars, чтобы после завершения процесса память освобождалась.
    Одновременно выполняется не более одного вызова (ограничение по _process_lock).
    """
    with _process_lock:
        ctx = multiprocessing.get_context('spawn')
        result_queue = ctx.Queue()
        p = ctx.Process(target=_run_worker, args=(result_queue, fn, VERBOSE) + args)
        p.start()
        # Сначала читаем результат, затем join: при большом ответе дочерний процесс
        # блокируется на put(), пока родитель не прочитает очередь — иначе дедлок.
        kind, payload = result_queue.get()
        p.join()
    if kind == 'error':
        raise RuntimeError(f'Worker error: {payload}')
    return payload
