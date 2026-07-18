"""
Spawn subprocess wrapper for Polars-heavy work.

Память Polars/Rust освобождается после завершения дочернего процесса.
"""

from __future__ import annotations

import logging
import multiprocessing
import sys
import threading
import time
from collections.abc import Callable

from main.runtime_limits import apply_runtime_limits

apply_runtime_limits()

logger = logging.getLogger(__name__)

VERBOSE = False

_process_lock = threading.Lock()

WORKER_RESULT_POLL_INTERVAL_SEC = 0.5
WORKER_RESULT_TIMEOUT_SEC = 12 * 3600
WORKER_JOIN_TIMEOUT_SEC = 30


def _run_worker(
    result_queue: multiprocessing.Queue,
    fn: Callable[..., object],
    verbose: bool,
    *args: object,
) -> None:
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
        stream=sys.stdout,
    )
    try:
        result = fn(*args)
        result_queue.put(('ok', result))
    except Exception as exception:
        logger.exception('Worker failed')
        result_queue.put(('error', str(exception)))


def _terminate_process(process: multiprocessing.Process) -> None:
    process.terminate()
    process.join(timeout=WORKER_JOIN_TIMEOUT_SEC)
    if process.is_alive():
        process.kill()
        process.join(timeout=WORKER_JOIN_TIMEOUT_SEC)


def run_in_spawned_process(fn: Callable[..., object], *args: object) -> object:
    """
    Запускает fn(*args) в отдельном процессе (spawn).
    Одновременно выполняется не более одного такого процесса.
    """
    with _process_lock:
        ctx = multiprocessing.get_context('spawn')
        result_queue = ctx.Queue()
        process = ctx.Process(
            target=_run_worker,
            args=(result_queue, fn, VERBOSE) + args,
        )
        process.start()

        deadline = time.monotonic() + WORKER_RESULT_TIMEOUT_SEC
        kind: str | None = None
        payload: object = None
        while time.monotonic() < deadline:
            if not result_queue.empty():
                kind, payload = result_queue.get_nowait()
                break
            if not process.is_alive():
                exit_code = process.exitcode
                process.join(timeout=WORKER_JOIN_TIMEOUT_SEC)
                raise RuntimeError(
                    f'Spawn worker died without result (exit_code={exit_code})',
                )
            time.sleep(WORKER_RESULT_POLL_INTERVAL_SEC)
        else:
            _terminate_process(process)
            raise RuntimeError(
                f'Spawn worker timed out after {WORKER_RESULT_TIMEOUT_SEC}s',
            )

        process.join(timeout=WORKER_JOIN_TIMEOUT_SEC)
        if process.is_alive():
            _terminate_process(process)
            raise RuntimeError('Spawn worker did not exit after sending result')

    if kind == 'error':
        raise RuntimeError(f'Worker error: {payload}')
    return payload
