"""
Spawn subprocess wrapper for Polars-heavy work.

Память Polars/Rust освобождается после завершения дочернего процесса.
"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing
import sys
import threading
import time
from collections.abc import Callable
from typing import Literal

from main.runtime_limits import apply_runtime_limits
from settings import settings

apply_runtime_limits()

logger = logging.getLogger(__name__)

VERBOSE = False

SpawnPoolKind = Literal['heavy', 'light', 'journal']

WORKER_RESULT_POLL_INTERVAL_SEC = 0.5
WORKER_RESULT_TIMEOUT_SEC = 12 * 3600
WORKER_JOIN_TIMEOUT_SEC = 30

_heavy_pool_semaphore = threading.Semaphore(settings.SPAWN_WORKER_HEAVY_POOL_SIZE)
_light_pool_semaphore = threading.Semaphore(settings.SPAWN_WORKER_LIGHT_POOL_SIZE)
_journal_pool_semaphore = threading.Semaphore(settings.SPAWN_WORKER_JOURNAL_POOL_SIZE)


def _pool_semaphore(pool_kind: SpawnPoolKind) -> threading.Semaphore:
    if pool_kind == 'light':
        return _light_pool_semaphore
    if pool_kind == 'journal':
        return _journal_pool_semaphore
    return _heavy_pool_semaphore


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


def _run_single_spawn_process(fn: Callable[..., object], *args: object) -> object:
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


def _execute_spawn_worker(
    pool_kind: SpawnPoolKind,
    fn: Callable[..., object],
    *args: object,
) -> object:
    with _pool_semaphore(pool_kind):
        return _run_single_spawn_process(fn, *args)


def run_in_spawned_process(
    fn: Callable[..., object],
    *args: object,
    pool_kind: SpawnPoolKind = 'heavy',
) -> object:
    """
    Запускает fn(*args) в отдельном процессе (spawn).
    heavy — bars/dow/trade-research (ограниченный пул, защита от OOM).
    light — COUNT x1 bars (может идти параллельно с heavy).
    journal — entry/exit/discard (serialize mutations, pool size 1).
    """
    return _execute_spawn_worker(pool_kind, fn, *args)


async def run_in_spawned_process_async(
    fn: Callable[..., object],
    *args: object,
    pool_kind: SpawnPoolKind = 'heavy',
) -> object:
    """Async-обёртка: ожидание spawn в thread pool, event loop не блокируется."""
    return await asyncio.to_thread(
        _execute_spawn_worker,
        pool_kind,
        fn,
        *args,
    )
