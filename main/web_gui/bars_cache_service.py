from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections.abc import Awaitable, Callable

import polars

from constants.redis import (
    BARS_REFRESH_LOCK_KEY,
    MAX_PART_SIZE_BYTES,
    get_web_gui_x1_bars_key,
    get_web_gui_x1_bars_meta_key,
)
from enumerations import CompressionAlgorithm, SymbolId
from settings import settings
from utils.redis import g_redis_manager

logger = logging.getLogger(__name__)


def _bars_meta_from_dataframe(
    symbol_id: SymbolId,
    limit: int,
    offset: int,
    dataframe: polars.DataFrame,
) -> dict[str, int | str]:
    first_row = dataframe.row(0, named=True)
    last_row = dataframe.row(dataframe.height - 1, named=True)
    return {
        'symbol_id': symbol_id.name,
        'limit': limit,
        'offset': offset,
        'row_count': int(dataframe.height),
        'first_start_trade_id': int(first_row['start_trade_id']),
        'last_start_trade_id': int(last_row['start_trade_id']),
        'updated_at_ms': int(time.time() * 1000),
    }


async def _load_cached_bars(
    cache_key: str,
) -> polars.DataFrame | None:
    dataframe = await g_redis_manager.load_dataframe(cache_key)
    if dataframe is None:
        return None
    logger.info('Redis cache hit: %s rows=%d', cache_key, int(dataframe.height))
    return dataframe


async def _save_cached_bars(
    symbol_id: SymbolId,
    limit: int,
    offset: int,
    dataframe: polars.DataFrame,
) -> None:
    cache_key = get_web_gui_x1_bars_key(
        symbol_id=symbol_id,
        limit=limit,
        offset=offset,
    )
    bars_meta_key = get_web_gui_x1_bars_meta_key(
        symbol_id=symbol_id,
        limit=limit,
        offset=offset,
    )
    bars_meta = _bars_meta_from_dataframe(
        symbol_id=symbol_id,
        limit=limit,
        offset=offset,
        dataframe=dataframe,
    )
    await g_redis_manager.save_dataframe(
        key=cache_key,
        dataframe=dataframe,
        compression=CompressionAlgorithm.LZ4,
        max_size_bytes=MAX_PART_SIZE_BYTES,
    )
    await g_redis_manager.set(
        key=bars_meta_key,
        value=json.dumps(bars_meta),
        ttl=settings.BARS_REDIS_CACHE_TTL_SEC,
    )
    logger.info(
        'Redis cache saved: %s rows=%d last_start_trade_id=%d',
        cache_key,
        int(dataframe.height),
        int(bars_meta['last_start_trade_id']),
    )


async def fetch_last_bars_with_redis_cache(
    symbol_id: SymbolId,
    limit: int,
    offset: int,
    load_from_db: Callable[[], Awaitable[polars.DataFrame | None]],
) -> polars.DataFrame | None:
    if not settings.WEB_GUI_BARS_REDIS_CACHE_ENABLED:
        return await load_from_db()

    cache_key = get_web_gui_x1_bars_key(
        symbol_id=symbol_id,
        limit=limit,
        offset=offset,
    )

    cached = await _load_cached_bars(cache_key=cache_key)
    if cached is not None:
        return cached

    lock_token = str(uuid.uuid4())
    acquired = await g_redis_manager.try_acquire_lock(
        lock_key=BARS_REFRESH_LOCK_KEY,
        lock_token=lock_token,
        ttl_sec=settings.BARS_REDIS_REFRESH_LOCK_TTL_SEC,
    )
    if not acquired:
        logger.info(
            'Redis refresh lock busy; waiting for cache refresh (symbol=%s limit=%d offset=%d)',
            symbol_id.name,
            limit,
            offset,
        )
        deadline = time.monotonic() + float(settings.BARS_REDIS_LOCK_WAIT_SEC)
        poll_interval = float(settings.BARS_REDIS_LOCK_POLL_INTERVAL_SEC)
        while time.monotonic() < deadline:
            cached_while_waiting = await _load_cached_bars(cache_key=cache_key)
            if cached_while_waiting is not None:
                return cached_while_waiting

            acquired = await g_redis_manager.try_acquire_lock(
                lock_key=BARS_REFRESH_LOCK_KEY,
                lock_token=lock_token,
                ttl_sec=settings.BARS_REDIS_REFRESH_LOCK_TTL_SEC,
            )
            if acquired:
                break
            await asyncio.sleep(poll_interval)

        if not acquired:
            raise RuntimeError(
                'Timed out waiting for bars Redis cache refresh lock '
                f'({settings.BARS_REDIS_LOCK_WAIT_SEC}s)',
            )

    try:
        cached_after_lock = await _load_cached_bars(cache_key=cache_key)
        if cached_after_lock is not None:
            return cached_after_lock

        logger.info(
            'Redis cache miss; loading x1 bars from DB (symbol=%s limit=%d offset=%d)',
            symbol_id.name,
            limit,
            offset,
        )
        dataframe = await load_from_db()
        if dataframe is None:
            return None

        await _save_cached_bars(
            symbol_id=symbol_id,
            limit=limit,
            offset=offset,
            dataframe=dataframe,
        )
        return dataframe
    finally:
        if acquired:
            await g_redis_manager.release_lock(
                lock_key=BARS_REFRESH_LOCK_KEY,
                lock_token=lock_token,
            )
