"""Redis checkpoint for stateful inference dataset (RFC 160)."""

from __future__ import annotations

import json
import logging
import time
import uuid

import polars

from constants.redis import (
    MAX_PART_SIZE_BYTES,
    get_inference_dataset_state_meta_key,
    get_inference_dataset_state_raw_x1_key,
    get_inference_dataset_state_write_lock_key,
)
from enumerations import CompressionAlgorithm, SymbolId
from main.web_gui.async_data_runtime import run_async_data
from settings import settings
from utils.redis import g_redis_manager

logger = logging.getLogger(__name__)


async def _load_checkpoint_meta(
    meta_key: str,
) -> dict[str, object] | None:
    raw_meta = await g_redis_manager.get(meta_key)
    if raw_meta is None:
        return None
    return json.loads(raw_meta.decode('utf-8'))


async def _save_checkpoint_meta(
    meta_key: str,
    meta: dict[str, object],
) -> None:
    await g_redis_manager.set(
        key=meta_key,
        value=json.dumps(meta),
        ttl=settings.INFERENCE_DATASET_STATE_REDIS_TTL_SEC,
    )


async def load_inference_dataset_state_raw_x1(
    symbol_id: SymbolId,
    config_hash: str,
) -> tuple[dict[str, object], polars.DataFrame] | None:
    meta_key = get_inference_dataset_state_meta_key(
        symbol_id=symbol_id,
        config_hash=config_hash,
    )
    meta = await _load_checkpoint_meta(meta_key=meta_key)
    if meta is None:
        return None

    raw_key = get_inference_dataset_state_raw_x1_key(
        symbol_id=symbol_id,
        config_hash=config_hash,
    )
    raw_x1 = await g_redis_manager.load_dataframe(raw_key)
    if raw_x1 is None:
        logger.warning(
            'inference dataset state meta exists but raw_x1 missing: symbol=%s hash=%s',
            symbol_id.name,
            config_hash,
        )
        return None

    if str(meta['config_hash']) != config_hash:
        raise RuntimeError(
            f'checkpoint config_hash mismatch in meta: {meta["config_hash"]} != {config_hash}',
        )

    logger.info(
        'Redis inference dataset state hit: symbol=%s rows=%d last_start_trade_id=%s',
        symbol_id.name,
        int(raw_x1.height),
        meta['last_start_trade_id'],
    )
    return meta, raw_x1


async def save_inference_dataset_state(
    symbol_id: SymbolId,
    config_hash: str,
    raw_x1: polars.DataFrame,
    meta: dict[str, object],
) -> None:
    lock_key = get_inference_dataset_state_write_lock_key(
        symbol_id=symbol_id,
        config_hash=config_hash,
    )
    lock_token = str(uuid.uuid4())
    acquired = await g_redis_manager.try_acquire_lock(
        lock_key=lock_key,
        lock_token=lock_token,
        ttl_sec=settings.BARS_REDIS_REFRESH_LOCK_TTL_SEC,
    )
    if not acquired:
        raise RuntimeError(
            f'inference dataset state write lock busy: symbol={symbol_id.name}',
        )

    started_at = time.monotonic()
    try:
        raw_key = get_inference_dataset_state_raw_x1_key(
            symbol_id=symbol_id,
            config_hash=config_hash,
        )
        meta_key = get_inference_dataset_state_meta_key(
            symbol_id=symbol_id,
            config_hash=config_hash,
        )
        await g_redis_manager.save_dataframe(
            key=raw_key,
            dataframe=raw_x1,
            compression=CompressionAlgorithm.LZ4,
            max_size_bytes=MAX_PART_SIZE_BYTES,
            ttl_sec=settings.INFERENCE_DATASET_STATE_REDIS_TTL_SEC,
        )
        meta['updated_at_ms'] = int(time.time() * 1000.0)
        await _save_checkpoint_meta(meta_key=meta_key, meta=meta)
        duration_ms = int((time.monotonic() - started_at) * 1000.0)
        logger.info(
            'Redis inference dataset state saved: symbol=%s rows=%d duration_ms=%d',
            symbol_id.name,
            int(raw_x1.height),
            duration_ms,
        )
    finally:
        await g_redis_manager.release_lock(
            lock_key=lock_key,
            lock_token=lock_token,
        )


def load_inference_dataset_state_raw_x1_sync(
    symbol_id: SymbolId,
    config_hash: str,
) -> tuple[dict[str, object], polars.DataFrame] | None:
    return run_async_data(
        lambda: load_inference_dataset_state_raw_x1(
            symbol_id=symbol_id,
            config_hash=config_hash,
        ),
    )


def save_inference_dataset_state_sync(
    symbol_id: SymbolId,
    config_hash: str,
    raw_x1: polars.DataFrame,
    meta: dict[str, object],
) -> None:
    run_async_data(
        lambda: save_inference_dataset_state(
            symbol_id=symbol_id,
            config_hash=config_hash,
            raw_x1=raw_x1,
            meta=meta,
        ),
    )
