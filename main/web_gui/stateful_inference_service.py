"""Stateful inference dataset cycle (Redis checkpoint + incremental DB delta)."""

from __future__ import annotations

import logging
import time

import polars

from main.offline_inference.trading_bot_imports import ensure_trading_bot_on_path

ensure_trading_bot_on_path()

from enumerations import SymbolId
from main.web_gui.data_service import fetch_last_bars_sync
from main.web_gui.inference_dataset_state_service import (
    load_inference_dataset_state_raw_x1_sync,
    save_inference_dataset_state_sync,
)
from main.web_gui.inference_service import (
    _prepare_inference_context_from_dataset,
    fetch_inference_metadata,
)
from main.web_gui.stateful_inference_common import build_stateful_inference_config
from settings import settings
from trading_bot_dataset.src.stateful_inference_common import DatasetContinuityError
from trading_bot_dataset.src.stateful_inference_dataset import (
    StatefulHybridTradeDatasetInference,
)

logger = logging.getLogger(__name__)


def prepare_stateful_inference_context(
    symbol_id: str,
) -> tuple[dict, dict[str, int | float]]:
    started_at = time.monotonic()
    symbol = SymbolId[symbol_id]
    metadata = fetch_inference_metadata()
    config = build_stateful_inference_config(metadata)

    checkpoint = load_inference_dataset_state_raw_x1_sync(
        symbol_id=symbol,
        config_hash=config.config_hash,
    )

    if checkpoint is None:
        logger.info(
            'Stateful inference cold start: symbol=%s config_hash=%s',
            symbol_id,
            config.config_hash,
        )
        raw_df = fetch_last_bars_sync(
            symbol_id=symbol,
            limit=settings.INFERENCE_DAEMON_BARS_LIMIT,
            offset=0,
            since_start_trade_id=None,
        )
        if raw_df is None:
            raise RuntimeError('Недостаточно данных для stateful cold start')
        stateful = StatefulHybridTradeDatasetInference.cold_start_from_dataframe(
            config=config,
            dataframe=raw_df,
        )
    else:
        _checkpoint_meta, raw_x1 = checkpoint
        stateful = StatefulHybridTradeDatasetInference.from_raw_checkpoint(
            config=config,
            raw_x1=raw_x1,
        )
        delta = fetch_last_bars_sync(
            symbol_id=symbol,
            limit=settings.INFERENCE_DATASET_STATE_DELTA_FETCH_LIMIT,
            offset=0,
            since_start_trade_id=stateful.last_start_trade_id,
        )
        if delta is None:
            delta = polars.DataFrame()
        if delta.height > 0:
            try:
                delta_result = stateful.apply_x1_delta(delta)
                logger.info(
                    'Stateful delta applied: action=%s rows=%d duration_ms=%d',
                    delta_result.action,
                    delta_result.rows_affected,
                    delta_result.duration_ms,
                )
            except DatasetContinuityError as exception:
                logger.error(
                    'Stateful delta continuity error; cold start fallback: %s',
                    exception,
                )
                raw_df = fetch_last_bars_sync(
                    symbol_id=symbol,
                    limit=settings.INFERENCE_DAEMON_BARS_LIMIT,
                    offset=0,
                    since_start_trade_id=None,
                )
                if raw_df is None:
                    raise RuntimeError(
                        'Недостаточно данных для stateful cold start fallback',
                    ) from exception
                stateful = StatefulHybridTradeDatasetInference.cold_start_from_dataframe(
                    config=config,
                    dataframe=raw_df,
                )

    required_rows = int(metadata['sequence_length']) * int(metadata['max_scale'])
    if stateful.raw_x1.height < required_rows:
        raise RuntimeError(
            'Инференс невозможен при таком количестве свечей x1 '
            f'(требуется минимум {required_rows}, получено {stateful.raw_x1.height})',
        )

    checkpoint_meta = stateful.checkpoint_meta()
    save_inference_dataset_state_sync(
        symbol_id=symbol,
        config_hash=config.config_hash,
        raw_x1=stateful.raw_x1,
        meta=checkpoint_meta,
    )

    payload_dict, provenance = _prepare_inference_context_from_dataset(
        df=stateful.raw_x1,
        metadata=metadata,
        inference_dataset=stateful.inference_dataset,
    )
    duration_ms = int((time.monotonic() - started_at) * 1000.0)
    logger.info(
        'Stateful inference context ready: symbol=%s raw_rows=%d duration_ms=%d',
        symbol_id,
        stateful.raw_x1.height,
        duration_ms,
    )
    return payload_dict, provenance


def run_stateful_inference_and_x_seq(
    symbol_id: str,
) -> tuple[dict[str, object], dict[str, object], dict[str, int | float]]:
    from main.web_gui.inference_service import (
        run_remote_inference_from_payload_dict,
        x_seq_2d_from_payload_dict,
    )

    payload_dict, provenance = prepare_stateful_inference_context(
        symbol_id=symbol_id,
    )
    inference_result = run_remote_inference_from_payload_dict(
        symbol_id=symbol_id,
        payload_dict=payload_dict,
    )
    x_seq = x_seq_2d_from_payload_dict(payload_dict)
    return inference_result, x_seq, provenance
