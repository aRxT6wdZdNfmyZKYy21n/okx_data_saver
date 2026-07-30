import os

from fastapi import HTTPException

from main.offline_inference.artifacts import (
    list_trade_research_horizons,
    resolve_trade_research_artifact,
)
from main.offline_inference.trade_research_loader import load_trade_research_response
from settings import settings


def run_trade_research_from_artifact(
    symbol_id: str,
    eval_horizon: str,
    step_bars: int,
    visible_min_start_trade_id: int | None,
    visible_max_start_trade_id: int | None,
) -> dict[str, object]:
    if not settings.WEB_GUI_INFERENCE_ENABLED:
        raise HTTPException(status_code=503, detail='Inference is disabled')

    resolved = resolve_trade_research_artifact(
        symbol_id=symbol_id,
        eval_horizon=eval_horizon,
    )
    if resolved is None:
        available_horizons = list_trade_research_horizons(symbol_id=symbol_id)
        if len(available_horizons) == 0:
            raise HTTPException(
                status_code=503,
                detail=(
                    f'Trade research artifact not found for {symbol_id} @ {eval_horizon}. '
                    'Run main.trade_research_export first.'
                ),
            )
        raise HTTPException(
            status_code=503,
            detail=(
                f'Trade research artifact not found for {symbol_id} @ {eval_horizon}. '
                f'Available horizons: {", ".join(available_horizons)}. '
                'Run main.trade_research_export with matching inference policy.'
            ),
        )

    npz_path, meta = resolved

    if meta['status'] == 'computing':
        raise HTTPException(
            status_code=503,
            detail=f'Trade research export is in progress for {eval_horizon}',
        )
    if meta['status'] == 'error':
        error_message = meta['error_message'] if 'error_message' in meta else 'unknown error'
        raise HTTPException(
            status_code=503,
            detail=f'Trade research artifact error @ {eval_horizon}: {error_message}',
        )

    return load_trade_research_response(
        symbol_id=symbol_id,
        eval_horizon=eval_horizon,
        step_bars=step_bars,
        visible_min_start_trade_id=visible_min_start_trade_id,
        visible_max_start_trade_id=visible_max_start_trade_id,
        meta=meta,
        npz_path=npz_path,
    )
