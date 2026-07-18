import os

from fastapi import HTTPException

from main.offline_inference.artifacts import read_trade_research_meta
from main.offline_inference.paths import trade_research_npz_path
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

    meta = read_trade_research_meta(symbol_id=symbol_id)
    npz_path = trade_research_npz_path(symbol_id)

    if meta is None or not os.path.isfile(npz_path):
        raise HTTPException(
            status_code=503,
            detail=(
                f'Trade research artifact not found for {symbol_id}. '
                'Run main.trade_research_export first.'
            ),
        )

    if meta['status'] == 'computing':
        raise HTTPException(
            status_code=503,
            detail='Trade research export is in progress',
        )
    if meta['status'] == 'error':
        error_message = meta['error_message'] if 'error_message' in meta else 'unknown error'
        raise HTTPException(
            status_code=503,
            detail=f'Trade research artifact error: {error_message}',
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
