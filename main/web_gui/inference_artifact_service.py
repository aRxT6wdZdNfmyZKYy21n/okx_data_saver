import os

from fastapi import HTTPException

from main.offline_inference.artifacts import read_latest_inference
from settings import settings


def get_inference_artifact(symbol_id: str) -> dict[str, object]:
    if not settings.WEB_GUI_INFERENCE_ENABLED:
        raise HTTPException(status_code=503, detail='Inference is disabled')

    artifact = read_latest_inference(symbol_id=symbol_id)
    if artifact is None:
        raise HTTPException(
            status_code=503,
            detail=(
                f'Inference artifact not found for {symbol_id}. '
                'Start main.inference_daemon first.'
            ),
        )
    return artifact
