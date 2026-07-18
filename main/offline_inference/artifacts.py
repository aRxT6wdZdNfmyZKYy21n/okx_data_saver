import os
import time
from typing import Any

from main.offline_inference.atomic_io import atomic_write_json, read_json
from main.offline_inference.paths import inference_artifact_path


def _now_ms() -> int:
    return int(time.time() * 1000)


def write_latest_inference_computing(symbol_id: str) -> None:
    atomic_write_json(
        path=inference_artifact_path(symbol_id),
        payload={
            'status': 'computing',
            'updated_at_ms': _now_ms(),
            'symbol_id': symbol_id,
        },
    )


def write_latest_inference_ok(
    symbol_id: str,
    payload: dict[str, Any],
) -> None:
    artifact = dict(payload)
    artifact['status'] = 'ok'
    artifact['updated_at_ms'] = _now_ms()
    artifact['symbol_id'] = symbol_id
    atomic_write_json(
        path=inference_artifact_path(symbol_id),
        payload=artifact,
    )


def write_latest_inference_error(
    symbol_id: str,
    error_message: str,
) -> None:
    atomic_write_json(
        path=inference_artifact_path(symbol_id),
        payload={
            'status': 'error',
            'updated_at_ms': _now_ms(),
            'symbol_id': symbol_id,
            'error_message': error_message,
        },
    )


def read_latest_inference(symbol_id: str) -> dict[str, Any] | None:
    path = inference_artifact_path(symbol_id)
    if not os.path.isfile(path):
        return None
    return read_json(path)


def write_trade_research_meta(
    symbol_id: str,
    payload: dict[str, Any],
) -> None:
    from main.offline_inference.paths import trade_research_meta_path

    artifact = dict(payload)
    artifact['updated_at_ms'] = _now_ms()
    artifact['symbol_id'] = symbol_id
    atomic_write_json(
        path=trade_research_meta_path(symbol_id),
        payload=artifact,
    )


def read_trade_research_meta(symbol_id: str) -> dict[str, Any] | None:
    from main.offline_inference.paths import trade_research_meta_path

    path = trade_research_meta_path(symbol_id)
    if not os.path.isfile(path):
        return None
    return read_json(path)
