import os
import time
from typing import Any

from main.offline_inference.atomic_io import atomic_write_json, read_json
from main.offline_inference.paths import inference_artifact_path, last_inference_ok_path


def _now_ms() -> int:
    return int(time.time() * 1000)


_INFERENCE_SNAPSHOT_KEYS = (
    'predictions',
    'policy',
    'entry_hint',
    'exit_policy',
    'exit_transformer',
    'bar_start_trade_id',
    'bar_timestamp_ms',
    'inference_completed_at_ms',
)


def _copy_inference_snapshot(
    source: dict[str, Any],
    target: dict[str, Any],
) -> None:
    for key in _INFERENCE_SNAPSHOT_KEYS:
        if key in source:
            target[key] = source[key]
    if 'inference_completed_at_ms' not in target and 'updated_at_ms' in source:
        target['inference_completed_at_ms'] = source['updated_at_ms']


def read_last_inference_ok(symbol_id: str) -> dict[str, Any] | None:
    path = last_inference_ok_path(symbol_id)
    if not os.path.isfile(path):
        return None
    return read_json(path)


def write_last_inference_ok_sidecar(
    symbol_id: str,
    artifact: dict[str, Any],
) -> None:
    sidecar: dict[str, Any] = {
        'symbol_id': symbol_id,
    }
    _copy_inference_snapshot(source=artifact, target=sidecar)
    if 'inference_completed_at_ms' not in sidecar:
        sidecar['inference_completed_at_ms'] = artifact['updated_at_ms']
    atomic_write_json(
        path=last_inference_ok_path(symbol_id),
        payload=sidecar,
    )


def ensure_last_inference_ok_sidecar(
    symbol_id: str,
    artifact: dict[str, Any],
) -> None:
    if 'predictions' not in artifact:
        return
    existing = read_last_inference_ok(symbol_id=symbol_id)
    if existing is not None and 'predictions' in existing:
        return
    write_last_inference_ok_sidecar(symbol_id=symbol_id, artifact=artifact)


def enrich_inference_artifact(
    artifact: dict[str, Any],
) -> dict[str, Any]:
    if artifact['status'] != 'computing':
        return artifact
    if 'predictions' in artifact:
        return artifact
    symbol_id = str(artifact['symbol_id'])
    last_ok = read_last_inference_ok(symbol_id=symbol_id)
    if last_ok is None:
        return artifact
    enriched = dict(artifact)
    _copy_inference_snapshot(source=last_ok, target=enriched)
    return enriched


def write_latest_inference_computing(symbol_id: str) -> None:
    now_ms = _now_ms()
    payload: dict[str, Any] = {
        'status': 'computing',
        'updated_at_ms': now_ms,
        'computing_started_at_ms': now_ms,
        'symbol_id': symbol_id,
    }
    existing = read_latest_inference(symbol_id=symbol_id)
    if existing is not None and existing['status'] in ('ok', 'computing'):
        _copy_inference_snapshot(source=existing, target=payload)
    if 'predictions' not in payload:
        last_ok = read_last_inference_ok(symbol_id=symbol_id)
        if last_ok is not None:
            _copy_inference_snapshot(source=last_ok, target=payload)
    atomic_write_json(
        path=inference_artifact_path(symbol_id),
        payload=payload,
    )


def write_latest_inference_ok(
    symbol_id: str,
    payload: dict[str, Any],
) -> None:
    artifact = dict(payload)
    artifact['status'] = 'ok'
    artifact['updated_at_ms'] = _now_ms()
    artifact['inference_completed_at_ms'] = artifact['updated_at_ms']
    artifact['symbol_id'] = symbol_id
    if 'computing_started_at_ms' in artifact:
        del artifact['computing_started_at_ms']
    atomic_write_json(
        path=inference_artifact_path(symbol_id),
        payload=artifact,
    )
    write_last_inference_ok_sidecar(symbol_id=symbol_id, artifact=artifact)


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
    eval_horizon: str,
    payload: dict[str, Any],
) -> None:
    from main.offline_inference.paths import trade_research_horizon_dir, trade_research_meta_path

    os.makedirs(trade_research_horizon_dir(symbol_id, eval_horizon), exist_ok=True)
    artifact = dict(payload)
    artifact['updated_at_ms'] = _now_ms()
    artifact['symbol_id'] = symbol_id
    artifact['eval_horizon'] = eval_horizon
    atomic_write_json(
        path=trade_research_meta_path(symbol_id, eval_horizon),
        payload=artifact,
    )


def read_trade_research_meta(
    symbol_id: str,
    eval_horizon: str,
) -> dict[str, Any] | None:
    from main.offline_inference.paths import trade_research_meta_path

    path = trade_research_meta_path(symbol_id, eval_horizon)
    if not os.path.isfile(path):
        return None
    return read_json(path)


def write_trade_research_inference_meta(
    symbol_id: str,
    eval_horizon: str,
    payload: dict[str, Any],
) -> None:
    from main.offline_inference.paths import (
        trade_research_horizon_dir,
        trade_research_inference_meta_path,
    )

    os.makedirs(trade_research_horizon_dir(symbol_id, eval_horizon), exist_ok=True)
    artifact = dict(payload)
    artifact['updated_at_ms'] = _now_ms()
    artifact['symbol_id'] = symbol_id
    artifact['eval_horizon'] = eval_horizon
    atomic_write_json(
        path=trade_research_inference_meta_path(symbol_id, eval_horizon),
        payload=artifact,
    )


def read_trade_research_inference_meta(
    symbol_id: str,
    eval_horizon: str,
) -> dict[str, Any] | None:
    from main.offline_inference.paths import trade_research_inference_meta_path

    path = trade_research_inference_meta_path(symbol_id, eval_horizon)
    if not os.path.isfile(path):
        return None
    return read_json(path)


def resolve_trade_research_inference_artifact(
    symbol_id: str,
    eval_horizon: str,
) -> tuple[str, dict[str, Any]] | None:
    from main.offline_inference.paths import trade_research_inference_npz_path

    meta = read_trade_research_inference_meta(
        symbol_id=symbol_id,
        eval_horizon=eval_horizon,
    )
    npz_path = trade_research_inference_npz_path(symbol_id, eval_horizon)
    if meta is not None and os.path.isfile(npz_path):
        return npz_path, meta
    return None


def _legacy_trade_research_meta(symbol_id: str) -> dict[str, Any] | None:
    from main.offline_inference.paths import trade_research_legacy_meta_path

    path = trade_research_legacy_meta_path(symbol_id)
    if not os.path.isfile(path):
        return None
    return read_json(path)


def list_trade_research_horizons(symbol_id: str) -> list[str]:
    from main.offline_inference.paths import (
        trade_research_dir,
        trade_research_meta_path,
        trade_research_npz_path,
    )

    horizons: list[str] = []
    base_dir = trade_research_dir(symbol_id)
    if os.path.isdir(base_dir):
        for entry_name in sorted(os.listdir(base_dir)):
            entry_path = os.path.join(base_dir, entry_name)
            if not os.path.isdir(entry_path):
                continue
            if not entry_name.startswith('x'):
                continue
            if not os.path.isfile(trade_research_meta_path(symbol_id, entry_name)):
                continue
            if not os.path.isfile(trade_research_npz_path(symbol_id, entry_name)):
                continue
            horizons.append(entry_name)

    legacy_meta = _legacy_trade_research_meta(symbol_id=symbol_id)
    if legacy_meta is not None:
        from main.offline_inference.paths import trade_research_legacy_npz_path

        if os.path.isfile(trade_research_legacy_npz_path(symbol_id)):
            if 'eval_horizon' in legacy_meta:
                legacy_horizon = str(legacy_meta['eval_horizon'])
                if legacy_horizon not in horizons:
                    horizons.append(legacy_horizon)

    horizons.sort(key=lambda horizon_name: int(horizon_name[1:]))
    return horizons


def resolve_trade_research_artifact(
    symbol_id: str,
    eval_horizon: str,
) -> tuple[str, dict[str, Any]] | None:
    from main.offline_inference.paths import (
        trade_research_legacy_npz_path,
        trade_research_npz_path,
    )

    meta = read_trade_research_meta(symbol_id=symbol_id, eval_horizon=eval_horizon)
    npz_path = trade_research_npz_path(symbol_id, eval_horizon)
    if meta is not None and os.path.isfile(npz_path):
        return npz_path, meta

    legacy_meta = _legacy_trade_research_meta(symbol_id=symbol_id)
    legacy_npz_path = trade_research_legacy_npz_path(symbol_id)
    if legacy_meta is None or not os.path.isfile(legacy_npz_path):
        return None
    if 'eval_horizon' not in legacy_meta:
        return None
    legacy_horizon = str(legacy_meta['eval_horizon'])
    if legacy_horizon != eval_horizon:
        return None
    return legacy_npz_path, legacy_meta
