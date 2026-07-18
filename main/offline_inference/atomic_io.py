import json
import os
from typing import Any

import numpy as np


def ensure_parent_dir(path: str) -> None:
    parent_dir = os.path.dirname(path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)


def atomic_write_text(path: str, content: str) -> None:
    ensure_parent_dir(path)
    temporary_path = f'{path}.tmp'
    with open(temporary_path, 'w', encoding='utf-8') as output_file:
        output_file.write(content)
    os.replace(temporary_path, path)


def atomic_write_json(path: str, payload: dict[str, Any]) -> None:
    atomic_write_text(
        path=path,
        content=json.dumps(payload, ensure_ascii=False, indent=2),
    )


def read_json(path: str) -> dict[str, Any]:
    with open(path, encoding='utf-8') as input_file:
        loaded = json.load(input_file)
    if not isinstance(loaded, dict):
        raise RuntimeError(f'JSON root must be an object: {path}')
    return loaded


def atomic_write_npz(path: str, **arrays: Any) -> None:
    ensure_parent_dir(path)
    base_path, extension = os.path.splitext(path)
    if extension != '.npz':
        raise RuntimeError(f'NPZ path must end with .npz: {path}')
    temporary_path = f'{base_path}.tmp{extension}'
    np.savez_compressed(temporary_path, **arrays)
    if not os.path.isfile(temporary_path):
        raise RuntimeError(f'NPZ temp file was not created: {temporary_path}')
    os.replace(temporary_path, path)
