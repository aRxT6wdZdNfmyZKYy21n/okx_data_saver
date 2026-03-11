"""
Сериализация строк (баров) для JSON.
По контракту данные не содержат NaN/Inf — при их появлении срабатывает assert.
"""

import math


def serialize_bar_row(r: dict) -> dict:
    """Приводит строку бара к JSON-безопасному виду. По контракту значений нет NaN/Inf."""
    out = {}
    for k, v in r.items():
        if isinstance(v, float):
            assert math.isfinite(v), f'Ожидаются только конечные числа, получено {v!r} в поле {k!r}'
        out[k] = v
    return out
