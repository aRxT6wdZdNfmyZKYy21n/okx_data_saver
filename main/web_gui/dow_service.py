"""
Сервис теории Доу для веб-GUI: прогон баров через калькулятор трендов, получение OHLCV по уровням.
Требует скомпилированный C++ модуль; при его отсутствии приложение не запускается.
"""

import logging
import math

import torch

from dow_theory_aggregator.src.trend_calculator_cpp_wrapper import (
    IncrementalTrendState,
    TrendCalculator,
)
from enumerations import SymbolId
from main.web_gui.data_service import fetch_last_bars
from settings import settings

logger = logging.getLogger(__name__)


DOW_LEVELS_COUNT = 10
LOG_EVERY_N_ROWS = 1000


def _bar_row_to_dow_row(bar: dict) -> dict[str, float | int]:
    """Преобразует бар (dict из Polars row) в формат строки для add_new_trend_level_0. direction считает C++."""
    return {
        'open_price': float(bar['open_price']),
        'close_price': float(bar['close_price']),
        'high_price': float(bar['high_price']),
        'low_price': float(bar['low_price']),
        'local_high_price': float(bar['high_price']),
        'local_low_price': float(bar['low_price']),
        'total_volume': float(bar['total_volume']),
        'buy_volume': float(bar['buy_volume']),
        'total_quantity': float(bar['total_quantity']),
        'buy_quantity': float(bar['buy_quantity']),
        'total_trades_count': int(bar['total_trades_count']),
        'buy_trades_count': int(bar['buy_trades_count']),
        'start_timestamp_ms': int(bar['start_timestamp_ms']),
        'end_timestamp_ms': int(bar['end_timestamp_ms']),
        'start_trade_id': int(bar['start_trade_id']),
        'end_trade_id': int(bar['end_trade_id']),
    }


def run_dow_pipeline(
    symbol_id: SymbolId,
    limit: int,
    level: int,
) -> dict[str, torch.Tensor] | None:
    """
    Загружает последние limit баров, прогоняет через калькулятор Доу с max_trend_levels=level,
    возвращает get_final_tensors(). Логирует каждую 1000-ю обработанную строку.
    """
    df = fetch_last_bars(symbol_id=symbol_id, limit=limit, offset=0)
    if df is None or df.height == 0:
        return None

    seq_len = settings.WEB_GUI_DOW_SEQUENCE_LENGTH
    state = IncrementalTrendState(max_trend_levels=level, sequence_length=seq_len)

    rows = df.to_dicts()
    for i, row in enumerate(rows):
        dow_row = _bar_row_to_dow_row(row)
        TrendCalculator.add_row_to_level_0_incremental(dow_row, state)
        if i % LOG_EVERY_N_ROWS == 0:
            logger.info('Processed row #%d', i)

    if rows:
        logger.info('Processed row #%d', len(rows))

    return state.get_final_tensors()


def _tensor_to_list(t: torch.Tensor) -> list[float]:
    if t is None or t.numel() == 0:
        return []
    return t.cpu().tolist()


def get_dow_bars_for_level(
    final_tensors: dict[str, torch.Tensor],
    level: int,
    symbol_id: SymbolId,
) -> list[dict] | None:
    """
    Извлекает из final_tensors (плоская структура с ключами open_price, close_price, ...)
    бары в формате API: start_timestamp_ms, open_price, high_price, low_price, close_price,
    total_volume, buy_volume_percent, sell_volume_percent, total_volume_log2.
    Параметр level не меняет набор ключей (C++ возвращает один набор по выбранному уровню или общий).
    """
    if 'open_price' not in final_tensors:
        return None
    level_data = final_tensors

    open_p = level_data['open_price']
    if open_p.numel() == 0:
        return []

    n = open_p.numel()
    start_timestamp_ms_tensor = level_data['start_timestamp_ms']
    high_p = level_data['high_price']
    low_p = level_data['low_price']
    close_p = level_data['close_price']
    total_vol = level_data['total_volume']
    buy_vol = level_data['buy_volume']

    start_timestamp_ms_list = _tensor_to_list(start_timestamp_ms_tensor)
    open_list = _tensor_to_list(open_p)
    high_list = _tensor_to_list(high_p)
    low_list = _tensor_to_list(low_p)
    close_list = _tensor_to_list(close_p)
    total_list = _tensor_to_list(total_vol)
    buy_list = _tensor_to_list(buy_vol)

    zero_volume_indices: list[int] = []
    zero_volume_timestamps_ms: list[int] = []
    timestamp_to_indices: dict[int, list[int]] = {}
    for i, ts_raw in enumerate(start_timestamp_ms_list):
        ts = int(ts_raw)
        if ts in timestamp_to_indices:
            timestamp_to_indices[ts].append(i)
        else:
            timestamp_to_indices[ts] = [i]

    duplicate_timestamp_indices: list[int] = []
    duplicate_timestamps_ms: list[int] = []
    for ts, indices in timestamp_to_indices.items():
        if len(indices) > 1:
            duplicate_timestamps_ms.append(ts)
            duplicate_timestamp_indices.extend(indices)

    bars = []
    for i in range(n):
        total = total_list[i]
        if total == 0:
            zero_volume_indices.append(i)
            zero_volume_timestamps_ms.append(int(start_timestamp_ms_list[i]))
            continue  # исключаем паддинг нулями

        buy = buy_list[i]
        buy_pct = (buy / total)
        sell_pct = 1.0 - buy_pct
        total_log2 = math.log2(total)

        bars.append({
            'start_timestamp_ms': start_timestamp_ms_list[i],
            'open_price': open_list[i],
            'high_price': high_list[i],
            'low_price': low_list[i],
            'close_price': close_list[i],
            'total_volume': total,
            'buy_volume_percent': buy_pct,
            'sell_volume_percent': sell_pct,
            'total_volume_log2': total_log2,
        })

    if duplicate_timestamps_ms:
        logger.warning(
            (
                'Dow diagnostics: duplicate start_timestamp_ms detected '
                '(symbol=%s, level=%d, duplicate_timestamps_count=%d, duplicate_indices_count=%d, '
                'duplicate_timestamps_ms=%s, duplicate_indices=%s)'
            ),
            symbol_id.name,
            level,
            len(duplicate_timestamps_ms),
            len(duplicate_timestamp_indices),
            duplicate_timestamps_ms,
            duplicate_timestamp_indices,
        )

    if zero_volume_indices:
        logger.warning(
            (
                'Dow diagnostics: total_volume == 0 bars filtered out '
                '(symbol=%s, level=%d, zero_volume_count=%d, zero_volume_indices=%s, '
                'zero_volume_timestamps_ms=%s)'
            ),
            symbol_id.name,
            level,
            len(zero_volume_indices),
            zero_volume_indices,
            zero_volume_timestamps_ms,
        )

    return bars


def get_dow_bars_for_api(
    symbol_id: SymbolId,
    limit: int,
    level: int,
) -> list[dict] | None:
    """
    Полный цикл: загрузка баров, прогон Доу, возврат баров для уровня level (1..5)
    в формате для API (как get_bars для масштабов).
    """
    if level < 1 or level > DOW_LEVELS_COUNT:
        return None

    final_tensors = run_dow_pipeline(symbol_id=symbol_id, limit=limit, level=level)
    if final_tensors is None:
        return None

    return get_dow_bars_for_level(
        final_tensors,
        level=level,
        symbol_id=symbol_id,
    )
