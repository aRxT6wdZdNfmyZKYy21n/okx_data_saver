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


DOW_LEVELS_COUNT = 5
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
    base_timestamp_ms: int,
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
    high_p = level_data.get('high_price')
    low_p = level_data.get('low_price')
    close_p = level_data.get('close_price')
    total_vol = level_data.get('total_volume')
    buy_vol = level_data.get('buy_volume')

    open_list = _tensor_to_list(open_p)
    high_list = _tensor_to_list(high_p) if high_p is not None else open_list
    low_list = _tensor_to_list(low_p) if low_p is not None else open_list
    close_list = _tensor_to_list(close_p) if close_p is not None else open_list
    total_list = _tensor_to_list(total_vol) if total_vol is not None else [0.0] * n
    buy_list = _tensor_to_list(buy_vol) if buy_vol is not None else [0.0] * n

    bars = []
    for i in range(n):
        total = total_list[i] if i < len(total_list) else 0.0
        if total == 0:
            continue  # исключаем паддинг нулями
        buy = buy_list[i] if i < len(buy_list) else 0.0
        buy_pct = (buy / total) if total > 0 else 0.0
        sell_pct = 1.0 - buy_pct
        total_log2 = math.log2(total) if total > 0 else 0.0

        bars.append({
            'start_timestamp_ms': base_timestamp_ms + i * 60_000,
            'open_price': open_list[i] if i < len(open_list) else None,
            'high_price': high_list[i] if i < len(high_list) else None,
            'low_price': low_list[i] if i < len(low_list) else None,
            'close_price': close_list[i] if i < len(close_list) else None,
            'total_volume': total,
            'buy_volume_percent': buy_pct,
            'sell_volume_percent': sell_pct,
            'total_volume_log2': total_log2,
        })

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

    df = fetch_last_bars(symbol_id=symbol_id, limit=1, offset=0)
    base_ts = int(df['start_timestamp_ms'][0]) if df is not None and df.height > 0 else 0

    return get_dow_bars_for_level(final_tensors, base_timestamp_ms=base_ts)
