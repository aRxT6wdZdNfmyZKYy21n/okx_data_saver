"""
REST API веб-GUI: символы, бары с пагинацией и масштабом.
"""

import logging
import math
import os

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from enumerations import SymbolId
from main.web_gui.constants import DOW_LEVEL_NAMES, SCALE_NAMES
from main.web_gui.data_service import get_bars_for_api
from main.web_gui.dow_service import get_dow_bars_for_api
from settings import settings

logger = logging.getLogger(__name__)

app = FastAPI(title='OKX Data Set Web GUI', version='0.1.0')


@app.get('/api/symbols')
def list_symbols() -> list[dict]:
    """Список символов из enum SymbolId."""
    return [{'id': s.name, 'name': s.name} for s in SymbolId]


@app.get('/api/scales')
def list_scales() -> list[str]:
    """Доступные масштабы агрегации."""
    return list(SCALE_NAMES)


@app.get('/api/dow_levels')
def list_dow_levels() -> list[str]:
    """Уровни теории Доу (1..5)."""
    return list(DOW_LEVEL_NAMES)


@app.get('/api/config')
def get_config() -> dict:
    """Параметры для фронта: лимит по умолчанию, интервал обновления (сек)."""
    return {
        'defaultLimit': settings.WEB_GUI_RECORDS_LIMIT,
        'refreshIntervalSec': settings.WEB_GUI_REFRESH_INTERVAL_SEC,
    }


@app.get('/api/bars')
def get_bars(
    symbol_id: str = Query(..., description='SymbolId, e.g. BTC_USDT'),
    limit: int | None = Query(None, ge=1, description='Max bars to return (default from config)'),
    offset: int = Query(0, ge=0),
    scale: str = Query('x1', description='Scale: x1, x2, x4, ... x2048'),
) -> dict:
    """
    Последние бары для символа. Пагинация: offset (пропуск от конца), limit.
    scale — агрегация на бэкенде (x1 = без агрегации).
    """
    try:
        symbol = SymbolId[symbol_id]
    except KeyError:
        raise HTTPException(422, detail=f'Unknown symbol_id: {symbol_id}')

    if scale not in SCALE_NAMES:
        raise HTTPException(422, detail=f'Unknown scale: {scale}')

    effective_limit = min(limit or settings.WEB_GUI_RECORDS_LIMIT, settings.WEB_GUI_RECORDS_LIMIT)
    df = get_bars_for_api(symbol_id=symbol, limit=effective_limit, offset=offset, scale=scale)
    if df is None:
        return {'bars': [], 'count': 0}

    # Сериализация: только нужные поля для графика и объёмов
    cols = [
        'start_trade_id', 'end_trade_id',
        'start_timestamp_ms', 'end_timestamp_ms',
        'open_price', 'high_price', 'low_price', 'close_price',
        'total_volume', 'buy_volume_percent', 'sell_volume_percent', 'total_volume_log2',
    ]
    available = [c for c in cols if c in df.columns]
    rows = df.select(available).to_dicts()

    bars = [_serialize_bar_row(r) for r in rows]
    return {'bars': bars, 'count': len(bars)}


def _serialize_bar_row(r: dict) -> dict:
    out = {}
    for k, v in r.items():
        if v is None or (isinstance(v, float) and math.isnan(v)):
            out[k] = None
        else:
            out[k] = v
    return out


@app.get('/api/dow')
def get_dow(
    symbol_id: str = Query(..., description='SymbolId, e.g. BTC_USDT'),
    limit: int | None = Query(None, ge=1),
    level: int = Query(..., ge=1, le=5, description='Уровень теории Доу 1..5'),
) -> dict:
    """
    Бары по теории Доу для выбранного уровня: OHLCV из тензоров после прогона баров через калькулятор.
    """
    try:
        symbol = SymbolId[symbol_id]
    except KeyError:
        raise HTTPException(422, detail=f'Unknown symbol_id: {symbol_id}')

    effective_limit = min(limit or settings.WEB_GUI_RECORDS_LIMIT, settings.WEB_GUI_RECORDS_LIMIT)
    dow_bars = get_dow_bars_for_api(symbol_id=symbol, limit=effective_limit, level=level)
    if dow_bars is None:
        raise HTTPException(503, detail='Dow theory aggregator not available or failed')
    bars = [_serialize_bar_row(r) for r in dow_bars]
    return {'bars': bars, 'count': len(bars)}


def mount_static(static_dir: str) -> None:
    """Монтирует папку со статикой (HTML, JS, CSS)."""
    if os.path.isdir(static_dir):
        app.mount('/static', StaticFiles(directory=static_dir), name='static')


# Монтируем static при загрузке модуля (работает и при запуске через uvicorn main.web_gui.api:app)
_static_dir = os.path.join(os.path.dirname(__file__), 'static')
mount_static(_static_dir)


@app.get('/', response_class=HTMLResponse)
def index() -> str:
    """Главная страница — отдаём index.html из static."""
    static_dir = os.path.join(os.path.dirname(__file__), 'static')
    index_path = os.path.join(static_dir, 'index.html')
    if os.path.isfile(index_path):
        with open(index_path, encoding='utf-8') as f:
            return f.read()
    return '<html><body><p>OKX Data Set Web GUI. Place index.html in main/web_gui/static/</p></body></html>'
