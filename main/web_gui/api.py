"""
REST API веб-GUI: символы, бары с пагинацией и масштабом.
"""

import logging
import os

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from enumerations import SymbolId
from main.web_gui.constants import DOW_LEVEL_NAMES, SCALE_NAMES
from main.web_gui.request_workers import (
    _worker_bars,
    _worker_dow,
    run_in_spawned_process,
)
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
    """Уровни теории Доу (1..10)."""
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
    bars = run_in_spawned_process(
        _worker_bars, symbol_id, effective_limit, offset, scale,
    )
    if bars is None:
        return {'bars': [], 'count': 0}
    return {'bars': bars, 'count': len(bars)}


@app.get('/api/dow')
def get_dow(
    symbol_id: str = Query(..., description='SymbolId, e.g. BTC_USDT'),
    limit: int | None = Query(None, ge=1),
    level: int = Query(..., ge=1, le=10, description='Уровень теории Доу 1..10'),
) -> dict:
    """
    Бары по теории Доу для выбранного уровня: OHLCV из тензоров после прогона баров через калькулятор.
    """
    try:
        symbol = SymbolId[symbol_id]
    except KeyError:
        raise HTTPException(422, detail=f'Unknown symbol_id: {symbol_id}')

    effective_limit = min(limit or settings.WEB_GUI_RECORDS_LIMIT, settings.WEB_GUI_RECORDS_LIMIT)
    bars = run_in_spawned_process(_worker_dow, symbol_id, effective_limit, level)
    if bars is None:
        raise HTTPException(503, detail='Dow theory aggregator not available or failed')
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
