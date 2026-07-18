"""
REST API веб-GUI: символы, бары с пагинацией и масштабом.
"""

import logging
import os

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from enumerations import SymbolId
from main.web_gui.constants import CHART_SHOW_LIMIT, DOW_LEVEL_NAMES, SCALE_NAMES
from main.web_gui.exit_policy_service import (
    build_exit_policy_disabled_response,
    run_remote_exit_policy,
)
from main.web_gui.exit_transformer_service import (
    build_exit_transformer_disabled_response,
    run_remote_exit_transformer,
)
from main.web_gui.inference_artifact_service import get_inference_artifact
from main.web_gui.inference_service import (
    fetch_inference_metadata,
)
from main.spawn_process import run_in_spawned_process
from main.web_gui.request_workers import (
    _worker_bars,
    _worker_dow,
    _worker_trade_journal_discard,
    _worker_trade_journal_entry,
    _worker_trade_journal_exit,
    _worker_trade_journal_state,
    _worker_trade_research_from_artifact,
)
from settings import settings

logger = logging.getLogger(__name__)

app = FastAPI(title='OKX Data Set Web GUI', version='0.1.0')

# Дефолтное число баров для обычных свечей:
# минимум между глобальным лимитом и 1000 (остальное пользователь указывает явно).
DEFAULT_BARS_LIMIT = min(settings.WEB_GUI_RECORDS_LIMIT, 1000)


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
    metadata = fetch_inference_metadata()
    inference_min_rows = int(metadata['sequence_length']) * int(metadata['max_scale'])
    policy_by_symbol = metadata['policy_by_symbol'] if 'policy_by_symbol' in metadata else {}
    checkpoint_path_by_symbol = (
        metadata['checkpoint_path_by_symbol']
        if 'checkpoint_path_by_symbol' in metadata
        else {}
    )
    exit_policy_by_symbol = (
        metadata['exit_policy_by_symbol']
        if 'exit_policy_by_symbol' in metadata
        else {}
    )
    exit_transformer_by_symbol = (
        metadata['exit_transformer_by_symbol']
        if 'exit_transformer_by_symbol' in metadata
        else {}
    )
    return {
        'defaultLimit': DEFAULT_BARS_LIMIT,
        'refreshIntervalSec': settings.WEB_GUI_REFRESH_INTERVAL_SEC,
        'inferenceMinRows': inference_min_rows,
        'inferenceErrorBySymbolAndHorizon': metadata['error_by_symbol_and_horizon'],
        'policyBySymbol': policy_by_symbol,
        'exitPolicyBySymbol': exit_policy_by_symbol,
        'exitTransformerBySymbol': exit_transformer_by_symbol,
        'checkpointPathBySymbol': checkpoint_path_by_symbol,
        'chartShowLimit': CHART_SHOW_LIMIT,
        'tradeResearchLimit': settings.WEB_GUI_TRADE_RESEARCH_LIMIT,
        'tradeResearchPnlStride': settings.WEB_GUI_TRADE_RESEARCH_PNL_STRIDE,
        'exitGbmEnabled': settings.WEB_GUI_EXIT_GBM_ENABLED,
        'exitTransformerEnabled': settings.WEB_GUI_EXIT_TRANSFORMER_ENABLED,
    }


@app.get('/api/bars')
def get_bars(
    symbol_id: str = Query(..., description='SymbolId, e.g. BTC_USDT'),
    limit: int | None = Query(None, ge=1, description='Max bars to return (default from config)'),
    offset: int = Query(0, ge=0),
    scale: str = Query('x4096', description='Scale: x1, x2, x4, ... x262144'),
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

    effective_limit = min(limit or DEFAULT_BARS_LIMIT, settings.WEB_GUI_RECORDS_LIMIT)
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


@app.get('/api/inference')
def get_inference(
    symbol_id: str = Query(..., description='SymbolId, e.g. BTC_USDT'),
    limit: int | None = Query(
        None,
        ge=1,
        description='Deprecated: inference is read from offline artifact',
    ),
) -> dict:
    try:
        SymbolId[symbol_id]
    except KeyError:
        raise HTTPException(422, detail=f'Unknown symbol_id: {symbol_id}')

    del limit
    return get_inference_artifact(symbol_id=symbol_id)


@app.get('/api/trade-research')
def get_trade_research(
    symbol_id: str = Query(..., description='SymbolId, e.g. BTC_USDT'),
    eval_horizon: str = Query('x2048', description='Eval horizon, e.g. x2048'),
    step_bars: int | None = Query(None, ge=1, description='Non-overlapping step in x1 bars'),
    visible_min_start_trade_id: int | None = Query(
        None,
        ge=0,
        description='First start_trade_id on chart (inclusive filter for returned segments)',
    ),
    visible_max_start_trade_id: int | None = Query(
        None,
        ge=0,
        description='Last start_trade_id on chart (inclusive filter for returned segments)',
    ),
) -> dict:
    try:
        SymbolId[symbol_id]
    except KeyError:
        raise HTTPException(422, detail=f'Unknown symbol_id: {symbol_id}')

    if step_bars is None:
        if not eval_horizon.startswith('x'):
            raise HTTPException(422, detail=f'Invalid eval_horizon: {eval_horizon}')
        effective_step_bars = int(eval_horizon[1:])
    else:
        effective_step_bars = step_bars

    return run_in_spawned_process(
        _worker_trade_research_from_artifact,
        symbol_id,
        eval_horizon,
        effective_step_bars,
        visible_min_start_trade_id,
        visible_max_start_trade_id,
    )


class TradeJournalEntryRequest(BaseModel):
    symbol_id: str
    side: str
    entry_price: float = Field(..., gt=0)
    entry_start_trade_id: int = Field(..., ge=0)
    entry_timestamp_ms: int = Field(..., ge=0)
    eval_horizon: str
    notional_usd: float = Field(..., gt=0)
    policy_action: str | None = None
    notes: str = ''
    entry_policy: dict | None = None
    entry_predictions: dict[str, float] | None = None


class TradeJournalExitRequest(BaseModel):
    exit_price: float = Field(..., gt=0)
    exit_start_trade_id: int = Field(..., ge=0)
    exit_timestamp_ms: int = Field(..., ge=0)
    notes: str = ''
    exit_overlay: dict | None = None


class ExitPolicyRequest(BaseModel):
    symbol_id: str
    side: str
    eval_horizon: str
    bars_held: int = Field(..., ge=0)
    entry_predictions: dict[str, float]
    current_predictions: dict[str, float]
    entry_policy: dict
    current_policy: dict
    unrealized_linear: float
    mfe_linear: float
    mae_linear: float
    giveback_linear: float


class ExitTransformerRequest(BaseModel):
    symbol_id: str
    side: str
    eval_horizon: str
    bars_held: int = Field(..., ge=0)
    bars_limit: int = Field(..., ge=1)
    entry_predictions: dict[str, float]
    current_predictions: dict[str, float]
    unrealized_linear: float
    mfe_linear: float
    mae_linear: float
    giveback_linear: float


@app.post('/api/exit-policy')
def post_exit_policy(body: ExitPolicyRequest) -> dict:
    try:
        SymbolId[body.symbol_id]
    except KeyError:
        raise HTTPException(422, detail=f'Unknown symbol_id: {body.symbol_id}')

    if not settings.WEB_GUI_EXIT_GBM_ENABLED:
        return build_exit_policy_disabled_response()

    return run_in_spawned_process(
        _worker_exit_policy,
        body.model_dump(),
    )


@app.post('/api/exit-transformer')
def post_exit_transformer(body: ExitTransformerRequest) -> dict:
    try:
        SymbolId[body.symbol_id]
    except KeyError:
        raise HTTPException(422, detail=f'Unknown symbol_id: {body.symbol_id}')

    if not settings.WEB_GUI_EXIT_TRANSFORMER_ENABLED:
        return build_exit_transformer_disabled_response()

    return run_in_spawned_process(
        _worker_exit_transformer,
        body.model_dump(),
    )


@app.get('/api/trade-journal')
def get_trade_journal(
    symbol_id: str = Query(..., description='SymbolId, e.g. BTC_USDT'),
    mark_price: float | None = Query(None, gt=0, description='Latest x1 close for unrealized PnL'),
) -> dict:
    try:
        SymbolId[symbol_id]
    except KeyError:
        raise HTTPException(422, detail=f'Unknown symbol_id: {symbol_id}')

    return run_in_spawned_process(_worker_trade_journal_state, symbol_id, mark_price)


@app.post('/api/trade-journal/entry')
def post_trade_journal_entry(body: TradeJournalEntryRequest) -> dict:
    try:
        SymbolId[body.symbol_id]
    except KeyError:
        raise HTTPException(422, detail=f'Unknown symbol_id: {body.symbol_id}')

    try:
        position = run_in_spawned_process(
            _worker_trade_journal_entry,
            body.model_dump(),
        )
    except ValueError as exception:
        raise HTTPException(409, detail=str(exception))
    return {'open_position': position}


@app.post('/api/trade-journal/exit')
def post_trade_journal_exit(body: TradeJournalExitRequest) -> dict:
    try:
        closed_trade = run_in_spawned_process(
            _worker_trade_journal_exit,
            body.model_dump(),
        )
    except ValueError as exception:
        raise HTTPException(409, detail=str(exception))
    return {'closed_trade': closed_trade}


@app.delete('/api/trade-journal/open')
def delete_trade_journal_open() -> dict:
    run_in_spawned_process(_worker_trade_journal_discard)
    return {'ok': True}


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
