"""
REST API веб-GUI: символы, бары с пагинацией и масштабом.
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from enumerations import SymbolId
from main.web_gui.constants import CHART_SHOW_LIMIT, DOW_LEVEL_NAMES, SCALE_NAMES, chart_x1_fetch_limit
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
from main.web_gui.trade_journal_service import build_trade_journal_api_response
from main.offline_inference.artifacts import list_trade_research_horizons
from main.web_gui.trade_research_service import (
    DEFAULT_EVAL_HORIZON,
    eval_horizon_from_metadata,
)
from main.spawn_process import run_in_spawned_process_async
from main.web_gui.request_workers import (
    _worker_bars,
    _worker_dow,
    _worker_exit_policy,
    _worker_exit_transformer,
    _worker_trade_journal_bars_elapsed,
    _worker_trade_journal_discard,
    _worker_trade_journal_entry,
    _worker_trade_journal_exit,
    _worker_trade_research_from_artifact,
)
from main.web_gui.static_assets import (
    compute_static_asset_version,
    render_index_html,
    static_dir_path,
)
from main.web_gui.trading_daemon_runner import trading_daemon_loop
from settings import settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def _app_lifespan(_app: FastAPI):
    trading_task: asyncio.Task | None = None
    if settings.WEB_GUI_TRADING_ENABLED:
        trading_task = asyncio.create_task(trading_daemon_loop())
    yield
    if trading_task is not None:
        trading_task.cancel()
        try:
            await trading_task
        except asyncio.CancelledError:
            pass


app = FastAPI(title='OKX Data Set Web GUI', version='0.1.0', lifespan=_app_lifespan)

# Дефолтное число баров для GUI (хвост графика).
DEFAULT_BARS_LIMIT = settings.WEB_GUI_RECORDS_LIMIT
DEFAULT_CHART_SCALE = 'x32'


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
async def get_config() -> dict:
    """Параметры для фронта: лимит по умолчанию, интервал обновления (сек)."""
    metadata = await asyncio.to_thread(fetch_inference_metadata)
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
    exit_stack_by_symbol = (
        metadata['exit_stack_by_symbol']
        if 'exit_stack_by_symbol' in metadata
        else {}
    )
    entry_hint_mode_by_symbol = (
        metadata['entry_hint_mode_by_symbol']
        if 'entry_hint_mode_by_symbol' in metadata
        else {}
    )
    entry_confidence_margin_by_symbol = (
        metadata['entry_confidence_margin_by_symbol']
        if 'entry_confidence_margin_by_symbol' in metadata
        else {}
    )
    trade_research_eval_horizon = DEFAULT_EVAL_HORIZON
    default_scale = DEFAULT_CHART_SCALE
    try:
        trade_research_eval_horizon = eval_horizon_from_metadata(
            metadata,
            settings.INFERENCE_DAEMON_SYMBOL,
        )
        default_scale = trade_research_eval_horizon
    except RuntimeError:
        pass
    trade_research_available_horizons = list_trade_research_horizons(
        symbol_id=settings.INFERENCE_DAEMON_SYMBOL,
    )
    return {
        'defaultLimit': DEFAULT_BARS_LIMIT,
        'defaultScale': default_scale,
        'refreshIntervalSec': settings.WEB_GUI_REFRESH_INTERVAL_SEC,
        'assetVersion': compute_static_asset_version(static_dir_path()),
        'inferenceMinRows': inference_min_rows,
        'inferenceErrorBySymbolAndHorizon': metadata['error_by_symbol_and_horizon'],
        'policyBySymbol': policy_by_symbol,
        'exitPolicyBySymbol': exit_policy_by_symbol,
        'exitTransformerBySymbol': exit_transformer_by_symbol,
        'exitStackBySymbol': exit_stack_by_symbol,
        'entryHintModeBySymbol': entry_hint_mode_by_symbol,
        'entryConfidenceMarginBySymbol': entry_confidence_margin_by_symbol,
        'checkpointPathBySymbol': checkpoint_path_by_symbol,
        'chartShowLimit': CHART_SHOW_LIMIT,
        'tradeResearchEvalHorizon': trade_research_eval_horizon,
        'tradeResearchAvailableHorizons': trade_research_available_horizons,
        'tradeResearchLimit': settings.WEB_GUI_TRADE_RESEARCH_LIMIT,
        'tradeResearchPnlStride': settings.WEB_GUI_TRADE_RESEARCH_PNL_STRIDE,
        'exitGbmEnabled': settings.WEB_GUI_EXIT_GBM_ENABLED,
        'exitTransformerEnabled': settings.WEB_GUI_EXIT_TRANSFORMER_ENABLED,
        'tradingEnabled': settings.WEB_GUI_TRADING_ENABLED,
        'tradingLoopIntervalSec': settings.WEB_GUI_TRADING_LOOP_INTERVAL_SEC,
        'tradingInitialBalanceUsd': settings.WEB_GUI_TRADING_INITIAL_BALANCE_USD,
    }


@app.get('/api/asset-version')
def get_asset_version() -> dict[str, str]:
    return {
        'assetVersion': compute_static_asset_version(static_dir_path()),
    }


@app.get('/api/bars')
async def get_bars(
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
        SymbolId[symbol_id]
    except KeyError:
        raise HTTPException(422, detail=f'Unknown symbol_id: {symbol_id}')

    if scale not in SCALE_NAMES:
        raise HTTPException(422, detail=f'Unknown scale: {scale}')

    effective_limit = chart_x1_fetch_limit(
        scale=scale,
        offset=offset,
        requested_limit=limit,
        records_cap=settings.WEB_GUI_RECORDS_LIMIT,
    )
    started = time.monotonic()
    logger.info(
        'GET /api/bars start symbol=%s scale=%s limit=%d offset=%d effective_limit=%d',
        symbol_id,
        scale,
        limit if limit is not None else -1,
        offset,
        effective_limit,
    )
    bars = await run_in_spawned_process_async(
        _worker_bars,
        symbol_id,
        effective_limit,
        offset,
        scale,
        pool_kind='heavy',
    )
    duration_ms = int((time.monotonic() - started) * 1000)
    if bars is None:
        logger.info(
            'GET /api/bars done symbol=%s scale=%s count=0 duration_ms=%d',
            symbol_id,
            scale,
            duration_ms,
        )
        return {'bars': [], 'count': 0}
    logger.info(
        'GET /api/bars done symbol=%s scale=%s count=%d duration_ms=%d',
        symbol_id,
        scale,
        len(bars),
        duration_ms,
    )
    return {'bars': bars, 'count': len(bars)}


@app.get('/api/dow')
async def get_dow(
    symbol_id: str = Query(..., description='SymbolId, e.g. BTC_USDT'),
    limit: int | None = Query(None, ge=1),
    level: int = Query(..., ge=1, le=10, description='Уровень теории Доу 1..10'),
) -> dict:
    """
    Бары по теории Доу для выбранного уровня: OHLCV из тензоров после прогона баров через калькулятор.
    """
    try:
        SymbolId[symbol_id]
    except KeyError:
        raise HTTPException(422, detail=f'Unknown symbol_id: {symbol_id}')

    effective_limit = chart_x1_fetch_limit(
        scale='x1',
        offset=0,
        requested_limit=limit,
        records_cap=settings.WEB_GUI_RECORDS_LIMIT,
    )
    bars = await run_in_spawned_process_async(
        _worker_dow,
        symbol_id,
        effective_limit,
        level,
        pool_kind='heavy',
    )
    if bars is None:
        raise HTTPException(503, detail='Dow theory aggregator not available or failed')
    return {'bars': bars, 'count': len(bars)}


@app.get('/api/inference')
async def get_inference(
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
    return await asyncio.to_thread(get_inference_artifact, symbol_id=symbol_id)


@app.get('/api/trade-research')
async def get_trade_research(
    symbol_id: str = Query(..., description='SymbolId, e.g. BTC_USDT'),
    eval_horizon: str = Query(DEFAULT_EVAL_HORIZON, description='Eval horizon, e.g. x1536'),
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

    started = time.monotonic()
    logger.info(
        'GET /api/trade-research start symbol=%s eval_horizon=%s step_bars=%d '
        'visible_min=%s visible_max=%s',
        symbol_id,
        eval_horizon,
        effective_step_bars,
        visible_min_start_trade_id,
        visible_max_start_trade_id,
    )
    try:
        payload = await run_in_spawned_process_async(
            _worker_trade_research_from_artifact,
            symbol_id,
            eval_horizon,
            effective_step_bars,
            visible_min_start_trade_id,
            visible_max_start_trade_id,
            pool_kind='heavy',
        )
    except RuntimeError as exception:
        duration_ms = int((time.monotonic() - started) * 1000)
        logger.error(
            'GET /api/trade-research failed symbol=%s duration_ms=%d error=%s',
            symbol_id,
            duration_ms,
            exception,
        )
        raise HTTPException(status_code=503, detail=str(exception)) from exception

    duration_ms = int((time.monotonic() - started) * 1000)
    segment_count = payload['segment_count'] if 'segment_count' in payload else '?'
    logger.info(
        'GET /api/trade-research done symbol=%s segments=%s duration_ms=%d',
        symbol_id,
        segment_count,
        duration_ms,
    )
    return payload


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
    exit_stack_mode: str | None = None
    exit_stack_eval_horizon: str | None = None
    exit_stack_min_hold_steps: int | None = None


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
    entry_predictions: dict[str, float] | None = None
    current_predictions: dict[str, float] | None = None
    entry_policy: dict | None = None
    current_policy: dict | None = None
    unrealized_linear: float | None = None
    mfe_linear: float | None = None
    mae_linear: float | None = None
    giveback_linear: float | None = None
    exit_stack_mode: str | None = None
    last_renew_segment_evaluated: int | None = None


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
async def post_exit_policy(body: ExitPolicyRequest) -> dict:
    if settings.WEB_GUI_TRADING_ENABLED:
        raise HTTPException(status_code=403, detail='Exit policy is daemon-only')

    try:
        SymbolId[body.symbol_id]
    except KeyError:
        raise HTTPException(422, detail=f'Unknown symbol_id: {body.symbol_id}')

    use_exit_stack = body.exit_stack_mode is not None
    if not settings.WEB_GUI_EXIT_GBM_ENABLED and not use_exit_stack:
        return build_exit_policy_disabled_response()

    payload = body.model_dump(exclude_none=True)
    return await run_in_spawned_process_async(
        _worker_exit_policy,
        payload,
        pool_kind='heavy',
    )


@app.post('/api/exit-transformer')
async def post_exit_transformer(body: ExitTransformerRequest) -> dict:
    try:
        SymbolId[body.symbol_id]
    except KeyError:
        raise HTTPException(422, detail=f'Unknown symbol_id: {body.symbol_id}')

    if not settings.WEB_GUI_EXIT_TRANSFORMER_ENABLED:
        return build_exit_transformer_disabled_response()

    return await run_in_spawned_process_async(
        _worker_exit_transformer,
        body.model_dump(),
        pool_kind='heavy',
    )


@app.get('/api/trade-journal')
def get_trade_journal(
    symbol_id: str = Query(..., description='SymbolId, e.g. BTC_USDT'),
    mark_price: float | None = Query(None, gt=0, description='Latest x1 close for unrealized PnL'),
    bars_elapsed: int | None = Query(
        None,
        ge=0,
        description='Cached bars elapsed from client (DB count via /bars-elapsed)',
    ),
    last_renew_segment_evaluated: int | None = Query(
        None,
        ge=-1,
        description='Renew segment cursor from latest exit_policy (max with journal)',
    ),
) -> dict:
    try:
        SymbolId[symbol_id]
    except KeyError:
        raise HTTPException(422, detail=f'Unknown symbol_id: {symbol_id}')

    return build_trade_journal_api_response(
        symbol_id_str=symbol_id,
        mark_price=mark_price,
        bars_elapsed=bars_elapsed,
        persist_mark_price=False,
        client_last_renew_segment_evaluated=last_renew_segment_evaluated,
    )


@app.get('/api/trade-journal/bars-elapsed')
async def get_trade_journal_bars_elapsed(
    symbol_id: str = Query(..., description='SymbolId, e.g. BTC_USDT'),
    entry_start_trade_id: int = Query(..., ge=0, description='Entry bar start_trade_id'),
) -> dict:
    try:
        SymbolId[symbol_id]
    except KeyError:
        raise HTTPException(422, detail=f'Unknown symbol_id: {symbol_id}')

    bars_elapsed = await run_in_spawned_process_async(
        _worker_trade_journal_bars_elapsed,
        symbol_id,
        entry_start_trade_id,
        pool_kind='light',
    )
    return {'bars_elapsed': bars_elapsed}


@app.post('/api/trade-journal/entry')
async def post_trade_journal_entry(body: TradeJournalEntryRequest) -> dict:
    if settings.WEB_GUI_TRADING_ENABLED:
        raise HTTPException(status_code=403, detail='Trade journal is daemon-only')

    try:
        SymbolId[body.symbol_id]
    except KeyError:
        raise HTTPException(422, detail=f'Unknown symbol_id: {body.symbol_id}')

    try:
        return await run_in_spawned_process_async(
            _worker_trade_journal_entry,
            body.model_dump(),
            pool_kind='journal',
        )
    except ValueError as exception:
        raise HTTPException(409, detail=str(exception))


@app.post('/api/trade-journal/exit')
async def post_trade_journal_exit(body: TradeJournalExitRequest) -> dict:
    if settings.WEB_GUI_TRADING_ENABLED:
        raise HTTPException(status_code=403, detail='Trade journal is daemon-only')

    try:
        return await run_in_spawned_process_async(
            _worker_trade_journal_exit,
            body.model_dump(),
            pool_kind='journal',
        )
    except ValueError as exception:
        raise HTTPException(409, detail=str(exception))


@app.delete('/api/trade-journal/open')
async def delete_trade_journal_open() -> dict:
    if settings.WEB_GUI_TRADING_ENABLED:
        raise HTTPException(status_code=403, detail='Trade journal is daemon-only')

    return await run_in_spawned_process_async(
        _worker_trade_journal_discard,
        pool_kind='journal',
    )


def mount_static(static_dir: str) -> None:
    """Монтирует папку со статикой (HTML, JS, CSS)."""
    if os.path.isdir(static_dir):
        app.mount('/static', StaticFiles(directory=static_dir), name='static')


# Монтируем static при загрузке модуля (работает и при запуске через uvicorn main.web_gui.api:app)
_static_dir = static_dir_path()
mount_static(_static_dir)

_INDEX_NO_CACHE_HEADERS = {
    'Cache-Control': 'no-store, no-cache, must-revalidate',
    'Pragma': 'no-cache',
}


@app.get('/', response_class=HTMLResponse)
def index() -> HTMLResponse:
    """Главная страница — index.html с подставленной версией статики."""
    return HTMLResponse(
        content=render_index_html(_static_dir),
        headers=_INDEX_NO_CACHE_HEADERS,
    )
