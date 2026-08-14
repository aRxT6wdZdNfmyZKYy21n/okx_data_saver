"""
Async background loop for automated paper trading inside web_gui.
"""

import asyncio
import logging

from main.web_gui.trading_daemon import run_trading_tick_safe
from settings import settings

logger = logging.getLogger(__name__)


async def trading_daemon_loop() -> None:
    if not settings.WEB_GUI_TRADING_ENABLED:
        logger.info('WEB_GUI_TRADING_ENABLED=false; trading daemon not started')
        return
    symbol_id = settings.INFERENCE_DAEMON_SYMBOL
    interval_sec = settings.WEB_GUI_TRADING_LOOP_INTERVAL_SEC
    logger.info(
        'Starting trading daemon loop symbol=%s interval_sec=%d',
        symbol_id,
        interval_sec,
    )
    while True:
        await asyncio.to_thread(run_trading_tick_safe, symbol_id)
        await asyncio.sleep(interval_sec)
