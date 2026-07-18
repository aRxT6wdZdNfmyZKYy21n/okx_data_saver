import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from utils.redis import g_redis_manager

_redis_connected = False

T = TypeVar('T')


async def ensure_redis_connected() -> None:
    global _redis_connected
    if _redis_connected:
        return
    await g_redis_manager.connect()
    _redis_connected = True


async def _run_with_redis_lifecycle(
    coroutine_factory: Callable[[], Awaitable[T]],
) -> T:
    global _redis_connected
    try:
        await ensure_redis_connected()
        return await coroutine_factory()
    finally:
        if _redis_connected:
            await g_redis_manager.disconnect()
            _redis_connected = False


def run_async_data(
    coroutine_factory: Callable[[], Awaitable[T]],
) -> T:
    return asyncio.run(_run_with_redis_lifecycle(coroutine_factory))
