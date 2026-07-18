import os
import sys


def ensure_trading_bot_on_path() -> str:
    from settings import settings

    trading_bot_root = os.path.abspath(settings.TRADING_BOT_ROOT)
    if trading_bot_root not in sys.path:
        sys.path.insert(0, trading_bot_root)
    return trading_bot_root
