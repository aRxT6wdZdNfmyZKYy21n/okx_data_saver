from pydantic import (
    SecretStr,
)
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)


class Settings(BaseSettings):
    """Application configuration."""

    POSTGRES_DB_HOST_NAME: str
    POSTGRES_DB_NAME: str
    POSTGRES_DB_PORT: int
    POSTGRES_DB_PASSWORD: SecretStr
    POSTGRES_DB_USER_NAME: str

    # Redis configuration
    REDIS_HOST: str = 'localhost'
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: SecretStr | None = None
    REDIS_DB: int = 0

    # Web GUI (okx_data_set_record_data_2 viewer)
    WEB_GUI_RECORDS_LIMIT: int = 1_000_000
    WEB_GUI_TRADE_RESEARCH_LIMIT: int = 10_000_000
    # Sample stride for sequential hybrid backtest PnL (1 = exact, 128 ≈ fast)
    WEB_GUI_TRADE_RESEARCH_PNL_STRIDE: int = 128
    WEB_GUI_REFRESH_INTERVAL_SEC: int = 30
    WEB_GUI_DOW_SEQUENCE_LENGTH: int = 32
    WEB_GUI_INFERENCE_ENABLED: bool = True
    WEB_GUI_INFERENCE_API_BASE_URL: str = 'http://127.0.0.1:8010'
    # Exit GBM overlay in micro-live journal (037i: L27 exit hurts L37 stack @ prod threshold)
    WEB_GUI_EXIT_GBM_ENABLED: bool = False
    # Exit Transformer v2 overlay (043: delta_pnl vs fixed-H)
    WEB_GUI_EXIT_TRANSFORMER_ENABLED: bool = False

    # Offline inference daemon (writes data/inference/{symbol}/latest_inference.json)
    INFERENCE_DAEMON_SYMBOL: str = 'BTC_USDT'
    INFERENCE_DAEMON_INTERVAL_SEC: int = 60
    INFERENCE_DAEMON_BARS_LIMIT: int = 10_000_000

    # Path to trading_bot repo for trade research NPZ post-processing (policy/gate)
    TRADING_BOT_ROOT: str = '/home/debian/Repositories/trading_bot'

    # Polars thread pool (leave headroom on 16-core host)
    POLARS_MAX_THREADS: int = 14

    # Redis-backed x1 bars cache for Web GUI / offline inference
    WEB_GUI_BARS_REDIS_CACHE_ENABLED: bool = True
    BARS_REDIS_CACHE_TTL_SEC: int = 60
    BARS_REDIS_REFRESH_LOCK_TTL_SEC: int = 600
    BARS_REDIS_LOCK_WAIT_SEC: int = 300
    BARS_REDIS_LOCK_POLL_INTERVAL_SEC: float = 0.5

    model_config = SettingsConfigDict(
        env_file='.env',
        extra='forbid',
    )


settings = Settings()  # noqa
