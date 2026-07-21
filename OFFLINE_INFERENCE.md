# Offline inference

Web GUI no longer calls `inference_api` on each refresh. Predictions are written to files under `data/` by background/offline scripts; the GUI reads those artifacts.

## Artifacts

| Path | Writer | Reader |
|------|--------|--------|
| `data/inference/BTC_USDT/latest_inference.json` | `main.inference_daemon` | `GET /api/inference` |
| `data/inference/BTC_USDT/last_inference_ok.json` | daemon (on each ok) | API enriches `computing` responses |
| `data/trade_research/BTC_USDT/predictions.npz` | `main.trade_research_export` | `GET /api/trade-research` |
| `data/trade_research/BTC_USDT/meta.json` | export script | trade research loader / UI status |

### `latest_inference.json`

- `status`: `computing` | `ok` | `error`
- On `computing`, previous `ok` snapshot (`predictions`, `policy`, …) is preserved; UI shows age + refresh in progress
- `inference_completed_at_ms` — timestamp of last successful inference
- `computing_started_at_ms` — present while `status=computing`
- On `error`, inference fields are cleared (only `error_message` + metadata)
- On `ok`: `predictions`, `policy`, `entry_hint`, optional `exit_policy` / `exit_transformer` when journal has an open BTC position

### Trade research NPZ (variant A)

- `sample_index[]`, `pred_x*[]`, `target_x*[]`, bar metadata columns, dataset metadata scalars
- Incremental: re-run export merges new samples; full rebuild if checkpoint/run_label changes

## Services

```bash
# Terminal 1 — inference API (trading_bot)
cd ~/Repositories/trading_bot
source .venv/bin/activate
python3 -m inference_api

# Terminal 2 — offline inference daemon (okx_data_saver)
cd ~/Repositories/okx_data_saver
source .venv/bin/activate
python3 -m main.inference_daemon -v

# Terminal 3 — Web GUI
python3 -m main.web_gui -v

# Manual trade research export (incremental; Polars work runs in spawn subprocess)
python3 -m main.trade_research_export --symbol BTC_USDT -v
```

## Environment

See `.env.example`:

- `TRADING_BOT_ROOT` — for NPZ loader policy/gate post-processing (one-way import, no cycle)
- `INFERENCE_DAEMON_*` — symbol, interval (default 60s), bars limit (default 10M)
- `WEB_GUI_INFERENCE_API_BASE_URL` — still used by daemon/export to call `inference_api`
- `WEB_GUI_TRADE_JOURNAL_PATH` — micro-live journal JSON (default `data/trade_journal.json`; set per web_gui instance)
- `WEB_GUI_TRADE_JOURNAL_DEFAULT_EVAL_HORIZON` — journal UI default horizon (`x2048`, `x1536`, …)
- `POLARS_MAX_THREADS` — Polars thread pool cap (default 14)
- `WEB_GUI_BARS_REDIS_CACHE_ENABLED`, `BARS_REDIS_*` — x1 bars Redis cache and refresh lock

## Memory / Polars

Heavy Polars loads (inference daemon cycle, trade research export, Web GUI bars/trade-research) run in **spawn subprocesses** via `main.spawn_process.run_in_spawned_process`. Memory is released when the child exits.

Polars thread pool is capped via `POLARS_MAX_THREADS` (default **14** on a 16-core host). Applied in process entry points and spawn workers via `main.runtime_limits.apply_runtime_limits()`.

Use `-v` / `--verbose` on `main.inference_daemon`, `main.trade_research_export`, and `main.web_gui` for INFO logs: DB reads, dataset preparation, inference batch progress.

## Redis x1 bars cache

Raw x1 Polars DataFrames (not HybridTradeDataset tensors) are cached in Redis with LZ4 IPC serialization (`utils.redis.save_dataframe` / `load_dataframe`).

| Key pattern | Purpose |
|-------------|---------|
| `web_gui:x1_bars:{symbol}:limit:{N}:offset:{O}` | Cached bars DataFrame (chunked) |
| `web_gui:x1_bars:{symbol}:limit:{N}:offset:{O}:bars_meta` | JSON metadata (`last_start_trade_id`, `updated_at_ms`, …) |
| `web_gui:x1_bars:refresh_lock` | Global exclusive lock — only one DB refresh at a time |

Waiters poll the cache until hit or acquire the lock. Toggle with `WEB_GUI_BARS_REDIS_CACHE_ENABLED` (default `true`). Default TTL is **60 seconds** (`BARS_REDIS_CACHE_TTL_SEC`), aligned with `INFERENCE_DAEMON_INTERVAL_SEC`. Redis `maxmemory` should be sized for ~10M-bar frames (24 GB is sufficient).

Sync callers (spawn workers, legacy services) use `fetch_last_bars_sync` / `get_bars_for_api_sync`, which run the async Redis path via `asyncio.run`.

## Deploy notes

1. Start `inference_api` before daemon or export.
2. Run `trade_research_export` after model/checkpoint changes (or when history grows).
3. Restart Web GUI after `.env` changes; daemon picks up env on start.
