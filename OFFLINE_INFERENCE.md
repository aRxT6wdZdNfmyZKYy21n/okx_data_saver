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
# Faster payload prep while GPU runs inference batches:
python3 -m main.trade_research_export --symbol BTC_USDT -v --num-workers 4 --prefetch-factor 2
```

## Environment

See `.env.example`:

- `TRADING_BOT_ROOT` — path to `trading_bot` repo (`/mnt/hdd2/Repositories/trading_bot`). Required when dataset targets include `*_range_min` / `*_range_max`: submodule imports `src.tools.horizon_range_target_common` from that repo. Set in `.env`; `inference_service` and `trade_research_export` call `ensure_trading_bot_on_path()` before building `HybridTradeDataset`.
- `INFERENCE_DAEMON_*` — symbol, interval (default 60s), bars limit (default 10M)
- `WEB_GUI_INFERENCE_API_BASE_URL` — still used by daemon/export to call `inference_api`
- `WEB_GUI_TRADE_JOURNAL_PATH` — micro-live journal JSON (default `data/trade_journal.json`; set per web_gui instance)
- `WEB_GUI_TRADE_JOURNAL_DEFAULT_EVAL_HORIZON` — journal UI default horizon (`x32` for L147 sign_only deploy; legacy fixed-H runs may use `x1536`, …)

## Micro live — sign_only renew (L147 x32)

When `inference_api` exposes `exit_stack_by_symbol.BTC_USDT.mode=rolling_h_renew_sign_only`:

- **Entry** uses deploy eval horizon **`x32`** (from exit stack / policy), not the journal dropdown.
- **Snapshot** stores `entry_predictions` keyed by `target_close_return_signed_log2_x32`.
- **Progress** is per **32-bar segment** (not fixed `x1536`): segment bar count, renew count, bars until next checkpoint.
- **Exit policy** (daemon + inference cycle): segment-based `rolling_h_renew_sign_only` eval on **latest available predictions** at pending checkpoint — no wait for a newer inference tick. Close on `sign_flip_at_checkpoint`; renew on `sign_valid_renewed`. Implementation: `main/web_gui/sign_only_renew_exit_common.py` (local eval; remote `/exit-policy` no longer gates daemon close).

- **Trading daemon freshness gate:** entry/exit use latest enriched artifact (`ok` or `computing` + `last_inference_ok` snapshot). Block only when predictions are older than `WEB_GUI_TRADING_MAX_PREDICTION_AGE_MS` (default **600000** = 10 min) or artifact is `error` / missing preds. Logs: `skip_tick` with `predictions_stale`. Aligns with lag-sweep live band (4–8 min); avoids idle ~95% of time while inference cycle runs.

**Bug fixed 2026-08-14:** daemon previously skipped all ticks when `latest_inference.json` status was `computing`, even though GUI showed fresh-enough preds from sidecar. Also fixed checkpoint exit waiting for post-checkpoint inference (`sign_flip_at_checkpoint` on latest preds).

Discard and re-open any journal position opened before this change (old rows used wrong `eval_horizon` → missing pred snapshot).

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

## `trading_bot_dataset` submodule (rc7)

Both `trading_bot` and `okx_data_saver` track **`trading_bot_dataset` branch `rc7`** (`.gitmodules` → `branch = rc7`).

**Current geometry:** `rc7` ≥ **`c290cb7`** — split level0 features/targets (doc **061o** in sibling `trading_bot` repo: `docs/auto_ml/061o_split_level0_features_targets.md`).

```bash
# After pulling trading_bot_dataset changes on rc7:
cd okx_data_saver
git submodule update --init --remote trading_bot_dataset
# or: cd trading_bot_dataset && git checkout rc7 && git pull

cd ../trading_bot   # optional: keep parent pointer in sync
git submodule update --init --remote trading_bot_dataset
git add trading_bot_dataset && git commit -m "Bump trading_bot_dataset rc7"
```

Keep **`TRADING_BOT_ROOT`** pointed at the same machine’s `trading_bot` checkout (not an stale copy under `/home/debian/...`).

## Deploy notes

1. Start `inference_api` before daemon or export.
2. Run `trade_research_export` after model/checkpoint changes (or when history grows).
3. Restart Web GUI after `.env` changes; daemon picks up env on start.
