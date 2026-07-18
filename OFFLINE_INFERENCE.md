# Offline inference

Web GUI no longer calls `inference_api` on each refresh. Predictions are written to files under `data/` by background/offline scripts; the GUI reads those artifacts.

## Artifacts

| Path | Writer | Reader |
|------|--------|--------|
| `data/inference/BTC_USDT/latest_inference.json` | `main.inference_daemon` | `GET /api/inference` |
| `data/trade_research/BTC_USDT/predictions.npz` | `main.trade_research_export` | `GET /api/trade-research` |
| `data/trade_research/BTC_USDT/meta.json` | export script | trade research loader / UI status |

### `latest_inference.json`

- `status`: `computing` | `ok` | `error`
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

## Memory / Polars

Heavy Polars loads (inference daemon cycle, trade research export, Web GUI bars/trade-research) run in **spawn subprocesses** via `main.spawn_process.run_in_spawned_process`. Memory is released when the child exits.

Use `-v` / `--verbose` on `main.inference_daemon`, `main.trade_research_export`, and `main.web_gui` for INFO logs: DB reads, dataset preparation, inference batch progress.

## Deploy notes

1. Start `inference_api` before daemon or export.
2. Run `trade_research_export` after model/checkpoint changes (or when history grows).
3. Restart Web GUI after `.env` changes; daemon picks up env on start.
