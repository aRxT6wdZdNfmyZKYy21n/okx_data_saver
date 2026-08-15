(function () {
  'use strict';

  const API = {
    async get(path, fetchOptions) {
      const timeoutMs = fetchOptions && fetchOptions.timeoutMs != null
        ? fetchOptions.timeoutMs
        : null;
      const externalSignal = fetchOptions ? fetchOptions.signal : null;

      const controller = new AbortController();
      let timeoutId = null;

      if (externalSignal) {
        if (externalSignal.aborted) {
          controller.abort();
        } else {
          externalSignal.addEventListener('abort', () => controller.abort(), { once: true });
        }
      }
      if (timeoutMs != null) {
        timeoutId = setTimeout(() => controller.abort(), timeoutMs);
      }

      try {
        const r = await fetch(path, { signal: controller.signal });
        if (!r.ok) throw new Error(await r.text());
        return r.json();
      } catch (exception) {
        if (exception.name === 'AbortError') {
          if (externalSignal && externalSignal.aborted) {
            throw exception;
          }
          throw new Error('Request timed out');
        }
        throw exception;
      } finally {
        if (timeoutId != null) {
          clearTimeout(timeoutId);
        }
      }
    },
    async config() { return this.get('./api/config'); },
    async symbols() { return this.get('./api/symbols'); },
    async scales() { return this.get('./api/scales'); },
    async dowLevels() { return this.get('./api/dow_levels'); },
    async bars(params) {
      const q = new URLSearchParams(params).toString();
      return this.get('./api/bars?' + q);
    },
    async dow(params) {
      const q = new URLSearchParams(params).toString();
      return this.get('./api/dow?' + q);
    },
    async inference(params, fetchOptions) {
      const q = new URLSearchParams(params).toString();
      const options = fetchOptions ? { ...fetchOptions } : {};
      if (options.timeoutMs == null) {
        options.timeoutMs = INFERENCE_FETCH_TIMEOUT_MS;
      }
      return this.get('./api/inference?' + q, options);
    },
    async tradeResearch(params) {
      const q = new URLSearchParams(params).toString();
      return this.get('./api/trade-research?' + q);
    },
    async tradeJournal(params) {
      const q = new URLSearchParams(params).toString();
      return this.get('./api/trade-journal?' + q);
    },
    async tradeJournalBarsElapsed(params) {
      const q = new URLSearchParams(params).toString();
      return this.get('./api/trade-journal/bars-elapsed?' + q);
    },
  };

  let config = { defaultLimit: 32768, defaultScale: 'x32', refreshIntervalSec: 30 };
  let chart = null;
  let candleSeries = null;
  let barsData = [];
  /** Объёмы по индексу свечи (1:1 с candleData после сортировки и слияния по time) */
  let volumeDataByCandleIndex = [];
  /** Свечи по индексу (time, open, high, low, close) для проекции линий экстремумов на ценовой график */
  let candleDataByIndex = [];
  /** Сегменты от экстремума к экстремуму: { green: [{indexFrom, valueFrom, indexTo, valueTo}, ...], red: [...] } */
  let extremaSegments = { green: [], red: [] };
  /** Серии линий non-overlapping trade research @ eval horizon */
  let tradeResearchSegments = [];
  let tradeResearchLineSeries = [];
  let tradeResearchMarkerPrimitive = null;
  let extremaLineSeries = [];
  let refreshTimer = null;
  let inferenceRefreshTimer = null;
  let journalRefreshTimer = null;
  let journalBarsElapsedTimer = null;
  let x1BarRefreshTimer = null;
  let assetVersionPollTimer = null;
  let loadBarsInFlight = false;
  let loadBarsRequestSeq = 0;
  let activeLoadBarsRequestId = 0;
  let loadTradeResearchInFlight = false;
  let tradeResearchRequestSeq = 0;
  let activeTradeResearchRequestId = 0;
  /** Инкремент при каждой перезагрузке баров — отсекает stale trade research. */
  let barsDataGeneration = 0;
  /** Последний запрос trade research, пропущенный из‑за inFlight (перезапуск в finally). */
  let pendingTradeResearch = null;

  function guiLog(event, details) {
    const payload = details === undefined ? '' : details;
    console.log(`[web-gui] ${new Date().toISOString()} ${event}`, payload);
  }
  let inferenceFetchAbortController = null;
  let loadInferenceRequestSeq = 0;
  let activeLoadInferenceRequestId = 0;
  let refreshJournalInFlight = false;
  let refreshBarsElapsedInFlight = false;
  let refreshX1BarInFlight = false;
  let lastJournalBarsElapsed = null;
  let lastJournalBarsElapsedEntryStartTradeId = null;
  let tradeJournalRequestSeq = 0;
  let journalBarsRefetchTimer = null;
  const INFERENCE_REFRESH_INTERVAL_SEC = 10;
  const INFERENCE_FETCH_TIMEOUT_MS = 30000;
  const JOURNAL_REFRESH_INTERVAL_SEC = 15;
  const JOURNAL_BARS_ELAPSED_INTERVAL_SEC = 30;
  const X1_BAR_REFRESH_INTERVAL_SEC = 60;
  const ASSET_VERSION_POLL_INTERVAL_SEC = 60;
  const scaleSelect = document.getElementById('scale');
  const symbolSelect = document.getElementById('symbol');
  const limitInput = document.getElementById('limit');
  const loadBtn = document.getElementById('load');
  const autoRefreshCheck = document.getElementById('autoRefresh');
  const extremaLinesEnabledCheck = document.getElementById('extremaLinesEnabled');
  const tradeResearchEnabledCheck = document.getElementById('tradeResearchEnabled');
  const tradeResearchEnabledLabel = document.getElementById('tradeResearchEnabledLabel');
  const tradeResearchEnabledText = document.getElementById('tradeResearchEnabledText');
  const statusEl = document.getElementById('status');
  const chartDiv = document.getElementById('chart');
  const concentrationCanvas = document.getElementById('concentrationCanvas');
  const concentrationPanel = document.getElementById('concentrationPanel');
  const cumulativeCanvas = document.getElementById('cumulativeCanvas');
  const cumulativePanel = document.getElementById('cumulativePanel');
  const volumeCanvas = document.getElementById('volumeCanvas');
  const volumePanel = document.getElementById('volumePanel');
  const cvdWindowSelect = document.getElementById('cvdWindow');
  const dowStub = document.getElementById('dowStub');
  const inferencePanel = document.getElementById('inferencePanel');
  const inferenceStatusBar = document.getElementById('inferenceStatusBar');
  const policySummary = document.getElementById('policySummary');
  const inferenceContent = document.getElementById('inferenceContent');
  const tradeJournalPanel = document.getElementById('tradeJournalPanel');
  const tradeJournalContent = document.getElementById('tradeJournalContent');
  const tradeJournalTotals = document.getElementById('tradeJournalTotals');
  let inferenceErrorBySymbolAndHorizon = {};
  let policyBySymbol = {};
  let exitPolicyBySymbol = {};
  let exitStackBySymbol = {};
  let entryHintModeBySymbol = {};
  let entryConfidenceMarginBySymbol = {};
  let tradingInitialBalanceUsd = 100;
  let checkpointPathBySymbol = {};
  let inferenceMinRows = 0;
  let chartShowLimit = 50000;
  let lastPolicy = null;
  let lastEntryHint = null;
  let lastPredictions = null;
  let lastExitPolicy = null;
  let lastInferenceStatus = null;
  let lastInferenceCompletedAtMs = null;
  let lastInferenceNetworkError = null;
  let lastComputingStartedAtMs = null;
  let inferenceStatusTickTimer = null;
  let lastInferenceBarProvenance = null;
  let latestX1Bar = null;
  let lastChartBarClose = null;
  let isFirstJournalLoad = true;
  let journalHasOpenPosition = false;
  let lastJournalState = null;
  const SCALE_NAMES = ['x1', 'x2', 'x4', 'x8', 'x16', 'x32', 'x64', 'x128', 'x256', 'x512', 'x1024', 'x1536', 'x2048', 'x4096', 'x8192', 'x16384', 'x32768', 'x65536', 'x131072', 'x262144'];
  const CVD_WINDOW_OPTIONS = ['x2', 'x4', 'x8', 'x16', 'x32', 'x64', 'x128', 'x256', 'x512', 'x1024', 'x2048', 'x4096', 'x8192', 'x16384'];
  const CVD_WINDOW_DEFAULT = 'x512';
  let tradeResearchEvalHorizon = 'x32';
  let tradeResearchScale = 'x32';
  let tradeResearchAvailableHorizons = [];

  function updateTradeResearchUi() {
    const horizon = tradeResearchEvalHorizon || '?';
    if (tradeResearchEnabledText) {
      tradeResearchEnabledText.textContent = `Исследовать сделки @ ${horizon}`;
    }
    if (tradeResearchEnabledLabel) {
      tradeResearchEnabledLabel.title =
        `Non-overlapping policy trades @ ${horizon} на x1`;
    }
  }


  function resolveDeployEvalHorizon(openPos, symbol) {
    if (openPos && openPos.exit_stack_eval_horizon) {
      return String(openPos.exit_stack_eval_horizon);
    }
    const exitStack = getExitStackForSymbol(symbol);
    if (exitStack && exitStack.eval_horizon) {
      return String(exitStack.eval_horizon);
    }
    if (openPos && openPos.entry_policy && openPos.entry_policy.eval_horizon) {
      return String(openPos.entry_policy.eval_horizon);
    }
    if (lastPolicy && lastPolicy.eval_horizon) {
      return String(lastPolicy.eval_horizon);
    }
    if (openPos && openPos.eval_horizon) {
      return String(openPos.eval_horizon);
    }
    return tradeResearchEvalHorizon || 'x32';
  }


  function renderEquityCurveSvg(equityCurve) {
    if (!Array.isArray(equityCurve) || equityCurve.length < 2) {
      return '';
    }
    const width = 640;
    const height = 120;
    const padding = 8;
    const balances = equityCurve.map(point => Number(point.balance_usd));
    const minBalance = Math.min(...balances);
    const maxBalance = Math.max(...balances);
    const span = maxBalance - minBalance;
    const effectiveSpan = span > 0 ? span : 1;
    const points = equityCurve.map((point, index) => {
      const x = padding + (
        (index / (equityCurve.length - 1)) * (width - padding * 2)
      );
      const balance = Number(point.balance_usd);
      const y = padding + (
        (1 - ((balance - minBalance) / effectiveSpan)) * (height - padding * 2)
      );
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(' ');
    const lastBalance = balances[balances.length - 1];
    return `
      <div class="trade-journal-equity">
        <h4>Equity (${formatUsd(lastBalance)})</h4>
        <svg class="trade-journal-equity-chart" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
          <polyline points="${points}" fill="none" stroke="#42a5f5" stroke-width="2" />
        </svg>
      </div>
    `;
  }

  function setStatus(text, isError = false) {
    statusEl.textContent = text;
    statusEl.style.color = isError ? '#ef5350' : '';
  }

  function parseErrorDetail(rawMessage) {
    if (!rawMessage) return 'Ошибка инференса';
    try {
      const parsed = JSON.parse(rawMessage);
      if (parsed && typeof parsed === 'object' && parsed.detail != null) return String(parsed.detail);
    } catch (_) {
      // no-op
    }
    return rawMessage;
  }

  function extractHorizon(targetName) {
    const m = targetName.match(/_(x\d+)(?:_|$)/);
    return m ? m[1] : targetName;
  }

  function isAuxiliaryPredictionKey(targetName) {
    return targetName.endsWith('_pred_long') || targetName.endsWith('_pred_short');
  }

  function signedLog2ToPercent(v) {
    return (2 ** v - 1) * 100;
  }

  function predictionKeyForHorizon(horizon) {
    return `target_close_return_signed_log2_${horizon}`;
  }

  function expectedMovePctForSide(signedLog2, side) {
    const linearPct = signedLog2ToPercent(Number(signedLog2));
    if (side === 'short') return -linearPct;
    return linearPct;
  }

  function entryPredictionSnapshot(openPos, symbol) {
    if (!openPos || !openPos.entry_predictions) return null;
    const evalHorizon = resolveDeployEvalHorizon(openPos, symbol);
    const predictionKey = predictionKeyForHorizon(evalHorizon);
    const entryPredictions = openPos.entry_predictions;
    if (!(predictionKey in entryPredictions)) return null;
    const signedLog2 = Number(entryPredictions[predictionKey]);
    if (!Number.isFinite(signedLog2)) return null;
    const expectedPct = expectedMovePctForSide(signedLog2, openPos.side);
    return {
      evalHorizon,
      signedLog2,
      expectedPct,
    };
  }

  function currentPredictionSnapshot(openPos, symbol) {
    if (!openPos || !lastPredictions) return null;
    const evalHorizon = resolveDeployEvalHorizon(openPos, symbol);
    const predictionKey = predictionKeyForHorizon(evalHorizon);
    if (!(predictionKey in lastPredictions)) return null;
    const signedLog2 = Number(lastPredictions[predictionKey]);
    if (!Number.isFinite(signedLog2)) return null;
    return {
      evalHorizon,
      signedLog2,
      expectedPct: expectedMovePctForSide(signedLog2, openPos.side),
    };
  }

  function renderEntryPredictionMetrics(openPos, metrics, symbol) {
    const entrySnap = entryPredictionSnapshot(openPos, symbol);
    if (!entrySnap) {
      return `
        <div class="trade-journal-prediction-hint">
          Pred @ entry: <strong>—</strong> (нет snapshot — переоткрой позицию после обновления)
        </div>
      `;
    }

    const unrealizedPct = Number(metrics.unrealized_net_return_pct);
    const expectedPct = entrySnap.expectedPct;
    const capturePct = expectedPct !== 0 && Number.isFinite(unrealizedPct)
      ? (100 * unrealizedPct / expectedPct)
      : null;
    const captureLabel = capturePct != null && Number.isFinite(capturePct)
      ? `${Math.max(0, capturePct).toFixed(0)}%`
      : '—';
    const captureClass = capturePct != null && capturePct >= 100 ? 'pred-capture-done' : '';
    const captureWidth = capturePct != null && Number.isFinite(capturePct)
      ? Math.min(100, Math.max(0, capturePct))
      : 0;

    const currentSnap = currentPredictionSnapshot(openPos, symbol);
    const currentPredLine = currentSnap
      ? `<span>Pred now (${currentSnap.evalHorizon}): <strong class="${pnlClass(currentSnap.expectedPct)}">${formatPct(currentSnap.expectedPct)}</strong></span>`
      : '';

    const deltaLine = currentSnap
      ? `<span>Δ pred: <strong class="${pnlClass(currentSnap.expectedPct - entrySnap.expectedPct)}">${formatPct(currentSnap.expectedPct - entrySnap.expectedPct)}</strong></span>`
      : '';

    return `
      <div class="trade-journal-prediction-block">
        <div class="trade-journal-metrics trade-journal-prediction-metrics">
          <span>Pred @ entry (${entrySnap.evalHorizon}): <strong class="${pnlClass(entrySnap.expectedPct)}">${formatPct(entrySnap.expectedPct)}</strong></span>
          <span>Capture: <strong class="${captureClass}">${captureLabel}</strong> (${formatPct(unrealizedPct)} / ${formatPct(expectedPct)})</span>
          ${currentPredLine}
          ${deltaLine}
        </div>
        <div class="trade-journal-pred-progress" title="Capture ${captureLabel} от pred @ entry">
          <div class="trade-journal-pred-progress-bar ${captureClass}" style="width: ${captureWidth}%"></div>
        </div>
      </div>
    `;
  }

  function setInferenceWarning(message) {
    inferencePanel.classList.remove('hidden');
    inferencePanel.classList.remove('inference-panel-stale');
    inferenceStatusBar.classList.add('hidden');
    inferenceStatusBar.innerHTML = '';
    policySummary.classList.add('hidden');
    policySummary.innerHTML = '';
    inferenceContent.innerHTML = `<div class="inference-warning">${message}</div>`;
  }

  function stopInferenceStatusTick() {
    if (inferenceStatusTickTimer) {
      clearInterval(inferenceStatusTickTimer);
      inferenceStatusTickTimer = null;
    }
  }

  function formatRelativeTimeAgo(timestampMs) {
    if (!Number.isFinite(timestampMs)) return '—';
    const deltaSec = Math.max(0, Math.floor((Date.now() - timestampMs) / 1000));
    if (deltaSec < 5) return 'только что';
    if (deltaSec < 60) return `${deltaSec} сек назад`;
    const minutes = Math.floor(deltaSec / 60);
    if (minutes === 1) return 'минуту назад';
    if (minutes < 60) return `${minutes} мин назад`;
    const hours = Math.floor(minutes / 60);
    if (hours === 1) return 'час назад';
    return `${hours} ч назад`;
  }

  function formatDurationSince(timestampMs) {
    if (!Number.isFinite(timestampMs)) return '—';
    const deltaSec = Math.max(0, Math.floor((Date.now() - timestampMs) / 1000));
    if (deltaSec < 60) return `${deltaSec} сек`;
    const minutes = Math.floor(deltaSec / 60);
    const seconds = deltaSec % 60;
    if (minutes < 60) {
      return seconds > 0 ? `${minutes} мин ${seconds} сек` : `${minutes} мин`;
    }
    const hours = Math.floor(minutes / 60);
    const remMin = minutes % 60;
    return remMin > 0 ? `${hours} ч ${remMin} мин` : `${hours} ч`;
  }

  function setInferencePanelStale(isStale) {
    inferencePanel.classList.toggle('inference-panel-stale', Boolean(isStale));
  }

  function captureInferenceBarProvenance(response) {
    if (response == null) {
      lastInferenceBarProvenance = null;
      return;
    }
    const barStartTradeId = response.bar_start_trade_id;
    const barTimestampMs = response.bar_timestamp_ms;
    if (barStartTradeId == null || barTimestampMs == null) {
      lastInferenceBarProvenance = null;
      return;
    }
    let barClosePrice = null;
    if (response.bar_close_price != null) {
      const closeValue = Number(response.bar_close_price);
      if (Number.isFinite(closeValue) && closeValue > 0) {
        barClosePrice = closeValue;
      }
    }
    lastInferenceBarProvenance = {
      bar_start_trade_id: Number(barStartTradeId),
      bar_timestamp_ms: Number(barTimestampMs),
      bar_close_price: barClosePrice,
    };
  }

  function resolveInferenceBarClosePrice(provenance) {
    if (provenance == null) {
      return null;
    }
    if (provenance.bar_close_price != null) {
      const closeValue = Number(provenance.bar_close_price);
      if (Number.isFinite(closeValue) && closeValue > 0) {
        return closeValue;
      }
    }
    if (
      latestX1Bar != null &&
      Number(latestX1Bar.start_trade_id) === Number(provenance.bar_start_trade_id) &&
      latestX1Bar.close_price != null
    ) {
      const chartClose = Number(latestX1Bar.close_price);
      if (Number.isFinite(chartClose) && chartClose > 0) {
        return chartClose;
      }
    }
    return null;
  }

  function isInferenceBarBehindChart(provenance) {
    if (provenance == null || latestX1Bar == null) {
      return false;
    }
    return Number(provenance.bar_start_trade_id) < Number(latestX1Bar.start_trade_id);
  }

  function formatInferenceBarProvenanceText(provenance) {
    if (provenance == null) {
      return '';
    }
    const utcLabel = formatInferenceX1Utc(provenance.bar_timestamp_ms);
    const closePrice = resolveInferenceBarClosePrice(provenance);
    const closeLabel = closePrice != null ? closePrice.toFixed(2) : '—';
    let text = `x1 ${utcLabel} @ ${closeLabel}`;
    if (isInferenceBarBehindChart(provenance)) {
      text = text + ' (не последний бар)';
    }
    return text;
  }

  function renderInferenceStatusBar() {
    if (lastInferenceStatus == null) {
      inferenceStatusBar.classList.add('hidden');
      inferenceStatusBar.innerHTML = '';
      return;
    }

    inferenceStatusBar.classList.remove('hidden');
    inferenceStatusBar.classList.toggle(
      'inference-status-computing',
      lastInferenceStatus === 'computing',
    );
    inferenceStatusBar.classList.toggle(
      'inference-status-network-error',
      lastInferenceStatus === 'network_error',
    );

    if (lastInferenceStatus === 'ok') {
      const ageLabel = formatRelativeTimeAgo(lastInferenceCompletedAtMs);
      const barProvenanceText = formatInferenceBarProvenanceText(lastInferenceBarProvenance);
      const barLine = barProvenanceText
        ? `<span class="inference-status-bar-anchor">Бар инференса: <strong>${barProvenanceText}</strong></span>`
        : '';
      inferenceStatusBar.innerHTML = `
        <span class="inference-status-age">Обновлено: <strong>${ageLabel}</strong></span>
        ${barLine}
      `;
      return;
    }

    if (lastInferenceStatus === 'computing') {
      const ageLabel = formatRelativeTimeAgo(lastInferenceCompletedAtMs);
      const refreshDuration = formatDurationSince(lastComputingStartedAtMs);
      const hasSnapshot = Number.isFinite(lastInferenceCompletedAtMs);
      const barProvenanceText = formatInferenceBarProvenanceText(lastInferenceBarProvenance);
      const barLine = barProvenanceText
        ? `<span class="inference-status-bar-anchor">Бар инференса: <strong>${barProvenanceText}</strong></span>`
        : '';
      const snapshotLine = hasSnapshot
        ? `<span class="inference-status-age">Предсказания от <strong>${ageLabel}</strong></span>`
        : '<span class="inference-status-age">Первый offline-инференс ещё не готов</span>';
      inferenceStatusBar.innerHTML = `
        ${snapshotLine}
        ${barLine}
        <span class="inference-status-computing-label">Обновление… (${refreshDuration})</span>
      `;
      return;
    }

    if (lastInferenceStatus === 'network_error') {
      const ageLabel = formatRelativeTimeAgo(lastInferenceCompletedAtMs);
      const errorMessage = lastInferenceNetworkError || 'сеть недоступна';
      const barProvenanceText = formatInferenceBarProvenanceText(lastInferenceBarProvenance);
      const barLine = barProvenanceText
        ? `<span class="inference-status-bar-anchor">Бар инференса: <strong>${barProvenanceText}</strong></span>`
        : '';
      inferenceStatusBar.innerHTML = `
        <span class="inference-status-age">Обновлено: <strong>${ageLabel}</strong></span>
        ${barLine}
        <span class="inference-status-network-error-label">${errorMessage}. Повтор через ${INFERENCE_REFRESH_INTERVAL_SEC} сек…</span>
      `;
    }
  }

  function startInferenceStatusTick() {
    stopInferenceStatusTick();
    inferenceStatusTickTimer = setInterval(() => {
      if (inferencePanel.classList.contains('hidden')) return;
      renderInferenceStatusBar();
    }, 10000);
  }

  function renderPolicy(policy, symbol, entryHint) {
    lastPolicy = policy || null;
    lastEntryHint = entryHint || null;
    if (!policy || !policy.action) {
      policySummary.classList.add('hidden');
      policySummary.innerHTML = '';
      return;
    }

    const action = String(policy.action).toUpperCase();
    const evalHorizon = policy.eval_horizon || '—';
    const runLabel = policy.run_label || '—';
    const probs = policy.probabilities || {};
    const holdPct = probs.hold != null ? (Number(probs.hold) * 100).toFixed(1) : '—';
    const longPct = probs.long != null ? (Number(probs.long) * 100).toFixed(1) : '—';
    const shortPct = probs.short != null ? (Number(probs.short) * 100).toFixed(1) : '—';
    const checkpointPath = checkpointPathBySymbol[symbol] || '—';
    let actionClass = 'policy-hold';
    if (action === 'LONG') actionClass = 'policy-long';
    if (action === 'SHORT') actionClass = 'policy-short';

    let entryHintHtml = '';
    if (entryHint) {
      const snr = entryHint.snr != null ? Number(entryHint.snr).toFixed(2) : '—';
      const snrThreshold = entryHint.snr_threshold != null ? Number(entryHint.snr_threshold) : 0.5;
      const rmsePct = entryHint.rmse_pct != null ? Number(entryHint.rmse_pct).toFixed(3) : '—';
      const recommended = entryHint.recommended_action
        ? String(entryHint.recommended_action).toUpperCase()
        : '—';
      const blocked = Boolean(entryHint.entry_blocked);
      const blockReason = entryHint.block_reason ? String(entryHint.block_reason) : '';
      const hintMode = entryHint.hint_mode ? String(entryHint.hint_mode) : 'snr_only';
      const isHybrid = hintMode === 'hybrid_gate_snr';
      const isSignFeeBand = hintMode === 'sign_fee_band';
      const marginLinear = entryHint.entry_threshold_linear != null
        ? Number(entryHint.entry_threshold_linear)
        : null;
      const holdProb = entryHint.hold_probability != null
        ? Number(entryHint.hold_probability).toFixed(2)
        : null;
      const holdThreshold = entryHint.hold_probability_threshold != null
        ? Number(entryHint.hold_probability_threshold).toFixed(2)
        : null;
      const gbmBlocks = Boolean(entryHint.gbm_blocks_entry);
      const snrBlocks = Boolean(entryHint.snr_blocks_entry);
      const title = isSignFeeBand
        ? `Entry hint @ ${evalHorizon} (sign_fee_band, threshold=${marginLinear != null ? formatPct(marginLinear * 100) : '—'}, rmse=${rmsePct}%)`
        : (isHybrid
        ? `Entry hint @ ${evalHorizon} (hybrid P(hold)≥${holdThreshold} ∨ SNR≥${snrThreshold}, rmse=${rmsePct}%)`
        : `Entry hint @ ${evalHorizon} (SNR≥${snrThreshold}, rmse=${rmsePct}%)`);
      const hybridMeta = isHybrid && holdProb != null
        ? `<span>P(hold): <strong>${holdProb}</strong>${gbmBlocks ? ' ⛔' : ''}</span>`
        : '';
      const blockTags = isHybrid
        ? `<span>${gbmBlocks ? 'GBM block' : 'GBM ok'} / ${snrBlocks ? 'SNR block' : 'SNR ok'}</span>`
        : '';
      const dualGate = entryHint.dual_scalar_entry_gate;
      const dualGateRunLabel = entryHint.dual_scalar_entry_gate_run_label
        ? String(entryHint.dual_scalar_entry_gate_run_label)
        : '';
      let dualScalarMeta = '';
      if (dualGate) {
        const predLongPct = signedLog2ToPercent(Number(dualGate.pred_long_log2)).toFixed(2);
        const predShortPct = signedLog2ToPercent(Number(dualGate.pred_short_log2)).toFixed(2);
        const gapLog2 = Number(dualGate.strength_gap_log2).toFixed(4);
        const epsLog2 = Number(dualGate.conflict_eps_log2).toFixed(4);
        const bothActive = Boolean(dualGate.both_active);
        const conflictHold = Boolean(dualGate.conflict_hold_blocks_entry);
        dualScalarMeta = `
            <span>long/short pred: <strong>${predLongPct}%</strong> / <strong>${predShortPct}%</strong></span>
            <span>gap: ${gapLog2} (eps ${epsLog2})</span>
            ${bothActive ? '<span>both active</span>' : ''}
            ${conflictHold ? '<span>conflict hold</span>' : ''}
            ${dualGateRunLabel ? `<span>gate: <strong>${dualGateRunLabel}</strong></span>` : ''}
        `;
      }
      const dualScalarTitle = dualGate
        ? `Entry hint @ ${evalHorizon} (dual-scalar ${dualGate.gate_mode || 'gate'}, rmse=${rmsePct}%)`
        : title;
      entryHintHtml = `
        <div class="entry-hint ${blocked ? 'entry-hint-blocked' : 'entry-hint-ok'}">
          <div class="entry-hint-title">${dualScalarTitle}</div>
          <div class="entry-hint-meta">
            <span>SNR: <strong>${snr}</strong></span>
            ${hybridMeta}
            <span>band: [${Number(entryHint.min_pct).toFixed(2)}%, ${Number(entryHint.max_pct).toFixed(2)}%]</span>
            ${blockTags}
            ${dualScalarMeta}
            <span>→ <strong>${recommended}</strong></span>
          </div>
          ${blocked ? `<div class="entry-hint-warning">${blockReason || 'uncertainty — подождать'}</div>` : ''}
        </div>
      `;
      if (blocked && (action === 'LONG' || action === 'SHORT')) {
        actionClass += ' policy-entry-blocked';
      }
    }

    policySummary.classList.remove('hidden');
    const barProvenanceText = formatInferenceBarProvenanceText(lastInferenceBarProvenance);
    const barProvenanceHtml = barProvenanceText
      ? `<div class="policy-bar-anchor">Вход модели: <strong>${barProvenanceText}</strong></div>`
      : '';
    policySummary.innerHTML = `
      <div class="policy-card ${actionClass}">
        <div class="policy-action">${action}</div>
        <div class="policy-meta">
          <span>eval: <strong>${evalHorizon}</strong></span>
          <span>stack: <strong>${runLabel}</strong></span>
          <span>P(hold/long/short): ${holdPct}% / ${longPct}% / ${shortPct}%</span>
        </div>
        ${barProvenanceHtml}
        ${entryHintHtml}
        <div class="policy-checkpoint" title="${checkpointPath}">base ckpt: ${checkpointPath}</div>
      </div>
    `;
  }

  function renderInference(predictions, symbol, policy, entryHint, isStale) {
    lastPredictions = predictions || null;
    const stale = Boolean(isStale);
    setInferencePanelStale(stale);
    const keys = Object.keys(predictions || {});
    if (keys.length === 0) {
      setInferenceWarning('Нет предсказаний для отображения');
      return;
    }

    const errorConfig = inferenceErrorBySymbolAndHorizon[symbol] || {};
    const evalHorizonForPolicy = policy && policy.eval_horizon ? String(policy.eval_horizon) : null;
    const sorted = keys.sort((a, b) => {
      const ha = extractHorizon(a);
      const hb = extractHorizon(b);
      const va = parseInt(ha.slice(1), 10);
      const vb = parseInt(hb.slice(1), 10);
      return va - vb;
    });

    const rows = [];
    for (const key of sorted) {
      if (isAuxiliaryPredictionKey(key)) continue;
      const horizon = extractHorizon(key);
      const signedLog2 = Number(predictions[key]);
      if (!Number.isFinite(signedLog2)) continue;
      const predictedPct = signedLog2ToPercent(signedLog2);
      const errorPct = errorConfig[horizon] != null ? Number(errorConfig[horizon]) : 0;
      const minPct = predictedPct - errorPct;
      const maxPct = predictedPct + errorPct;
      const allNegative = maxPct < 0;
      const allPositive = minPct > 0;
      const forecastClass = predictedPct >= 0 ? 'positive' : 'negative';
      const minClass = minPct >= 0 ? 'positive' : 'negative';
      const maxClass = maxPct >= 0 ? 'positive' : 'negative';
      const lineClass = allNegative ? 'short-signal' : (allPositive ? 'positive' : (predictedPct >= 0 ? 'positive' : 'negative'));
      const evalHorizonClass = evalHorizonForPolicy === horizon ? ' inference-eval-horizon' : '';
      rows.push(
        `<div class="inference-line ${lineClass}${evalHorizonClass}" title="Точное значение: ${predictedPct.toFixed(3)}%${errorPct > 0 ? '' : ' (rmse не задан)'}">
          <span class="inference-horizon">${horizon}</span>
          ${allNegative ? '<span class="inference-short">SHORT</span>' : ''}
          ${allPositive ? '<span class="inference-long">LONG</span>' : ''}
          <span>Прогноз: <span class="inference-value ${forecastClass}">${predictedPct.toFixed(2)}%</span></span>
          <span>Мин: <span class="inference-value ${minClass}">${minPct.toFixed(2)}%</span></span>
          <span>Макс: <span class="inference-value ${maxClass}">${maxPct.toFixed(2)}%</span></span>
        </div>`
      );
    }

    inferencePanel.classList.remove('hidden');
    renderPolicy(policy, symbol, entryHint);
    inferenceContent.innerHTML = rows.join('');
  }

  function handleInferenceFetchError(error, symbol) {
    const message = parseErrorDetail(error.message);
    guiLog('inference refresh error', {
      message,
      hasStalePredictions: Boolean(lastPredictions),
    });

    if (lastPredictions) {
      lastInferenceStatus = 'network_error';
      lastInferenceNetworkError = message;
      renderInference(
        lastPredictions,
        symbol,
        lastPolicy,
        lastEntryHint,
        true,
      );
      renderInferenceStatusBar();
      startInferenceStatusTick();
      return;
    }

    stopInferenceStatusTick();
    lastInferenceStatus = null;
    setInferenceWarning(message);
  }

  function loadInference(symbol, limit, fetchSignal, requestId) {
    const fetchOptions = fetchSignal ? { signal: fetchSignal } : null;
    return API.inference({ symbol_id: symbol, limit }, fetchOptions)
      .then(response => {
        if (activeLoadInferenceRequestId !== requestId) {
          return;
        }
        const status = response.status != null ? String(response.status) : 'ok';
        if (status === 'computing') {
          lastInferenceStatus = 'computing';
          captureInferenceBarProvenance(response);
          lastComputingStartedAtMs = response.computing_started_at_ms != null
            ? Number(response.computing_started_at_ms)
            : (response.updated_at_ms != null ? Number(response.updated_at_ms) : null);
          lastInferenceCompletedAtMs = response.inference_completed_at_ms != null
            ? Number(response.inference_completed_at_ms)
            : null;

          let predictions = response.predictions || null;
          let policy = response.policy || null;
          let entryHint = response.entry_hint || null;
          if (!predictions && lastPredictions) {
            predictions = lastPredictions;
            policy = lastPolicy;
            entryHint = lastEntryHint;
          }

          if (predictions) {
            renderInference(
              predictions,
              symbol,
              policy,
              entryHint,
              true,
            );
            rerenderTradeJournalIfOpen(symbol);
            renderInferenceStatusBar();
            startInferenceStatusTick();
            return;
          }

          stopInferenceStatusTick();
          lastInferenceStatus = 'computing';
          lastInferenceCompletedAtMs = null;
          inferencePanel.classList.remove('hidden');
          inferencePanel.classList.remove('inference-panel-stale');
          policySummary.classList.add('hidden');
          policySummary.innerHTML = '';
          inferenceContent.innerHTML = '<div class="inference-warning">Ожидание первого offline-инференса…</div>';
          renderInferenceStatusBar();
          startInferenceStatusTick();
          return;
        }
        if (status === 'error') {
          stopInferenceStatusTick();
          lastInferenceStatus = null;
          lastInferenceCompletedAtMs = null;
          lastComputingStartedAtMs = null;
          const errorMessage = response.error_message != null
            ? String(response.error_message)
            : 'unknown error';
          setInferenceWarning(`Offline inference error: ${errorMessage}`);
          lastPredictions = null;
          lastPolicy = null;
          lastEntryHint = null;
          lastExitPolicy = null;
          return;
        }
        if (!response.predictions) {
          stopInferenceStatusTick();
          lastInferenceStatus = null;
          setInferenceWarning('Offline inference artifact missing predictions');
          return;
        }
        lastInferenceStatus = 'ok';
        lastInferenceNetworkError = null;
        captureInferenceBarProvenance(response);
        lastInferenceCompletedAtMs = response.inference_completed_at_ms != null
          ? Number(response.inference_completed_at_ms)
          : (response.updated_at_ms != null ? Number(response.updated_at_ms) : null);
        lastComputingStartedAtMs = null;
        renderInference(
          response.predictions,
          symbol,
          response.policy || null,
          response.entry_hint || null,
          false,
        );
        rerenderTradeJournalIfOpen(symbol);
        renderInferenceStatusBar();
        startInferenceStatusTick();
      })
      .catch(e => {
        if (activeLoadInferenceRequestId !== requestId) {
          return;
        }
        if (e.name === 'AbortError') {
          return;
        }
        handleInferenceFetchError(e, symbol);
      });
  }

  function formatUsd(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return '—';
    const sign = n >= 0 ? '+' : '';
    return `${sign}$${n.toFixed(4)}`;
  }

  function formatPct(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return '—';
    const sign = n >= 0 ? '+' : '';
    return `${sign}${n.toFixed(3)}%`;
  }

  function pnlClass(value) {
    const n = Number(value);
    if (!Number.isFinite(n) || n === 0) return '';
    return n > 0 ? 'pnl-positive' : 'pnl-negative';
  }

  function policyActionToSide(action) {
    const normalized = String(action || '').toUpperCase();
    if (normalized === 'LONG') return 'long';
    if (normalized === 'SHORT') return 'short';
    return null;
  }

  function fetchLatestX1Bar(symbol) {
    return API.bars({ symbol_id: symbol, scale: 'x1', limit: 1 })
      .then(data => {
        const bars = data.bars || [];
        if (bars.length === 0) {
          latestX1Bar = null;
          return null;
        }
        latestX1Bar = bars[bars.length - 1];
        return latestX1Bar;
      });
  }

  function updateLatestX1BarFromBarsData(data, scale) {
    const bars = data.bars || [];
    if (bars.length === 0) {
      return;
    }
    const lastBar = bars[bars.length - 1];
    if (lastBar.close_price != null) {
      lastChartBarClose = Number(lastBar.close_price);
    }
    if (scale === 'x1') {
      latestX1Bar = lastBar;
    }
  }

  function getMarkPriceForJournal() {
    if (latestX1Bar && latestX1Bar.close_price != null) {
      return Number(latestX1Bar.close_price);
    }
    if (lastChartBarClose != null) {
      return lastChartBarClose;
    }
    return null;
  }

  function resetJournalBarsElapsedCache() {
    lastJournalBarsElapsed = null;
    lastJournalBarsElapsedEntryStartTradeId = null;
    if (journalBarsRefetchTimer) {
      clearTimeout(journalBarsRefetchTimer);
      journalBarsRefetchTimer = null;
    }
  }

  function fetchAndApplyTradeJournal(symbol) {
    const requestSeq = tradeJournalRequestSeq + 1;
    tradeJournalRequestSeq = requestSeq;
    return API.tradeJournal(buildTradeJournalParams(symbol))
      .then(state => {
        if (requestSeq !== tradeJournalRequestSeq) {
          return lastJournalState;
        }
        applyJournalState(state, symbol);
        return state;
      });
  }

  function advanceJournalBarsElapsed(entryStartTradeId, nextBarsElapsed) {
    const entryId = Number(entryStartTradeId);
    const next = Number(nextBarsElapsed);
    if (!Number.isFinite(entryId) || !Number.isFinite(next) || next < 0) {
      return lastJournalBarsElapsed;
    }
    if (lastJournalBarsElapsedEntryStartTradeId !== entryId) {
      lastJournalBarsElapsedEntryStartTradeId = entryId;
      lastJournalBarsElapsed = next;
      return next;
    }
    if (lastJournalBarsElapsed == null || next > lastJournalBarsElapsed) {
      lastJournalBarsElapsed = next;
    }
    return lastJournalBarsElapsed;
  }

  function resolveDisplayBarsElapsed(openPos) {
    if (!openPos) {
      return null;
    }
    let best = lastJournalBarsElapsed;
    if (openPos.metrics && openPos.metrics.bars_elapsed != null) {
      const fromMetrics = Number(openPos.metrics.bars_elapsed);
      if (best == null || fromMetrics > best) {
        best = fromMetrics;
      }
    }
    if (lastExitPolicy && lastExitPolicy.bars_held != null) {
      const fromExitPolicy = Number(lastExitPolicy.bars_held);
      if (best == null || fromExitPolicy > best) {
        best = fromExitPolicy;
      }
    }
    return best;
  }

  function scheduleJournalRefetchForBarsAdvance(symbol) {
    if (!symbol || !journalHasOpenPosition) {
      return;
    }
    if (journalBarsRefetchTimer) {
      clearTimeout(journalBarsRefetchTimer);
    }
    journalBarsRefetchTimer = setTimeout(() => {
      journalBarsRefetchTimer = null;
      if (!journalHasOpenPosition) {
        return;
      }
      fetchAndApplyTradeJournal(symbol);
    }, 100);
  }

  function buildTradeJournalParams(symbol) {
    const params = { symbol_id: symbol };
    const markPrice = getMarkPriceForJournal();
    if (markPrice != null) {
      params.mark_price = String(markPrice);
    }
    if (lastJournalBarsElapsed != null) {
      params.bars_elapsed = String(lastJournalBarsElapsed);
    }
    if (
      lastExitPolicy
      && lastExitPolicy.last_renew_segment_evaluated != null
    ) {
      params.last_renew_segment_evaluated = String(
        lastExitPolicy.last_renew_segment_evaluated,
      );
    }
    return params;
  }

  function pollJournalBarsElapsed(symbol, entryStartTradeId) {
    if (refreshBarsElapsedInFlight || !journalHasOpenPosition) {
      return Promise.resolve();
    }
    refreshBarsElapsedInFlight = true;
    return API.tradeJournalBarsElapsed({
      symbol_id: symbol,
      entry_start_trade_id: String(entryStartTradeId),
    })
      .then(data => {
        if (!journalHasOpenPosition) {
          return;
        }
        if (data.bars_elapsed == null) {
          return;
        }
        const prevBarsElapsed = lastJournalBarsElapsed;
        const mergedBarsElapsed = advanceJournalBarsElapsed(
          entryStartTradeId,
          Number(data.bars_elapsed),
        );
        if (mergedBarsElapsed === prevBarsElapsed) {
          return;
        }
        return fetchAndApplyTradeJournal(symbol);
      })
      .catch(() => {})
      .finally(() => {
        refreshBarsElapsedInFlight = false;
      });
  }

  function refreshLatestX1Bar() {
    const symbol = symbolSelect.value;
    if (!symbol || refreshX1BarInFlight) {
      return Promise.resolve();
    }
    refreshX1BarInFlight = true;
    return fetchLatestX1Bar(symbol)
      .then(() => {
        if (symbolSelect.value === symbol) {
          return fetchAndApplyTradeJournal(symbol);
        }
      })
      .finally(() => {
        refreshX1BarInFlight = false;
      });
  }

  function refreshInferenceBarProvenanceDisplay() {
    if (lastInferenceStatus == null) {
      return;
    }
    renderInferenceStatusBar();
    if (lastPolicy && symbolSelect.value) {
      renderPolicy(lastPolicy, symbolSelect.value, lastEntryHint);
    }
  }

  function refreshInferencePanel() {
    const symbol = symbolSelect.value;
    if (!symbol) {
      return Promise.resolve();
    }
    if (inferenceFetchAbortController) {
      inferenceFetchAbortController.abort();
    }
    inferenceFetchAbortController = new AbortController();
    const fetchController = inferenceFetchAbortController;
    loadInferenceRequestSeq = loadInferenceRequestSeq + 1;
    const requestId = loadInferenceRequestSeq;
    activeLoadInferenceRequestId = requestId;
    const limit = limitInput.value ? parseInt(limitInput.value, 10) : config.defaultLimit;
    return loadInference(symbol, limit, fetchController.signal, requestId)
      .finally(() => {
        if (inferenceFetchAbortController === fetchController) {
          inferenceFetchAbortController = null;
        }
      });
  }


  function getBarsLimit() {
    return limitInput.value ? parseInt(limitInput.value, 10) : config.defaultLimit;
  }

  function getExitStackForSymbol(symbol) {
    return exitStackBySymbol[symbol] || null;
  }

  function exitStackUsesSignOnlyRenew(symbol) {
    const exitStack = getExitStackForSymbol(symbol);
    if (!exitStack || !exitStack.mode) {
      return false;
    }
    return String(exitStack.mode) === 'rolling_h_renew_sign_only';
  }

  function exitPolicyIsSignOnly(exitPolicy) {
    return Boolean(
      exitPolicy
      && exitPolicy.mode === 'rolling_h_renew_sign_only',
    );
  }

  function exitPolicyIsRenderable(exitPolicy) {
    if (!exitPolicy) {
      return false;
    }
    if (exitPolicyIsSignOnly(exitPolicy)) {
      return true;
    }
    return exitPolicy.close_probability != null;
  }


  let refreshExitPolicyRequestSeq = 0;

  function resolveRenewSegmentEvaluatedForExitPolicy(openPos) {
    let renewSegment = openPositionRenewSegmentEvaluated(openPos);
    if (
      lastExitPolicy
      && lastExitPolicy.last_renew_segment_evaluated != null
    ) {
      const exitPolicyRenewSegment = Number(lastExitPolicy.last_renew_segment_evaluated);
      if (exitPolicyRenewSegment > renewSegment) {
        renewSegment = exitPolicyRenewSegment;
      }
    }
    return renewSegment;
  }


  function formatSignOnlyExitReason(exitReason) {
    const reason = String(exitReason || '');
    if (reason === 'before_min_hold') {
      return 'до min hold';
    }
    if (reason === 'between_renew_checkpoints') {
      return 'между checkpoint';
    }
    if (reason === 'sign_valid_renewed') {
      return 'знак OK · renew';
    }
    if (reason === 'sign_flip_at_checkpoint') {
      return 'разворот pred';
    }
    if (!reason) {
      return '—';
    }
    return reason;
  }

  function resolveSignOnlyRenewUi(lastExitPolicy, metrics) {
    if (lastExitPolicy && lastExitPolicy.suggest_close) {
      return {
        phase: 'close',
        stateLabel: 'разворот pred',
        stateClass: 'renew-state-close',
        progressClass: 'renew-close',
        checkpointHint: 'flip @ checkpoint — CLOSE',
        infoHtml: (
          '<div class="trade-journal-alert trade-journal-alert-close">'
          + '⏹ Pred flip @ checkpoint — рассмотри выход (не renew)'
          + '</div>'
        ),
      };
    }
    if (lastExitPolicy && lastExitPolicy.exit_reason === 'sign_valid_renewed') {
      const segmentNumber = lastExitPolicy.current_renew_segment != null
        ? Number(lastExitPolicy.current_renew_segment)
        : null;
      const renewText = segmentNumber != null
        ? `✓ Renew OK — сегмент #${segmentNumber}, pred в сторону сделки · держим`
        : '✓ Renew OK — pred в сторону сделки · держим';
      return {
        phase: 'renewed',
        stateLabel: 'знак OK · renew',
        stateClass: 'renew-state-renewed',
        progressClass: 'renew-ok',
        checkpointHint: 'renew OK · новый сегмент',
        infoHtml: `<div class="trade-journal-info">${renewText}</div>`,
      };
    }
    const atCheckpoint = lastExitPolicy && lastExitPolicy.at_renew_checkpoint != null
      ? Boolean(lastExitPolicy.at_renew_checkpoint)
      : Boolean(metrics && (metrics.pending_segment_eval || metrics.at_renew_checkpoint));
    if (atCheckpoint) {
      return {
        phase: 'checkpoint',
        stateLabel: 'checkpoint',
        stateClass: 'renew-state-checkpoint',
        progressClass: 'renew-checkpoint',
        checkpointHint: 'checkpoint — проверка pred…',
        infoHtml: '',
      };
    }
    const barsUntilCheckpoint = metrics && metrics.bars_until_checkpoint != null
      ? Number(metrics.bars_until_checkpoint)
      : null;
    const betweenLabel = lastExitPolicy && lastExitPolicy.exit_reason === 'between_renew_checkpoints'
      ? 'между checkpoint'
      : (metrics && Number(metrics.bars_elapsed) < Number(metrics.min_hold_steps || 0)
        ? 'до min hold'
        : 'между checkpoint');
    return {
      phase: 'counting',
      stateLabel: betweenLabel,
      stateClass: '',
      progressClass: '',
      checkpointHint: barsUntilCheckpoint != null
        ? `до checkpoint: ${barsUntilCheckpoint} bar`
        : 'до checkpoint',
      infoHtml: '',
    };
  }

  function openPositionRenewSegmentEvaluated(openPos) {
    if (!openPos || openPos.last_renew_segment_evaluated == null) {
      return -1;
    }
    return Number(openPos.last_renew_segment_evaluated);
  }


  function renderSignOnlyRenewMetrics(openPos, m, symbol) {
    const deployHorizon = resolveDeployEvalHorizon(openPos, symbol);
    const intervalSteps = m.renew_interval_steps != null
      ? Number(m.renew_interval_steps)
      : Number(m.eval_horizon_steps);
    const displayBarsElapsed = resolveDisplayBarsElapsed(openPos);
    const totalBarsElapsed = displayBarsElapsed != null
      ? displayBarsElapsed
      : Number(m.bars_elapsed);
    const segmentBars = m.segment_bars_elapsed != null
      ? Number(m.segment_bars_elapsed)
      : totalBarsElapsed;
    const segmentsCompleted = m.segments_completed != null
      ? Number(m.segments_completed)
      : 0;
    const renewUi = resolveSignOnlyRenewUi(lastExitPolicy, m);
    const renewState = renewUi.stateLabel;
    const stateClass = renewUi.stateClass;
    const progressClass = renewUi.progressClass;
    const checkpointHint = renewUi.checkpointHint;
    return `
        <div class="trade-journal-metrics trade-journal-sign-only-metrics">
          <span>Exit: <strong>sign_only renew @ ${deployHorizon}</strong></span>
          <span>Всего баров: <strong>${totalBarsElapsed}</strong></span>
          <span>Сегмент: <strong>${segmentBars}</strong> / ${intervalSteps}</span>
          <span>Продлений: <strong>${segmentsCompleted}</strong></span>
          <span>${checkpointHint}</span>
          <span>state: <strong class="${stateClass}">${renewState}</strong></span>
        </div>
        <div class="trade-journal-progress" title="${Number(m.progress_pct).toFixed(1)}% сегмента">
          <div class="trade-journal-progress-bar ${progressClass}" style="width: ${Math.min(100, Number(m.progress_pct))}%"></div>
        </div>
    `;
  }

  function renderExitPolicyCard(exitPolicy, displayBarsElapsed) {
    if (!exitPolicy) return '';
    if (exitPolicy.mode === 'rolling_h_renew_sign_only') {
      const action = String(exitPolicy.action || 'hold').toUpperCase();
      const runLabel = exitPolicy.run_label || 'rolling_h_renew_sign_only';
      const barsHeld = displayBarsElapsed != null
        ? displayBarsElapsed
        : (exitPolicy.bars_held != null ? exitPolicy.bars_held : '—');
      const minHold = exitPolicy.min_hold_steps != null ? exitPolicy.min_hold_steps : '—';
      const predLinear = exitPolicy.pred_eval_linear != null
        ? formatPct(Number(exitPolicy.pred_eval_linear) * 100)
        : '—';
      const reason = formatSignOnlyExitReason(exitPolicy.exit_reason);
      const exitEvalHorizon = exitPolicy.eval_horizon
        ? String(exitPolicy.eval_horizon)
        : (exitPolicy.configured_eval_horizon
          ? String(exitPolicy.configured_eval_horizon)
          : 'x32');
      let actionClass = 'exit-policy-hold';
      let actionLabel = `Exit sign_only @ ${exitEvalHorizon}: ${action}`;
      if (action === 'CLOSE') {
        actionClass = 'exit-policy-close-flip';
        actionLabel = `Exit sign_only @ ${exitEvalHorizon}: CLOSE · pred flip`;
      } else if (exitPolicy.exit_reason === 'sign_valid_renewed') {
        actionClass = 'exit-policy-renewed';
        actionLabel = `Exit sign_only @ ${exitEvalHorizon}: HOLD · renew OK`;
      }
      return `
      <div class="exit-policy-card ${actionClass}">
        <div class="exit-policy-action">${actionLabel}</div>
        <div class="exit-policy-meta">
          <span>pred: <strong>${predLinear}</strong></span>
          <span>reason: <strong>${reason}</strong></span>
          <span>stack: <strong>${runLabel}</strong></span>
          <span>бары: <strong>${barsHeld}</strong> (min ${minHold})</span>
        </div>
      </div>
    `;
    }
    if (exitPolicy.close_probability == null) return '';

    const pClose = Number(exitPolicy.close_probability);
    const threshold = Number(exitPolicy.close_probability_threshold);
    const pClosePct = Number.isFinite(pClose) ? (pClose * 100).toFixed(1) : '—';
    const thresholdPct = Number.isFinite(threshold) ? (threshold * 100).toFixed(1) : '—';
    const action = String(exitPolicy.action || 'hold').toUpperCase();
    const runLabel = exitPolicy.run_label || '—';
    const minHold = exitPolicy.min_hold_steps != null ? exitPolicy.min_hold_steps : '—';
    const barsHeld = displayBarsElapsed != null
      ? displayBarsElapsed
      : (exitPolicy.bars_held != null ? exitPolicy.bars_held : '—');
    let actionClass = 'exit-policy-hold';
    if (action === 'CLOSE') actionClass = 'exit-policy-close';

    return `
      <div class="exit-policy-card ${actionClass}">
        <div class="exit-policy-action">Exit GBM: ${action}</div>
        <div class="exit-policy-meta">
          <span>P(close): <strong>${pClosePct}%</strong> / порог ${thresholdPct}%</span>
          <span>stack: <strong>${runLabel}</strong></span>
          <span>бары: <strong>${barsHeld}</strong> (min ${minHold})</span>
        </div>
      </div>
    `;
  }


  function syncJournalBarsElapsedFromState(state) {
    const openPos = state.open_position;
    if (
      openPos
      && openPos.metrics
      && openPos.metrics.bars_elapsed != null
      && openPos.entry_start_trade_id != null
    ) {
      advanceJournalBarsElapsed(
        openPos.entry_start_trade_id,
        Number(openPos.metrics.bars_elapsed),
      );
      return;
    }
    if (!openPos) {
      resetJournalBarsElapsedCache();
    }
  }

  function applyJournalState(state, symbol) {
    syncJournalBarsElapsedFromState(state);
    const openPos = state.open_position;
    if (
      openPos
      && openPos.metrics
      && openPos.entry_start_trade_id != null
      && lastJournalBarsElapsed != null
      && Number(openPos.metrics.bars_elapsed) < lastJournalBarsElapsed
    ) {
      scheduleJournalRefetchForBarsAdvance(symbol);
    }
    renderTradeJournal(state, symbol);
    if (
      openPos
      && openPos.symbol_id === symbol
      && openPos.entry_start_trade_id != null
      && lastJournalBarsElapsed == null
    ) {
      pollJournalBarsElapsed(symbol, Number(openPos.entry_start_trade_id));
    }
  }


  function rerenderTradeJournalIfOpen(symbol) {
    if (!lastJournalState || !journalHasOpenPosition || !symbol) {
      return;
    }
    const openPos = lastJournalState.open_position;
    if (!openPos || openPos.symbol_id !== symbol) {
      return;
    }
    renderTradeJournal(lastJournalState, symbol);
  }

  function refreshTradeJournal(symbol) {
    if (!symbol || refreshJournalInFlight) {
      return Promise.resolve();
    }
    refreshJournalInFlight = true;
    return fetchAndApplyTradeJournal(symbol)
      .then(state => {
        const openPos = state.open_position;
        if (!openPos || openPos.symbol_id !== symbol) {
          resetJournalBarsElapsedCache();
        }
      })
      .catch(e => {
        tradeJournalContent.innerHTML = `<div class="trade-journal-loading">Ошибка журнала: ${parseErrorDetail(e.message)}</div>`;
      })
      .finally(() => {
        refreshJournalInFlight = false;
      });
  }

  function refreshJournalBarsElapsedOnly() {
    const symbol = symbolSelect.value;
    if (!symbol || !journalHasOpenPosition || !lastJournalState) {
      return Promise.resolve();
    }
    const openPos = lastJournalState.open_position;
    if (
      !openPos
      || openPos.symbol_id !== symbol
      || openPos.entry_start_trade_id == null
    ) {
      return Promise.resolve();
    }
    return pollJournalBarsElapsed(symbol, Number(openPos.entry_start_trade_id));
  }

  function renderTradeJournal(state, symbol) {
    lastJournalState = state;
    const openPos = state.open_position;
    journalHasOpenPosition = Boolean(openPos);
    const closedTrades = state.closed_trades || [];
    const totalPnl = state.total_realized_pnl_usd;
    const closedCount = state.closed_trades_count || 0;
    const hasOpen = openPos && openPos.side;
    const cashBalance = state.cash_balance_usd != null
      ? Number(state.cash_balance_usd)
      : (tradingInitialBalanceUsd + Number(totalPnl || 0));

    tradeJournalTotals.textContent = `Balance: ${formatUsd(cashBalance)} · Realized: ${formatUsd(totalPnl)} · Closed: ${closedCount}`;

    if (hasOpen && openPos.daemon_last_exit_policy) {
      lastExitPolicy = openPos.daemon_last_exit_policy;
    } else if (!hasOpen) {
      lastExitPolicy = null;
    }

    const sideLabel = hasOpen ? openPos.side.toUpperCase() : 'FLAT';
    const sideClass = hasOpen ? (openPos.side === 'long' ? 'position-long' : 'position-short') : 'position-flat';

    let metricsHtml = '';
    let infoHtml = '';
    if (hasOpen && openPos.metrics) {
      const m = openPos.metrics;
      const displayBarsElapsed = resolveDisplayBarsElapsed(openPos);
      const progressClass = m.at_target_horizon ? 'at-target' : '';
      const evalHorizonLabel = resolveDeployEvalHorizon(openPos, symbol);
      const signOnlyRenew = exitStackUsesSignOnlyRenew(symbol) || Boolean(m.sign_only_renew);
      const renewUi = signOnlyRenew ? resolveSignOnlyRenewUi(lastExitPolicy, m) : null;
      if (renewUi && renewUi.infoHtml) {
        infoHtml = renewUi.infoHtml;
      }
      const mfeLine = m.mfe_net_return_pct != null
        ? `<span>MFE: <strong class="${pnlClass(m.mfe_pnl_usd)}">${formatPct(m.mfe_net_return_pct)} (${formatUsd(m.mfe_pnl_usd)})</strong></span>`
        : '';
      const maeLine = m.mae_net_return_pct != null
        ? `<span>MAE: <strong class="${pnlClass(m.mae_pnl_usd)}">${formatPct(m.mae_net_return_pct)} (${formatUsd(m.mae_pnl_usd)})</strong></span>`
        : '';
      const givebackLine = m.giveback_net_return_pct != null
        ? `<span>Giveback: <strong class="${pnlClass(-m.giveback_pnl_usd)}">${formatPct(m.giveback_net_return_pct)} (${formatUsd(m.giveback_pnl_usd)})</strong></span>`
        : '';
      const renewMetricsHtml = signOnlyRenew
        ? renderSignOnlyRenewMetrics(openPos, m, symbol)
        : `
        <div class="trade-journal-metrics">
          <span>Бары: <strong>${m.bars_elapsed}</strong> / ${m.eval_horizon_steps}</span>
          <span>Осталось: <strong>${m.bars_remaining}</strong></span>
          <span>Entry: <strong>${Number(openPos.entry_price).toFixed(2)}</strong></span>
          <span>Mark: <strong>${Number(m.mark_price).toFixed(2)}</strong></span>
          <span>Unrealized: <strong class="${pnlClass(m.unrealized_pnl_usd)}">${formatPct(m.unrealized_net_return_pct)} (${formatUsd(m.unrealized_pnl_usd)})</strong></span>
          ${mfeLine}
          ${maeLine}
          ${givebackLine}
          <span>Notional: <strong>$${Number(openPos.notional_usd).toFixed(2)}</strong></span>
        </div>
        <div class="trade-journal-progress" title="${m.progress_pct.toFixed(1)}%">
          <div class="trade-journal-progress-bar ${progressClass}" style="width: ${Math.min(100, m.progress_pct)}%"></div>
        </div>
      `;
      metricsHtml = `
        ${renderExitPolicyCard(lastExitPolicy, displayBarsElapsed)}
        ${renderEntryPredictionMetrics(openPos, m, symbol)}
        ${renewMetricsHtml}
        <div class="trade-journal-metrics">
          <span>Entry: <strong>${Number(openPos.entry_price).toFixed(2)}</strong></span>
          <span>Mark: <strong>${Number(m.mark_price).toFixed(2)}</strong></span>
          <span>eval: <strong>${evalHorizonLabel}</strong></span>
          <span>Unrealized: <strong class="${pnlClass(m.unrealized_pnl_usd)}">${formatPct(m.unrealized_net_return_pct)} (${formatUsd(m.unrealized_pnl_usd)})</strong></span>
          ${mfeLine}
          ${maeLine}
          ${givebackLine}
          <span>Notional: <strong>$${Number(openPos.notional_usd).toFixed(2)}</strong></span>
        </div>
      `;
    } else if (hasOpen) {
      const entryPrice = Number(openPos.entry_price).toFixed(2);
      const evalHorizonLabel = openPos.eval_horizon
        ? String(openPos.eval_horizon)
        : (openPos.eval_horizon_steps ? `x${openPos.eval_horizon_steps}` : '—');
      metricsHtml = `
        <div class="trade-journal-metrics">
          <span>Entry: <strong>${entryPrice}</strong></span>
          <span>Horizon: <strong>${evalHorizonLabel}</strong></span>
          <span>Notional: <strong>$${Number(openPos.notional_usd).toFixed(2)}</strong></span>
          <span>Mark / бары: <strong>загрузка…</strong></span>
        </div>
      `;
    } else {
      const hintRecommended = lastEntryHint && lastEntryHint.recommended_action
        ? String(lastEntryHint.recommended_action).toUpperCase()
        : null;
      metricsHtml = `
        <div class="trade-journal-metrics">
          <span>Policy: <strong>${lastPolicy && lastPolicy.action ? String(lastPolicy.action).toUpperCase() : '—'}</strong></span>
          <span>eval (policy): <strong>${lastPolicy && lastPolicy.eval_horizon ? lastPolicy.eval_horizon : '—'}</strong></span>
          ${hintRecommended ? `<span>entry hint: <strong>${hintRecommended}</strong></span>` : ''}
        </div>
      `;
      if (lastEntryHint && lastEntryHint.entry_blocked) {
        const blockReason = lastEntryHint.block_reason
          ? String(lastEntryHint.block_reason)
          : 'uncertainty @ eval horizon';
        const hintLabel = lastEntryHint.hint_mode === 'hybrid_gate_snr'
          ? 'Hybrid gate'
          : 'SNR gate';
        infoHtml = `<div class="trade-journal-info">⏸ ${hintLabel}: ${blockReason}</div>`;
      }
    }

    const equityCurveHtml = renderEquityCurveSvg(state.equity_curve);

    let historyHtml = '';
    if (closedTrades.length > 0) {
      const rows = closedTrades.map(t => {
        const mfeCell = t.mfe_net_return_pct != null
          ? `<td class="${pnlClass(t.mfe_pnl_usd)}">${formatPct(t.mfe_net_return_pct)}</td>`
          : '<td>—</td>';
        const givebackCell = t.giveback_net_return_pct != null
          ? `<td class="${pnlClass(-t.giveback_pnl_usd)}">${formatPct(t.giveback_net_return_pct)}</td>`
          : '<td>—</td>';
        return `
        <tr>
          <td>${String(t.side).toUpperCase()}</td>
          <td>${Number(t.entry_price).toFixed(1)} → ${Number(t.exit_price).toFixed(1)}</td>
          <td class="${pnlClass(t.realized_pnl_usd)}">${formatUsd(t.realized_pnl_usd)}</td>
          ${mfeCell}
          ${givebackCell}
        </tr>
      `;
      }).join('');
      historyHtml = `
        <div class="trade-journal-history">
          <h4>Последние сделки</h4>
          <table>
            <thead><tr><th>Side</th><th>Entry → Exit</th><th>PnL $</th><th>MFE</th><th>Giveback</th></tr></thead>
            <tbody>${rows}</tbody>
          </table>
        </div>
      `;
    }

    tradeJournalContent.innerHTML = `
      <div class="trade-journal-card ${sideClass}">
        <div class="trade-journal-side">${sideLabel}${hasOpen ? ` · ${openPos.symbol_id}` : ''}</div>
        ${infoHtml}
        ${metricsHtml}
      </div>
      ${equityCurveHtml}
      ${historyHtml}
    `;

    isFirstJournalLoad = false;
  }


  function isDowLevel(value) {
    return typeof value === 'string' && /^Уровень \d+$/.test(value);
  }

  function parseDowLevel(value) {
    const m = value && value.match(/^Уровень (\d+)$/);
    return m ? parseInt(m[1], 10) : 0;
  }

  function initDropdowns() {
    return Promise.all([
      API.symbols().then(symbols => {
        symbolSelect.innerHTML = '';
        symbols.forEach(s => {
          const opt = document.createElement('option');
          opt.value = s.id;
          opt.textContent = s.name;
          symbolSelect.appendChild(opt);
        });
      }),
      API.scales().then(scales => {
        return API.dowLevels().then(dowLevels => {
          scaleSelect.innerHTML = '';
          scales.forEach(s => {
            const opt = document.createElement('option');
            opt.value = s;
            opt.textContent = s;
            scaleSelect.appendChild(opt);
          });
          dowLevels.forEach(l => {
            const opt = document.createElement('option');
            opt.value = l;
            opt.textContent = l;
            scaleSelect.appendChild(opt);
          });
          const defaultScale = config.defaultScale != null ? String(config.defaultScale) : 'x32';
          if (scales.includes(defaultScale)) {
            scaleSelect.value = defaultScale;
          }
        });
      }),
    ]).catch(e => {
      setStatus('Ошибка загрузки списков: ' + e.message, true);
    });
  }

  /**
   * Строит данные свечей и объёмов в одном порядке (строго по времени, дубли по time склеены).
   * Возвращает { candleData, volumeData } — volumeData[i] соответствует candleData[i].
   * При встрече non-finite или некорректных значений — выбрасывает ошибку.
   */
  function barsToCandleAndVolumeData(bars, diagnosticsLabel = '') {
    const raw = bars.map((b, index) => {
      const time = b.start_timestamp_ms != null ? Math.floor(b.start_timestamp_ms / 1000) : null;
      const open = b.open_price != null ? Number(b.open_price) : null;
      const high = b.high_price != null ? Number(b.high_price) : null;
      const low = b.low_price != null ? Number(b.low_price) : null;
      const close = b.close_price != null ? Number(b.close_price) : null;
      const totalVolume = b.total_volume != null ? Number(b.total_volume) : null;
      const buyPctRaw = b.buy_volume_percent != null ? Number(b.buy_volume_percent) : null;
      if (time == null || !Number.isFinite(time)) {
        throw new Error(`Бар #${index}: неверный time (start_timestamp_ms=${b.start_timestamp_ms})`);
      }
      if (open == null || !Number.isFinite(open) || high == null || !Number.isFinite(high) ||
          low == null || !Number.isFinite(low) || close == null || !Number.isFinite(close)) {
        throw new Error(
          `Бар #${index} (start_trade_id=${b.start_trade_id}): ожидаются конечные open/high/low/close, ` +
          `получено open=${b.open_price} high=${b.high_price} low=${b.low_price} close=${b.close_price}`
        );
      }
      if (totalVolume == null || !Number.isFinite(totalVolume) || totalVolume < 0) {
        throw new Error(`Бар #${index} (start_trade_id=${b.start_trade_id}): неверный total_volume=${b.total_volume}`);
      }
      if (buyPctRaw != null && (!Number.isFinite(buyPctRaw) || buyPctRaw < 0 || buyPctRaw > 1)) {
        throw new Error(`Бар #${index} (start_trade_id=${b.start_trade_id}): неверный buy_volume_percent=${b.buy_volume_percent}`);
      }
      const buyPct = buyPctRaw != null ? buyPctRaw : 0;
      return { time, open, high, low, close, totalVolume, buyPct, sourceIndex: index };
    });
    raw.sort((a, b) => a.time - b.time);
    const duplicateTimeGroups = [];
    const zeroVolumeIndices = [];
    let currentGroupStart = 0;
    while (currentGroupStart < raw.length) {
      const currentTime = raw[currentGroupStart].time;
      const groupIndices = [raw[currentGroupStart].sourceIndex];
      let j = currentGroupStart + 1;
      while (j < raw.length && raw[j].time === currentTime) {
        groupIndices.push(raw[j].sourceIndex);
        j += 1;
      }
      if (groupIndices.length > 1) {
        duplicateTimeGroups.push({ time: currentTime, sourceIndices: groupIndices });
      }
      currentGroupStart = j;
    }
    for (let i = 0; i < raw.length; i++) {
      if (raw[i].totalVolume === 0) zeroVolumeIndices.push(raw[i].sourceIndex);
    }

    if (diagnosticsLabel) {
      const duplicateIndices = duplicateTimeGroups.flatMap((g) => g.sourceIndices);
      if (duplicateTimeGroups.length > 0) {
        console.warn(
          `[${diagnosticsLabel}] Duplicate start_timestamp_ms/time detected`,
          {
            duplicateStartTimestampCount: duplicateTimeGroups.length,
            duplicateIndicesCount: duplicateIndices.length,
            duplicateGroups: duplicateTimeGroups,
            duplicateIndices,
          },
        );
      }
      if (zeroVolumeIndices.length > 0) {
        console.warn(
          `[${diagnosticsLabel}] Bars with total_volume == 0 detected`,
          {
            zeroVolumeCount: zeroVolumeIndices.length,
            zeroVolumeIndices,
          },
        );
      }
      if (duplicateTimeGroups.length === 0 && zeroVolumeIndices.length === 0) {
        console.info(
          `[${diagnosticsLabel}] Diagnostics: no duplicate start_timestamp_ms/time and no total_volume == 0 bars`,
        );
      }
    }

    const candleData = [];
    const volumeData = [];
    for (let i = 0; i < raw.length; i++) {
      const cur = raw[i];
      if (candleData.length > 0 && candleData[candleData.length - 1].time === cur.time) {
        const lastC = candleData[candleData.length - 1];
        lastC.high = Math.max(lastC.high, cur.high);
        lastC.low = Math.min(lastC.low, cur.low);
        lastC.close = cur.close;
        const lastV = volumeData[volumeData.length - 1];
        const buySum = lastV.buyVolumeSum + cur.buyPct * cur.totalVolume;
        const totalSum = lastV.totalVolumeSum + cur.totalVolume;
        lastV.totalVolumeSum = totalSum;
        lastV.buyVolumeSum = buySum;
      } else {
        candleData.push({ time: cur.time, open: cur.open, high: cur.high, low: cur.low, close: cur.close });
        volumeData.push({ totalVolumeSum: cur.totalVolume, buyVolumeSum: cur.buyPct * cur.totalVolume });
      }
    }
    const volumeDataNormalized = volumeData.map((v) => {
      const total = v.totalVolumeSum;
      const buyPct = total > 0 ? v.buyVolumeSum / total : 0;
      const volume_delta = 2 * v.buyVolumeSum - total;
      return {
        buy_volume_percent: buyPct,
        sell_volume_percent: 1 - buyPct,
        total_volume: total,
        volume_delta,
      };
    });
    return { candleData, volumeData: volumeDataNormalized };
  }

  function getCvdWindowSize() {
    const raw = cvdWindowSelect && cvdWindowSelect.value ? cvdWindowSelect.value : CVD_WINDOW_DEFAULT;
    if (!raw.startsWith('x')) return parseInt(CVD_WINDOW_DEFAULT.slice(1), 10);
    return parseInt(raw.slice(1), 10) || 512;
  }

  function computeCumulativeWithWindow(volumeData, windowSize) {
    const n = volumeData.length;
    if (n === 0) return;
    const W = Math.max(1, windowSize);
    let sum = volumeData[0].volume_delta;
    volumeData[0].cumulative_volume_delta = sum;
    for (let i = 1; i < n; i++) {
      sum += volumeData[i].volume_delta;
      if (i >= W) sum -= volumeData[i - W].volume_delta;
      volumeData[i].cumulative_volume_delta = sum;
    }
  }

  /**
   * Строит сегменты между экстремумами на участках между пересечениями нуля.
   * 1) Находим индексы пересечения нуля (<=0 -> >0 и >=0 -> <0).
   * 2) Между соседними такими индексами берём глобальный экстремум по модулю:
   *    - если значения > 0 — максимум;
   *    - если значения < 0 — минимум.
   * 3) Получаем последовательность точек (index, value) с чередующимся знаком и соединяем соседние:
   *    - зелёные: из отрицательного в (>= 0);
   *    - красные: из положительного в (<= 0).
   */
  function computeExtremaSegments(volumeData) {
    extremaSegments = { green: [], red: [] };
    const n = volumeData.length;
    if (n === 0) return;

    const sign = (x) => (x > 0 ? 1 : x < 0 ? -1 : 0);

    // Индексы, где происходит пересечение нуля (границы участков).
    const boundaries = [0];
    for (let i = 1; i < n; i++) {
      const prev = volumeData[i - 1].cumulative_volume_delta || 0;
      const cur = volumeData[i].cumulative_volume_delta || 0;
      const sPrev = sign(prev);
      const sCur = sign(cur);
      if ((sPrev <= 0 && sCur > 0) || (sPrev >= 0 && sCur < 0)) {
        boundaries.push(i);
      }
    }
    boundaries.push(n);

    const points = [];

    for (let b = 0; b < boundaries.length - 1; b++) {
      const start = boundaries[b];
      const end = boundaries[b + 1] - 1;
      if (start > end) continue;

      let idx = -1;
      let val = 0;

      // Ищем первый ненулевой, чтобы определить знак участка.
      for (let i = start; i <= end; i++) {
        const v = volumeData[i].cumulative_volume_delta || 0;
        if (v !== 0) {
          idx = i;
          val = v;
          break;
        }
      }
      if (idx === -1) {
        // Весь участок в нуле — пропускаем.
        continue;
      }

      const s = sign(val);
      let bestIndex = idx;
      let bestValue = val;

      if (s > 0) {
        // Участок выше нуля: ищем максимум.
        for (let i = idx + 1; i <= end; i++) {
          const v = volumeData[i].cumulative_volume_delta || 0;
          if (v > bestValue) {
            bestValue = v;
            bestIndex = i;
          }
        }
      } else if (s < 0) {
        // Участок ниже нуля: ищем минимум.
        for (let i = idx + 1; i <= end; i++) {
          const v = volumeData[i].cumulative_volume_delta || 0;
          if (v < bestValue) {
            bestValue = v;
            bestIndex = i;
          }
        }
      }

      points.push({ index: bestIndex, value: bestValue });
    }

    // Строим сегменты между соседними точками.
    for (let i = 0; i < points.length - 1; i++) {
      const a = points[i];
      const b = points[i + 1];

      if (a.value < 0 && b.value >= 0) {
        extremaSegments.green.push({
          indexFrom: a.index,
          valueFrom: a.value,
          indexTo: b.index,
          valueTo: b.value,
        });
      } else if (a.value > 0 && b.value <= 0) {
        extremaSegments.red.push({
          indexFrom: a.index,
          valueFrom: a.value,
          indexTo: b.index,
          valueTo: b.value,
        });
      }
    }
  }

  function isExtremaLinesEnabled() {
    return Boolean(extremaLinesEnabledCheck && extremaLinesEnabledCheck.checked);
  }

  function refreshExtremaOverlays() {
    if (volumeDataByCandleIndex.length === 0) {
      extremaSegments = { green: [], red: [] };
      removeExtremaLineSeries();
      return;
    }
    if (isExtremaLinesEnabled()) {
      computeExtremaSegments(volumeDataByCandleIndex);
    } else {
      extremaSegments = { green: [], red: [] };
    }
    addExtremaLinesToChart();
    const range = chart && chart.timeScale().getVisibleLogicalRange();
    if (range) drawCumulativeBars(range);
  }

  function isTradeResearchEnabled() {
    return Boolean(tradeResearchEnabledCheck && tradeResearchEnabledCheck.checked);
  }

  function ensureTradeResearchScale() {
    if (!isTradeResearchEnabled()) return true;
    if (scaleSelect.value === tradeResearchScale) return true;
    scaleSelect.value = tradeResearchScale;
    return false;
  }

  function removeTradeResearchLineSeries() {
    if (!chart) return;
    tradeResearchLineSeries.forEach((series) => chart.removeSeries(series));
    tradeResearchLineSeries = [];
    if (tradeResearchMarkerPrimitive != null) {
      tradeResearchMarkerPrimitive.setMarkers([]);
      tradeResearchMarkerPrimitive = null;
    }
  }

  function isTradeResearchEntryPointSegment(segment) {
    return segment.segment_kind === 'entry_point';
  }

  function buildCandleByTimeLookup() {
    const candleByTime = new Map();
    for (const candle of candleDataByIndex) {
      candleByTime.set(candle.time, candle);
    }
    return candleByTime;
  }

  function findBarForStartTradeId(startTradeId) {
    if (barsData.length === 0) {
      return null;
    }
    const targetId = Number(startTradeId);
    if (!Number.isFinite(targetId)) {
      return null;
    }
    let lo = 0;
    let hi = barsData.length - 1;
    let bestIndex = -1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      const barStartId = Number(barsData[mid].start_trade_id);
      if (barStartId <= targetId) {
        bestIndex = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    if (bestIndex < 0) {
      return null;
    }
    const bar = barsData[bestIndex];
    const barEndId = Number(bar.end_trade_id);
    if (!Number.isFinite(barEndId) || targetId > barEndId) {
      return null;
    }
    return bar;
  }

  function candleFromBar(bar, candleByTime) {
    if (!bar) {
      return null;
    }
    const time = Math.floor(Number(bar.start_timestamp_ms) / 1000);
    return candleByTime.get(time) || null;
  }

  function buildStartTradeIdCandleLookup() {
    const candleByTime = buildCandleByTimeLookup();
    const lookup = new Map();
    for (const bar of barsData) {
      const startTradeId = Number(bar.start_trade_id);
      const candle = candleFromBar(bar, candleByTime);
      if (candle) {
        lookup.set(startTradeId, candle);
      }
    }
    return lookup;
  }

  function findCandleForTimestampMs(timestampMs) {
    if (timestampMs == null || candleDataByIndex.length === 0) {
      return null;
    }
    const targetSec = Math.floor(Number(timestampMs) / 1000);
    if (!Number.isFinite(targetSec)) {
      return null;
    }
    let lo = 0;
    let hi = candleDataByIndex.length - 1;
    let bestIndex = -1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      const candleTime = candleDataByIndex[mid].time;
      if (candleTime <= targetSec) {
        bestIndex = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    if (bestIndex < 0) {
      return null;
    }
    return candleDataByIndex[bestIndex];
  }

  function resolveSegmentChartCandle(timestampMs) {
    return findCandleForTimestampMs(timestampMs);
  }

  function positiveFiniteNumber(value) {
    const numberValue = Number(value);
    if (!Number.isFinite(numberValue) || numberValue <= 0) {
      return null;
    }
    return numberValue;
  }

  function requireSegmentPositiveFinitePrice(segment, fieldName) {
    const price = positiveFiniteNumber(segment[fieldName]);
    if (price == null) {
      const sampleIndex = segment.sample_index;
      throw new Error(
        'Trade research segment missing or invalid '
        + fieldName
        + ' (sample_index='
        + sampleIndex
        + '); re-run main.trade_research_export',
      );
    }
    return price;
  }

  function resolveTradeResearchSegmentEntryPrice(segment) {
    return requireSegmentPositiveFinitePrice(segment, 'pred_start_price');
  }

  function resolveTradeResearchSegmentPredTargetClose(segment) {
    return requireSegmentPositiveFinitePrice(segment, 'pred_target_price');
  }

  function resolveSegmentInferenceX1TimestampMs(segment) {
    if (segment.inference_x1_timestamp_ms != null) {
      const value = Number(segment.inference_x1_timestamp_ms);
      if (Number.isFinite(value) && value > 0) {
        return value;
      }
    }
    if (segment.entry_timestamp_ms != null) {
      const fallback = Number(segment.entry_timestamp_ms);
      if (Number.isFinite(fallback) && fallback > 0) {
        return fallback;
      }
    }
    return null;
  }

  function resolveSegmentInferenceEntryClose(segment) {
    if (segment.inference_entry_close != null) {
      const value = Number(segment.inference_entry_close);
      if (Number.isFinite(value) && value > 0) {
        return value;
      }
    }
    return resolveTradeResearchSegmentEntryPrice(segment);
  }

  function formatInferenceX1Utc(timestampMs) {
    if (timestampMs == null) {
      return '?';
    }
    const date = new Date(timestampMs);
    if (Number.isNaN(date.getTime())) {
      return '?';
    }
    return date.toISOString().replace('T', ' ').replace('.000Z', ' UTC');
  }

  function formatTradeResearchLastInferProvenance(provenance) {
    if (provenance == null) {
      return '';
    }
    const timestampMs = Number(provenance.inference_x1_timestamp_ms);
    const entryClose = Number(provenance.inference_entry_close);
    const sampleIndex = provenance.sample_index;
    if (!Number.isFinite(timestampMs) || !Number.isFinite(entryClose)) {
      return '';
    }
    let text =
      `last infer x1: ${formatInferenceX1Utc(timestampMs)} @ ${entryClose.toFixed(2)}`;
    if (sampleIndex != null) {
      text = text + ` (sample ${sampleIndex})`;
    }
    if (provenance.action != null) {
      text = text + ` ${provenance.action}`;
    }
    return text;
  }

  function tradeResearchEntryPointMarkerText(segment) {
    const timestampMs = resolveSegmentInferenceX1TimestampMs(segment);
    const entryClose = resolveSegmentInferenceEntryClose(segment);
    if (timestampMs == null || entryClose == null) {
      return '';
    }
    const utcLabel = formatInferenceX1Utc(timestampMs);
    const shortUtc = utcLabel.length > 16 ? utcLabel.slice(0, 16) : utcLabel;
    return `${shortUtc} @ ${entryClose.toFixed(0)}`;
  }

  function buildTradeResearchEntryPointMarker(segment, entryCandle) {
    const marker = {
      time: entryCandle.time,
      position: segment.action === 'long' ? 'belowBar' : 'aboveBar',
      color: segment.action === 'long' ? '#26a69a' : '#ef5350',
      shape: segment.action === 'long' ? 'arrowUp' : 'arrowDown',
    };
    const markerText = tradeResearchEntryPointMarkerText(segment);
    if (markerText) {
      marker.text = markerText;
    }
    return marker;
  }

  function addTradeResearchLinesToChart() {
    if (!chart || !isTradeResearchEnabled()) {
      removeTradeResearchLineSeries();
      return 0;
    }
    removeTradeResearchLineSeries();
    if (candleDataByIndex.length === 0 || tradeResearchSegments.length === 0) {
      return 0;
    }
    const renderStartedMs = performance.now();
    console.time('[trade-research] js-prep');
    const drawSpecs = [];
    const entryPointMarkers = [];
    let missingEntryCount = 0;
    let missingExitCount = 0;
    for (const segment of tradeResearchSegments) {
      const entryCandle = resolveSegmentChartCandle(segment.entry_timestamp_ms);
      if (entryCandle == null) {
        missingEntryCount = missingEntryCount + 1;
        continue;
      }
      const entryPrice = resolveTradeResearchSegmentEntryPrice(segment);
      const predTargetClose = resolveTradeResearchSegmentPredTargetClose(segment);
      if (isTradeResearchEntryPointSegment(segment)) {
        entryPointMarkers.push(buildTradeResearchEntryPointMarker(segment, entryCandle));
        continue;
      }
      const exitCandle = resolveSegmentChartCandle(segment.exit_timestamp_ms);
      if (exitCandle == null || exitCandle.time <= entryCandle.time) {
        entryPointMarkers.push(buildTradeResearchEntryPointMarker(segment, entryCandle));
        missingExitCount = missingExitCount + 1;
        continue;
      }
      drawSpecs.push({
        entryTimeSec: entryCandle.time,
        exitTimeSec: exitCandle.time,
        entryPrice,
        predTargetClose,
        color: segment.action === 'long' ? '#26a69a' : '#ef5350',
      });
    }
    console.timeEnd('[trade-research] js-prep');
    const LineSeries = LightweightCharts.LineSeries;
    if (!LineSeries) {
      return entryPointMarkers.length;
    }
    const opts = {
      priceScaleId: 'right',
      lineWidth: 2,
      lastValueVisible: false,
      priceLineVisible: false,
    };
    console.time('[trade-research] chart-add-series');
    for (const spec of drawSpecs) {
      const series = chart.addSeries(LineSeries, { ...opts, color: spec.color });
      series.setData([
        { time: spec.entryTimeSec, value: spec.entryPrice },
        { time: spec.exitTimeSec, value: spec.predTargetClose },
      ]);
      tradeResearchLineSeries.push(series);
    }
    console.timeEnd('[trade-research] chart-add-series');
    if (entryPointMarkers.length > 0 && candleSeries && LightweightCharts.createSeriesMarkers) {
      tradeResearchMarkerPrimitive = LightweightCharts.createSeriesMarkers(
        candleSeries,
        entryPointMarkers,
      );
    }
    const renderedSegmentCount = drawSpecs.length + entryPointMarkers.length;
    console.info(
      '[trade-research] render',
      {
        segments: tradeResearchSegments.length,
        rendered: renderedSegmentCount,
        renderedLines: drawSpecs.length,
        renderedEntryPoints: entryPointMarkers.length,
        missingEntry: missingEntryCount,
        missingExit: missingExitCount,
        anchor: 'aligned_coarse_candle_time_pred_start_price_to_pred_target_price',
        durationMs: Math.round(performance.now() - renderStartedMs),
        chartBars: barsData.length,
        chartCandles: candleDataByIndex.length,
      },
    );
    return renderedSegmentCount;
  }

  function getVisibleStartTradeIdRange() {
    if (barsData.length === 0) {
      return { min: null, max: null };
    }
    const lastBar = barsData[barsData.length - 1];
    return {
      min: Number(barsData[0].start_trade_id),
      // Верхняя граница — end_trade_id последней свечи: entry внутри bucket не отрезается.
      max: Number(lastBar.end_trade_id),
    };
  }

  function formatTradeResearchNetPnl(linearSum) {
    const pct = Number(linearSum) * 100.0;
    if (!Number.isFinite(pct)) {
      return '?';
    }
    const sign = pct >= 0.0 ? '+' : '';
    return sign + pct.toFixed(2) + '%';
  }

  function formatBacktestMetricsBlock(label, metrics) {
    if (metrics == null) {
      return '';
    }
    const tradeCount = metrics.trade_count;
    if (tradeCount == null || Number(tradeCount) === 0) {
      return '';
    }
    let block =
      `${label} linear ${formatTradeResearchNetPnl(metrics.net_pnl_sum)} ` +
      `(${tradeCount}`;
    if (metrics.avg_trade_pnl != null) {
      block = block + `, avg ${formatTradeResearchNetPnl(metrics.avg_trade_pnl)}`;
    }
    block = block + ')';
    if (metrics.compounded_return != null) {
      block = block + ` compound ${formatTradeResearchNetPnl(metrics.compounded_return)}`;
    }
    return block;
  }

  function readBacktestMetrics(payload, prefix) {
    const tradeCount = payload[`${prefix}_trade_count`];
    if (tradeCount == null) {
      return null;
    }
    return {
      net_pnl_sum: payload[`${prefix}_net_pnl_sum`],
      trade_count: tradeCount,
      avg_trade_pnl: payload[`${prefix}_avg_trade_pnl`],
      compounded_return: payload[`${prefix}_compounded_return`],
    };
  }

  function loadTradeResearch(symbol, trigger) {
    if (!isTradeResearchEnabled()) {
      tradeResearchSegments = [];
      removeTradeResearchLineSeries();
      return;
    }
    if (scaleSelect.value !== tradeResearchScale) {
      tradeResearchSegments = [];
      removeTradeResearchLineSeries();
      setStatus(`Trade research: выберите масштаб ${tradeResearchScale}`, true);
      guiLog('loadTradeResearch skipped wrong scale', {
        trigger,
        scale: scaleSelect.value,
        expected: tradeResearchScale,
      });
      return;
    }
    if (loadTradeResearchInFlight) {
      pendingTradeResearch = { symbol, trigger };
      guiLog('loadTradeResearch skipped: in flight (queued retry)', {
        trigger,
        activeTradeResearchRequestId,
      });
      return;
    }
    pendingTradeResearch = null;

    const horizonSteps = Number(tradeResearchEvalHorizon.slice(1));
    const visibleRange = getVisibleStartTradeIdRange();
    const requestParams = {
      symbol_id: symbol,
      eval_horizon: tradeResearchEvalHorizon,
      step_bars: horizonSteps,
    };
    if (visibleRange.min != null) {
      requestParams.visible_min_start_trade_id = visibleRange.min;
    }
    if (visibleRange.max != null) {
      requestParams.visible_max_start_trade_id = visibleRange.max;
    }

    loadTradeResearchInFlight = true;
    tradeResearchRequestSeq = tradeResearchRequestSeq + 1;
    const requestId = tradeResearchRequestSeq;
    activeTradeResearchRequestId = requestId;
    const barsGenerationAtRequest = barsDataGeneration;
    const tradeResearchStartedMs = performance.now();
    guiLog('loadTradeResearch start', {
      requestId,
      trigger,
      requestParams,
    });

    API.tradeResearch(requestParams)
      .then((payload) => {
        if (activeTradeResearchRequestId !== requestId) {
          guiLog('loadTradeResearch stale response ignored', { requestId, trigger });
          return;
        }
        if (barsGenerationAtRequest !== barsDataGeneration) {
          guiLog('loadTradeResearch stale bars generation ignored', {
            requestId,
            trigger,
            barsGenerationAtRequest,
            barsDataGeneration,
          });
          return;
        }
        guiLog('loadTradeResearch done', {
          requestId,
          trigger,
          durationMs: Math.round(performance.now() - tradeResearchStartedMs),
          segmentCount: Array.isArray(payload.segments) ? payload.segments.length : 0,
          sampleCount: payload.sample_count,
        });
        tradeResearchSegments = Array.isArray(payload.segments) ? payload.segments : [];
        if (payload.eval_horizon) {
          tradeResearchEvalHorizon = String(payload.eval_horizon);
          tradeResearchScale = tradeResearchEvalHorizon;
          updateTradeResearchUi();
        }
        const renderedSegmentCount = addTradeResearchLinesToChart();
        const artifactHorizon = payload.eval_horizon != null
          ? String(payload.eval_horizon)
          : tradeResearchEvalHorizon;
        const requestedHorizon = payload.requested_eval_horizon != null
          ? String(payload.requested_eval_horizon)
          : artifactHorizon;
        const entryHintMode = payload.entry_hint_mode != null
          ? String(payload.entry_hint_mode)
          : '?';
        const sampleCount = payload.sample_count != null ? payload.sample_count : '?';
        const tradeCount = payload.trade_inference_count != null ? payload.trade_inference_count : '?';
        const entryAllowedCount = payload.entry_allowed_count != null ? payload.entry_allowed_count : '?';
        const realBarsLoaded = payload.real_bars_loaded != null ? payload.real_bars_loaded : null;
        const barsLoaded = payload.bars_loaded != null ? payload.bars_loaded : '?';
        const paddingSite = payload.forward_target_padding_site != null
          ? String(payload.forward_target_padding_site)
          : null;
        let barsContextText = `${barsLoaded} x1`;
        if (paddingSite === 'level0') {
          barsContextText = `${barsLoaded} x1 (raw pad ${payload.forward_target_padding_bars})`;
        } else if (
          realBarsLoaded != null &&
          barsLoaded !== '?' &&
          Number(realBarsLoaded) !== Number(barsLoaded)
        ) {
          barsContextText =
            `${realBarsLoaded} real + ${Number(barsLoaded) - Number(realBarsLoaded)} pad x1`;
        }
        const backtestVisibleNetPnl = payload.grid_backtest_visible_net_pnl_sum;
        const backtestVisibleTradeCount = payload.grid_backtest_visible_trade_count;
        const pnlStride = payload.pnl_stride != null ? payload.pnl_stride : '?';
        const sequentialHybridMetrics = readBacktestMetrics(payload, 'sequential_backtest');
        const sequentialEntryOkMetrics = readBacktestMetrics(payload, 'sequential_entry_ok_backtest');
        const gridHybridMetrics = readBacktestMetrics(payload, 'grid_backtest');
        const gridEntryOkMetrics = readBacktestMetrics(payload, 'grid_entry_ok_backtest');
        const sequentialValMetrics = readBacktestMetrics(payload, 'sequential_backtest_val');
        const gridValMetrics = readBacktestMetrics(payload, 'grid_backtest_val');
        const valSplitAvailable = Boolean(payload.val_split_available);
        const trainSizeRatio = payload.train_size_ratio;
        let statusText =
          `Trade research: ${tradeResearchSegments.length} на графике ` +
          `(${entryAllowedCount} entry ok / ${tradeCount} policy long/short из ${sampleCount} grid @ ${artifactHorizon}, ` +
          `mode ${entryHintMode}, контекст ${barsContextText})`;
        if (requestedHorizon !== artifactHorizon) {
          statusText = statusText + ` [запрос ${requestedHorizon} → artifact ${artifactHorizon}]`;
        } else if (
          tradeResearchAvailableHorizons.length > 0 &&
          !tradeResearchAvailableHorizons.includes(artifactHorizon)
        ) {
          statusText =
            statusText +
            ` [artifact ${artifactHorizon} не в списке: ${tradeResearchAvailableHorizons.join(', ')}]`;
        }
        const seqHybridBlock = formatBacktestMetricsBlock('seq hybrid', sequentialHybridMetrics);
        if (seqHybridBlock) {
          statusText = statusText + `, ${seqHybridBlock} (stride ${pnlStride}, walk-forward)`;
        }
        const seqEntryOkBlock = formatBacktestMetricsBlock('seq entry-ok', sequentialEntryOkMetrics);
        if (seqEntryOkBlock) {
          statusText = statusText + `; ${seqEntryOkBlock}`;
        }
        const gridHybridBlock = formatBacktestMetricsBlock('grid hybrid', gridHybridMetrics);
        if (gridHybridBlock) {
          statusText = statusText + `; ${gridHybridBlock}`;
        }
        const gridEntryOkBlock = formatBacktestMetricsBlock('grid entry-ok', gridEntryOkMetrics);
        if (gridEntryOkBlock) {
          statusText = statusText + `; ${gridEntryOkBlock} (=линии)`;
        }
        if (valSplitAvailable) {
          const valRatioPct = trainSizeRatio != null
            ? Math.round((1.0 - Number(trainSizeRatio)) * 100.0)
            : 25;
          const valSequentialBlock = formatBacktestMetricsBlock('seq val', sequentialValMetrics);
          const valGridBlock = formatBacktestMetricsBlock('grid val', gridValMetrics);
          if (valSequentialBlock || valGridBlock) {
            statusText = statusText + ` | val ~${valRatioPct}% tail`;
            if (valSequentialBlock) {
              statusText = statusText + `: ${valSequentialBlock}`;
            }
            if (valGridBlock) {
              statusText = statusText + `; ${valGridBlock}`;
            }
          }
        } else {
          statusText = statusText + ' | val split: re-export NPZ';
        }
        if (
          backtestVisibleNetPnl != null &&
          backtestVisibleTradeCount != null &&
          tradeResearchSegments.length > 0 &&
          Number(backtestVisibleTradeCount) !== Number(entryAllowedCount)
        ) {
          statusText =
            statusText +
            ` / visible entry-ok linear ${formatTradeResearchNetPnl(backtestVisibleNetPnl)} ` +
            `(${backtestVisibleTradeCount} на графике)`;
        }
        if (payload.sample_selection_note) {
          statusText = statusText + ` [${payload.sample_selection_note}]`;
        }
        const lastInferText = formatTradeResearchLastInferProvenance(
          payload.last_grid_inference_provenance,
        );
        if (lastInferText) {
          statusText = statusText + ` | ${lastInferText}`;
        }
        if (tradeResearchSegments.length === 0 && Number(entryAllowedCount) > 0) {
          statusText = statusText + ' — entry ok, но линии не привязались к свечам';
        } else if (tradeResearchSegments.length === 0 && Number(tradeCount) > 0) {
          statusText = statusText + ' — SNR/gate заблокировали entry или exit вне графика';
        } else if (tradeResearchSegments.length === 0 && Number(sampleCount) > 0) {
          statusText = statusText + ' — нет long/short в видимом окне';
        }
        const renderedLines = tradeResearchLineSeries.length;
        const renderedEntryPoints = renderedSegmentCount - renderedLines;
        if (tradeResearchSegments.length > 0 && renderedSegmentCount === 0) {
          statusText = statusText + ' — линии не привязались к свечам (см. console [trade-research] render)';
        } else if (renderedSegmentCount > 0 && renderedSegmentCount < tradeResearchSegments.length) {
          statusText = statusText + ` — нарисовано ${renderedSegmentCount}/${tradeResearchSegments.length}`;
          if (renderedEntryPoints > 0) {
            statusText = statusText + ` (${renderedLines} линий, ${renderedEntryPoints} маркеров)`;
          } else {
            statusText = statusText + ' линий';
          }
        }
        setStatus(statusText);
      })
      .catch((error) => {
        if (activeTradeResearchRequestId !== requestId) {
          return;
        }
        if (barsGenerationAtRequest !== barsDataGeneration) {
          return;
        }
        guiLog('loadTradeResearch error', {
          requestId,
          trigger,
          durationMs: Math.round(performance.now() - tradeResearchStartedMs),
          message: error.message,
        });
        tradeResearchSegments = [];
        removeTradeResearchLineSeries();
        setStatus('Trade research: ' + parseErrorDetail(error.message), true);
      })
      .finally(() => {
        if (activeTradeResearchRequestId === requestId) {
          loadTradeResearchInFlight = false;
          if (pendingTradeResearch != null) {
            const pending = pendingTradeResearch;
            pendingTradeResearch = null;
            if (symbolSelect.value === pending.symbol) {
              guiLog('loadTradeResearch retry after inFlight', {
                requestId,
                trigger: pending.trigger,
              });
              loadTradeResearch(pending.symbol, `${pending.trigger}:retry`);
            }
          }
        }
        guiLog('loadTradeResearch finished', {
          requestId,
          trigger,
          durationMs: Math.round(performance.now() - tradeResearchStartedMs),
          inFlight: loadTradeResearchInFlight,
        });
      });
  }

  function scheduleTradeResearch(symbol, trigger) {
    if (!symbol) {
      return;
    }
    if (symbolSelect.value !== symbol) {
      return;
    }
    loadTradeResearch(symbol, trigger);
  }

  /** Удаляет серии линий экстремумов с графика и обновляет extremaLineSeries. */
  function removeExtremaLineSeries() {
    if (!chart) return;
    extremaLineSeries.forEach((s) => chart.removeSeries(s));
    extremaLineSeries = [];
  }

  /**
   * Добавляет на основной ценовой график линии сегментов экстремумов (по времени и цене close).
   * Требует candleDataByIndex и заполненный extremaSegments.
   */
  function addExtremaLinesToChart() {
    if (!chart || candleDataByIndex.length === 0) return;
    removeExtremaLineSeries();
    if (!isExtremaLinesEnabled()) return;
    const LineSeries = LightweightCharts.LineSeries;
    if (!LineSeries) return;
    const opts = {
      priceScaleId: 'right',
      lineWidth: 2,
      lastValueVisible: false,
      priceLineVisible: false,
    };
    for (const seg of extremaSegments.green) {
      const cFrom = candleDataByIndex[seg.indexFrom];
      const cTo = candleDataByIndex[seg.indexTo];
      if (!cFrom || !cTo) continue;
      const series = chart.addSeries(LineSeries, { ...opts, color: '#26a69a' });
      series.setData([
        { time: cFrom.time, value: cFrom.close },
        { time: cTo.time, value: cTo.close },
      ]);
      extremaLineSeries.push(series);
    }
    for (const seg of extremaSegments.red) {
      const cFrom = candleDataByIndex[seg.indexFrom];
      const cTo = candleDataByIndex[seg.indexTo];
      if (!cFrom || !cTo) continue;
      const series = chart.addSeries(LineSeries, { ...opts, color: '#ef5350' });
      series.setData([
        { time: cFrom.time, value: cFrom.close },
        { time: cTo.time, value: cTo.close },
      ]);
      extremaLineSeries.push(series);
    }
  }

  function ensureChart() {
    if (chart) return;
    const w = Math.max(chartDiv.clientWidth || 800, 1);
    const h = Math.max(chartDiv.clientHeight || 400, 300);
    chart = LightweightCharts.createChart(chartDiv, {
      layout: { background: { type: 'solid', color: '#131722' }, textColor: '#d1d4dc' },
      grid: { vertLines: { color: '#2a2e39' }, horzLines: { color: '#2a2e39' } },
      width: w,
      height: h,
      timeScale: { timeVisible: true, secondsVisible: false },
      rightPriceScale: { borderColor: '#2a2e39' },
    });
    candleSeries = chart.addSeries(LightweightCharts.CandlestickSeries, {
      priceScaleId: 'right',
      upColor: '#26a69a',
      downColor: '#ef5350',
      borderVisible: false,
      wickUpColor: '#26a69a',
      wickDownColor: '#ef5350',
    });
    chart.timeScale().fitContent();

    chart.timeScale().subscribeVisibleLogicalRangeChange(range => {
      if (!range || volumeDataByCandleIndex.length === 0) return;
      if (!volumePanel.classList.contains('hidden')) drawVolumeBars(range);
      if (!concentrationPanel.classList.contains('hidden')) drawConcentrationBars(range);
      if (!cumulativePanel.classList.contains('hidden')) drawCumulativeBars(range);
    });
  }

  function drawCumulativeBars(visibleRange) {
    if (!visibleRange || volumeDataByCandleIndex.length === 0 || !chart) return;
    const ctx = cumulativeCanvas.getContext('2d');
    const w = cumulativeCanvas.width;
    const h = cumulativeCanvas.height;
    if (!w || !h) return;

    const from = Math.max(0, Math.floor(visibleRange.from));
    const to = Math.min(volumeDataByCandleIndex.length, Math.ceil(visibleRange.to));
    if (from >= to) return;

    const ts = chart.timeScale();
    let maxAbs = 0;
    for (let i = from; i < to; i++) {
      const c = volumeDataByCandleIndex[i].cumulative_volume_delta;
      if (c != null && Number.isFinite(c)) maxAbs = Math.max(maxAbs, Math.abs(c));
    }
    if (maxAbs <= 0) maxAbs = 1;

    const centerY = h / 2;
    const halfH = (h - 4) / 2;
    ctx.clearRect(0, 0, w, h);
    ctx.strokeStyle = '#2a2e39';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(0, centerY);
    ctx.lineTo(w, centerY);
    ctx.stroke();

    for (let i = from; i < to; i++) {
      const b = volumeDataByCandleIndex[i];
      const cum = b.cumulative_volume_delta != null ? b.cumulative_volume_delta : 0;
      if (!Number.isFinite(cum)) continue;
      const x = Math.round(ts.logicalToCoordinate(i));
      const barW = Math.max(1, Math.round(ts.logicalToCoordinate(i + 1)) - x);
      const norm = cum / maxAbs;
      const barH = Math.abs(norm) * halfH;
      if (barH < 0.5) continue;
      if (cum >= 0) {
        ctx.fillStyle = '#26a69a';
        ctx.fillRect(x, centerY - barH, barW, barH);
      } else {
        ctx.fillStyle = '#ef5350';
        ctx.fillRect(x, centerY, barW, barH);
      }
    }

    const valueToY = (value) => centerY - (value / maxAbs) * halfH;
    const drawSegment = (seg, color) => {
      if (seg.indexTo < from || seg.indexFrom > to) return;
      const x1 = Math.round(ts.logicalToCoordinate(seg.indexFrom));
      const y1 = Math.round(valueToY(seg.valueFrom));
      const x2 = Math.round(ts.logicalToCoordinate(seg.indexTo));
      const y2 = Math.round(valueToY(seg.valueTo));
      ctx.strokeStyle = color;
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.moveTo(x1, y1);
      ctx.lineTo(x2, y2);
      ctx.stroke();
    };
    if (isExtremaLinesEnabled()) {
      extremaSegments.green.forEach((seg) => drawSegment(seg, '#26a69a'));
      extremaSegments.red.forEach((seg) => drawSegment(seg, '#ef5350'));
    }
  }

  function drawConcentrationBars(visibleRange) {
    if (!visibleRange || volumeDataByCandleIndex.length === 0 || !chart) return;
    const ctx = concentrationCanvas.getContext('2d');
    const w = concentrationCanvas.width;
    const h = concentrationCanvas.height;
    if (!w || !h) return;

    const from = Math.max(0, Math.floor(visibleRange.from));
    const to = Math.min(volumeDataByCandleIndex.length, Math.ceil(visibleRange.to));
    if (from >= to) return;

    const ts = chart.timeScale();
    let maxAbs = 0;
    for (let i = from; i < to; i++) {
      const c = volumeDataByCandleIndex[i].volume_delta;
      if (c != null && Number.isFinite(c)) maxAbs = Math.max(maxAbs, Math.abs(c));
    }
    if (maxAbs <= 0) maxAbs = 1;

    const centerY = h / 2;
    const halfH = (h - 4) / 2;
    ctx.clearRect(0, 0, w, h);
    ctx.strokeStyle = '#2a2e39';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(0, centerY);
    ctx.lineTo(w, centerY);
    ctx.stroke();

    for (let i = from; i < to; i++) {
      const b = volumeDataByCandleIndex[i];
      const volumeDelta = b.volume_delta != null ? b.volume_delta : 0;
      if (!Number.isFinite(volumeDelta)) continue;
      const x = Math.round(ts.logicalToCoordinate(i));
      const barW = Math.max(1, Math.round(ts.logicalToCoordinate(i + 1)) - x);
      const norm = volumeDelta / maxAbs;
      const barH = Math.abs(norm) * halfH;
      if (barH < 0.5) continue;
      if (volumeDelta >= 0) {
        ctx.fillStyle = '#26a69a';
        ctx.fillRect(x, centerY - barH, barW, barH);
      } else {
        ctx.fillStyle = '#ef5350';
        ctx.fillRect(x, centerY, barW, barH);
      }
    }
  }

  function drawVolumeBars(visibleRange) {
    if (!visibleRange || volumeDataByCandleIndex.length === 0 || !chart) return;
    const ctx = volumeCanvas.getContext('2d');
    const w = volumeCanvas.width;
    const h = volumeCanvas.height;
    if (!w || !h) return;

    const from = Math.max(0, Math.floor(visibleRange.from));
    const to = Math.min(volumeDataByCandleIndex.length, Math.ceil(visibleRange.to));
    if (from >= to) return;

    const ts = chart.timeScale();
    let maxVolume = 0;
    for (let i = from; i < to; i++) {
      const b = volumeDataByCandleIndex[i];
      const v = b.total_volume;
      if (v == null || !Number.isFinite(v) || v < 0) {
        throw new Error(`volumeData[${i}]: ожидается конечный total_volume >= 0, получено ${v}`);
      }
      if (b.buy_volume_percent == null || !Number.isFinite(b.buy_volume_percent) || b.buy_volume_percent < 0 || b.buy_volume_percent > 1) {
        throw new Error(`volumeData[${i}]: ожидается buy_volume_percent в [0,1], получено ${b.buy_volume_percent}`);
      }
      if (b.sell_volume_percent == null || !Number.isFinite(b.sell_volume_percent) || b.sell_volume_percent < 0 || b.sell_volume_percent > 1) {
        throw new Error(`volumeData[${i}]: ожидается sell_volume_percent в [0,1], получено ${b.sell_volume_percent}`);
      }
      const buyVolume = v * b.buy_volume_percent;
      const sellVolume = v * b.sell_volume_percent;
      maxVolume = Math.max(maxVolume, buyVolume, sellVolume);
    }
    if (maxVolume <= 0) maxVolume = 1;

    const centerY = h / 2;
    const halfH = (h - 4) / 2;

    ctx.clearRect(0, 0, w, h);
    ctx.strokeStyle = '#2a2e39';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(0, centerY);
    ctx.lineTo(w, centerY);
    ctx.stroke();

    for (let i = from; i < to; i++) {
      const b = volumeDataByCandleIndex[i];
      const v = b.total_volume;
      const buyVolume = v * b.buy_volume_percent;
      const sellVolume = v * b.sell_volume_percent;

      const x = Math.round(ts.logicalToCoordinate(i));
      const barW = Math.max(1, Math.round(ts.logicalToCoordinate(i + 1)) - x);

      const normBuy = buyVolume / maxVolume;
      const normSell = sellVolume / maxVolume;
      const barHBuy = normBuy * halfH;
      const barHSell = normSell * halfH;

      if (barHBuy >= 0.5) {
        ctx.fillStyle = '#26a69a';
        ctx.fillRect(x, centerY - barHBuy, barW, barHBuy);
      }
      if (barHSell >= 0.5) {
        ctx.fillStyle = '#ef5350';
        ctx.fillRect(x, centerY, barW, barHSell);
      }
    }
  }

  function applyBarsToChart(data, selectedScale = '') {
    barsData = data.bars || [];
    setStatus(`Загружено ${data.count} баров`);

    const diagnosticsLabel = isDowLevel(selectedScale)
      ? `Dow ${selectedScale}`
      : '';
    const { candleData, volumeData } = barsToCandleAndVolumeData(barsData, diagnosticsLabel);
    if (candleData.length === 0) return;

    computeCumulativeWithWindow(volumeData, getCvdWindowSize());
    volumeDataByCandleIndex = volumeData;
    candleDataByIndex = candleData;
    if (isExtremaLinesEnabled()) {
      computeExtremaSegments(volumeData);
    } else {
      extremaSegments = { green: [], red: [] };
    }
    ensureChart();
    candleSeries.setData(candleData);
    chart.timeScale().fitContent();
    addExtremaLinesToChart();
    if (isTradeResearchEnabled()) {
      removeTradeResearchLineSeries();
    }

    concentrationPanel.classList.remove('hidden');
    cumulativePanel.classList.remove('hidden');
    const w = Math.max(chartDiv.clientWidth || 800, 1);
    const h = Math.max(chartDiv.clientHeight || 400, 300);
    chart.applyOptions({ width: w, height: chartDiv.clientHeight });
    concentrationCanvas.width = concentrationPanel.clientWidth;
    concentrationCanvas.height = concentrationPanel.clientHeight;
    cumulativeCanvas.width = cumulativePanel.clientWidth;
    cumulativeCanvas.height = cumulativePanel.clientHeight;

    requestAnimationFrame(() => {
      const range = chart.timeScale().getVisibleLogicalRange();
      if (range) {
        drawVolumeBars(range);
        drawConcentrationBars(range);
        drawCumulativeBars(range);
      }
    });
  }

  function loadBars() {
    if (loadBarsInFlight) {
      guiLog('loadBars skipped: previous request still in flight', {
        activeLoadBarsRequestId,
      });
      return;
    }
    const scale = scaleSelect.value;
    if (!scale) return;

    if (isTradeResearchEnabled()) {
      ensureTradeResearchScale();
    }

    dowStub.classList.add('hidden');
    volumePanel.classList.remove('hidden');
    concentrationPanel.classList.remove('hidden');
    cumulativePanel.classList.remove('hidden');

    const symbol = symbolSelect.value;
    if (!symbol) return;
    const limit = limitInput.value ? parseInt(limitInput.value, 10) : config.defaultLimit;
    setStatus('Загрузка…');
    barsDataGeneration = barsDataGeneration + 1;
    tradeResearchSegments = [];
    removeTradeResearchLineSeries();
    pendingTradeResearch = null;
    loadTradeResearchInFlight = false;
    tradeResearchRequestSeq = tradeResearchRequestSeq + 1;
    activeTradeResearchRequestId = tradeResearchRequestSeq;
    loadBarsInFlight = true;
    loadBarsRequestSeq = loadBarsRequestSeq + 1;
    const loadRequestId = loadBarsRequestSeq;
    activeLoadBarsRequestId = loadRequestId;
    const loadStartedMs = performance.now();

    const effectiveScale = scaleSelect.value;
    const barsParams = isDowLevel(effectiveScale)
      ? { symbol_id: symbol, limit, level: parseDowLevel(effectiveScale) }
      : { symbol_id: symbol, limit, scale: effectiveScale };
    guiLog('loadBars start', {
      loadRequestId,
      symbol,
      effectiveScale,
      limit,
      tradeResearchEnabled: isTradeResearchEnabled(),
    });
    const promise = isDowLevel(effectiveScale)
      ? API.dow(barsParams)
      : API.bars(barsParams);

    promise
      .then(data => {
        guiLog('loadBars bars done', {
          loadRequestId,
          durationMs: Math.round(performance.now() - loadStartedMs),
          count: data.count,
        });
        applyBarsToChart(data, effectiveScale);
        updateLatestX1BarFromBarsData(data, effectiveScale);
        refreshInferenceBarProvenanceDisplay();
        refreshTradeJournal(symbol);
      })
      .catch(e => {
        guiLog('loadBars error', {
          loadRequestId,
          durationMs: Math.round(performance.now() - loadStartedMs),
          message: e.message,
        });
        setStatus('Ошибка: ' + e.message, true);
      })
      .finally(() => {
        if (activeLoadBarsRequestId === loadRequestId) {
          loadBarsInFlight = false;
        }
        guiLog('loadBars finished', {
          loadRequestId,
          durationMs: Math.round(performance.now() - loadStartedMs),
          inFlight: loadBarsInFlight,
        });
        scheduleTradeResearch(symbol, 'afterBars');
      });
  }

  function stopIndependentRefreshTimers() {
    if (inferenceRefreshTimer) clearInterval(inferenceRefreshTimer);
    if (journalRefreshTimer) clearInterval(journalRefreshTimer);
    if (journalBarsElapsedTimer) clearInterval(journalBarsElapsedTimer);
    if (x1BarRefreshTimer) clearInterval(x1BarRefreshTimer);
    inferenceRefreshTimer = null;
    journalRefreshTimer = null;
    journalBarsElapsedTimer = null;
    x1BarRefreshTimer = null;
  }

  function resolveLoadedAssetVersion() {
    const meta = document.querySelector('meta[name="okx-asset-version"]');
    if (meta && meta.content) {
      return String(meta.content);
    }
    return null;
  }

  function stopAssetVersionPoll() {
    if (assetVersionPollTimer) {
      clearInterval(assetVersionPollTimer);
      assetVersionPollTimer = null;
    }
  }

  function startAssetVersionPoll(loadedAssetVersion) {
    stopAssetVersionPoll();
    if (!loadedAssetVersion) {
      return;
    }
    assetVersionPollTimer = setInterval(async () => {
      try {
        const payload = await API.get('./api/asset-version');
        const serverAssetVersion = payload && payload.assetVersion != null
          ? String(payload.assetVersion)
          : null;
        if (serverAssetVersion && serverAssetVersion !== loadedAssetVersion) {
          guiLog('asset_version_changed', {
            loaded: loadedAssetVersion,
            server: serverAssetVersion,
          });
          window.location.reload();
        }
      } catch (exception) {
        guiLog('asset_version_poll_failed', exception.message);
      }
    }, ASSET_VERSION_POLL_INTERVAL_SEC * 1000);
  }

  function startIndependentRefreshTimers() {
    stopIndependentRefreshTimers();
    inferenceRefreshTimer = setInterval(
      refreshInferencePanel,
      INFERENCE_REFRESH_INTERVAL_SEC * 1000,
    );
    journalRefreshTimer = setInterval(() => {
      const symbol = symbolSelect.value;
      if (symbol) {
        refreshTradeJournal(symbol);
      }
    }, JOURNAL_REFRESH_INTERVAL_SEC * 1000);
    journalBarsElapsedTimer = setInterval(
      refreshJournalBarsElapsedOnly,
      JOURNAL_BARS_ELAPSED_INTERVAL_SEC * 1000,
    );
    x1BarRefreshTimer = setInterval(
      refreshLatestX1Bar,
      X1_BAR_REFRESH_INTERVAL_SEC * 1000,
    );
  }

  function startAutoRefresh() {
    if (refreshTimer) clearInterval(refreshTimer);
    refreshTimer = null;
    if (!autoRefreshCheck.checked) {
      return;
    }
    refreshTimer = setInterval(loadBars, config.refreshIntervalSec * 1000);
  }

  function startBackgroundRefreshTimers() {
    stopIndependentRefreshTimers();
    startIndependentRefreshTimers();
  }

  function initCvdWindowDropdown() {
    cvdWindowSelect.innerHTML = '';
    CVD_WINDOW_OPTIONS.forEach((opt) => {
      const option = document.createElement('option');
      option.value = opt;
      option.textContent = opt;
      if (opt === CVD_WINDOW_DEFAULT) option.selected = true;
      cvdWindowSelect.appendChild(option);
    });
  }

  loadBtn.addEventListener('click', loadBars);
  cvdWindowSelect.addEventListener('change', () => {
    if (volumeDataByCandleIndex.length === 0) return;
    computeCumulativeWithWindow(volumeDataByCandleIndex, getCvdWindowSize());
    refreshExtremaOverlays();
    const range = chart && chart.timeScale().getVisibleLogicalRange();
    if (range) drawCumulativeBars(range);
  });
  scaleSelect.addEventListener('change', () => {
    dowStub.classList.add('hidden');
    volumePanel.classList.remove('hidden');
    concentrationPanel.classList.remove('hidden');
    cumulativePanel.classList.remove('hidden');
    loadBars();
    refreshInferencePanel();
    const symbol = symbolSelect.value;
    if (symbol) {
      refreshTradeJournal(symbol);
    }
  });
  autoRefreshCheck.addEventListener('change', startAutoRefresh);
  if (extremaLinesEnabledCheck) {
    extremaLinesEnabledCheck.addEventListener('change', refreshExtremaOverlays);
  }
  if (tradeResearchEnabledCheck) {
    tradeResearchEnabledCheck.addEventListener('change', () => {
      if (isTradeResearchEnabled()) {
        ensureTradeResearchScale();
        autoRefreshCheck.checked = false;
        startAutoRefresh();
      } else {
        tradeResearchSegments = [];
        removeTradeResearchLineSeries();
      }
      loadBars();
    });
  }

  (async function init() {
    try {
      config = await API.config();
      inferenceMinRows = config.inferenceMinRows != null ? Number(config.inferenceMinRows) : 0;
      chartShowLimit = config.chartShowLimit != null ? Number(config.chartShowLimit) : 50000;
      inferenceErrorBySymbolAndHorizon = config.inferenceErrorBySymbolAndHorizon || {};
      policyBySymbol = config.policyBySymbol || {};
      exitPolicyBySymbol = config.exitPolicyBySymbol || {};
      exitStackBySymbol = config.exitStackBySymbol || {};
      entryHintModeBySymbol = config.entryHintModeBySymbol || {};
      entryConfidenceMarginBySymbol = config.entryConfidenceMarginBySymbol || {};
      if (config.tradingInitialBalanceUsd != null) {
        tradingInitialBalanceUsd = Number(config.tradingInitialBalanceUsd);
      }
      checkpointPathBySymbol = config.checkpointPathBySymbol || {};
      if (config.tradeResearchEvalHorizon) {
        tradeResearchEvalHorizon = String(config.tradeResearchEvalHorizon);
        tradeResearchScale = tradeResearchEvalHorizon;
      }
      if (Array.isArray(config.tradeResearchAvailableHorizons)) {
        tradeResearchAvailableHorizons = config.tradeResearchAvailableHorizons.map(String);
      }
      updateTradeResearchUi();
      if (config.defaultLimit) {
        limitInput.placeholder = config.defaultLimit;
        limitInput.value = config.defaultLimit;
      }
      const loadedAssetVersion = config.assetVersion != null
        ? String(config.assetVersion)
        : resolveLoadedAssetVersion();
      startAssetVersionPoll(loadedAssetVersion);
      await initDropdowns();
      initCvdWindowDropdown();
      chartDiv.style.height = '100%';
      volumeCanvas.width = volumePanel.clientWidth;
      volumeCanvas.height = volumePanel.clientHeight;
      window.addEventListener('resize', () => {
        if (chart) chart.applyOptions({ width: chartDiv.clientWidth, height: chartDiv.clientHeight });
        volumeCanvas.width = volumePanel.clientWidth;
        volumeCanvas.height = volumePanel.clientHeight;
        concentrationCanvas.width = concentrationPanel.clientWidth;
        concentrationCanvas.height = concentrationPanel.clientHeight;
        cumulativeCanvas.width = cumulativePanel.clientWidth;
        cumulativeCanvas.height = cumulativePanel.clientHeight;
        const range = chart && chart.timeScale().getVisibleLogicalRange();
        if (range) {
          drawVolumeBars(range);
          drawConcentrationBars(range);
          drawCumulativeBars(range);
        }
      });
      loadBars();
      refreshLatestX1Bar();
      refreshInferencePanel();
      const initialSymbol = symbolSelect.value;
      if (initialSymbol) {
        refreshTradeJournal(initialSymbol);
      }
      startBackgroundRefreshTimers();
      startAutoRefresh();
      window.addEventListener('online', () => {
        refreshInferencePanel();
      });
    } catch (e) {
      setStatus('Ошибка инициализации: ' + e.message, true);
    }
  })();
})();
