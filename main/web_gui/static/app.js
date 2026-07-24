(function () {
  'use strict';

  const API = {
    async get(path) {
      const r = await fetch(path);
      if (!r.ok) throw new Error(await r.text());
      return r.json();
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
    async inference(params) {
      const q = new URLSearchParams(params).toString();
      return this.get('./api/inference?' + q);
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
    async tradeJournalEntry(body) {
      const r = await fetch('./api/trade-journal/entry', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const text = await r.text();
        throw new Error(text);
      }
      return r.json();
    },
    async tradeJournalExit(body) {
      const r = await fetch('./api/trade-journal/exit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const text = await r.text();
        throw new Error(text);
      }
      return r.json();
    },
    async tradeJournalDiscardOpen() {
      const r = await fetch('./api/trade-journal/open', { method: 'DELETE' });
      if (!r.ok) {
        const text = await r.text();
        throw new Error(text);
      }
      return r.json();
    },
    async exitPolicy(body) {
      const r = await fetch('./api/exit-policy', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const text = await r.text();
        throw new Error(text);
      }
      return r.json();
    },
    async exitTransformer(body) {
      const r = await fetch('./api/exit-transformer', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const text = await r.text();
        throw new Error(text);
      }
      return r.json();
    },
  };

  let config = { defaultLimit: 10000000, defaultScale: 'x2048', refreshIntervalSec: 30 };
  let chart = null;
  let candleSeries = null;
  let barsData = [];
  /** Объёмы по индексу свечи (1:1 с candleData после сортировки и слияния по time) */
  let volumeDataByCandleIndex = [];
  /** Свечи по индексу (time, open, high, low, close) для проекции линий экстремумов на ценовой график */
  let candleDataByIndex = [];
  /** Сегменты от экстремума к экстремуму: { green: [{indexFrom, valueFrom, indexTo, valueTo}, ...], red: [...] } */
  let extremaSegments = { green: [], red: [] };
  /** Серии линий non-overlapping trade research @ x2048 */
  let tradeResearchSegments = [];
  let tradeResearchLineSeries = [];
  let extremaLineSeries = [];
  let refreshTimer = null;
  let inferenceRefreshTimer = null;
  let journalRefreshTimer = null;
  let journalBarsElapsedTimer = null;
  let x1BarRefreshTimer = null;
  let loadBarsInFlight = false;
  let loadInferenceInFlight = false;
  let refreshJournalInFlight = false;
  let refreshBarsElapsedInFlight = false;
  let refreshX1BarInFlight = false;
  let lastJournalBarsElapsed = null;
  const INFERENCE_REFRESH_INTERVAL_SEC = 10;
  const JOURNAL_REFRESH_INTERVAL_SEC = 15;
  const JOURNAL_BARS_ELAPSED_INTERVAL_SEC = 30;
  const X1_BAR_REFRESH_INTERVAL_SEC = 60;
  const scaleSelect = document.getElementById('scale');
  const symbolSelect = document.getElementById('symbol');
  const limitInput = document.getElementById('limit');
  const loadBtn = document.getElementById('load');
  const autoRefreshCheck = document.getElementById('autoRefresh');
  const extremaLinesEnabledCheck = document.getElementById('extremaLinesEnabled');
  const tradeResearchEnabledCheck = document.getElementById('tradeResearchEnabled');
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
  const journalNotionalInput = document.getElementById('journalNotional');
  const journalFillPriceInput = document.getElementById('journalFillPrice');
  const journalFillPriceLabel = document.getElementById('journalFillPriceLabel');
  const journalActionStatusEl = document.getElementById('journalActionStatus');
  const journalEvalHorizonSelect = document.getElementById('journalEvalHorizon');
  const journalSoundEnabledCheck = document.getElementById('journalSoundEnabled');
  let inferenceErrorBySymbolAndHorizon = {};
  let policyBySymbol = {};
  let exitPolicyBySymbol = {};
  let exitTransformerBySymbol = {};
  let exitGbmEnabled = false;
  let exitTransformerEnabled = false;
  let checkpointPathBySymbol = {};
  let inferenceMinRows = 0;
  let chartShowLimit = 50000;
  let lastPolicy = null;
  let lastEntryHint = null;
  let lastPredictions = null;
  let lastExitPolicy = null;
  let lastExitTransformer = null;
  let lastInferenceStatus = null;
  let lastInferenceCompletedAtMs = null;
  let lastComputingStartedAtMs = null;
  let inferenceStatusTickTimer = null;
  let latestX1Bar = null;
  let lastChartBarClose = null;
  let journalDefaults = {
    notional_usd: 7,
    eval_horizon: 'x2048',
    round_trip_fee_rate: 0.001,
  };
  let previousAtTargetHorizon = false;
  let isFirstJournalLoad = true;
  let horizonAlertPositionId = null;
  let previousExitGbmSuggestClose = false;
  let exitGbmAlertPositionId = null;
  let previousExitTransformerSuggestClose = false;
  let exitTransformerAlertPositionId = null;
  let exitOverlaySession = null;
  let previousEntryAllowedAction = null;
  let isFirstEntryHintSample = true;
  let journalHasOpenPosition = false;
  let lastJournalState = null;
  let journalActionPending = null;
  let audioContext = null;
  let audioUnlocked = false;

  const SCALE_NAMES = ['x1', 'x2', 'x4', 'x8', 'x16', 'x32', 'x64', 'x128', 'x256', 'x512', 'x1024', 'x2048', 'x4096', 'x8192', 'x16384', 'x32768', 'x65536', 'x131072', 'x262144'];
  const CVD_WINDOW_OPTIONS = ['x2', 'x4', 'x8', 'x16', 'x32', 'x64', 'x128', 'x256', 'x512', 'x1024', 'x2048', 'x4096', 'x8192', 'x16384'];
  const CVD_WINDOW_DEFAULT = 'x512';
  const JOURNAL_EVAL_HORIZON_OPTIONS = ['x512', 'x1024', 'x1536', 'x2048', 'x3072', 'x4096'];
  const TRADE_RESEARCH_EVAL_HORIZON = 'x2048';
  const TRADE_RESEARCH_SCALE = 'x2048';
  const JOURNAL_SETTINGS_STORAGE_KEY = 'okx_web_gui_journal_settings';
  const JOURNAL_SOUND_ENABLED_STORAGE_KEY = 'okx_web_gui_journal_sound_enabled';

  function loadJournalSettingsFromStorage() {
    try {
      const raw = localStorage.getItem(JOURNAL_SETTINGS_STORAGE_KEY);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== 'object') return null;
      return parsed;
    } catch (_) {
      return null;
    }
  }

  function saveJournalSettingsToStorage() {
    const evalHorizon = journalEvalHorizonSelect.value;
    if (!evalHorizon) return;
    localStorage.setItem(
      JOURNAL_SETTINGS_STORAGE_KEY,
      JSON.stringify({ eval_horizon: evalHorizon }),
    );
  }

  function parseJournalFillPrice() {
    const value = Number(journalFillPriceInput.value);
    if (!Number.isFinite(value) || value <= 0) {
      return null;
    }
    return value;
  }

  function parseJournalNotionalUsd() {
    const value = Number(journalNotionalInput.value);
    if (!Number.isFinite(value) || value <= 0) {
      return null;
    }
    return value;
  }

  function journalEntryFillValid() {
    return parseJournalFillPrice() !== null && parseJournalNotionalUsd() !== null;
  }

  function journalExitFillValid() {
    return parseJournalFillPrice() !== null;
  }

  function resetJournalFillFields() {
    journalFillPriceInput.value = '';
    journalNotionalInput.value = '';
  }

  function initJournalFillFieldsOnLoad() {
    resetJournalFillFields();
  }

  function setJournalActionPending(pending) {
    journalActionPending = pending;
    updateJournalActionStatusUi();
    if (lastJournalState && symbolSelect.value) {
      renderTradeJournal(lastJournalState, symbolSelect.value);
    }
  }

  function updateJournalActionStatusUi() {
    if (!journalActionStatusEl) return;
    if (!journalActionPending) {
      journalActionStatusEl.classList.add('hidden');
      journalActionStatusEl.textContent = '';
      return;
    }
    journalActionStatusEl.classList.remove('hidden');
    journalActionStatusEl.textContent = journalActionPending.message;
  }

  function getJournalNotionalUsd() {
    const parsed = parseJournalNotionalUsd();
    if (parsed !== null) {
      return parsed;
    }
    return Number(journalDefaults.notional_usd);
  }

  function getJournalEvalHorizon() {
    const value = journalEvalHorizonSelect.value;
    if (value) return value;
    return journalDefaults.eval_horizon;
  }

  function setJournalEvalHorizon(value) {
    if (!value) return;
    if (!JOURNAL_EVAL_HORIZON_OPTIONS.includes(value)) return;
    journalEvalHorizonSelect.value = value;
    saveJournalSettingsToStorage();
  }

  function initJournalSettingsControls() {
    journalEvalHorizonSelect.innerHTML = '';
    JOURNAL_EVAL_HORIZON_OPTIONS.forEach((horizon) => {
      const option = document.createElement('option');
      option.value = horizon;
      option.textContent = horizon;
      journalEvalHorizonSelect.appendChild(option);
    });

    const stored = loadJournalSettingsFromStorage();
    if (stored && stored.eval_horizon != null && JOURNAL_EVAL_HORIZON_OPTIONS.includes(stored.eval_horizon)) {
      journalEvalHorizonSelect.value = stored.eval_horizon;
    } else {
      journalEvalHorizonSelect.value = journalDefaults.eval_horizon;
    }

    initJournalFillFieldsOnLoad();
    journalNotionalInput.addEventListener('input', () => {
      if (lastJournalState && symbolSelect.value && !journalHasOpenPosition) {
        renderTradeJournal(lastJournalState, symbolSelect.value);
      }
    });
    journalFillPriceInput.addEventListener('input', () => {
      if (lastJournalState && symbolSelect.value) {
        renderTradeJournal(lastJournalState, symbolSelect.value);
      }
    });
    journalEvalHorizonSelect.addEventListener('change', saveJournalSettingsToStorage);
  }

  function syncJournalSettingsDisabled(hasOpen) {
    journalNotionalInput.disabled = hasOpen || journalActionPending !== null;
    journalFillPriceInput.disabled = journalActionPending !== null;
    journalEvalHorizonSelect.disabled = hasOpen || journalActionPending !== null;
    if (journalFillPriceLabel) {
      journalFillPriceLabel.textContent = hasOpen ? 'Цена выхода $' : 'Цена входа $';
    }
  }

  function isJournalSoundEnabled() {
    return journalSoundEnabledCheck.checked;
  }

  function loadJournalSoundEnabledFromStorage() {
    const raw = localStorage.getItem(JOURNAL_SOUND_ENABLED_STORAGE_KEY);
    if (raw === '0') {
      journalSoundEnabledCheck.checked = false;
      return;
    }
    journalSoundEnabledCheck.checked = true;
  }

  function saveJournalSoundEnabledToStorage() {
    localStorage.setItem(
      JOURNAL_SOUND_ENABLED_STORAGE_KEY,
      journalSoundEnabledCheck.checked ? '1' : '0',
    );
  }

  function getAudioContext() {
    if (!audioContext) {
      const AudioCtx = window.AudioContext || window.webkitAudioContext;
      audioContext = new AudioCtx();
    }
    return audioContext;
  }

  function unlockAudio() {
    const ctx = getAudioContext();
    if (ctx.state === 'suspended') {
      ctx.resume();
    }
    audioUnlocked = true;
  }

  function playTone(frequencyHz, durationSec, startSec, gainValue) {
    const ctx = getAudioContext();
    const oscillator = ctx.createOscillator();
    const gainNode = ctx.createGain();
    oscillator.type = 'sine';
    oscillator.frequency.value = frequencyHz;
    gainNode.gain.value = gainValue;
    oscillator.connect(gainNode);
    gainNode.connect(ctx.destination);
    const startAt = ctx.currentTime + startSec;
    oscillator.start(startAt);
    oscillator.stop(startAt + durationSec);
  }

  function playHorizonReachedSound() {
    if (!isJournalSoundEnabled()) return;
    unlockAudio();
    playTone(880, 0.12, 0, 0.08);
    playTone(1175, 0.18, 0.18, 0.08);
  }

  function playHorizonOverdueSound() {
    if (!isJournalSoundEnabled()) return;
    unlockAudio();
    playTone(740, 0.14, 0, 0.1);
    playTone(740, 0.14, 0.22, 0.1);
    playTone(988, 0.22, 0.44, 0.1);
  }

  function emptyExitOverlaySession() {
    return {
      exit_gbm_enabled: false,
      exit_transformer_enabled: false,
      exit_policy_at_close: null,
      exit_transformer_at_close: null,
      exit_gbm_alert_fired: false,
      exit_transformer_alert_fired: false,
      first_exit_gbm_alert: null,
      first_exit_transformer_alert: null,
    };
  }

  function resetExitOverlaySession() {
    exitOverlaySession = emptyExitOverlaySession();
    exitOverlaySession.exit_gbm_enabled = exitGbmEnabled;
    exitOverlaySession.exit_transformer_enabled = exitTransformerEnabled;
  }

  function sanitizeExitPolicyForJournal(exitPolicy) {
    if (!exitPolicy || exitPolicy.close_probability == null) {
      return null;
    }
    return {
      enabled: exitPolicy.enabled !== false,
      action: exitPolicy.action,
      suggest_close: Boolean(exitPolicy.suggest_close),
      close_probability: Number(exitPolicy.close_probability),
      close_probability_threshold: Number(exitPolicy.close_probability_threshold),
      min_hold_steps: exitPolicy.min_hold_steps,
      bars_held: exitPolicy.bars_held,
      run_label: exitPolicy.run_label,
      eval_horizon: exitPolicy.eval_horizon,
    };
  }

  function sanitizeExitTransformerForJournal(exitTransformer) {
    if (!exitTransformer || exitTransformer.predicted_delta_pnl == null) {
      return null;
    }
    return {
      enabled: exitTransformer.enabled !== false,
      action: exitTransformer.action,
      suggest_close: Boolean(exitTransformer.suggest_close),
      predicted_delta_pnl: Number(exitTransformer.predicted_delta_pnl),
      delta_pnl_threshold: Number(exitTransformer.delta_pnl_threshold),
      min_hold_steps: exitTransformer.min_hold_steps,
      bars_held: exitTransformer.bars_held,
      run_label: exitTransformer.run_label,
      eval_horizon: exitTransformer.eval_horizon,
    };
  }

  function buildPositionMetricsSnapshot(openPos) {
    if (!openPos || !openPos.metrics) {
      return null;
    }
    const metrics = openPos.metrics;
    return {
      bars_elapsed: metrics.bars_elapsed,
      unrealized_net_return_pct: metrics.unrealized_net_return_pct,
      mfe_net_return_pct: metrics.mfe_net_return_pct,
      mae_net_return_pct: metrics.mae_net_return_pct,
      giveback_net_return_pct: metrics.giveback_net_return_pct,
      mark_price: metrics.mark_price,
    };
  }

  function updateExitOverlaySession(openPos) {
    if (!exitOverlaySession) {
      resetExitOverlaySession();
    }
    exitOverlaySession.exit_gbm_enabled = exitGbmEnabled;
    exitOverlaySession.exit_transformer_enabled = exitTransformerEnabled;
    const metricsSnapshot = buildPositionMetricsSnapshot(openPos);

    if (exitGbmEnabled && lastExitPolicy) {
      const sanitized = sanitizeExitPolicyForJournal(lastExitPolicy);
      exitOverlaySession.exit_policy_at_close = sanitized;
      if (sanitized && sanitized.suggest_close && !exitOverlaySession.exit_gbm_alert_fired) {
        exitOverlaySession.exit_gbm_alert_fired = true;
        exitOverlaySession.first_exit_gbm_alert = {
          overlay: sanitized,
          position_metrics: metricsSnapshot,
        };
      }
    }

    if (exitTransformerEnabled && lastExitTransformer) {
      const sanitized = sanitizeExitTransformerForJournal(lastExitTransformer);
      exitOverlaySession.exit_transformer_at_close = sanitized;
      if (sanitized && sanitized.suggest_close && !exitOverlaySession.exit_transformer_alert_fired) {
        exitOverlaySession.exit_transformer_alert_fired = true;
        exitOverlaySession.first_exit_transformer_alert = {
          overlay: sanitized,
          position_metrics: metricsSnapshot,
        };
      }
    }
  }

  function buildExitOverlayPayloadForClose() {
    if (!exitOverlaySession) {
      resetExitOverlaySession();
    }
    return {
      exit_gbm_enabled: exitOverlaySession.exit_gbm_enabled,
      exit_transformer_enabled: exitOverlaySession.exit_transformer_enabled,
      exit_policy_at_close: exitOverlaySession.exit_policy_at_close,
      exit_transformer_at_close: exitOverlaySession.exit_transformer_at_close,
      exit_gbm_alert_fired: exitOverlaySession.exit_gbm_alert_fired,
      exit_transformer_alert_fired: exitOverlaySession.exit_transformer_alert_fired,
      first_exit_gbm_alert: exitOverlaySession.first_exit_gbm_alert,
      first_exit_transformer_alert: exitOverlaySession.first_exit_transformer_alert,
    };
  }

  function resetHorizonAlertState() {
    previousAtTargetHorizon = false;
    horizonAlertPositionId = null;
  }

  function resetExitGbmAlertState() {
    previousExitGbmSuggestClose = false;
    exitGbmAlertPositionId = null;
    lastExitPolicy = null;
  }

  function resetExitTransformerAlertState() {
    previousExitTransformerSuggestClose = false;
    exitTransformerAlertPositionId = null;
    lastExitTransformer = null;
  }

  resetExitOverlaySession();

  function resetEntryAllowedAlertState() {
    previousEntryAllowedAction = null;
  }

  function playEntryLongSound() {
    if (!isJournalSoundEnabled()) return;
    unlockAudio();
    playTone(523, 0.1, 0, 0.09);
    playTone(659, 0.1, 0.12, 0.09);
    playTone(784, 0.16, 0.24, 0.09);
  }

  function playEntryShortSound() {
    if (!isJournalSoundEnabled()) return;
    unlockAudio();
    playTone(784, 0.1, 0, 0.09);
    playTone(659, 0.1, 0.12, 0.09);
    playTone(523, 0.16, 0.24, 0.09);
  }

  function entryAllowedActionFromHint(entryHint) {
    if (!entryHint || !entryHint.recommended_action) {
      return null;
    }
    const recommended = String(entryHint.recommended_action).toLowerCase();
    if (recommended === 'long' || recommended === 'short') {
      return recommended;
    }
    return null;
  }

  function showEntryAllowedBrowserNotification(title, body) {
    if (!('Notification' in window)) return;
    if (Notification.permission !== 'granted') return;
    try {
      new Notification(title, { body, tag: 'okx-micro-live-entry' });
    } catch (_) {
      // no-op
    }
  }

  function maybeNotifyEntryAllowedAlert(entryHint) {
    if (journalHasOpenPosition) {
      resetEntryAllowedAlertState();
      return;
    }

    const currentAction = entryAllowedActionFromHint(entryHint);
    const previousAction = previousEntryAllowedAction;

    if (isFirstEntryHintSample) {
      isFirstEntryHintSample = false;
      previousEntryAllowedAction = currentAction;
      return;
    }

    const becameAllowed = currentAction != null && previousAction !== currentAction;
    if (becameAllowed) {
      if (currentAction === 'long') {
        playEntryLongSound();
      } else if (currentAction === 'short') {
        playEntryShortSound();
      }
      const policyAction = lastPolicy && lastPolicy.action
        ? String(lastPolicy.action).toUpperCase()
        : currentAction.toUpperCase();
      const snr = entryHint.snr != null ? Number(entryHint.snr).toFixed(2) : '—';
      const title = `Micro live: вход ${currentAction.toUpperCase()}`;
      const body = `Policy ${policyAction}, SNR=${snr} — SNR/gate ok @ ${entryHint.eval_horizon || 'eval'}`;
      showEntryAllowedBrowserNotification(title, body);
    }

    previousEntryAllowedAction = currentAction;
  }

  function playExitGbmAlertSound() {
    if (!isJournalSoundEnabled()) return;
    unlockAudio();
    playTone(988, 0.12, 0, 0.09);
    playTone(1319, 0.2, 0.16, 0.09);
  }

  function maybeNotifyExitGbmAlert(openPos, exitPolicy) {
    if (!exitGbmEnabled) {
      previousExitGbmSuggestClose = false;
      return;
    }
    if (!openPos || !exitPolicy) {
      previousExitGbmSuggestClose = false;
      return;
    }
    const suggestClose = Boolean(exitPolicy.suggest_close);
    if (!suggestClose) {
      previousExitGbmSuggestClose = false;
      return;
    }

    const positionId = openPos.id || `${openPos.symbol_id}:${openPos.entry_start_trade_id}`;
    const sideLabel = String(openPos.side || '').toUpperCase();
    const threshold = exitPolicy.close_probability_threshold;
    const pClose = exitPolicy.close_probability;
    const title = 'Micro live: Exit GBM → CLOSE';
    const body = `${sideLabel} ${openPos.symbol_id} — P(close)=${(Number(pClose) * 100).toFixed(1)}% ≥ ${(Number(threshold) * 100).toFixed(1)}%`;

    if (!previousExitGbmSuggestClose) {
      playExitGbmAlertSound();
      showExitGbmBrowserNotification(title, body);
    }

    exitGbmAlertPositionId = positionId;
    previousExitGbmSuggestClose = true;
  }

  function showExitGbmBrowserNotification(title, body) {
    if (!('Notification' in window)) return;
    if (Notification.permission !== 'granted') return;
    try {
      new Notification(title, { body, tag: 'okx-micro-live-exit-gbm' });
    } catch (_) {
      // no-op
    }
  }

  function playExitTransformerAlertSound() {
    if (!isJournalSoundEnabled()) return;
    unlockAudio();
    playTone(880, 0.12, 0, 0.09);
    playTone(1175, 0.2, 0.16, 0.09);
  }

  function maybeNotifyExitTransformerAlert(openPos, exitTransformer) {
    if (!exitTransformerEnabled) {
      previousExitTransformerSuggestClose = false;
      return;
    }
    if (!openPos || !exitTransformer) {
      previousExitTransformerSuggestClose = false;
      return;
    }
    const suggestClose = Boolean(exitTransformer.suggest_close);
    if (!suggestClose) {
      previousExitTransformerSuggestClose = false;
      return;
    }

    const positionId = openPos.id || `${openPos.symbol_id}:${openPos.entry_start_trade_id}`;
    const sideLabel = String(openPos.side || '').toUpperCase();
    const threshold = exitTransformer.delta_pnl_threshold;
    const deltaPnl = exitTransformer.predicted_delta_pnl;
    const title = 'Micro live: Exit Transformer v2 → CLOSE';
    const body = `${sideLabel} ${openPos.symbol_id} — Δpnl=${formatPct(Number(deltaPnl) * 100)} > ${formatPct(Number(threshold) * 100)}`;

    if (!previousExitTransformerSuggestClose) {
      playExitTransformerAlertSound();
      showExitTransformerBrowserNotification(title, body);
    }

    exitTransformerAlertPositionId = positionId;
    previousExitTransformerSuggestClose = true;
  }

  function showExitTransformerBrowserNotification(title, body) {
    if (!('Notification' in window)) return;
    if (Notification.permission !== 'granted') return;
    try {
      new Notification(title, { body, tag: 'okx-micro-live-exit-transformer' });
    } catch (_) {
      // no-op
    }
  }

  function maybeNotifyHorizonAlert(openPos, atTargetHorizon) {
    if (!openPos || !atTargetHorizon) {
      previousAtTargetHorizon = false;
      return;
    }

    const positionId = openPos.id || `${openPos.symbol_id}:${openPos.entry_start_trade_id}`;
    const evalHorizonLabel = openPos.eval_horizon || 'horizon';
    const sideLabel = String(openPos.side || '').toUpperCase();
    const title = `Micro live: выход по ${evalHorizonLabel}`;
    const body = `${sideLabel} ${openPos.symbol_id} — счётчик баров достиг горизонта, закрой позицию на бирже.`;

    const isFirstCrossThisSession = !previousAtTargetHorizon && !isFirstJournalLoad;
    if (isFirstCrossThisSession) {
      playHorizonReachedSound();
    } else {
      playHorizonOverdueSound();
    }

    if (!previousAtTargetHorizon) {
      showHorizonBrowserNotification(title, body);
    }

    horizonAlertPositionId = positionId;
    previousAtTargetHorizon = true;
  }

  function showHorizonBrowserNotification(title, body) {
    if (!('Notification' in window)) return;
    if (Notification.permission !== 'granted') return;
    try {
      new Notification(title, { body, tag: 'okx-micro-live-horizon' });
    } catch (_) {
      // no-op
    }
  }

  function requestNotificationPermissionIfNeeded() {
    if (!('Notification' in window)) return;
    if (Notification.permission !== 'default') return;
    Notification.requestPermission();
  }

  function initJournalSoundControls() {
    loadJournalSoundEnabledFromStorage();
    journalSoundEnabledCheck.addEventListener('change', () => {
      saveJournalSoundEnabledToStorage();
      if (journalSoundEnabledCheck.checked) {
        unlockAudio();
        requestNotificationPermissionIfNeeded();
      }
    });

    document.addEventListener('click', () => {
      unlockAudio();
    }, { once: true });
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
    const m = targetName.match(/_(x\d+)$/);
    return m ? m[1] : targetName;
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

  function entryPredictionSnapshot(openPos) {
    if (!openPos || !openPos.entry_predictions) return null;
    const evalHorizon = openPos.eval_horizon || journalDefaults.eval_horizon;
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

  function currentPredictionSnapshot(openPos) {
    if (!openPos || !lastPredictions) return null;
    const evalHorizon = openPos.eval_horizon || journalDefaults.eval_horizon;
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

  function renderEntryPredictionMetrics(openPos, metrics) {
    const entrySnap = entryPredictionSnapshot(openPos);
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

    const currentSnap = currentPredictionSnapshot(openPos);
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

    if (lastInferenceStatus === 'ok') {
      const ageLabel = formatRelativeTimeAgo(lastInferenceCompletedAtMs);
      inferenceStatusBar.innerHTML = `
        <span class="inference-status-age">Обновлено: <strong>${ageLabel}</strong></span>
      `;
      return;
    }

    if (lastInferenceStatus === 'computing') {
      const ageLabel = formatRelativeTimeAgo(lastInferenceCompletedAtMs);
      const refreshDuration = formatDurationSince(lastComputingStartedAtMs);
      const hasSnapshot = Number.isFinite(lastInferenceCompletedAtMs);
      const snapshotLine = hasSnapshot
        ? `<span class="inference-status-age">Предсказания от <strong>${ageLabel}</strong></span>`
        : '<span class="inference-status-age">Первый offline-инференс ещё не готов</span>';
      inferenceStatusBar.innerHTML = `
        ${snapshotLine}
        <span class="inference-status-computing-label">Обновление… (${refreshDuration})</span>
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
    if (policy.eval_horizon) {
      setJournalEvalHorizon(policy.eval_horizon);
    }
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
      const holdProb = entryHint.hold_probability != null
        ? Number(entryHint.hold_probability).toFixed(2)
        : null;
      const holdThreshold = entryHint.hold_probability_threshold != null
        ? Number(entryHint.hold_probability_threshold).toFixed(2)
        : null;
      const gbmBlocks = Boolean(entryHint.gbm_blocks_entry);
      const snrBlocks = Boolean(entryHint.snr_blocks_entry);
      const title = isHybrid
        ? `Entry hint @ ${evalHorizon} (hybrid P(hold)≥${holdThreshold} ∨ SNR≥${snrThreshold}, rmse=${rmsePct}%)`
        : `Entry hint @ ${evalHorizon} (SNR≥${snrThreshold}, rmse=${rmsePct}%)`;
      const hybridMeta = isHybrid && holdProb != null
        ? `<span>P(hold): <strong>${holdProb}</strong>${gbmBlocks ? ' ⛔' : ''}</span>`
        : '';
      const blockTags = isHybrid
        ? `<span>${gbmBlocks ? 'GBM block' : 'GBM ok'} / ${snrBlocks ? 'SNR block' : 'SNR ok'}</span>`
        : '';
      entryHintHtml = `
        <div class="entry-hint ${blocked ? 'entry-hint-blocked' : 'entry-hint-ok'}">
          <div class="entry-hint-title">${title}</div>
          <div class="entry-hint-meta">
            <span>SNR: <strong>${snr}</strong></span>
            ${hybridMeta}
            <span>band: [${Number(entryHint.min_pct).toFixed(2)}%, ${Number(entryHint.max_pct).toFixed(2)}%]</span>
            ${blockTags}
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
    policySummary.innerHTML = `
      <div class="policy-card ${actionClass}">
        <div class="policy-action">${action}</div>
        <div class="policy-meta">
          <span>eval: <strong>${evalHorizon}</strong></span>
          <span>stack: <strong>${runLabel}</strong></span>
          <span>P(hold/long/short): ${holdPct}% / ${longPct}% / ${shortPct}%</span>
        </div>
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
    maybeNotifyEntryAllowedAlert(entryHint);
  }

  function loadInference(symbol, limit) {
    return API.inference({ symbol_id: symbol, limit })
      .then(response => {
        const status = response.status != null ? String(response.status) : 'ok';
        if (status === 'computing') {
          lastInferenceStatus = 'computing';
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
            lastExitPolicy = response.exit_policy || lastExitPolicy;
            lastExitTransformer = response.exit_transformer || lastExitTransformer;
            renderInference(
              predictions,
              symbol,
              policy,
              entryHint,
              true,
            );
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
          lastExitTransformer = null;
          return;
        }
        if (!response.predictions) {
          stopInferenceStatusTick();
          lastInferenceStatus = null;
          setInferenceWarning('Offline inference artifact missing predictions');
          return;
        }
        lastInferenceStatus = 'ok';
        lastInferenceCompletedAtMs = response.inference_completed_at_ms != null
          ? Number(response.inference_completed_at_ms)
          : (response.updated_at_ms != null ? Number(response.updated_at_ms) : null);
        lastComputingStartedAtMs = null;
        lastExitPolicy = response.exit_policy || null;
        lastExitTransformer = response.exit_transformer || null;
        renderInference(
          response.predictions,
          symbol,
          response.policy || null,
          response.entry_hint || null,
          false,
        );
        renderInferenceStatusBar();
        startInferenceStatusTick();
      })
      .catch(e => {
        setInferenceWarning(parseErrorDetail(e.message));
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

  function buildTradeJournalParams(symbol) {
    const params = { symbol_id: symbol };
    const markPrice = getMarkPriceForJournal();
    if (markPrice != null) {
      params.mark_price = String(markPrice);
    }
    if (lastJournalBarsElapsed != null) {
      params.bars_elapsed = String(lastJournalBarsElapsed);
    }
    return params;
  }

  function fetchAndApplyTradeJournal(symbol) {
    return API.tradeJournal(buildTradeJournalParams(symbol))
      .then(state => {
        applyJournalState(state, symbol);
        return state;
      });
  }

  function pollJournalBarsElapsed(symbol, entryStartTradeId) {
    if (refreshBarsElapsedInFlight) {
      return Promise.resolve();
    }
    refreshBarsElapsedInFlight = true;
    return API.tradeJournalBarsElapsed({
      symbol_id: symbol,
      entry_start_trade_id: String(entryStartTradeId),
    })
      .then(data => {
        if (data.bars_elapsed == null) {
          return;
        }
        const nextBarsElapsed = Number(data.bars_elapsed);
        if (lastJournalBarsElapsed === nextBarsElapsed) {
          return;
        }
        lastJournalBarsElapsed = nextBarsElapsed;
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

  function refreshInferencePanel() {
    const symbol = symbolSelect.value;
    if (!symbol || loadInferenceInFlight) {
      return Promise.resolve();
    }
    loadInferenceInFlight = true;
    const limit = limitInput.value ? parseInt(limitInput.value, 10) : config.defaultLimit;
    return loadInference(symbol, limit)
      .finally(() => {
        loadInferenceInFlight = false;
      });
  }

  function buildEntryJournalPayload(side) {
    const evalHorizon = getJournalEvalHorizon();
    let entryPolicy = null;
    if (lastPolicy) {
      entryPolicy = {
        action: lastPolicy.action != null ? String(lastPolicy.action) : null,
        eval_horizon: lastPolicy.eval_horizon != null ? String(lastPolicy.eval_horizon) : evalHorizon,
        run_label: lastPolicy.run_label != null ? String(lastPolicy.run_label) : null,
        probabilities: lastPolicy.probabilities || null,
        entry_hint: lastEntryHint || null,
      };
    }
    let entryPredictions = null;
    if (lastPredictions) {
      entryPredictions = {};
      Object.keys(lastPredictions).forEach((key) => {
        const value = Number(lastPredictions[key]);
        if (Number.isFinite(value)) {
          entryPredictions[key] = value;
        }
      });
    }
    return { entryPolicy, entryPredictions };
  }

  function getBarsLimit() {
    return limitInput.value ? parseInt(limitInput.value, 10) : config.defaultLimit;
  }

  function buildExitPolicyPayload(symbol, openPos) {
    if (!openPos || !openPos.side || !openPos.metrics) return null;
    if (!lastPredictions || !openPos.entry_predictions || !openPos.entry_policy) return null;
    if (!lastPolicy || !lastPolicy.probabilities) return null;
    if (!openPos.entry_policy.probabilities) return null;
    if (!exitPolicyBySymbol[symbol]) return null;

    const m = openPos.metrics;
    const linearMetric = (value) => {
      const n = Number(value);
      if (!Number.isFinite(n)) return 0;
      return n / 100.0;
    };

    return {
      symbol_id: symbol,
      side: openPos.side,
      eval_horizon: openPos.eval_horizon,
      bars_held: m.bars_elapsed,
      entry_predictions: openPos.entry_predictions,
      current_predictions: lastPredictions,
      entry_policy: openPos.entry_policy,
      current_policy: {
        action: lastPolicy.action,
        action_id: lastPolicy.action_id,
        probabilities: lastPolicy.probabilities,
      },
      unrealized_linear: linearMetric(m.unrealized_net_return_pct),
      mfe_linear: linearMetric(m.mfe_net_return_pct),
      mae_linear: linearMetric(m.mae_net_return_pct),
      giveback_linear: linearMetric(m.giveback_net_return_pct),
    };
  }

  function buildExitTransformerPayload(symbol, openPos, barsLimit) {
    if (!openPos || !openPos.side || !openPos.metrics) return null;
    if (!lastPredictions || !openPos.entry_predictions) return null;
    if (!exitTransformerBySymbol[symbol]) return null;
    if (inferenceMinRows > 0 && Number(barsLimit) < inferenceMinRows) return null;

    const m = openPos.metrics;
    const linearMetric = (value) => {
      const n = Number(value);
      if (!Number.isFinite(n)) return 0;
      return n / 100.0;
    };

    return {
      symbol_id: symbol,
      side: openPos.side,
      eval_horizon: openPos.eval_horizon,
      bars_held: m.bars_elapsed,
      bars_limit: barsLimit,
      entry_predictions: openPos.entry_predictions,
      current_predictions: lastPredictions,
      unrealized_linear: linearMetric(m.unrealized_net_return_pct),
      mfe_linear: linearMetric(m.mfe_net_return_pct),
      mae_linear: linearMetric(m.mae_net_return_pct),
      giveback_linear: linearMetric(m.giveback_net_return_pct),
    };
  }

  function refreshExitPolicy(symbol, openPos) {
    if (!exitGbmEnabled) {
      lastExitPolicy = null;
      return Promise.resolve(null);
    }
    const payload = buildExitPolicyPayload(symbol, openPos);
    if (!payload) {
      lastExitPolicy = null;
      return Promise.resolve(null);
    }
    return API.exitPolicy(payload)
      .then(result => {
        lastExitPolicy = result;
        return result;
      })
      .catch(() => {
        lastExitPolicy = null;
        return null;
      });
  }

  function refreshExitTransformer(symbol, openPos, barsLimit) {
    if (!exitTransformerEnabled) {
      lastExitTransformer = null;
      return Promise.resolve(null);
    }
    const payload = buildExitTransformerPayload(symbol, openPos, barsLimit);
    if (!payload) {
      lastExitTransformer = null;
      return Promise.resolve(null);
    }
    return API.exitTransformer(payload)
      .then(result => {
        lastExitTransformer = result;
        return result;
      })
      .catch(() => {
        lastExitTransformer = null;
        return null;
      });
  }

  function renderExitPolicyCard(exitPolicy) {
    if (!exitPolicy || exitPolicy.close_probability == null) return '';

    const pClose = Number(exitPolicy.close_probability);
    const threshold = Number(exitPolicy.close_probability_threshold);
    const pClosePct = Number.isFinite(pClose) ? (pClose * 100).toFixed(1) : '—';
    const thresholdPct = Number.isFinite(threshold) ? (threshold * 100).toFixed(1) : '—';
    const action = String(exitPolicy.action || 'hold').toUpperCase();
    const runLabel = exitPolicy.run_label || '—';
    const minHold = exitPolicy.min_hold_steps != null ? exitPolicy.min_hold_steps : '—';
    const barsHeld = exitPolicy.bars_held != null ? exitPolicy.bars_held : '—';
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

  function renderExitTransformerCard(exitTransformer) {
    if (!exitTransformer || exitTransformer.predicted_delta_pnl == null) return '';

    const deltaPnl = Number(exitTransformer.predicted_delta_pnl);
    const threshold = Number(exitTransformer.delta_pnl_threshold);
    const deltaPct = Number.isFinite(deltaPnl) ? formatPct(deltaPnl * 100) : '—';
    const thresholdPct = Number.isFinite(threshold) ? formatPct(threshold * 100) : '—';
    const action = String(exitTransformer.action || 'hold').toUpperCase();
    const runLabel = exitTransformer.run_label || '—';
    const minHold = exitTransformer.min_hold_steps != null ? exitTransformer.min_hold_steps : '—';
    const barsHeld = exitTransformer.bars_held != null ? exitTransformer.bars_held : '—';
    let actionClass = 'exit-policy-hold';
    if (action === 'CLOSE') actionClass = 'exit-policy-close';

    return `
      <div class="exit-policy-card exit-policy-transformer ${actionClass}">
        <div class="exit-policy-action">Exit Transformer v2: ${action}</div>
        <div class="exit-policy-meta">
          <span>Δpnl: <strong>${deltaPct}</strong> / порог ${thresholdPct}</span>
          <span>stack: <strong>${runLabel}</strong></span>
          <span>бары: <strong>${barsHeld}</strong> (min ${minHold})</span>
        </div>
      </div>
    `;
  }

  function applyJournalDefaultsFromState(state) {
    if (state.defaults) {
      journalDefaults = state.defaults;
      if (!loadJournalSettingsFromStorage()) {
        if (JOURNAL_EVAL_HORIZON_OPTIONS.includes(journalDefaults.eval_horizon)) {
          journalEvalHorizonSelect.value = journalDefaults.eval_horizon;
        }
      }
    }
  }

  function syncJournalBarsElapsedFromState(state) {
    const openPos = state.open_position;
    if (openPos && openPos.metrics && openPos.metrics.bars_elapsed != null) {
      lastJournalBarsElapsed = Number(openPos.metrics.bars_elapsed);
      return;
    }
    lastJournalBarsElapsed = null;
  }

  function applyJournalState(state, symbol) {
    syncJournalBarsElapsedFromState(state);
    applyJournalDefaultsFromState(state);
    renderTradeJournal(state, symbol);
    const openPos = state.open_position;
    if (
      openPos
      && openPos.symbol_id === symbol
      && !openPos.metrics
      && openPos.entry_start_trade_id != null
    ) {
      pollJournalBarsElapsed(symbol, Number(openPos.entry_start_trade_id));
    }
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
          lastJournalBarsElapsed = null;
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
    if (openPos) {
      resetEntryAllowedAlertState();
    }
    const closedTrades = state.closed_trades || [];
    const totalPnl = state.total_realized_pnl_usd;
    const closedCount = state.closed_trades_count || 0;

    tradeJournalTotals.textContent = `Закрыто: ${closedCount} · Realized: ${formatUsd(totalPnl)}`;

    const policySide = policyActionToSide(lastPolicy && lastPolicy.action);
    const hasOpen = openPos && openPos.side;
    const sideLabel = hasOpen ? openPos.side.toUpperCase() : 'FLAT';
    const sideClass = hasOpen ? (openPos.side === 'long' ? 'position-long' : 'position-short') : 'position-flat';

    let metricsHtml = '';
    let alertHtml = '';
    if (hasOpen && openPos.metrics) {
      const m = openPos.metrics;
      maybeNotifyHorizonAlert(openPos, m.at_target_horizon);
      maybeNotifyExitGbmAlert(openPos, lastExitPolicy);
      maybeNotifyExitTransformerAlert(openPos, lastExitTransformer);
      updateExitOverlaySession(openPos);
      const progressClass = m.at_target_horizon ? 'at-target' : '';
      const evalHorizonLabel = openPos.eval_horizon || `x${m.eval_horizon_steps}`;
      alertHtml = m.at_target_horizon
        ? `<div class="trade-journal-alert">⚠ Достигнут горизонт ${evalHorizonLabel} — по policy пора выходить</div>`
        : '';
      if (exitGbmEnabled && lastExitPolicy && lastExitPolicy.suggest_close) {
        const thresholdPct = (Number(lastExitPolicy.close_probability_threshold) * 100).toFixed(1);
        const pClosePct = (Number(lastExitPolicy.close_probability) * 100).toFixed(1);
        alertHtml += `<div class="trade-journal-alert">⏹ Exit GBM: P(close)=${pClosePct}% ≥ ${thresholdPct}% — рассмотри ранний выход</div>`;
      }
      if (exitTransformerEnabled && lastExitTransformer && lastExitTransformer.suggest_close) {
        const thresholdPct = formatPct(Number(lastExitTransformer.delta_pnl_threshold) * 100);
        const deltaPct = formatPct(Number(lastExitTransformer.predicted_delta_pnl) * 100);
        alertHtml += `<div class="trade-journal-alert">⏹ Exit Transformer v2: Δpnl=${deltaPct} > ${thresholdPct} — рассмотри ранний выход</div>`;
      }
      const policySideNow = policyActionToSide(lastPolicy && lastPolicy.action);
      if (policySideNow && policySideNow !== openPos.side) {
        alertHtml += `<div class="trade-journal-alert">↔ Policy flip → ${policySideNow.toUpperCase()}, открыт ${String(openPos.side).toUpperCase()} — рассмотри exit</div>`;
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
      metricsHtml = `
        ${renderExitPolicyCard(lastExitPolicy)}
        ${renderExitTransformerCard(lastExitTransformer)}
        ${renderEntryPredictionMetrics(openPos, m)}
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
    } else if (!hasOpen) {
      resetHorizonAlertState();
      resetExitGbmAlertState();
      resetExitTransformerAlertState();
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
        alertHtml += `<div class="trade-journal-alert">⏸ ${hintLabel}: ${blockReason}</div>`;
      }
    }

    syncJournalSettingsDisabled(hasOpen);
    updateJournalActionStatusUi();
    const isPolicyOnlyHint = lastEntryHint && lastEntryHint.hint_mode === 'policy_only';
    const snrAllowLong = isPolicyOnlyHint
      || !lastEntryHint
      || lastEntryHint.allow_long !== false;
    const snrAllowShort = isPolicyOnlyHint
      || !lastEntryHint
      || lastEntryHint.allow_short !== false;
    const entryFillReady = journalEntryFillValid();
    const exitFillReady = journalExitFillValid();
    const actionsLocked = journalActionPending !== null;
    const canEnterLong = !actionsLocked && !hasOpen && policySide === 'long' && snrAllowLong && entryFillReady;
    const canEnterShort = !actionsLocked && !hasOpen && policySide === 'short' && snrAllowShort && entryFillReady;
    const canEnterManual = !actionsLocked && !hasOpen && !policySide && entryFillReady;
    const canExit = !actionsLocked && hasOpen && exitFillReady;
    const canDiscard = !actionsLocked && hasOpen;

    const longLabel = journalActionPending && journalActionPending.actionKey === 'entry-long'
      ? 'Входим LONG…'
      : 'Вошёл LONG';
    const shortLabel = journalActionPending && journalActionPending.actionKey === 'entry-short'
      ? 'Входим SHORT…'
      : 'Вошёл SHORT';
    const exitLabel = journalActionPending && journalActionPending.actionKey === 'exit'
      ? 'Выходим…'
      : 'Вышел';
    const discardLabel = journalActionPending && journalActionPending.actionKey === 'discard'
      ? 'Сброс…'
      : 'Сброс';

    const longLoadingClass = journalActionPending && journalActionPending.actionKey === 'entry-long' ? ' is-loading' : '';
    const shortLoadingClass = journalActionPending && journalActionPending.actionKey === 'entry-short' ? ' is-loading' : '';
    const exitLoadingClass = journalActionPending && journalActionPending.actionKey === 'exit' ? ' is-loading' : '';
    const discardLoadingClass = journalActionPending && journalActionPending.actionKey === 'discard' ? ' is-loading' : '';

    const actionsHtml = `
      <div class="trade-journal-actions">
        <button type="button" id="btnJournalEntryLong" class="btn-entry-long${longLoadingClass}" ${canEnterLong || canEnterManual ? '' : 'disabled'} title="${canEnterLong ? '' : (!entryFillReady && !hasOpen ? 'Укажите цену и notional с биржи' : (policySide === 'long' && !snrAllowLong ? 'Entry hint: подождать' : ''))}">
          ${longLabel}
        </button>
        <button type="button" id="btnJournalEntryShort" class="btn-entry-short${shortLoadingClass}" ${canEnterShort || canEnterManual ? '' : 'disabled'} title="${canEnterShort ? '' : (!entryFillReady && !hasOpen ? 'Укажите цену и notional с биржи' : (policySide === 'short' && !snrAllowShort ? 'Entry hint: подождать' : ''))}">
          ${shortLabel}
        </button>
        <button type="button" id="btnJournalExit" class="btn-exit${exitLoadingClass}" ${canExit ? '' : 'disabled'} title="${canExit ? '' : (hasOpen && !exitFillReady ? 'Укажите цену выхода с биржи' : '')}">
          ${exitLabel}
        </button>
        <button type="button" id="btnJournalDiscard"${discardLoadingClass ? ` class="btn-discard${discardLoadingClass}"` : ''} ${canDiscard ? '' : 'disabled'} title="Сбросить без записи в историю">
          ${discardLabel}
        </button>
      </div>
    `;

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
        ${alertHtml}
        ${metricsHtml}
        ${actionsHtml}
      </div>
      ${historyHtml}
    `;

    const btnLong = document.getElementById('btnJournalEntryLong');
    const btnShort = document.getElementById('btnJournalEntryShort');
    const btnExit = document.getElementById('btnJournalExit');
    const btnDiscard = document.getElementById('btnJournalDiscard');

    if (btnLong) {
      btnLong.addEventListener('click', () => handleJournalEntry(symbol, 'long'));
    }
    if (btnShort) {
      btnShort.addEventListener('click', () => handleJournalEntry(symbol, 'short'));
    }
    if (btnExit) {
      btnExit.addEventListener('click', () => handleJournalExit(symbol));
    }
    if (btnDiscard) {
      btnDiscard.addEventListener('click', () => handleJournalDiscard(symbol));
    }

    isFirstJournalLoad = false;
  }

  function handleJournalEntry(symbol, side) {
    if (journalActionPending) return;
    if (!latestX1Bar) {
      setStatus('Нет x1-бара для привязки по времени', true);
      return;
    }
    const entryPrice = parseJournalFillPrice();
    const notionalUsd = parseJournalNotionalUsd();
    if (entryPrice === null || notionalUsd === null) {
      setStatus('Укажите цену и notional с биржи', true);
      return;
    }
    const actionKey = side === 'long' ? 'entry-long' : 'entry-short';
    const pendingMessage = side === 'long' ? 'Входим LONG…' : 'Входим SHORT…';
    setJournalActionPending({ actionKey, message: pendingMessage });
    setStatus('Запись входа…');

    const policyAction = lastPolicy && lastPolicy.action ? String(lastPolicy.action).toUpperCase() : null;
    const evalHorizon = getJournalEvalHorizon();
    const entrySnapshot = buildEntryJournalPayload(side);

    API.tradeJournalEntry({
      symbol_id: symbol,
      side,
      entry_price: entryPrice,
      entry_start_trade_id: Number(latestX1Bar.start_trade_id),
      entry_timestamp_ms: Number(latestX1Bar.start_timestamp_ms),
      eval_horizon: evalHorizon,
      notional_usd: notionalUsd,
      policy_action: policyAction,
      notes: '',
      entry_policy: entrySnapshot.entryPolicy,
      entry_predictions: entrySnapshot.entryPredictions,
    })
      .then((state) => {
        resetJournalFillFields();
        resetHorizonAlertState();
        resetExitGbmAlertState();
        resetExitTransformerAlertState();
        resetExitOverlaySession();
        setJournalActionPending(null);
        applyJournalState(state, symbol);
        setStatus('Вход записан');
      })
      .catch(e => {
        setJournalActionPending(null);
        setStatus('Entry: ' + parseErrorDetail(e.message), true);
      });
  }

  function handleJournalExit(symbol) {
    if (journalActionPending) return;
    if (!latestX1Bar) {
      setStatus('Нет x1-бара для привязки по времени', true);
      return;
    }
    const exitPrice = parseJournalFillPrice();
    if (exitPrice === null) {
      setStatus('Укажите цену выхода с биржи', true);
      return;
    }
    setJournalActionPending({ actionKey: 'exit', message: 'Выходим…' });
    setStatus('Запись выхода…');

    API.tradeJournalExit({
      exit_price: exitPrice,
      exit_start_trade_id: Number(latestX1Bar.start_trade_id),
      exit_timestamp_ms: Number(latestX1Bar.end_timestamp_ms || latestX1Bar.start_timestamp_ms),
      notes: '',
      exit_overlay: buildExitOverlayPayloadForClose(),
    })
      .then((state) => {
        resetJournalFillFields();
        resetHorizonAlertState();
        resetExitGbmAlertState();
        resetExitTransformerAlertState();
        resetExitOverlaySession();
        setJournalActionPending(null);
        applyJournalState(state, symbol);
        setStatus('Выход записан');
      })
      .catch(e => {
        setJournalActionPending(null);
        setStatus('Exit: ' + parseErrorDetail(e.message), true);
      });
  }

  function handleJournalDiscard(symbol) {
    if (journalActionPending) return;
    if (!window.confirm('Сбросить открытую позицию без записи в историю?')) return;
    setJournalActionPending({ actionKey: 'discard', message: 'Сброс…' });
    setStatus('Сброс позиции…');

    API.tradeJournalDiscardOpen()
      .then((state) => {
        resetJournalFillFields();
        resetHorizonAlertState();
        resetExitGbmAlertState();
        resetExitTransformerAlertState();
        resetExitOverlaySession();
        setJournalActionPending(null);
        applyJournalState(state, symbol);
        setStatus('Позиция сброшена');
      })
      .catch(e => {
        setJournalActionPending(null);
        setStatus('Discard: ' + parseErrorDetail(e.message), true);
      });
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
          const defaultScale = config.defaultScale != null ? String(config.defaultScale) : 'x2048';
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
    if (scaleSelect.value === TRADE_RESEARCH_SCALE) return true;
    scaleSelect.value = TRADE_RESEARCH_SCALE;
    return false;
  }

  function removeTradeResearchLineSeries() {
    if (!chart) return;
    tradeResearchLineSeries.forEach((series) => chart.removeSeries(series));
    tradeResearchLineSeries = [];
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

  function resolveSegmentCandle(startTradeId, timestampMs, candleByTime) {
    const exactBar = findBarForStartTradeId(startTradeId);
    const barCandle = candleFromBar(exactBar, candleByTime);
    if (barCandle) {
      return barCandle;
    }
    if (timestampMs == null) {
      return null;
    }
    const time = Math.floor(Number(timestampMs) / 1000);
    return candleByTime.get(time) || null;
  }

  function addTradeResearchLinesToChart() {
    if (!chart || !isTradeResearchEnabled()) {
      removeTradeResearchLineSeries();
      return;
    }
    removeTradeResearchLineSeries();
    if (candleDataByIndex.length === 0 || tradeResearchSegments.length === 0) {
      return;
    }
    const candleByTime = buildCandleByTimeLookup();
    const LineSeries = LightweightCharts.LineSeries;
    if (!LineSeries) return;
    const opts = {
      priceScaleId: 'right',
      lineWidth: 2,
      lastValueVisible: false,
      priceLineVisible: false,
    };
    let renderedCount = 0;
    let missingEntryCount = 0;
    let missingExitCount = 0;
    for (const segment of tradeResearchSegments) {
      const entryCandle = resolveSegmentCandle(
        segment.entry_start_trade_id,
        segment.entry_timestamp_ms,
        candleByTime,
      );
      const exitCandle = resolveSegmentCandle(
        segment.exit_start_trade_id,
        segment.exit_timestamp_ms,
        candleByTime,
      );
      if (!entryCandle) {
        missingEntryCount = missingEntryCount + 1;
        continue;
      }
      if (!exitCandle) {
        missingExitCount = missingExitCount + 1;
        continue;
      }
      const predTargetOpen = Number(segment.pred_target_open);
      if (!Number.isFinite(predTargetOpen) || predTargetOpen <= 0) {
        missingExitCount = missingExitCount + 1;
        continue;
      }
      const entryOpen = Number(segment.entry_open);
      const entryBar = findBarForStartTradeId(segment.entry_start_trade_id);
      let entryStartPrice = entryOpen;
      if (!Number.isFinite(entryStartPrice) || entryStartPrice <= 0) {
        if (entryBar && entryBar.open_price != null) {
          entryStartPrice = Number(entryBar.open_price);
        }
      }
      if (!Number.isFinite(entryStartPrice) || entryStartPrice <= 0) {
        missingEntryCount = missingEntryCount + 1;
        continue;
      }
      const color = segment.action === 'long' ? '#26a69a' : '#ef5350';
      const series = chart.addSeries(LineSeries, { ...opts, color });
      series.setData([
        { time: entryCandle.time, value: entryStartPrice },
        { time: exitCandle.time, value: predTargetOpen },
      ]);
      tradeResearchLineSeries.push(series);
      renderedCount = renderedCount + 1;
    }
    console.info(
      '[trade-research] render',
      {
        segments: tradeResearchSegments.length,
        rendered: renderedCount,
        missingEntry: missingEntryCount,
        missingExit: missingExitCount,
        chartBars: barsData.length,
        chartCandles: candleDataByIndex.length,
      },
    );
  }

  function getVisibleStartTradeIdRange() {
    if (barsData.length === 0) {
      return { min: null, max: null };
    }
    return {
      min: Number(barsData[0].start_trade_id),
      max: Number(barsData[barsData.length - 1].start_trade_id),
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

  function loadTradeResearch(symbol) {
    if (!isTradeResearchEnabled()) {
      tradeResearchSegments = [];
      removeTradeResearchLineSeries();
      return Promise.resolve(null);
    }
    if (scaleSelect.value !== TRADE_RESEARCH_SCALE) {
      tradeResearchSegments = [];
      removeTradeResearchLineSeries();
      setStatus(`Trade research: выберите масштаб ${TRADE_RESEARCH_SCALE}`, true);
      return Promise.resolve(null);
    }
    const horizonSteps = Number(TRADE_RESEARCH_EVAL_HORIZON.slice(1));
    const visibleRange = getVisibleStartTradeIdRange();
    setStatus('Trade research: онлайн-инференс на полной истории…');
    const requestParams = {
      symbol_id: symbol,
      eval_horizon: TRADE_RESEARCH_EVAL_HORIZON,
      step_bars: horizonSteps,
    };
    if (visibleRange.min != null) {
      requestParams.visible_min_start_trade_id = visibleRange.min;
    }
    if (visibleRange.max != null) {
      requestParams.visible_max_start_trade_id = visibleRange.max;
    }
    return API.tradeResearch(requestParams)
      .then((payload) => {
        tradeResearchSegments = Array.isArray(payload.segments) ? payload.segments : [];
        addTradeResearchLinesToChart();
        const sampleCount = payload.sample_count != null ? payload.sample_count : '?';
        const tradeCount = payload.trade_inference_count != null ? payload.trade_inference_count : '?';
        const entryAllowedCount = payload.entry_allowed_count != null ? payload.entry_allowed_count : '?';
        const barsLoaded = payload.bars_loaded != null ? payload.bars_loaded : '?';
        const backtestNetPnl = payload.sequential_backtest_net_pnl_sum != null
          ? payload.sequential_backtest_net_pnl_sum
          : payload.backtest_net_pnl_sum;
        const backtestTradeCount = payload.sequential_backtest_trade_count != null
          ? payload.sequential_backtest_trade_count
          : payload.backtest_trade_count;
        const backtestVisibleNetPnl = payload.sequential_backtest_visible_net_pnl_sum != null
          ? payload.sequential_backtest_visible_net_pnl_sum
          : payload.backtest_visible_net_pnl_sum;
        const backtestVisibleTradeCount = payload.sequential_backtest_visible_trade_count != null
          ? payload.sequential_backtest_visible_trade_count
          : payload.backtest_visible_trade_count;
        const gridBacktestNetPnl = payload.grid_backtest_net_pnl_sum;
        const gridBacktestTradeCount = payload.grid_backtest_trade_count;
        const pnlStride = payload.pnl_stride != null ? payload.pnl_stride : '?';
        let statusText =
          `Trade research: ${tradeResearchSegments.length} на графике ` +
          `(${entryAllowedCount} entry ok / ${tradeCount} policy long/short из ${sampleCount} точек @ ${TRADE_RESEARCH_EVAL_HORIZON}, ` +
          `контекст ${barsLoaded} x1)`;
        if (backtestNetPnl != null && backtestTradeCount != null) {
          statusText =
            statusText +
            `, sequential PnL ${formatTradeResearchNetPnl(backtestNetPnl)} ` +
            `(${backtestTradeCount} trades, stride ${pnlStride})`;
          if (gridBacktestNetPnl != null && gridBacktestTradeCount != null) {
            statusText =
              statusText +
              `; grid ${formatTradeResearchNetPnl(gridBacktestNetPnl)} ` +
              `(${gridBacktestTradeCount})`;
          }
          if (
            backtestVisibleNetPnl != null &&
            backtestVisibleTradeCount != null &&
            tradeResearchSegments.length > 0 &&
            Number(backtestVisibleTradeCount) !== Number(backtestTradeCount)
          ) {
            statusText =
              statusText +
              ` / ${formatTradeResearchNetPnl(backtestVisibleNetPnl)} ` +
              `(${backtestVisibleTradeCount} на графике)`;
          }
        }
        if (payload.sample_selection_note) {
          statusText = statusText + ` [${payload.sample_selection_note}]`;
        }
        if (tradeResearchSegments.length === 0 && Number(entryAllowedCount) > 0) {
          statusText = statusText + ' — entry ok, но линии не привязались к свечам';
        } else if (tradeResearchSegments.length === 0 && Number(tradeCount) > 0) {
          statusText = statusText + ' — SNR/gate заблокировали entry или exit вне графика';
        } else if (tradeResearchSegments.length === 0 && Number(sampleCount) > 0) {
          statusText = statusText + ' — нет long/short в видимом окне';
        }
        const renderedLines = tradeResearchLineSeries.length;
        if (tradeResearchSegments.length > 0 && renderedLines === 0) {
          statusText = statusText + ' — линии не привязались к свечам (см. console [trade-research] render)';
        } else if (renderedLines > 0 && renderedLines < tradeResearchSegments.length) {
          statusText = statusText + ` — нарисовано ${renderedLines}/${tradeResearchSegments.length} линий`;
        }
        setStatus(statusText);
        return payload;
      })
      .catch((error) => {
        tradeResearchSegments = [];
        removeTradeResearchLineSeries();
        setStatus('Trade research: ' + parseErrorDetail(error.message), true);
        return null;
      });
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
      if (tradeResearchSegments.length > 0) {
        addTradeResearchLinesToChart();
      } else {
        removeTradeResearchLineSeries();
      }
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
    loadBarsInFlight = true;

    const effectiveScale = scaleSelect.value;
    const promise = isDowLevel(effectiveScale)
      ? API.dow({ symbol_id: symbol, limit, level: parseDowLevel(effectiveScale) })
      : API.bars({ symbol_id: symbol, limit, scale: effectiveScale });

    promise
      .then(data => {
        applyBarsToChart(data, effectiveScale);
        updateLatestX1BarFromBarsData(data, effectiveScale);
        if (!isTradeResearchEnabled()) {
          return null;
        }
        return loadTradeResearch(symbol);
      })
      .catch(e => {
        setStatus('Ошибка: ' + e.message, true);
      })
      .finally(() => {
        loadBarsInFlight = false;
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
      exitTransformerBySymbol = config.exitTransformerBySymbol || {};
      exitGbmEnabled = Boolean(config.exitGbmEnabled);
      exitTransformerEnabled = Boolean(config.exitTransformerEnabled);
      checkpointPathBySymbol = config.checkpointPathBySymbol || {};
      if (config.defaultLimit) {
        limitInput.placeholder = config.defaultLimit;
        limitInput.value = config.defaultLimit;
      }
      await initDropdowns();
      initCvdWindowDropdown();
      initJournalSettingsControls();
      initJournalSoundControls();
      requestNotificationPermissionIfNeeded();
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
    } catch (e) {
      setStatus('Ошибка инициализации: ' + e.message, true);
    }
  })();
})();
