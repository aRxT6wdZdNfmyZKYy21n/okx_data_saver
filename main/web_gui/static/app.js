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
  };

  let config = { defaultLimit: 100000, refreshIntervalSec: 30 };
  let chart = null;
  let candleSeries = null;
  let barsData = [];
  /** Объёмы по индексу свечи (1:1 с candleData после сортировки и слияния по time) */
  let volumeDataByCandleIndex = [];
  let refreshTimer = null;
  const scaleSelect = document.getElementById('scale');
  const symbolSelect = document.getElementById('symbol');
  const limitInput = document.getElementById('limit');
  const loadBtn = document.getElementById('load');
  const autoRefreshCheck = document.getElementById('autoRefresh');
  const statusEl = document.getElementById('status');
  const chartDiv = document.getElementById('chart');
  const concentrationCanvas = document.getElementById('concentrationCanvas');
  const concentrationPanel = document.getElementById('concentrationPanel');
  const volumeCanvas = document.getElementById('volumeCanvas');
  const volumePanel = document.getElementById('volumePanel');
  const dowStub = document.getElementById('dowStub');

  const SCALE_NAMES = ['x1', 'x2', 'x4', 'x8', 'x16', 'x32', 'x64', 'x128', 'x256', 'x512', 'x1024', 'x2048'];

  function setStatus(text, isError = false) {
    statusEl.textContent = text;
    statusEl.style.color = isError ? '#ef5350' : '';
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
  function barsToCandleAndVolumeData(bars) {
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
      return { time, open, high, low, close, totalVolume, buyPct };
    });
    raw.sort((a, b) => a.time - b.time);
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
    const volumeDataNormalized = volumeData.map((v, i) => {
      const total = v.totalVolumeSum;
      const buyPct = total > 0 ? v.buyVolumeSum / total : 0;
      const open = candleData[i].open;
      const close = candleData[i].close;
      const volumeDelta = 2 * v.buyVolumeSum - total;
      const closePriceDeltaPercent = open !== 0 ? (close - open) / open : 0;
      const concentration = closePriceDeltaPercent * volumeDelta;
      return {
        buy_volume_percent: buyPct,
        sell_volume_percent: 1 - buyPct,
        total_volume: total,
        concentration,
      };
    });
    return { candleData, volumeData: volumeDataNormalized };
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
    });
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
      const c = volumeDataByCandleIndex[i].concentration;
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
      const concentration = b.concentration != null ? b.concentration : 0;
      if (!Number.isFinite(concentration)) continue;
      const x = Math.round(ts.logicalToCoordinate(i));
      const barW = Math.max(1, Math.round(ts.logicalToCoordinate(i + 1)) - x);
      const norm = concentration / maxAbs;
      const barH = Math.abs(norm) * halfH;
      if (barH < 0.5) continue;
      if (concentration >= 0) {
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
      const v = volumeDataByCandleIndex[i].total_volume;
      if (v == null || !Number.isFinite(v) || v < 0) {
        throw new Error(`volumeData[${i}]: ожидается конечный total_volume >= 0, получено ${v}`);
      }
      maxVolume = Math.max(maxVolume, v);
    }
    if (maxVolume <= 0) maxVolume = 1;

    ctx.clearRect(0, 0, w, h);
    for (let i = from; i < to; i++) {
      const b = volumeDataByCandleIndex[i];
      const x = Math.round(ts.logicalToCoordinate(i));
      const barW = Math.max(1, Math.round(ts.logicalToCoordinate(i + 1)) - x);
      const totalH = (b.total_volume / maxVolume) * (h - 4);
      if (b.buy_volume_percent == null || !Number.isFinite(b.buy_volume_percent) || b.buy_volume_percent < 0 || b.buy_volume_percent > 1) {
        throw new Error(`volumeData[${i}]: ожидается buy_volume_percent в [0,1], получено ${b.buy_volume_percent}`);
      }
      if (b.sell_volume_percent == null || !Number.isFinite(b.sell_volume_percent) || b.sell_volume_percent < 0 || b.sell_volume_percent > 1) {
        throw new Error(`volumeData[${i}]: ожидается sell_volume_percent в [0,1], получено ${b.sell_volume_percent}`);
      }
      const buyPct = b.buy_volume_percent;
      const sellPct = b.sell_volume_percent;
      const buyH = totalH * buyPct;
      const sellH = totalH * sellPct;

      if (buyH > 0) {
        ctx.fillStyle = '#26a69a';
        ctx.fillRect(x, h - buyH, barW, buyH);
      }
      if (sellH > 0) {
        ctx.fillStyle = '#ef5350';
        ctx.fillRect(x, h - buyH - sellH, barW, sellH);
      }
    }
  }

  function applyBarsToChart(data) {
    barsData = data.bars || [];
    setStatus(`Загружено ${data.count} баров`);

    const { candleData, volumeData } = barsToCandleAndVolumeData(barsData);
    if (candleData.length === 0) return;

    volumeDataByCandleIndex = volumeData;
    ensureChart();
    candleSeries.setData(candleData);
    chart.timeScale().fitContent();

    concentrationPanel.classList.remove('hidden');
    const w = Math.max(chartDiv.clientWidth || 800, 1);
    const h = Math.max(chartDiv.clientHeight || 400, 300);
    chart.applyOptions({ width: w, height: chartDiv.clientHeight });
    concentrationCanvas.width = concentrationPanel.clientWidth;
    concentrationCanvas.height = concentrationPanel.clientHeight;

    requestAnimationFrame(() => {
      const range = chart.timeScale().getVisibleLogicalRange();
      if (range) {
        drawVolumeBars(range);
        drawConcentrationBars(range);
      }
    });
  }

  function loadBars() {
    const scale = scaleSelect.value;
    if (!scale) return;

    dowStub.classList.add('hidden');
    volumePanel.classList.remove('hidden');
    concentrationPanel.classList.remove('hidden');

    const symbol = symbolSelect.value;
    if (!symbol) return;
    const limit = limitInput.value ? parseInt(limitInput.value, 10) : config.defaultLimit;
    setStatus('Загрузка…');

    const promise = isDowLevel(scale)
      ? API.dow({ symbol_id: symbol, limit, level: parseDowLevel(scale) })
      : API.bars({ symbol_id: symbol, limit, scale });

    promise
      .then(data => applyBarsToChart(data))
      .catch(e => {
        setStatus('Ошибка: ' + e.message, true);
      });
  }

  function startAutoRefresh() {
    if (refreshTimer) clearInterval(refreshTimer);
    if (!autoRefreshCheck.checked) return;
    refreshTimer = setInterval(loadBars, config.refreshIntervalSec * 1000);
  }

  loadBtn.addEventListener('click', loadBars);
  scaleSelect.addEventListener('change', () => {
    dowStub.classList.add('hidden');
    volumePanel.classList.remove('hidden');
    concentrationPanel.classList.remove('hidden');
    loadBars();
  });
  autoRefreshCheck.addEventListener('change', startAutoRefresh);

  (async function init() {
    try {
      config = await API.config();
      if (config.defaultLimit) limitInput.placeholder = config.defaultLimit;
      await initDropdowns();
      chartDiv.style.height = '100%';
      volumeCanvas.width = volumePanel.clientWidth;
      volumeCanvas.height = volumePanel.clientHeight;
      window.addEventListener('resize', () => {
        if (chart) chart.applyOptions({ width: chartDiv.clientWidth, height: chartDiv.clientHeight });
        volumeCanvas.width = volumePanel.clientWidth;
        volumeCanvas.height = volumePanel.clientHeight;
        concentrationCanvas.width = concentrationPanel.clientWidth;
        concentrationCanvas.height = concentrationPanel.clientHeight;
        const range = chart && chart.timeScale().getVisibleLogicalRange();
        if (range) {
          drawVolumeBars(range);
          drawConcentrationBars(range);
        }
      });
      loadBars();
      startAutoRefresh();
    } catch (e) {
      setStatus('Ошибка инициализации: ' + e.message, true);
    }
  })();
})();
