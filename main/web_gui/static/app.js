(function () {
  'use strict';

  const API = {
    async get(path) {
      const r = await fetch(path);
      if (!r.ok) throw new Error(await r.text());
      return r.json();
    },
    async config() { return this.get('/api/config'); },
    async symbols() { return this.get('/api/symbols'); },
    async scales() { return this.get('/api/scales'); },
    async dowLevels() { return this.get('/api/dow_levels'); },
    async bars(params) {
      const q = new URLSearchParams(params).toString();
      return this.get('/api/bars?' + q);
    },
  };

  let config = { defaultLimit: 100000, refreshIntervalSec: 30 };
  let chart = null;
  let candleSeries = null;
  let barsData = [];
  let refreshTimer = null;
  const scaleSelect = document.getElementById('scale');
  const symbolSelect = document.getElementById('symbol');
  const limitInput = document.getElementById('limit');
  const loadBtn = document.getElementById('load');
  const autoRefreshCheck = document.getElementById('autoRefresh');
  const statusEl = document.getElementById('status');
  const chartDiv = document.getElementById('chart');
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

  function initDropdowns() {
    API.symbols().then(symbols => {
      symbolSelect.innerHTML = '';
      symbols.forEach(s => {
        const opt = document.createElement('option');
        opt.value = s.id;
        opt.textContent = s.name;
        symbolSelect.appendChild(opt);
      });
    }).catch(e => setStatus('Ошибка загрузки символов: ' + e.message, true));

    API.scales().then(scales => {
      API.dowLevels().then(dowLevels => {
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
    }).catch(e => setStatus('Ошибка загрузки масштабов: ' + e.message, true));
  }

  function barsToCandleData(bars) {
    return bars.map(b => ({
      time: Math.floor(b.start_timestamp_ms / 1000),
      open: b.open_price,
      high: b.high_price,
      low: b.low_price,
      close: b.close_price,
    }));
  }

  function ensureChart() {
    if (chart) return;
    chart = LightweightCharts.createChart(chartDiv, {
      layout: { background: { type: 'solid', color: '#131722' }, textColor: '#d1d4dc' },
      grid: { vertLines: { color: '#2a2e39' }, horzLines: { color: '#2a2e39' } },
      width: chartDiv.clientWidth,
      height: chartDiv.clientHeight,
      timeScale: { timeVisible: true, secondsVisible: false },
      rightPriceScale: { borderColor: '#2a2e39' },
    });
    candleSeries = chart.addSeries(LightweightCharts.CandlestickSeries, {
      upColor: '#26a69a',
      downColor: '#ef5350',
      borderVisible: false,
    });
    chart.timeScale().fitContent();

    chart.timeScale().subscribeVisibleLogicalRangeChange(range => {
      if (!range || barsData.length === 0 || volumePanel.classList.contains('hidden')) return;
      drawVolumeBars(range);
    });
  }

  function drawVolumeBars(visibleRange) {
    if (!visibleRange || barsData.length === 0) return;
    const ctx = volumeCanvas.getContext('2d');
    const w = volumeCanvas.width;
    const h = volumeCanvas.height;
    if (!w || !h) return;

    const from = Math.max(0, Math.floor(visibleRange.from));
    const to = Math.min(barsData.length, Math.ceil(visibleRange.to));
    const count = to - from;
    if (count <= 0) return;

    const barWidth = w / count;
    let maxLog = 0;
    for (let i = from; i < to; i++) {
      const v = barsData[i].total_volume_log2;
      if (v != null && isFinite(v)) maxLog = Math.max(maxLog, v);
    }
    if (maxLog <= 0) maxLog = 1;

    ctx.clearRect(0, 0, w, h);
    for (let i = from; i < to; i++) {
      const b = barsData[i];
      const x = (i - from) * barWidth;
      const totalH = ((b.total_volume_log2 != null && isFinite(b.total_volume_log2)) ? b.total_volume_log2 : 0) / maxLog * (h - 4);
      const buyPct = b.buy_volume_percent != null ? Math.max(0, Math.min(1, b.buy_volume_percent)) : 0;
      const sellPct = b.sell_volume_percent != null ? Math.max(0, Math.min(1, b.sell_volume_percent)) : 0;
      const buyH = totalH * buyPct;
      const sellH = totalH * sellPct;

      if (buyH > 0) {
        ctx.fillStyle = '#26a69a';
        ctx.fillRect(x, h - buyH, Math.max(1, barWidth - 1), buyH);
      }
      if (sellH > 0) {
        ctx.fillStyle = '#ef5350';
        ctx.fillRect(x, h - buyH - sellH, Math.max(1, barWidth - 1), sellH);
      }
    }
  }

  function loadBars() {
    const scale = scaleSelect.value;
    if (isDowLevel(scale)) {
      dowStub.classList.remove('hidden');
      volumePanel.classList.add('hidden');
      if (chart) chart.remove();
      chart = null;
      candleSeries = null;
      setStatus('Выбран уровень теории Доу (в разработке)');
      return;
    }
    dowStub.classList.add('hidden');
    volumePanel.classList.remove('hidden');

    const symbol = symbolSelect.value;
    const limit = limitInput.value ? parseInt(limitInput.value, 10) : config.defaultLimit;
    setStatus('Загрузка…');

    API.bars({ symbol_id: symbol, limit, scale })
      .then(data => {
        barsData = data.bars || [];
        setStatus(`Загружено ${data.count} баров`);

        ensureChart();
        const candleData = barsToCandleData(barsData);
        candleSeries.setData(candleData);
        chart.timeScale().fitContent();

        requestAnimationFrame(() => {
          const range = chart.timeScale().getVisibleLogicalRange();
          if (range) drawVolumeBars(range);
        });
      })
      .catch(e => {
        setStatus('Ошибка: ' + e.message, true);
      });
  }

  function startAutoRefresh() {
    if (refreshTimer) clearInterval(refreshTimer);
    if (!autoRefreshCheck.checked) return;
    refreshTimer = setInterval(() => {
      if (isDowLevel(scaleSelect.value)) return;
      loadBars();
    }, config.refreshIntervalSec * 1000);
  }

  loadBtn.addEventListener('click', loadBars);
  scaleSelect.addEventListener('change', () => {
    if (isDowLevel(scaleSelect.value)) {
      dowStub.classList.remove('hidden');
      volumePanel.classList.add('hidden');
      if (chart) { chart.remove(); chart = null; candleSeries = null; }
    } else {
      dowStub.classList.add('hidden');
      volumePanel.classList.remove('hidden');
      loadBars();
    }
  });
  autoRefreshCheck.addEventListener('change', startAutoRefresh);

  (async function init() {
    try {
      config = await API.config();
      if (config.defaultLimit) limitInput.placeholder = config.defaultLimit;
      initDropdowns();
      ensureChart();
      chartDiv.style.height = '100%';
      volumeCanvas.width = volumePanel.clientWidth;
      volumeCanvas.height = volumePanel.clientHeight;
      window.addEventListener('resize', () => {
        if (chart) chart.applyOptions({ width: chartDiv.clientWidth, height: chartDiv.clientHeight });
        volumeCanvas.width = volumePanel.clientWidth;
        volumeCanvas.height = volumePanel.clientHeight;
        const range = chart && chart.timeScale().getVisibleLogicalRange();
        if (range) drawVolumeBars(range);
      });
      loadBars();
      startAutoRefresh();
    } catch (e) {
      setStatus('Ошибка инициализации: ' + e.message, true);
    }
  })();
})();
