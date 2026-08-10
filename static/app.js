const state = {
  timeframe: 5,
  universe: [],
  poll: null,
  sizingMode: "capital",
  sizingDirty: false,
  saveTimer: null,
  countTimer: null,
  applyingRemote: false,
  settingsDirty: false,
  listsDirty: false,
  dirtyVersion: 0,
  polling: false,
};

const $ = (id) => document.getElementById(id);

function enabledStrategies() {
  const enabled = {};
  document.querySelectorAll(".strategy").forEach((input) => {
    enabled[input.dataset.key] = input.checked;
  });
  return enabled;
}

function payload(options = {}) {
  const data = {
    universe_text: $("universeText").value,
    long_text: $("longText").value,
    short_text: $("shortText").value,
    settings: {
      timeframe: state.timeframe,
      sizing_mode: state.sizingMode,
      per_trade_capital: Number($("perTradeCapital").value || 10000),
      per_trade_risk: Number($("perTradeRisk").value || 20),
      dry_run: $("dryRun").checked,
      sma_period: Number($("smaPeriod").value || 20),
      near_high_percent: Number($("nearHigh").value || 70),
      volume_multiplier: Number($("volumeMultiplier").value || 8),
      fixed_sl_percent: Number($("fixedSl").value || 0.7),
      scalper_sl_percent: Number($("scalperSl").value || 0.8),
      scalper_pyramiding: $("scalperPyramiding").checked,
      scalper_max_adds: Number($("scalperMaxAdds").value || 2),
      risk_reward: Number($("riskReward").value || 3),
      use_sector_filter: $("sectorFilter").checked,
      top_sector_count: Number($("topSectorCount").value || 2),
      enabled: enabledStrategies(),
    },
  };
  if (options.includeCredentials) {
    data.credentials = {
      client_id: $("clientId").value.trim(),
      access_token: $("accessToken").value.trim(),
    };
  }
  return data;
}

async function api(path, options = {}) {
  const { timeoutMs: rawTimeoutMs, ...fetchOptions } = options;
  const controller = new AbortController();
  const timeoutMs = Number(rawTimeoutMs || 12000);
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(path, {
      headers: { "Content-Type": "application/json" },
      ...fetchOptions,
      signal: controller.signal,
    });
    const text = await res.text();
    let data = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch (_err) {
      data = { detail: text || res.statusText || "Request failed" };
    }
    if (!res.ok) throw new Error(data.detail || "Request failed");
    return data;
  } finally {
    clearTimeout(timer);
  }
}

function td(value) {
  return `<td>${escapeHtml(value ?? "-")}</td>`;
}

async function saveConfig() {
  const data = await api("/api/config", { method: "POST", body: JSON.stringify(payload({ includeCredentials: true })) });
  state.settingsDirty = false;
  state.listsDirty = false;
  state.sizingDirty = false;
  render(data);
}

async function saveSettings(saveVersion = state.dirtyVersion) {
  const data = await api("/api/config", { method: "POST", body: JSON.stringify(payload()) });
  if (saveVersion === state.dirtyVersion) {
    state.settingsDirty = false;
    state.listsDirty = false;
    state.sizingDirty = false;
    render(data);
  }
}

function scheduleSave(delay = 350, options = {}) {
  if (state.applyingRemote) return;
  state.dirtyVersion += 1;
  state.settingsDirty = true;
  if (options.lists) state.listsDirty = true;
  clearTimeout(state.saveTimer);
  const saveVersion = state.dirtyVersion;
  state.saveTimer = setTimeout(async () => {
    try {
      await saveSettings(saveVersion);
    } catch (_err) {}
  }, delay);
}

async function refreshCounts() {
  const universe = await api("/api/extract-symbols", { method: "POST", body: JSON.stringify({ text: $("universeText").value }), timeoutMs: 5000 });
  state.universe = universe.symbols;
  $("universeCount").textContent = universe.symbols.length;
  const long = await api("/api/extract-symbols", { method: "POST", body: JSON.stringify({ text: $("longText").value }), timeoutMs: 5000 });
  $("longCount").textContent = long.symbols.length;
  const short = await api("/api/extract-symbols", { method: "POST", body: JSON.stringify({ text: $("shortText").value }), timeoutMs: 5000 });
  $("shortCount").textContent = short.symbols.length;
}

async function refreshPremarketReport() {
  const report = await api("/api/premarket-report");
  renderCacheReport(report);
}

function render(data) {
  state.applyingRemote = true;
  try {
  $("runState").textContent = data.running ? "Running" : "Stopped";
  $("runState").className = data.running ? "pill" : "pill bad";
  $("feedState").textContent = data.market_connected ? "Feed Live" : "Feed Off";
  $("feedState").className = data.market_connected ? "pill" : "pill muted";
  const orderFallback = Boolean(data.order_last_error && String(data.order_last_error).includes("fallback"));
  $("orderState").textContent = data.order_connected ? "Orders Live" : (orderFallback ? "Orders Fallback" : (data.order_last_error ? "Orders Reconnecting" : "Orders Off"));
  $("orderState").className = data.order_connected ? "pill" : (orderFallback ? "pill warn" : "pill muted");
  renderBrokerAlert(data.broker_reconcile || {});

  $("positions").innerHTML = (data.positions || []).map((p) => `
    <tr>
      ${td(p.symbol)}${td(p.side)}${td(p.strategy)}
      ${td(p.quantity || "-")}${td(money(p.entry_price))}${td(money(p.stop_loss))}${td(money(p.target))}${td(p.status)}
    </tr>
  `).join("") || `<tr><td colspan="8">No trades yet</td></tr>`;

  $("instruments").innerHTML = (data.latest || []).map((r) => `
    <tr>
      ${td(r.symbol)}${td(r.security_id)}${td(r.sector || "-")}
      ${td(money(r.price))}${td(r.candles_1m)}${td(r.candles_5m)}${td(r.locked ? "LOCKED" : "-")}
    </tr>
  `).join("") || `<tr><td colspan="7">No resolved instruments yet</td></tr>`;

  $("sectors").innerHTML = (data.sectors || []).map((r) => `
    <tr>
      ${td(r.sector)}${td(r.security_id)}
      ${td(money(r.price))}${td(money(r.previous_close))}${td(percent(r.change))}
    </tr>
  `).join("") || `<tr><td colspan="5">Sector index cache not ready</td></tr>`;

  const events = [...(data.events || [])];
  if (data.last_error) events.unshift({ kind: "ERROR", time: new Date().toISOString(), message: data.last_error });
  if (data.order_last_error && !data.order_connected) events.unshift({ kind: "INFO", time: new Date().toISOString(), message: data.order_last_error });
  if (data.premarket?.message) events.unshift({ kind: "INFO", time: new Date().toISOString(), message: `Premarket: ${data.premarket.message} (${data.premarket.progress || 0}%)` });
  if (data.reconcile?.message) events.unshift({ kind: "INFO", time: data.reconcile.last_run || new Date().toISOString(), message: `Reconcile: ${data.reconcile.message}` });
  if (data.broker_reconcile?.message) {
    const brokerProblem = !data.broker_reconcile.running && (
      data.broker_reconcile.entries_blocked_until_reconcile ||
      (data.broker_reconcile.locked_symbols || []).length
      || (data.broker_reconcile.mismatches || []).length
      || (data.broker_reconcile.pending_orders || []).length
    );
    events.unshift({
      kind: brokerProblem ? "ERROR" : "INFO",
      time: data.broker_reconcile.last_run || new Date().toISOString(),
      message: `Broker: ${data.broker_reconcile.message}`,
    });
  }
  $("events").innerHTML = events.slice(0, 40).map((e) => `
    <div class="event ${eventKindClass(e.kind)}">
      <time>${formatTime(e.time)} - ${escapeHtml(e.kind || "")}</time>
      <div>${escapeHtml(e.message || "")}</div>
    </div>
  `).join("");

  if (data.settings && !state.settingsDirty) {
    state.timeframe = data.settings.timeframe || state.timeframe;
    document.querySelectorAll(".tf").forEach((btn) => btn.classList.toggle("active", Number(btn.dataset.value) === state.timeframe));
    if (!state.sizingDirty) {
      state.sizingMode = data.settings.sizing_mode || "capital";
    }
    applySizingMode();
    setInputValue("perTradeCapital", data.settings.per_trade_capital || 10000);
    setInputValue("perTradeRisk", data.settings.per_trade_risk || 20);
    setInputValue("smaPeriod", data.settings.sma_period || 20);
    setInputValue("nearHigh", data.settings.near_high_percent || 70);
    setInputValue("volumeMultiplier", data.settings.volume_multiplier || 8);
    setInputValue("fixedSl", data.settings.fixed_sl_percent || 0.7);
    setInputValue("scalperSl", data.settings.scalper_sl_percent || 0.8);
    setInputValue("riskReward", data.settings.risk_reward || 3);
    setInputValue("scalperMaxAdds", data.settings.scalper_max_adds ?? 2);
    setInputValue("topSectorCount", data.settings.top_sector_count || 2);
    $("dryRun").checked = Boolean(data.settings.dry_run);
    $("scalperPyramiding").checked = Boolean(data.settings.scalper_pyramiding);
    $("sectorFilter").checked = Boolean(data.settings.use_sector_filter);
    document.querySelectorAll(".strategy").forEach((input) => {
      input.checked = Boolean((data.settings.enabled || {})[input.dataset.key]);
    });
  }
  renderPremarketSummary(data.premarket);
  renderCacheReport(data.premarket);
  if (document.activeElement !== $("clientId") && !$("clientId").value && data.credential_client_id) {
    $("clientId").value = data.credential_client_id;
  }
  if (!state.listsDirty && document.activeElement !== $("universeText") && !$("universeText").value && (data.universe_symbols || []).length) {
    $("universeText").value = data.universe_symbols.join("\n");
  }
  if (!state.listsDirty && document.activeElement !== $("longText") && !$("longText").value && (data.long_symbols || []).length) {
    $("longText").value = data.long_symbols.join("\n");
  }
  if (!state.listsDirty && document.activeElement !== $("shortText") && !$("shortText").value && (data.short_symbols || []).length) {
    $("shortText").value = data.short_symbols.join("\n");
  }
  } finally {
    state.applyingRemote = false;
  }
}

function renderBrokerAlert(status) {
  const alert = $("brokerAlert");
  const locked = status.locked_symbols || [];
  const mismatches = status.mismatches || [];
  const pending = status.pending_orders || [];
  const entriesBlocked = Boolean(status.entries_blocked_until_reconcile);
  if (!entriesBlocked && !locked.length && !mismatches.length && !pending.length) {
    alert.classList.add("hidden");
    alert.innerHTML = "";
    return;
  }
  alert.classList.remove("hidden");
  const mismatchText = mismatches.map((row) => `${row.symbol}: app ${row.app_qty}, broker ${row.broker_qty}`).join(" | ");
  const pendingText = pending.map((row) => `${row.symbol}: ${row.status} ${row.quantity}`).join(" | ");
  const blockedFor = locked.length ? locked.join(", ") : (entriesBlocked ? "all new entries" : "none");
  alert.innerHTML = `
    <strong>Broker/app reconciliation lock active</strong>
    <div>${escapeHtml(status.message || "New entries are blocked until broker reconcile succeeds.")}</div>
    <div>New entries are blocked for: ${escapeHtml(blockedFor)}</div>
    ${mismatchText ? `<div>Mismatches: ${escapeHtml(mismatchText)}</div>` : ""}
    ${pendingText ? `<div>Pending orders: ${escapeHtml(pendingText)}</div>` : ""}
  `;
}

function renderPremarketSummary(premarket) {
  const summary = premarket?.summary || {};
  const stockTotal = Number(summary.stock_total || 0);
  const sectorTotal = Number(summary.sector_total || 0);
  const stockCached = Number(summary.stock_cached || 0);
  const sectorCached = Number(summary.sector_cached || 0);
  $("premarketStats").textContent = stockTotal || sectorTotal
    ? `Cache: stocks ${stockCached}/${stockTotal}, sectors ${sectorCached}/${sectorTotal}`
    : "Cache idle";
  const stockMissing = summary.stock_missing_all || summary.stock_missing_symbols || [];
  const sectorMissing = summary.sector_missing_all || summary.sector_missing_symbols || [];
  $("missingStocks").textContent = stockMissing.length ? `Missing stocks: ${stockMissing.length}` : "";
  $("missingSectors").textContent = sectorMissing.length ? `Missing sectors: ${sectorMissing.join(", ")}` : "";
}

function renderCacheReport(source) {
  const report = source || {};
  const summary = report.summary || {};
  const missing = report.missing || {};
  const stocks = missing.stocks || summary.stock_missing_all || summary.stock_missing_symbols || [];
  const sectors = missing.sectors || summary.sector_missing_all || summary.sector_missing_symbols || [];
  const stockTotal = Number(summary.stock_total || 0);
  const sectorTotal = Number(summary.sector_total || 0);
  const stockCached = Number(summary.stock_cached || 0);
  const sectorCached = Number(summary.sector_cached || 0);
  $("cacheStockCount").textContent = `${stockCached}/${stockTotal}`;
  $("cacheSectorCount").textContent = `${sectorCached}/${sectorTotal}`;
  $("cacheStockMissingCount").textContent = String(Number(summary.stock_missing ?? stocks.length ?? 0));
  $("cacheSectorMissingCount").textContent = String(Number(summary.sector_missing ?? sectors.length ?? 0));
  $("cacheStockMissingList").textContent = stocks.length ? stocks.join(", ") : "No missing stocks";
  $("cacheSectorMissingList").textContent = sectors.length ? sectors.join(", ") : "No missing sectors";
  const generated = report.generated_at ? `Report generated ${formatTime(report.generated_at)}` : "";
  const runningMessage = source?.running ? source.message : "";
  const idleMessage = stockTotal || sectorTotal
    ? `Cache report loaded | missing stocks ${stocks.length} | missing sectors ${sectors.length}`
    : "";
  const message = report.message || runningMessage || generated || idleMessage;
  $("cacheReportStatus").textContent = message || "Report not loaded";
  const reportFile = report.cache_file || source?.report_file || "data/premarket_cache_report.json";
  $("cacheReportFile").textContent = report.cache_file || source?.report_file ? `Report file: ${reportFile}` : "";
}

function setInputValue(id, value) {
  const input = $(id);
  if (document.activeElement !== input) {
    input.value = value;
  }
}

function applySizingMode() {
  $("capitalSizing").checked = state.sizingMode !== "risk";
  $("riskSizing").checked = state.sizingMode === "risk";
}

async function setSizingMode(mode) {
  state.sizingMode = mode === "risk" ? "risk" : "capital";
  state.sizingDirty = true;
  state.settingsDirty = true;
  state.dirtyVersion += 1;
  const saveVersion = state.dirtyVersion;
  applySizingMode();
  try {
    await saveSettings(saveVersion);
  } finally {
    if (saveVersion === state.dirtyVersion) {
      state.sizingDirty = false;
    }
  }
}

function money(value) {
  const number = Number(value || 0);
  return number ? number.toFixed(2) : "-";
}

function percent(value) {
  if (value === null || value === undefined) return "-";
  const number = Number(value);
  return Number.isFinite(number) ? `${number.toFixed(2)}%` : "-";
}

function formatTime(value) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "-" : date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;" }[ch]));
}

function eventKindClass(value) {
  const kind = String(value || "").toUpperCase();
  return ["INFO", "WARN", "ERROR", "ENTRY", "EXIT", "ARMED"].includes(kind) ? kind : "INFO";
}

document.querySelectorAll(".tf").forEach((button) => {
  button.addEventListener("click", () => {
    state.timeframe = Number(button.dataset.value);
    document.querySelectorAll(".tf").forEach((btn) => btn.classList.toggle("active", btn === button));
    scheduleSave(0);
  });
});

["universeText", "longText", "shortText"].forEach((id) => {
  $(id).addEventListener("input", () => {
    clearTimeout(state.countTimer);
    state.countTimer = setTimeout(refreshCounts, 250);
    scheduleSave(650, { lists: true });
  });
});

["perTradeCapital", "perTradeRisk", "smaPeriod", "nearHigh", "volumeMultiplier", "fixedSl", "scalperSl", "riskReward", "scalperMaxAdds", "topSectorCount"].forEach((id) => {
  $(id).addEventListener("input", () => scheduleSave(400));
  $(id).addEventListener("change", () => scheduleSave(0));
});

["dryRun", "scalperPyramiding", "sectorFilter"].forEach((id) => {
  $(id).addEventListener("change", () => scheduleSave(0));
});

document.querySelectorAll(".strategy").forEach((input) => {
  input.addEventListener("change", () => scheduleSave(0));
});

$("saveConfig").addEventListener("click", async () => {
  try { await saveConfig(); } catch (err) { alert(err.message); }
});
$("capitalSizing").addEventListener("change", async () => {
  if ($("capitalSizing").checked) {
    try { await setSizingMode("capital"); } catch (err) { alert(err.message); }
  }
});
$("riskSizing").addEventListener("change", async () => {
  if ($("riskSizing").checked) {
    try { await setSizingMode("risk"); } catch (err) { alert(err.message); }
  }
});
$("startAlgo").addEventListener("click", async () => {
  try { render(await api("/api/start", { method: "POST", body: JSON.stringify(payload({ includeCredentials: true })) })); } catch (err) { alert(err.message); }
});
$("stopAlgo").addEventListener("click", async () => {
  try { render(await api("/api/stop", { method: "POST", body: "{}" })); } catch (err) { alert(err.message); }
});
$("premarket").addEventListener("click", async () => {
  try {
    const data = await api("/api/premarket-cache", { method: "POST", body: JSON.stringify({ ...payload({ includeCredentials: true }), force: false }) });
    render(data);
    if (!data.premarket?.running) {
      await refreshPremarketReport();
    }
  } catch (err) { alert(err.message); }
});
$("refreshReport").addEventListener("click", async () => {
  try { await refreshPremarketReport(); } catch (err) { alert(err.message); }
});
$("brokerReconcile").addEventListener("click", async () => {
  try { render(await api("/api/broker-reconcile", { method: "POST", body: "{}" })); } catch (err) { alert(err.message); }
});

async function poll() {
  if (state.polling) return;
  state.polling = true;
  try { render(await api("/api/status", { timeoutMs: 5000 })); } catch (_err) {
  } finally {
    state.polling = false;
  }
}

poll();
refreshPremarketReport().catch(() => {});
state.poll = setInterval(poll, 1500);
