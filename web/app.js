/* global L, Plotly */

const API_BASE = (() => {
  const override = new URLSearchParams(window.location.search).get("api");
  if (override) return override.replace(/\/$/, "");

  if (window.location.port === "8000") return window.location.origin;
  return `${window.location.protocol}//${window.location.hostname}:8000`;
})();

const statusEl = document.getElementById("status");
const apiBaseEl = document.getElementById("api-base");
const themeSelectEl = document.getElementById("theme-select");
const liveIndicatorEl = document.getElementById("live-indicator");
const cacheIndicatorEl = document.getElementById("cache-indicator");
const copyLinkButton = document.getElementById("copy-link");

const sidebarEl = document.getElementById("sidebar");
const sidebarResizerEl = document.getElementById("sidebar-resizer");
const chartResizerEl = document.getElementById("chart-resizer");

const freshnessPillEl = document.getElementById("freshness-pill");
const freshnessTextEl = document.getElementById("freshness-text");
const liveAutoRefreshEl = document.getElementById("live-auto-refresh");
const liveIntervalSecondsEl = document.getElementById("live-interval-seconds");
const liveRefreshEl = document.getElementById("live-refresh");

const segmentSearchEl = document.getElementById("segment-search");
const segmentSelectEl = document.getElementById("segment-select");
const segmentInfoEl = document.getElementById("segment-info");

const corridorSelectEl = document.getElementById("corridor-select");
const corridorInfoEl = document.getElementById("corridor-info");

const entityTypeEl = document.getElementById("entity-type");
const followLatestEl = document.getElementById("follow-latest");
const liveWindowHoursEl = document.getElementById("live-window-hours");
const startEl = document.getElementById("start");
const endEl = document.getElementById("end");
const minutesEl = document.getElementById("minutes");
const loadButton = document.getElementById("load");

const rankingTypeEl = document.getElementById("ranking-type");
const rankingLimitEl = document.getElementById("ranking-limit");
const loadRankingsButton = document.getElementById("load-rankings");
const rankingsEl = document.getElementById("rankings");
const rankingSearchEl = document.getElementById("ranking-search");
const rankingSortEl = document.getElementById("ranking-sort");
const rankingsHintEl = document.getElementById("rankings-hint");
const rankingsHintTextEl = document.getElementById("rankings-hint-text");
const rankingsQuickMinSamplesEl = document.getElementById("rankings-quick-min-samples");
const rankingsQuick24hEl = document.getElementById("rankings-quick-24h");

const loadEventsButton = document.getElementById("load-events");
const eventsEl = document.getElementById("events");
const eventInfoEl = document.getElementById("event-info");
const eventsAutoEl = document.getElementById("events-auto");
const eventsTypeEl = document.getElementById("events-type");
const eventsWithinRangeEl = document.getElementById("events-within-range");
const eventsSearchEl = document.getElementById("events-search");
const clearEventsButton = document.getElementById("clear-events");
const eventsHintEl = document.getElementById("events-hint");
const eventsHintTextEl = document.getElementById("events-hint-text");
const eventsRelaxFiltersButton = document.getElementById("events-relax-filters");

const hotspotMetricEl = document.getElementById("hotspot-metric");
const loadHotspotsButton = document.getElementById("load-hotspots");
const clearHotspotsButton = document.getElementById("clear-hotspots");
const hotspotInfoEl = document.getElementById("hotspot-info");
const hotspotAutoEl = document.getElementById("hotspot-auto");
const hotspotHintEl = document.getElementById("hotspot-hint");
const hotspotHintTextEl = document.getElementById("hotspot-hint-text");
const hotspotQuickMinSamplesEl = document.getElementById("hotspot-quick-min-samples");
const hotspotQuick24hEl = document.getElementById("hotspot-quick-24h");
const hotspotLegendEl = document.getElementById("hotspot-legend");
const hotspotLegendTitleEl = document.getElementById("hotspot-legend-title");
const hotspotLegendBarEl = document.getElementById("hotspot-legend-bar");
const hotspotLegendMinEl = document.getElementById("hotspot-legend-min");
const hotspotLegendMidEl = document.getElementById("hotspot-legend-mid");
const hotspotLegendMaxEl = document.getElementById("hotspot-legend-max");

const toggleAnomaliesEl = document.getElementById("toggle-anomalies");
const toggleHotspotsEl = document.getElementById("toggle-hotspots");
const toggleEventsEl = document.getElementById("toggle-events");
const toggleImpactEl = document.getElementById("toggle-impact");

const reliabilityThresholdEl = document.getElementById("reliability-threshold");
const reliabilityMinSamplesEl = document.getElementById("reliability-min-samples");
const reliabilityWeightMeanEl = document.getElementById("reliability-weight-mean");
const reliabilityWeightStdEl = document.getElementById("reliability-weight-std");
const reliabilityWeightCongEl = document.getElementById("reliability-weight-cong");

const anomaliesWindowEl = document.getElementById("anomalies-window");
const anomaliesZEl = document.getElementById("anomalies-z");
const anomaliesDirectionEl = document.getElementById("anomalies-direction");
const anomaliesMaxGapEl = document.getElementById("anomalies-max-gap");
const anomaliesMinEventPointsEl = document.getElementById("anomalies-min-event-points");

const impactRadiusEl = document.getElementById("impact-radius");
const impactMaxSegmentsEl = document.getElementById("impact-max-segments");
const impactBaselineMinutesEl = document.getElementById("impact-baseline-minutes");
const impactRecoveryMinutesEl = document.getElementById("impact-recovery-minutes");
const impactRecoveryRatioEl = document.getElementById("impact-recovery-ratio");
const impactWeightingEl = document.getElementById("impact-weighting");
const impactEndFallbackEl = document.getElementById("impact-end-fallback");
const impactMinBaselineEl = document.getElementById("impact-min-baseline");
const impactMinEventEl = document.getElementById("impact-min-event");

const applySettingsButton = document.getElementById("apply-settings");
const resetSettingsButton = document.getElementById("reset-settings");

const shortcutsOverlayEl = document.getElementById("shortcuts-overlay");
const closeShortcutsButton = document.getElementById("close-shortcuts");

const chartEl = document.getElementById("chart");

apiBaseEl.textContent = API_BASE;

const UI_STORAGE_KEY = "trafficpulse.ui.v1";

function loadUiState() {
  try {
    const raw = localStorage.getItem(UI_STORAGE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch (err) {
    return {};
  }
}

function saveUiState(state) {
  try {
    localStorage.setItem(UI_STORAGE_KEY, JSON.stringify(state));
  } catch (err) {
    // ignore
  }
}

function getNested(obj, path, fallback) {
  const parts = path.split(".");
  let current = obj;
  for (const part of parts) {
    if (!current || typeof current !== "object" || !(part in current)) return fallback;
    current = current[part];
  }
  return current;
}

function setCssVar(name, value) {
  document.documentElement.style.setProperty(name, value);
}

function getCssVar(name, fallback) {
  const source = document.body || document.documentElement;
  const value = window.getComputedStyle(source).getPropertyValue(name);
  const trimmed = String(value || "").trim();
  return trimmed || fallback;
}

function parseHexColor(hex, fallbackRgb) {
  const text = String(hex || "").trim();
  const m = /^#?([0-9a-fA-F]{6})$/.exec(text);
  if (!m) return fallbackRgb;
  const raw = m[1];
  const r = parseInt(raw.slice(0, 2), 16);
  const g = parseInt(raw.slice(2, 4), 16);
  const b = parseInt(raw.slice(4, 6), 16);
  if (![r, g, b].every((n) => Number.isFinite(n))) return fallbackRgb;
  return [r, g, b];
}

function rgba(rgb, alpha) {
  const a = Number(alpha);
  if (!rgb || rgb.length !== 3) return `rgba(0,0,0,${Number.isFinite(a) ? a : 1})`;
  return `rgba(${rgb[0]}, ${rgb[1]}, ${rgb[2]}, ${Number.isFinite(a) ? a : 1})`;
}

function themeColors() {
  const accentHex = getCssVar("--accent", "#2563eb");
  const accent2Hex = getCssVar("--accent-2", "#06b6d4");
  const dangerHex = getCssVar("--danger", "#ef4444");
  const text = getCssVar("--text", "rgba(15, 23, 42, 0.92)");
  const muted = getCssVar("--muted", "rgba(51, 65, 85, 0.78)");
  const panelBorder = getCssVar("--panel-border", "rgba(15, 23, 42, 0.10)");
  return {
    accentHex,
    accent2Hex,
    dangerHex,
    text,
    muted,
    panelBorder,
    accentRgb: parseHexColor(accentHex, [37, 99, 235]),
    accent2Rgb: parseHexColor(accent2Hex, [6, 182, 212]),
    dangerRgb: parseHexColor(dangerHex, [239, 68, 68]),
  };
}

const map = L.map("map", { zoomControl: true }).setView([25.033, 121.5654], 12);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 19,
  attribution: "&copy; OpenStreetMap contributors",
}).addTo(map);

const markers = L.layerGroup().addTo(map);
const eventMarkers = L.layerGroup().addTo(map);
const impactSegmentsLayer = L.layerGroup().addTo(map);
const hotspotsLayer = L.layerGroup().addTo(map);

let uiDefaults = null;
let uiState = loadUiState();
const apiOverrideParam = new URLSearchParams(window.location.search).get("api");
let suppressUrlSync = false;
let urlSyncTimer = null;
let pendingUrlSelection = null;
let urlOverrides = null;

function scheduleUrlSync() {
  if (suppressUrlSync) return;
  if (urlSyncTimer) window.clearTimeout(urlSyncTimer);
  urlSyncTimer = window.setTimeout(() => {
    urlSyncTimer = null;
    syncUrlFromUi();
  }, 120);
}

function parseBoolParam(value) {
  if (value == null) return null;
  const v = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(v)) return true;
  if (["0", "false", "no", "n", "off"].includes(v)) return false;
  return null;
}

function parseNumberParam(value) {
  if (value == null) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function applyUrlStateOverrides() {
  const params = new URLSearchParams(window.location.search);
  if (!params.size) return;

  urlOverrides = {
    theme: params.get("theme"),
    minutes: params.get("minutes"),
    follow_latest: parseBoolParam(params.get("follow_latest")),
    window_h: parseNumberParam(params.get("window_h")),
    start: params.get("start"),
    end: params.get("end"),
    hotspot_metric: params.get("hotspot_metric"),
    ranking_type: params.get("ranking_type"),
    ranking_limit: params.get("ranking_limit"),
    ranking_sort: params.get("ranking_sort"),
    min_samples: parseNumberParam(params.get("min_samples")),
    threshold_kph: parseNumberParam(params.get("threshold_kph")),
    entity: params.get("entity"),
    segment_id: params.get("segment_id"),
    corridor_id: params.get("corridor_id"),
  };

  const theme = urlOverrides.theme;
  if (theme) {
    uiState.layout = uiState.layout || {};
    uiState.layout.theme = theme === "policy" ? "policy" : "product";
  }

  if (urlOverrides.follow_latest != null) {
    uiState.timeseries = uiState.timeseries || {};
    uiState.timeseries.followLatest = urlOverrides.follow_latest;
  }

  if (urlOverrides.window_h != null) {
    uiState.timeseries = uiState.timeseries || {};
    uiState.timeseries.liveWindowHours = Math.round(clamp(urlOverrides.window_h, 1, 168));
  }

  const minSamples = urlOverrides.min_samples;
  const threshold = urlOverrides.threshold_kph;
  if (minSamples != null || threshold != null) {
    uiState.overrides = uiState.overrides || {};
    uiState.overrides.reliability = uiState.overrides.reliability || {};
    if (minSamples != null) uiState.overrides.reliability.min_samples = Math.max(1, Math.trunc(minSamples));
    if (threshold != null) uiState.overrides.reliability.congestion_speed_threshold_kph = Math.max(1, threshold);
  }

  const entity = urlOverrides.entity;
  if (entity === "corridor" && urlOverrides.corridor_id) {
    pendingUrlSelection = { type: "corridor", id: urlOverrides.corridor_id };
  } else if (urlOverrides.segment_id) {
    pendingUrlSelection = { type: "segment", id: urlOverrides.segment_id };
  }
}

function applyUrlOverridesToForm() {
  if (!urlOverrides) return;
  suppressUrlSync = true;
  try {
    if (minutesEl && urlOverrides.minutes) minutesEl.value = String(urlOverrides.minutes);

    if (followLatestEl && urlOverrides.follow_latest != null) followLatestEl.checked = urlOverrides.follow_latest;
    if (liveWindowHoursEl && urlOverrides.window_h != null)
      liveWindowHoursEl.value = String(Math.round(clamp(urlOverrides.window_h, 1, 168)));

    if (startEl && endEl && urlOverrides.start && urlOverrides.end) {
      const startDt = new Date(urlOverrides.start);
      const endDt = new Date(urlOverrides.end);
      if (Number.isFinite(startDt.getTime()) && Number.isFinite(endDt.getTime()) && endDt > startDt) {
        startEl.value = toLocalInputValue(startDt);
        endEl.value = toLocalInputValue(endDt);
        if (followLatestEl) followLatestEl.checked = false;
      }
    }

    if (hotspotMetricEl && urlOverrides.hotspot_metric) hotspotMetricEl.value = String(urlOverrides.hotspot_metric);
    if (rankingTypeEl && urlOverrides.ranking_type) rankingTypeEl.value = String(urlOverrides.ranking_type);
    if (rankingLimitEl && urlOverrides.ranking_limit) rankingLimitEl.value = String(urlOverrides.ranking_limit);
    if (rankingSortEl && urlOverrides.ranking_sort) rankingSortEl.value = String(urlOverrides.ranking_sort);
  } finally {
    suppressUrlSync = false;
  }
}

function syncUrlFromUi() {
  const params = new URLSearchParams();

  if (apiOverrideParam) params.set("api", apiOverrideParam);

  const theme = String(getNested(uiState, "layout.theme", "product"));
  if (theme && theme !== "product") params.set("theme", theme);

  if (entityTypeEl && entityTypeEl.value) params.set("entity", entityTypeEl.value);
  if (selectedSegmentId) params.set("segment_id", String(selectedSegmentId));
  if (selectedCorridorId) params.set("corridor_id", String(selectedCorridorId));

  if (minutesEl && minutesEl.value) params.set("minutes", String(minutesEl.value));

  if (followLatestEl && followLatestEl.checked) params.set("follow_latest", "1");
  if (liveWindowHoursEl && followLatestEl && followLatestEl.checked && liveWindowHoursEl.value) {
    params.set("window_h", String(liveWindowHoursEl.value));
  }

  const range = getIsoRange();
  if (range && (!followLatestEl || !followLatestEl.checked)) {
    params.set("start", range.start);
    params.set("end", range.end);
  }

  if (hotspotMetricEl && hotspotMetricEl.value) params.set("hotspot_metric", String(hotspotMetricEl.value));
  if (rankingTypeEl && rankingTypeEl.value) params.set("ranking_type", String(rankingTypeEl.value));
  if (rankingLimitEl && rankingLimitEl.value) params.set("ranking_limit", String(rankingLimitEl.value));
  if (rankingSortEl && rankingSortEl.value) params.set("ranking_sort", String(rankingSortEl.value));

  const minSamples = reliabilityMinSamplesEl ? parseIntValue(reliabilityMinSamplesEl.value) : null;
  if (minSamples != null) params.set("min_samples", String(minSamples));
  const threshold = reliabilityThresholdEl ? parseNumberValue(reliabilityThresholdEl.value) : null;
  if (threshold != null) params.set("threshold_kph", String(threshold));

  const next = `${window.location.pathname}${params.toString() ? `?${params.toString()}` : ""}`;
  window.history.replaceState({}, "", next);
}

applyUrlStateOverrides();

let showAnomalies = getNested(uiState, "overlays.anomalies", true);
let showHotspots = getNested(uiState, "overlays.hotspots", true);
let showEvents = getNested(uiState, "overlays.events", true);
let showImpact = getNested(uiState, "overlays.impact", true);

let segments = [];
let segmentsById = new Map();
let markerById = new Map();
let selectedSegmentId = null;

let corridors = [];
let corridorsById = new Map();
let selectedCorridorId = null;

let events = [];
let eventsById = new Map();
let eventMarkerById = new Map();
let selectedEventId = null;

let hotspotRows = [];
let hotspotsLoaded = false;
let eventsLoaded = false;
let rankingsLoaded = false;
let latestObservationMs = null;
let latestObservationIso = null;
let latestObservationMinutes = [];
let lastRankings = [];
let lastEventImpact = null;

let lastTimeseries = { entity: null, range: null, minutes: null, points: [], anomalies: null };
let lastEventsClearedReason = null;
let lastHotspotsReason = null;
let lastRankingsReason = null;
let lastEventsReason = null;
let lastUiStatus = null;
let lastUiStatusPrev = null;
let uiStatusStream = null;
let uiStatusStreamRetryMs = 800;
let freshnessTicker = null;
let lastLiveRefreshAtMs = 0;
let lastCacheInfo = { hotspots: null, rankings: null, events: null };
let lastSseState = { mode: "polling", detail: "Polling" };

function setStatus(text) {
  statusEl.textContent = text;
}

function setLiveIndicator({ mode, detail }) {
  lastSseState = { mode: mode || "polling", detail: detail || "Polling" };
  if (!liveIndicatorEl) return;
  liveIndicatorEl.classList.remove("connected", "reconnecting", "polling", "offline");
  const cls =
    lastSseState.mode === "connected"
      ? "connected"
      : lastSseState.mode === "reconnecting"
        ? "reconnecting"
        : lastSseState.mode === "offline"
          ? "offline"
          : "polling";
  liveIndicatorEl.classList.add(cls);
  liveIndicatorEl.textContent = lastSseState.detail;
}

function formatCacheBadge(entry) {
  if (!entry || !entry.status) return "—";
  const ttl = entry.ttl != null && entry.ttl !== "" ? `${entry.ttl}s` : "";
  const ttlPart = ttl ? `(${ttl})` : "";
  return `${entry.status}${ttlPart}`;
}

function updateCacheIndicator() {
  if (!cacheIndicatorEl) return;
  const h = formatCacheBadge(lastCacheInfo.hotspots);
  const r = formatCacheBadge(lastCacheInfo.rankings);
  const e = formatCacheBadge(lastCacheInfo.events);
  cacheIndicatorEl.textContent = `Cache · H:${h} R:${r} E:${e}`;
}

function recordCache(kind, cache) {
  if (!cache) return;
  if (!["hotspots", "rankings", "events"].includes(kind)) return;
  lastCacheInfo = {
    ...lastCacheInfo,
    [kind]: {
      status: cache.status ? String(cache.status).toUpperCase() : null,
      ttl: cache.ttl != null ? String(cache.ttl) : null,
      atMs: Date.now(),
    },
  };
  updateCacheIndicator();
}

function setFreshness({ label, detail, level }) {
  if (!freshnessPillEl || !freshnessTextEl) return;
  freshnessPillEl.textContent = label;
  freshnessTextEl.textContent = detail;
  freshnessPillEl.classList.remove("good", "warn", "bad");
  if (level) freshnessPillEl.classList.add(level);
}

function parseIsoToMs(text) {
  const ms = Date.parse(text);
  return Number.isFinite(ms) ? ms : null;
}

function formatAge(seconds) {
  if (seconds < 60) return `${Math.round(seconds)}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  return `${(seconds / 3600).toFixed(1)}h ago`;
}

function updateFreshnessDisplay() {
  if (!freshnessPillEl) return;
  const minutes = Array.isArray(latestObservationMinutes) ? latestObservationMinutes : [];
  const lastMs = latestObservationMs;
  if (lastMs == null) {
    setFreshness({
      label: latestObservationIso ? "Unknown" : "No data",
      detail: latestObservationIso
        ? `Last timestamp: ${String(latestObservationIso)} (minutes: ${minutes.join(", ") || "?"})`
        : `No observations found. Built minutes: ${minutes.join(", ") || "none"}`,
      level: "bad",
    });
    return;
  }

  const nowMs = Date.now();
  const ageSeconds = Math.max(0, (nowMs - lastMs) / 1000);
  const level = ageSeconds <= 5 * 60 ? "good" : ageSeconds <= 60 * 60 ? "warn" : "bad";
  setFreshness({
    label: "Observations",
    detail: `${formatAge(ageSeconds)} • last=${new Date(lastMs).toISOString()} • minutes=${minutes.join(", ")}`,
    level,
  });
}

function startFreshnessTicker() {
  if (freshnessTicker) window.clearInterval(freshnessTicker);
  freshnessTicker = window.setInterval(() => updateFreshnessDisplay(), 1000);
}

function applyUiStatus(status) {
  if (!status || typeof status !== "object") return;
  lastUiStatusPrev = lastUiStatus;
  lastUiStatus = status;
  const last = status.observations_last_timestamp_utc;
  const minutes = status.observations_minutes_available || [];
  latestObservationIso = last || null;
  latestObservationMinutes = Array.isArray(minutes) ? minutes : [];
  latestObservationMs = last ? parseIsoToMs(last) : null;
  updateFreshnessDisplay();

  applyFollowLatestWindow();

  const config = getLiveConfigFromState();
  if (!config.enabled) return;
  if (!last) return;

  if (status.last_ingest_ok === false) {
    setLiveIndicator({ mode: "offline", detail: "Ingest error" });
  }

  const nowMs = Date.now();
  const minIntervalMs = clamp(Number(config.intervalSeconds) || 60, 5, 3600) * 1000;
  if (nowMs - lastLiveRefreshAtMs < minIntervalMs) return;

  // Only refresh expensive views when the dataset version changed.
  const prevVersion = lastUiStatusPrev?.dataset_version || null;
  const nextVersion = status.dataset_version || null;
  if (prevVersion != null && nextVersion != null && prevVersion === nextVersion) return;
  if (prevVersion == null && nextVersion == null) {
    const prevMs = lastUiStatusPrev?.observations_last_timestamp_utc
      ? parseIsoToMs(lastUiStatusPrev.observations_last_timestamp_utc)
      : null;
    if (prevMs != null && latestObservationMs != null && prevMs === latestObservationMs) return;
  }

  lastLiveRefreshAtMs = nowMs;
  refreshLiveNow({ silent: true, skipStatus: true });
}

async function refreshUiStatus() {
  if (!freshnessPillEl) return;
  try {
    const status = await fetchJson(`${API_BASE}/ui/status`);
    applyUiStatus(status);
  } catch (err) {
    setFreshness({ label: "Error", detail: `Failed to load /ui/status: ${err.message}`, level: "bad" });
  }
}

function isStatusStreamSupported() {
  return typeof window !== "undefined" && typeof window.EventSource !== "undefined";
}

function startStatusStream() {
  if (!isStatusStreamSupported()) return;
  if (uiStatusStream) return;

  const url = new URL(`${API_BASE}/stream/status`);
  url.searchParams.set("interval_seconds", "5");

  setLiveIndicator({ mode: "reconnecting", detail: "SSE connecting" });
  try {
    uiStatusStream = new EventSource(url.toString());
  } catch (err) {
    uiStatusStream = null;
    return;
  }

  uiStatusStream.addEventListener("open", () => {
    setLiveIndicator({ mode: "connected", detail: "SSE connected" });
  });

  uiStatusStream.addEventListener("message", (ev) => {
    try {
      const data = JSON.parse(ev.data);
      applyUiStatus(data);
      uiStatusStreamRetryMs = 800;
    } catch (err) {
      // ignore parse errors
    }
  });

  uiStatusStream.addEventListener("error", () => {
    setLiveIndicator({ mode: "reconnecting", detail: "SSE reconnecting" });
    try {
      uiStatusStream?.close();
    } catch (err) {
      // ignore
    }
    uiStatusStream = null;
    const delay = clamp(uiStatusStreamRetryMs, 500, 30000);
    uiStatusStreamRetryMs = Math.min(30000, Math.round(uiStatusStreamRetryMs * 1.6));
    window.setTimeout(() => startStatusStream(), delay);
  });
}

function stopStatusStream() {
  if (!uiStatusStream) return;
  try {
    uiStatusStream.close();
  } catch (err) {
    // ignore
  }
  uiStatusStream = null;
  setLiveIndicator({ mode: "offline", detail: "SSE closed" });
}

function setLayerVisible(layer, visible) {
  if (visible) {
    if (!map.hasLayer(layer)) layer.addTo(map);
  } else if (map.hasLayer(layer)) {
    map.removeLayer(layer);
  }
}

function applyOverlayVisibility() {
  setLayerVisible(hotspotsLayer, Boolean(showHotspots));
  setLayerVisible(eventMarkers, Boolean(showEvents));
  setLayerVisible(impactSegmentsLayer, Boolean(showImpact));
}

function setHintVisible(el, visible) {
  if (!el) return;
  if (visible) el.classList.remove("hidden");
  else el.classList.add("hidden");
}

function getCurrentRangeOrNull() {
  return getIsoRange();
}

function getRangeHours(range) {
  if (!range) return null;
  const startMs = Date.parse(range.start);
  const endMs = Date.parse(range.end);
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) return null;
  return (endMs - startMs) / (60 * 60 * 1000);
}

function setWindowHours(hours) {
  const clamped = clamp(Number(hours), 1, 168);
  if (followLatestEl && followLatestEl.checked && liveWindowHoursEl) {
    liveWindowHoursEl.value = String(Math.round(clamped));
    uiState.timeseries = uiState.timeseries || {};
    uiState.timeseries.liveWindowHours = Math.round(clamped);
    saveUiState(uiState);
    applyFollowLatestWindow();
    return;
  }

  const endVal = endEl && endEl.value ? new Date(endEl.value) : new Date();
  const endMs = endVal instanceof Date && !Number.isNaN(endVal.getTime()) ? endVal.getTime() : Date.now();
  const startMs = endMs - clamped * 60 * 60 * 1000;
  startEl.value = toLocalInputValue(new Date(startMs));
  endEl.value = toLocalInputValue(new Date(endMs));
}

function getEffectiveMinSamples() {
  const raw = parseIntValue(reliabilityMinSamplesEl?.value);
  if (raw != null) return raw;
  const placeholder = parseIntValue(reliabilityMinSamplesEl?.placeholder);
  return placeholder != null ? placeholder : null;
}

function quickSetMinSamplesOne() {
  if (!reliabilityMinSamplesEl) return;
  reliabilityMinSamplesEl.value = "1";
  applySettingsFromForm({ refresh: true });
  setStatus("Applied quick fix: Min samples = 1.");
}

function quickUse24hWindow() {
  setWindowHours(24);
  if (hotspotsLoaded) loadHotspots();
  if (rankingsLoaded) loadRankings();
  if (lastTimeseries && lastTimeseries.entity && lastTimeseries.entity.id) loadTimeseries();
  if (eventsWithinRangeEl && eventsWithinRangeEl.checked) applyEventsFilters();
  setStatus("Applied quick fix: 24h time window.");
}

function updateHotspotsHint({ empty }) {
  if (!hotspotHintEl) return;
  if (!empty) {
    setHintVisible(hotspotHintEl, false);
    return;
  }

  const range = getCurrentRangeOrNull();
  const hours = getRangeHours(range);
  const minSamples = getEffectiveMinSamples();
  const parts = [];
  if (lastHotspotsReason && lastHotspotsReason.message) parts.push(lastHotspotsReason.message);
  if (hours != null) parts.push(`Window: ${hours.toFixed(1)}h`);
  if (minSamples != null) parts.push(`Min samples: ${minSamples}`);
  parts.push(lastHotspotsReason?.suggestion || "Try: Set Min samples = 1, or use a wider time window (24h).");
  if (hotspotHintTextEl) hotspotHintTextEl.textContent = parts.join(" • ");
  setHintVisible(hotspotHintEl, true);
}

function updateRankingsHint({ empty }) {
  if (!rankingsHintEl) return;
  if (!empty) {
    setHintVisible(rankingsHintEl, false);
    return;
  }

  const range = getCurrentRangeOrNull();
  const hours = getRangeHours(range);
  const minSamples = getEffectiveMinSamples();
  const parts = [];
  if (lastRankingsReason && lastRankingsReason.message) parts.push(lastRankingsReason.message);
  if (hours != null) parts.push(`Window: ${hours.toFixed(1)}h`);
  if (minSamples != null) parts.push(`Min samples: ${minSamples}`);
  parts.push(lastRankingsReason?.suggestion || "Try: Set Min samples = 1, or use a wider time window (24h).");
  if (rankingsHintTextEl) rankingsHintTextEl.textContent = parts.join(" • ");
  setHintVisible(rankingsHintEl, true);
}

function applyLayoutFromState() {
  const sidebarWidth = getNested(uiState, "layout.sidebarWidthPx", null);
  if (typeof sidebarWidth === "number" && Number.isFinite(sidebarWidth)) {
    setCssVar("--sidebar-width", `${Math.round(sidebarWidth)}px`);
  }

  const chartHeight = getNested(uiState, "layout.chartHeightPx", null);
  if (typeof chartHeight === "number" && Number.isFinite(chartHeight)) {
    setCssVar("--chart-height", `${Math.round(chartHeight)}px`);
  }
}

function applyThemeFromState() {
  const theme = String(getNested(uiState, "layout.theme", "product"));
  document.body.classList.remove("theme-policy", "theme-product");
  if (theme === "policy") document.body.classList.add("theme-policy");
  else document.body.classList.add("theme-product");
  if (themeSelectEl) themeSelectEl.value = theme === "policy" ? "policy" : "product";
}

function initThemeToggle() {
  applyThemeFromState();
  if (!themeSelectEl) return;
  themeSelectEl.addEventListener("change", () => {
    const theme = themeSelectEl.value === "policy" ? "policy" : "product";
    uiState.layout = uiState.layout || {};
    uiState.layout.theme = theme;
    saveUiState(uiState);
    applyThemeFromState();
    applyThemeToVisuals();
    scheduleUrlSync();
  });
}

function applyThemeToVisuals() {
  const colors = themeColors();

  markers.eachLayer((layer) => {
    if (!layer || typeof layer.setStyle !== "function") return;
    layer.setStyle({
      color: colors.accentHex,
      fillColor: rgba(colors.accentRgb, 0.22),
    });
  });

  eventMarkers.eachLayer((layer) => {
    if (!layer || typeof layer.setStyle !== "function") return;
    layer.setStyle({
      color: colors.dangerHex,
      fillColor: rgba(colors.dangerRgb, 0.2),
    });
  });

  impactSegmentsLayer.eachLayer((layer) => {
    if (!layer || typeof layer.setStyle !== "function") return;
    layer.setStyle({
      color: colors.dangerHex,
      fillColor: rgba(colors.dangerRgb, 0.28),
    });
  });

  if (hotspotsLoaded && hotspotRows && hotspotRows.length) {
    renderHotspots(hotspotRows, hotspotMetricEl ? hotspotMetricEl.value : "mean_speed_kph");
  } else {
    updateHotspotLegend(null, null);
  }

  if (selectedEventId && lastEventImpact) {
    renderEventImpactChart(lastEventImpact);
  } else if (lastTimeseries && lastTimeseries.points && lastTimeseries.points.length) {
    renderTimeseries(lastTimeseries.points, {
      title: getEntityTitle(lastTimeseries.entity),
      anomalies: lastTimeseries.anomalies,
    });
  }
}

async function copyShareLink() {
  syncUrlFromUi();
  const url = window.location.href;
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(url);
      setStatus("Link copied to clipboard.");
      return;
    }
  } catch (err) {
    // fall back below
  }

  try {
    window.prompt("Copy this link:", url);
  } catch (err) {
    // ignore
  }
}

let _layoutResizeRaf = null;
function scheduleLayoutResize() {
  if (_layoutResizeRaf != null) return;
  _layoutResizeRaf = window.requestAnimationFrame(() => {
    _layoutResizeRaf = null;
    try {
      map.invalidateSize({ animate: false });
    } catch (err) {
      // ignore
    }
    try {
      if (chartEl && typeof Plotly !== "undefined" && Plotly.Plots && Plotly.Plots.resize) {
        Plotly.Plots.resize(chartEl);
      }
    } catch (err) {
      // ignore
    }
  });
}

function initResizer(element, { axis, minPx, maxPx, onCommit }) {
  if (!element) return;
  let dragging = false;
  let startPos = 0;
  let startValue = 0;
  let currentValue = 0;

  const onPointerMove = (ev) => {
    if (!dragging) return;
    const delta = axis === "x" ? ev.clientX - startPos : ev.clientY - startPos;
    currentValue = clamp(startValue + delta, minPx, maxPx);
    onCommit(currentValue, { live: true });
    ev.preventDefault();
  };

  const onPointerUp = (ev) => {
    if (!dragging) return;
    dragging = false;
    onCommit(currentValue, { live: false });
    element.releasePointerCapture(ev.pointerId);
    document.removeEventListener("pointermove", onPointerMove);
    document.removeEventListener("pointerup", onPointerUp);
    ev.preventDefault();
  };

  element.addEventListener("pointerdown", (ev) => {
    dragging = true;
    startPos = axis === "x" ? ev.clientX : ev.clientY;
    if (axis === "x") {
      startValue = sidebarEl.getBoundingClientRect().width;
    } else {
      const chartContainer = chartEl.closest(".chart") || chartEl;
      startValue = chartContainer.getBoundingClientRect().height;
    }
    currentValue = startValue;
    element.setPointerCapture(ev.pointerId);
    document.addEventListener("pointermove", onPointerMove);
    document.addEventListener("pointerup", onPointerUp);
    ev.preventDefault();
  });
}

function initLayoutResizers() {
  initResizer(sidebarResizerEl, {
    axis: "x",
    minPx: 280,
    maxPx: 700,
    onCommit: (value, { live }) => {
      setCssVar("--sidebar-width", `${Math.round(value)}px`);
      scheduleLayoutResize();
      uiState.layout = uiState.layout || {};
      uiState.layout.sidebarWidthPx = Math.round(value);
      if (!live) saveUiState(uiState);
    },
  });

  initResizer(chartResizerEl, {
    axis: "y",
    minPx: 200,
    maxPx: 700,
    onCommit: (value, { live }) => {
      setCssVar("--chart-height", `${Math.round(value)}px`);
      scheduleLayoutResize();
      uiState.layout = uiState.layout || {};
      uiState.layout.chartHeightPx = Math.round(value);
      if (!live) saveUiState(uiState);
    },
  });
}

function initPanelCollapse() {
  const panels = Array.from(document.querySelectorAll(".panel[data-panel-id]"));
  for (const panel of panels) {
    const panelId = panel.getAttribute("data-panel-id");
    const title = panel.querySelector(".panel-title");
    if (!panelId || !title) continue;

    const collapsed = Boolean(getNested(uiState, `panels.${panelId}.collapsed`, false));
    if (collapsed) panel.classList.add("collapsed");

    title.addEventListener("click", () => {
      panel.classList.toggle("collapsed");
      const nowCollapsed = panel.classList.contains("collapsed");
      uiState.panels = uiState.panels || {};
      uiState.panels[panelId] = uiState.panels[panelId] || {};
      uiState.panels[panelId].collapsed = nowCollapsed;
      saveUiState(uiState);
    });
  }
}

function getLiveConfigFromState() {
  return {
    enabled: Boolean(getNested(uiState, "live.autoRefresh", false)),
    intervalSeconds: Number(getNested(uiState, "live.intervalSeconds", 60)),
  };
}

function applyLiveStateToForm() {
  if (!liveAutoRefreshEl || !liveIntervalSecondsEl) return;
  const config = getLiveConfigFromState();
  liveAutoRefreshEl.checked = Boolean(config.enabled);
  if (Number.isFinite(config.intervalSeconds)) {
    liveIntervalSecondsEl.value = String(Math.round(config.intervalSeconds));
  }
}

let liveTimer = null;
function stopLiveRefresh() {
  if (liveTimer) window.clearInterval(liveTimer);
  liveTimer = null;
  if (!isStatusStreamSupported()) return;
  // Keep SSE stream active; only stop when the user disables live mode.
}

function startLiveRefresh() {
  stopLiveRefresh();
  const config = getLiveConfigFromState();
  if (!config.enabled) return;

  // Prefer SSE-driven refresh (no polling).
  if (isStatusStreamSupported()) {
    startStatusStream();
    setLiveIndicator({ mode: "connected", detail: uiStatusStream ? "SSE connected" : "SSE" });
    return;
  }

  setLiveIndicator({ mode: "polling", detail: "Polling" });
  const safe = clamp(Number.isFinite(config.intervalSeconds) ? config.intervalSeconds : 60, 5, 3600);
  liveTimer = window.setInterval(() => {
    refreshLiveNow({ silent: true });
  }, safe * 1000);
}

async function refreshLiveNow({ silent, skipStatus } = { silent: false, skipStatus: false }) {
  if (!silent) setStatus("Refreshing live views...");
  const prevLatestMs = latestObservationMs;
  if (!skipStatus) {
    await refreshUiStatus();
  }

  applyFollowLatestWindow();

  const latestChanged =
    !skipStatus && prevLatestMs != null && latestObservationMs != null && latestObservationMs !== prevLatestMs;
  const shouldRefreshViews = skipStatus ? true : !silent || latestChanged || prevLatestMs == null;

  if (!shouldRefreshViews) return;

  if (hotspotsLoaded && showHotspots) {
    await loadHotspots();
  }

  if (rankingsLoaded) {
    await loadRankings();
  }

  if (lastTimeseries && lastTimeseries.entity && lastTimeseries.entity.id) {
    await loadTimeseries();
  }

  if (eventsLoaded && showEvents) {
    await loadEvents();
  }

  if (!silent) setStatus("Live refresh complete.");
}

function initLivePanel() {
  if (!liveAutoRefreshEl || !liveIntervalSecondsEl || !liveRefreshEl) return;

  applyLiveStateToForm();
  refreshUiStatus();
  startFreshnessTicker();
  startStatusStream();
  updateCacheIndicator();
  if (!isStatusStreamSupported()) setLiveIndicator({ mode: "polling", detail: "Polling" });
  startLiveRefresh();

  liveAutoRefreshEl.addEventListener("change", () => {
    uiState.live = uiState.live || {};
    uiState.live.autoRefresh = Boolean(liveAutoRefreshEl.checked);
    saveUiState(uiState);
    if (!uiState.live.autoRefresh) setLiveIndicator({ mode: "polling", detail: "Paused" });
    startLiveRefresh();
  });

  liveIntervalSecondsEl.addEventListener("change", () => {
    const value = clamp(Number(liveIntervalSecondsEl.value || 60), 5, 3600);
    liveIntervalSecondsEl.value = String(Math.round(value));
    uiState.live = uiState.live || {};
    uiState.live.intervalSeconds = Math.round(value);
    saveUiState(uiState);
    startLiveRefresh();
  });

  liveRefreshEl.addEventListener("click", () => refreshLiveNow({ silent: false }));
}

function parseNumberValue(text) {
  const trimmed = String(text || "").trim();
  if (!trimmed) return null;
  const num = Number(trimmed);
  return Number.isFinite(num) ? num : null;
}

function parseIntValue(text) {
  const num = parseNumberValue(text);
  if (num == null) return null;
  return Math.trunc(num);
}

function setInputValue(el, value) {
  if (!el) return;
  el.value = value == null ? "" : String(value);
}

function setInputPlaceholder(el, value) {
  if (!el) return;
  el.placeholder = value == null ? "" : String(value);
}

function reliabilityOverridesFromForm() {
  return {
    congestion_speed_threshold_kph: parseNumberValue(reliabilityThresholdEl.value),
    min_samples: parseIntValue(reliabilityMinSamplesEl.value),
    weight_mean_speed: parseNumberValue(reliabilityWeightMeanEl.value),
    weight_speed_std: parseNumberValue(reliabilityWeightStdEl.value),
    weight_congestion_frequency: parseNumberValue(reliabilityWeightCongEl.value),
  };
}

function anomaliesOverridesFromForm() {
  return {
    window_points: parseIntValue(anomaliesWindowEl.value),
    z_threshold: parseNumberValue(anomaliesZEl.value),
    direction: anomaliesDirectionEl.value || null,
    max_gap_minutes: parseIntValue(anomaliesMaxGapEl.value),
    min_event_points: parseIntValue(anomaliesMinEventPointsEl.value),
  };
}

function impactOverridesFromForm() {
  return {
    radius_meters: parseNumberValue(impactRadiusEl.value),
    max_segments: parseIntValue(impactMaxSegmentsEl.value),
    baseline_window_minutes: parseIntValue(impactBaselineMinutesEl.value),
    recovery_horizon_minutes: parseIntValue(impactRecoveryMinutesEl.value),
    recovery_ratio: parseNumberValue(impactRecoveryRatioEl.value),
    speed_weighting: impactWeightingEl.value || null,
    end_time_fallback_minutes: parseIntValue(impactEndFallbackEl.value),
    min_baseline_points: parseIntValue(impactMinBaselineEl.value),
    min_event_points: parseIntValue(impactMinEventEl.value),
  };
}

function applyReliabilityOverrides(url) {
  const o = getNested(uiState, "overrides.reliability", null);
  if (!o) return;
  if (o.congestion_speed_threshold_kph != null)
    url.searchParams.set("congestion_speed_threshold_kph", String(o.congestion_speed_threshold_kph));
  if (o.min_samples != null) url.searchParams.set("min_samples", String(o.min_samples));
  if (o.weight_mean_speed != null) url.searchParams.set("weight_mean_speed", String(o.weight_mean_speed));
  if (o.weight_speed_std != null) url.searchParams.set("weight_speed_std", String(o.weight_speed_std));
  if (o.weight_congestion_frequency != null)
    url.searchParams.set("weight_congestion_frequency", String(o.weight_congestion_frequency));
}

function applyAnomaliesOverrides(url) {
  const o = getNested(uiState, "overrides.anomalies", null);
  if (!o) return;
  if (o.window_points != null) url.searchParams.set("window_points", String(o.window_points));
  if (o.z_threshold != null) url.searchParams.set("z_threshold", String(o.z_threshold));
  if (o.direction) url.searchParams.set("direction", String(o.direction));
  if (o.max_gap_minutes != null) url.searchParams.set("max_gap_minutes", String(o.max_gap_minutes));
  if (o.min_event_points != null) url.searchParams.set("min_event_points", String(o.min_event_points));
}

function applyImpactOverrides(url) {
  const o = getNested(uiState, "overrides.impact", null);
  if (!o) return;
  if (o.radius_meters != null) url.searchParams.set("radius_meters", String(o.radius_meters));
  if (o.max_segments != null) url.searchParams.set("max_segments", String(o.max_segments));
  if (o.baseline_window_minutes != null)
    url.searchParams.set("baseline_window_minutes", String(o.baseline_window_minutes));
  if (o.recovery_horizon_minutes != null)
    url.searchParams.set("recovery_horizon_minutes", String(o.recovery_horizon_minutes));
  if (o.recovery_ratio != null) url.searchParams.set("recovery_ratio", String(o.recovery_ratio));
  if (o.speed_weighting) url.searchParams.set("speed_weighting", String(o.speed_weighting));
  if (o.end_time_fallback_minutes != null)
    url.searchParams.set("end_time_fallback_minutes", String(o.end_time_fallback_minutes));
  if (o.min_baseline_points != null)
    url.searchParams.set("min_baseline_points", String(o.min_baseline_points));
  if (o.min_event_points != null) url.searchParams.set("min_event_points", String(o.min_event_points));
}

async function loadUiDefaultsFromApi() {
  try {
    uiDefaults = await fetchJson(`${API_BASE}/ui/settings`);
    return uiDefaults;
  } catch (err) {
    return null;
  }
}

function applyDefaultsToForm(defaults) {
  if (!defaults || !defaults.analytics) return;

  const rel = defaults.analytics.reliability || {};
  setInputPlaceholder(reliabilityThresholdEl, rel.congestion_speed_threshold_kph);
  setInputPlaceholder(reliabilityMinSamplesEl, rel.min_samples);
  setInputPlaceholder(reliabilityWeightMeanEl, getNested(rel, "weights.mean_speed", null));
  setInputPlaceholder(reliabilityWeightStdEl, getNested(rel, "weights.speed_std", null));
  setInputPlaceholder(reliabilityWeightCongEl, getNested(rel, "weights.congestion_frequency", null));
  if (reliabilityThresholdEl && !reliabilityThresholdEl.value)
    setInputValue(reliabilityThresholdEl, rel.congestion_speed_threshold_kph);
  if (reliabilityMinSamplesEl && !reliabilityMinSamplesEl.value)
    setInputValue(reliabilityMinSamplesEl, rel.min_samples);
  if (reliabilityWeightMeanEl && !reliabilityWeightMeanEl.value)
    setInputValue(reliabilityWeightMeanEl, getNested(rel, "weights.mean_speed", null));
  if (reliabilityWeightStdEl && !reliabilityWeightStdEl.value)
    setInputValue(reliabilityWeightStdEl, getNested(rel, "weights.speed_std", null));
  if (reliabilityWeightCongEl && !reliabilityWeightCongEl.value)
    setInputValue(reliabilityWeightCongEl, getNested(rel, "weights.congestion_frequency", null));

  const an = defaults.analytics.anomalies || {};
  setInputPlaceholder(anomaliesWindowEl, an.window_points);
  setInputPlaceholder(anomaliesZEl, an.z_threshold);
  setInputPlaceholder(anomaliesMaxGapEl, an.max_gap_minutes);
  setInputPlaceholder(anomaliesMinEventPointsEl, an.min_event_points);

  if (anomaliesWindowEl && !anomaliesWindowEl.value) setInputValue(anomaliesWindowEl, an.window_points);
  if (anomaliesZEl && !anomaliesZEl.value) setInputValue(anomaliesZEl, an.z_threshold);
  if (anomaliesMaxGapEl && !anomaliesMaxGapEl.value) setInputValue(anomaliesMaxGapEl, an.max_gap_minutes);
  if (anomaliesMinEventPointsEl && !anomaliesMinEventPointsEl.value)
    setInputValue(anomaliesMinEventPointsEl, an.min_event_points);
  if (an.direction && anomaliesDirectionEl) anomaliesDirectionEl.value = String(an.direction);

  const impact = defaults.analytics.event_impact || {};
  setInputPlaceholder(impactRadiusEl, impact.radius_meters);
  setInputPlaceholder(impactMaxSegmentsEl, impact.max_segments);
  setInputPlaceholder(impactBaselineMinutesEl, impact.baseline_window_minutes);
  setInputPlaceholder(impactRecoveryMinutesEl, impact.recovery_horizon_minutes);
  setInputPlaceholder(impactRecoveryRatioEl, impact.recovery_ratio);
  setInputPlaceholder(impactEndFallbackEl, impact.end_time_fallback_minutes);
  setInputPlaceholder(impactMinBaselineEl, impact.min_baseline_points);
  setInputPlaceholder(impactMinEventEl, impact.min_event_points);

  if (impactRadiusEl && !impactRadiusEl.value) setInputValue(impactRadiusEl, impact.radius_meters);
  if (impactMaxSegmentsEl && !impactMaxSegmentsEl.value) setInputValue(impactMaxSegmentsEl, impact.max_segments);
  if (impactBaselineMinutesEl && !impactBaselineMinutesEl.value)
    setInputValue(impactBaselineMinutesEl, impact.baseline_window_minutes);
  if (impactRecoveryMinutesEl && !impactRecoveryMinutesEl.value)
    setInputValue(impactRecoveryMinutesEl, impact.recovery_horizon_minutes);
  if (impactRecoveryRatioEl && !impactRecoveryRatioEl.value)
    setInputValue(impactRecoveryRatioEl, impact.recovery_ratio);
  if (impactEndFallbackEl && !impactEndFallbackEl.value)
    setInputValue(impactEndFallbackEl, impact.end_time_fallback_minutes);
  if (impactMinBaselineEl && !impactMinBaselineEl.value)
    setInputValue(impactMinBaselineEl, impact.min_baseline_points);
  if (impactMinEventEl && !impactMinEventEl.value) setInputValue(impactMinEventEl, impact.min_event_points);

  if (impact.speed_weighting && impactWeightingEl) impactWeightingEl.value = String(impact.speed_weighting);
}

function applyStateToForm(state) {
  const rel = getNested(state, "overrides.reliability", null);
  if (rel) {
    setInputValue(reliabilityThresholdEl, rel.congestion_speed_threshold_kph);
    setInputValue(reliabilityMinSamplesEl, rel.min_samples);
    setInputValue(reliabilityWeightMeanEl, rel.weight_mean_speed);
    setInputValue(reliabilityWeightStdEl, rel.weight_speed_std);
    setInputValue(reliabilityWeightCongEl, rel.weight_congestion_frequency);
  }

  const an = getNested(state, "overrides.anomalies", null);
  if (an) {
    setInputValue(anomaliesWindowEl, an.window_points);
    setInputValue(anomaliesZEl, an.z_threshold);
    setInputValue(anomaliesMaxGapEl, an.max_gap_minutes);
    setInputValue(anomaliesMinEventPointsEl, an.min_event_points);
    if (an.direction && anomaliesDirectionEl) anomaliesDirectionEl.value = String(an.direction);
  }

  const impact = getNested(state, "overrides.impact", null);
  if (impact) {
    setInputValue(impactRadiusEl, impact.radius_meters);
    setInputValue(impactMaxSegmentsEl, impact.max_segments);
    setInputValue(impactBaselineMinutesEl, impact.baseline_window_minutes);
    setInputValue(impactRecoveryMinutesEl, impact.recovery_horizon_minutes);
    setInputValue(impactRecoveryRatioEl, impact.recovery_ratio);
    setInputValue(impactEndFallbackEl, impact.end_time_fallback_minutes);
    setInputValue(impactMinBaselineEl, impact.min_baseline_points);
    setInputValue(impactMinEventEl, impact.min_event_points);
    if (impact.speed_weighting && impactWeightingEl) impactWeightingEl.value = String(impact.speed_weighting);
  }

  if (toggleAnomaliesEl) toggleAnomaliesEl.checked = Boolean(showAnomalies);
  if (toggleHotspotsEl) toggleHotspotsEl.checked = Boolean(showHotspots);
  if (toggleEventsEl) toggleEventsEl.checked = Boolean(showEvents);
  if (toggleImpactEl) toggleImpactEl.checked = Boolean(showImpact);
}

function persistOverlays() {
  uiState.overlays = uiState.overlays || {};
  uiState.overlays.anomalies = Boolean(showAnomalies);
  uiState.overlays.hotspots = Boolean(showHotspots);
  uiState.overlays.events = Boolean(showEvents);
  uiState.overlays.impact = Boolean(showImpact);
  saveUiState(uiState);
}

function applySettingsFromForm({ refresh } = { refresh: true }) {
  uiState.overrides = uiState.overrides || {};
  uiState.overrides.reliability = reliabilityOverridesFromForm();
  uiState.overrides.anomalies = anomaliesOverridesFromForm();
  uiState.overrides.impact = impactOverridesFromForm();

  showAnomalies = Boolean(toggleAnomaliesEl.checked);
  showHotspots = Boolean(toggleHotspotsEl.checked);
  showEvents = Boolean(toggleEventsEl.checked);
  showImpact = Boolean(toggleImpactEl.checked);

  persistOverlays();
  applyOverlayVisibility();
  saveUiState(uiState);
  scheduleUrlSync();

  if (refresh) refreshAfterSettings();
}

function resetSettingsToDefaults() {
  uiState.overrides = {};
  showAnomalies = true;
  showHotspots = true;
  showEvents = true;
  showImpact = true;
  persistOverlays();

  setInputValue(reliabilityThresholdEl, null);
  setInputValue(reliabilityMinSamplesEl, null);
  setInputValue(reliabilityWeightMeanEl, null);
  setInputValue(reliabilityWeightStdEl, null);
  setInputValue(reliabilityWeightCongEl, null);

  setInputValue(anomaliesWindowEl, null);
  setInputValue(anomaliesZEl, null);
  setInputValue(anomaliesMaxGapEl, null);
  setInputValue(anomaliesMinEventPointsEl, null);

  setInputValue(impactRadiusEl, null);
  setInputValue(impactMaxSegmentsEl, null);
  setInputValue(impactBaselineMinutesEl, null);
  setInputValue(impactRecoveryMinutesEl, null);
  setInputValue(impactRecoveryRatioEl, null);
  setInputValue(impactEndFallbackEl, null);
  setInputValue(impactMinBaselineEl, null);
  setInputValue(impactMinEventEl, null);

  applyDefaultsToForm(uiDefaults);
  applyStateToForm(uiState);
  applyOverlayVisibility();
  saveUiState(uiState);
  refreshAfterSettings();
}

function refreshAfterSettings() {
  const entity = getSelectedEntity();
  const range = getIsoRange();
  if (entity && entity.id && range) loadTimeseries();
  if (rankingsLoaded) loadRankings();
  if (hotspotsLoaded) loadHotspots();
  if (selectedEventId) selectEvent(selectedEventId, { centerMap: false });
}

function initSettingsPanel() {
  if (toggleAnomaliesEl) {
    toggleAnomaliesEl.checked = Boolean(showAnomalies);
    toggleAnomaliesEl.addEventListener("change", () => {
      showAnomalies = Boolean(toggleAnomaliesEl.checked);
      persistOverlays();
      if (lastTimeseries.points && lastTimeseries.points.length) loadTimeseries();
    });
  }

  if (toggleHotspotsEl) {
    toggleHotspotsEl.checked = Boolean(showHotspots);
    toggleHotspotsEl.addEventListener("change", () => {
      showHotspots = Boolean(toggleHotspotsEl.checked);
      persistOverlays();
      applyOverlayVisibility();
    });
  }

  if (toggleEventsEl) {
    toggleEventsEl.checked = Boolean(showEvents);
    toggleEventsEl.addEventListener("change", () => {
      showEvents = Boolean(toggleEventsEl.checked);
      persistOverlays();
      applyOverlayVisibility();
    });
  }

  if (toggleImpactEl) {
    toggleImpactEl.checked = Boolean(showImpact);
    toggleImpactEl.addEventListener("change", () => {
      showImpact = Boolean(toggleImpactEl.checked);
      persistOverlays();
      applyOverlayVisibility();
    });
  }

  if (applySettingsButton) {
    applySettingsButton.addEventListener("click", () => applySettingsFromForm({ refresh: true }));
  }
  if (resetSettingsButton) {
    resetSettingsButton.addEventListener("click", resetSettingsToDefaults);
  }
}

function setShortcutsOverlayOpen(open) {
  if (!shortcutsOverlayEl) return;
  if (open) shortcutsOverlayEl.classList.remove("hidden");
  else shortcutsOverlayEl.classList.add("hidden");
}

function toggleShortcutsOverlay() {
  if (!shortcutsOverlayEl) return;
  setShortcutsOverlayOpen(shortcutsOverlayEl.classList.contains("hidden"));
}

function initShortcutsOverlay() {
  if (!shortcutsOverlayEl) return;
  if (closeShortcutsButton) closeShortcutsButton.addEventListener("click", () => setShortcutsOverlayOpen(false));
  shortcutsOverlayEl.addEventListener("click", (ev) => {
    if (ev.target === shortcutsOverlayEl) setShortcutsOverlayOpen(false);
  });
}

function isTypingInInput() {
  const el = document.activeElement;
  if (!el) return false;
  const tag = el.tagName ? el.tagName.toLowerCase() : "";
  if (tag === "input" || tag === "textarea" || tag === "select") return true;
  return Boolean(el.isContentEditable);
}

function panMap(dx, dy, { fast } = { fast: false }) {
  const base = fast ? 220 : 120;
  map.panBy([dx * base, dy * base], { animate: false });
}

function focusSelected() {
  if (selectedSegmentId && markerById.has(selectedSegmentId)) {
    selectSegment(selectedSegmentId, { centerMap: true });
    return;
  }
  if (selectedEventId && eventMarkerById.has(selectedEventId)) {
    selectEvent(selectedEventId, { centerMap: true });
    return;
  }
  if (segments.length) {
    const bounds = [];
    for (const seg of segments) {
      if (seg.lat == null || seg.lon == null) continue;
      bounds.push([Number(seg.lat), Number(seg.lon)]);
    }
    if (bounds.length) map.fitBounds(bounds, { padding: [30, 30] });
  }
}

function parseLocalDateTimeInput(value) {
  const text = String(value || "").trim();
  if (!text) return null;
  const [datePart, timePart] = text.split("T");
  if (!datePart || !timePart) return null;
  const [year, month, day] = datePart.split("-").map((n) => Number(n));
  const [hour, minute] = timePart.split(":").map((n) => Number(n));
  if (![year, month, day, hour, minute].every((n) => Number.isFinite(n))) return null;
  return new Date(year, month - 1, day, hour, minute, 0, 0);
}

function toLocalDateTimeInputValue(date) {
  const pad2 = (n) => String(n).padStart(2, "0");
  return `${date.getFullYear()}-${pad2(date.getMonth() + 1)}-${pad2(date.getDate())}T${pad2(
    date.getHours()
  )}:${pad2(date.getMinutes())}`;
}

function shiftTimeRange(direction) {
  const start = parseLocalDateTimeInput(startEl.value);
  const end = parseLocalDateTimeInput(endEl.value);
  if (!start || !end) return;
  const deltaMs = end.getTime() - start.getTime();
  if (!Number.isFinite(deltaMs) || deltaMs <= 0) return;

  const nextStart = new Date(start.getTime() + direction * deltaMs);
  const nextEnd = new Date(end.getTime() + direction * deltaMs);
  startEl.value = toLocalDateTimeInputValue(nextStart);
  endEl.value = toLocalDateTimeInputValue(nextEnd);
  loadTimeseries();
}

function initKeyboardShortcuts() {
  document.addEventListener("keydown", (ev) => {
    if (isTypingInInput()) return;
    if (ev.key === "Escape") {
      setShortcutsOverlayOpen(false);
      return;
    }

    if (ev.key === "?" || (ev.key === "/" && ev.shiftKey)) {
      toggleShortcutsOverlay();
      ev.preventDefault();
      return;
    }

    const key = String(ev.key || "").toLowerCase();
    const fast = ev.shiftKey;

    if (key === "w" || ev.key === "ArrowUp") {
      panMap(0, -1, { fast });
      ev.preventDefault();
      return;
    }
    if (key === "s" || ev.key === "ArrowDown") {
      panMap(0, 1, { fast });
      ev.preventDefault();
      return;
    }
    if (key === "a" || ev.key === "ArrowLeft") {
      panMap(-1, 0, { fast });
      ev.preventDefault();
      return;
    }
    if (key === "d" || ev.key === "ArrowRight") {
      panMap(1, 0, { fast });
      ev.preventDefault();
      return;
    }

    if (ev.key === "+" || ev.key === "=") {
      map.setZoom(map.getZoom() + 1, { animate: false });
      ev.preventDefault();
      return;
    }
    if (ev.key === "-" || ev.key === "_") {
      map.setZoom(map.getZoom() - 1, { animate: false });
      ev.preventDefault();
      return;
    }

    if (key === "l") {
      loadTimeseries();
      ev.preventDefault();
      return;
    }
    if (key === "r") {
      loadTimeseries();
      ev.preventDefault();
      return;
    }
    if (key === "f") {
      focusSelected();
      ev.preventDefault();
      return;
    }

    if (ev.key === "[") {
      shiftTimeRange(-1);
      ev.preventDefault();
      return;
    }
    if (ev.key === "]") {
      shiftTimeRange(1);
      ev.preventDefault();
      return;
    }

    if (key === "h") {
      showHotspots = !showHotspots;
      if (toggleHotspotsEl) toggleHotspotsEl.checked = Boolean(showHotspots);
      persistOverlays();
      applyOverlayVisibility();
      ev.preventDefault();
      return;
    }
    if (key === "e") {
      showEvents = !showEvents;
      if (toggleEventsEl) toggleEventsEl.checked = Boolean(showEvents);
      persistOverlays();
      applyOverlayVisibility();
      ev.preventDefault();
      return;
    }
    if (key === "n") {
      showAnomalies = !showAnomalies;
      if (toggleAnomaliesEl) toggleAnomaliesEl.checked = Boolean(showAnomalies);
      persistOverlays();
      loadTimeseries();
      ev.preventDefault();
      return;
    }
  });
}

function formatSegmentLabel(segment) {
  const name = segment.name ? ` - ${segment.name}` : "";
  const city = segment.city ? ` (${segment.city})` : "";
  return `${segment.segment_id}${name}${city}`;
}

function formatCorridorLabel(corridor) {
  const name = corridor.corridor_name ? ` - ${corridor.corridor_name}` : "";
  return `${corridor.corridor_id}${name}`;
}

function normalizeText(value) {
  return String(value || "").toLowerCase();
}

function populateSegmentSelect(filtered) {
  segmentSelectEl.innerHTML = "";
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = "Select a segment...";
  segmentSelectEl.appendChild(placeholder);

  for (const seg of filtered) {
    const option = document.createElement("option");
    option.value = seg.segment_id;
    option.textContent = formatSegmentLabel(seg);
    segmentSelectEl.appendChild(option);
  }
}

function populateCorridorSelect(list) {
  corridorSelectEl.innerHTML = "";
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = "Select a corridor...";
  corridorSelectEl.appendChild(placeholder);

  for (const corridor of list) {
    const option = document.createElement("option");
    option.value = corridor.corridor_id;
    option.textContent = formatCorridorLabel(corridor);
    corridorSelectEl.appendChild(option);
  }
}

function setDefaultTimeRange() {
  const now = new Date();
  const end = new Date(now.getTime());
  const start = new Date(now.getTime() - 6 * 60 * 60 * 1000);

  if (!startEl.value) startEl.value = toLocalInputValue(start);
  if (!endEl.value) endEl.value = toLocalInputValue(end);
}

function toLocalInputValue(dt) {
  const pad2 = (n) => String(n).padStart(2, "0");
  return `${dt.getFullYear()}-${pad2(dt.getMonth() + 1)}-${pad2(dt.getDate())}T${pad2(
    dt.getHours()
  )}:${pad2(dt.getMinutes())}`;
}

function applyFollowLatestWindow() {
  if (!followLatestEl || !followLatestEl.checked) return;
  if (latestObservationMs == null) return;

  const hours = clamp(Number(liveWindowHoursEl?.value || 6), 1, 168);
  const endDt = new Date(latestObservationMs);
  const startDt = new Date(endDt.getTime() - hours * 60 * 60 * 1000);
  startEl.value = toLocalInputValue(startDt);
  endEl.value = toLocalInputValue(endDt);

  if (eventsWithinRangeEl && eventsWithinRangeEl.checked) applyEventsFilters();
}

function updateSegmentInfo(segment) {
  if (!segment) {
    segmentInfoEl.textContent = "No segment selected.";
    return;
  }

  const parts = [];
  parts.push(`ID: ${segment.segment_id}`);
  if (segment.name) parts.push(`Name: ${segment.name}`);
  if (segment.city) parts.push(`City: ${segment.city}`);
  if (segment.direction) parts.push(`Direction: ${segment.direction}`);
  if (segment.road_name) parts.push(`Road: ${segment.road_name}`);
  if (segment.link_id) parts.push(`Link: ${segment.link_id}`);
  if (segment.lat != null && segment.lon != null)
    parts.push(`Location: ${segment.lat.toFixed(6)}, ${segment.lon.toFixed(6)}`);

  segmentInfoEl.textContent = parts.join("\n");
}

function updateCorridorInfo(corridor) {
  if (!corridor) {
    corridorInfoEl.textContent = "No corridor selected.";
    return;
  }

  const parts = [];
  parts.push(`ID: ${corridor.corridor_id}`);
  if (corridor.corridor_name) parts.push(`Name: ${corridor.corridor_name}`);
  if (corridor.segment_count != null) parts.push(`Segments: ${corridor.segment_count}`);
  corridorInfoEl.textContent = parts.join("\n");
}

function updateEventInfo(event, impact) {
  if (!event) {
    if (lastEventsClearedReason) {
      eventInfoEl.textContent = `No event selected.\n${lastEventsClearedReason}`;
      if (eventsHintTextEl) eventsHintTextEl.textContent = lastEventsClearedReason;
      setHintVisible(eventsHintEl, true);
    } else {
      eventInfoEl.textContent = "No event selected.";
      setHintVisible(eventsHintEl, false);
    }
    return;
  }
  lastEventsClearedReason = null;
  setHintVisible(eventsHintEl, false);

  const parts = [];
  parts.push(`ID: ${event.event_id}`);
  if (event.event_type) parts.push(`Type: ${event.event_type}`);
  if (event.road_name) parts.push(`Road: ${event.road_name}`);
  if (event.direction) parts.push(`Direction: ${event.direction}`);
  if (event.city) parts.push(`City: ${event.city}`);
  if (event.severity != null) parts.push(`Severity: ${event.severity}`);
  if (event.start_time) parts.push(`Start: ${event.start_time}`);
  if (event.end_time) parts.push(`End: ${event.end_time}`);
  if (event.lat != null && event.lon != null)
    parts.push(`Location: ${Number(event.lat).toFixed(6)}, ${Number(event.lon).toFixed(6)}`);
  if (event.description) parts.push(`Description: ${event.description}`);

  if (impact) {
    const baseline = impact.baseline_mean_speed_kph != null ? Number(impact.baseline_mean_speed_kph).toFixed(1) : "—";
    const during = impact.event_mean_speed_kph != null ? Number(impact.event_mean_speed_kph).toFixed(1) : "—";
    const delta = impact.speed_delta_mean_kph != null ? Number(impact.speed_delta_mean_kph).toFixed(1) : "—";
    const rec = impact.recovery_minutes != null ? `${Math.round(Number(impact.recovery_minutes))} min` : "—";
    parts.push(`Baseline mean speed: ${baseline} kph`);
    parts.push(`Event mean speed: ${during} kph`);
    parts.push(`Delta mean speed: ${delta} kph`);
    parts.push(`Recovery: ${rec}`);
  }

  eventInfoEl.textContent = parts.join("\n");
}

async function fetchJson(url) {
  const resp = await fetch(url, { headers: { accept: "application/json" } });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`${resp.status} ${resp.statusText}: ${text}`);
  }
  return await resp.json();
}

function unwrapItemsResponse(data) {
  if (Array.isArray(data)) return { items: data, reason: null };
  if (data && typeof data === "object" && Array.isArray(data.items)) {
    return { items: data.items, reason: data.reason || null };
  }
  return {
    items: [],
    reason: { code: "invalid_response", message: "Unexpected response shape from API.", suggestion: null },
  };
}

async function fetchItems(url) {
  const resp = await fetch(url, { headers: { accept: "application/json" } });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`${resp.status} ${resp.statusText}: ${text}`);
  }
  const data = await resp.json();
  const { items, reason } = unwrapItemsResponse(data);
  return {
    items,
    reason,
    cache: {
      status: resp.headers.get("x-cache"),
      ttl: resp.headers.get("x-cache-ttl"),
    },
  };
}

function getIsoRange() {
  const startVal = startEl.value;
  const endVal = endEl.value;
  if (!startVal || !endVal) return null;

  const start = new Date(startVal);
  const end = new Date(endVal);
  if (!(start instanceof Date) || isNaN(start.getTime()) || isNaN(end.getTime())) return null;
  if (end <= start) return null;
  return { start: start.toISOString(), end: end.toISOString() };
}

function applyTimeseriesStateToForm() {
  if (!followLatestEl) return;
  followLatestEl.checked = Boolean(getNested(uiState, "timeseries.followLatest", false));
  if (liveWindowHoursEl) {
    const hours = Number(getNested(uiState, "timeseries.liveWindowHours", 6));
    if (Number.isFinite(hours)) liveWindowHoursEl.value = String(Math.round(hours));
  }

  const follow = Boolean(followLatestEl.checked);
  startEl.disabled = follow;
  endEl.disabled = follow;
}

function initTimeseriesFollowLatest() {
  if (!followLatestEl) return;
  applyTimeseriesStateToForm();
  applyFollowLatestWindow();

  followLatestEl.addEventListener("change", () => {
    uiState.timeseries = uiState.timeseries || {};
    uiState.timeseries.followLatest = Boolean(followLatestEl.checked);
    saveUiState(uiState);
    applyTimeseriesStateToForm();
    applyFollowLatestWindow();
    scheduleUrlSync();
  });

  if (liveWindowHoursEl) {
    liveWindowHoursEl.addEventListener("change", () => {
      const value = clamp(Number(liveWindowHoursEl.value || 6), 1, 168);
      liveWindowHoursEl.value = String(Math.round(value));
      uiState.timeseries = uiState.timeseries || {};
      uiState.timeseries.liveWindowHours = Math.round(value);
      saveUiState(uiState);
      applyFollowLatestWindow();
      scheduleUrlSync();
    });
  }
}

function getSelectedEntity() {
  const type = entityTypeEl.value || "segment";
  if (type === "corridor") return { type, id: selectedCorridorId };
  return { type, id: selectedSegmentId };
}

function getEntityTitle(entity) {
  if (!entity) return "";
  if (entity.type === "corridor") {
    const corridor = corridorsById.get(entity.id);
    return corridor ? formatCorridorLabel(corridor) : String(entity.id);
  }
  const segment = segmentsById.get(entity.id);
  return segment ? formatSegmentLabel(segment) : String(entity.id);
}

function formatEventTimeLocal(iso) {
  if (!iso) return "";
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return iso;
  const pad2 = (n) => String(n).padStart(2, "0");
  return `${pad2(date.getMonth() + 1)}-${pad2(date.getDate())} ${pad2(date.getHours())}:${pad2(date.getMinutes())}`;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function lerp(a, b, t) {
  return a + (b - a) * t;
}

function lerpColor(fromRgb, toRgb, t) {
  const tt = clamp(t, 0, 1);
  const r = Math.round(lerp(fromRgb[0], toRgb[0], tt));
  const g = Math.round(lerp(fromRgb[1], toRgb[1], tt));
  const b = Math.round(lerp(fromRgb[2], toRgb[2], tt));
  return `rgb(${r}, ${g}, ${b})`;
}

function metricLabel(metric) {
  switch (metric) {
    case "mean_speed_kph":
      return "Mean speed";
    case "speed_std_kph":
      return "Speed std";
    case "congestion_frequency":
      return "Congestion frequency";
    default:
      return metric;
  }
}

function formatMetricValue(metric, value) {
  if (value == null || !Number.isFinite(Number(value))) return "—";
  const num = Number(value);
  if (metric === "congestion_frequency") return `${Math.round(num * 100)}%`;
  return `${num.toFixed(1)}`;
}

function computeMetricRange(rows, metric) {
  const values = rows
    .map((r) => (r ? Number(r[metric]) : NaN))
    .filter((v) => Number.isFinite(v));
  if (!values.length) return { min: 0, max: 1 };
  let min = Math.min(...values);
  let max = Math.max(...values);
  if (min === max) max = min + 1;
  return { min, max };
}

function updateHotspotInfo(text) {
  hotspotInfoEl.textContent = text || "No hotspots loaded.";
}

function updateHotspotLegend(metric, range) {
  if (!hotspotLegendEl || !hotspotLegendTitleEl || !hotspotLegendBarEl) return;
  if (!metric || !range) {
    hotspotLegendEl.classList.add("hidden");
    return;
  }

  hotspotLegendEl.classList.remove("hidden");
  const unit = metric === "congestion_frequency" ? "%" : "kph";
  const hint =
    metric === "congestion_frequency"
      ? "low → high"
      : metric === "speed_std_kph"
        ? "low variance → high variance"
        : "slow → fast";
  hotspotLegendTitleEl.textContent = `Hotspots · ${metricLabel(metric)} (${unit}) · ${hint}`;

  const colors = themeColors();
  const accent = `rgba(${colors.accent2Rgb[0]}, ${colors.accent2Rgb[1]}, ${colors.accent2Rgb[2]}, 0.92)`;
  const danger = `rgba(${colors.dangerRgb[0]}, ${colors.dangerRgb[1]}, ${colors.dangerRgb[2]}, 0.92)`;
  const gradient =
    metric === "congestion_frequency"
      ? `linear-gradient(90deg, ${accent}, ${danger})`
      : `linear-gradient(90deg, ${danger}, ${accent})`;
  hotspotLegendBarEl.style.background = gradient;

  const mid = (Number(range.min) + Number(range.max)) / 2;
  const toLabel = (value) => {
    if (!Number.isFinite(Number(value))) return "—";
    if (metric === "congestion_frequency") return `${Math.round(Number(value) * 100)}%`;
    return `${Number(value).toFixed(1)}`;
  };

  const minText = toLabel(range.min);
  const midText = toLabel(mid);
  const maxText = toLabel(range.max);

  if (hotspotLegendMinEl) hotspotLegendMinEl.textContent = minText;
  if (hotspotLegendMidEl) hotspotLegendMidEl.textContent = midText;
  if (hotspotLegendMaxEl) hotspotLegendMaxEl.textContent = maxText;
}

function renderHotspots(rows, metric) {
  hotspotsLayer.clearLayers();
  if (!rows || !rows.length) {
    updateHotspotInfo("No hotspots loaded.");
    updateHotspotLegend(null, null);
    updateHotspotsHint({ empty: hotspotsLoaded });
    return;
  }
  updateHotspotsHint({ empty: false });

  const colors = themeColors();
  const accent = colors.accent2Rgb;
  const danger = colors.dangerRgb;
  const range = computeMetricRange(rows, metric);
  updateHotspotLegend(metric, range);

  let rendered = 0;
  for (const row of rows) {
    if (!row) continue;
    const lat = Number(row.lat);
    const lon = Number(row.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    const raw = row[metric];
    const value = raw == null ? null : Number(raw);
    let color = "rgba(15, 23, 42, 0.12)";
    if (value != null && Number.isFinite(value)) {
      if (metric === "congestion_frequency") {
        color = lerpColor(accent, danger, clamp(value, 0, 1));
      } else {
        const t = (value - range.min) / (range.max - range.min);
        color = lerpColor(danger, accent, t);
      }
    }

    const marker = L.circleMarker([lat, lon], {
      radius: 7,
      color,
      weight: 2,
      fillColor: color,
      fillOpacity: 0.78,
    }).addTo(hotspotsLayer);

    const label = metricLabel(metric);
    const formatted = formatMetricValue(metric, value);
    marker.bindPopup(`${row.segment_id} · ${label}: ${formatted}`, { closeButton: false });
    marker.on("click", () => {
      selectSegment(String(row.segment_id), { centerMap: false });
      loadTimeseries();
    });

    rendered += 1;
  }

  const minText =
    metric === "congestion_frequency"
      ? `${Math.round(range.min * 100)}%`
      : `${Number(range.min).toFixed(1)}`;
  const maxText =
    metric === "congestion_frequency"
      ? `${Math.round(range.max * 100)}%`
      : `${Number(range.max).toFixed(1)}`;
  updateHotspotInfo(`Loaded ${rendered} segments.\nMetric: ${metricLabel(metric)}\nRange: ${minText} → ${maxText}`);
}

function clearHotspots() {
  hotspotRows = [];
  hotspotsLoaded = false;
  lastHotspotsReason = null;
  hotspotsLayer.clearLayers();
  updateHotspotLegend(null, null);
  updateHotspotInfo("No hotspots loaded.");
  setStatus("Hotspots cleared.");
}

async function loadHotspots() {
  setStatus("Loading hotspots...");

  const metric = hotspotMetricEl.value || "mean_speed_kph";
  const url = new URL(`${API_BASE}/map/snapshot`);

  const range = getIsoRange();
  if (range) {
    url.searchParams.set("start", range.start);
    url.searchParams.set("end", range.end);
  }

  const minutes = minutesEl.value;
  if (minutes) url.searchParams.set("minutes", minutes);

  const bounds = map.getBounds();
  url.searchParams.set(
    "bbox",
    `${bounds.getWest()},${bounds.getSouth()},${bounds.getEast()},${bounds.getNorth()}`
  );
  url.searchParams.set("limit", "5000");
  applyReliabilityOverrides(url);

  try {
    const { items, reason, cache } = await fetchItems(url.toString());
    hotspotRows = items;
    lastHotspotsReason = reason;
    recordCache("hotspots", cache);
  } catch (err) {
    hotspotsLoaded = false;
    lastHotspotsReason = null;
    updateHotspotInfo("Failed to load hotspots. Ensure the API includes /map/snapshot and a dataset is built.");
    setStatus(`Failed to load hotspots: ${err.message}`);
    updateHotspotsHint({ empty: false });
    return;
  }

  hotspotsLoaded = true;
  renderHotspots(hotspotRows, metric);
  setStatus(`Hotspots loaded (${hotspotRows.length} rows).`);
}

function applyHotspotStateToForm() {
  if (!hotspotAutoEl) return;
  hotspotAutoEl.checked = Boolean(getNested(uiState, "hotspots.autoReload", false));
}

function initHotspotAutoReload() {
  if (!hotspotAutoEl) return;
  applyHotspotStateToForm();
  hotspotAutoEl.addEventListener("change", () => {
    uiState.hotspots = uiState.hotspots || {};
    uiState.hotspots.autoReload = Boolean(hotspotAutoEl.checked);
    saveUiState(uiState);
  });

  let timer = null;
  let inFlight = false;
  let pending = false;

  const schedule = () => {
    if (!hotspotAutoEl.checked) return;
    if (!showHotspots) return;
    if (!hotspotsLoaded) return;
    if (timer) window.clearTimeout(timer);
    timer = window.setTimeout(async () => {
      timer = null;
      if (inFlight) {
        pending = true;
        return;
      }
      inFlight = true;
      try {
        await loadHotspots();
      } finally {
        inFlight = false;
        if (pending) {
          pending = false;
          schedule();
        }
      }
    }, 450);
  };

  map.on("moveend", schedule);
  map.on("zoomend", schedule);
}

async function loadAnomalies(entity, range, minutes) {
  const url =
    entity.type === "corridor"
      ? new URL(`${API_BASE}/anomalies/corridors`)
      : new URL(`${API_BASE}/anomalies`);

  if (entity.type === "corridor") url.searchParams.set("corridor_id", entity.id);
  else url.searchParams.set("segment_id", entity.id);

  url.searchParams.set("start", range.start);
  url.searchParams.set("end", range.end);
  if (minutes) url.searchParams.set("minutes", minutes);
  applyAnomaliesOverrides(url);

  try {
    return await fetchJson(url.toString());
  } catch (err) {
    return null;
  }
}

function renderTimeseries(points, { title, anomalies } = {}) {
  if (!points.length) {
    Plotly.purge(chartEl);
    setStatus("No data returned for this time range.");
    return;
  }

  const colors = themeColors();
  const x = points.map((p) => p.timestamp);
  const speed = points.map((p) => p.speed_kph);
  const volume = points.map((p) => p.volume);

  const traces = [
    {
      x,
      y: speed,
      type: "scatter",
      mode: "lines",
      name: "Speed (kph)",
      line: { color: colors.accentHex, width: 2 },
      yaxis: "y",
    },
    {
      x,
      y: volume,
      type: "bar",
      name: "Volume",
      marker: { color: rgba(colors.accentRgb, 0.18) },
      yaxis: "y2",
    },
  ];

  if (anomalies && anomalies.length) {
    const byTs = new Map(anomalies.map((a) => [a.timestamp, a]));
    const baseline = x.map((t) => {
      const row = byTs.get(t);
      return row && row.baseline_mean_kph != null ? row.baseline_mean_kph : null;
    });

    const anomalyX = [];
    const anomalyY = [];
    for (let i = 0; i < x.length; i += 1) {
      const row = byTs.get(x[i]);
      if (row && row.is_anomaly) {
        anomalyX.push(x[i]);
        anomalyY.push(speed[i]);
      }
    }

    traces.push({
      x,
      y: baseline,
      type: "scatter",
      mode: "lines",
      name: "Baseline (mean)",
      line: { color: colors.muted, width: 1, dash: "dot" },
      yaxis: "y",
    });

    traces.push({
      x: anomalyX,
      y: anomalyY,
      type: "scatter",
      mode: "markers",
      name: "Anomaly",
      marker: { size: 7, color: colors.dangerHex, line: { width: 1, color: "rgba(15,23,42,0.18)" } },
      yaxis: "y",
    });
  }

  const layout = {
    title: {
      text: title || "Time Series",
      font: { size: 12, color: colors.text },
      x: 0.02,
    },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    margin: { l: 48, r: 48, t: 32, b: 34 },
    xaxis: {
      type: "date",
      gridcolor: colors.panelBorder,
      tickfont: { color: colors.muted, size: 10 },
    },
    yaxis: {
      title: { text: "Speed (kph)", font: { size: 10, color: colors.muted } },
      gridcolor: colors.panelBorder,
      tickfont: { color: colors.muted, size: 10 },
    },
    yaxis2: {
      title: { text: "Volume", font: { size: 10, color: colors.muted } },
      overlaying: "y",
      side: "right",
      tickfont: { color: colors.muted, size: 10 },
      showgrid: false,
    },
    legend: {
      orientation: "h",
      x: 0.02,
      y: 1.12,
      font: { size: 10, color: colors.muted },
    },
  };

  Plotly.react(chartEl, traces, layout, { responsive: true, displayModeBar: false });
  setStatus(`Loaded ${points.length} points.`);
}

function renderEventImpactChart(impact) {
  const points = impact.timeseries || [];
  if (!points.length) {
    Plotly.purge(chartEl);
    setStatus("No time series available for this event impact.");
    return;
  }

  const colors = themeColors();
  const x = points.map((p) => p.timestamp);
  const speed = points.map((p) => p.speed_kph);
  const volume = points.map((p) => p.volume);

  const baselineMean = impact.baseline_mean_speed_kph != null ? Number(impact.baseline_mean_speed_kph) : null;
  const baselineLine = baselineMean != null ? x.map(() => baselineMean) : x.map(() => null);

  const traces = [
    {
      x,
      y: speed,
      type: "scatter",
      mode: "lines",
      name: "Speed (kph)",
      line: { color: colors.accentHex, width: 2 },
      yaxis: "y",
    },
    {
      x,
      y: volume,
      type: "bar",
      name: "Volume",
      marker: { color: rgba(colors.accentRgb, 0.18) },
      yaxis: "y2",
    },
  ];

  if (baselineMean != null) {
    traces.push({
      x,
      y: baselineLine,
      type: "scatter",
      mode: "lines",
      name: "Baseline mean",
      line: { color: colors.muted, width: 1, dash: "dot" },
      yaxis: "y",
    });
  }

  const shapes = [];
  if (impact.event && impact.event.start_time && impact.event.end_time) {
    shapes.push({
      type: "rect",
      xref: "x",
      yref: "paper",
      x0: impact.event.start_time,
      x1: impact.event.end_time,
      y0: 0,
      y1: 1,
      fillcolor: rgba(colors.dangerRgb, 0.1),
      line: { width: 0 },
    });
  }

  const title = impact.event ? `Event ${impact.event.event_id}` : "Event impact";
  const layout = {
    title: { text: title, font: { size: 12, color: colors.text }, x: 0.02 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    margin: { l: 48, r: 48, t: 32, b: 34 },
    xaxis: {
      type: "date",
      gridcolor: colors.panelBorder,
      tickfont: { color: colors.muted, size: 10 },
    },
    yaxis: {
      title: { text: "Speed (kph)", font: { size: 10, color: colors.muted } },
      gridcolor: colors.panelBorder,
      tickfont: { color: colors.muted, size: 10 },
    },
    yaxis2: {
      title: { text: "Volume", font: { size: 10, color: colors.muted } },
      overlaying: "y",
      side: "right",
      tickfont: { color: colors.muted, size: 10 },
      showgrid: false,
    },
    legend: {
      orientation: "h",
      x: 0.02,
      y: 1.12,
      font: { size: 10, color: colors.muted },
    },
    shapes,
  };

  Plotly.react(chartEl, traces, layout, { responsive: true, displayModeBar: false });
}

async function loadTimeseries() {
  const entity = getSelectedEntity();
  if (!entity.id) {
    setStatus(entity.type === "corridor" ? "Select a corridor first." : "Select a segment first.");
    return;
  }

  const range = getIsoRange();
  if (!range) {
    setStatus("Invalid time range. Please select start/end.");
    return;
  }

  const minutes = minutesEl.value;
  const url =
    entity.type === "corridor"
      ? new URL(`${API_BASE}/timeseries/corridors`)
      : new URL(`${API_BASE}/timeseries`);

  if (entity.type === "corridor") url.searchParams.set("corridor_id", entity.id);
  else url.searchParams.set("segment_id", entity.id);
  url.searchParams.set("start", range.start);
  url.searchParams.set("end", range.end);
  if (minutes) url.searchParams.set("minutes", minutes);

  setStatus("Loading timeseries...");
  try {
    const raw = await fetchJson(url.toString());
    const points =
      entity.type === "corridor"
        ? raw.map((p) => ({ timestamp: p.timestamp, speed_kph: p.speed_kph, volume: p.volume }))
        : raw.map((p) => ({ timestamp: p.timestamp, speed_kph: p.speed_kph, volume: p.volume }));

    lastTimeseries = { entity, range, minutes, points, anomalies: null };

    let anomalies = null;
    if (showAnomalies) {
      anomalies = await loadAnomalies(entity, range, minutes);
      lastTimeseries.anomalies = anomalies;
    }

    renderTimeseries(points, { title: getEntityTitle(entity), anomalies });
  } catch (err) {
    setStatus(`Failed to load timeseries: ${err.message}`);
  }
}

function selectSegment(segmentId, { centerMap } = { centerMap: true }) {
  entityTypeEl.value = "segment";
  selectedSegmentId = segmentId;
  const seg = segmentsById.get(segmentId);
  updateSegmentInfo(seg);
  segmentSelectEl.value = segmentId;

  const marker = markerById.get(segmentId);
  if (centerMap && marker) {
    map.setView(marker.getLatLng(), Math.max(map.getZoom(), 14), { animate: true });
    marker.openPopup();
  }
  scheduleUrlSync();
}

function centerMapOnCorridor(corridorId) {
  const corridor = corridorsById.get(corridorId);
  const minLat = corridor?.min_lat;
  const minLon = corridor?.min_lon;
  const maxLat = corridor?.max_lat;
  const maxLon = corridor?.max_lon;
  if (
    minLat == null ||
    minLon == null ||
    maxLat == null ||
    maxLon == null ||
    !Number.isFinite(Number(minLat)) ||
    !Number.isFinite(Number(minLon)) ||
    !Number.isFinite(Number(maxLat)) ||
    !Number.isFinite(Number(maxLon))
  ) {
    const centerLat = corridor?.center_lat;
    const centerLon = corridor?.center_lon;
    if (
      centerLat != null &&
      centerLon != null &&
      Number.isFinite(Number(centerLat)) &&
      Number.isFinite(Number(centerLon))
    ) {
      map.setView([Number(centerLat), Number(centerLon)], Math.max(map.getZoom(), 12), { animate: true });
    }
    return;
  }

  const bounds = L.latLngBounds([Number(minLat), Number(minLon)], [Number(maxLat), Number(maxLon)]);
  map.fitBounds(bounds.pad(0.08), { animate: true });
}

function selectCorridor(corridorId, { centerMap } = { centerMap: false }) {
  entityTypeEl.value = "corridor";
  selectedCorridorId = corridorId;
  const corridor = corridorsById.get(corridorId);
  updateCorridorInfo(corridor);
  corridorSelectEl.value = corridorId;
  if (centerMap) {
    centerMapOnCorridor(corridorId);
    setStatus(`Centered on corridor ${corridorId}.`);
  }
  scheduleUrlSync();
}

function selectEvent(eventId, { centerMap } = { centerMap: true }) {
  selectedEventId = eventId;
  const event = eventsById.get(eventId);
  updateEventInfo(event, null);

  if (eventsLoaded) renderEvents(getFilteredEvents(events));

  impactSegmentsLayer.clearLayers();
  lastEventImpact = null;

  const marker = eventMarkerById.get(eventId);
  if (centerMap && marker) {
    map.setView(marker.getLatLng(), Math.max(map.getZoom(), 14), { animate: true });
    marker.openPopup();
  }

  if (!event) return;

  const minutes = minutesEl.value;
  const url = new URL(`${API_BASE}/events/${encodeURIComponent(eventId)}/impact`);
  url.searchParams.set("include_timeseries", "true");
  if (minutes) url.searchParams.set("minutes", minutes);
  applyImpactOverrides(url);

  setStatus("Loading event impact...");
  fetchJson(url.toString())
    .then((impact) => {
      lastEventImpact = impact;
      updateEventInfo(event, impact);
      renderEventImpactChart(impact);

      if (impact.affected_segments && impact.affected_segments.length) {
        for (const seg of impact.affected_segments) {
          const m = L.circleMarker([seg.lat, seg.lon], {
            radius: 5,
            color: themeColors().dangerHex,
            weight: 2,
            fillColor: rgba(themeColors().dangerRgb, 0.28),
            fillOpacity: 0.65,
          }).addTo(impactSegmentsLayer);
          m.bindPopup(`${seg.segment_id} (${Math.round(seg.distance_m)} m)`, { closeButton: false });
        }
      }

      setStatus("Event impact loaded.");
    })
    .catch((err) => {
      setStatus(`Failed to load event impact: ${err.message}`);
    });
}

async function loadSegments() {
  setStatus("Loading segments...");
  const url = `${API_BASE}/segments`;
  try {
    segments = await fetchJson(url);
  } catch (err) {
    setStatus(`Failed to load segments: ${err.message}`);
    return;
  }

  segmentsById = new Map(segments.map((s) => [s.segment_id, s]));

  const filtered = segments.slice(0, 2000);
  populateSegmentSelect(filtered);

  markers.clearLayers();
  markerById.clear();

  let bounds = null;
  for (const seg of segments) {
    if (seg.lat == null || seg.lon == null) continue;
    const lat = Number(seg.lat);
    const lon = Number(seg.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    const marker = L.circleMarker([lat, lon], {
      radius: 5,
      color: themeColors().accentHex,
      weight: 2,
      fillColor: rgba(themeColors().accentRgb, 0.22),
      fillOpacity: 0.6,
    }).addTo(markers);

    marker.bindPopup(formatSegmentLabel(seg), { closeButton: false });
    marker.on("click", () => {
      selectSegment(seg.segment_id, { centerMap: false });
      loadTimeseries();
    });

    markerById.set(seg.segment_id, marker);
    bounds = bounds ? bounds.extend([lat, lon]) : L.latLngBounds([lat, lon], [lat, lon]);
  }

  if (bounds) map.fitBounds(bounds.pad(0.08));
  setStatus(`Loaded ${segments.length} segments.`);
}

async function loadCorridors() {
  const url = `${API_BASE}/corridors`;
  try {
    corridors = await fetchJson(url);
  } catch (err) {
    corridorInfoEl.textContent = "Failed to load corridors. Check configs/corridors.csv.";
    return;
  }

  corridorsById = new Map(corridors.map((c) => [c.corridor_id, c]));
  populateCorridorSelect(corridors);
}

function applySearchFilter() {
  const q = normalizeText(segmentSearchEl.value);
  if (!q) {
    populateSegmentSelect(segments.slice(0, 2000));
    return;
  }
  const filtered = segments
    .filter((seg) => {
      return (
        normalizeText(seg.segment_id).includes(q) ||
        normalizeText(seg.name).includes(q) ||
        normalizeText(seg.city).includes(q)
      );
    })
    .slice(0, 2000);
  populateSegmentSelect(filtered);
}

function renderRankings(items, type) {
  rankingsEl.innerHTML = "";
  if (!items || !items.length) {
    rankingsEl.textContent = "No rankings returned.";
    updateRankingsHint({ empty: true });
    return;
  }
  updateRankingsHint({ empty: false });

  const q = normalizeText(rankingSearchEl ? rankingSearchEl.value : "");
  let filtered = items;
  if (q) {
    filtered = items.filter((row) => {
      if (type === "corridors") {
        return (
          normalizeText(row.corridor_id).includes(q) ||
          normalizeText(row.corridor_name).includes(q)
        );
      }
      return normalizeText(row.segment_id).includes(q);
    });
  }

  const sortMode = rankingSortEl ? rankingSortEl.value : "rank";
  const sorted = filtered.slice();
  const byNumber = (v) => (v == null ? NaN : Number(v));
  if (sortMode === "score_desc") {
    sorted.sort((a, b) => (byNumber(b.reliability_score) || 0) - (byNumber(a.reliability_score) || 0));
  } else if (sortMode === "mean_speed_asc") {
    sorted.sort((a, b) => (byNumber(a.mean_speed_kph) || 0) - (byNumber(b.mean_speed_kph) || 0));
  } else if (sortMode === "congestion_desc") {
    sorted.sort(
      (a, b) => (byNumber(b.congestion_frequency) || 0) - (byNumber(a.congestion_frequency) || 0)
    );
  }

  for (const row of sorted) {
    const el = document.createElement("div");
    el.className = "ranking-row";

    const rankEl = document.createElement("div");
    rankEl.className = "ranking-rank";
    rankEl.textContent = `#${row.rank}`;

    const mainEl = document.createElement("div");
    mainEl.className = "ranking-main";

    const idEl = document.createElement("div");
    idEl.className = "ranking-id";

    const scoreEl = document.createElement("div");
    scoreEl.className = "ranking-score";
    scoreEl.textContent =
      row.reliability_score != null ? Number(row.reliability_score).toFixed(3) : "—";

    const subEl = document.createElement("div");
    subEl.className = "ranking-sub";
    const mean = row.mean_speed_kph != null ? Number(row.mean_speed_kph).toFixed(1) : "—";
    const freq =
      row.congestion_frequency != null ? `${Math.round(Number(row.congestion_frequency) * 100)}%` : "—";
    subEl.textContent = `Mean: ${mean} kph · Cong: ${freq}`;

    if (type === "corridors") {
      const name = row.corridor_name ? ` - ${row.corridor_name}` : "";
      idEl.textContent = `${row.corridor_id}${name}`;
      el.addEventListener("click", () => {
        Array.from(rankingsEl.querySelectorAll(".ranking-row")).forEach((n) => n.classList.remove("selected"));
        el.classList.add("selected");
        selectCorridor(row.corridor_id, { centerMap: true });
        loadTimeseries();
      });
    } else {
      idEl.textContent = row.segment_id;
      el.addEventListener("click", () => {
        Array.from(rankingsEl.querySelectorAll(".ranking-row")).forEach((n) => n.classList.remove("selected"));
        el.classList.add("selected");
        selectSegment(row.segment_id, { centerMap: true });
        loadTimeseries();
      });
    }

    mainEl.appendChild(idEl);
    mainEl.appendChild(subEl);

    el.appendChild(rankEl);
    el.appendChild(mainEl);
    el.appendChild(scoreEl);
    rankingsEl.appendChild(el);
  }
}

async function loadRankings() {
  const type = rankingTypeEl.value || "segments";
  const limit = rankingLimitEl.value || "20";
  const minutes = minutesEl.value;

  const url =
    type === "corridors"
      ? new URL(`${API_BASE}/rankings/reliability/corridors`)
      : new URL(`${API_BASE}/rankings/reliability`);

  url.searchParams.set("limit", limit);
  if (minutes) url.searchParams.set("minutes", minutes);
  applyReliabilityOverrides(url);

  const range = getIsoRange();
  if (range) {
    url.searchParams.set("start", range.start);
    url.searchParams.set("end", range.end);
  }

  setStatus("Loading rankings...");
  try {
    const { items, reason, cache } = await fetchItems(url.toString());
    lastRankings = items;
    lastRankingsReason = reason;
    recordCache("rankings", cache);
    renderRankings(items, type);
    rankingsLoaded = true;
    setStatus(`Loaded ${items.length} ranking rows.`);
  } catch (err) {
    rankingsLoaded = false;
    lastRankingsReason = null;
    rankingsEl.textContent = "Failed to load rankings.";
    updateRankingsHint({ empty: false });
    setStatus(`Failed to load rankings: ${err.message}`);
  }
}

function _eventOverlapsRange(event, range) {
  if (!range) return true;
  const startMs = event?.start_time ? Date.parse(event.start_time) : NaN;
  const endMs = event?.end_time ? Date.parse(event.end_time) : startMs;
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return true;
  const rangeStartMs = Date.parse(range.start);
  const rangeEndMs = Date.parse(range.end);
  if (!Number.isFinite(rangeStartMs) || !Number.isFinite(rangeEndMs)) return true;
  return startMs < rangeEndMs && endMs > rangeStartMs;
}

function getFilteredEvents(items) {
  if (!items || !items.length) return [];

  const q = normalizeText(eventsSearchEl ? eventsSearchEl.value : "");
  const type = eventsTypeEl ? String(eventsTypeEl.value || "all") : "all";
  const onlyWithinRange = Boolean(eventsWithinRangeEl && eventsWithinRangeEl.checked);
  const range = onlyWithinRange ? getIsoRange() : null;

  return items.filter((event) => {
    if (!event) return false;

    if (type !== "all" && String(event.event_type || "") !== type) return false;
    if (range && !_eventOverlapsRange(event, range)) return false;

    if (!q) return true;
    return (
      normalizeText(event.event_id).includes(q) ||
      normalizeText(event.road_name).includes(q) ||
      normalizeText(event.event_type).includes(q) ||
      normalizeText(event.description).includes(q)
    );
  });
}

function populateEventsTypeOptions(items) {
  if (!eventsTypeEl) return;
  const prev = String(eventsTypeEl.value || "all");
  const types = Array.from(
    new Set(
      (items || [])
        .map((e) => (e && e.event_type ? String(e.event_type) : ""))
        .filter((t) => t && t.trim())
    )
  ).sort((a, b) => a.localeCompare(b));

  eventsTypeEl.innerHTML = "";
  const all = document.createElement("option");
  all.value = "all";
  all.textContent = "All";
  eventsTypeEl.appendChild(all);

  for (const t of types) {
    const opt = document.createElement("option");
    opt.value = t;
    opt.textContent = t;
    eventsTypeEl.appendChild(opt);
  }

  eventsTypeEl.value = types.includes(prev) ? prev : "all";
}

function renderEventMarkers(items) {
  eventMarkers.clearLayers();
  eventMarkerById.clear();

  for (const event of items) {
    if (event.lat == null || event.lon == null) continue;
    const lat = Number(event.lat);
    const lon = Number(event.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    const marker = L.circleMarker([lat, lon], {
      radius: 6,
      color: themeColors().dangerHex,
      weight: 2,
      fillColor: rgba(themeColors().dangerRgb, 0.2),
      fillOpacity: 0.85,
    }).addTo(eventMarkers);

    const label = `${event.event_id}${event.event_type ? ` - ${event.event_type}` : ""}`;
    marker.bindPopup(label, { closeButton: false });
    marker.on("click", () => selectEvent(event.event_id, { centerMap: false }));
    eventMarkerById.set(event.event_id, marker);
  }
}

function applyEventsFilters() {
  if (!eventsLoaded) return;
  const filtered = getFilteredEvents(events);
  renderEventMarkers(filtered);
  renderEvents(filtered);

  if (selectedEventId) {
    const stillVisible = filtered.some((e) => e && e.event_id === selectedEventId);
    if (!stillVisible) {
      selectedEventId = null;
      lastEventImpact = null;
      impactSegmentsLayer.clearLayers();
      lastEventsClearedReason = "Event selection cleared (filtered out).";
      updateEventInfo(null, null);
    }
  }
}

function renderEvents(items) {
  eventsEl.innerHTML = "";
  if (!items || !items.length) {
    const parts = [];
    if (lastEventsReason && lastEventsReason.message) parts.push(lastEventsReason.message);
    parts.push("No events returned.");
    if (lastEventsReason && lastEventsReason.suggestion) parts.push(lastEventsReason.suggestion);
    eventsEl.textContent = parts.join("\n");
    return;
  }

  for (const event of items) {
    const el = document.createElement("div");
    el.className = "event-row";
    if (selectedEventId && event.event_id === selectedEventId) el.classList.add("selected");

    const timeEl = document.createElement("div");
    timeEl.className = "event-time";
    timeEl.textContent = formatEventTimeLocal(event.start_time);

    const mainEl = document.createElement("div");
    mainEl.className = "event-main";

    const titleEl = document.createElement("div");
    titleEl.className = "event-title";
    const title = event.road_name || event.event_type || "Event";
    titleEl.textContent = title;

    const subEl = document.createElement("div");
    subEl.className = "event-sub";
    const desc = event.description || event.event_id;
    subEl.textContent = desc;

    const badgeEl = document.createElement("div");
    badgeEl.className = "event-badge";
    badgeEl.textContent = event.severity != null ? `sev ${event.severity}` : "";

    mainEl.appendChild(titleEl);
    mainEl.appendChild(subEl);

    el.appendChild(timeEl);
    el.appendChild(mainEl);
    el.appendChild(badgeEl);

    el.addEventListener("click", () => selectEvent(event.event_id));
    eventsEl.appendChild(el);
  }
}

async function loadEvents() {
  setStatus("Loading events...");
  const url = new URL(`${API_BASE}/events`);

  const range = getIsoRange();
  if (range) {
    url.searchParams.set("start", range.start);
    url.searchParams.set("end", range.end);
  }

  const bounds = map.getBounds();
  url.searchParams.set(
    "bbox",
    `${bounds.getWest()},${bounds.getSouth()},${bounds.getEast()},${bounds.getNorth()}`
  );
  url.searchParams.set("limit", "1000");

  try {
    const { items, reason, cache } = await fetchItems(url.toString());
    events = items;
    lastEventsReason = reason;
    recordCache("events", cache);
  } catch (err) {
    eventsLoaded = false;
    eventsEl.textContent = "Failed to load events. Run scripts/build_events.py and check ingestion.events config.";
    setStatus(`Failed to load events: ${err.message}`);
    return;
  }

  eventsLoaded = true;
  eventsById = new Map(events.map((e) => [e.event_id, e]));

  impactSegmentsLayer.clearLayers();
  selectedEventId = null;
  lastEventImpact = null;
  lastEventsClearedReason = null;
  updateEventInfo(null, null);
  populateEventsTypeOptions(events);
  applyEventsFilters();
  setStatus(`Loaded ${events.length} events (${getFilteredEvents(events).length} shown).`);
}

function clearEvents() {
  events = [];
  eventsLoaded = false;
  eventsById = new Map();
  lastEventsReason = null;
  eventMarkers.clearLayers();
  eventMarkerById.clear();
  impactSegmentsLayer.clearLayers();
  selectedEventId = null;
  lastEventImpact = null;
  lastEventsClearedReason = null;
  updateEventInfo(null, null);
  setHintVisible(eventsHintEl, false);
  if (eventsEl) eventsEl.textContent = "No events loaded.";
  setStatus("Events cleared.");
}

function initEventsPanel() {
  if (eventsAutoEl) {
    eventsAutoEl.checked = Boolean(getNested(uiState, "events.autoReload", false));
    eventsAutoEl.addEventListener("change", () => {
      uiState.events = uiState.events || {};
      uiState.events.autoReload = Boolean(eventsAutoEl.checked);
      saveUiState(uiState);
    });
  }

  if (eventsTypeEl) {
    eventsTypeEl.value = String(getNested(uiState, "events.type", "all"));
    eventsTypeEl.addEventListener("change", () => {
      uiState.events = uiState.events || {};
      uiState.events.type = String(eventsTypeEl.value || "all");
      saveUiState(uiState);
      applyEventsFilters();
    });
  }

  if (eventsWithinRangeEl) {
    eventsWithinRangeEl.checked = Boolean(getNested(uiState, "events.onlyWithinRange", false));
    eventsWithinRangeEl.addEventListener("change", () => {
      uiState.events = uiState.events || {};
      uiState.events.onlyWithinRange = Boolean(eventsWithinRangeEl.checked);
      saveUiState(uiState);
      applyEventsFilters();
    });
  }

  if (eventsSearchEl) {
    eventsSearchEl.addEventListener("input", () => {
      applyEventsFilters();
    });
  }

  if (clearEventsButton) {
    clearEventsButton.addEventListener("click", clearEvents);
  }

  if (eventsRelaxFiltersButton) {
    eventsRelaxFiltersButton.addEventListener("click", () => {
      if (eventsWithinRangeEl) eventsWithinRangeEl.checked = false;
      if (eventsSearchEl) eventsSearchEl.value = "";
      if (eventsTypeEl) eventsTypeEl.value = "all";
      uiState.events = uiState.events || {};
      uiState.events.onlyWithinRange = false;
      uiState.events.type = "all";
      saveUiState(uiState);
      applyEventsFilters();
      setHintVisible(eventsHintEl, false);
      setStatus("Events filters relaxed.");
    });
  }

  let timer = null;
  let inFlight = false;
  let pending = false;
  const schedule = () => {
    if (!eventsAutoEl || !eventsAutoEl.checked) return;
    if (!showEvents) return;
    if (!eventsLoaded) return;
    if (timer) window.clearTimeout(timer);
    timer = window.setTimeout(async () => {
      timer = null;
      if (inFlight) {
        pending = true;
        return;
      }
      inFlight = true;
      try {
        await loadEvents();
      } finally {
        inFlight = false;
        if (pending) {
          pending = false;
          schedule();
        }
      }
    }, 450);
  };

  map.on("moveend", schedule);
  map.on("zoomend", schedule);
}

function initQuickGuides() {
  if (hotspotQuickMinSamplesEl) hotspotQuickMinSamplesEl.addEventListener("click", quickSetMinSamplesOne);
  if (hotspotQuick24hEl) hotspotQuick24hEl.addEventListener("click", quickUse24hWindow);
  if (rankingsQuickMinSamplesEl) rankingsQuickMinSamplesEl.addEventListener("click", quickSetMinSamplesOne);
  if (rankingsQuick24hEl) rankingsQuick24hEl.addEventListener("click", quickUse24hWindow);
}

segmentSearchEl.addEventListener("input", applySearchFilter);
segmentSelectEl.addEventListener("change", () => {
  const segmentId = segmentSelectEl.value;
  if (!segmentId) return;
  selectSegment(segmentId, { centerMap: true });
  loadTimeseries();
});
corridorSelectEl.addEventListener("change", () => {
  const corridorId = corridorSelectEl.value;
  if (!corridorId) return;
  selectCorridor(corridorId);
  loadTimeseries();
});
if (entityTypeEl) entityTypeEl.addEventListener("change", scheduleUrlSync);
loadButton.addEventListener("click", loadTimeseries);
loadRankingsButton.addEventListener("click", loadRankings);
loadEventsButton.addEventListener("click", loadEvents);
loadHotspotsButton.addEventListener("click", loadHotspots);
clearHotspotsButton.addEventListener("click", clearHotspots);
hotspotMetricEl.addEventListener("change", () => renderHotspots(hotspotRows, hotspotMetricEl.value));
if (rankingSearchEl) rankingSearchEl.addEventListener("input", () => rankingsLoaded && renderRankings(lastRankings, rankingTypeEl.value || "segments"));
if (rankingSortEl) rankingSortEl.addEventListener("change", () => rankingsLoaded && renderRankings(lastRankings, rankingTypeEl.value || "segments"));
hotspotMetricEl.addEventListener("change", scheduleUrlSync);
if (minutesEl) minutesEl.addEventListener("change", scheduleUrlSync);
if (rankingTypeEl) rankingTypeEl.addEventListener("change", scheduleUrlSync);
if (rankingLimitEl) rankingLimitEl.addEventListener("change", scheduleUrlSync);
if (rankingSortEl) rankingSortEl.addEventListener("change", scheduleUrlSync);
if (startEl)
  startEl.addEventListener("change", () => {
    if (eventsWithinRangeEl?.checked) applyEventsFilters();
    scheduleUrlSync();
  });
if (endEl)
  endEl.addEventListener("change", () => {
    if (eventsWithinRangeEl?.checked) applyEventsFilters();
    scheduleUrlSync();
  });

applyLayoutFromState();
initThemeToggle();
applyOverlayVisibility();
initLayoutResizers();
initPanelCollapse();
initSettingsPanel();
initShortcutsOverlay();
initKeyboardShortcuts();
initLivePanel();
initHotspotAutoReload();
initTimeseriesFollowLatest();
initEventsPanel();
initQuickGuides();
if (copyLinkButton) copyLinkButton.addEventListener("click", copyShareLink);

loadUiDefaultsFromApi().then((defaults) => {
  if (defaults) applyDefaultsToForm(defaults);
  applyStateToForm(uiState);
  applyUrlOverridesToForm();
  applyThemeFromState();
  scheduleUrlSync();
});

setDefaultTimeRange();
Promise.allSettled([loadSegments(), loadCorridors()]).then(() => {
  if (pendingUrlSelection && pendingUrlSelection.type === "corridor") {
    const corridorId = String(pendingUrlSelection.id);
    if (corridorsById && corridorsById.has(corridorId)) {
      selectCorridor(corridorId, { centerMap: true });
      loadTimeseries();
    }
  } else if (pendingUrlSelection && pendingUrlSelection.type === "segment") {
    const segmentId = String(pendingUrlSelection.id);
    if (segmentsById && segmentsById.has(segmentId)) {
      selectSegment(segmentId, { centerMap: true });
      loadTimeseries();
    }
  }
  scheduleUrlSync();
});
