/* global L, Plotly */

const API_OVERRIDE_PARAM = new URLSearchParams(window.location.search).get("api");
const API_OVERRIDE_FORCE = new URLSearchParams(window.location.search).get("api_force") === "1";

const API_OVERRIDE_INFO = (() => {
  if (!API_OVERRIDE_PARAM) return { applied: false, ignored: false, reason: null, base: null };
  let url = null;
  try {
    url = new URL(API_OVERRIDE_PARAM);
  } catch (err) {
    return { applied: false, ignored: true, reason: "Invalid api override URL.", base: null };
  }
  const normalized = url.toString().replace(/\/$/, "");
  if (API_OVERRIDE_FORCE) return { applied: true, ignored: false, reason: null, base: normalized };

  // Avoid CORS footguns by default. If you really want cross-origin, add `api_force=1`.
  if (url.origin !== window.location.origin) {
    return {
      applied: false,
      ignored: true,
      reason: `Ignoring api override (${url.origin}) because it is cross-origin. Use api_force=1 if intentional.`,
      base: null,
    };
  }
  return { applied: true, ignored: false, reason: null, base: normalized };
})();

const API_BASE = (() => {
  if (API_OVERRIDE_INFO.applied && API_OVERRIDE_INFO.base) return API_OVERRIDE_INFO.base;
  if (window.location.port === "8003") return window.location.origin;
  return `${window.location.protocol}//${window.location.hostname}:8003`;
})();

const statusEl = document.getElementById("status");
const apiBaseEl = document.getElementById("api-base");
const themeSelectEl = document.getElementById("theme-select");
const topnavEl = document.getElementById("topnav");
const liveIndicatorEl = document.getElementById("live-indicator");
const liveDetailEl = document.getElementById("live-detail");
const rateIndicatorEl = document.getElementById("rate-indicator");
const trendIndicatorEl = document.getElementById("trend-indicator");
const weatherIndicatorEl = document.getElementById("weather-indicator");
const cacheIndicatorEl = document.getElementById("cache-indicator");
const copyLinkButton = document.getElementById("copy-link");
const exportSnapshotButton = document.getElementById("export-snapshot");
const qualityLinkEl = document.getElementById("link-quality");
const diagnosticsLinkEl = document.getElementById("link-diagnostics");
const alertsLinkEl = document.getElementById("link-alerts");

const EVENTS_FIX_COMMAND = `systemctl --user start trafficpulse-events.service\njournalctl --user -u trafficpulse-events.service -n 120 --no-pager`;

const pipelineRefreshButton = document.getElementById("pipeline-refresh");
const pipelineCopyEventsFixButton = document.getElementById("pipeline-copy-events-fix");
const pipelineSummaryEl = document.getElementById("pipeline-summary");
const pipelineTrendsChartEl = document.getElementById("pipeline-trends-chart");
const pipelineAlertsEl = document.getElementById("pipeline-alerts");

const dataHealthTextEl = document.getElementById("data-health-text");
const dataHealthRefreshButton = document.getElementById("data-health-refresh");
const dataHealthCopyCommandsButton = document.getElementById("data-health-copy-commands");
const dataHealthHintEl = document.getElementById("data-health-hint");
const dataHealthHintTextEl = document.getElementById("data-health-hint-text");
const exportSegmentsCsvButton = document.getElementById("export-segments-csv");
const exportCorridorsCsvButton = document.getElementById("export-corridors-csv");

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
const eventsHintTitleEl = document.getElementById("events-hint-title");
const eventsHintTextEl = document.getElementById("events-hint-text");
const eventsRelaxFiltersButton = document.getElementById("events-relax-filters");
const eventsCopyFixButton = document.getElementById("events-copy-fix");

const hotspotMetricEl = document.getElementById("hotspot-metric");
const hotspotColorModeEl = document.getElementById("hotspot-color-mode");
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

const mapToggleSegmentsEl = document.getElementById("map-toggle-segments");
const mapToggleHotspotsEl = document.getElementById("map-toggle-hotspots");
const mapToggleEventsEl = document.getElementById("map-toggle-events");
const mapToggleImpactEl = document.getElementById("map-toggle-impact");
const mapSpotlightEl = document.getElementById("map-spotlight");
const mapFocusButton = document.getElementById("map-focus");
const mapReloadHotspotsButton = document.getElementById("map-reload-hotspots");
const mapReloadEventsButton = document.getElementById("map-reload-events");
const mapClearSelectionButton = document.getElementById("map-clear-selection");
const mapShowNextStepButton = document.getElementById("map-show-nextstep");

const mapNextStepEl = document.getElementById("map-nextstep");
const mapNextStepTitleEl = document.getElementById("map-nextstep-title");
const mapNextStepTextEl = document.getElementById("map-nextstep-text");
const mapNextStepActionsEl = document.getElementById("map-nextstep-actions");
const mapNextStepCloseButton = document.getElementById("map-nextstep-close");

const overviewEl = document.getElementById("overview");
const overviewSentenceEl = document.getElementById("overview-sentence");
const overviewOpenExploreButton = document.getElementById("overview-open-explore");
const overviewCopyLinkButton = document.getElementById("overview-copy-link");
const overviewLoadHotspotsButton = document.getElementById("overview-load-hotspots");
const overviewLoadRankingsButton = document.getElementById("overview-load-rankings");
const overviewLoadEventsButton = document.getElementById("overview-load-events");
const overviewQuick24hButton = document.getElementById("overview-quick-24h");
const overviewMinSamples1Button = document.getElementById("overview-min-samples-1");
const overviewCopyEventsFixButton = document.getElementById("overview-copy-events-fix");
const overviewHotspotsMetricEl = document.getElementById("overview-hotspots-metric");
const overviewHotspotsTextEl = document.getElementById("overview-hotspots-text");
const overviewRankingsMetricEl = document.getElementById("overview-rankings-metric");
const overviewRankingsTextEl = document.getElementById("overview-rankings-text");
const overviewEventsMetricEl = document.getElementById("overview-events-metric");
const overviewEventsTextEl = document.getElementById("overview-events-text");

const storyEl = document.getElementById("page-story");
const storyTitleEl = document.getElementById("story-title");
const storySubtitleEl = document.getElementById("story-subtitle");
const storyNarrativeEl = document.getElementById("story-narrative");
const storyNextListEl = document.getElementById("story-next-list");
const storyPrimaryButtonEl = document.getElementById("story-primary");
const storySecondaryButtonEl = document.getElementById("story-secondary");
const storyTertiaryButtonEl = document.getElementById("story-tertiary");
const kpiDataAgeCardEl = document.getElementById("kpi-data-age");
const kpiIngestCardEl = document.getElementById("kpi-ingest");
const kpi429CardEl = document.getElementById("kpi-429");
const kpiDatasetCardEl = document.getElementById("kpi-dataset");
const kpiPage1LabelEl = document.getElementById("kpi-page-1-label");
const kpiPage1ValueEl = document.getElementById("kpi-page-1-value");
const kpiPage2LabelEl = document.getElementById("kpi-page-2-label");
const kpiPage2ValueEl = document.getElementById("kpi-page-2-value");
let exploreStoryCollapseButtonEl = null;
let exploreStoryCollapsedSummaryEl = null;

const timeseriesKpisEl = document.getElementById("timeseries-kpis");
const timeseriesControlsEl = document.getElementById("timeseries-controls");
const timeseriesNarrativeEl = document.getElementById("timeseries-narrative");
const eventsNarrativeEl = document.getElementById("events-narrative");
const tsLayerSpeedEl = document.getElementById("ts-layer-speed");
const tsLayerVolumeEl = document.getElementById("ts-layer-volume");
const tsLayerBaselineEl = document.getElementById("ts-layer-baseline");
const tsLayerAnomaliesEl = document.getElementById("ts-layer-anomalies");
const tsLayerEventWindowEl = document.getElementById("ts-layer-event-window");
const tsApplyBrushButton = document.getElementById("ts-apply-brush");
const tsOpenExploreButton = document.getElementById("ts-open-explore");

const toggleAnomaliesEl = document.getElementById("toggle-anomalies");
const toggleHotspotsEl = document.getElementById("toggle-hotspots");
const toggleEventsEl = document.getElementById("toggle-events");
const toggleImpactEl = document.getElementById("toggle-impact");

const chartHintEl = document.getElementById("chart-hint");
const chartHintTitleEl = document.getElementById("chart-hint-title");
const chartHintTextEl = document.getElementById("chart-hint-text");
const chartHintActionsEl = document.getElementById("chart-hint-actions");

const reliabilityThresholdEl = document.getElementById("reliability-threshold");
const reliabilityMinSamplesEl = document.getElementById("reliability-min-samples");
const reliabilityMinCoverageEl = document.getElementById("reliability-min-coverage");
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
if (qualityLinkEl) qualityLinkEl.href = `${API_BASE}/ui/quality`;
if (diagnosticsLinkEl) diagnosticsLinkEl.href = `${API_BASE}/ui/diagnostics`;
if (alertsLinkEl) alertsLinkEl.href = `${API_BASE}/ui/alerts?tail=400`;

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
  const warnHex = getCssVar("--warn", "#f59e0b");
  const text = getCssVar("--text", "rgba(15, 23, 42, 0.92)");
  const muted = getCssVar("--muted", "rgba(51, 65, 85, 0.78)");
  const panelBorder = getCssVar("--panel-border", "rgba(15, 23, 42, 0.10)");
  return {
    accentHex,
    accent2Hex,
    dangerHex,
    warnHex,
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

const haloPane = map.createPane("haloPane");
haloPane.style.zIndex = 650;

const markers = L.layerGroup().addTo(map);
const eventMarkers = L.layerGroup().addTo(map);
const impactSegmentsLayer = L.layerGroup().addTo(map);
const hotspotsLayer = L.layerGroup().addTo(map);
const linkedHotspotsLayer = L.layerGroup().addTo(map);
const selectionHaloLayer = L.layerGroup().addTo(map);

let segmentSelectionHalo = null;
let eventSelectionHalo = null;

let uiDefaults = null;
let uiState = loadUiState();
const apiOverrideParam = API_OVERRIDE_INFO.applied ? API_OVERRIDE_PARAM : null;
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
    page: params.get("page"),
    theme: params.get("theme"),
    minutes: params.get("minutes"),
    follow_latest: parseBoolParam(params.get("follow_latest")),
    window_h: parseNumberParam(params.get("window_h")),
    start: params.get("start"),
    end: params.get("end"),
    hotspot_metric: params.get("hotspot_metric"),
    hotspot_color_mode: params.get("hotspot_color_mode"),
    ranking_type: params.get("ranking_type"),
    ranking_limit: params.get("ranking_limit"),
    ranking_sort: params.get("ranking_sort"),
    min_samples: parseNumberParam(params.get("min_samples")),
    min_coverage_pct: parseNumberParam(params.get("min_coverage_pct")),
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

  const page = urlOverrides.page;
  if (page) {
    uiState.layout = uiState.layout || {};
    uiState.layout.page = String(page);
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
  const minCoverage = urlOverrides.min_coverage_pct;
  if (minSamples != null || threshold != null || minCoverage != null) {
    uiState.overrides = uiState.overrides || {};
    uiState.overrides.reliability = uiState.overrides.reliability || {};
    if (minSamples != null) uiState.overrides.reliability.min_samples = Math.max(1, Math.trunc(minSamples));
    if (threshold != null) uiState.overrides.reliability.congestion_speed_threshold_kph = Math.max(1, threshold);
    if (minCoverage != null) uiState.overrides.reliability.min_coverage_pct = clamp(minCoverage, 0, 100);
  }

  const colorMode = urlOverrides.hotspot_color_mode;
  if (colorMode) {
    uiState.hotspots = uiState.hotspots || {};
    const mode = String(colorMode);
    uiState.hotspots.colorMode = ["metric", "relative_drop_pct", "coverage_pct"].includes(mode) ? mode : "metric";
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
  if (hotspotColorModeEl && urlOverrides.hotspot_color_mode)
    hotspotColorModeEl.value = String(urlOverrides.hotspot_color_mode);
  if (rankingTypeEl && urlOverrides.ranking_type) rankingTypeEl.value = String(urlOverrides.ranking_type);
  if (rankingLimitEl && urlOverrides.ranking_limit) rankingLimitEl.value = String(urlOverrides.ranking_limit);
  if (rankingSortEl && urlOverrides.ranking_sort) rankingSortEl.value = String(urlOverrides.ranking_sort);
  } finally {
    suppressUrlSync = false;
  }
}

function normalizePage(value) {
  const v = String(value || "").trim().toLowerCase();
  if (["overview", "explore", "timeseries", "events", "rankings", "pipeline"].includes(v)) return v;
  return "explore";
}

function getPageFromDocument() {
  const bodyPage = document.body && document.body.dataset ? document.body.dataset.page : null;
  if (bodyPage) return normalizePage(bodyPage);

  const path = String(window.location.pathname || "/").replace(/\/+$/, "/");
  const m = /^\/(overview|explore|timeseries|events|rankings|pipeline)\/$/.exec(path);
  if (m) return normalizePage(m[1]);
  return null;
}

const NAV_ACTION_KEY = "trafficpulse.nav_action.v1";

function setNavAction(action) {
  try {
    if (!action) {
      window.sessionStorage.removeItem(NAV_ACTION_KEY);
      return;
    }
    window.sessionStorage.setItem(NAV_ACTION_KEY, JSON.stringify({ action: String(action), at: Date.now() }));
  } catch (err) {
    // ignore
  }
}

function consumeNavAction() {
  try {
    const raw = window.sessionStorage.getItem(NAV_ACTION_KEY);
    if (!raw) return null;
    window.sessionStorage.removeItem(NAV_ACTION_KEY);
    const parsed = JSON.parse(raw);
    const action = parsed && typeof parsed === "object" ? parsed.action : null;
    return action ? String(action) : null;
  } catch (err) {
    return null;
  }
}

function navigateToPage(page, { action } = {}) {
  syncUrlFromUi();
  if (action) setNavAction(action);
  const next = normalizePage(page);
  const url = new URL(window.location.href);
  url.pathname = `/${next}/`;
  url.searchParams.delete("page"); // legacy SPA param
  window.location.assign(url.toString());
}

function setPage(page, { syncUrl } = { syncUrl: true }) {
  const next = normalizePage(page);
  currentPage = next;
  uiState.layout = uiState.layout || {};
  uiState.layout.page = next;
  saveUiState(uiState);

  document.body.classList.remove(
    "page-overview",
    "page-explore",
    "page-timeseries",
    "page-events",
    "page-rankings",
    "page-pipeline"
  );
  document.body.classList.add(`page-${next}`);

  if (topnavEl) {
    for (const btn of Array.from(topnavEl.querySelectorAll("[data-page]"))) {
      const p = btn.getAttribute("data-page");
      btn.classList.toggle("active", p === next);
    }
  }

  if (overviewEl) overviewEl.classList.toggle("hidden", next !== "overview");

  if (next === "timeseries") {
    if (timeseriesKpisEl) timeseriesKpisEl.classList.remove("hidden");
    if (timeseriesControlsEl) timeseriesControlsEl.classList.remove("hidden");
    if (timeseriesNarrativeEl) timeseriesNarrativeEl.classList.remove("hidden");
  } else {
    if (timeseriesKpisEl) timeseriesKpisEl.classList.add("hidden");
    if (timeseriesControlsEl) timeseriesControlsEl.classList.add("hidden");
    if (timeseriesNarrativeEl) timeseriesNarrativeEl.classList.add("hidden");
    pendingBrushRange = null;
    if (tsApplyBrushButton) tsApplyBrushButton.classList.add("hidden");
  }
  if (eventsNarrativeEl) eventsNarrativeEl.classList.toggle("hidden", next !== "events");

  focusPagePanels(next);

  if (syncUrl) scheduleUrlSync();

  window.setTimeout(() => {
    try {
      map.invalidateSize({ animate: false });
    } catch (err) {
      // ignore
    }
  }, 50);

  renderEventsStory();
  renderStory();
  syncExploreStoryCollapseUi();
}

function getExploreStoryCollapsedSetting() {
  const stored = getNested(uiState, "layout.exploreStoryCollapsed", null);
  if (stored == null) return true; // default: collapsed for a cleaner map
  return Boolean(stored);
}

function setExploreStoryCollapsed(collapsed, { persist } = { persist: true }) {
  if (!storyEl) return;
  storyEl.classList.toggle("collapsed", Boolean(collapsed));
  if (exploreStoryCollapseButtonEl) exploreStoryCollapseButtonEl.textContent = collapsed ? "Expand" : "Collapse";
  if (persist) {
    uiState.layout = uiState.layout || {};
    uiState.layout.exploreStoryCollapsed = Boolean(collapsed);
    saveUiState(uiState);
  }
}

function syncExploreStoryCollapseUi() {
  if (!storyEl) return;
  if (currentPage !== "explore") return;

  const actions = storyEl.querySelector(".story-actions");
  const hero = storyEl.querySelector(".story-hero");
  if (!actions || !hero) return;

  if (!exploreStoryCollapsedSummaryEl) {
    const titleBlock = hero.firstElementChild;
    if (titleBlock) {
      const summary = document.createElement("div");
      summary.className = "story-collapsed-summary mono";
      summary.textContent = "";
      titleBlock.appendChild(summary);
      exploreStoryCollapsedSummaryEl = summary;
    }
  }

  if (!exploreStoryCollapseButtonEl) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "button secondary compact story-collapse-toggle";
    btn.textContent = "Collapse";
    btn.addEventListener("click", (ev) => {
      ev.preventDefault();
      const next = !storyEl.classList.contains("collapsed");
      setExploreStoryCollapsed(next);
    });
    actions.insertBefore(btn, actions.firstChild);
    exploreStoryCollapseButtonEl = btn;
  }

  if (!hero.dataset.storyCollapseInit) {
    hero.dataset.storyCollapseInit = "1";
    hero.addEventListener("click", (ev) => {
      if (!storyEl.classList.contains("collapsed")) return;
      const target = ev.target;
      if (target && typeof target.closest === "function" && target.closest(".story-collapse-toggle")) return;
      setExploreStoryCollapsed(false);
    });
  }

  setExploreStoryCollapsed(getExploreStoryCollapsedSetting(), { persist: false });
}

function focusPagePanels(page) {
  const panelId =
    page === "events"
      ? "events"
      : page === "rankings"
        ? "rankings"
        : page === "pipeline"
          ? "pipeline"
          : null;
  if (!panelId) return;
  const panel = document.querySelector(`.panel[data-panel-id="${panelId}"]`);
  if (!panel) return;
  panel.classList.remove("collapsed");
  panel.scrollIntoView({ behavior: "smooth", block: "start" });
}

function renderEventsStory() {
  if (!eventsNarrativeEl) return;
  if (currentPage !== "events") {
    eventsNarrativeEl.textContent = "";
    return;
  }

  if (!eventsLoaded) {
    eventsNarrativeEl.textContent =
      "Load events to start an incident story: markers → select an event → impact overlay → linked hotspots.";
    return;
  }

  if (!selectedEventId) {
    eventsNarrativeEl.textContent = "Pick an event marker or click an event in the list to see its impact story.";
    return;
  }

  const event = eventsById.get(String(selectedEventId));
  if (!event) {
    eventsNarrativeEl.textContent = "Selected event is not available in the current dataset.";
    return;
  }

  const parts = [];
  parts.push(`${event.event_type || "Event"} ${event.event_id}.`);
  if (event.road_name) parts.push(`Road: ${event.road_name}.`);
  if (event.start_time) parts.push(`Start: ${formatEventTimeLocal(event.start_time)}.`);
  if (event.severity != null) parts.push(`Severity: ${event.severity}.`);

  if (lastEventImpact) {
    const baseline = lastEventImpact.baseline_mean_speed_kph != null ? Number(lastEventImpact.baseline_mean_speed_kph) : null;
    const during = lastEventImpact.event_mean_speed_kph != null ? Number(lastEventImpact.event_mean_speed_kph) : null;
    const delta = lastEventImpact.speed_delta_mean_kph != null ? Number(lastEventImpact.speed_delta_mean_kph) : null;
    const rec = lastEventImpact.recovery_minutes != null ? Number(lastEventImpact.recovery_minutes) : null;
    if (baseline != null && during != null) {
      parts.push(`Mean speed: ${baseline.toFixed(1)} → ${during.toFixed(1)} kph.`);
    }
    if (delta != null) parts.push(`Delta: ${delta.toFixed(1)} kph.`);
    if (rec != null) parts.push(`Recovery: ${Math.round(rec)} min.`);
  } else {
    parts.push("Impact: loading…");
  }

  if (lastEventLinksInfo && !lastEventLinksInfo.loading) {
    parts.push(`Linked hotspots: ${Math.round(lastEventLinksInfo.count || 0)}.`);
  }

  parts.push("Tip: enable Spotlight on the map toolbar to focus on the selected event.");
  eventsNarrativeEl.textContent = parts.join(" ");
}

function syncUrlFromUi() {
  const params = new URLSearchParams();

  if (apiOverrideParam) params.set("api", apiOverrideParam);

  const theme = String(getNested(uiState, "layout.theme", "product"));
  if (theme && theme !== "product") params.set("theme", theme);

  const entityType = entityTypeEl && entityTypeEl.value ? String(entityTypeEl.value) : "segment";
  if (entityType) params.set("entity", entityType);
  if (entityType === "corridor") {
    if (selectedCorridorId) params.set("corridor_id", String(selectedCorridorId));
  } else if (selectedSegmentId) {
    params.set("segment_id", String(selectedSegmentId));
  }

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
  if (hotspotColorModeEl && hotspotColorModeEl.value && hotspotColorModeEl.value !== "metric")
    params.set("hotspot_color_mode", String(hotspotColorModeEl.value));
  if (rankingTypeEl && rankingTypeEl.value) params.set("ranking_type", String(rankingTypeEl.value));
  if (rankingLimitEl && rankingLimitEl.value) params.set("ranking_limit", String(rankingLimitEl.value));
  if (rankingSortEl && rankingSortEl.value) params.set("ranking_sort", String(rankingSortEl.value));

  const minSamples = reliabilityMinSamplesEl ? parseIntValue(reliabilityMinSamplesEl.value) : null;
  if (minSamples != null) params.set("min_samples", String(minSamples));
  const minCoverage = reliabilityMinCoverageEl ? parseNumberValue(reliabilityMinCoverageEl.value) : null;
  if (minCoverage != null) params.set("min_coverage_pct", String(minCoverage));
  const threshold = reliabilityThresholdEl ? parseNumberValue(reliabilityThresholdEl.value) : null;
  if (threshold != null) params.set("threshold_kph", String(threshold));

  const next = `${window.location.pathname}${params.toString() ? `?${params.toString()}` : ""}`;
  window.history.replaceState({}, "", next);
}

applyUrlStateOverrides();

let showAnomalies = getNested(uiState, "overlays.anomalies", true);
let showSegments = getNested(uiState, "overlays.segments", true);
let showHotspots = getNested(uiState, "overlays.hotspots", true);
let showEvents = getNested(uiState, "overlays.events", true);
let showImpact = getNested(uiState, "overlays.impact", true);
let spotlightMode = getNested(uiState, "map.spotlight", false);
let nextStepDismissed = getNested(uiState, "map.nextStepDismissed", false);

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
let currentPage = "explore";
let pendingBrushRange = null;
let suppressBrush = false;
let lastUiStatus = null;
let lastUiStatusPrev = null;
let uiStatusStream = null;
let uiStatusStreamRetryMs = 800;
let freshnessTicker = null;
let lastLiveRefreshAtMs = 0;
let lastCacheInfo = { hotspots: null, rankings: null, events: null };
let lastSseState = { mode: "polling", detail: "Polling" };
let hotspotMarkerBySegmentId = new Map();
let lastTrends = null;
let trendsRefreshTimer = null;
let weatherRefreshTimer = null;
let pipelineRefreshTimer = null;
let diagnosticsRefreshTimer = null;

const dataHub = (() => {
  const state = {
    status: null,
    trends: null,
    alerts: null,
    diagnostics: null,
    weather: null,
    lastUpdatedMs: {},
  };
  const listeners = [];

  function notify(kind) {
    for (const fn of listeners) {
      try {
        fn(state, kind);
      } catch (err) {
        // ignore
      }
    }
  }

  function onUpdate(fn) {
    if (typeof fn === "function") listeners.push(fn);
  }

  function set(kind, value) {
    state[kind] = value;
    state.lastUpdatedMs[kind] = Date.now();
    notify(kind);
  }

  async function refreshStatus() {
    const data = await fetchJson(`${API_BASE}/ui/status`);
    set("status", data);
    return data;
  }

  async function refreshTrends() {
    const data = await fetchJson(`${API_BASE}/ui/trends?window_hours=24`);
    set("trends", data);
    return data;
  }

  async function refreshAlerts() {
    const data = await fetchJson(`${API_BASE}/ui/alerts?tail=200&window_hours=24`);
    set("alerts", data);
    return data;
  }

  async function refreshDiagnostics() {
    const data = await fetchJson(`${API_BASE}/ui/diagnostics`);
    set("diagnostics", data);
    return data;
  }

  async function refreshWeather() {
    const data = await fetchJson(`${API_BASE}/ui/weather/latest?limit=1`);
    set("weather", data);
    return data;
  }

  async function refreshAll({ includeStatus } = { includeStatus: false }) {
    const jobs = [refreshTrends(), refreshAlerts(), refreshDiagnostics(), refreshWeather()];
    if (includeStatus) jobs.unshift(refreshStatus());
    await Promise.allSettled(jobs);
  }

  function start({ includeStatus } = { includeStatus: false }) {
    refreshAll({ includeStatus: Boolean(includeStatus) });
    if (trendsRefreshTimer) window.clearInterval(trendsRefreshTimer);
    trendsRefreshTimer = window.setInterval(() => refreshTrends().catch(() => {}), 60 * 1000);
    if (weatherRefreshTimer) window.clearInterval(weatherRefreshTimer);
    weatherRefreshTimer = window.setInterval(() => refreshWeather().catch(() => {}), 10 * 60 * 1000);
    if (pipelineRefreshTimer) window.clearInterval(pipelineRefreshTimer);
    pipelineRefreshTimer = window.setInterval(() => refreshAlerts().catch(() => {}), 2 * 60 * 1000);
    if (diagnosticsRefreshTimer) window.clearInterval(diagnosticsRefreshTimer);
    diagnosticsRefreshTimer = window.setInterval(() => refreshDiagnostics().catch(() => {}), 5 * 60 * 1000);
  }

  return { state, onUpdate, set, refreshAll, refreshStatus, refreshTrends, refreshAlerts, refreshDiagnostics, refreshWeather, start };
})();

let timeseriesAbortController = null;
let anomaliesAbortController = null;
let impactAbortController = null;
let impactCache = new Map();
let linksAbortController = null;
let lastEventLinksInfo = null;

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

function computeHealthLevel(status) {
  if (!status || typeof status !== "object") return { level: "bad", label: "DOWN" };

  if (status.last_ingest_ok === false) return { level: "bad", label: "DOWN" };
  const failures = typeof status.ingest_consecutive_failures === "number" ? status.ingest_consecutive_failures : 0;
  if (failures > 0) return { level: "warn", label: "WARN" };

  const lastMs = status.observations_last_timestamp_utc ? parseIsoToMs(status.observations_last_timestamp_utc) : null;
  if (lastMs == null) return { level: "bad", label: "NO DATA" };

  const ageSeconds = Math.max(0, (Date.now() - lastMs) / 1000);
  if (ageSeconds > 4 * 60 * 60) return { level: "bad", label: "STALE" };
  if (ageSeconds > 60 * 60) return { level: "warn", label: "STALE" };

  const rl = status.ingest_rate_limit;
  const count = rl && typeof rl.count_1h === "number" ? rl.count_1h : 0;
  if (count >= 30) return { level: "warn", label: "THROTTLED" };
  return { level: "good", label: "OK" };
}

function applyHealthBadge(status) {
  if (!liveIndicatorEl) return;
  liveIndicatorEl.classList.remove("health-good", "health-warn", "health-bad");
  const health = computeHealthLevel(status);
  liveIndicatorEl.classList.add(`health-${health.level}`);

  const mode = lastSseState?.mode || "polling";
  const prefix = mode === "connected" ? "LIVE" : mode === "reconnecting" ? "RETRY" : mode === "offline" ? "OFFLINE" : "POLL";
  liveIndicatorEl.textContent = `${prefix} ${health.label}`;
  liveIndicatorEl.title = `${prefix} ${health.label}`;
}

function formatCacheBadge(entry) {
  if (!entry || !entry.status) return "—";
  const ttl = entry.ttl != null && entry.ttl !== "" ? `${entry.ttl}s` : "";
  const ttlPart = ttl ? `(${ttl})` : "";
  return `${entry.status}${ttlPart}`;
}

function formatCacheDetail(kind, entry) {
  if (!entry || !entry.status) return `${kind}: —`;
  const ttl = entry.ttl != null && entry.ttl !== "" ? `${entry.ttl}s` : "—";
  const ageSeconds = entry.atMs ? Math.max(0, (Date.now() - entry.atMs) / 1000) : null;
  const age = ageSeconds != null ? formatAge(ageSeconds) : "—";
  return `${kind}: ${entry.status} ttl=${ttl} age=${age}`;
}

function updateCacheIndicator() {
  if (!cacheIndicatorEl) return;
  const h = formatCacheBadge(lastCacheInfo.hotspots);
  const r = formatCacheBadge(lastCacheInfo.rankings);
  const e = formatCacheBadge(lastCacheInfo.events);
  cacheIndicatorEl.textContent = `Cache · H:${h} R:${r} E:${e}`;
  cacheIndicatorEl.title = [
    formatCacheDetail("hotspots", lastCacheInfo.hotspots),
    formatCacheDetail("rankings", lastCacheInfo.rankings),
    formatCacheDetail("events", lastCacheInfo.events),
  ].join("\n");
}

function formatRateLimitSummary(summary) {
  if (!summary || typeof summary !== "object") return "—";
  const count = typeof summary.count_1h === "number" ? summary.count_1h : null;
  const avg = typeof summary.avg_retry_after_seconds_1h === "number" ? summary.avg_retry_after_seconds_1h : null;
  const interval = typeof summary.adaptive_min_interval_seconds === "number" ? summary.adaptive_min_interval_seconds : null;
  const parts = [];
  if (count != null) parts.push(`429_1h:${Math.round(count)}`);
  if (avg != null) parts.push(`avgRA:${avg.toFixed(1)}s`);
  if (interval != null) parts.push(`throttle:${interval.toFixed(1)}s`);
  return parts.length ? parts.join(" ") : "—";
}

function updateTopbarDetail(status) {
  if (!liveDetailEl) return;
  const last = status && status.observations_last_timestamp_utc ? parseIsoToMs(status.observations_last_timestamp_utc) : null;
  const failures = status && typeof status.ingest_consecutive_failures === "number" ? status.ingest_consecutive_failures : null;
  const backoff = status && typeof status.ingest_backoff_seconds === "number" ? status.ingest_backoff_seconds : null;
  const ok = status && typeof status.last_ingest_ok === "boolean" ? status.last_ingest_ok : null;

  if (last == null) {
    liveDetailEl.textContent = "Data: —";
    return;
  }

  const ageSeconds = Math.max(0, (Date.now() - last) / 1000);
  const bits = [`Data:${formatAge(ageSeconds)}`];
  if (ok === false) bits.push("ingest=ERR");
  else if (ok === true) bits.push("ingest=OK");
  if (failures != null && failures > 0) bits.push(`failures=${Math.round(failures)}`);
  if (backoff != null && backoff > 0) bits.push(`backoff=${Math.round(backoff)}s`);
  liveDetailEl.textContent = bits.join(" • ");
  liveDetailEl.title = bits.join(" • ");
}

function updateRateIndicator(status) {
  if (!rateIndicatorEl) return;
  const summary = status && status.ingest_rate_limit ? status.ingest_rate_limit : null;
  const text = formatRateLimitSummary(summary);
  rateIndicatorEl.textContent = `Rate: ${text}`;
  rateIndicatorEl.title = `Rate limit: ${text}`;
}

function formatTrendsSummary(summary) {
  if (!summary || typeof summary !== "object") return { text: "Trend: —", title: "" };
  const ok = typeof summary.vd_ok_total === "number" ? summary.vd_ok_total : 0;
  const err = typeof summary.vd_error_total === "number" ? summary.vd_error_total : 0;
  const rl = typeof summary.rate_limit_429_total === "number" ? summary.rate_limit_429_total : 0;
  const maxBackoff = typeof summary.max_backoff_seconds_24h === "number" ? summary.max_backoff_seconds_24h : 0;
  const maxFailures = typeof summary.max_consecutive_failures_24h === "number" ? summary.max_consecutive_failures_24h : 0;
  const text = `Trend 24h: ok=${ok} err=${err} 429=${rl} maxB=${Math.round(maxBackoff)}s maxF=${Math.round(maxFailures)}`;
  const codes = summary.error_codes && typeof summary.error_codes === "object" ? summary.error_codes : null;
  const topCodes = [];
  if (codes) {
    for (const [k, v] of Object.entries(codes).slice(0, 4)) topCodes.push(`${k}:${v}`);
  }
  const title = topCodes.length ? `Top error codes: ${topCodes.join(", ")}` : "";
  return { text: `Trend: ${text.replace("Trend 24h: ", "")}`, title };
}

function updateTrendIndicator(trends) {
  if (!trendIndicatorEl) return;
  const summary = trends && typeof trends === "object" ? trends.summary : null;
  const { text, title } = formatTrendsSummary(summary);
  trendIndicatorEl.textContent = text;
  if (title) trendIndicatorEl.title = title;
}

function formatPipelineSummary() {
  const status = dataHub.state.status;
  const trends = dataHub.state.trends;
  const alerts = dataHub.state.alerts;
  const diag = dataHub.state.diagnostics;

  const parts = [];
  if (status && status.observations_last_timestamp_utc) parts.push(`obs_last=${status.observations_last_timestamp_utc}`);
  if (status && status.last_ingest_ok != null) parts.push(`ingest_ok=${String(status.last_ingest_ok)}`);
  if (status && status.ingest_consecutive_failures != null) parts.push(`failures=${status.ingest_consecutive_failures}`);
  if (status && status.ingest_backoff_seconds != null) parts.push(`backoff=${status.ingest_backoff_seconds}s`);

  const sum = trends && trends.summary ? trends.summary : null;
  if (sum) {
    if (sum.vd_ok_total != null) parts.push(`vd_ok_24h=${sum.vd_ok_total}`);
    if (sum.vd_error_total != null) parts.push(`vd_err_24h=${sum.vd_error_total}`);
    if (sum.rate_limit_429_total != null) parts.push(`429_24h=${sum.rate_limit_429_total}`);
    if (sum.max_backoff_seconds_24h != null) parts.push(`max_backoff_24h=${sum.max_backoff_seconds_24h}s`);
  }

  if (diag && diag.event_hotspot_links && diag.event_hotspot_links.exists === false) parts.push("event_links=missing");

  const byCategory = alerts && alerts.summary && alerts.summary.by_category ? alerts.summary.by_category : null;
  if (byCategory) {
    parts.push(`alerts(network=${byCategory.network || 0}, rate_limit=${byCategory.rate_limit || 0}, data=${byCategory.data || 0})`);
  }
  return parts.length ? parts.join(" • ") : "—";
}

function renderPipelineTrendsChart() {
  if (!pipelineTrendsChartEl) return;
  if (typeof Plotly === "undefined") {
    pipelineTrendsChartEl.textContent = "Plotly not available.";
    return;
  }

  const trends = dataHub.state.trends;
  const buckets = trends && Array.isArray(trends.buckets) ? trends.buckets : [];
  if (!buckets.length) {
    pipelineTrendsChartEl.textContent = "No trend data.";
    return;
  }

  const x = buckets.map((b) => b.hour_start_utc);
  const ok = buckets.map((b) => Number(b.vd_ok || 0));
  const err = buckets.map((b) => Number(b.vd_error || 0));
  const rl = buckets.map((b) => Number(b.rate_limit_429 || 0));

  const colors = themeColors();
  const data = [
    { x, y: ok, type: "scatter", mode: "lines", name: "VD ok", line: { color: colors.accentHex } },
    { x, y: err, type: "scatter", mode: "lines", name: "VD error", line: { color: colors.dangerHex } },
    { x, y: rl, type: "scatter", mode: "lines", name: "429", line: { color: colors.warnHex } },
  ];
  const layout = {
    margin: { l: 34, r: 10, t: 10, b: 28 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    xaxis: { showgrid: false, tickfont: { size: 10 } },
    yaxis: { showgrid: true, gridcolor: "rgba(15,23,42,0.06)", tickfont: { size: 10 } },
    legend: { orientation: "h", y: -0.25, x: 0 },
    font: { size: 11, color: colors.text },
  };
  Plotly.react(pipelineTrendsChartEl, data, layout, { displayModeBar: false, responsive: true });
}

function renderPipelineAlerts() {
  if (!pipelineAlertsEl) return;
  const alerts = dataHub.state.alerts;
  const lines = alerts && Array.isArray(alerts.lines) ? alerts.lines : [];
  pipelineAlertsEl.textContent = lines.slice(-10).join("\n") || "No alerts.";
}

function renderPipelinePanel() {
  if (pipelineSummaryEl) pipelineSummaryEl.textContent = formatPipelineSummary();
  renderPipelineTrendsChart();
  renderPipelineAlerts();
}

async function refreshTrends() {
  try {
    const data = await dataHub.refreshTrends();
    lastTrends = data;
    updateTrendIndicator(data);
  } catch (err) {
    // Keep UI stable; trends are best-effort.
  }
}

function formatWeatherRow(row) {
  if (!row || typeof row !== "object") return "—";
  const city = row.city ? String(row.city) : "—";
  const rain = row.rain_mm != null && Number.isFinite(Number(row.rain_mm)) ? `${Number(row.rain_mm).toFixed(1)}mm` : "—";
  const wind = row.wind_mps != null && Number.isFinite(Number(row.wind_mps)) ? `${Number(row.wind_mps).toFixed(1)}m/s` : "—";
  const temp = row.temperature_c != null && Number.isFinite(Number(row.temperature_c)) ? `${Number(row.temperature_c).toFixed(1)}°C` : "—";
  return `${city} rain:${rain} wind:${wind} temp:${temp}`;
}

async function refreshWeatherLatest() {
  if (!weatherIndicatorEl) return;
  try {
    const data = await dataHub.refreshWeather();
    const items = data && Array.isArray(data.items) ? data.items : [];
    const row = items.length ? items[0] : null;
    weatherIndicatorEl.textContent = `Weather: ${formatWeatherRow(row)}`;
    if (row && row.timestamp) weatherIndicatorEl.title = `Weather timestamp: ${row.timestamp}`;
  } catch (err) {
    // best-effort
  }
}

dataHub.onUpdate((state, kind) => {
  if (kind === "trends") {
    lastTrends = state.trends;
    updateTrendIndicator(state.trends);
  }
  if (kind === "weather") {
    const items = state.weather && Array.isArray(state.weather.items) ? state.weather.items : [];
    const row = items.length ? items[0] : null;
    if (weatherIndicatorEl) {
      weatherIndicatorEl.textContent = `Weather: ${formatWeatherRow(row)}`;
      if (row && row.timestamp) weatherIndicatorEl.title = `Weather timestamp: ${row.timestamp}`;
    }
  }
  if (["trends", "alerts", "diagnostics", "status"].includes(kind)) {
    renderPipelinePanel();
  }
  if (["diagnostics", "status"].includes(kind)) {
    renderNextStepCard();
  }
  if (["trends", "diagnostics", "status"].includes(kind)) {
    renderOverview();
  }
  if (["trends", "alerts", "diagnostics", "status"].includes(kind)) {
    renderStory();
  }
});

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

function storageKey(kind) {
  return `trafficpulse.cache.${kind}`;
}

function saveCached(kind, payload) {
  try {
    localStorage.setItem(storageKey(kind), JSON.stringify(payload));
  } catch (err) {
    // ignore
  }
}

function loadCached(kind) {
  try {
    const raw = localStorage.getItem(storageKey(kind));
    if (!raw) return null;
    return JSON.parse(raw);
  } catch (err) {
    return null;
  }
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
  dataHub.set("status", status);
  const last = status.observations_last_timestamp_utc;
  const minutes = status.observations_minutes_available || [];
  latestObservationIso = last || null;
  latestObservationMinutes = Array.isArray(minutes) ? minutes : [];
  latestObservationMs = last ? parseIsoToMs(last) : null;
  updateFreshnessDisplay();
  updateTopbarDetail(status);
  updateRateIndicator(status);
  applyHealthBadge(status);
  renderPipelinePanel();
  renderNextStepCard();

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
    const status = await dataHub.refreshStatus();
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
  setLayerVisible(markers, Boolean(showSegments));
  setLayerVisible(hotspotsLayer, Boolean(showHotspots));
  setLayerVisible(eventMarkers, Boolean(showEvents));
  setLayerVisible(impactSegmentsLayer, Boolean(showImpact));
  setLayerVisible(linkedHotspotsLayer, Boolean(showEvents));
  // Halos follow selection; keep visible only when there is a selection.
  setLayerVisible(selectionHaloLayer, Boolean(showSegments || showEvents));
}

function setNextStepCard({ title, text, actions } = {}) {
  if (!mapNextStepEl || !mapNextStepTitleEl || !mapNextStepTextEl || !mapNextStepActionsEl) return;
  if (nextStepDismissed) {
    mapNextStepEl.classList.add("hidden");
    return;
  }

  mapNextStepEl.classList.remove("hidden");
  mapNextStepTitleEl.textContent = title || "Next step";
  mapNextStepTextEl.textContent = text || "";

  mapNextStepActionsEl.innerHTML = "";
  for (const action of actions || []) {
    if (!action || !action.label || typeof action.onClick !== "function") continue;
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = action.primary ? "button" : "button secondary";
    btn.textContent = String(action.label);
    btn.addEventListener("click", () => {
      try {
        action.onClick();
      } catch (err) {
        // ignore
      }
    });
    mapNextStepActionsEl.appendChild(btn);
  }
}

function renderNextStepCard() {
  if (!mapNextStepEl) return;
  if (nextStepDismissed) {
    mapNextStepEl.classList.add("hidden");
    return;
  }

  const diag = dataHub.state.diagnostics;
  const entityType = entityTypeEl && entityTypeEl.value ? String(entityTypeEl.value) : "segment";

  if (!segments.length) {
    setNextStepCard({
      title: "Loading",
      text: "Loading segments…",
      actions: [],
    });
    return;
  }

  if (entityType === "corridor") {
    if (!selectedCorridorId) {
      setNextStepCard({
        title: "Select a corridor",
        text: "Pick a corridor from the dropdown, then load its time series.",
        actions: [
          { label: "Focus corridor select", onClick: () => corridorSelectEl?.focus?.(), primary: true },
          { label: "Load time series", onClick: () => loadTimeseries(), primary: false },
        ],
      });
      return;
    }
  } else if (!selectedSegmentId) {
    setNextStepCard({
      title: "Select a segment",
      text: "Click a marker on the map, or pick a segment from the dropdown.",
      actions: [
        { label: "Focus segment select", onClick: () => segmentSelectEl?.focus?.(), primary: true },
        { label: "Focus map", onClick: () => map.getContainer()?.focus?.(), primary: false },
      ],
    });
    return;
  }

  const hasTimeseries =
    lastTimeseries &&
    lastTimeseries.entity &&
    lastTimeseries.entity.id &&
    Array.isArray(lastTimeseries.points) &&
    lastTimeseries.points.length > 0;
  if (!hasTimeseries) {
    setNextStepCard({
      title: "Load a time series",
      text: "You have a selection. Load its time series for the current time window.",
      actions: [
        { label: "Load time series", onClick: () => loadTimeseries(), primary: true },
        { label: "Use 24h window", onClick: () => quickUse24hWindow(), primary: false },
      ],
    });
    return;
  }

  if (!hotspotsLoaded) {
    setNextStepCard({
      title: "Load hotspots",
      text: "Color the map by speed / congestion / drop vs baseline. If you see an empty result, widen the time window or lower Min samples.",
      actions: [
        { label: "Load hotspots", onClick: () => loadHotspots(), primary: true },
        { label: "Min samples = 1", onClick: () => quickSetMinSamplesOne(), primary: false },
        { label: "Use 24h window", onClick: () => quickUse24hWindow(), primary: false },
      ],
    });
    return;
  }

  if (showEvents && !eventsLoaded) {
    const eventsExists = Boolean(diag?.events_csv?.exists);
    const hint = !eventsExists
      ? "Events are not available yet (events.csv is missing). This is often caused by upstream rate limits."
      : "Events exist, but they are not loaded yet for the current map window.";
    setNextStepCard({
      title: "Load events (optional)",
      text: `${hint} You can wait for the hourly timer, or run the events service manually.`,
      actions: [
        { label: "Load events", onClick: () => loadEvents(), primary: true },
        { label: "Copy events fix", onClick: () => copyText(EVENTS_FIX_COMMAND), primary: false },
      ],
    });
    return;
  }

  setNextStepCard({
    title: "Explore",
    text: "Tip: toggle Spotlight to focus on the selected entity. Press “?” for keyboard shortcuts.",
    actions: [
      { label: "Focus selected", onClick: () => focusSelected(), primary: true },
      { label: "Reload hotspots", onClick: () => loadHotspots(), primary: false },
    ],
  });
}

function setHintVisible(el, visible) {
  if (!el) return;
  if (visible) el.classList.remove("hidden");
  else el.classList.add("hidden");
}

function updateSelectionHalos() {
  selectionHaloLayer.clearLayers();
  segmentSelectionHalo = null;
  eventSelectionHalo = null;

  const colors = themeColors();

  const segId = selectedSegmentId != null ? String(selectedSegmentId) : null;
  const evId = selectedEventId != null ? String(selectedEventId) : null;

  if (segId && markerById.has(segId)) {
    const marker = markerById.get(segId);
    const ll = marker?.getLatLng?.();
    if (ll) {
      segmentSelectionHalo = L.circleMarker(ll, {
        pane: "haloPane",
        radius: 18,
        color: colors.accent2Hex,
        weight: 3,
        fillColor: rgba(colors.accent2Rgb, 0.22),
        fillOpacity: 0.32,
        opacity: 0.95,
        interactive: false,
        className: "selection-halo",
      }).addTo(selectionHaloLayer);
    }
  }

  if (evId && eventMarkerById.has(evId)) {
    const marker = eventMarkerById.get(evId);
    const ll = marker?.getLatLng?.();
    if (ll) {
      eventSelectionHalo = L.circleMarker(ll, {
        pane: "haloPane",
        radius: 18,
        color: colors.dangerHex,
        weight: 3,
        fillColor: rgba(colors.dangerRgb, 0.18),
        fillOpacity: 0.32,
        opacity: 0.95,
        interactive: false,
        className: "selection-halo",
      }).addTo(selectionHaloLayer);
    }
  }
}

function applySpotlightStyles() {
  const on = Boolean(spotlightMode);
  const selectedSeg = selectedSegmentId ? String(selectedSegmentId) : null;
  const selectedEv = selectedEventId ? String(selectedEventId) : null;

  const dim = { opacity: 0.18, fillOpacity: 0.08 };
  const normalSeg = { opacity: 1, fillOpacity: 0.6 };
  const normalHotspot = { opacity: 1, fillOpacity: 0.78 };
  const normalEvent = { opacity: 1, fillOpacity: 0.85 };

  for (const [segId, marker] of markerById.entries()) {
    if (!marker || typeof marker.setStyle !== "function") continue;
    const isSelected = selectedSeg != null && String(segId) === selectedSeg;
    if (!on || isSelected || !selectedSeg) marker.setStyle(normalSeg);
    else marker.setStyle(dim);
  }

  for (const [segId, marker] of hotspotMarkerBySegmentId.entries()) {
    if (!marker || typeof marker.setStyle !== "function") continue;
    const isSelected = selectedSeg != null && String(segId) === selectedSeg;
    if (!on || isSelected || !selectedSeg) marker.setStyle(normalHotspot);
    else marker.setStyle(dim);
  }

  for (const [eventId, marker] of eventMarkerById.entries()) {
    if (!marker || typeof marker.setStyle !== "function") continue;
    const isSelected = selectedEv != null && String(eventId) === selectedEv;
    if (!on || isSelected || !selectedEv) marker.setStyle(normalEvent);
    else marker.setStyle(dim);
  }
}

function setEventsHint({ title, text, showRelaxFilters, showCopyFix } = {}) {
  if (eventsHintTitleEl) eventsHintTitleEl.textContent = title || "Notice";
  if (eventsHintTextEl) eventsHintTextEl.textContent = text || "";
  if (eventsRelaxFiltersButton) eventsRelaxFiltersButton.style.display = showRelaxFilters ? "" : "none";
  if (eventsCopyFixButton) eventsCopyFixButton.style.display = showCopyFix ? "" : "none";
  setHintVisible(eventsHintEl, Boolean(text));
}

function isLikelyDatasetMissing(reason) {
  if (!reason || typeof reason !== "object") return false;
  const code = String(reason.code || "").toLowerCase();
  const msg = String(reason.message || "").toLowerCase();
  if (!code && !msg) return false;
  if (code.includes("dataset_missing")) return true;
  if (code.includes("file_missing")) return true;
  if (code.includes("missing") && msg.includes("events")) return true;
  if (msg.includes("events.csv") && (msg.includes("missing") || msg.includes("not found"))) return true;
  return false;
}

function setChartHint(opts) {
  const { title, text, actions } = opts || {};
  if (!chartHintEl) return;
  if (!text) {
    setHintVisible(chartHintEl, false);
    return;
  }
  if (chartHintTitleEl) chartHintTitleEl.textContent = title || "Notice";
  if (chartHintTextEl) chartHintTextEl.textContent = text;
  if (chartHintActionsEl) {
    chartHintActionsEl.innerHTML = "";
    for (const action of actions || []) {
      if (!action || !action.label) continue;
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "button secondary";
      btn.textContent = String(action.label);
      btn.addEventListener("click", () => {
        try {
          action.onClick?.();
        } catch (err) {
          // ignore
        }
      });
      chartHintActionsEl.appendChild(btn);
    }
  }
  setHintVisible(chartHintEl, true);
}

function renderEmptyState(containerEl, { title, text, actions } = {}) {
  if (!containerEl) return;
  containerEl.innerHTML = "";

  const wrap = document.createElement("div");
  wrap.className = "empty-state";

  const titleEl = document.createElement("div");
  titleEl.className = "empty-title";
  titleEl.textContent = title || "Nothing to show yet";

  const textEl = document.createElement("div");
  textEl.className = "empty-text";
  textEl.textContent = text || "";

  wrap.appendChild(titleEl);
  wrap.appendChild(textEl);

  const btnRow = document.createElement("div");
  btnRow.className = "button-row";
  let didAddAction = false;
  for (const [i, action] of (actions || []).entries()) {
    if (!action || !action.label || typeof action.onClick !== "function") continue;
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = i === 0 ? "button" : "button secondary";
    btn.textContent = String(action.label);
    btn.addEventListener("click", () => {
      try {
        action.onClick();
      } catch (err) {
        // ignore
      }
    });
    btnRow.appendChild(btn);
    didAddAction = true;
  }
  if (didAddAction) wrap.appendChild(btnRow);

  containerEl.appendChild(wrap);
}

async function copyText(text) {
  const value = String(text || "");
  if (!value) return;
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(value);
      return true;
    }
  } catch (err) {
    // ignore
  }
  try {
    window.prompt("Copy this text:", value);
    return true;
  } catch (err) {
    return false;
  }
}

function formatFileInfo(info) {
  if (!info) return "missing";
  if (!info.exists) return "missing";
  const size = info.size_bytes != null ? `${info.size_bytes}B` : "—";
  const mtime = info.mtime_utc ? String(info.mtime_utc) : "—";
  return `ok (mtime=${mtime}, size=${size})`;
}

function countOkFiles(list) {
  if (!Array.isArray(list)) return 0;
  return list.filter((x) => x && x.exists).length;
}

function buildFixCommands({ hasSegments, hasObservations, hasEvents, hasCorridors } = {}) {
  const commands = [];
  if (!hasSegments || !hasObservations) {
    commands.push("python scripts/ingest_runner.py --live-only --live-max-iterations 2 --no-cache");
    commands.push("python scripts/build_dataset.py");
    commands.push("python scripts/aggregate_observations.py");
  }
  if (hasCorridors && (!hasObservations || !hasSegments)) {
    commands.push("python scripts/build_corridor_rankings.py --limit 200");
  }
  if (!hasEvents) {
    commands.push("systemctl --user start trafficpulse-events.service");
    commands.push("journalctl --user -u trafficpulse-events.service -n 120 --no-pager");
  }
  commands.push("python scripts/materialize_defaults.py --window-hours 24");
  return commands;
}

async function refreshDataHealth() {
  if (!dataHealthTextEl) return;
  dataHealthTextEl.textContent = "Loading...";
  setHintVisible(dataHealthHintEl, false);

  let status = null;
  let diagnostics = null;
  try {
    status = await dataHub.refreshStatus();
  } catch (err) {
    // ignore
  }
  try {
    diagnostics = await dataHub.refreshDiagnostics();
  } catch (err) {
    // ignore
  }

  const lines = [];
  lines.push(`API_BASE: ${API_BASE}`);
  lines.push(`Exports (current window):`);
  lines.push(`- ${buildExportUrl("/exports/reliability/segments.csv")}`);
  lines.push(`- ${buildExportUrl("/exports/reliability/corridors.csv")}`);
  if (status) {
    lines.push(`Status generated_at_utc: ${status.generated_at_utc || "—"}`);
    lines.push(`observations_last_timestamp_utc: ${status.observations_last_timestamp_utc || "—"}`);
    lines.push(`observations_minutes_available: ${(status.observations_minutes_available || []).join(", ") || "—"}`);
    lines.push(`dataset_version: ${status.dataset_version || "—"}`);
    lines.push(`live_loop_last_snapshot_timestamp: ${status.live_loop_last_snapshot_timestamp || "—"}`);
    lines.push(`daily_backfill_last_date: ${status.daily_backfill_last_date || "—"}`);
    lines.push(`last_ingest_ok: ${status.last_ingest_ok == null ? "—" : String(status.last_ingest_ok)}`);
    lines.push(`last_error: ${status.last_error || "—"}`);
    if (status.last_error_code) lines.push(`last_error_code: ${status.last_error_code}`);
    if (status.last_error_kind) lines.push(`last_error_kind: ${status.last_error_kind}`);
    if (status.ingest_consecutive_failures != null) lines.push(`consecutive_failures: ${status.ingest_consecutive_failures}`);
    if (status.ingest_backoff_seconds != null) lines.push(`backoff_seconds: ${status.ingest_backoff_seconds}`);
    if (status.ingest_last_success_utc) lines.push(`last_success_utc: ${status.ingest_last_success_utc}`);
    if (status.ingest_rate_limit) lines.push(`rate_limit: ${formatRateLimitSummary(status.ingest_rate_limit)}`);
    if (Array.isArray(status.updated_files) && status.updated_files.length) {
      lines.push(`updated_files: ${status.updated_files.join(", ")}`);
    }
  } else {
    lines.push("Status: unavailable (failed to fetch /ui/status)");
  }

  let hasSegments = false;
  let hasObservations = false;
  let hasEvents = false;
  let hasCorridors = false;
  if (diagnostics) {
    lines.push("");
    lines.push("Diagnostics:");
    lines.push(`processed_dir: ${diagnostics.processed_dir || "—"}`);
    lines.push(`parquet_dir: ${diagnostics.parquet_dir || "—"}`);
    lines.push(`segments.csv: ${formatFileInfo(diagnostics.segments_csv)}`);
    hasSegments = Boolean(diagnostics.segments_csv && diagnostics.segments_csv.exists);
    lines.push(`events.csv: ${formatFileInfo(diagnostics.events_csv)}`);
    hasEvents = Boolean(diagnostics.events_csv && diagnostics.events_csv.exists);
    lines.push(`weather_observations.csv: ${formatFileInfo(diagnostics.weather_csv)}`);
    lines.push(`corridors.csv: ${diagnostics.corridors_csv_exists ? "ok" : "missing"} (${diagnostics.corridors_csv || "—"})`);
    hasCorridors = Boolean(diagnostics.corridors_csv_exists);
    const obsFiles = diagnostics.observations_csv_files || [];
    hasObservations = Array.isArray(obsFiles) && obsFiles.some((f) => f && f.exists);
    lines.push(`observations CSV files: ${Array.isArray(obsFiles) ? obsFiles.length : 0} (${hasObservations ? "ok" : "missing"})`);
    if (Array.isArray(obsFiles) && obsFiles.length) {
      for (const f of obsFiles.slice(0, 8)) {
        lines.push(`- ${f.path}: ${formatFileInfo(f)}`);
      }
      if (obsFiles.length > 8) lines.push(`- … (${obsFiles.length - 8} more)`);
    }
    lines.push(`live_loop_state.json: ${formatFileInfo(diagnostics.live_loop_state)}`);
    lines.push(`backfill_checkpoint.json: ${formatFileInfo(diagnostics.backfill_checkpoint)}`);

    lines.push("");
    lines.push("Processing outputs:");
    lines.push(`materialized_defaults.json: ${formatFileInfo(diagnostics.materialized_defaults)}`);
    lines.push(`baselines_speed_*.csv: ${countOkFiles(diagnostics.baselines_speed_files)} file(s)`);
    lines.push(`segment_quality_*.csv: ${countOkFiles(diagnostics.segment_quality_files)} file(s)`);
    lines.push(`congestion_alerts.csv: ${formatFileInfo(diagnostics.congestion_alerts)}`);
    lines.push(`event_hotspot_links.csv: ${formatFileInfo(diagnostics.event_hotspot_links)}`);
  } else {
    lines.push("");
    lines.push("Diagnostics: unavailable (failed to fetch /ui/diagnostics)");
  }

  dataHealthTextEl.textContent = lines.join("\n");

  const suggestions = [];
  if (!hasSegments) suggestions.push("Missing segments.csv (run ingestion + build_dataset).");
  if (!hasObservations) suggestions.push("Missing observations CSV (run ingestion + build_dataset).");
  if (!hasEvents) suggestions.push("Missing events.csv (run trafficpulse-events.service).");
  if (!hasCorridors) suggestions.push("corridors.csv missing (optional; needed for corridor rankings).");
  if (status && status.last_ingest_ok === false) suggestions.push(`Ingestion error: ${status.last_error || "unknown"}`);

  if (suggestions.length && dataHealthHintTextEl) {
    dataHealthHintTextEl.textContent = suggestions.join(" ");
    setHintVisible(dataHealthHintEl, true);
  } else {
    setHintVisible(dataHealthHintEl, false);
  }

  return { hasSegments, hasObservations, hasEvents, hasCorridors };
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
    renderHotspots(
      hotspotRows,
      hotspotMetricEl ? hotspotMetricEl.value : "mean_speed_kph",
      hotspotColorModeEl?.value || "metric"
    );
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
  await copyText(url);
  setStatus("Link copied to clipboard.");
}

function downloadJson(filename, payload) {
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function exportSnapshot() {
  syncUrlFromUi();
  const payload = {
    exported_at_utc: new Date().toISOString(),
    url: window.location.href,
    api_base: API_BASE,
    ui_state: uiState,
    ui_status: lastUiStatus,
    cache: lastCacheInfo,
    selection: {
      entity_type: entityTypeEl?.value || null,
      segment_id: selectedSegmentId,
      corridor_id: selectedCorridorId,
      event_id: selectedEventId,
    },
    data: {
      hotspots: hotspotRows,
      rankings: lastRankings,
      events,
      timeseries: lastTimeseries,
      event_impact: lastEventImpact,
    },
  };
  downloadJson(`trafficpulse_snapshot_${new Date().toISOString().replace(/[:.]/g, "-")}.json`, payload);
  setStatus("Exported JSON snapshot.");
}

function buildExportUrl(path) {
  const url = new URL(`${API_BASE}${path}`);
  const range = getIsoRange();
  if (range) {
    url.searchParams.set("start", range.start);
    url.searchParams.set("end", range.end);
  }
  if (minutesEl && minutesEl.value) url.searchParams.set("minutes", String(minutesEl.value));
  const overrides = getNested(uiState, "overrides.reliability", null);
  if (overrides) {
    if (overrides.congestion_speed_threshold_kph != null)
      url.searchParams.set("congestion_speed_threshold_kph", String(overrides.congestion_speed_threshold_kph));
    if (overrides.min_samples != null) url.searchParams.set("min_samples", String(overrides.min_samples));
    if (overrides.weight_mean_speed != null) url.searchParams.set("weight_mean_speed", String(overrides.weight_mean_speed));
    if (overrides.weight_speed_std != null) url.searchParams.set("weight_speed_std", String(overrides.weight_speed_std));
    if (overrides.weight_congestion_frequency != null)
      url.searchParams.set("weight_congestion_frequency", String(overrides.weight_congestion_frequency));
  }
  return url.toString();
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

  const allowEventsLiveReload = Boolean(eventsAutoEl && eventsAutoEl.checked);
  if (eventsLoaded && showEvents && allowEventsLiveReload) {
    await loadEvents();
  }

  if (!silent) setStatus("Live refresh complete.");
}

function initLivePanel() {
  if (!liveAutoRefreshEl || !liveIntervalSecondsEl || !liveRefreshEl) return;

  applyLiveStateToForm();
  refreshUiStatus();
  dataHub.start({ includeStatus: false });
  initPageNav();
  initOverviewActions();
  initTimeseriesControls();
  if (API_OVERRIDE_INFO.ignored && API_OVERRIDE_INFO.reason) {
    setStatus(API_OVERRIDE_INFO.reason);
  }
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
    min_coverage_pct: parseNumberValue(reliabilityMinCoverageEl?.value),
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
  if (o.min_coverage_pct != null) url.searchParams.set("min_coverage_pct", String(o.min_coverage_pct));
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
    setInputValue(reliabilityMinCoverageEl, rel.min_coverage_pct);
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
  syncMapToolbarFromState();
}

function persistOverlays() {
  uiState.overlays = uiState.overlays || {};
  uiState.overlays.anomalies = Boolean(showAnomalies);
  uiState.overlays.segments = Boolean(showSegments);
  uiState.overlays.hotspots = Boolean(showHotspots);
  uiState.overlays.events = Boolean(showEvents);
  uiState.overlays.impact = Boolean(showImpact);
  uiState.map = uiState.map || {};
  uiState.map.spotlight = Boolean(spotlightMode);
  uiState.map.nextStepDismissed = Boolean(nextStepDismissed);
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
      syncMapToolbarFromState();
      renderNextStepCard();
    });
  }

  if (toggleEventsEl) {
    toggleEventsEl.checked = Boolean(showEvents);
    toggleEventsEl.addEventListener("change", () => {
      showEvents = Boolean(toggleEventsEl.checked);
      persistOverlays();
      applyOverlayVisibility();
      syncMapToolbarFromState();
      renderNextStepCard();
    });
  }

  if (toggleImpactEl) {
    toggleImpactEl.checked = Boolean(showImpact);
    toggleImpactEl.addEventListener("change", () => {
      showImpact = Boolean(toggleImpactEl.checked);
      persistOverlays();
      applyOverlayVisibility();
      syncMapToolbarFromState();
      renderNextStepCard();
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

function clearSelection() {
  selectedSegmentId = null;
  selectedCorridorId = null;
  selectedEventId = null;

  if (entityTypeEl) entityTypeEl.value = "segment";
  if (segmentSelectEl) segmentSelectEl.value = "";
  if (corridorSelectEl) corridorSelectEl.value = "";

  updateSegmentInfo(null);
  updateCorridorInfo(null);

  impactSegmentsLayer.clearLayers();
  linkedHotspotsLayer.clearLayers();
  lastEventImpact = null;
  lastEventLinksInfo = null;
  lastEventsClearedReason = null;
  updateEventInfo(null, null, null);

  updateSelectionStyles();
  scheduleUrlSync();
  renderNextStepCard();
}

function syncMapToolbarFromState() {
  if (mapToggleSegmentsEl) mapToggleSegmentsEl.checked = Boolean(showSegments);
  if (mapToggleHotspotsEl) mapToggleHotspotsEl.checked = Boolean(showHotspots);
  if (mapToggleEventsEl) mapToggleEventsEl.checked = Boolean(showEvents);
  if (mapToggleImpactEl) mapToggleImpactEl.checked = Boolean(showImpact);
  if (mapSpotlightEl) mapSpotlightEl.checked = Boolean(spotlightMode);
}

function initPageNav() {
  const docPage = getPageFromDocument();
  const urlPage = urlOverrides && urlOverrides.page ? urlOverrides.page : null;
  const storedPage = getNested(uiState, "layout.page", null);
  const initial = normalizePage(docPage || urlPage || storedPage || "explore");
  setPage(initial, { syncUrl: false });

  if (!topnavEl) return;
  for (const btn of Array.from(topnavEl.querySelectorAll("[data-page]"))) {
    // Dedicated pages use <a href="/{page}/"> navigation. Keep SPA button support
    // for backwards compatibility on the legacy root page.
    if (btn.tagName !== "BUTTON") continue;
    btn.addEventListener("click", () => {
      const p = btn.getAttribute("data-page");
      setPage(p || "explore");
      renderOverview();
      renderTimeseriesInsights();
    });
  }
}

function renderOverview() {
  if (!overviewEl) return;

  const status = dataHub.state.status;
  const trends = dataHub.state.trends;
  const diag = dataHub.state.diagnostics;

  const lastMs = status?.observations_last_timestamp_utc ? parseIsoToMs(status.observations_last_timestamp_utc) : null;
  const age = lastMs != null ? formatAge(Math.max(0, (Date.now() - lastMs) / 1000)) : "—";
  const rl = status?.ingest_rate_limit ? formatRateLimitSummary(status.ingest_rate_limit) : "—";
  const ok = status?.last_ingest_ok;
  const okText = ok === true ? "ingestion OK" : ok === false ? "ingestion error" : "ingestion unknown";
  const hotCount = hotspotsLoaded ? hotspotRows.length : 0;
  const rankCount = rankingsLoaded ? lastRankings.length : 0;
  const evCount = eventsLoaded ? events.length : 0;

  const trendSummary = trends?.summary ? formatTrendsSummary(trends.summary).text.replace("Trend: ", "") : "—";
  const eventsExists = Boolean(diag?.events_csv?.exists);

  if (overviewSentenceEl) {
    overviewSentenceEl.textContent = `Data age: ${age} • ${okText} • Rate: ${rl} • Trend: ${trendSummary}`;
  }

  if (overviewHotspotsMetricEl) overviewHotspotsMetricEl.textContent = hotspotsLoaded ? `loaded=${hotCount}` : "not loaded";
  if (overviewHotspotsTextEl)
    overviewHotspotsTextEl.textContent = hotspotsLoaded
      ? "Hotspots are ready. Switch to Explore to inspect the map."
      : "Load hotspots to color the map by speed / congestion / baseline drop.";

  if (overviewRankingsMetricEl) overviewRankingsMetricEl.textContent = rankingsLoaded ? `loaded=${rankCount}` : "not loaded";
  if (overviewRankingsTextEl)
    overviewRankingsTextEl.textContent = rankingsLoaded
      ? "Rankings are ready. Click a row to jump to a segment/corridor."
      : "Load rankings to find low-reliability segments/corridors.";

  if (overviewEventsMetricEl) overviewEventsMetricEl.textContent = eventsLoaded ? `loaded=${evCount}` : eventsExists ? "available" : "missing";
  if (overviewEventsTextEl)
    overviewEventsTextEl.textContent = eventsExists
      ? "Load events to see markers and impact overlays."
      : "Events dataset is not available yet (likely upstream rate-limited).";
}

function setCardValue(cardEl, valueText) {
  if (!cardEl) return;
  const el = cardEl.querySelector(".kpi-value");
  if (!el) return;
  el.textContent = valueText;
}

function setStoryButton(btnEl, { label, onClick, hidden } = {}) {
  if (!btnEl) return;
  const hide = Boolean(hidden) || !label || typeof onClick !== "function";
  btnEl.classList.toggle("hidden", hide);
  if (hide) {
    btnEl.onclick = null;
    btnEl.textContent = "—";
    return;
  }
  btnEl.textContent = String(label);
  btnEl.onclick = (ev) => {
    ev.preventDefault();
    try {
      onClick();
    } catch (err) {
      // ignore
    }
  };
}

function shortDatasetVersion(version) {
  const v = String(version || "");
  if (!v) return "—";
  if (v.length <= 26) return v;
  return `${v.slice(0, 10)}…${v.slice(-12)}`;
}

function setStoryCallout(kind) {
  if (!storyNarrativeEl) return;
  storyNarrativeEl.classList.remove("warn", "danger");
  if (kind === "warn") storyNarrativeEl.classList.add("warn");
  if (kind === "danger") storyNarrativeEl.classList.add("danger");
}

function setStoryNext(items) {
  if (!storyNextListEl) return;
  storyNextListEl.textContent = "";
  for (const text of Array.isArray(items) ? items : []) {
    const li = document.createElement("li");
    li.textContent = String(text);
    storyNextListEl.appendChild(li);
  }
}

function renderStory() {
  if (!storyTitleEl || !storySubtitleEl || !storyNarrativeEl || !storyEl) return;

  if (currentPage === "overview") {
    storyEl.classList.add("hidden");
    return;
  }
  storyEl.classList.remove("hidden");

  const status = dataHub.state.status;
  const trends = dataHub.state.trends;
  const diag = dataHub.state.diagnostics;
  const alerts = dataHub.state.alerts;

  const lastMs = status?.observations_last_timestamp_utc ? parseIsoToMs(status.observations_last_timestamp_utc) : null;
  const ageText = lastMs != null ? formatAge(Math.max(0, (Date.now() - lastMs) / 1000)) : "—";
  const ok = status?.last_ingest_ok;
  const failures = typeof status?.ingest_consecutive_failures === "number" ? status.ingest_consecutive_failures : 0;
  const backoff = typeof status?.ingest_backoff_seconds === "number" ? status.ingest_backoff_seconds : 0;
  const rlCount = typeof status?.ingest_rate_limit?.count_1h === "number" ? status.ingest_rate_limit.count_1h : null;

  setCardValue(kpiDataAgeCardEl, ageText);
  setCardValue(
    kpiIngestCardEl,
    ok === true ? (failures > 0 ? `OK (failures=${failures})` : "OK") : ok === false ? "ERROR" : "—"
  );
  setCardValue(kpi429CardEl, rlCount != null ? String(Math.round(rlCount)) : "—");
  setCardValue(kpiDatasetCardEl, shortDatasetVersion(status?.dataset_version));

  const storyBits = [];
  storyBits.push(`Data age: ${ageText}.`);
  if (ok === true) storyBits.push("Ingestion is healthy.");
  else if (ok === false) storyBits.push("Ingestion is reporting errors.");
  if (failures > 0) storyBits.push(`Consecutive failures: ${failures}.`);
  if (backoff > 0) storyBits.push(`Backoff: ${Math.round(backoff)}s.`);
  if (rlCount != null && rlCount > 0) storyBits.push(`429 (last 1h): ${Math.round(rlCount)}.`);

  const trendSummary = trends?.summary || null;
  if (trendSummary && typeof trendSummary.max_backoff_seconds_24h === "number") {
    storyBits.push(`Max backoff (24h): ${Math.round(trendSummary.max_backoff_seconds_24h)}s.`);
  }

  let calloutKind = null;
  if (ok === false) calloutKind = "danger";
  else if (failures > 0 || (rlCount != null && rlCount >= 10)) calloutKind = "warn";
  setStoryCallout(calloutKind);

  const page = currentPage;
  if (page === "explore") {
    storyTitleEl.textContent = "Explore congestion hotspots on the map";
    storySubtitleEl.textContent = selectedSegmentId
      ? `Selected segment: ${String(selectedSegmentId)}. Use Hotspots to color the network, then load a time series.`
      : "Pan/zoom, load hotspots for the current bounds, then click a segment to inspect time series.";

    if (kpiPage1LabelEl) kpiPage1LabelEl.textContent = "Hotspots";
    if (kpiPage1ValueEl) kpiPage1ValueEl.textContent = hotspotsLoaded ? `loaded=${hotspotRows.length}` : "not loaded";
    if (kpiPage2LabelEl) kpiPage2LabelEl.textContent = "Events";
    if (kpiPage2ValueEl) kpiPage2ValueEl.textContent = eventsLoaded ? `loaded=${events.length}` : "not loaded";

    if (exploreStoryCollapsedSummaryEl) {
      const parts = [];
      parts.push(hotspotsLoaded ? `hotspots=${hotspotRows.length}` : "hotspots=—");
      if (selectedSegmentId) parts.push(`seg=${String(selectedSegmentId)}`);
      parts.push(eventsLoaded ? `events=${events.length}` : "events=—");
      exploreStoryCollapsedSummaryEl.textContent = parts.join(" • ");
    }

    setStoryButton(storyPrimaryButtonEl, { label: "Load hotspots", onClick: () => loadHotspots().catch(() => {}) });
    setStoryButton(storySecondaryButtonEl, { label: "Load events", onClick: () => loadEvents().catch(() => {}) });
    setStoryButton(storyTertiaryButtonEl, { label: "Copy link", onClick: () => copyShareLink().catch(() => {}) });

    storyNarrativeEl.textContent = storyBits.join(" ");
    setStoryNext([
      "1) Click “Load hotspots” to paint congestion on the map (it is bounded by the visible area).",
      "2) Click a segment to load its time series (or use the dropdown).",
      "3) Use Spotlight + Focus to keep the story centered on your selection.",
    ]);
    return;
  }

  if (page === "timeseries") {
    const entity = entityTypeEl?.value === "corridor" ? "corridor" : "segment";
    const selected = entity === "corridor" ? selectedCorridorId : selectedSegmentId;
    const metrics = getTimeseriesDerivedMetrics();

    storyTitleEl.textContent = "Time series analysis";
    storySubtitleEl.textContent = selected
      ? `Selected ${entity}: ${String(selected)}. Use the layer toggles to compare speed / baseline / anomalies.`
      : "Select a segment/corridor first, then load a time series for the chosen time window.";

    if (kpiPage1LabelEl) kpiPage1LabelEl.textContent = "Drop vs baseline";
    if (kpiPage1ValueEl)
      kpiPage1ValueEl.textContent =
        metrics?.dropVsBaselinePct == null || !Number.isFinite(metrics.dropVsBaselinePct) ? "—" : `${metrics.dropVsBaselinePct.toFixed(1)}%`;
    if (kpiPage2LabelEl) kpiPage2LabelEl.textContent = "Coverage";
    if (kpiPage2ValueEl)
      kpiPage2ValueEl.textContent =
        metrics?.coveragePct == null || !Number.isFinite(metrics.coveragePct) ? "—" : `${metrics.coveragePct.toFixed(1)}%`;

    setStoryButton(storyPrimaryButtonEl, { label: "Load time series", onClick: () => loadTimeseries().catch(() => {}) });
    setStoryButton(storySecondaryButtonEl, { label: "Use 24h window", onClick: () => quickUse24hWindow() });
    setStoryButton(storyTertiaryButtonEl, { label: "Open Explore", onClick: () => navigateToPage("explore") });

    storyNarrativeEl.textContent = storyBits.join(" ");
    setStoryNext([
      "1) Pick a segment/corridor, then click “Load time series”.",
      "2) Toggle Baseline and Event window overlays to explain changes.",
      "3) Use brush selection to set the global time window (then Apply).",
    ]);
    return;
  }

  if (page === "events") {
    const eventsExists = Boolean(diag?.events_csv?.exists);
    const evCount = eventsLoaded ? events.length : 0;
    const selected = selectedEventId ? String(selectedEventId) : null;
    const linkedText = (() => {
      if (!selected) return "—";
      if (!lastEventLinksInfo) return "—";
      if (lastEventLinksInfo.loading) return "loading";
      if (typeof lastEventLinksInfo.count === "number") return String(Math.round(lastEventLinksInfo.count));
      return "—";
    })();

    storyTitleEl.textContent = "Events: build an incident story";
    storySubtitleEl.textContent = eventsExists
      ? "Load events for the current map bounds, then select one to view impact overlays and linked hotspots."
      : "Events dataset is missing (often due to upstream rate limiting). Use the fix command and wait for the timer.";

    if (kpiPage1LabelEl) kpiPage1LabelEl.textContent = "Linked hotspots";
    if (kpiPage1ValueEl) kpiPage1ValueEl.textContent = linkedText;
    if (kpiPage2LabelEl) kpiPage2LabelEl.textContent = "Loaded events";
    if (kpiPage2ValueEl) kpiPage2ValueEl.textContent = eventsLoaded ? String(evCount) : eventsExists ? "available" : "missing";

    setStoryButton(storyPrimaryButtonEl, {
      label: eventsExists ? "Load events" : "Copy events fix",
      onClick: eventsExists
        ? () => loadEvents().catch(() => {})
        : () => copyText(EVENTS_FIX_COMMAND).then(() => setStatus("Events fix command copied to clipboard.")),
    });
    setStoryButton(storySecondaryButtonEl, {
      label: "Relax filters",
      onClick: () => eventsRelaxFiltersButton?.click(),
      hidden: !eventsRelaxFiltersButton,
    });
    setStoryButton(storyTertiaryButtonEl, { label: "Copy link", onClick: () => copyShareLink().catch(() => {}) });

    if (!eventsExists) setStoryCallout("danger");
    storyNarrativeEl.textContent = storyBits.join(" ");
    setStoryNext([
      "1) Click “Load events” to place markers (bounded by current map view).",
      "2) Select an event to fetch impact summary and draw the overlay.",
      "3) Use search/type filters and keep the overlay in sync with what you see.",
    ]);
    return;
  }

  if (page === "rankings") {
    const type = rankingTypeEl?.value || "segments";
    const totalCount = rankingsLoaded ? lastRankings.length : 0;
    const filteredCount = rankingsLoaded ? filterRankingsByQuery(lastRankings, type).length : 0;

    storyTitleEl.textContent = "Rankings: find unstable segments/corridors";
    storySubtitleEl.textContent =
      "Load rankings, filter/search, then click a row to jump to the map and sync selection for deeper inspection.";

    if (kpiPage1LabelEl) kpiPage1LabelEl.textContent = "Filtered";
    if (kpiPage1ValueEl)
      kpiPage1ValueEl.textContent = rankingsLoaded ? `${filteredCount}/${totalCount}` : "—";
    if (kpiPage2LabelEl) kpiPage2LabelEl.textContent = "Type";
    if (kpiPage2ValueEl) kpiPage2ValueEl.textContent = String(type);

    setStoryButton(storyPrimaryButtonEl, { label: "Load rankings", onClick: () => loadRankings().catch(() => {}) });
    setStoryButton(storySecondaryButtonEl, { label: "Min samples = 1", onClick: () => quickSetMinSamplesOne() });
    setStoryButton(storyTertiaryButtonEl, { label: "Open Explore", onClick: () => navigateToPage("explore") });

    storyNarrativeEl.textContent = storyBits.join(" ");
    setStoryNext([
      "1) Click “Load rankings” to compute worst reliability (time window affects results).",
      "2) Use search + sort to narrow to the corridor/road you care about.",
      "3) Click a row to focus the map and auto-load time series.",
    ]);
    return;
  }

  if (page === "pipeline") {
    const byCategory = alerts?.summary?.by_category || {};
    const network = byCategory.network || 0;
    const rateLimit = byCategory.rate_limit || 0;
    const data = byCategory.data || 0;
    const maxBackoff = typeof trendSummary?.max_backoff_seconds_24h === "number" ? trendSummary.max_backoff_seconds_24h : null;

    storyTitleEl.textContent = "Pipeline health";
    storySubtitleEl.textContent = "Monitor ingestion, rate limits, and common data issues without digging through logs.";

    if (kpiPage1LabelEl) kpiPage1LabelEl.textContent = "Alerts (24h)";
    if (kpiPage1ValueEl) kpiPage1ValueEl.textContent = `net:${network} rl:${rateLimit} data:${data}`;
    if (kpiPage2LabelEl) kpiPage2LabelEl.textContent = "Max backoff (24h)";
    if (kpiPage2ValueEl) kpiPage2ValueEl.textContent = maxBackoff != null ? `${Math.round(maxBackoff)}s` : "—";

    setStoryButton(storyPrimaryButtonEl, {
      label: "Refresh pipeline",
      onClick: () =>
        dataHub
          .refreshAll({ includeStatus: true })
          .then(() => {
            renderPipelinePanel();
            renderStory();
            setStatus("Pipeline refreshed.");
          })
          .catch(() => {}),
    });
    setStoryButton(storySecondaryButtonEl, { label: "Open alerts", onClick: () => window.open(`${API_BASE}/ui/alerts?tail=400`, "_blank") });
    setStoryButton(storyTertiaryButtonEl, {
      label: "Copy events fix",
      onClick: () => copyText(EVENTS_FIX_COMMAND).then(() => setStatus("Events fix command copied to clipboard.")),
    });

    storyNarrativeEl.textContent = storyBits.join(" ");
    setStoryNext([
      "1) If you see network/rate_limit alerts, increase MIN_REQUEST_INTERVAL_SECONDS and reduce ingest frequency.",
      "2) If data alerts appear, open /ui/quality and /ui/diagnostics for concrete missing-file reasons.",
      "3) Use dataset version changes to confirm new data is arriving over time.",
    ]);
    return;
  }
}

function initOverviewActions() {
  if (overviewOpenExploreButton) overviewOpenExploreButton.addEventListener("click", () => navigateToPage("explore"));
  if (overviewCopyLinkButton) overviewCopyLinkButton.addEventListener("click", copyShareLink);
  if (overviewLoadHotspotsButton)
    overviewLoadHotspotsButton.addEventListener("click", () => navigateToPage("explore", { action: "load_hotspots" }));
  if (overviewLoadRankingsButton)
    overviewLoadRankingsButton.addEventListener("click", () => navigateToPage("rankings", { action: "load_rankings" }));
  if (overviewLoadEventsButton)
    overviewLoadEventsButton.addEventListener("click", () => navigateToPage("events", { action: "load_events" }));
  if (overviewQuick24hButton) overviewQuick24hButton.addEventListener("click", () => quickUse24hWindow());
  if (overviewMinSamples1Button) overviewMinSamples1Button.addEventListener("click", () => quickSetMinSamplesOne());
  if (overviewCopyEventsFixButton)
    overviewCopyEventsFixButton.addEventListener("click", async () => {
      await copyText(EVENTS_FIX_COMMAND);
      setStatus("Events fix command copied to clipboard.");
    });
}

function getTimeseriesLayerState() {
  const state = getNested(uiState, "timeseries.layers", null);
  return {
    speed: state?.speed !== false,
    volume: state?.volume !== false,
    baseline: state?.baseline !== false,
    anomalies: Boolean(state?.anomalies),
    eventWindow: state?.eventWindow !== false,
  };
}

function saveTimeseriesLayerState(next) {
  uiState.timeseries = uiState.timeseries || {};
  uiState.timeseries.layers = { ...(uiState.timeseries.layers || {}), ...(next || {}) };
  saveUiState(uiState);
}

function syncTimeseriesControlsFromState() {
  const s = getTimeseriesLayerState();
  if (tsLayerSpeedEl) tsLayerSpeedEl.checked = Boolean(s.speed);
  if (tsLayerVolumeEl) tsLayerVolumeEl.checked = Boolean(s.volume);
  if (tsLayerBaselineEl) tsLayerBaselineEl.checked = Boolean(s.baseline);
  if (tsLayerAnomaliesEl) tsLayerAnomaliesEl.checked = Boolean(s.anomalies);
  if (tsLayerEventWindowEl) tsLayerEventWindowEl.checked = Boolean(s.eventWindow);
}

function initTimeseriesControls() {
  syncTimeseriesControlsFromState();
  const onChange = () => {
    saveTimeseriesLayerState({
      speed: Boolean(tsLayerSpeedEl?.checked),
      volume: Boolean(tsLayerVolumeEl?.checked),
      baseline: Boolean(tsLayerBaselineEl?.checked),
      anomalies: Boolean(tsLayerAnomaliesEl?.checked),
      eventWindow: Boolean(tsLayerEventWindowEl?.checked),
    });
    // Re-render best-effort without re-fetch.
    if (lastTimeseries && Array.isArray(lastTimeseries.points)) {
      renderTimeseriesInsights();
      renderTimeseries(lastTimeseries.points, {
        title: getEntityTitle(lastTimeseries.entity),
        anomalies: lastTimeseries.anomalies,
      });
    }
  };
  if (tsLayerSpeedEl) tsLayerSpeedEl.addEventListener("change", onChange);
  if (tsLayerVolumeEl) tsLayerVolumeEl.addEventListener("change", onChange);
  if (tsLayerBaselineEl) tsLayerBaselineEl.addEventListener("change", onChange);
  if (tsLayerAnomaliesEl) tsLayerAnomaliesEl.addEventListener("change", onChange);
  if (tsLayerEventWindowEl) tsLayerEventWindowEl.addEventListener("change", onChange);

  if (tsOpenExploreButton) tsOpenExploreButton.addEventListener("click", () => navigateToPage("explore"));

  if (tsApplyBrushButton)
    tsApplyBrushButton.addEventListener("click", () => {
      if (!pendingBrushRange) return;
      const startDt = new Date(pendingBrushRange.start);
      const endDt = new Date(pendingBrushRange.end);
      if (!Number.isFinite(startDt.getTime()) || !Number.isFinite(endDt.getTime()) || endDt <= startDt) return;
      if (followLatestEl) followLatestEl.checked = false;
      startEl.value = toLocalInputValue(startDt);
      endEl.value = toLocalInputValue(endDt);
      pendingBrushRange = null;
      if (tsApplyBrushButton) tsApplyBrushButton.classList.add("hidden");
      scheduleUrlSync();
      loadTimeseries();
      if (hotspotsLoaded) loadHotspots();
      if (rankingsLoaded) loadRankings();
      if (eventsWithinRangeEl?.checked) applyEventsFilters();
      setStatus("Applied brush to time window.");
    });
}

function getTimeseriesDerivedMetrics() {
  const entity = lastTimeseries?.entity;
  const points = Array.isArray(lastTimeseries?.points) ? lastTimeseries.points : [];
  const range = lastTimeseries?.range || getIsoRange();

  if (!entity || !entity.id || !points.length) return null;

  const speeds = points.map((p) => (p?.speed_kph == null ? NaN : Number(p.speed_kph))).filter((v) => Number.isFinite(v));
  const meanSpeedKph = speeds.length ? speeds.reduce((a, b) => a + b, 0) / speeds.length : null;

  const threshold = getNested(uiState, "overrides.reliability.congestion_speed_threshold_kph", null);
  const thr = threshold != null ? Number(threshold) : null;
  const congestionFrequency = meanSpeedKph != null && thr != null ? speeds.filter((v) => v <= thr).length / speeds.length : null;

  let expected = null;
  if (range && lastTimeseries?.minutes) {
    const startMs = Date.parse(range.start);
    const endMs = Date.parse(range.end);
    const minutes = Number(lastTimeseries.minutes);
    if (Number.isFinite(startMs) && Number.isFinite(endMs) && Number.isFinite(minutes) && minutes > 0) {
      expected = Math.max(1, Math.round((endMs - startMs) / (minutes * 60 * 1000)));
    }
  }
  const coveragePct = expected != null ? (points.length / expected) * 100 : null;

  let baselineMedianSpeedKph = null;
  if (entity.type === "segment" && hotspotRows && hotspotRows.length) {
    const row = hotspotRows.find((r) => r && String(r.segment_id) === String(entity.id));
    const v = row?.baseline_median_speed_kph;
    baselineMedianSpeedKph = v != null && Number.isFinite(Number(v)) ? Number(v) : null;
  }
  const dropVsBaselinePct =
    meanSpeedKph != null && baselineMedianSpeedKph != null && baselineMedianSpeedKph > 0
      ? ((baselineMedianSpeedKph - meanSpeedKph) / baselineMedianSpeedKph) * 100
      : null;

  return {
    meanSpeedKph,
    congestionFrequency,
    coveragePct,
    baselineMedianSpeedKph,
    dropVsBaselinePct,
    thresholdKph: thr,
  };
}

function renderTimeseriesInsights() {
  if (!timeseriesKpisEl || !timeseriesNarrativeEl) return;
  if (currentPage !== "timeseries") return;

  const entity = lastTimeseries?.entity;
  const points = Array.isArray(lastTimeseries?.points) ? lastTimeseries.points : [];

  const metrics = getTimeseriesDerivedMetrics();
  if (!entity?.id) {
    renderEmptyState(timeseriesKpisEl, {
      title: "No selection",
      text: "Select a segment/corridor and load a time series to see insights.",
      actions: [{ label: "Open Explore", onClick: () => navigateToPage("explore") }],
    });
    if (timeseriesKpisEl.firstElementChild) timeseriesKpisEl.firstElementChild.style.gridColumn = "1 / -1";
    timeseriesNarrativeEl.textContent = "Select a segment/corridor and load a time series to see insights.";
    return;
  }

  if (!points.length || !metrics) {
    const minSamples = getEffectiveMinSamples();
    const hours = getRangeHours(getCurrentRangeOrNull());
    const parts = [];
    parts.push("No observations were returned for the current selection/time window.");
    if (hours != null) parts.push(`Window: ${hours.toFixed(1)}h`);
    if (minSamples != null) parts.push(`Min samples: ${minSamples}`);
    parts.push("Try widening the window, or lowering Min samples.");

    const actions = [];
    actions.push({ label: "Use 24h window", onClick: () => quickUse24hWindow() });
    if (typeof minSamples === "number" && minSamples > 1) actions.push({ label: "Min samples = 1", onClick: () => quickSetMinSamplesOne() });
    actions.push({ label: "Reload time series", onClick: () => loadTimeseries().catch(() => {}) });

    renderEmptyState(timeseriesKpisEl, {
      title: "No time series data",
      text: parts.join(" "),
      actions,
    });
    if (timeseriesKpisEl.firstElementChild) timeseriesKpisEl.firstElementChild.style.gridColumn = "1 / -1";
    timeseriesNarrativeEl.textContent = parts.join(" ");
    return;
  }

  const fmt = (v, suffix = "") => (v == null || !Number.isFinite(v) ? "—" : `${v.toFixed(1)}${suffix}`);
  const fmtInt = (v) => (v == null || !Number.isFinite(v) ? "—" : `${Math.round(v)}`);

  timeseriesKpisEl.innerHTML = [
    `<div class="kpi"><div class="kpi-label">Mean speed</div><div class="kpi-value mono">${fmt(metrics.meanSpeedKph, " kph")}</div></div>`,
    `<div class="kpi"><div class="kpi-label">Congestion freq</div><div class="kpi-value mono">${metrics.congestionFrequency == null ? "—" : `${Math.round(metrics.congestionFrequency * 100)}%`}</div></div>`,
    `<div class="kpi"><div class="kpi-label">Coverage</div><div class="kpi-value mono">${metrics.coveragePct == null ? "—" : `${metrics.coveragePct.toFixed(1)}%`}</div></div>`,
    `<div class="kpi"><div class="kpi-label">Baseline median</div><div class="kpi-value mono">${fmt(metrics.baselineMedianSpeedKph, " kph")}</div></div>`,
    `<div class="kpi"><div class="kpi-label">Drop vs baseline</div><div class="kpi-value mono">${fmt(metrics.dropVsBaselinePct, "%")}</div></div>`,
  ].join("");

  const parts = [];
  parts.push(`${getEntityTitle(entity)} • points=${fmtInt(points.length)}.`);
  if (metrics.dropVsBaselinePct != null) parts.push(`Relative to baseline, speed is lower by ${metrics.dropVsBaselinePct.toFixed(0)}%.`);
  if (metrics.coveragePct != null) parts.push(`Coverage is ${metrics.coveragePct.toFixed(0)}% (missing data reduces confidence).`);
  if (metrics.thresholdKph != null && metrics.congestionFrequency != null)
    parts.push(
      `Congestion frequency (≤ ${Math.round(metrics.thresholdKph)} kph) is ${Math.round(metrics.congestionFrequency * 100)}%.`
    );
  timeseriesNarrativeEl.textContent = parts.join(" ");
}

function initMapToolbarAndTips() {
  syncMapToolbarFromState();
  applyOverlayVisibility();
  renderNextStepCard();

  if (mapToggleSegmentsEl) {
    mapToggleSegmentsEl.addEventListener("change", () => {
      showSegments = Boolean(mapToggleSegmentsEl.checked);
      persistOverlays();
      applyOverlayVisibility();
      updateSelectionStyles();
      renderNextStepCard();
    });
  }

  if (mapToggleHotspotsEl) {
    mapToggleHotspotsEl.addEventListener("change", () => {
      showHotspots = Boolean(mapToggleHotspotsEl.checked);
      if (toggleHotspotsEl) toggleHotspotsEl.checked = Boolean(showHotspots);
      persistOverlays();
      applyOverlayVisibility();
      renderNextStepCard();
    });
  }

  if (mapToggleEventsEl) {
    mapToggleEventsEl.addEventListener("change", () => {
      showEvents = Boolean(mapToggleEventsEl.checked);
      if (toggleEventsEl) toggleEventsEl.checked = Boolean(showEvents);
      persistOverlays();
      applyOverlayVisibility();
      renderNextStepCard();
    });
  }

  if (mapToggleImpactEl) {
    mapToggleImpactEl.addEventListener("change", () => {
      showImpact = Boolean(mapToggleImpactEl.checked);
      if (toggleImpactEl) toggleImpactEl.checked = Boolean(showImpact);
      persistOverlays();
      applyOverlayVisibility();
      renderNextStepCard();
    });
  }

  if (mapSpotlightEl) {
    mapSpotlightEl.addEventListener("change", () => {
      spotlightMode = Boolean(mapSpotlightEl.checked);
      persistOverlays();
      updateSelectionStyles();
      renderNextStepCard();
    });
  }

  if (mapFocusButton) mapFocusButton.addEventListener("click", () => focusSelected());
  if (mapReloadHotspotsButton)
    mapReloadHotspotsButton.addEventListener("click", async () => {
      showHotspots = true;
      if (toggleHotspotsEl) toggleHotspotsEl.checked = true;
      if (mapToggleHotspotsEl) mapToggleHotspotsEl.checked = true;
      persistOverlays();
      applyOverlayVisibility();
      await loadHotspots();
    });
  if (mapReloadEventsButton)
    mapReloadEventsButton.addEventListener("click", async () => {
      showEvents = true;
      if (toggleEventsEl) toggleEventsEl.checked = true;
      if (mapToggleEventsEl) mapToggleEventsEl.checked = true;
      persistOverlays();
      applyOverlayVisibility();
      await loadEvents();
    });
  if (mapClearSelectionButton) mapClearSelectionButton.addEventListener("click", clearSelection);

  if (mapNextStepCloseButton)
    mapNextStepCloseButton.addEventListener("click", () => {
      nextStepDismissed = true;
      persistOverlays();
      if (mapNextStepEl) mapNextStepEl.classList.add("hidden");
    });
  if (mapShowNextStepButton)
    mapShowNextStepButton.addEventListener("click", () => {
      nextStepDismissed = false;
      persistOverlays();
      renderNextStepCard();
    });
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
      if (mapToggleHotspotsEl) mapToggleHotspotsEl.checked = Boolean(showHotspots);
      persistOverlays();
      applyOverlayVisibility();
      renderNextStepCard();
      ev.preventDefault();
      return;
    }
    if (key === "e") {
      showEvents = !showEvents;
      if (toggleEventsEl) toggleEventsEl.checked = Boolean(showEvents);
      if (mapToggleEventsEl) mapToggleEventsEl.checked = Boolean(showEvents);
      persistOverlays();
      applyOverlayVisibility();
      renderNextStepCard();
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

function escapeHtml(text) {
  return String(text || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function buildSegmentPopupHtml(seg) {
  if (!seg) return "<b>Segment</b>";
  const id = escapeHtml(seg.segment_id);
  const road = seg.road_name ? escapeHtml(seg.road_name) : "—";
  const dir = seg.direction ? escapeHtml(seg.direction) : "";
  const city = seg.city ? escapeHtml(seg.city) : "";
  const line2 = [road, dir].filter(Boolean).join(" · ");
  const line3 = city ? `City: ${city}` : "Click to load time series";
  return `<b>${id}</b><br/><span class="mono">${line2 || "—"}</span><br/><span>${line3}</span>`;
}

function buildEventPopupHtml(event) {
  if (!event) return "<b>Event</b>";
  const id = escapeHtml(event.event_id);
  const type = event.event_type ? escapeHtml(event.event_type) : "Event";
  const time = event.start_time ? escapeHtml(formatEventTimeLocal(event.start_time)) : "—";
  const sev = event.severity != null ? `sev ${escapeHtml(event.severity)}` : "sev —";
  return `<b>${type}</b><br/><span class="mono">${time} · ${sev}</span><br/><span class="mono">${id}</span>`;
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

function updateEventInfo(event, impact, linksInfo) {
  if (!event) {
    if (lastEventsClearedReason) {
      eventInfoEl.textContent = `No event selected.\n${lastEventsClearedReason}`;
      setEventsHint({
        title: "Selection hidden by filters",
        text: lastEventsClearedReason,
        showRelaxFilters: true,
        showCopyFix: false,
      });
    } else {
      eventInfoEl.textContent = "No event selected.";
      setEventsHint({ text: "" });
    }
    return;
  }
  lastEventsClearedReason = null;
  setEventsHint({ text: "" });

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

  if (linksInfo) {
    if (linksInfo.loading) {
      parts.push("Linked hotspots: loading...");
    } else if (typeof linksInfo.count === "number") {
      parts.push(`Linked hotspots: ${Math.round(linksInfo.count)}`);
      if (linksInfo.reason && linksInfo.count === 0) {
        const msg = linksInfo.reason.message || linksInfo.reason.code || "No links.";
        parts.push(`Link note: ${msg}`);
      }
    }
  }

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

async function fetchJsonWithMeta(url, { signal } = {}) {
  const resp = await fetch(url, { headers: { accept: "application/json" }, signal });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`${resp.status} ${resp.statusText}: ${text}`);
  }
  return {
    data: await resp.json(),
    cache: {
      status: resp.headers.get("x-cache"),
      ttl: resp.headers.get("x-cache-ttl"),
    },
  };
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

async function fetchItems(url, { signal } = {}) {
  const resp = await fetch(url, { headers: { accept: "application/json" }, signal });
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
    case "relative_drop_pct":
      return "Drop vs baseline";
    case "coverage_pct":
      return "Coverage";
    default:
      return metric;
  }
}

function formatMetricValue(metric, value) {
  if (value == null || !Number.isFinite(Number(value))) return "—";
  const num = Number(value);
  if (metric === "congestion_frequency") return `${Math.round(num * 100)}%`;
  if (metric === "relative_drop_pct" || metric === "coverage_pct") return `${num.toFixed(1)}%`;
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
  const unit = metric === "congestion_frequency" || metric === "relative_drop_pct" || metric === "coverage_pct" ? "%" : "kph";
  const hint =
    metric === "coverage_pct"
      ? "low coverage → high coverage"
      : metric === "relative_drop_pct"
        ? "low drop → high drop"
        : metric === "congestion_frequency"
          ? "rare → frequent"
          : metric === "speed_std_kph"
            ? "stable → volatile"
            : "slow → fast";
  hotspotLegendTitleEl.textContent = `Hotspots · ${metricLabel(metric)} · ${hint} (${unit})`;

  const colors = themeColors();
  const accent = `rgba(${colors.accent2Rgb[0]}, ${colors.accent2Rgb[1]}, ${colors.accent2Rgb[2]}, 0.92)`;
  const danger = `rgba(${colors.dangerRgb[0]}, ${colors.dangerRgb[1]}, ${colors.dangerRgb[2]}, 0.92)`;
  const gradient =
    metric === "congestion_frequency" || metric === "coverage_pct" || metric === "relative_drop_pct"
      ? `linear-gradient(90deg, ${accent}, ${danger})`
      : `linear-gradient(90deg, ${danger}, ${accent})`;
  hotspotLegendBarEl.style.background = gradient;

  const mid = (Number(range.min) + Number(range.max)) / 2;
  const toLabel = (value) => {
    if (!Number.isFinite(Number(value))) return "—";
    if (metric === "congestion_frequency") return `${Math.round(Number(value) * 100)}%`;
    if (metric === "relative_drop_pct" || metric === "coverage_pct") return `${Number(value).toFixed(1)}%`;
    return `${Number(value).toFixed(1)}`;
  };

  const minText = toLabel(range.min);
  const midText = toLabel(mid);
  const maxText = toLabel(range.max);

  if (hotspotLegendMinEl) hotspotLegendMinEl.textContent = minText;
  if (hotspotLegendMidEl) hotspotLegendMidEl.textContent = midText;
  if (hotspotLegendMaxEl) hotspotLegendMaxEl.textContent = maxText;
}

function renderHotspots(rows, metric, colorMode = "metric") {
  hotspotsLayer.clearLayers();
  hotspotMarkerBySegmentId = new Map();
  if (!rows || !rows.length) {
    updateHotspotInfo("No hotspots loaded.");
    updateHotspotLegend(null, null);
    updateHotspotsHint({ empty: hotspotsLoaded });
    return;
  }
  updateHotspotsHint({ empty: false });

  const key = colorMode === "metric" ? metric : String(colorMode || metric);
  const colors = themeColors();
  const accent = colors.accent2Rgb;
  const danger = colors.dangerRgb;
  const range = computeMetricRange(rows, key);
  updateHotspotLegend(key, range);

  let rendered = 0;
  for (const row of rows) {
    if (!row) continue;
    const lat = Number(row.lat);
    const lon = Number(row.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    const raw = row[key];
    const value = raw == null ? null : Number(raw);
    let color = "rgba(15, 23, 42, 0.12)";
    if (value != null && Number.isFinite(value)) {
      if (key === "congestion_frequency") {
        color = lerpColor(accent, danger, clamp(value, 0, 1));
      } else if (key === "relative_drop_pct" || key === "coverage_pct") {
        const t = (value - range.min) / (range.max - range.min);
        color = lerpColor(accent, danger, t);
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

    hotspotMarkerBySegmentId.set(String(row.segment_id), marker);

    const label = metricLabel(key);
    const formatted = formatMetricValue(key, value);
    const extras = [];
    if (row.baseline_median_speed_kph != null && Number.isFinite(Number(row.baseline_median_speed_kph))) {
      extras.push(`base=${Number(row.baseline_median_speed_kph).toFixed(1)}kph`);
    }
    if (row.relative_drop_pct != null && Number.isFinite(Number(row.relative_drop_pct))) {
      extras.push(`drop=${Number(row.relative_drop_pct).toFixed(0)}%`);
    }
    if (row.coverage_pct != null && Number.isFinite(Number(row.coverage_pct))) {
      extras.push(`cov=${Number(row.coverage_pct).toFixed(1)}%`);
    }
    const extraText = extras.length ? `<br/><span class="mono">${escapeHtml(extras.join(" · "))}</span>` : "";
    marker.bindPopup(
      `<b>${escapeHtml(row.segment_id)}</b><br/><span class="mono">${escapeHtml(label)}: ${escapeHtml(formatted)}</span>${extraText}`,
      { closeButton: false }
    );
    marker.on("click", () => {
      selectSegment(String(row.segment_id), { centerMap: false });
      loadTimeseries();
    });

    rendered += 1;
  }

  const minText = formatMetricValue(key, range.min);
  const maxText = formatMetricValue(key, range.max);
  updateHotspotInfo(`Loaded ${rendered} segments.\nColor: ${metricLabel(key)}\nRange: ${minText} → ${maxText}`);
}

function clearHotspots() {
  hotspotRows = [];
  hotspotsLoaded = false;
  lastHotspotsReason = null;
  hotspotsLayer.clearLayers();
  hotspotMarkerBySegmentId = new Map();
  updateHotspotLegend(null, null);
  updateHotspotInfo("No hotspots loaded.");
  setStatus("Hotspots cleared.");
}

async function loadHotspots() {
  setStatus("Loading hotspots...");

  const metric = hotspotMetricEl.value || "mean_speed_kph";
  const colorMode = hotspotColorModeEl ? String(hotspotColorModeEl.value || "metric") : "metric";
  const url = new URL(`${API_BASE}/map/snapshot`);
  url.searchParams.set("include_baseline", "true");
  url.searchParams.set("include_quality", "true");

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
    saveCached("hotspots", { at_utc: new Date().toISOString(), url: url.toString(), items, reason });
  } catch (err) {
    const cached = loadCached("hotspots");
    if (cached && Array.isArray(cached.items)) {
      hotspotRows = cached.items;
      lastHotspotsReason = cached.reason || null;
      hotspotsLoaded = true;
      renderHotspots(hotspotRows, metric, hotspotColorModeEl?.value || "metric");
      setStatus(`Hotspots loaded from cache (${hotspotRows.length} rows, cached_at=${cached.at_utc || "?"}).`);
      renderStory();
      return;
    }
    hotspotsLoaded = false;
    lastHotspotsReason = null;
    updateHotspotInfo("Failed to load hotspots. Ensure the API includes /map/snapshot and a dataset is built.");
    setStatus(`Failed to load hotspots: ${err.message}`);
    updateHotspotsHint({ empty: false });
    renderNextStepCard();
    renderStory();
    return;
  }

  hotspotsLoaded = true;
  renderHotspots(hotspotRows, metric, colorMode);
  setStatus(`Hotspots loaded (${hotspotRows.length} rows).`);
  renderNextStepCard();
  renderStory();
}

function applyHotspotStateToForm() {
  if (!hotspotAutoEl) return;
  hotspotAutoEl.checked = Boolean(getNested(uiState, "hotspots.autoReload", false));
  if (hotspotColorModeEl) {
    const mode = String(getNested(uiState, "hotspots.colorMode", hotspotColorModeEl.value || "metric"));
    hotspotColorModeEl.value = ["metric", "relative_drop_pct", "coverage_pct"].includes(mode) ? mode : "metric";
  }
}

function initHotspotAutoReload() {
  if (!hotspotAutoEl) return;
  applyHotspotStateToForm();
  hotspotAutoEl.addEventListener("change", () => {
    uiState.hotspots = uiState.hotspots || {};
    uiState.hotspots.autoReload = Boolean(hotspotAutoEl.checked);
    saveUiState(uiState);
  });
  if (hotspotColorModeEl) {
    hotspotColorModeEl.addEventListener("change", () => {
      uiState.hotspots = uiState.hotspots || {};
      uiState.hotspots.colorMode = String(hotspotColorModeEl.value || "metric");
      saveUiState(uiState);
    });
  }

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
  if (anomaliesAbortController) anomaliesAbortController.abort();
  anomaliesAbortController = new AbortController();
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
    const { items } = await fetchItems(url.toString(), { signal: anomaliesAbortController.signal });
    return items;
  } catch (err) {
    return null;
  }
}

function renderTimeseries(points, { title, anomalies } = {}) {
  if (typeof Plotly === "undefined") {
    if (chartEl) chartEl.textContent = "Plotly failed to load (check network/CDN access).";
    setStatus("Plotly failed to load.");
    return;
  }
  if (!points.length) {
    Plotly.purge(chartEl);
    setStatus("No data returned for this time range.");
    return;
  }

  const colors = themeColors();
  const layers = getTimeseriesLayerState();
  const entity = lastTimeseries?.entity || null;
  const x = points.map((p) => p.timestamp);
  const speed = points.map((p) => p.speed_kph);
  const volume = points.map((p) => p.volume);

  const traces = [];
  if (layers.speed) {
    traces.push({
      x,
      y: speed,
      type: "scatter",
      mode: "lines",
      name: "Speed (kph)",
      line: { color: colors.accentHex, width: 2 },
      yaxis: "y",
    });
  }
  if (layers.volume) {
    traces.push({
      x,
      y: volume,
      type: "bar",
      name: "Volume",
      marker: { color: rgba(colors.accentRgb, 0.18) },
      yaxis: "y2",
    });
  }

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

    if (layers.baseline) {
      traces.push({
        x,
        y: baseline,
        type: "scatter",
        mode: "lines",
        name: "Baseline (mean)",
        line: { color: colors.muted, width: 1, dash: "dot" },
        yaxis: "y",
      });
    }

    if (layers.anomalies) {
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
      rangeslider: { visible: true, bgcolor: "rgba(0,0,0,0)" },
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
    shapes: [],
  };

  if (layers.baseline && entity?.type === "segment" && hotspotRows && hotspotRows.length) {
    const row = hotspotRows.find((r) => r && String(r.segment_id) === String(entity.id));
    const v = row?.baseline_median_speed_kph;
    const baselineMedian = v != null && Number.isFinite(Number(v)) ? Number(v) : null;
    if (baselineMedian != null) {
      traces.push({
        x,
        y: x.map(() => baselineMedian),
        type: "scatter",
        mode: "lines",
        name: "Baseline (median)",
        line: { color: colors.muted, width: 1, dash: "dash" },
        yaxis: "y",
      });
    }
  }

  if (layers.eventWindow && selectedEventId && lastEventImpact?.event?.start_time && lastEventImpact?.event?.end_time) {
    layout.shapes.push({
      type: "rect",
      xref: "x",
      yref: "paper",
      x0: lastEventImpact.event.start_time,
      x1: lastEventImpact.event.end_time,
      y0: 0,
      y1: 1,
      fillcolor: rgba(colors.dangerRgb, 0.08),
      line: { width: 0 },
    });
  }

  Plotly.react(chartEl, traces, layout, { responsive: true, displayModeBar: false });
  setStatus(`Loaded ${points.length} points.`);

  try {
    chartEl.removeAllListeners?.("plotly_relayout");
  } catch (err) {
    // ignore
  }
  chartEl.on?.("plotly_relayout", (ev) => {
    if (!ev || typeof ev !== "object") return;
    const start = ev["xaxis.range[0]"];
    const end = ev["xaxis.range[1]"];
    if (!start || !end) return;
    pendingBrushRange = { start: String(start), end: String(end) };
    if (tsApplyBrushButton) tsApplyBrushButton.classList.remove("hidden");
  });
}

function renderEventImpactChart(impact) {
  if (typeof Plotly === "undefined") {
    if (chartEl) chartEl.textContent = "Plotly failed to load (check network/CDN access).";
    setStatus("Plotly failed to load.");
    return;
  }
  const points = impact.timeseries || [];
  if (!points.length) {
    Plotly.purge(chartEl);
    setStatus("No time series available for this event impact.");
    setChartHint({
      title: "No impact timeseries",
      text: "No time series is available for this event impact (try a different granularity or time window).",
    });
    return;
  }
  setChartHint(null);

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
    setChartHint({
      title: "Missing selection",
      text: entity.type === "corridor" ? "Select a corridor to load a time series." : "Select a segment to load a time series.",
    });
    return;
  }

  const range = getIsoRange();
  if (!range) {
    setStatus("Invalid time range. Please select start/end.");
    setChartHint({ title: "Invalid time range", text: "Please select a valid start/end time window." });
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
  setChartHint(null);
  if (timeseriesAbortController) timeseriesAbortController.abort();
  timeseriesAbortController = new AbortController();
  try {
    const { items } = await fetchItems(url.toString(), { signal: timeseriesAbortController.signal });
    const points = items.map((p) => ({ timestamp: p.timestamp, speed_kph: p.speed_kph, volume: p.volume }));

    lastTimeseries = { entity, range, minutes, points, anomalies: null };

    let anomalies = null;
    if (showAnomalies) {
      anomalies = await loadAnomalies(entity, range, minutes);
      lastTimeseries.anomalies = anomalies;
    }

    if (!points.length) {
      setChartHint({
        title: "No data",
        text: "No time series points returned for this time window.",
        actions: [{ label: "Use 24h window", onClick: () => quickUse24hWindow() }],
      });
    }
    renderTimeseries(points, { title: getEntityTitle(entity), anomalies });
    renderTimeseriesInsights();
    renderStory();
  } catch (err) {
    if (err?.name === "AbortError") return;
    setStatus(`Failed to load timeseries: ${err.message}`);
    setChartHint({
      title: "Timeseries error",
      text: `Failed to load time series: ${err.message}`,
      actions: [{ label: "Refresh health", onClick: () => refreshDataHealth() }],
    });
    renderStory();
  }
}

function selectSegment(segmentId, { centerMap } = { centerMap: true }) {
  entityTypeEl.value = "segment";
  selectedSegmentId = segmentId;
  selectedCorridorId = null;
  const seg = segmentsById.get(segmentId);
  updateSegmentInfo(seg);
  segmentSelectEl.value = segmentId;

  const marker = markerById.get(segmentId);
  if (centerMap && marker) {
    map.setView(marker.getLatLng(), Math.max(map.getZoom(), 14), { animate: true });
    marker.openPopup();
  }
  updateSelectionStyles();
  scheduleUrlSync();
  renderNextStepCard();
  renderStory();
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

function updateSelectionStyles() {
  const selectedSeg = selectedSegmentId ? String(selectedSegmentId) : null;
  const selectedEv = selectedEventId ? String(selectedEventId) : null;

  for (const [segId, marker] of markerById.entries()) {
    if (!marker) continue;
    const isSelected = selectedSeg != null && String(segId) === selectedSeg;
    if (typeof marker.setStyle === "function") {
      marker.setStyle({
        weight: isSelected ? 3 : 2,
        fillOpacity: isSelected ? 0.85 : 0.6,
      });
    }
    if (typeof marker.setRadius === "function") marker.setRadius(isSelected ? 7 : 5);
  }

  for (const [eventId, marker] of eventMarkerById.entries()) {
    if (!marker) continue;
    const isSelected = selectedEv != null && String(eventId) === selectedEv;
    if (typeof marker.setStyle === "function") {
      marker.setStyle({
        weight: isSelected ? 3 : 2,
        fillOpacity: isSelected ? 0.95 : 0.85,
      });
    }
    if (typeof marker.setRadius === "function") marker.setRadius(isSelected ? 8 : 6);
  }

  for (const [segId, marker] of hotspotMarkerBySegmentId.entries()) {
    if (!marker) continue;
    const isSelected = selectedSeg != null && String(segId) === selectedSeg;
    if (typeof marker.setStyle === "function") {
      marker.setStyle({
        weight: isSelected ? 3 : 2,
        fillOpacity: isSelected ? 0.9 : 0.78,
      });
    }
    if (typeof marker.setRadius === "function") marker.setRadius(isSelected ? 9 : 7);
  }

  applySpotlightStyles();
  updateSelectionHalos();
}

function selectCorridor(corridorId, { centerMap } = { centerMap: false }) {
  entityTypeEl.value = "corridor";
  selectedCorridorId = corridorId;
  selectedSegmentId = null;
  const corridor = corridorsById.get(corridorId);
  updateCorridorInfo(corridor);
  corridorSelectEl.value = corridorId;
  if (centerMap) {
    centerMapOnCorridor(corridorId);
    setStatus(`Centered on corridor ${corridorId}.`);
  }
  updateSelectionStyles();
  scheduleUrlSync();
  renderNextStepCard();
  renderStory();
}

function selectEvent(eventId, { centerMap } = { centerMap: true }) {
  selectedEventId = eventId;
  const event = eventsById.get(eventId);
  lastEventLinksInfo = { loading: true, count: 0, reason: null };
  linkedHotspotsLayer.clearLayers();
  updateEventInfo(event, null, lastEventLinksInfo);

  if (eventsLoaded) renderEvents(getFilteredEvents(events));

  impactSegmentsLayer.clearLayers();
  lastEventImpact = null;
  updateSelectionStyles();
  renderNextStepCard();
  renderEventsStory();
  renderStory();

  const marker = eventMarkerById.get(eventId);
  if (centerMap && marker) {
    map.setView(marker.getLatLng(), Math.max(map.getZoom(), 14), { animate: true });
    marker.openPopup();
  }

  if (!event) return;

  // Load linked hotspots (best-effort; depends on processing output).
  if (linksAbortController) linksAbortController.abort();
  linksAbortController = new AbortController();
  const linksUrl = new URL(`${API_BASE}/ui/event_hotspot_links`);
  linksUrl.searchParams.set("event_id", String(eventId));
  linksUrl.searchParams.set("limit", "200");
  fetchItems(linksUrl.toString(), { signal: linksAbortController.signal })
    .then(({ items, reason }) => {
      lastEventLinksInfo = { loading: false, count: Array.isArray(items) ? items.length : 0, reason };
      updateEventInfo(event, lastEventImpact, lastEventLinksInfo);
      renderEventsStory();

      linkedHotspotsLayer.clearLayers();
      if (!Array.isArray(items) || !items.length) return;
      for (const link of items) {
        const segId = link && link.segment_id ? String(link.segment_id) : null;
        if (!segId) continue;
        const seg = segmentsById.get(segId);
        if (!seg || seg.lat == null || seg.lon == null) continue;
        const lat = Number(seg.lat);
        const lon = Number(seg.lon);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
        const score = link.score != null ? Number(link.score) : null;
        const labelScore = score != null && Number.isFinite(score) ? `score=${score.toFixed(1)}` : "score=—";
        const ring = L.circleMarker([lat, lon], {
          radius: 11,
          color: themeColors().accent2Hex,
          weight: 3,
          fillOpacity: 0,
          opacity: 0.9,
        }).addTo(linkedHotspotsLayer);
        ring.bindPopup(`Linked hotspot: ${segId}\n${labelScore}`, { closeButton: false });
        ring.on("click", () => {
          selectSegment(segId, { centerMap: false });
          loadTimeseries();
        });
      }
    })
    .catch((err) => {
      if (err?.name === "AbortError") return;
      lastEventLinksInfo = { loading: false, count: 0, reason: { code: "failed", message: err.message, suggestion: null } };
      updateEventInfo(event, lastEventImpact, lastEventLinksInfo);
      renderEventsStory();
    });

  const minutes = minutesEl.value;
  const url = new URL(`${API_BASE}/events/${encodeURIComponent(eventId)}/impact`);
  url.searchParams.set("include_timeseries", "true");
  if (minutes) url.searchParams.set("minutes", minutes);
  applyImpactOverrides(url);

  setStatus("Loading event impact...");
  if (impactAbortController) impactAbortController.abort();
  impactAbortController = new AbortController();

  const cacheKey = url.toString();
  const cached = impactCache.get(cacheKey);
  const loadPromise = cached
    ? Promise.resolve({ data: cached, cache: { status: "HIT", ttl: null } })
    : fetchJsonWithMeta(cacheKey, { signal: impactAbortController.signal });

  loadPromise
    .then(({ data }) => {
      const impact = data;
      impactCache.set(cacheKey, impact);
      lastEventImpact = impact;
      updateEventInfo(event, impact, lastEventLinksInfo);
      renderEventImpactChart(impact);
      renderEventsStory();

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
      if (err?.name === "AbortError") return;
      setStatus(`Failed to load event impact: ${err.message}`);
      setChartHint({
        title: "Event impact error",
        text: `Failed to load event impact: ${err.message}`,
        actions: [{ label: "Refresh health", onClick: () => refreshDataHealth() }],
      });
      updateEventInfo(event, null, lastEventLinksInfo);
      renderEventsStory();
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

    marker.bindPopup(buildSegmentPopupHtml(seg), { closeButton: false });
    marker.on("click", () => {
      selectSegment(seg.segment_id, { centerMap: false });
      loadTimeseries();
    });

    markerById.set(seg.segment_id, marker);
    bounds = bounds ? bounds.extend([lat, lon]) : L.latLngBounds([lat, lon], [lat, lon]);
  }

  if (bounds) map.fitBounds(bounds.pad(0.08));
  updateSelectionStyles();
  setStatus(`Loaded ${segments.length} segments.`);
  renderNextStepCard();
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

function filterRankingsByQuery(items, type) {
  const q = normalizeText(rankingSearchEl ? rankingSearchEl.value : "");
  if (!q) return items;
  return items.filter((row) => {
    if (type === "corridors") {
      return normalizeText(row.corridor_id).includes(q) || normalizeText(row.corridor_name).includes(q);
    }
    return normalizeText(row.segment_id).includes(q);
  });
}

function renderRankings(items, type) {
  rankingsEl.innerHTML = "";
  if (!items || !items.length) {
    const minSamples = getEffectiveMinSamples();
    const hours = getRangeHours(getCurrentRangeOrNull());
    const parts = [];
    if (lastRankingsReason?.message) parts.push(lastRankingsReason.message);
    parts.push("No rankings returned for the current settings.");
    if (hours != null) parts.push(`Window: ${hours.toFixed(1)}h`);
    if (minSamples != null) parts.push(`Min samples: ${minSamples}`);
    if (lastRankingsReason?.suggestion) parts.push(lastRankingsReason.suggestion);

    const actions = [];
    actions.push({ label: "Use 24h window", onClick: () => quickUse24hWindow() });
    if (typeof minSamples === "number" && minSamples > 1) actions.push({ label: "Min samples = 1", onClick: () => quickSetMinSamplesOne() });
    actions.push({ label: "Reload rankings", onClick: () => loadRankings().catch(() => {}) });

    renderEmptyState(rankingsEl, {
      title: "No rankings",
      text: parts.join(" "),
      actions,
    });
    updateRankingsHint({ empty: true });
    return;
  }
  updateRankingsHint({ empty: false });

  const filtered = filterRankingsByQuery(items, type);
  if (!filtered.length) {
    const q = normalizeText(rankingSearchEl ? rankingSearchEl.value : "");
    const actions = [];
    if (q && rankingSearchEl) {
      actions.push({
        label: "Clear search",
        onClick: () => {
          rankingSearchEl.value = "";
          renderRankings(items, type);
          renderStory();
        },
      });
    }
    actions.push({ label: "Reload rankings", onClick: () => loadRankings().catch(() => {}) });
    renderEmptyState(rankingsEl, {
      title: "No matches",
      text: "No rankings match the current search query.",
      actions,
    });
    return;
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
    const extras = [];
    if (row.coverage_pct != null && Number.isFinite(Number(row.coverage_pct))) extras.push(`Cov: ${Number(row.coverage_pct).toFixed(1)}%`);
    if (row.baseline_median_speed_kph != null && Number.isFinite(Number(row.baseline_median_speed_kph)))
      extras.push(`Base: ${Number(row.baseline_median_speed_kph).toFixed(1)} kph`);
    subEl.textContent = `Mean: ${mean} kph · Cong: ${freq}${extras.length ? ` · ${extras.join(" · ")}` : ""}`;

    if (type === "corridors") {
      const name = row.corridor_name ? ` - ${row.corridor_name}` : "";
      const corridorId = String(row.corridor_id);
      idEl.textContent = `${corridorId}${name}`;
      el.addEventListener("click", () => {
        Array.from(rankingsEl.querySelectorAll(".ranking-row")).forEach((n) => n.classList.remove("selected"));
        el.classList.add("selected");
        selectCorridor(corridorId, { centerMap: true });
        loadTimeseries();
      });
    } else {
      const segmentId = String(row.segment_id);
      idEl.textContent = segmentId;
      el.addEventListener("click", () => {
        Array.from(rankingsEl.querySelectorAll(".ranking-row")).forEach((n) => n.classList.remove("selected"));
        el.classList.add("selected");
        selectSegment(segmentId, { centerMap: true });
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
  url.searchParams.set("include_quality", "true");
  url.searchParams.set("include_baseline", "true");
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
    saveCached("rankings", { at_utc: new Date().toISOString(), url: url.toString(), items, reason, type });
    renderRankings(items, type);
    rankingsLoaded = true;
    setStatus(`Loaded ${items.length} ranking rows.`);
    renderNextStepCard();
    renderStory();
  } catch (err) {
    const cached = loadCached("rankings");
    if (cached && Array.isArray(cached.items)) {
      lastRankings = cached.items;
      lastRankingsReason = cached.reason || null;
      const cachedType = cached.type || type;
      renderRankings(lastRankings, cachedType);
      rankingsLoaded = true;
      setStatus(`Rankings loaded from cache (${lastRankings.length} rows, cached_at=${cached.at_utc || "?"}).`);
      renderStory();
      return;
    }
    rankingsLoaded = false;
    lastRankingsReason = null;
    rankingsEl.textContent = "Failed to load rankings.";
    updateRankingsHint({ empty: false });
    setStatus(`Failed to load rankings: ${err.message}`);
    renderNextStepCard();
    renderStory();
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

    marker.bindPopup(buildEventPopupHtml(event), { closeButton: false });
    marker.on("click", () => selectEvent(event.event_id, { centerMap: false }));
    eventMarkerById.set(event.event_id, marker);
  }
  updateSelectionStyles();
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
      updateEventInfo(null, null, null);
    }
  }
}

function renderEvents(items) {
  eventsEl.innerHTML = "";
  if (!items || !items.length) {
    const parts = [];
    const actions = [];

    if (Array.isArray(events) && events.length) {
      parts.push("No events match the current filters.");
      if (eventsWithinRangeEl?.checked) parts.push("“Only within current time range” may be too strict.");
      if (eventsSearchEl?.value) parts.push("Search query may be too narrow.");

      if (eventsRelaxFiltersButton) {
        actions.push({ label: "Relax filters", onClick: () => eventsRelaxFiltersButton.click() });
      }
      if (eventsWithinRangeEl?.checked) {
        actions.push({
          label: "Show all times",
          onClick: () => {
            eventsWithinRangeEl.checked = false;
            uiState.events = uiState.events || {};
            uiState.events.onlyWithinRange = false;
            saveUiState(uiState);
            applyEventsFilters();
          },
        });
      }
      if (eventsSearchEl?.value) {
        actions.push({
          label: "Clear search",
          onClick: () => {
            eventsSearchEl.value = "";
            applyEventsFilters();
          },
        });
      }
    } else {
      if (lastEventsReason?.message) parts.push(lastEventsReason.message);
      parts.push("No events returned for the current bbox/time window.");
      if (lastEventsReason?.suggestion) parts.push(lastEventsReason.suggestion);

      actions.push({ label: "Load events", onClick: () => loadEvents().catch(() => {}) });
      actions.push({
        label: "Copy events fix",
        onClick: () => copyText(EVENTS_FIX_COMMAND).then(() => setStatus("Events fix command copied to clipboard.")),
      });
      actions.push({ label: "Use 24h window", onClick: () => quickUse24hWindow() });
    }

    renderEmptyState(eventsEl, {
      title: "No events",
      text: parts.join(" "),
      actions,
    });
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
    saveCached("events", { at_utc: new Date().toISOString(), url: url.toString(), items, reason });
  } catch (err) {
    const cached = loadCached("events");
    if (cached && Array.isArray(cached.items)) {
      events = cached.items;
      lastEventsReason = cached.reason || null;
      eventsLoaded = true;
      eventsById = new Map(events.map((e) => [e.event_id, e]));
      impactSegmentsLayer.clearLayers();
      selectedEventId = null;
      lastEventImpact = null;
      lastEventsClearedReason = null;
      updateEventInfo(null, null, null);
      populateEventsTypeOptions(events);
      applyEventsFilters();
      setStatus(`Events loaded from cache (${events.length} rows, cached_at=${cached.at_utc || "?"}).`);
      renderNextStepCard();
      renderStory();
      return;
    }
    eventsLoaded = false;
    eventsEl.textContent = "Failed to load events.";

    let diag = dataHub.state.diagnostics;
    if (!diag) {
      try {
        diag = await dataHub.refreshDiagnostics();
      } catch (e) {
        diag = null;
      }
    }
    const eventsExists = Boolean(diag?.events_csv?.exists);
    const linksExists = Boolean(diag?.event_hotspot_links?.exists);

    const hintParts = [];
    if (!eventsExists) hintParts.push("events.csv is missing (likely upstream rate-limited).");
    else hintParts.push("events.csv exists, but the /events request failed (API/network issue).");
    if (!linksExists) hintParts.push("event_hotspot_links.csv is not available yet.");
    hintParts.push("Wait for the next hourly events timer, or run the events service manually.");
    hintParts.push("Use “Copy events fix” to copy the command.");

    setEventsHint({
      title: "Events dataset unavailable",
      text: hintParts.join(" "),
      showRelaxFilters: false,
      showCopyFix: true,
    });
    setStatus(`Failed to load events: ${err.message}`);
    renderNextStepCard();
    renderEventsStory();
    renderStory();
    return;
  }

  eventsLoaded = true;
  eventsById = new Map(events.map((e) => [e.event_id, e]));

  impactSegmentsLayer.clearLayers();
  selectedEventId = null;
  lastEventImpact = null;
  lastEventsClearedReason = null;
  updateEventInfo(null, null, null);
  populateEventsTypeOptions(events);
  applyEventsFilters();
  if (!selectedEventId && showEvents && showImpact) {
    const candidates = getFilteredEvents(events);
    const first = candidates && candidates.length ? candidates[0] : events.length ? events[0] : null;
    if (first && first.event_id) {
      selectEvent(String(first.event_id), { centerMap: true });
    }
  }
  if (!events.length) {
    if (isLikelyDatasetMissing(lastEventsReason)) {
      let diag = dataHub.state.diagnostics;
      if (!diag) {
        try {
          diag = await dataHub.refreshDiagnostics();
        } catch (e) {
          diag = null;
        }
      }
      const eventsExists = Boolean(diag?.events_csv?.exists);
      const linksExists = Boolean(diag?.event_hotspot_links?.exists);
      const hintParts = [];
      if (!eventsExists) hintParts.push("events.csv is missing (likely upstream rate-limited).");
      if (!linksExists) hintParts.push("event_hotspot_links.csv is not available yet.");
      hintParts.push("Wait for the next hourly events timer, or run the events service manually.");
      hintParts.push("Use “Copy events fix” to copy the command.");
      setEventsHint({
        title: "Events dataset unavailable",
        text: hintParts.join(" "),
        showRelaxFilters: false,
        showCopyFix: true,
      });
    } else {
      const msgParts = [];
      if (lastEventsReason?.message) msgParts.push(lastEventsReason.message);
      msgParts.push("No events were returned for the current bbox/time window.");
      if (lastEventsReason?.suggestion) msgParts.push(lastEventsReason.suggestion);
      setEventsHint({
        title: "No events returned",
        text: msgParts.join(" "),
        showRelaxFilters: true,
        showCopyFix: false,
      });
    }
  }
  setStatus(`Loaded ${events.length} events (${getFilteredEvents(events).length} shown).`);
  renderNextStepCard();
  renderEventsStory();
  renderStory();
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
  updateEventInfo(null, null, null);
  setEventsHint({ text: "" });
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
      setEventsHint({ text: "" });
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
hotspotMetricEl.addEventListener("change", () => renderHotspots(hotspotRows, hotspotMetricEl.value, hotspotColorModeEl?.value || "metric"));
if (hotspotColorModeEl)
  hotspotColorModeEl.addEventListener("change", () => renderHotspots(hotspotRows, hotspotMetricEl.value, hotspotColorModeEl.value));
if (rankingSearchEl)
  rankingSearchEl.addEventListener("input", () => {
    if (!rankingsLoaded) return;
    renderRankings(lastRankings, rankingTypeEl.value || "segments");
    renderStory();
  });
if (rankingSortEl)
  rankingSortEl.addEventListener("change", () => {
    if (!rankingsLoaded) return;
    renderRankings(lastRankings, rankingTypeEl.value || "segments");
  });
hotspotMetricEl.addEventListener("change", scheduleUrlSync);
if (hotspotColorModeEl) hotspotColorModeEl.addEventListener("change", scheduleUrlSync);
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
  initMapToolbarAndTips();
	initShortcutsOverlay();
	initKeyboardShortcuts();
	initLivePanel();
	initHotspotAutoReload();
	initTimeseriesFollowLatest();
	initEventsPanel();
	initQuickGuides();
if (copyLinkButton) copyLinkButton.addEventListener("click", copyShareLink);
if (exportSnapshotButton) exportSnapshotButton.addEventListener("click", exportSnapshot);
if (exportSegmentsCsvButton)
  exportSegmentsCsvButton.addEventListener("click", () => window.open(buildExportUrl("/exports/reliability/segments.csv"), "_blank"));
if (exportCorridorsCsvButton)
  exportCorridorsCsvButton.addEventListener("click", () => window.open(buildExportUrl("/exports/reliability/corridors.csv"), "_blank"));
if (dataHealthRefreshButton) dataHealthRefreshButton.addEventListener("click", refreshDataHealth);
if (dataHealthCopyCommandsButton)
  dataHealthCopyCommandsButton.addEventListener("click", async () => {
    const health = await refreshDataHealth();
    const cmds = buildFixCommands(health || {});
    await copyText(cmds.join("\n"));
    setStatus("Fix commands copied to clipboard.");
  });
	if (pipelineRefreshButton)
	  pipelineRefreshButton.addEventListener("click", async () => {
	    await dataHub.refreshAll({ includeStatus: true });
	    renderPipelinePanel();
	    setStatus("Pipeline refreshed.");
	  });
	if (pipelineCopyEventsFixButton)
	  pipelineCopyEventsFixButton.addEventListener("click", async () => {
	    await copyText(EVENTS_FIX_COMMAND);
	    setStatus("Events fix command copied to clipboard.");
	  });
	if (eventsCopyFixButton)
	  eventsCopyFixButton.addEventListener("click", async () => {
	    await copyText(EVENTS_FIX_COMMAND);
	    setStatus("Events fix command copied to clipboard.");
	  });

loadUiDefaultsFromApi().then((defaults) => {
  if (defaults) applyDefaultsToForm(defaults);
  applyStateToForm(uiState);
  applyUrlOverridesToForm();
  applyThemeFromState();
  scheduleUrlSync();
  refreshDataHealth();
});

setDefaultTimeRange();
Promise.allSettled([loadSegments(), loadCorridors()]).then(() => {
  const navAction = consumeNavAction();
  if (navAction === "load_hotspots" && currentPage === "explore") loadHotspots().catch(() => {});
  if (navAction === "load_rankings" && currentPage === "rankings") loadRankings().catch(() => {});
  if (navAction === "load_events" && currentPage === "events") loadEvents().catch(() => {});

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
