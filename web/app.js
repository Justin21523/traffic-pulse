/* global L, Plotly */

const API_BASE = (() => {
  const override = new URLSearchParams(window.location.search).get("api");
  if (override) return override.replace(/\/$/, "");

  if (window.location.port === "8000") return window.location.origin;
  return `${window.location.protocol}//${window.location.hostname}:8000`;
})();

const statusEl = document.getElementById("status");
const apiBaseEl = document.getElementById("api-base");

const segmentSearchEl = document.getElementById("segment-search");
const segmentSelectEl = document.getElementById("segment-select");
const segmentInfoEl = document.getElementById("segment-info");

const corridorSelectEl = document.getElementById("corridor-select");
const corridorInfoEl = document.getElementById("corridor-info");

const entityTypeEl = document.getElementById("entity-type");
const startEl = document.getElementById("start");
const endEl = document.getElementById("end");
const minutesEl = document.getElementById("minutes");
const loadButton = document.getElementById("load");

const rankingTypeEl = document.getElementById("ranking-type");
const rankingLimitEl = document.getElementById("ranking-limit");
const loadRankingsButton = document.getElementById("load-rankings");
const rankingsEl = document.getElementById("rankings");

const chartEl = document.getElementById("chart");

apiBaseEl.textContent = API_BASE;

const map = L.map("map", { zoomControl: true }).setView([25.033, 121.5654], 12);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 19,
  attribution: "&copy; OpenStreetMap contributors",
}).addTo(map);

const markers = L.layerGroup().addTo(map);

let segments = [];
let segmentsById = new Map();
let markerById = new Map();
let selectedSegmentId = null;

let corridors = [];
let corridorsById = new Map();
let selectedCorridorId = null;

function setStatus(text) {
  statusEl.textContent = text;
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

  const pad2 = (n) => String(n).padStart(2, "0");
  const toLocalInputValue = (dt) => {
    return `${dt.getFullYear()}-${pad2(dt.getMonth() + 1)}-${pad2(dt.getDate())}T${pad2(
      dt.getHours()
    )}:${pad2(dt.getMinutes())}`;
  };

  if (!startEl.value) startEl.value = toLocalInputValue(start);
  if (!endEl.value) endEl.value = toLocalInputValue(end);
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

async function fetchJson(url) {
  const resp = await fetch(url, { headers: { accept: "application/json" } });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`${resp.status} ${resp.statusText}: ${text}`);
  }
  return await resp.json();
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
      line: { color: "#4cc9f0", width: 2 },
      yaxis: "y",
    },
    {
      x,
      y: volume,
      type: "bar",
      name: "Volume",
      marker: { color: "rgba(255, 255, 255, 0.25)" },
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
      line: { color: "rgba(255,255,255,0.55)", width: 1, dash: "dot" },
      yaxis: "y",
    });

    traces.push({
      x: anomalyX,
      y: anomalyY,
      type: "scatter",
      mode: "markers",
      name: "Anomaly",
      marker: { size: 7, color: "#ff4d6d", line: { width: 1, color: "rgba(0,0,0,0.25)" } },
      yaxis: "y",
    });
  }

  const layout = {
    title: {
      text: title || "Time Series",
      font: { size: 12, color: "rgba(255,255,255,0.85)" },
      x: 0.02,
    },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    margin: { l: 48, r: 48, t: 32, b: 34 },
    xaxis: {
      type: "date",
      gridcolor: "rgba(255,255,255,0.08)",
      tickfont: { color: "rgba(255,255,255,0.75)", size: 10 },
    },
    yaxis: {
      title: { text: "Speed (kph)", font: { size: 10, color: "rgba(255,255,255,0.75)" } },
      gridcolor: "rgba(255,255,255,0.08)",
      tickfont: { color: "rgba(255,255,255,0.75)", size: 10 },
    },
    yaxis2: {
      title: { text: "Volume", font: { size: 10, color: "rgba(255,255,255,0.75)" } },
      overlaying: "y",
      side: "right",
      tickfont: { color: "rgba(255,255,255,0.75)", size: 10 },
      showgrid: false,
    },
    legend: {
      orientation: "h",
      x: 0.02,
      y: 1.12,
      font: { size: 10, color: "rgba(255,255,255,0.75)" },
    },
  };

  Plotly.react(chartEl, traces, layout, { responsive: true, displayModeBar: false });
  setStatus(`Loaded ${points.length} points.`);
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

    const anomalies = await loadAnomalies(entity, range, minutes);
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
}

function selectCorridor(corridorId) {
  entityTypeEl.value = "corridor";
  selectedCorridorId = corridorId;
  const corridor = corridorsById.get(corridorId);
  updateCorridorInfo(corridor);
  corridorSelectEl.value = corridorId;
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
      color: "#4cc9f0",
      weight: 2,
      fillColor: "rgba(76, 201, 240, 0.6)",
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
    return;
  }

  for (const row of items) {
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
        selectCorridor(row.corridor_id);
        loadTimeseries();
      });
    } else {
      idEl.textContent = row.segment_id;
      el.addEventListener("click", () => {
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

  const range = getIsoRange();
  if (range) {
    url.searchParams.set("start", range.start);
    url.searchParams.set("end", range.end);
  }

  setStatus("Loading rankings...");
  try {
    const items = await fetchJson(url.toString());
    renderRankings(items, type);
    setStatus(`Loaded ${items.length} ranking rows.`);
  } catch (err) {
    rankingsEl.textContent = "Failed to load rankings.";
    setStatus(`Failed to load rankings: ${err.message}`);
  }
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
loadButton.addEventListener("click", loadTimeseries);
loadRankingsButton.addEventListener("click", loadRankings);

setDefaultTimeRange();
loadSegments();
loadCorridors();
