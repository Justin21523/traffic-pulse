const fs = require("node:fs");
const path = require("node:path");
const { test, expect } = require("@playwright/test");

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

async function installOfflineStubs(page) {
  await page.addInitScript(() => {
    if (!window.Plotly) {
      window.Plotly = {
        newPlot: async () => {},
        react: async () => {},
        purge: () => {},
        Plots: { resize: () => {} },
      };
    }

    if (!window.L) {
      const noop = () => {};
      const makeLayerGroup = () => ({
        addTo: () => makeLayerGroup(),
        clearLayers: noop,
        addLayer: noop,
        removeLayer: noop,
      });

      const makeMarker = (latlng) => ({
        addTo: () => makeMarker(latlng),
        bindPopup: noop,
        on: noop,
        setStyle: noop,
        setRadius: noop,
        getLatLng: () => ({ lat: latlng[0], lng: latlng[1] }),
        openPopup: noop,
      });

      const makeBounds = (a, b) => {
        const sw = { lat: a[0], lng: a[1] };
        const ne = { lat: b[0], lng: b[1] };
        return {
          extend: () => makeBounds(a, b),
          pad: () => makeBounds(a, b),
          getWest: () => sw.lng,
          getSouth: () => sw.lat,
          getEast: () => ne.lng,
          getNorth: () => ne.lat,
        };
      };

      const makeMap = () => ({
        setView: () => makeMap(),
        fitBounds: () => makeMap(),
        getZoom: () => 12,
        getBounds: () => makeBounds([25.0, 121.0], [25.1, 121.1]),
        createPane: () => ({ style: {} }),
        invalidateSize: noop,
        on: noop,
      });

      window.L = {
        map: () => makeMap(),
        tileLayer: () => ({ addTo: noop }),
        layerGroup: () => makeLayerGroup(),
        circleMarker: (latlng) => makeMarker(latlng),
        latLngBounds: (a, b) => makeBounds(a, b),
      };
    }
  });
}

const screenshotsDir = path.join(process.cwd(), "docs", "screenshots");
ensureDir(screenshotsDir);

const pages = [
  { name: "explore", url: "/explore/", waitFor: "#page-story" },
  { name: "timeseries", url: "/timeseries/", waitFor: "#page-story" },
  { name: "events", url: "/events/", waitFor: "#page-story" },
  { name: "rankings", url: "/rankings/", waitFor: "#page-story" },
  { name: "pipeline", url: "/pipeline/", waitFor: "#page-story" },
  { name: "overview", url: "/overview/", waitFor: "#overview" },
];

test.describe("UI screenshots", () => {
  for (const p of pages) {
    test(`capture ${p.name} (product)`, async ({ page }) => {
      await installOfflineStubs(page);
      await page.goto(p.url, { waitUntil: "domcontentloaded" });
      await expect(page.locator(p.waitFor)).toBeVisible();
      await page.waitForTimeout(350);
      await page.screenshot({ path: path.join(screenshotsDir, `${p.name}.png`), fullPage: true });
    });
  }

  test("capture explore (policy)", async ({ page }) => {
    await installOfflineStubs(page);
    await page.goto("/explore/?theme=policy", { waitUntil: "domcontentloaded" });
    await expect(page.locator("#page-story")).toBeVisible();
    await page.waitForTimeout(350);
    await page.screenshot({ path: path.join(screenshotsDir, "explore-policy.png"), fullPage: true });
  });
});

