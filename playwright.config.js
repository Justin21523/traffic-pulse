// @ts-check

const { defineConfig } = require("@playwright/test");

module.exports = defineConfig({
  testDir: "./tests/e2e",
  timeout: 30_000,
  expect: { timeout: 7_000 },
  fullyParallel: true,
  outputDir: "test-results",
  reporter: [["list"], ["html", { open: "never", outputFolder: "playwright-report" }]],
  use: {
    baseURL: "http://127.0.0.1:8003",
    browserName: "chromium",
    viewport: { width: 1440, height: 900 },
    launchOptions: {
      args: ["--no-sandbox"],
    },
  },
  webServer: {
    command: "python -m http.server 8003 --directory web",
    port: 8003,
    reuseExistingServer: !process.env.CI,
    timeout: 30_000,
  },
});
