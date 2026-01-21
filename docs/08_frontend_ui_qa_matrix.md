# Frontend UI QA Matrix

This checklist is a lightweight “visual contract” for UI polish work. It is designed to catch regressions quickly while iterating on `web/styles.css` and small DOM changes.

## Pages
- `/explore/`
- `/timeseries/`
- `/events/`
- `/rankings/`
- `/pipeline/`
- `/overview/`

## States (check all pages)

### 1) Loaded (happy path)
- Content loads with no JS errors.
- Layout spacing is consistent (no overlapping panels/overlays).
- Primary actions are discoverable and reachable.

### 2) Empty (no items returned)
- Empty-state card appears (not raw text dumps).
- At least 1 “one-click fix” action is present (e.g. widen window, relax filters, reload).
- Empty state is visually consistent with other pages.

### 3) Filtered → Empty
- When search/filters hide everything:
  - Empty-state explains “no matches” (not “no data”).
  - Provide a “clear search / relax filters” action.

### 4) Error / Unavailable dataset
- Error state is readable and actionable.
- “Copy fix” / “open diagnostics” / “reload” actions are available where relevant.
- No UI elements overlap or break layout.

### 5) Selection state
- Selecting a row/marker produces a clear selected style.
- Selected style is consistent (outline/focus ring), and does not rely on color alone.

## Explore (`/explore/`) specifics
- Story overlay:
  - Default collapsed.
  - Collapsed view shows a short KPI summary.
  - Expand works by clicking the header area.
- Overlays do not clash:
  - Story (top-left), toolbar (top-right), next step (bottom-left) remain usable.
  - On narrow viewports, overlays reflow instead of overlapping.

## Time Series (`/timeseries/`) specifics
- Story KPIs show `Drop vs baseline` and `Coverage` when available.
- When no time series points exist:
  - Show empty-state in the KPI area with “Use 24h window” and “Min samples = 1” options.

## Events (`/events/`) specifics
- Story KPI shows `Linked hotspots` for selected event (loading / count / —).
- When no events match filters:
  - Show empty-state with “Relax filters / Clear search / Show all times”.
- When dataset missing/unavailable:
  - Show empty-state with “Copy events fix” and “Use 24h window”.

## Rankings (`/rankings/`) specifics
- Story KPI shows `Filtered (n/total)` and updates while typing in search.
- When search yields no rows:
  - Empty-state offers “Clear search”.

