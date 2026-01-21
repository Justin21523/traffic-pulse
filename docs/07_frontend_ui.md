# Frontend UI Style Guide (TrafficPulse)

## Goals
- Map-first dashboard UI with strong readability.
- Consistent “deck-like” presentation for demos (especially in `theme-policy`).
- Small, incremental changes (avoid large refactors and new dependencies).

## Themes
- `theme-product`: higher contrast, more “product” feel.
- `theme-policy`: calmer palette, more “deck” feel.

All colors should come from CSS tokens (variables). Prefer tokens over hard-coded `rgba(...)`.

## Design Tokens
Tokens live in `web/styles.css` under `:root` and are overridden by `body.theme-policy`.

### Color & Surfaces
- `--bg`, `--bg-2`: page background layers
- `--panel`, `--panel-2`: primary surfaces
- `--panel-border`: default border
- `--text`, `--muted`, `--muted-2`: text colors
- `--accent`, `--accent-2`, `--danger`, `--warn`: semantic accents

### Typography
- Base font is `ui-sans-serif` (policy mode may use serif for title only).
- Use `.mono` for metrics and IDs.
- Prefer tokenized sizes: `--font-sm`, `--font-md`, `--font-lg`.

### Radius, Shadows, Motion
- `--radius`, `--radius-sm`
- `--shadow`, `--shadow-sm`
- Motion should respect `prefers-reduced-motion`.

### Z-Index Layers
Keep map overlays predictable:
- `--z-map`: map content
- `--z-overlay`: overlays (story, map toolbar, next step)
- `--z-dialog`: modal overlays

## Component Rules
### Buttons
- `.button` is primary; `.button.secondary` is neutral.
- Provide `:hover`, `:active`, and `:focus-visible` states via tokens.

### Cards / Panels
- Cards and panels should share border/shadow/radius tokens.
- Empty/notice states should use `.empty-state` and `.hint-box`.

### Explore Story Overlay
- Default to collapsed for a cleaner map.
- Collapsed state shows a single-line summary and an explicit expand affordance.
- Avoid overlap with map toolbar and next-step card via safe zones and z-index tokens.

## QA States (per page)
For each page, check:
- Normal (loaded)
- Empty (no items returned)
- Error (fetch failed / dataset missing)
- Filtered (search or toggles hide all items)
- Selection state (segment/event selected)

