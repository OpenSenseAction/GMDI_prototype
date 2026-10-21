# UI/UX improvement backlog

Suggestions collected from users. Items marked **done** ship in `feat/map-ux-improvements`; the rest are open.

Effort scale: **S** = small, **M** = medium, **L** = large.

## Done

| Source | Item |
|---|---|
| user1 #1 | Easier line selection on map (canvas renderer, 8 px click tolerance) |
| user1 #6 | Map scale bar |
| user1 #3 (map part), user2 | Length, frequency and polarization in the map popup |
| user2 | Default Grafana refresh set to 1 min |

## Open

### 1. Zoom to and highlight the CML chosen in the dropdown (user1 #2)

- **Done:** choosing a CML in the dropdown opens its map popup (with metadata), panning the map only if the line is out of view. The iframe URL is polled every 500 ms in `watchGrafanaSelection()`.
- **Remaining:** a stronger highlight of the selected line, always zooming to it, and the same behaviour in `admin_realtime.html` (key is `provider|cml_id`).
- **Problem:** the CML list is the Grafana `cml_id` variable inside the iframe, so the parent page is never told when it changes.
- **To do (remaining part):**
  - Give the selected polyline a thicker or highlighted style and reset the previous one.
  - Optionally zoom with `map.fitBounds()` instead of only panning.
  - Mirror in the admin page.
- **Effort:** S.

### 2. Show length and frequency band above the graph (user1 #3)

- **To do:**
  - Option A: add a Grafana text/stat panel or change the panel title using template variables backed by a `cml_metadata` query (`length`, `frequency`, `polarization` for `$cml_id`).
  - Option B: render a small header bar in `realtime.html` above the iframe and fill it from `cmlMeta` when a CML is selected (no Grafana change; pairs with item 1).
  - The iframe uses `viewPanel=2&kiosk`, so a Grafana-side change only shows up if it is part of panel 2's title. Option B is simpler.
- **Effort:** S (option B) / M (option A).

### 3. Compare two CMLs (user1 #4)

- **To do:**
  - Make the Grafana `cml_id` variable multi-value and let the panel query return one series per CML (colour-coded), or add a second iframe stacked below for a split view.
  - Add map UI to pick a second CML (e.g. shift+click) and a toggle for merged vs. split.
  - Make sure the time sync and the grey coloring-window annotation (`/api/coloring-annotation`) apply to both panels.
  - Update the dashboard provisioning JSON and the SQL used by the panel (`cml_data_secure`).
- **Effort:** L. Decide first on merged vs. split layout.

### 4. Valid-record counts for 1 day and 3 days in the popup (user1 #5)

- **To do:**
  - Add `total_records_1d`, `valid_records_1d`, `total_records_3d`, `valid_records_3d` to `cml_stats` and `cml_stats_history` (new migration in `database/migrations/`, plus `init.sql`).
  - Extend `update_cml_stats()` and `get_cml_stats_at()`. Keep RLS and `user_id` handling consistent.
  - Add the fields to `/api/cml-stats` (live and historical column order) and to `buildPopupText()`.
  - Update `parser/backfill_stats_history.py` and the tests for the stats API.
  - Check the cost of the 3 d window on `cml_data` (compressed chunks); consider computing from `cml_data_1h` instead.
- **Effort:** M. Database and API changes with a migration.

### 5. Search and display by link number (user2)

- **Request:** search by link number, i.e. IDs with `55` at the 3rd and 4th digit (e.g. `30655 1234`); IDs with `25` or `27` at that position are unwanted.
- **Needs clarification:** is the `55` ID a separate field from the current `cml_id`, or a transformation of it? Which IDs should be hidden or mapped?
- **To do:**
  - Add a search box to the map (filter or jump to a CML, zoom via item 1).
  - If the 55-number is a different identifier, add a column to `cml_metadata` (migration, parser mapping, `db_writer.py`) and show it in the popup and the search.
  - If only a filter is needed, filter or flag the list server-side in `get_available_cmls()` and `/api/cml-map`.
- **Effort:** S (search by existing ID) / M (new identifier column through the pipeline).

### 6. Sublink numbers (network element numbers with 55) (user2)

- **Needs clarification:** the source of these numbers. `cml_metadata.sublink_id` currently holds names such as `sublink_1`.
- **To do:** add a network-element ID per sublink in the metadata (migration, parser mappings, `db_writer.py`), then show it in the popup next to frequency and polarization and make it searchable (item 5).
- **Effort:** M. Depends on what the provider data contains.

### 7. Grey area for periods with possible rain-related outage (user2)

- **Needs clarification:** what defines such a period (a wet/dry flag from the processing, rain gauge or radar data, or a RSL-drop threshold)? Is it per CML or for the whole area?
- **To do:**
  - Define and compute the rain-event periods (processor service or SQL), and store them (table or Grafana annotations).
  - Draw them as shaded regions in the Grafana panel, e.g. via an annotation query on the dashboard, similar to the existing coloring-window annotation.
  - Optionally mark affected CMLs on the map.
- **Effort:** L. Mostly a data and algorithm question; the display part is S-M.

## Follow-ups on done items

- Confirm units in `cml_metadata`: the popup assumes `length` in metres and `frequency` in MHz (as in the archive CSV). The `other_mno_csv` parser maps `length_km` straight into `length`, so that source may be stored in km. Normalize in the parser if so. Effort: S.
- Show the same metadata in the admin map tooltip (`admin_realtime.html`, `/api/admin/cml-map`). Effort: S.
