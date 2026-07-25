# Plan: Historical time slider for the realtime CML map

**Date:** 2026-07-24
**Target branch:** fresh feature branch off `upstream/main`
**Merge path:** upstream PR → `upstream/main` → cherry-pick or merge into `ifu/main`

---

## Summary

Add a time slider to the realtime CML map (`/realtime`) that lets users pick any past
hour and see the map paths coloured by pre-materialized stats for that moment.  The
selected time is simultaneously reflected in the Grafana dashboard embedded below the
map: the iframe time range shifts to a ±30-minute window around the selection, and a
vertical annotation marker pins the exact chosen instant on the time-series panel.

**Architecture at a glance**

```
cml_data_1h (TimescaleDB 1h continuous aggregate, already exists)
      │
      ▼  hourly background job in parser (new)          is_provisional=FALSE (definitive)
      │
cml_stats (rolling-window, refreshed every 60 s, already exists)
      │
      ▼  every 60 s background job in parser (new)      is_provisional=TRUE  (current partial hour)
      │
      ├──► cml_stats_history(snapshot_time, cml_id, user_id, …, is_provisional)   ← new hypertable
      │
      ▼  GET /api/cml-stats?at=<iso>  (new query path in webserver)
realtime.html time slider  →  map path colours
      │
      ▼  contentWindow URL update (same pattern as var-cml_id today)
Grafana iframe  from/to  +  var-marker_time  →  panel time window + annotation pin
```

Key properties:
- **No raw-data scans for historical reads** — all stats come from `cml_stats_history`,
  which is populated from the `cml_data_1h` aggregate.  Reads are instant index lookups.
- **Current (partial) calendar hour is always reachable on the slider** — a provisional
  snapshot sourced directly from the live `cml_stats` rolling window is written to
  `cml_stats_history` every 60 s, marked `is_provisional = TRUE`.  The data is at most
  60 s stale.  When the calendar hour turns, `materialize_cml_stats_snapshot` overwrites
  the provisional row with a definitive one (`is_provisional = FALSE`) sourced from the
  fully-materialised `cml_data_1h` bucket.
- **Completeness semantics** — `completeness_percent_1h` is the fraction of non-null RSL
  records within a **rolling** 1-hour window ending at the time of writing.  This is the
  correct meaning for map colouring ("how healthy was data delivery in the last hour?")
  and is already correct in the live `cml_stats` today.  The provisional snapshot
  preserves this semantics; no calendar-based normalisation is needed.
- **Live mode unchanged** — omitting `?at=` still returns pre-computed `cml_stats` rows
  (the existing windowed stats), with the same auto-refresh behaviour as today.
- **Grafana sync reuses existing mechanism** — the `contentWindow.location` approach
  already in use for `var-cml_id` is extended; no new iframe communication protocol
  is needed.

---

## PR 1 — Database + Parser: `cml_stats_history` table and hourly snapshot writer

### 1a. New migration: `cml_stats_history` + helper functions

File: `database/migrations/010_add_cml_stats_history.sql`

#### Table

```sql
CREATE TABLE IF NOT EXISTS cml_stats_history (
    snapshot_time             TIMESTAMPTZ NOT NULL,
    cml_id                    TEXT        NOT NULL,
    user_id                   TEXT        NOT NULL,
    completeness_percent_6h   REAL,
    total_records_6h          BIGINT,
    valid_records_6h          BIGINT,
    mean_rsl_6h               REAL,
    stddev_rsl_6h             REAL,
    completeness_percent_1h   REAL,
    mean_rsl_1h               REAL,
    stddev_rsl_1h             REAL,
    last_rsl                  REAL,
    is_provisional            BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (snapshot_time, cml_id, user_id)
);

SELECT create_hypertable(
    'cml_stats_history', 'snapshot_time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Compress old snapshots (>7 days) — they are never updated
ALTER TABLE cml_stats_history
    SET (timescaledb.compress,
         timescaledb.compress_segmentby = 'user_id, cml_id',
         timescaledb.compress_orderby   = 'snapshot_time DESC');

SELECT add_compression_policy('cml_stats_history', INTERVAL '7 days');
```

`snapshot_time` is truncated to the hour (the caller is responsible).  The primary key
guarantees idempotent re-materialization via `ON CONFLICT DO UPDATE`.

#### Materialization function

```sql
CREATE OR REPLACE FUNCTION materialize_cml_stats_snapshot(
    p_at_time   TIMESTAMPTZ,   -- must be hour-truncated by caller
    p_user_id   TEXT
) RETURNS INT                  -- number of rows upserted
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_rows INT;
BEGIN
    -- Validate caller is a known user
    IF NOT EXISTS (SELECT 1 FROM cml_metadata WHERE user_id = p_user_id LIMIT 1) THEN
        RAISE EXCEPTION 'Unknown user: %', p_user_id;
    END IF;

    INSERT INTO cml_stats_history (
        snapshot_time, cml_id, user_id,
        completeness_percent_6h, total_records_6h, valid_records_6h,
        mean_rsl_6h, stddev_rsl_6h,
        completeness_percent_1h, mean_rsl_1h, stddev_rsl_1h,
        last_rsl, is_provisional
    )
    SELECT
        p_at_time,
        m.cml_id,
        p_user_id,
        -- 6-hour window: sum the 6 hourly buckets ending at p_at_time
        ROUND(
            100.0 * SUM(h6.rsl_count)::numeric
                  / NULLIF(SUM(h6.record_count), 0),
            2) AS completeness_percent_6h,
        SUM(h6.record_count)                     AS total_records_6h,
        SUM(h6.rsl_count)                        AS valid_records_6h,
        ROUND(AVG(h6.rsl_avg)::numeric, 2)       AS mean_rsl_6h,
        ROUND(AVG(h6.rsl_stddev)::numeric, 2)    AS stddev_rsl_6h,   -- avg of hourly stddevs; good approximation
        -- 1-hour window: the single bucket containing p_at_time
        ROUND(
            100.0 * h1.rsl_count::numeric
                  / NULLIF(h1.record_count, 0),
            2) AS completeness_percent_1h,
        ROUND(h1.rsl_avg::numeric, 2)            AS mean_rsl_1h,
        ROUND(h1.rsl_stddev::numeric, 2)         AS stddev_rsl_1h,
        h1.rsl_max                               AS last_rsl,        -- best proxy from 1h agg
        FALSE                                    AS is_provisional   -- definitive row from cml_data_1h
    FROM cml_metadata m
    -- 6h subquery from the 1h continuous aggregate
    LEFT JOIN LATERAL (
        SELECT
            SUM(record_count)                          AS record_count,
            SUM(CASE WHEN rsl_max IS NOT NULL THEN record_count ELSE 0 END) AS rsl_count,
            AVG(rsl_avg)                               AS rsl_avg,
            AVG(rsl_max - rsl_min)                     AS rsl_stddev   -- proxy; replace if stddev col added to agg
        FROM cml_data_1h
        WHERE user_id    = p_user_id
          AND cml_id     = m.cml_id
          AND bucket     >= p_at_time - INTERVAL '6 hours'
          AND bucket     <  p_at_time
    ) h6 ON TRUE
    -- 1h subquery: the bucket immediately before p_at_time
    LEFT JOIN LATERAL (
        SELECT record_count, rsl_avg, rsl_max, rsl_min,
               (rsl_max - rsl_min) AS rsl_stddev   -- same proxy
        FROM cml_data_1h
        WHERE user_id = p_user_id
          AND cml_id  = m.cml_id
          AND bucket  = p_at_time - INTERVAL '1 hour'
    ) h1 ON TRUE
    WHERE m.user_id = p_user_id
    ON CONFLICT (snapshot_time, cml_id, user_id) DO UPDATE SET
        completeness_percent_6h = EXCLUDED.completeness_percent_6h,
        total_records_6h        = EXCLUDED.total_records_6h,
        valid_records_6h        = EXCLUDED.valid_records_6h,
        mean_rsl_6h             = EXCLUDED.mean_rsl_6h,
        stddev_rsl_6h           = EXCLUDED.stddev_rsl_6h,
        completeness_percent_1h = EXCLUDED.completeness_percent_1h,
        mean_rsl_1h             = EXCLUDED.mean_rsl_1h,
        stddev_rsl_1h           = EXCLUDED.stddev_rsl_1h,
        last_rsl                = EXCLUDED.last_rsl,
        is_provisional          = FALSE;   -- definitive row always clears the flag

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows;
END;
$$;
```

> **Note on `rsl_stddev` proxy:** `cml_data_1h` stores `rsl_min`/`rsl_max` but not
> `stddev`.  `rsl_max - rsl_min` is a rough approximation.  If more accuracy is needed,
> a follow-up can add a `stddev` column to `cml_data_1h` (requires dropping and
> re-creating the continuous aggregate).  For map colouring purposes the proxy is
> sufficient.

> **Note on `is_provisional`:** Rows written from `cml_data_1h` (completed calendar
> hours) have `is_provisional = FALSE`.  Rows written every 60 s for the current
> (incomplete) hour from live `cml_stats` have `is_provisional = TRUE`.  When the hour
> turns, `materialize_cml_stats_snapshot` overwrites the provisional row with a
> definitive one via the `ON CONFLICT … DO UPDATE` clause above, setting
> `is_provisional = FALSE`.

#### Read function (for webserver / Grafana)

```sql
CREATE OR REPLACE FUNCTION get_cml_stats_at(
    p_at_time   TIMESTAMPTZ,
    p_user_id   TEXT
) RETURNS TABLE (
    cml_id                   TEXT,
    completeness_percent_6h  REAL,
    total_records_6h         BIGINT,
    valid_records_6h         BIGINT,
    mean_rsl_6h              REAL,
    stddev_rsl_6h            REAL,
    completeness_percent_1h  REAL,
    mean_rsl_1h              REAL,
    stddev_rsl_1h            REAL,
    last_rsl                 REAL,
    is_provisional           BOOLEAN
) LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public
AS $$
    SELECT
        h.cml_id,
        h.completeness_percent_6h,
        h.total_records_6h,
        h.valid_records_6h,
        h.mean_rsl_6h,
        h.stddev_rsl_6h,
        h.completeness_percent_1h,
        h.mean_rsl_1h,
        h.stddev_rsl_1h,
        h.last_rsl,
        h.is_provisional
    FROM cml_stats_history h
    WHERE h.user_id       = p_user_id
      AND h.snapshot_time = date_trunc('hour', p_at_time)
    ORDER BY h.cml_id;
$$;
```

#### Grants

```sql
-- Writers (parser roles)
GRANT SELECT, INSERT, UPDATE ON cml_stats_history TO demo_openmrg, demo_orange_cameroun;
GRANT EXECUTE ON FUNCTION materialize_cml_stats_snapshot(TIMESTAMPTZ, TEXT)
    TO demo_openmrg, demo_orange_cameroun;

-- Readers (webserver, Grafana)
GRANT SELECT ON cml_stats_history TO webserver_role;
GRANT EXECUTE ON FUNCTION get_cml_stats_at(TIMESTAMPTZ, TEXT)
    TO webserver_role, demo_openmrg, demo_orange_cameroun;
```

New users added via `generate_config.py` must receive the same grants — update the
template in `scripts/generate_config.py` (same pattern as the existing
`update_cml_stats_windowed` grant).

### 1b. Parser: `write_stats_snapshot()` and `write_provisional_snapshot()` in `DBWriter`

`parser/db_writer.py` — add two public methods:

```python
def write_stats_snapshot(self, at_time: datetime) -> int:
    """Materialize a cml_stats_history snapshot for the given hour.

    Calls materialize_cml_stats_snapshot(date_trunc('hour', at_time), user_id).
    Returns the number of rows upserted.
    """
    at_hour = at_time.replace(minute=0, second=0, microsecond=0)
    cur = self.conn.cursor()
    try:
        cur.execute(
            "SELECT materialize_cml_stats_snapshot(%s::timestamptz, %s)",
            (at_hour, self.user_id),
        )
        rows = cur.fetchone()[0]
        self.conn.commit()
        logger.info("Materialized cml_stats_history snapshot at %s (%d rows)", at_hour, rows)
        return rows
    except Exception:
        try:
            self.conn.rollback()
        except Exception:
            pass
        logger.exception("Failed to write stats snapshot at %s", at_hour)
        raise
    finally:
        if cur and not cur.closed:
            cur.close()

def write_provisional_snapshot(self) -> int:
    """Copy live cml_stats rolling-window values into cml_stats_history for
    the current (incomplete) calendar hour, marked is_provisional=TRUE.

    Runs every 60 s alongside refresh_windowed_stats so the slider can always
    reach the current hour with data that is at most 60 s stale.  The
    ON CONFLICT DO UPDATE overwrites any previous provisional row for the same
    hour.  When the hour turns, write_stats_snapshot() writes a definitive row
    (is_provisional=FALSE) that permanently replaces this one.
    """
    from datetime import timezone
    current_hour = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
    cur = self.conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO cml_stats_history (
                snapshot_time, cml_id, user_id,
                completeness_percent_6h, total_records_6h, valid_records_6h,
                mean_rsl_6h, stddev_rsl_6h,
                completeness_percent_1h, mean_rsl_1h, stddev_rsl_1h,
                last_rsl, is_provisional
            )
            SELECT
                %s, cml_id, user_id,
                completeness_percent_6h, total_records_6h, valid_records_6h,
                mean_rsl_6h, stddev_rsl_6h,
                completeness_percent_1h, mean_rsl_1h, stddev_rsl_1h,
                last_rsl, TRUE
            FROM cml_stats
            WHERE user_id = %s
            ON CONFLICT (snapshot_time, cml_id, user_id) DO UPDATE SET
                completeness_percent_6h = EXCLUDED.completeness_percent_6h,
                total_records_6h        = EXCLUDED.total_records_6h,
                valid_records_6h        = EXCLUDED.valid_records_6h,
                mean_rsl_6h             = EXCLUDED.mean_rsl_6h,
                stddev_rsl_6h           = EXCLUDED.stddev_rsl_6h,
                completeness_percent_1h = EXCLUDED.completeness_percent_1h,
                mean_rsl_1h             = EXCLUDED.mean_rsl_1h,
                stddev_rsl_1h           = EXCLUDED.stddev_rsl_1h,
                last_rsl                = EXCLUDED.last_rsl,
                is_provisional          = TRUE   -- keep provisional until definitive row arrives
            WHERE cml_stats_history.is_provisional = TRUE  -- never overwrite a definitive row
            """,
            (current_hour, self.user_id),
        )
        rows = cur.rowcount
        self.conn.commit()
        logger.debug("Wrote provisional snapshot at %s (%d rows)", current_hour, rows)
        return rows
    except Exception:
        try:
            self.conn.rollback()
        except Exception:
            pass
        logger.exception("Failed to write provisional stats snapshot")
        raise
    finally:
        if cur and not cur.closed:
            cur.close()
```

### 1c. Parser: wire both snapshot methods into the stats background thread

`parser/entrypoints/sftp_push.py` — the `stats_loop` already fires every
`STATS_REFRESH_INTERVAL` seconds (default 60 s).  Two things happen on every tick:

1. `write_provisional_snapshot()` — always runs, cheaply copying `cml_stats` rows into
   `cml_stats_history` for the current hour.  The `WHERE is_provisional = TRUE` guard
   prevents overwriting a definitive row that was written in the same minute.
2. `write_stats_snapshot(now)` — runs only when the calendar hour turns, materialising
   a definitive row for the just-completed hour from `cml_data_1h`.

```python
# Inside stats_loop, after refresh_windowed_stats():
from datetime import timezone
now = datetime.now(tz=timezone.utc)
current_hour = now.replace(minute=0, second=0, microsecond=0)

# Always: keep the current-hour provisional snapshot fresh (at most 60 s stale)
stats_db.write_provisional_snapshot()

# Once per hour: materialise the just-completed hour from cml_data_1h
if current_hour != last_snapshot_hour:
    stats_db.write_stats_snapshot(now)
    last_snapshot_hour = current_hour
```

`last_snapshot_hour` is initialised to `None` so the first tick always materialises a
definitive snapshot for the current hour-boundary on startup.

### 1d. Backfill script

A standalone script `parser/backfill_stats_history.py` (or a CLI flag
`--backfill-stats-history` on `parse_netcdf_archive.py`) that:

1. Queries `SELECT DISTINCT date_trunc('hour', bucket) FROM cml_data_1h WHERE user_id = ?` to find all available hours.
2. For each hour (oldest-first), calls `write_stats_snapshot(hour)`.
3. Logs progress every 100 hours.

Run once after the migration is applied.

---

## PR 2 — Webserver + Frontend: time slider and Grafana sync

Depends on PR 1 being merged (migration 010 applied).

### 2a. Webserver: optional `?at=` parameter on `/api/cml-stats`

`webserver/main.py` — extend `api_cml_stats()`:

```python
@app.route("/api/cml-stats")
@login_required
def api_cml_stats():
    at_param = request.args.get("at")   # ISO 8601 string or absent

    try:
        with user_db_scope(current_user.id) as conn:
            cur = conn.cursor()

            if at_param:
                # Historical: parse the timestamp and query cml_stats_history
                try:
                    at_ts = datetime.fromisoformat(at_param.replace("Z", "+00:00"))
                except ValueError:
                    return jsonify({"error": "invalid 'at' parameter"}), 400

                cur.execute(
                    "SELECT * FROM get_cml_stats_at(%s::timestamptz, %s)",
                    (at_ts, current_user.id),
                )
            else:
                # Live: existing query from cml_stats (windowed, pre-computed)
                cur.execute(
                    """
                    SELECT cml_id::text,
                           completeness_percent_6h, total_records_6h, valid_records_6h,
                           mean_rsl_6h, stddev_rsl_6h,
                           completeness_percent_1h, stddev_rsl_1h, last_rsl
                    FROM cml_stats
                    ORDER BY cml_id
                    """
                )

            data = cur.fetchall()
            cur.close()

        # Response shape is identical in both branches; is_provisional only present for ?at= path
        stats = [
            {
                "cml_id":                   str(row[0]),
                "completeness_percent":     safe_float(row[1]),
                "total_records":            int(row[2] or 0),
                "valid_records":            int(row[3] or 0),
                "mean_rsl":                 safe_float(row[4]),
                "stddev_rsl":               safe_float(row[5]),
                "completeness_percent_1h":  safe_float(row[6]),
                "stddev_last_60min":        safe_float(row[7]),
                "last_rsl":                 safe_float(row[8]),
                "is_provisional":           bool(row[9]) if at_param and len(row) > 9 else False,
            }
            for row in data
        ]
        return jsonify(stats)
    except Exception as e:
        print(f"Error fetching CML stats: {e}")
        return jsonify([])
```

Add a `/api/cml-stats-time-range` endpoint so the frontend can configure the slider's
min/max.  The `max` is always the current server hour (a provisional snapshot for it
is always present); only `min` requires a DB query:

```python
@app.route("/api/cml-stats-time-range")
@login_required
def api_cml_stats_time_range():
    from datetime import timezone
    now = datetime.now(tz=timezone.utc)
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    try:
        with user_db_scope(current_user.id) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT MIN(snapshot_time) FROM cml_stats_history WHERE user_id = %s",
                (current_user.id,),
            )
            row = cur.fetchone()
            cur.close()
        min_ts = row[0].isoformat() if row and row[0] else None
        return jsonify({"min": min_ts, "max": current_hour.isoformat()})
    except Exception as e:
        print(f"Error fetching stats time range: {e}")
        return jsonify({"min": None, "max": current_hour.isoformat()})
```

### 2b. Frontend: time slider in `realtime.html`

#### State variables (add to the existing JS block)

```js
let selectedTime = null;      // null = live mode
let statsRefreshTimer = null; // auto-refresh handle
const LIVE_REFRESH_MS = 30000;
```

#### Replace the existing one-shot `/api/cml-stats` fetch with a function

```js
function fetchAndApplyStats(atIso) {
    var url = '/api/cml-stats';
    if (atIso) url += '?at=' + encodeURIComponent(atIso);
    fetch(url)
        .then(r => r.json())
        .then(stats => {
            // Surface provisional state in the time display label
            var anyProvisional = stats.some(function(s) { return s.is_provisional; });
            var disp = document.getElementById('timeDisplay');
            if (atIso && anyProvisional && disp && !disp.textContent.includes('(partial)')) {
                disp.textContent += ' (partial)';
            }
            stats.forEach(function(stat) {
                cmlStats[stat.cml_id] = stat;
                applyStatsToLine(stat.cml_id, stat);
            });
        })
        .catch(function(err) { console.error('Stats fetch error:', err); });
}
```

#### Extend the `ColorControl` Leaflet control with a time slider section

```js
// Inside ColorControl.onAdd(), after the existing "Color by:" select:

var divider = L.DomUtil.create('hr', '', container);
divider.style.margin = '8px 0';

var timeLabel = L.DomUtil.create('div', '', container);
timeLabel.innerHTML = '<strong>Time:</strong>';
timeLabel.style.marginBottom = '4px';

var liveBtn = L.DomUtil.create('button', '', container);
liveBtn.id = 'timeLiveBtn';
liveBtn.textContent = '● Live';
liveBtn.style.cssText = 'width:100%;margin-bottom:4px;background:#22c55e;color:white;border:none;border-radius:4px;padding:3px 6px;cursor:pointer;';

var timeSlider = L.DomUtil.create('input', '', container);
timeSlider.type = 'range';
timeSlider.id = 'timeSlider';
timeSlider.style.cssText = 'width:100%;margin:4px 0;';
timeSlider.disabled = true;   // enabled once time range is loaded

var timeDisplay = L.DomUtil.create('div', '', container);
timeDisplay.id = 'timeDisplay';
timeDisplay.style.cssText = 'font-size:11px;text-align:center;color:#555;';
timeDisplay.textContent = 'Loading range…';

L.DomEvent.disableClickPropagation(container);
L.DomEvent.disableScrollPropagation(container);
```

#### Time range loader (call from `initializeMap()` after `loadCmlData()`)

```js
function initTimeSlider() {
    fetch('/api/cml-stats-time-range')
        .then(r => r.json())
        .then(range => {
            if (!range.min || !range.max) {
                document.getElementById('timeDisplay').textContent = 'No history';
                return;
            }
            var minEpoch = Math.floor(new Date(range.min).getTime() / 1000);
            var maxEpoch = Math.floor(new Date(range.max).getTime() / 1000);
            var slider   = document.getElementById('timeSlider');
            slider.min   = minEpoch;
            slider.max   = maxEpoch;
            slider.step  = 3600;   // 1-hour steps
            slider.value = maxEpoch;
            slider.disabled = false;
            document.getElementById('timeDisplay').textContent = 'Live';
        })
        .catch(() => {
            document.getElementById('timeDisplay').textContent = 'Unavailable';
        });
}
```

#### Slider event handlers

```js
// Live button
document.getElementById('timeLiveBtn').addEventListener('click', function () {
    selectedTime = null;
    document.getElementById('timeDisplay').textContent = 'Live';
    this.style.background = '#22c55e';
    clearTimeout(statsRefreshTimer);
    fetchAndApplyStats(null);
    startLiveRefresh();
    syncGrafanaTime(null);
});

// Slider drag (debounced 300 ms)
var sliderDebounce;
document.getElementById('timeSlider').addEventListener('input', function () {
    clearTimeout(sliderDebounce);
    var epochSec = parseInt(this.value, 10);
    var dt = new Date(epochSec * 1000);
    // Check whether this is the current (partial) hour
    var nowHour = new Date(); nowHour.setMinutes(0, 0, 0, 0);
    var sliderHour = new Date(dt); sliderHour.setMinutes(0, 0, 0, 0);
    var label = dt.toISOString().replace('T', ' ').slice(0, 16) + ' UTC';
    if (sliderHour.getTime() >= nowHour.getTime()) label += ' (partial)';
    document.getElementById('timeDisplay').textContent = label;
    sliderDebounce = setTimeout(function () {
        selectedTime = dt.toISOString();
        stopLiveRefresh();
        document.getElementById('timeLiveBtn').style.background = '#aaa';
        fetchAndApplyStats(selectedTime);   // provisional snapshot handles current hour
        syncGrafanaTime(epochSec);
    }, 300);
});

function startLiveRefresh() {
    stopLiveRefresh();
    statsRefreshTimer = setInterval(function () { fetchAndApplyStats(null); }, LIVE_REFRESH_MS);
}
function stopLiveRefresh() {
    if (statsRefreshTimer) clearInterval(statsRefreshTimer);
    statsRefreshTimer = null;
}
```

Start live refresh at the end of `initializeMap()` (after `loadCmlData()`):

```js
fetchAndApplyStats(null);
startLiveRefresh();
initTimeSlider();
```

### 2c. Grafana iframe sync

```js
function syncGrafanaTime(epochSec) {
    var grafanaPanel = document.getElementById('grafana-panel');
    var iframeWindow = grafanaPanel.contentWindow;
    try {
        var currentUrl = new URL(iframeWindow.location.href);
        if (epochSec === null) {
            // Live: restore relative time range
            currentUrl.searchParams.set('from', 'now-1h');
            currentUrl.searchParams.set('to',   'now');
            currentUrl.searchParams.delete('var-marker_time');
        } else {
            // Historical: ±30 min window around selected second
            var halfWindow = 30 * 60 * 1000;    // 30 min in ms
            var epochMs = epochSec * 1000;
            currentUrl.searchParams.set('from', String(epochMs - halfWindow));
            currentUrl.searchParams.set('to',   String(epochMs + halfWindow));
            currentUrl.searchParams.set('var-marker_time', String(epochMs));
        }
        iframeWindow.history.pushState(null, '', currentUrl.toString());
        iframeWindow.dispatchEvent(new PopStateEvent('popstate', { state: null }));
    } catch (e) {
        console.warn('Grafana sync skipped (iframe not ready):', e);
    }
}
```

### 2d. Grafana dashboard: `$marker_time` variable + annotation

Edit `grafana/provisioning/dashboards/definitions/cml-realtime.json`:

1. **Add template variable** (`templating.list`):
```json
{
  "name":  "marker_time",
  "type":  "textbox",
  "label": "Marker time (epoch ms)",
  "hide":  2,
  "current": { "value": "" }
}
```

2. **Add annotation** (`annotations.list`):
```json
{
  "name":       "Selected time",
  "enable":     true,
  "hide":       false,
  "iconColor":  "red",
  "type":       "dashboard",
  "builtIn":    0,
  "datasource": { "type": "grafana", "uid": "-- Grafana --" },
  "rawQuery":   true,
  "query":      "SELECT $marker_time AS time, 'selected' AS text",
  "timeField":  "time"
}
```

> Grafana 13's built-in "Grafana" datasource can evaluate a constant timestamp as an
> annotation without a DB query.  If the variable is empty the annotation is silently
> suppressed.

---

## Testing notes

- **Unit tests for `write_stats_snapshot`** follow the same pattern as
  `test_refresh_windowed_stats_commits_on_success` in
  `parser/tests/test_db_writer.py`.
- **Unit test for `api_cml_stats` with `?at=`** follows the pattern in
  `webserver/tests/test_api_cml_stats.py` — mock `user_db_scope`, assert the SQL
  contains `get_cml_stats_at`.
- **Integration smoke test**: run `psql … -c "SELECT materialize_cml_stats_snapshot(date_trunc('hour', now()), 'demo_openmrg')"` and confirm rows appear in `cml_stats_history`.

## Open questions / future work

- Add `rsl_stddev` as a proper column to `cml_data_1h` (requires dropping and
  re-creating the continuous aggregate) to replace the `rsl_max - rsl_min` proxy.
- Consider adding `@grafana/iframe-api` bidirectional sync (Grafana 13 native) to
  make the slider follow when the user zooms inside Grafana — not required for the
  initial feature but a natural follow-up.
- Retention policy for `cml_stats_history`: the compression policy (7 days) keeps
  storage small; consider an explicit `drop_chunks` policy if long-term history is
  not needed.
