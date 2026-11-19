# railway.gov.gr public API notes

Findings from inspecting the public, unauthenticated API endpoints that the website
https://railway.gov.gr uses. Observed on 2026-11-19, in the evening (Athens time). **None of
this is documented by the operator**: it is observed behaviour and may change or disappear
without notice. This is an unofficial description; the operator's terms of use have not been
checked here, so check them before relying on or redistributing any of it.

## 1. Summary of the API surface

| Endpoint | Method | Auth | Purpose |
|---|---|---|---|
| `/api/msd/geo` | GET | none | Static infrastructure map as GeoJSON |
| `/api/train-stream` | GET (SSE) | none | Live train positions, history, schedule "ghost" cards |
| `/api/public/schedule-board?date=YYYY-MM-DD` | GET | none | **Full day timetable with actual times and final delay** (section 4) |
| `/api/public/schedule-telemetry/stream` | GET (SSE) | none | **Schedule-keyed live telemetry, no coordinates** (section 5) |

Notes:
- Unknown or protected paths answer **307 -> `/auth/signin?callbackUrl=...`**. That is a
  catch-all, so a 307 does not mean the path exists. A real miss on a public path gives a
  404 HTML page.
- Other paths of the site require a login and are not used by these tools.
- Guessed paths that do not exist or are not public: `/api/trains`, `/api/stations`,
  `/api/schedules`, `/api/msd/schedules`, `/api/msd/stream`, `/api/train-stream/history`,
  `/api/train-stream/ghosts`, `/api/public/schedules`, `/api/public/stations`.

## 2. `GET /api/msd/geo` - static infrastructure

Returns one JSON document (`content-type: application/json`).

```json
{ "success": true,
  "data": { "type": "FeatureCollection", "features": [ ... ] },
  "meta": { "featureCount": 12009, "types": ["station", "signal", ...] } }
```

Errors: `{"error":"Invalid limit"}` (HTTP 200) or HTTP 400 for unparsable `limit`.

### 2.1 Query parameters

The parameter names are the ones the website's own map code sends.

| Parameter | Behaviour |
|---|---|
| `types` | Comma-separated (`,` or `%2C`) list of entity types. Default (server side) is all types. The frontend default is `station,track,crossing,signal,switch,tunnel,bridge` (no `dead_zone`). Unknown types give an empty result. |
| `limit` | **Per entity type**, not total. Default 1000. **Maximum 10000** (10001+ gives `Invalid limit`). `limit=0` or non-numeric gives HTTP 400. |
| `mode` | Frontend always sends `all`. The server accepts any value without error and behaves like `all`. No other meaningful value found. |
| `corridor` | Filters by corridor code (same codes as the stream, see 3.5). Verified: `KIATO` -> 23 stations + 448 tracks. Unknown code gives an empty result (frontend then retries without it). With no `types` given and a corridor set, only stations and tracks came back, so it may not cover every type. |
| `station_railway_type` | Filter on station `railway_type` (`station`, `halt`, `stop`). Only applied to stations. |
| `station_status` | Filter on station `status` (`active`, `disused`). Only applied to stations. |
| `bbox` | Not used by the frontend. `minLon,minLat,maxLon,maxLat` seems to filter point types only. A Thessaloniki box returned stations, signals, crossings and switches, but no tunnels, bridges or tracks. Unverified and undocumented. |

Ignored parameters (tested, no effect): `offset`, `skip`, `page`, `cursor` (always the
same first N features), `station_operator`, `track_*`, `crossing_*`, `switch_*`,
`signal_type`, `railway_type`, and `type`. Paging is not needed because one request with
`limit=10000` returns everything.

### 2.2 Feature types (full dataset, `mode=all&limit=10000`, 12,009 features)

| entityType | Geometry | Count | Properties (besides the common ones) |
|---|---|---|---|
| `station` | Point | 389 | `osm_id`, `railway_type` (station 181, halt 135, stop 73), `status` (active/disused), `operator` ("ΟΣΕ" or "Hellenic Train") |
| `signal` | Point | 450 | `osm_id`, `signal_type` (e.g. `main`) |
| `crossing` | Point | 1,988 | `osm_id`, `crossing_type` (e.g. `level_crossing`), `hasBarrier` (`yes`/`no`) |
| `switch` | Point | 2,477 | `osm_id`, `switch_type`, `isElectric` (bool) |
| `tunnel` | LineString | 353 | `osm_id`, `tunnel_type`, `length` (rarely present) |
| `bridge` | LineString | 1,031 | `osm_id`, `bridge_type` |
| `track` | LineString | 5,096 | `osm_id`, `railway_type` (rail 4,940 / narrow_gauge 153 / preserved 3), `status`, `electrified`, `usage`, `service`, `ref`, `railway_ref`, `line_ref`, `isOOS`, `outOfService` |
| `dead_zone` | Polygon | 225 | `zoneType` (e.g. `tunnel`), `severity`, `source` (e.g. `tunnel_seed`), `status` (e.g. `confirmed`). No `osm_id`. |

Common properties on every feature: `_id` (Mongo ObjectId), `featureId`
(`"<entityType>:<osm_id>"`), `entityType`, `name` (often just the osm_id when unnamed).

Origin: apparently an OpenStreetMap extract (`osm_id`) plus the site's own `dead_zone`
layer. The data is static, not live.

### 2.3 Links to the stream

- Stream `railwayId` = track `osm_id`.
- Stream `nextStationId` = station `_id`.
- Stream `ux.signal.inDeadZone` / `zoneType` relate to the `dead_zone` polygons.

## 3. `GET /api/train-stream` - live feed (Server-Sent Events)

`content-type: text/event-stream; charset=utf-8`, `cache-control: no-cache, no-transform`.
The server ends the connection **every 15 minutes** (observed: opened 21:30:32 UTC, closed 21:45:33 and 22:00:34); a client reconnects and receives the `trainHistory` event again. A few `:` comment (keep-alive) lines were seen. The frontend
opens it with `new EventSource(NEXT_PUBLIC_TRAIN_STREAM_SSE_URL || "/api/train-stream",
{withCredentials: true})` and sends no query parameters (`?corridor=` has no effect).

Frontend listeners: `trainHistory`, `trainPositionsUx`, `scheduleCards`, plus
`trainPositions` and `delayUpdates`. The last two were **not observed** in about 2
minutes of capture in total. They may be legacy or rare.

### 3.1 Rates and sizes (about 90 s capture)

| Event | Frequency | Size | Content |
|---|---|---|---|
| `trainHistory` | once, on connect | about 190 KB | recent trail per train |
| `trainPositionsUx` | about 0.86 per second (77 in 90 s) | about 30 KB | current position of every tracked train |
| `scheduleCards` | every 4-5 s (19 in 90 s) | about 13 KB | scheduled trains with no GPS ("ghosts") |

About 2.8 MB per 90 s raw (about 2.7 GB/day, about 19 GB/week). gzip ratio about 13:1.
About 25 trains per `trainPositionsUx` push in the evening. Daytime counts will be higher.
About 95% of trains carry a new `timestamp` on each push, so positions are really per-second.

### 3.2 `trainHistory`

`{ "history": { "<trainId>": [ point, ... ] } }`. In one capture: 29 trains, 561 points,
about 1 point per second per train. Point fields: `ts`, `receivedAt`, `lat`, `lng`,
`speed`, `course`, `hasCourse`, `trackHeading`, `trackDirection`, `railwayId`,
`alongTrackDistance`, `fxk`.

### 3.3 `trainPositionsUx`

`{ "positions": [ train, ... ] }`. Fields per train:

| Group | Fields |
|---|---|
| Identity | `id`, `trainId`, `name`, `trainNumber`, `trainType` (`passenger` / `intercity` / `unassigned`), `serviceType` (e.g. `Suburban`), `operationPattern` (e.g. `circuit_directional`), `circuitDirectional` |
| Schedule | `scheduleId`, `scheduleNumber` (format `<trainNo>-<YYYYMMDD>-<HHMM>`), `scheduleStatus` (`in_progress` / `scheduled` / null), `origin`, `destination`, `scheduledDeparture`, `scheduledArrival`, `delay`, `corridor`, `progress` |
| Next stop | `nextStationId`, `nextStation`, `distanceToNextStation_m`, `distanceToDestination_m`, `chainage_m` |
| Position | `lat`, `lng`, `altitude`, `speed` (km/h), `course`, `hasCourse`, `trackHeading`, `trackDirection`, `headingSource`, `railwayId`, `alongTrackDistance`, `approximate`, `timestamp` |
| Locomotive | `locomotiveNumber`, `locomotiveName`, `locomotiveStatus` |
| Quality | `signalStatus` (`ok` / `degraded` / `lost`), `signalLost` (rare), `watermarked`, `fxk`, `ux: {watermarked, delayMs, signal: {inDeadZone, zoneType}}` |

Observed behaviour (about 1,980 train observations, evening sample):
- `unassigned` trains (locomotives with no schedule) have all schedule fields null.
  Assigned trains (`passenger`, `intercity`) carry the schedule. About 59% of
  observations had a `scheduleId`.
- `delay` was **0 in every observation** (1,980 positions and 437 ghost cards). It may
  be unpopulated, or just zero in this sample. This must be checked over a longer
  period before relying on it.
- `distanceToNextStation_m` was **always null**. `nextStation` was set in about 69% of
  observations. `progress` was set in about 59%. `chainage_m` in about 98%.
- `signalStatus`: `ok` 94.6%, `lost` 4.7%, `degraded` 0.7%.
- `ux.delayMs` = 10000, which suggests positions are deliberately published about
  10 s late.
- `scheduledDeparture` and `scheduledArrival` are only for **origin and destination**.
  No intermediate stop times are present.

### 3.4 `scheduleCards`

`{ "ghosts": [ card, ... ] }`. About 99 cards per event in the first capture, so it
covers much more than the trains currently in `trainPositionsUx`. Fields: `id`
(`schedule:<scheduleId>`), `name`, `trainNumber`, `scheduleId`, `scheduleNumber`,
`scheduleStatus`, `origin`, `destination`, `scheduledDeparture`, `scheduledArrival`,
`serviceType`, `category` (Greek, e.g. "Προαστιακός"), `delay`, `corridor`, `ghost: true`,
`gpsStatus` (`none` in all observations), `signalStatus`, `lat`, `lng`, `speed`, `course`
(all null), `progress`, `timestamp`.

Timestamps are ISO-8601 UTC (Z). Athens is UTC+2 in winter, UTC+3 in summer.

### 3.5 Corridor codes seen

`KIATO`, `PIRAIR`, `CHALKIDA`, `THESSER`, `E85`, `THESLAR`, `THESFLOR`, `ALEXOR`,
`KIAAIGIO`. These are the same codes `/api/msd/geo?corridor=` accepts.

## 4. `GET /api/public/schedule-board?date=YYYY-MM-DD` - timetable and punctuality

Public, plain JSON:
`{ "schedules": [ ... ] }` with no wrapper and no `meta`. Missing or unknown dates
behave as today. `date=garbage` gives `{"error":"Invalid date"}`.

### 4.1 Parameters

| Parameter | Behaviour |
|---|---|
| `date` | Athens local day (a day starts at `T22:00Z` or `T21:00Z` the previous UTC day). Defaults to today. |
| `corridor` | Works (`PIRAIR` -> 106 of 234, `KIATO` -> 36). |
| `serviceType` | Works (`InterCity` -> 4). |
| `status`, `trainNumber`, `limit` | Ignored (still 234). |

### 4.2 Retention (key finding)

- **History is available far back.** 2026-11-12 and 2026-11-18 return full days, all
  `completed`, with actual times. Stepping back 7 days at a time, data existed until
  about 287 days back, and 2026-01-29 (294 days back) was empty. The exact earliest day
  was not pinned down.
- **Future days are available** as `scheduled` (checked 2026-11-20, 2026-11-26 and
  2026-12-19). Day count varies: 234 on most days, 184 on 2026-11-21 and 2026-12-19
  (likely weekends).

So punctuality at the terminals can be backfilled for the past months without
recording anything, and the timetable for coming days is known in advance.

### 4.3 Schedule fields (234 schedules for 2026-11-19)

| Field | Notes |
|---|---|
| `_id`, `scheduleNumber` | `scheduleNumber` = `<trainNumber>-<YYYYMMDD>-<HHMM>`, the same as in the stream. `_id` is the stream's `scheduleId`. |
| `trainNumber`, `serviceType`, `corridor` | `serviceType`: Suburban 210, Regional 16, InterCity 4, Freight 4. Corridors as in 3.5, plus `ATHLIA`. |
| `status` | `completed` 185, `scheduled` 29, `in_progress` 18, `cancelled` 2 (on 2026-11-19 at about 20:08 Athens time) |
| `scheduledDeparture`, `scheduledArrival` | ISO-8601 UTC |
| `actualDeparture`, `actualArrival` | ISO-8601 UTC, second precision. Present (non-null) on 181 and 185 of 234, so not on every schedule. |
| `delay` | Integer **minutes**, 0-21 on completed trains. About 167 of 185 completed trains had a non-zero delay. |
| `origin`, `destination` | `{name, nameGreek}` |
| `routeStations[]` | `{stationId, name, nameGreek, role, scheduledArrival, scheduledDeparture, index}`. `role`: `origin` / `stop` / `destination`. `stationId` = station `_id` from `/api/msd/geo`. 2 to 20 stops per route. |
| `routeAdjusted`, `segment` | `false` and `null` for every schedule seen. Meaning unknown. |

### 4.4 What this gives and what it does not

- `delay` equals `actualArrival - scheduledArrival` in minutes (within 1 minute for
  178 of 185 completed trains).
- It is only set once the train has completed. For `in_progress` and `scheduled` trains
  it is 0, which is probably why the stream's `delay` was always 0 in the live capture.
- Actual times exist **only at origin and destination**. There is no actual time for
  intermediate stops. Those must be derived from stream positions near each station,
  and compared with the scheduled times in `routeStations`.
- 2 of 234 were `cancelled`. Not every `completed` schedule has `actualDeparture`.
- The stream's `scheduleId` joins to the board's `_id`, and `routeStations[].stationId`
  joins to geo stations. So the live stream, the timetable and the map link up.

## 5. `GET /api/public/schedule-telemetry/stream` - schedule-keyed live telemetry (SSE)

Public, `text/event-stream`, closed by the server every 15 minutes like the other stream, a few `:` keep-alive
comments. About 0.87 MB per 60 s raw (about 31 KB gzipped, so about 28:1). Observed
about 60 s at about 20:10 Athens time, so counts are from one short evening sample.

Unlike `/api/train-stream`, rows are keyed by **schedule** (`scheduleId`), not by
locomotive, and **have no `lat`/`lng`/`course`/`altitude`**. They carry progress along
the route instead. Every event is `{ "stream": "...", "telemetry": [ row, ... ],
"emittedAt": "<ISO>" }`.

### 5.1 Three event types (three views of the same data)

| Event name | `stream` | Events/60 s | Rows/event | Content |
|---|---|---|---|---|
| `scheduleTelemetry` | `ux` | 39 | about 18 | Schedules with live GPS (`source: live` 83%) plus `last_known` rows (17%). Likely the delayed "UX" view, like `trainPositionsUx`. |
| `scheduleTelemetryRaw` | `raw` | 17 | about 19 | Same 19 schedules as `ux`, only `source: live`, all `signalStatus: ok`. Likely the undelayed raw view. Emitted less often (about every 3-4 s). |
| `scheduleTelemetryGhosts` | `schedule` | 13 | about 24 | `last_known` (234 rows, all `signalStatus: lost`) and `source: schedule` (78 rows, `scheduled`, not yet running, progress 0). About 12 KB per event. |

Union over the 3 streams: 25 distinct schedules (19 live/`in_progress` on `ux`/`raw`, 24
on `ghosts`). All 25 exist in the `schedule-board` for the same date. The board had 18
`in_progress` at about the same moment.

**Verified by capturing both streams at the same time (about 75 s):**
- `scheduleTelemetry` (`ux`) is the **same data as `train-stream`'s `trainPositionsUx`**
  for schedule-bound trains. For 51 compared rows, `timestamp`, `progress`, speed,
  `nextStation` and `signalStatus` were identical. The only difference is naming and
  the missing coordinates.
- `ux` is **delayed by about 10 s**: `emittedAt - timestamp` was a median 10.0 s (`ux.delayMs`
  in `train-stream` is 10000).
- `scheduleTelemetryRaw` is the **undelayed** view: `emittedAt - timestamp` median 1.1 s, and
  its newest fix per schedule was a median 8.9 s newer than `ux` (0 to 12.8 s).
- `scheduleTelemetryGhosts` covers exactly the same 23 schedules as `train-stream`'s
  `scheduleCards` (23 of 23 in both directions).

### 5.2 Row fields

`source` (`live` / `last_known` / `schedule`), `scheduleId`, `scheduleNumber`,
`trainNumber`, `serviceType`, `corridor`, `operationPattern`, `scheduleStatus`
(`in_progress` / `scheduled`), `nextStationId`, `nextStation`, `delay`, `progress` (0-1),
`speedKmh`, `distanceToNextStationMeters`, `distanceToDestinationMeters`,
`distanceSource` (`chainage`), `signalStatus` (`ok` / `degraded` / `lost`), `fixOk`,
`approximate`, `timestamp` (position time), `updatedAt` (server time), `lastKnown`,
`lastKnownAt`.

Notes from the sample:
- `source: schedule` rows (not yet running) only have identity, `delay`, `progress`
  (0), `timestamp`, `updatedAt`, `lastKnown`, `lastKnownAt`. The other fields are absent.
- `last_known` means the GPS signal was lost (`fixOk: false`, `signalStatus: lost`).
  `timestamp` and `lastKnownAt` hold the time of the last good fix, so they can be
  minutes old. The row keeps being re-emitted with a fresh `updatedAt`.
- `distanceToNextStationMeters` was null in all 1,368 live rows, as in the other stream.
  `nextStation` is filled in.
- `delay` was 0 in every row (all three event types), again. This stream also looks to
  carry no live delay, at least in this sample.
- One schedule (`520-20261119-1958`) had no `distanceSource` in every `ux` row.
- Per-schedule `timestamp` changed on every event for `ux` (39 distinct values in 60 s)
  and `raw` (17 in 60 s), so rows are refreshed on every event, not only on new fixes.
- Field names differ from `train-stream` (`speedKmh` vs `speed`,
  `distanceToDestinationMeters` vs `distanceToDestination_m`).

### 5.3 How it relates to the other APIs

| | `train-stream` | `schedule-telemetry/stream` |
|---|---|---|
| Key | locomotive/`trainId` (and `scheduleId` when assigned) | `scheduleId` only |
| Position | `lat`, `lng`, `course`, `altitude`, `railwayId`, `alongTrackDistance` | none, only `progress` and distances |
| Unassigned locomotives | yes (no schedule) | no |
| Scheduled but not started | `scheduleCards` ghosts | `source: schedule` rows |
| Joins | `scheduleId` = board `_id`, `nextStationId` = geo station `_id` | same |

Conclusion: `train-stream` = coordinates plus locomotives without a schedule, with all
positions delayed by 10 s. `schedule-telemetry/stream` = no coordinates, but the same
schedule data both delayed (`ux`) and **undelayed (`raw`)**, plus explicit `last_known`
handling. Nothing in either stream carried a non-zero `delay` yet.

## 6. Open questions

1. Does `delay` ever become non-zero for a running train, in either stream? It was 0 in
   every live row of both streams. Is `delayUpdates` (train-stream) the event that carries
   it? Needs a longer capture, ideally covering a day with delays.
   Also: do `ux` and `raw` in `schedule-telemetry` ever differ?
2. What do `delayUpdates` and `trainPositions` carry, and when do they fire?
3. How far back does `schedule-board` go exactly? When is a past day finalised?
   Are `actualDeparture` and `actualArrival` ever revised afterwards?
4. What do `routeAdjusted` and `segment` mean? Are there other endpoints under
   `/api/public/` (e.g. a per-train detail)?
5. Does `bbox` work for line features, and what other `mode` values exist?
6. Terms of use for these APIs are unchecked. Check them before long-term or shared
   use, and especially before bulk-downloading 9+ months of history.
