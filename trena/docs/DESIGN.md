# Design notes: railway live-data collector

This document records how the collector works and **why** it is built that way. The API
facts it relies on are in `API_NOTES.md`. The code is in `collector/`, the tests in `tests/`
(standard library only).

## 1. Goal and scope

Record the live data of the Greek railway for a run of a few days to weeks, with the smallest
reasonable footprint, so that **punctuality can be reconstructed afterwards**:

- at origin and destination: from the timetable board (actual times are published),
- at intermediate stops: from position samples compared with the scheduled stop times.

Non-goals: real-time dashboards, per-second resolution, serving the data. Pulling old
timetable days is possible (the API keeps roughly nine months) and is what
`collector.board_daily` is for.

## 2. Decisions and reasons

| # | Decision | Reason |
|---|---|---|
| D1 | **Sample both streams once per minute.** | A one-minute error at a stop is acceptable for punctuality, and it keeps the database tiny. |
| D2 | **One persistent SSE connection per stream** (2 in total), sample in memory. | Reconnecting each minute would re-download the 190 KB `trainHistory` each time. The streams cannot be filtered or compressed (the server ignores `Accept-Encoding: gzip`, section 3.1), so the connection is the cost. |
| D3 | `/api/msd/geo` stations are loaded **once** (only if the `station` table is empty). A full copy of the dataset is saved at setup if none exists yet for that UTC day. | Static data. The full copy is reference material for later post-processing (tracks, signals). |
| D4 | In the live collector, `/api/public/schedule-board` is fetched **for the day before yesterday (D-2)**, from 03:00 Europe/Athens. | The Athens service day runs past midnight (latest arrival seen 08:44). D-2 is certainly complete, so every row is final: no provisional pass, no finality check. Cost: a 2-day lag. The standalone puller (D25) uses the exact finality time instead. |
| D5 | **Missed board days are caught up** from `start_date` to D-2. | A machine that was off must not lose a day silently. |
| D6 | **Database: SQLite**, WAL mode, one file. | Lightest option: no server, standard library, a week fits in a few MB. |
| D7 | **Keep unassigned locomotives** (rows without `scheduleId`, about 35% of rows). | They are fleet data, and dropping them would hide trains that run without a published schedule. |
| D8 | **Use both streams.** `train-stream` is primary. `schedule-telemetry/stream` adds only `delay_tel` and `src` to the same row. | Lets us compare both `delay` values over the week (both were 0 in every live row seen). Gives the explicit `last_known` marker. |
| D9 | **Keep a sampled raw archive** for a first run, delete it once the schema is validated. | Sampling discards data permanently; the archive allows re-parsing if a field was misread. |
| D10 | All times stored as **UTC epoch seconds**. Athens time is used only to compute board dates. | Avoids DST mistakes. |
| D11 | A position's time is the row's **own `timestamp`**, not the sampling time. | The stream is held back about 10 s; the row's own time is the true fix time. |
| D12 | **Stored `src` comes from the `ux` telemetry view, `delay_tel` from `raw`.** | Measured: for trains that lost GPS, `raw` keeps saying `live`/`ok` with a stale timestamp, only `ux` says `last_known`/`lost`. `raw` is the undelayed view, so it is preferred for `delay`. |
| D13 | **One writer thread.** Readers only update in-memory state and an outage queue. | No SQLite contention, no locks around the database, and one place where a failure is handled. |
| D14 | **No separate 03:00 trigger.** A board date becomes due through `effective_today()` (Athens date, minus one day before 03:00), and **at most one date is fetched per minute**. | Gives "03:00 for D-2" and the catch-up with the same code. A backlog (for example after downtime) drains at one date per minute and can never delay a position sample. |
| D15 | A failed board date is retried with **doubling delays (10 min, 20 min, ... up to 6 h)** and one ERROR log line at the 5th failure in a row. A day with **no usable schedule** (empty list, or none with an `_id`) **counts as a failure**. | Avoids hammering a failing server, yet makes a persistent problem loud. A real service day has about 180-234 schedules, so an empty answer is an upstream fault and must not mark the day as fetched forever (found in review, twice). |
| D16 | `start_date` is stored in table `meta` on the first start and **read back on every later start**. | A restart must not move the first board date forward and silently lose days. |
| D17 | **Graceful shutdown** on SIGTERM/SIGINT: stop readers, one final tick and archive write, bounded (120 s) board catch-up, WAL checkpoint, close. | A time limit such as `timeout 8d` ends the process with SIGTERM; the last minute of samples and all due board days must survive it. |
| D18 | **Standard library only** (`urllib`, `sqlite3`, `threading`, `gzip`, `zoneinfo`). | Nothing to install on the host, nothing to keep patched. |
| D19 | **All I/O is injectable** (connect, clock, sleep, fetch functions). | Every module is tested with fakes and no network; real sockets and the real servers were tested separately. |
| D20 | **One bad train row never costs the other trains' samples.** Each train is stored independently; a non-numeric field becomes NULL, an unusable timestamp skips that train only; `last_stored_ts` is updated only after the commit. | Found in review: one odd field in one row used to abort the whole tick and silently stop all storage until that train sent a good row. The API is undocumented and may drift. |
| D21 | **An outage is written to the database when it opens** (`end_ts` NULL) and closed on recovery. | A stream that never comes back (the last days of a run) used to leave no trace, so a trailing gap looked like "no trains ran". |
| D22 | **Read timeout equals the idle limit (30 s)**, and a timeout means "idle": reconnect. Any unexpected exception in the read loop also costs a reconnect, never the thread. The minute loop replaces a reader thread that has died. | Real-socket test: after one timeout a `urllib` response cannot be read again (`cannot read from timed out object`), so "poll every 5 s and continue" cannot work. A silently dead reader thread would mean a stream gone for the rest of the run. |
| D23 | **A schedule stub from the stream never modifies an existing `schedule` row.** | The board is authoritative; after a restart the stream used to overwrite its timetable columns. |
| D24 | **The last days of a run are pulled in one separate command**, `--board-catchup-only --through <last day>`, on the morning after the stop, refused while that day's board is not final (before 10:00 Athens the next day). | The D-2 rule means a run stopped on day N has timetables up to day N-2 only; the final two days must be fetched afterwards. The D-2 rule and the plain catch-up can never reach them. |
| D25 | **A standalone daily timetable puller**, `python -m collector.board_daily`. It stores every board day once that day is final (10:00 Athens the next day), **never ends unless `--until` sets a last day**, accepts a `--start-date` far in the past for a backfill, and has a `--once` mode for cron or a systemd timer. A hard floor of one pass per minute protects the server from any scheduling bug. | Some users only want the timetables, not the live streams. An open-ended default suits a daily job; an explicit last day makes a bounded export easy. Using the exact finality time instead of a fixed two-day lag gets each day one day earlier. |

## 3. Components (as built)

One process, `python -m collector`. Three kinds of thread:

| Piece | File | What it does |
|---|---|---|
| SSE parser | `sse.py` | Incremental: handles events split across chunks, `:` comments, CRLF, multi-line `data:`; an incomplete trailing event stays in the buffer. |
| Readers (2 threads) | `readers.py` | `SSEReader`: `urllib` connection with `Accept-Encoding: identity`, `read1(65536)` into the parser. Callback errors never kill the thread. **Watchdog:** 30 s read timeout; a timeout means idle and reconnects (reason `idle 30s`). EOF, any network error and any unexpected exception reconnect (D22). **Backoff** 1 s doubling to 60 s, reset after a connection that delivered an event. **Outage:** on loss it reports `(stream, lost_at, reason)` once (D21); when the next connection delivers its first event it reports `(stream, lost_at, recovered_at, reason)`, which closes the row. |
| In-memory state | `sampler.State` | Latest row per `trainId` (from `trainPositionsUx`), latest telemetry per `scheduleId` (raw wins, kept separately from the latest `ux` row, D12), latest event of each type for the archive. Lock-protected. Malformed JSON is logged and skipped. |
| Minute tick | `sampler.tick` | One transaction per minute, each train isolated (D20); see section 5. |
| Database | `db.py` | Schema, `load_stations`, `IdMaps` (Mongo id to small integer, cached; a schedule stub never modifies an existing row, D23). |
| Raw archive | `archive.py` | One JSON line per event, each line a **separate gzip member**, in `YYYY-MM-DD.jsonl.gz` (UTC day). Thread-safe; I/O errors are logged and never raised. |
| Board job | `board.py` | `fetch_board`, `store_board` (one transaction, idempotent; fails if no schedule was usable; injected clock), `due_dates`, `run_catchup`. |
| Wiring | `app.py` | `Collector`: setup, readers (with the dead-thread safety net), minute loop, board step, shutdown, `catchup_only(through=...)`. |
| Daily timetable puller | `board_daily.py` | Standalone: stores each board day once it is final, open-ended unless `--until` is given (D25). Reuses `board.py` and the same schema. |
| CLI | `__main__.py` | argparse (`--board-catchup-only`, `--through`, `--force`), UTC log format, rotating log file, signal handlers. |

Why these choices in the readers: the stream sends no `Content-Length` and the server ends it
every 15 minutes, so a reader must always be ready to reconnect, and the only way to notice a dead
connection is silence, hence the idle watchdog (D22). A one-second backoff start keeps outages short; the 60 s cap avoids
hammering. An outage is opened once per loss and closed only when the next event arrives,
which gives a true "recovered at" time, and several failed attempts in a row stay one
outage (the earliest loss time is kept).

Politeness: two long-lived connections plus one board request per minute at most while
catching up (normally one per day). Generic `User-Agent: trainose-collector/0.1`.

### 3.1 Traffic (measured)

Inbound about 2.8 MB/min (train-stream about 1.9, telemetry about 0.9), **about 4 GB a
day, about 32 GB over 8 days.** Both servers ignore `Accept-Encoding: gzip`, so this
cannot be reduced.

## 4. Schema (SQLite)

Integers throughout, for compact storage. Strings live only in dimension tables.

```
station(id, mongo_id UNIQUE, name, lat_e5, lng_e5, railway_type, status, operator)   -- 389 rows
train(id, mongo_id UNIQUE, train_type, locomotive_number, locomotive_name)
schedule(id, mongo_id UNIQUE, schedule_number, train_number, service_type, corridor,
         origin, destination, sched_dep, sched_arr,
         status, delay_min, actual_dep, actual_arr)       -- stub from the stream, completed by the board
schedule_stop(schedule_id, idx, station_id, role, sched_arr, sched_dep)   -- PK (schedule_id, idx), WITHOUT ROWID
position(train_id, ts, schedule_id, lat_e5, lng_e5, speed_x10, progress_x1e4,
         along_m, dest_m, next_station, delay, delay_tel, sig, src)       -- PK (train_id, ts), WITHOUT ROWID
board_fetch(date PK, fetched_at, n)
outage(stream, start_ts, end_ts, reason)                  -- end_ts NULL while the outage is open
meta(key PK, value)                                        -- start_date (D16)
```

Encodings: `lat_e5`/`lng_e5` = degrees x 1e5 (about 1.1 m), `speed_x10` = km/h x 10,
`progress_x1e4` = fraction x 10000, `sig` 0 ok / 1 degraded / 2 lost, `src` 0 live /
1 last_known / 2 schedule, NULL when no telemetry row. `ts` is the row's own timestamp in
UTC epoch **seconds** (D10, D11). `delay` is stored as received (unit unverified).

Why these columns: `along_m`, `dest_m` and `progress` allow passage detection without
recomputing geometry; `next_station` allows a cross-check; `delay` and `delay_tel` let us
test whether either stream ever carries a live delay. Dropped on purpose: altitude,
course, heading, track direction, `railwayId`, `chainage_m`, `watermarked`, `fxk`,
`approximate`, `ux`. They are in the raw archive if needed.

## 5. The minute tick

At second 0 of every UTC minute the writer thread calls `tick()`:

1. For every train in memory whose `timestamp` is newer than the last stored one for that
   train, insert one `position` row (`INSERT OR IGNORE`, one transaction for the whole tick).
2. Create the `train` and `schedule` dimension rows on first sight.
3. Look up telemetry: `delay_tel` from the raw/latest row, `src` from the `ux` row (D12).
4. Write the archive snapshot (five event types), drain the outage queue, check the reader
   threads, then run one board step.

**Isolation (D20).** Inside the tick every train is stored on its own: a field that is not a
finite number is stored as NULL with a warning, an unusable timestamp skips that train only,
and any other error in one row skips that train and the tick goes on. The in-memory
`last_stored_ts` is updated only after the transaction has committed, so a row that could not
be written is retried the next minute instead of being marked as stored. The four steps of
`minute_step` are also wrapped separately, so a failure in one never stops the others.

Why "skip, do not repeat": a train without a fresh fix produces no row, so a gap in
`position` honestly means "no new fix", and `sig`/`src` explain it when rows resume.
Duplicates are impossible: the primary key plus `INSERT OR IGNORE` makes restarts and
replays harmless, even if the in-memory `last_stored_ts` fails to reload.

Known property: `ts` has one-second granularity, so two fixes of one train within the
same second collapse into one row. Irrelevant at minute sampling.

## 6. Scheduled work

| Work | When | Detail |
|---|---|---|
| Station load | At setup, only if `station` is empty | `GET /api/msd/geo?types=station&limit=10000`, retried with backoff (5 s doubling to 60 s) for up to 10 minutes, then the process refuses to start. Stations must exist before the first tick, otherwise `next_station` would be cached as unknown for the whole run. |
| Full geo copy | At setup, if `geo_YYYYMMDD.json.gz` does not exist | `mode=all&limit=10000` (about 2.4 MB, about 0.9 MB gzipped). Best effort, never fatal. |
| Board step | Every minute, after the tick | At most one due date per call (D14), doubling retry delay (D15). A date is due from `start_date` to `effective_today - 2`. |
| Final catch-up | At shutdown | All remaining due dates (up to D-2), 2 s pause between requests, bounded to 120 s in total. |
| `collector.board_daily` | Daily (own process, or `--once` from a timer) | Every missing board day from `start_date` up to `--until` (or up to the last final day), one request at a time with a pause; a failed day is retried in an hour. |
| `--board-catchup-only` | Manually | Setup without readers, fetch all due dates (up to D-2), checkpoint, exit. |
| **Lone pull of the last days (D24)** | **Manually, the morning after the stop** | `--board-catchup-only --through <last data day>`: every date up to and including that day, so the two days the D-2 rule never reaches. **Refused** while the board of that day is not final, that is before **10:00 Athens time on the next day** (the latest arrival seen was 08:44); `--force` overrides. |

The D-2 lag (D4) means that a run stopped on day N has timetables for days up to N-2
only; the lone pull supplies N-1 and N. Day N's board is final only on day N+1 at 10:00.

## 7. Raw archive

Once per minute the latest event of each type (`trainPositionsUx`, `scheduleCards`,
`scheduleTelemetry`, `scheduleTelemetryRaw`, `scheduleTelemetryGhosts`) is appended as
`{"t","ev","data"}`, skipping an event whose payload object has not changed since the
last write; `trainHistory` is written once per connect.

Why a gzip member per line: a crash in the middle of a write can only damage the last
line, never the lines before it. The price is compression: **measured about 20 KB per
minute** (about 160 KB for 7 minutes including two 190 KB history events), so about
**30 MB a day, about 230 MB over 8 days**. A single member per minute would compress
better; not done, because the archive is temporary (D9) and the size is acceptable.

## 8. Size and cost

| Item | Measured or estimated |
|---|---|
| Position rows | 31-34 per minute in the evening dry run (about 33 trains); about 0.3-1 million for 8 days, depending on daytime load |
| `position` size | about 25-30 bytes per row: **about 10-35 MB** |
| Board data | about 230 schedules and about 3,400 stop rows per day; 2 days added 471 schedules and 6,836 stops in the dry run |
| Raw archive | about 20 KB/min, **about 230 MB** over 8 days (temporary) |
| Network | **about 32 GB inbound** over 8 days (section 3.1) |
| Process | about 40 MB RSS, 3 threads (CPU not measured) |
| Host | any small machine; disk 1 GB is plenty |

## 9. Differences from the original design, and why

| Original design | Built | Cause |
|---|---|---|
| `src` from the telemetry row (raw preferred) | `src` from the `ux` row, `delay_tel` from raw (D12) | Found on real captures: raw never marks a lost train. A regression test covers it. |
| Board job "at 03:00 daily" | `effective_today()` gate plus one date per minute (D14) | Same effect, plus catch-up, with no second code path and no risk to the minute tick. |
| Board catch-up "at start and at each 03:00" | Continuous: any due date is drained one per minute | Simpler and self-healing; also covers the start-up case. |
| Empty board day stored normally | A day with no usable schedule is a failure and stays due (D15) | Found in review: it would have been marked fetched forever. |
| Fixed 10-minute board retry | Doubling up to 6 h, ERROR at the 5th failure (D15) | Review: a deterministic failure was retried every 10 minutes forever, quietly. |
| `start_date` implicit | Stored in `meta` (D16) | A restart could otherwise skip days. |
| One tick, a failure inside it stops everything | Per-train isolation (D20) | Review blocker, reproduced: one string in a numeric field stored nothing for anyone. |
| Outage written on recovery | Written at loss time (D21) | Review: a stream that never recovers left no trace. |
| Reader polls every 5 s and keeps reading after a timeout | Read timeout 30 s means idle (D22) | Real-socket test: `urllib` cannot read again after a timeout. |
| Board catch-up reaches D-2 | Plus a lone pull `--through` (D24) | The last two days of a run need a timetable pull the next morning. |
| Archive about 50 MB for the week | About 230 MB for 8 days | Measured; per-line gzip members compress worse than I estimated. |
| Network 31 MB/min | 2.8 MB/min, 32 GB over 8 days | An early unit error (KB per second); corrected from measurements. |
| `requests`/`httpx` HTTP client | `urllib` | No dependencies (D18). |

## 10. Operations

Examples for every item below are in `examples/` (systemd units, a timer, a cron line, a screen
launcher). Paths in them are placeholders.

**Running unattended.** Run `python -m collector` under `systemd`, `screen` or `tmux`. For a run
with a fixed length wrap it in `timeout -k 300 <duration>`: after the duration `timeout` sends
SIGTERM, the collector shuts down gracefully (about 2 s, at most about 2 minutes with a board
catch-up), and `-k 300` kills it if it hangs.

**The last days of a run (D24).** Let E be the Athens date on which the process stops. During the
run the D-2 rule fetches the boards up to E-2. From 10:00 Athens time on the day after the stop run
once: `python -m collector --db <db> --archive-dir <dir> --board-catchup-only --through <E>`.
It fetches every missing board date up to and including E, prints the dates stored and exits;
before 10:00 it refuses with a message and changes nothing. It can be scheduled (see
`examples/`); it must not run while the collector is still running (single writer).

**Daily timetables only (D25).** `python -m collector.board_daily --db <db> --start-date <day>`
runs forever and stores each day once it is final; add `--until <day>` for a last day.

**Health:** the log has one `tick ts=... n_rows=... n_trains_seen=... n_skipped=...` line per
minute (UTC). Alert if there is no tick line for 3 minutes, if `SELECT max(ts) FROM position` is
older than 5 minutes (it normally lags the tick by about 10-20 s because the stream is held back),
or if the log shows `ERROR`. `outage` rows with `end_ts` NULL are streams that are down right now.
The log rotates at 5 MB x 3.

**Restart safety:** `last_stored_ts` is reloaded from `position`, the primary key prevents
duplicates, `start_date` is read back from `meta`. A restart does not close outage rows left open
by the previous process, and the time the process itself was down is not recorded as an outage (a
gap in `position` with no `outage` row means the collector was not running).

**Clock:** the host clock must be correct (NTP): ticks and `effective_today` use it. The host needs
`tzdata` for `Europe/Athens`.

**Single writer:** only one process writes to a database file at a time. Do not run
`--board-catchup-only` or `collector.board_daily` on a database the collector is using.

## 11. Verification performed

- **Unit tests:** all with fakes, no network, including real captured samples for the parser, the
  tick and the board (`python -m unittest discover -s tests`).
- **Mutation checks:** about 45 deliberate breakages of key behaviour (D-2 rule, 03:00 gate, one
  date per call, retry delay and escalation, start-date persistence, backoff, idle watchdog both
  ways, archive skip, stop replacement, actual-arrival update, final tick, tick isolation, outage
  open and close, stub overwrite, all-unusable board day, injected clock, dead-reader check,
  `--through`, finality, `--until`, minimum pass interval). Each is caught by a test. Several were
  not caught by the first version of the tests (the update of the actual arrival on a later board
  fetch, the final tick at shutdown, the dead-reader check inside the minute loop, the idle-timeout
  rule, and one test that never ran its timeouts); tests were added or tightened for each.
- **Independent review** of the code by a second reader, read-only. Findings were reproduced
  before acting. Fixed: tick isolation (blocker), board day with no usable schedule and unbounded
  quiet retries, outage written only on recovery, stub overwrite of board columns, board clock not
  injectable, `last_stored_ts` advancing before the commit. Documented, not fixed: the retry deadline
  and failure count are in memory (lost on restart), `--board-catchup-only` has no signal handler (a
  kill during a board store rolls back and the day stays due).
- **Live runs against the real servers:** minute ticks at second 0 with about 30-35 rows, board days
  stored one per minute, no errors, `integrity_check` ok, SIGTERM shutdown in about 2 s, restart on
  the same database without duplicates.
- **Real-socket tests against a local fake server** (real HTTP chunked SSE, switched between normal,
  closing and silent): closed connections gave reconnects with the 1, 2, 4, 8 s backoff and one
  outage row per stream, closed on recovery; silence gave `idle 30s` reconnects after 30 s and one
  merged outage per stream.
- **Daily puller:** finality checked against `board_ready_at` every 15 minutes across both DST
  changes, plus live runs against the real API for a finite run, an open-ended run, a day that is
  not final yet, and the sleeping daemon with a clean SIGTERM exit.

## 12. Known limits and open items

1. **`delay` is 0 in every live row** (both streams, also in the dry runs). Whether it ever
   moves, and its unit, is unknown. Stored as received. The board's `delay_min` is the
   reliable value (in the dry-run board days 423 of 450 completed schedules had a delay
   above 0, maximum 103 minutes).
2. **Board stop stations missing from the geo data:** 10 of 151 distinct station ids in the
   sample board are not among the 389 geo stations, so their `schedule_stop.station_id` is
   NULL. Intermediate punctuality cannot be computed for those stops.
3. **Unassigned locomotives cannot be attributed to a schedule.** A locomotive seen without a
   `scheduleId` while it actually runs a service has no link to that service in the database
   (the raw archive may allow a join afterwards). Intermediate punctuality is unrecoverable
   for those minutes.
4. **A slow board request delays the tick loop:** the board step runs inline after the
   tick. A hung request (60 s timeout) would cost at most one minute of samples. Accepted.
5. **The server closes each stream every 15 minutes** (observed on the live collector: connections
   opened at 21:30:32 UTC ended with EOF at 21:45:33 and 22:00:34, both streams each time). The
   collector reconnects within about a second and every minute still has its rows, so this costs
   no data, but it writes one short `outage` row per stream per 15 minutes (about 190 a day):
   filter outages by duration when analysing. Each reconnect downloads the `trainHistory` event
   again (about 190 KB, so about 18 MB a day, negligible next to the 4 GB). Other failure modes
   (a stalled connection, a server that is down for a long time) were tested only against a
   local fake server on real sockets.
6. **Undocumented APIs** may change without notice; the raw archive (D9) and the per-train
   isolation (D20) are the mitigations.
7. **Terms of use** of railway.gov.gr are unchecked.
8. **Unseen events:** `trainPositions` and `delayUpdates` (train-stream) were never observed.
   If they appear in the archive they need a decision.
9. **Train identity:** whether `trainId` is stable across days and whether a locomotive
   changes `scheduleId` mid-day is unverified. The schema allows `schedule_id` to change.
10. **In-memory board retry state** (deadline, failure count) is lost on a restart: a
    crash-looping process would retry a failing day every minute. Accepted.
11. **Gaps when the process is down** are not recorded as outages (section 10).
12. **Positions use second granularity** (section 5). Irrelevant at minute sampling.
13. **Traffic:** about 32 GB inbound over 8 days (section 3.1). Check the host's allowance.

## 13. Post-processing (not part of the collector)

- **Terminus punctuality:** read it from `schedule` (`delay_min`, `actual_arr` against
  `sched_arr`, `actual_dep` against `sched_dep`).
- **Intermediate stops:** for each `schedule_stop`, find where the train's samples come
  within a radius of the station (`station` coordinates, with `along_m`/`dest_m` as
  cross-checks), interpolate the passage time between the two neighbouring samples, and
  subtract `schedule_stop.sched_arr`. Expected error about one minute (D1).
- **Delay comparison:** compare `position.delay`, `position.delay_tel` and
  `schedule.delay_min` for the same schedule to learn whether the live streams carry a delay.
