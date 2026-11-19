# trainose-collector

Tools that pull the **public** live and timetable data of the Greek railway
(<https://railway.gov.gr>) into a small SQLite database, so that train **punctuality** can be
analysed afterwards. Python standard library only, no dependencies.

There are two programs:

| Program | What it does |
|---|---|
| `python -m collector.board_daily` | **Daily timetable puller.** Stores the timetable of every day (scheduled and *actual* times, final delay, all stops) once that day is final. Light: about one request every two seconds while it works, then idle. Never ends unless you give it a last day. |
| `python -m collector` | **Live collector.** Keeps two live data streams open and writes one row per train per minute (position, speed, progress, next station, delay), plus the timetables. Heavy on traffic (about 4 GB a day downloaded). |

> **Please read before you run anything.**
> This is an unofficial tool. It is not affiliated with the railway operator or the website. It
> only uses endpoints that answer without a login, but those endpoints are **undocumented** and may
> change or disappear at any time, and **the operator's terms of use have not been checked** by the
> author: check them before you rely on the data, publish it, or run the heavy collector for a long
> time. Be considerate: do not run several copies, and do not lower `--pause`. See
> [Being a good citizen](#being-a-good-citizen).

## Requirements

* Python with the `zoneinfo` module (3.9 or newer) and time zone data (`tzdata` on most Linux
  distributions; `pip install tzdata` on Windows). It has been developed and tested with
  **Python 3.12 only**. The code uses no syntax newer than 3.9, so older versions may well work, but
  that has not been tried.
* Network access to `railway.gov.gr`.
* Disk: tens of megabytes for the database; the live collector's raw archive adds about 30 MB a day
  (see [Disk and traffic](#disk-and-traffic)).

No installation step: run from the project directory.

```
python3 -m unittest discover -s tests        # about 130 tests, no network needed, a few seconds
```

## Quick start: the daily timetable puller

Pull a window of past days and stop (the API keeps roughly nine months of history):

```
python3 -m collector.board_daily --db data/board.db \
    --start-date 2026-11-01 --until 2026-11-10 --once
```

`--once` makes one pass and exits. Without `--once` the program would sleep and check again every
day until `--until` is complete. **Leave `--until` out and it never ends**: it checks every day
at 10:30 Athens time, stores yesterday's timetable, and sleeps again.

```
# run forever, from a start day on (backfills every day since then, then follows the calendar)
python3 -m collector.board_daily --db data/board.db --start-date 2026-11-01
```

### How it decides what to fetch

* A timetable day D is **final from 10:00 Athens time on D+1**: trains that start late on D run past
  midnight (the latest arrival seen was 08:44). Only final days are fetched, so every stored day is
  complete, with actual arrival times and delays.
* Days already stored are never fetched again, so you can stop and restart freely.
* `--start-date` is stored in the database on first use (later runs reuse it; passing it again
  overrides). Without it the first run starts today.
* A failing day (network error, empty answer) is skipped, logged, and retried in an hour (or by
  the next `--once` run). Other days are not held up.
* `--until DAY` is the **last day to pull, inclusive**. When every day from the start through
  `--until` is stored, the program logs `finished` and exits with code 0. Days after `--until` are
  never fetched.

### Options

| Option | Meaning |
|---|---|
| `--db PATH` | SQLite file (default `data/board.db`). |
| `--start-date YYYY-MM-DD` | First day to pull. May be far in the past. |
| `--until YYYY-MM-DD` | Last day to pull, inclusive. **Omit to never end.** |
| `--run-at HH:MM` | Daily check time, **Athens** local time (default `10:30`; use 10:00 or later). |
| `--once` | One pass, then exit. For cron or a systemd timer. |
| `--pause SECONDS` | Pause between two requests (default 2). |
| `--base-url URL` | Server to use (default `https://railway.gov.gr`). |
| `--log-file PATH` | Also log to this rotating file. |
| `-v` | Debug logging. |

Exit codes: `0` ok (or finished), `3` with `--once`: some final days failed and will be retried,
`1` could not start (for example the station list could not be loaded), `2` bad arguments.

Schedule it with a timer or cron instead of leaving it running: see `examples/systemd/` and
`examples/cron/`.

## Quick start: the live collector

```
python3 -m collector --db data/trains.db --archive-dir data/archive --log-file data/collector.log
```

Once a minute (at second 0, UTC) it writes the latest position of every train it knows about.
It also keeps the timetables up to the day before yesterday in the same database (see the
[design notes](docs/DESIGN.md) for why that lag exists). Stop it with Ctrl-C or SIGTERM: it shuts down
gracefully (writes the final samples, closes the database). It can be restarted at any time; it
resumes without duplicating rows.

For a run of fixed length, wrap it in `timeout`, which sends SIGTERM when the time is up:

```
timeout -k 300 8d python3 -m collector --db data/trains.db --archive-dir data/archive
```

**The last days of a run.** The collector only reaches the timetable of the day before yesterday,
so after a run that stopped on day E, fetch the rest from 10:00 Athens time the next morning
(before that the command refuses and changes nothing):

```
python3 -m collector --db data/trains.db --archive-dir data/archive --board-catchup-only --through E
```

`examples/lone_pull_after_run.sh` does this, works out E from the database, and is safe to schedule.
Alternatively point `collector.board_daily` at the same database file once the collector has
stopped.

### Options

| Option | Meaning |
|---|---|
| `--db PATH` | SQLite file (default `data/trains.db`). |
| `--archive-dir PATH` | Raw archive: once a minute the latest event of each stream, gzipped (default `data/archive`). Can be deleted at any time. |
| `--start-date YYYY-MM-DD` | First timetable day to catch up (default today, Athens). Stored; a restart does not move it. |
| `--max-seconds N` | Stop by itself after N seconds (for tests). |
| `--board-catchup-only` | Fetch the due timetable days and exit; do not start the streams. |
| `--through YYYY-MM-DD` | With `--board-catchup-only`: every day up to and including this one. Refused while that day is not final. |
| `--force` | With `--through`: fetch even if not final yet (the data may be incomplete). |
| `--base-url`, `--log-file`, `-v` | As above. |

## What is stored

One SQLite file. The two programs share the same layout (the daily puller leaves `position`
empty). Times are UTC epoch seconds; coordinates are degrees times 100000, speed is km/h times 10.

| Table | Contents |
|---|---|
| `schedule` | One row per scheduled train service: number, type, corridor, origin, destination, scheduled and **actual** departure/arrival, `status`, `delay_min` (the final delay at the destination, minutes). |
| `schedule_stop` | The scheduled stops of each service (arrival and departure time, station). Actual times at intermediate stops are **not** published. |
| `station` | The stations from the map data (389). A few stop ids of the timetable are not in it, so their `station_id` is NULL. |
| `position` (live collector) | One row per train per minute: time, lat/lng, speed, progress along the route, distance to destination, next station, `delay` values, signal state. |
| `train` | Locomotive or unit ids seen. |
| `board_fetch` | Which timetable days are stored. |
| `outage` (live collector) | When a stream was down (`end_ts` is NULL while it still is). The server ends every stream every 15 minutes, so expect a one-second outage row per stream about every quarter of an hour: filter by duration. |
| `meta` | The stored start date. |

Example queries (all tested):

```sql
-- punctuality at the destination, per service day and type
SELECT substr(schedule_number, instr(schedule_number,'-')+1, 8) AS service_day, service_type,
       count(*) AS trains, round(avg(delay_min),1) AS avg_delay_min, max(delay_min) AS worst_min
FROM schedule WHERE status='completed'
GROUP BY service_day, service_type ORDER BY service_day, service_type;

-- the worst delays
SELECT schedule_number, origin, destination, delay_min,
       datetime(sched_arr,'unixepoch') AS scheduled_utc, datetime(actual_arr,'unixepoch') AS actual_utc
FROM schedule WHERE status='completed' ORDER BY delay_min DESC LIMIT 10;

-- the stops of one service
SELECT st.idx, coalesce(s.name,'(station not in the map data)') AS station, st.role,
       datetime(st.sched_arr,'unixepoch') AS scheduled_arrival_utc
FROM schedule_stop st LEFT JOIN station s ON s.id = st.station_id
WHERE st.schedule_id = (SELECT id FROM schedule WHERE schedule_number = '1557-20261119-1901')
ORDER BY st.idx;

-- latest position of every train (live collector)
SELECT t.locomotive_number, datetime(p.ts,'unixepoch') AS utc, p.lat_e5/1e5 AS lat, p.lng_e5/1e5 AS lng,
       p.speed_x10/10.0 AS kmh, s.name AS next_station
FROM position p JOIN train t ON t.id = p.train_id LEFT JOIN station s ON s.id = p.next_station
WHERE p.ts = (SELECT max(ts) FROM position WHERE train_id = p.train_id)
ORDER BY p.ts DESC;

-- times the streams were down
SELECT stream, datetime(start_ts,'unixepoch') AS from_utc, datetime(end_ts,'unixepoch') AS to_utc, reason
FROM outage ORDER BY start_ts;
```

`schedule.status` is `completed`, `in_progress`, `scheduled` or `cancelled`. `delay_min` is only
meaningful for completed services. Intermediate-stop punctuality has to be derived by comparing the
minute positions with `schedule_stop` times; it is accurate to about a minute.

## Disk and traffic

| | Live collector | Daily puller |
|---|---|---|
| Downloaded | about 2.8 MB a minute, **about 4 GB a day** (the servers ignore compression) | about 0.9 MB per timetable day |
| Database growth | about 10-35 MB for 8 days (positions) plus about 0.2 MB per timetable day | about 0.2 MB per timetable day |
| Raw archive | about 30 MB a day (can be deleted at any time) | none |

## Running unattended

`examples/` has ready-made files, with placeholder paths and user names that you must adapt:

* `systemd/trainose-collector.service`: the live collector as a service.
* `systemd/trainose-board-daily.service`: the daily puller as a long-lived service.
* `systemd/trainose-board-once.service` and `.timer`: the daily puller, one pass per day.
* `cron/board-daily.cron`: the same with cron.
* `run_in_screen.sh`: start the collector detached in `screen` with a time limit.
* `lone_pull_after_run.sh`: the timetable pull after a collector run.

A `screen` session does not survive a reboot; use systemd for anything long-lived. The host clock must
be correct. Only one program may write to a database file at a time.

## Being a good citizen

* These are somebody else's servers. The live collector holds two streams open all day; the daily
  puller is much lighter. If you only need timetables, use `collector.board_daily`.
* Do not run several instances, do not lower `--pause`, and do not shorten the daily schedule.
* Respect the operator's terms of use, which the author has not verified.
* The programs only use public endpoints. They do not log in and do not try to get around anything.
* The requests identify themselves as `trainose-collector/0.1` and send no personal information.

## Known limits

* **Undocumented API.** Field names, paths or behaviour may change. The live collector skips a
  train row it cannot parse instead of failing, and logs it.
* **`delay` in the live streams was 0 in every observation.** The reliable delay is `schedule.delay_min`
  from the timetable, which exists for the destination only.
* Actual times at intermediate stops are not published; they must be estimated from the positions.
* Locomotives that run without a published schedule cannot be linked to a service.
* The server closes each stream every 15 minutes. The collector reconnects within about a second
  and loses no samples, but it logs a warning and writes a short `outage` row each time.
* A gap in `position` with no `outage` row means the collector was not running.
* The timetable of a day is stored once it is final; later corrections by the operator (if any) are
  not picked up.

More detail, with the reasons behind every choice, is in [`docs/DESIGN.md`](docs/DESIGN.md); what is
known about the API is in [`docs/API_NOTES.md`](docs/API_NOTES.md).

## Layout

```
collector/            the code
  board_daily.py      daily timetable puller
  app.py  __main__.py live collector and its command line
  readers.py sse.py   the two SSE stream readers and the parser
  sampler.py          the per-minute sampling into the database
  board.py            timetable fetch and storage
  db.py archive.py    schema, id maps, raw archive
tests/                unit tests (fakes only, no network)
samples/              small real captures of the API used by the tests
examples/             systemd, cron and screen examples
docs/                 DESIGN.md, API_NOTES.md
```

## License

No license is included. Add the one you want before you publish this.
