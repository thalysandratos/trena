"""SQLite layer for the railway collector (schema: docs/DESIGN.md, section 4).

All times are UTC epoch seconds (D10). Integers everywhere for compact storage.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS station (
  id INTEGER PRIMARY KEY, mongo_id TEXT UNIQUE, name TEXT,
  lat_e5 INTEGER, lng_e5 INTEGER, railway_type TEXT, status TEXT, operator TEXT);

CREATE TABLE IF NOT EXISTS train (
  id INTEGER PRIMARY KEY, mongo_id TEXT UNIQUE,
  train_type TEXT, locomotive_number TEXT, locomotive_name TEXT);

CREATE TABLE IF NOT EXISTS schedule (
  id INTEGER PRIMARY KEY, mongo_id TEXT UNIQUE,
  schedule_number TEXT, train_number TEXT, service_type TEXT, corridor TEXT,
  origin TEXT, destination TEXT,
  sched_dep INTEGER, sched_arr INTEGER,
  status TEXT, delay_min INTEGER, actual_dep INTEGER, actual_arr INTEGER);

CREATE TABLE IF NOT EXISTS schedule_stop (
  schedule_id INTEGER, idx INTEGER, station_id INTEGER, role TEXT,
  sched_arr INTEGER, sched_dep INTEGER,
  PRIMARY KEY (schedule_id, idx)) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS position (
  train_id    INTEGER NOT NULL,
  ts          INTEGER NOT NULL,
  schedule_id INTEGER,
  lat_e5 INTEGER, lng_e5 INTEGER,
  speed_x10 INTEGER,
  progress_x1e4 INTEGER,
  along_m INTEGER,
  dest_m INTEGER,
  next_station INTEGER,
  delay INTEGER,
  delay_tel INTEGER,
  sig INTEGER,
  src INTEGER,
  PRIMARY KEY (train_id, ts)) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS board_fetch (date TEXT PRIMARY KEY, fetched_at INTEGER, n INTEGER);
CREATE TABLE IF NOT EXISTS outage (stream TEXT, start_ts INTEGER, end_ts INTEGER, reason TEXT);
"""


def connect(path: str) -> sqlite3.Connection:
    """Open (creating if needed) the collector DB in WAL mode."""
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    """Idempotent schema creation."""
    conn.executescript(SCHEMA)
    conn.commit()


def iso_to_epoch(s: str | None) -> int | None:
    """ISO-8601 UTC string (e.g. ``2026-11-19T17:06:49.000Z``) -> UTC epoch seconds.

    ``None``/empty -> ``None``; unparseable -> ``None`` with a warning (never raises,
    so a bad stream field cannot kill the writer).
    """
    if not s:
        return None
    if not isinstance(s, str):  # an API change (e.g. epoch milliseconds) must not kill the writer
        log.warning("iso_to_epoch: non-string timestamp %r", s)
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        log.warning("iso_to_epoch: unparseable timestamp %r", s)
        return None
    if dt.tzinfo is None:  # assume UTC when no offset is given
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def load_stations(conn: sqlite3.Connection, geo_json: dict) -> int:
    """Fill ``station`` from a ``/api/msd/geo?types=station`` response.

    Idempotent: upserts by ``mongo_id``. Returns the number of features seen.
    """
    data = geo_json.get("data", geo_json)
    features = data.get("features", [])
    rows = []
    for f in features:
        props = f.get("properties", {})
        coords = f.get("geometry", {}).get("coordinates") or [None, None]
        lng, lat = coords[0], coords[1]
        rows.append(
            (
                props.get("_id"),
                props.get("name"),
                round(lat * 1e5) if lat is not None else None,
                round(lng * 1e5) if lng is not None else None,
                props.get("railway_type"),
                props.get("status"),
                props.get("operator"),
            )
        )
    conn.executemany(
        """INSERT INTO station (mongo_id, name, lat_e5, lng_e5, railway_type, status, operator)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(mongo_id) DO UPDATE SET
             name=excluded.name, lat_e5=excluded.lat_e5, lng_e5=excluded.lng_e5,
             railway_type=excluded.railway_type, status=excluded.status,
             operator=excluded.operator""",
        rows,
    )
    conn.commit()
    return len(rows)


class IdMaps:
    """Mongo-id -> integer-id mapping with in-process caches.

    Only the writer thread uses this, so no locking is needed here. Unknown
    stations are cached as ``None`` so a missing geo entry costs one lookup.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self._train: dict[str, int] = {}
        self._schedule: dict[str, int] = {}
        self._station: dict[str, int | None] = {}

    def train(self, mongo_id: str, train_type: str | None, loco_number: str | None,
              loco_name: str | None) -> int:
        cached = self._train.get(mongo_id)
        if cached is not None:
            return cached
        row = self.conn.execute("SELECT id FROM train WHERE mongo_id=?", (mongo_id,)).fetchone()
        if row is None:
            cur = self.conn.execute(
                "INSERT INTO train (mongo_id, train_type, locomotive_number, locomotive_name)"
                " VALUES (?, ?, ?, ?)",
                (mongo_id, train_type, loco_number, loco_name),
            )
            train_id = cur.lastrowid
        else:
            train_id = row[0]
        self._train[mongo_id] = train_id
        return train_id

    def schedule(self, mongo_id: str, **stub_fields) -> int:
        """Insert-on-first-sight schedule stub; never touches the board-filled columns
        (status, delay_min, actual_dep, actual_arr) of an existing row."""
        cached = self._schedule.get(mongo_id)
        if cached is not None:
            return cached
        cols = ["mongo_id", "schedule_number", "train_number", "service_type", "corridor",
                "origin", "destination", "sched_dep", "sched_arr"]
        values = [mongo_id,
                  stub_fields.get("schedule_number"), stub_fields.get("train_number"),
                  stub_fields.get("service_type"), stub_fields.get("corridor"),
                  stub_fields.get("origin"), stub_fields.get("destination"),
                  stub_fields.get("sched_dep"), stub_fields.get("sched_arr")]
        placeholders = ", ".join("?" for _ in cols)
        # DO NOTHING: an existing row (completed by the board, or created by an earlier stub)
        # is never modified from the stream; after a restart the stream stub would otherwise
        # overwrite the board's timetable columns
        self.conn.execute(
            f"INSERT INTO schedule ({', '.join(cols)}) VALUES ({placeholders})"
            f" ON CONFLICT(mongo_id) DO NOTHING",
            values,
        )
        schedule_id = self.conn.execute(
            "SELECT id FROM schedule WHERE mongo_id=?", (mongo_id,)).fetchone()[0]
        self._schedule[mongo_id] = schedule_id
        return schedule_id

    def station(self, mongo_id: str | None) -> int | None:
        """Look up only; ``None`` if the station is unknown (or the id is null)."""
        if mongo_id is None:
            return None
        if mongo_id in self._station:
            return self._station[mongo_id]
        row = self.conn.execute("SELECT id FROM station WHERE mongo_id=?", (mongo_id,)).fetchone()
        station_id = row[0] if row else None
        self._station[mongo_id] = station_id
        return station_id
