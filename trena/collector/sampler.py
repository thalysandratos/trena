"""Minute-tick sampler (docs/DESIGN.md, section 5).

Pure and testable: no threads, no network. Reader threads only push events into
``State``; the single writer thread calls ``tick`` once per minute.
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
import threading
from typing import Any

from .db import IdMaps, iso_to_epoch

log = logging.getLogger(__name__)

# position.signalStatus -> sig, telemetry source -> src
SIG_MAP = {"ok": 0, "degraded": 1, "lost": 2}
SRC_MAP = {"live": 0, "last_known": 1, "schedule": 2}


def _round(value: Any, factor: float) -> int | None:
    """``round(value * factor)``; ``None`` stays ``NULL``.

    Anything that is not a finite number (a string, a dict, NaN, a bool) becomes ``NULL``
    with a warning instead of raising: one odd field must not cost the whole tick.
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        log.warning("tick: non-numeric value %r stored as NULL", value)
        return None
    return round(value * factor)


class State:
    """Latest in-memory state of both streams, guarded by ``lock``."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.latest_pos: dict[str, dict] = {}     # trainId -> raw trainPositionsUx row
        self.latest_tel: dict[str, dict] = {}     # scheduleId -> telemetry row for delay (raw wins)
        self.latest_tel_ux: dict[str, dict] = {}  # scheduleId -> latest 'ux' row: the only view
                                                  # that marks last_known (raw keeps saying 'live')
        self.latest_events: dict[str, Any] = {}   # event name -> latest full payload (archive)
        self._tel_raw: set[str] = set()           # scheduleIds that have a raw telemetry row

    def handle_train_stream_event(self, name: str, data_json_str: str) -> None:
        try:
            payload = json.loads(data_json_str)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            log.warning("train-stream event %r: malformed JSON skipped (%s)", name, exc)
            return
        with self.lock:
            self.latest_events[name] = payload
            if name == "trainPositionsUx":
                positions = payload.get("positions") if isinstance(payload, dict) else None
                if not isinstance(positions, list):
                    log.warning("trainPositionsUx: payload has no 'positions' list")
                    return
                for row in positions:
                    if isinstance(row, dict) and row.get("trainId"):
                        self.latest_pos[row["trainId"]] = row

    def handle_telemetry_event(self, name: str, data_json_str: str) -> None:
        try:
            payload = json.loads(data_json_str)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            log.warning("telemetry event %r: malformed JSON skipped (%s)", name, exc)
            return
        with self.lock:
            self.latest_events[name] = payload
            if name in ("scheduleTelemetry", "scheduleTelemetryRaw"):
                rows = payload.get("telemetry") if isinstance(payload, dict) else None
                if not isinstance(rows, list):
                    log.warning("%s: payload has no 'telemetry' list", name)
                    return
                is_raw = name == "scheduleTelemetryRaw"
                for row in rows:
                    if not (isinstance(row, dict) and row.get("scheduleId")):
                        continue
                    sid = row["scheduleId"]
                    if not is_raw:
                        self.latest_tel_ux[sid] = row
                    # raw wins; the ux view is a fallback only while no raw row
                    # exists for that schedule yet
                    if is_raw:
                        self.latest_tel[sid] = row
                        self._tel_raw.add(sid)
                    elif sid not in self._tel_raw:
                        self.latest_tel[sid] = row


def tick(state: State, conn: sqlite3.Connection, idmaps: IdMaps,
         last_stored_ts: dict[int, int], now_epoch: int) -> int:
    """One minute tick: store one row per train with a fix newer than the last stored one.

    All inserts happen in a single transaction. Returns the number of rows inserted.
    """
    with state.lock:
        positions = dict(state.latest_pos)
        latest_tel = dict(state.latest_tel)
        latest_tel_ux = dict(state.latest_tel_ux)
    n_trains_seen = len(positions)
    n_rows = 0
    n_skipped = 0
    stored: dict[int, int] = {}  # applied to last_stored_ts only after the commit
    with conn:  # one transaction per tick
        for train_key, row in positions.items():
            try:
                inserted, train_id, ts = _store_row(conn, idmaps, last_stored_ts, train_key, row,
                                                    latest_tel, latest_tel_ux)
            except Exception as exc:  # one bad row must not cost the other trains' samples
                n_skipped += 1
                log.warning("tick: train %s skipped (%s: %s)", train_key, type(exc).__name__, exc)
                continue
            if train_id is not None:
                stored[train_id] = ts
            n_rows += inserted
    last_stored_ts.update(stored)  # only reached when the transaction committed
    log.info("tick ts=%d n_rows=%d n_trains_seen=%d n_skipped=%d",
             now_epoch, n_rows, n_trains_seen, n_skipped)
    return n_rows


def _store_row(conn: sqlite3.Connection, idmaps: IdMaps, last_stored_ts: dict[int, int],
               train_key: str, row: dict, latest_tel: dict, latest_tel_ux: dict
               ) -> tuple[int, int | None, int]:
    """Insert one train's row. Returns ``(rows_inserted, train_id or None, ts)``; ``train_id``
    is ``None`` when the row is skipped (no usable or no newer timestamp)."""
    train_id = idmaps.train(row.get("trainId") or train_key, row.get("trainType"),
                            row.get("locomotiveNumber"), row.get("locomotiveName"))
    ts = iso_to_epoch(row.get("timestamp"))
    if ts is None:
        log.warning("tick: train %s has no usable timestamp, skipped", train_key)
        return 0, None, 0
    if train_id in last_stored_ts and ts <= last_stored_ts[train_id]:
        return 0, None, ts  # stale or repeated fix: no new row
    sid = row.get("scheduleId")
    schedule_id = None
    if sid:
        schedule_id = idmaps.schedule(
            sid,
            schedule_number=row.get("scheduleNumber"),
            train_number=row.get("trainNumber"),
            service_type=row.get("serviceType"),
            corridor=row.get("corridor"),
            origin=row.get("origin"),
            destination=row.get("destination"),
            sched_dep=iso_to_epoch(row.get("scheduledDeparture")),
            sched_arr=iso_to_epoch(row.get("scheduledArrival")),
        )
    tel = latest_tel.get(sid) if sid else None
    # src comes from the ux view: raw reports 'live' even for a train that lost GPS
    src_row = (latest_tel_ux.get(sid) or tel) if sid else None
    cur = conn.execute(
        "INSERT OR IGNORE INTO position (train_id, ts, schedule_id, lat_e5, lng_e5,"
        " speed_x10, progress_x1e4, along_m, dest_m, next_station, delay, delay_tel,"
        " sig, src) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            train_id, ts, schedule_id,
            _round(row.get("lat"), 1e5), _round(row.get("lng"), 1e5),
            _round(row.get("speed"), 10), _round(row.get("progress"), 1e4),
            _round(row.get("alongTrackDistance"), 1),
            _round(row.get("distanceToDestination_m"), 1),
            idmaps.station(row.get("nextStationId")),
            row.get("delay"),
            tel.get("delay") if tel else None,
            SIG_MAP.get(row.get("signalStatus")),
            SRC_MAP.get(src_row.get("source")) if src_row else None,
        ),
    )
    return cur.rowcount, train_id, ts  # rowcount 0 when the (train_id, ts) row already existed


def load_last_stored_ts(conn: sqlite3.Connection) -> dict[int, int]:
    """Max stored ts per train_id, reloaded from ``position`` at restart."""
    return {train_id: ts for train_id, ts in
            conn.execute("SELECT train_id, max(ts) FROM position GROUP BY train_id")}
