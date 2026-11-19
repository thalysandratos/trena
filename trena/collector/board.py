"""Schedule-board job (docs/DESIGN.md, sections 4 and 6, D4/D5).

``/api/public/schedule-board?date=YYYY-MM-DD`` is fetched once a day at 03:00
Europe/Athens for the day before yesterday (D-2), and missed days are caught up
from ``start_date``. This module is a library: no globals, no threads, all I/O
injectable (``opener`` for HTTP, ``fetch`` for catch-up, ``sleep`` for pauses).
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from .db import IdMaps, iso_to_epoch
from .readers import USER_AGENT

log = logging.getLogger(__name__)

ATHENS = ZoneInfo("Europe/Athens")
BOARD_PATH = "/api/public/schedule-board"


def athens_today(now: float | None = None) -> date:
    """The current calendar day in Europe/Athens."""
    dt = datetime.fromtimestamp(now if now is not None else time.time(), tz=timezone.utc)
    return dt.astimezone(ATHENS).date()


def due_dates(conn: sqlite3.Connection, start_date: date, today: date) -> list[str]:
    """ISO dates from ``start_date`` up to ``today - 2 days`` inclusive that are
    not yet in ``board_fetch`` (D5). Empty when ``start_date > today - 2``."""
    last = today - timedelta(days=2)
    if start_date > last:
        return []
    done = {row[0] for row in conn.execute("SELECT date FROM board_fetch")}
    out: list[str] = []
    d = start_date
    while d <= last:
        iso = d.isoformat()
        if iso not in done:
            out.append(iso)
        d += timedelta(days=1)
    return out


def fetch_board(date_str: str, base_url: str, *, opener: Callable | None = None,
                timeout: float = 60) -> dict:
    """GET ``{base_url}/api/public/schedule-board?date={date_str}`` and parse it.

    Requires a dict with a ``schedules`` list; the API answers
    ``{"error": ...}`` (HTTP 200) on bad input, which raises ``ValueError``.
    """
    url = f"{base_url}{BOARD_PATH}?{urllib.parse.urlencode({'date': date_str})}"
    open_fn = opener or urllib.request.urlopen
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with open_fn(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("schedules"), list):
        raise ValueError(f"schedule-board {date_str}: unexpected payload "
                         f"{str(payload)[:120]!r}")
    return payload


def _name_of(node: Any) -> str | None:
    """``{name, nameGreek}`` dict -> ``name``; a plain string passes through."""
    if isinstance(node, dict):
        return node.get("name")
    return node if isinstance(node, str) else None


SCHEDULE_COLS = (
    "schedule_number", "train_number", "service_type", "corridor", "origin",
    "destination", "sched_dep", "sched_arr", "status", "delay_min",
    "actual_dep", "actual_arr",
)


def store_board(conn: sqlite3.Connection, idmaps: IdMaps, date_str: str,
                payload: dict, *, now: Callable[[], float] = time.time) -> int:
    """Store one day's board in ONE transaction. Idempotent. Returns the number
    of schedules stored.

    Stream stubs (same ``mongo_id``) are completed in place, keeping their ``id``.
    """
    schedules = payload["schedules"]
    stored = 0
    with conn:  # one transaction
        for s in schedules:
            mongo_id = s.get("_id")
            if not mongo_id:
                log.warning("board %s: schedule without _id skipped", date_str)
                continue
            stored += 1
            conn.execute(
                """INSERT INTO schedule (mongo_id, schedule_number, train_number,
                     service_type, corridor, origin, destination, sched_dep, sched_arr,
                     status, delay_min, actual_dep, actual_arr)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(mongo_id) DO UPDATE SET
                     schedule_number=excluded.schedule_number,
                     train_number=excluded.train_number,
                     service_type=excluded.service_type,
                     corridor=excluded.corridor,
                     origin=excluded.origin,
                     destination=excluded.destination,
                     sched_dep=excluded.sched_dep,
                     sched_arr=excluded.sched_arr,
                     status=excluded.status,
                     delay_min=excluded.delay_min,
                     actual_dep=excluded.actual_dep,
                     actual_arr=excluded.actual_arr""",
                (
                    mongo_id, s.get("scheduleNumber"), s.get("trainNumber"),
                    s.get("serviceType"), s.get("corridor"),
                    _name_of(s.get("origin")), _name_of(s.get("destination")),
                    iso_to_epoch(s.get("scheduledDeparture")),
                    iso_to_epoch(s.get("scheduledArrival")),
                    s.get("status"), s.get("delay"),
                    iso_to_epoch(s.get("actualDeparture")),
                    iso_to_epoch(s.get("actualArrival")),
                ),
            )
            schedule_id = conn.execute(
                "SELECT id FROM schedule WHERE mongo_id=?", (mongo_id,)).fetchone()[0]
            conn.execute("DELETE FROM schedule_stop WHERE schedule_id=?", (schedule_id,))
            stops = s.get("routeStations") or []
            if stops:
                conn.executemany(
                    """INSERT INTO schedule_stop (schedule_id, idx, station_id, role,
                         sched_arr, sched_dep) VALUES (?,?,?,?,?,?)""",
                    [
                        (
                            schedule_id, st.get("index"), idmaps.station(st.get("stationId")),
                            st.get("role"),
                            iso_to_epoch(st.get("scheduledArrival")),
                            iso_to_epoch(st.get("scheduledDeparture")),
                        )
                        for st in stops
                    ],
                )
        if stored == 0:
            # e.g. the API renamed `_id`: marking the day as fetched would lose it for good.
            # Raising inside the transaction rolls everything back, the date stays due.
            raise ValueError(f"board {date_str}: none of {len(schedules)} schedules is usable")
        conn.execute(
            "INSERT INTO board_fetch (date, fetched_at, n) VALUES (?,?,?)"
            " ON CONFLICT(date) DO UPDATE SET fetched_at=excluded.fetched_at, n=excluded.n",
            (date_str, int(now()), stored),
        )
    return stored


def run_catchup(conn: sqlite3.Connection, idmaps: IdMaps, start_date: date, today: date,
                *, fetch: Callable[[str, str], dict] = fetch_board, base_url: str,
                now: Callable[[], float] = time.time,
                sleep: Callable[[float], None] = time.sleep,
                pause: float = 2.0) -> list[str]:
    """Fetch and store every due date (D5). A failure on one date is logged and
    skipped (it stays due); the others continue. Returns the dates stored."""
    stored: list[str] = []
    due = due_dates(conn, start_date, today)
    for i, date_str in enumerate(due):
        if i > 0 and pause > 0:
            sleep(pause)  # politeness between requests, never before the first
        try:
            payload = fetch(date_str, base_url)
            if not payload.get("schedules"):
                # a real service day has about 180-234 schedules; an empty answer is an
                # upstream problem, so keep the date due instead of marking it fetched
                raise ValueError("empty schedules list")
            n = store_board(conn, idmaps, date_str, payload, now=now)
        except Exception as exc:
            log.warning("board catch-up %s failed, skipped (stays due): %s", date_str, exc)
            continue
        log.info("board %s stored %d schedules", date_str, n)
        stored.append(date_str)
    return stored
