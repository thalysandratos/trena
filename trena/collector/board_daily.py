"""Daily schedule-board puller: stores every railway timetable day, once that day is final.

Standalone: no live streams, only the public ``/api/public/schedule-board`` endpoint (plus
the station list, loaded once). Run it as a long-lived process (it sleeps until the daily
check time), or with ``--once`` from cron or a systemd timer.

* A board day D is **final** from 10:00 Athens time on the next day (trains that start on D
  run past midnight; the latest arrival seen was 08:44). Only final days are fetched.
* ``--start-date`` is the first day to pull (it may be far in the past: the API keeps
  roughly nine months of history). ``--until`` is the last day to pull; **leave it out and
  the puller never ends**. With ``--until`` it exits once every day through it is stored.
* Everything is stored in the same SQLite schema as the live collector, so the two can share
  a database file (do not run both on one file at the same time).

All I/O (clock, sleep, HTTP) is injectable, so the tests run without a network.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sqlite3
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from . import board, db
from .app import ATHENS, BOARD_FINAL_HOUR, default_fetch_geo

log = logging.getLogger(__name__)

DEFAULT_RUN_AT = "10:30"
MIN_PASS_INTERVAL_S = 60  # hard floor: never hit the server more often than once a minute


def last_final_day(now: float) -> date:
    """The newest Athens day whose board is final at ``now`` (10:00 Athens time the next day)."""
    local = datetime.fromtimestamp(now, tz=timezone.utc).astimezone(ATHENS)
    return (local - timedelta(hours=BOARD_FINAL_HOUR)).date() - timedelta(days=1)


def parse_run_at(text: str) -> tuple[int, int]:
    """``"10:30"`` -> ``(10, 30)``; raises ``ValueError`` on anything else."""
    hh, mm = text.split(":")
    h, m = int(hh), int(mm)
    if not (0 <= h < 24 and 0 <= m < 60):
        raise ValueError(f"not a time of day: {text!r}")
    return h, m


def next_run_at(now: float, run_at: tuple[int, int]) -> float:
    """The next moment strictly after ``now`` at which it is ``run_at`` Athens local time."""
    local = datetime.fromtimestamp(now, tz=timezone.utc).astimezone(ATHENS)
    candidate = local.replace(hour=run_at[0], minute=run_at[1], second=0, microsecond=0)
    if candidate.timestamp() <= now:
        candidate = (candidate + timedelta(days=1)).replace(
            hour=run_at[0], minute=run_at[1], second=0, microsecond=0)
    return candidate.timestamp()


@dataclass
class Settings:
    db_path: str
    base_url: str = "https://railway.gov.gr"
    start_date: date | None = None   # first board day; stored in the database on first use
    until: date | None = None        # last board day, inclusive; None = never ends
    run_at: tuple[int, int] = (10, 30)
    pause: float = 2.0               # seconds between two requests
    failed_retry_s: int = 3600       # next pass after a pass in which a day failed


@dataclass
class PassResult:
    stored: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    waiting: list[str] = field(default_factory=list)  # in range, but not final yet


class BoardDaily:
    def __init__(self, settings: Settings, *, clock: Callable[[], float] = time.time,
                 sleep: Callable[[float], None] | None = None,
                 fetch: Callable[[str, str], dict] = board.fetch_board,
                 fetch_geo: Callable[[str], dict] = default_fetch_geo) -> None:
        self.settings = settings
        self.clock = clock
        self._sleep = sleep
        self._fetch = fetch
        self._fetch_geo = fetch_geo
        self.stop_event = threading.Event()
        self.conn: sqlite3.Connection | None = None
        self.idmaps: db.IdMaps | None = None
        self.start_date: date | None = None

    # ------------------------------------------------------------------ helpers
    def _wait(self, seconds: float) -> None:
        if self._sleep is not None:
            self._sleep(seconds)
        else:
            self.stop_event.wait(seconds)

    def stop(self) -> None:
        self.stop_event.set()

    # -------------------------------------------------------------------- setup
    def setup(self) -> None:
        st = self.settings
        parent = os.path.dirname(os.path.abspath(st.db_path))
        os.makedirs(parent, exist_ok=True)
        self.conn = db.connect(st.db_path)
        db.init_schema(self.conn)
        self.conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
        self.conn.commit()
        self.idmaps = db.IdMaps(self.conn)
        self._load_stations()
        self._resolve_start_date()
        log.info("ready: db=%s start_date=%s until=%s run_at=%02d:%02d Athens",
                 st.db_path, self.start_date, st.until or "(never ends)", *st.run_at)

    def _load_stations(self) -> None:
        """The station table maps stop ids of the board to small integers; load it once."""
        assert self.conn is not None
        if self.conn.execute("SELECT count(*) FROM station").fetchone()[0] > 0:
            return
        delay = 5.0
        for attempt in range(1, 5):
            try:
                db.load_stations(self.conn, self._fetch_geo(self.settings.base_url))
                if self.conn.execute("SELECT count(*) FROM station").fetchone()[0] > 0:
                    return
                raise ValueError("geo response contained no stations")
            except Exception as exc:
                if attempt == 4:
                    raise RuntimeError(f"cannot load the station list: {exc}") from exc
                log.warning("station list failed (%s), retrying in %.0fs", exc, delay)
                self._wait(delay)
                delay *= 2

    def _resolve_start_date(self) -> None:
        """An explicit ``--start-date`` wins and is stored; otherwise the stored one is used;
        on the very first run without either, the start is today (Athens)."""
        assert self.conn is not None
        stored = self.conn.execute("SELECT value FROM meta WHERE key='start_date'").fetchone()
        wanted = self.settings.start_date
        if wanted is None:
            wanted = date.fromisoformat(stored[0]) if stored else board.athens_today(self.clock())
        self.start_date = wanted
        with self.conn:
            self.conn.execute("INSERT INTO meta (key, value) VALUES ('start_date', ?) "
                              "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                              (wanted.isoformat(),))
        if self.settings.until is not None and self.settings.until < wanted:
            raise ValueError(f"--until {self.settings.until} is before the start date {wanted}")

    # ------------------------------------------------------------------- a pass
    def missing_days(self) -> list[str]:
        """Every day from the start date to ``until`` (or to the last final day when there is
        no ``until``) that is not stored yet, final or not."""
        assert self.conn is not None and self.start_date is not None
        end = self.settings.until or last_final_day(self.clock())
        done = {row[0] for row in self.conn.execute("SELECT date FROM board_fetch")}
        out, d = [], self.start_date
        while d <= end:
            if d.isoformat() not in done:
                out.append(d.isoformat())
            d += timedelta(days=1)
        return out

    def finished(self) -> bool:
        """True when ``until`` is set and every day through it is stored."""
        return self.settings.until is not None and not self.missing_days()

    def run_pass(self) -> PassResult:
        """Fetch every missing day that is final now. A failing day is skipped and stays missing."""
        assert self.conn is not None and self.idmaps is not None
        result = PassResult()
        final_through = last_final_day(self.clock())
        first_request = True
        for iso in self.missing_days():
            if self.stop_event.is_set():
                break
            if date.fromisoformat(iso) > final_through:
                result.waiting.append(iso)
                continue
            if not first_request:
                self._wait(self.settings.pause)
            first_request = False
            try:
                payload = self._fetch(iso, self.settings.base_url)
                # store_board refuses an answer without a usable schedule (an upstream fault,
                # not a day off), so that day stays missing and is retried
                n = board.store_board(self.conn, self.idmaps, iso, payload, now=self.clock)
            except Exception as exc:
                log.warning("board %s failed, will be retried: %s", iso, exc)
                result.failed.append(iso)
            else:
                log.info("board %s stored (%d schedules)", iso, n)
                result.stored.append(iso)
        return result

    # --------------------------------------------------------------------- loop
    def run(self, once: bool = False) -> int:
        """0 = ok, 3 = (``--once``) some final days failed. Runs until ``until`` is complete,
        or forever when there is no ``until``."""
        self.setup()
        try:
            while not self.stop_event.is_set():
                result = self.run_pass()
                log.info("pass done: stored=%d failed=%d waiting=%d",
                         len(result.stored), len(result.failed), len(result.waiting))
                if self.finished():
                    log.info("finished: every board day from %s through %s is stored",
                             self.start_date, self.settings.until)
                    return 0
                if once:
                    return 3 if result.failed else 0
                now = self.clock()
                wake = (now + self.settings.failed_retry_s if result.failed
                        else next_run_at(now, self.settings.run_at))
                wake = max(wake, now + MIN_PASS_INTERVAL_S)
                log.info("sleeping until %s",
                         datetime.fromtimestamp(wake, tz=timezone.utc).astimezone(ATHENS)
                         .strftime("%Y-%m-%d %H:%M Athens"))
                while not self.stop_event.is_set() and self.clock() < wake:
                    self._wait(min(30.0, wake - self.clock()))
            return 0
        finally:
            if self.conn is not None:
                try:
                    self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                except sqlite3.Error:
                    pass
                self.conn.close()
                self.conn = None


# ------------------------------------------------------------------------- CLI
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m collector.board_daily",
        description="Store the daily railway timetable (schedule board) of every day, once the "
                    "day is final. Never ends unless --until is given.")
    p.add_argument("--db", default="data/board.db", metavar="PATH",
                   help="SQLite database (default: data/board.db)")
    p.add_argument("--base-url", default="https://railway.gov.gr", metavar="URL")
    p.add_argument("--start-date", metavar="YYYY-MM-DD",
                   help="first board day to pull (may be in the past; stored in the database; "
                        "default: the stored one, else today in Athens)")
    p.add_argument("--until", metavar="YYYY-MM-DD",
                   help="last board day to pull, inclusive; the program exits when every day "
                        "through it is stored. Leave it out to never end.")
    p.add_argument("--run-at", default=DEFAULT_RUN_AT, metavar="HH:MM",
                   help=f"daily check time, Athens local time (default {DEFAULT_RUN_AT}; use "
                        "10:00 or later, a day is only final from 10:00 on the next day)")
    p.add_argument("--once", action="store_true",
                   help="do one pass and exit (for cron or a systemd timer)")
    p.add_argument("--pause", type=float, default=2.0, metavar="SECONDS",
                   help="pause between two requests (default 2)")
    p.add_argument("--log-file", metavar="PATH", help="also log to this file (rotating)")
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def settings_from_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> Settings:
    def day(text: str | None, flag: str) -> date | None:
        if text is None:
            return None
        try:
            return date.fromisoformat(text)
        except ValueError:
            parser.error(f"{flag}: not a date (YYYY-MM-DD): {text!r}")
    try:
        run_at = parse_run_at(args.run_at)
    except ValueError:
        parser.error(f"--run-at: not a time (HH:MM): {args.run_at!r}")
    start, until = day(args.start_date, "--start-date"), day(args.until, "--until")
    if start and until and until < start:
        parser.error("--until is before --start-date")
    return Settings(db_path=args.db, base_url=args.base_url, start_date=start, until=until,
                    run_at=run_at, pause=args.pause)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = settings_from_args(parser, args)
    logging.Formatter.converter = time.gmtime
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(fmt)
    root.addHandler(handler)
    if args.log_file:
        from logging.handlers import RotatingFileHandler
        os.makedirs(os.path.dirname(os.path.abspath(args.log_file)), exist_ok=True)
        fh = RotatingFileHandler(args.log_file, maxBytes=2_000_000, backupCount=3)
        fh.setFormatter(fmt)
        root.addHandler(fh)
    puller = BoardDaily(settings)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: puller.stop())
    try:
        return puller.run(once=args.once)
    except (RuntimeError, ValueError) as exc:
        log.error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
