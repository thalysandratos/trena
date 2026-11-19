"""Application loop and wiring (docs/DESIGN.md, sections 3, 5, 6 and 10).

``Collector`` owns the DB connection, the in-memory ``State``, the archive
writer and the two SSE reader threads. The main (writer) thread is the only
one that touches SQLite; the readers only push events into ``State`` and put
outages on a queue that the writer drains once per minute.

All I/O (clock, sleep, HTTP connect, geo/board fetches) is injectable, so the
tests run with fakes: no network, no real sleeping, no sockets.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import queue
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from . import board, db
from .archive import ArchiveWriter
from .readers import USER_AGENT, SSEReader, default_connect
from .sampler import State, load_last_stored_ts, tick

log = logging.getLogger(__name__)

ATHENS = ZoneInfo("Europe/Athens")
GEO_PATH = "/api/msd/geo"
TRAIN_STREAM_PATH = "/api/train-stream"
TELEMETRY_PATH = "/api/public/schedule-telemetry/stream"

BOARD_FINAL_HOUR = 10  # Athens hour on the day after D from which the board of D is final
SHUTDOWN_CATCHUP_BUDGET_S = 120.0
MAX_BOARD_RETRY_S = 6 * 3600
READER_JOIN_TIMEOUT_S = 10.0


@dataclass
class Config:
    db_path: str
    archive_dir: str
    base_url: str = "https://railway.gov.gr"
    start_date: date | None = None      # default: athens_today() at setup
    board_hour: int = 3                 # Athens local hour from which a new board day is due
    retry_after_s: int = 600            # per-date retry delay after a failed board fetch
    geo_retry_total_s: int = 600
    max_seconds: float | None = None    # stop by itself after this many seconds (dry runs)


def effective_today(now: float, board_hour: int = 3) -> date:
    """The Athens calendar date of ``now``, minus one day if the Athens local time
    is before ``board_hour``:00.

    This implements "fetch the board of D-2 at 03:00" without a separate trigger:
    pass ``effective_today(...)`` as ``today`` to ``board.due_dates``.
    """
    dt = datetime.fromtimestamp(now, tz=timezone.utc).astimezone(ATHENS)
    d = dt.date()
    if dt.hour < board_hour:
        d -= timedelta(days=1)
    return d


def board_ready_at(d: date) -> float:
    """Epoch from which the board of Athens day ``d`` is final: 10:00 Athens time on the
    next day. Trains that started on ``d`` run past midnight (latest arrival seen: 08:44)."""
    nxt = datetime(d.year, d.month, d.day, BOARD_FINAL_HOUR, tzinfo=ATHENS) + timedelta(days=1)
    return nxt.timestamp()


def default_fetch_geo(base_url: str) -> dict:
    """GET ``{base_url}/api/msd/geo?types=station&limit=10000``, parsed JSON."""
    url = f"{base_url}{GEO_PATH}?{urllib.parse.urlencode({'types': 'station', 'limit': 10000})}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def default_fetch_geo_full(base_url: str) -> bytes:
    """GET ``{base_url}/api/msd/geo?mode=all&limit=10000``, raw bytes."""
    url = f"{base_url}{GEO_PATH}?{urllib.parse.urlencode({'mode': 'all', 'limit': 10000})}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read()


class Collector:
    """One collector process: DB + state + archive + readers + minute loop."""

    def __init__(self, config: Config, *, clock: Callable[[], float] = time.time,
                 sleep: Callable[[float], None] | None = None,
                 connect: Callable[[str, float], Any] = default_connect,
                 fetch_geo: Callable[[str], dict] | None = None,
                 fetch_geo_full: Callable[[str], bytes] | None = None,
                 fetch_board: Callable[[str, str], dict] = board.fetch_board,
                 start_readers: bool = True) -> None:
        self.config = config
        self.clock = clock
        self._sleep = sleep
        self._connect = connect
        self._fetch_geo = fetch_geo or default_fetch_geo
        self._fetch_geo_full = fetch_geo_full or default_fetch_geo_full
        self._fetch_board = fetch_board
        self._start_readers = start_readers
        self._catchup_mode = False  # a board-only pull has no use for the full geo copy

        self.stop_event = threading.Event()
        self.state = State()
        self.outage_queue: queue.Queue = queue.Queue()
        self.retry_at: dict[str, int] = {}  # board date -> epoch until which it is skipped
        self.fail_count: dict[str, int] = {}  # board date -> consecutive failures

        self.conn: sqlite3.Connection | None = None
        self.idmaps: db.IdMaps | None = None
        self.archive: ArchiveWriter | None = None
        self.last_stored_ts: dict[int, int] = {}
        self.readers: list[SSEReader] = []
        self.start_date: date | None = config.start_date

        self._lifecycle_lock = threading.Lock()
        self._setup_done = False
        self._shutdown_done = False

    # ------------------------------------------------------------------ helpers

    def _sleep_for(self, seconds: float) -> None:
        """Sleep, defaulting to ``stop_event.wait`` so a stop interrupts it."""
        if self._sleep is not None:
            self._sleep(seconds)
        else:
            self.stop_event.wait(seconds)

    def _guarded(self, what: str, fn: Callable[[], Any]) -> None:
        """Run one step of the loop; an exception is logged and does not stop the loop."""
        try:
            fn()
        except Exception:
            log.exception("minute_step: %s failed", what)

    # ------------------------------------------------------------------- setup

    def setup(self) -> None:
        """Open the DB, load stations, resolve/persist start_date, build the readers.

        Idempotent. Stations must be loaded before any tick (D3)."""
        with self._lifecycle_lock:
            if self._setup_done:
                return
            cfg = self.config
            os.makedirs(os.path.dirname(os.path.abspath(cfg.db_path)), exist_ok=True)
            os.makedirs(cfg.archive_dir, exist_ok=True)
            self.conn = db.connect(cfg.db_path)
            db.init_schema(self.conn)
            # tiny key/value table for values that must survive a restart
            self.conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
            self.conn.commit()
            self.idmaps = db.IdMaps(self.conn)
            self.archive = ArchiveWriter(cfg.archive_dir)
            self.last_stored_ts = load_last_stored_ts(self.conn)
            self._load_stations()
            self._save_geo_full()
            self._resolve_start_date()
            if self._start_readers:
                self._make_readers()
            self._setup_done = True
            log.info("setup done: db=%s archive=%s start_date=%s stations=%d",
                     cfg.db_path, cfg.archive_dir, self.start_date,
                     self.conn.execute("SELECT count(*) FROM station").fetchone()[0])

    def _load_stations(self) -> None:
        """Load ``station`` if empty (D3), retrying the geo fetch with backoff."""
        assert self.conn is not None
        if self.conn.execute("SELECT count(*) FROM station").fetchone()[0] > 0:
            return
        total = self.config.geo_retry_total_s
        waited = 0.0
        delay = 5.0
        while True:
            try:
                geo = self._fetch_geo(self.config.base_url)
                db.load_stations(self.conn, geo)
                if self.conn.execute("SELECT count(*) FROM station").fetchone()[0] == 0:
                    raise ValueError("geo response loaded 0 stations")
                return
            except Exception as exc:
                if waited >= total:
                    raise RuntimeError(
                        f"geo load failed after {waited:.0f}s: {exc}") from exc
                log.warning("geo fetch failed (%s), retrying in %.0fs", exc, delay)
                self._sleep_for(delay)
                waited += delay
                delay = min(delay * 2, 60.0)

    def _save_geo_full(self) -> None:
        """Best effort: save the full geo dataset once as gzipped JSON (section 6)."""
        assert self.archive is not None
        if self._catchup_mode:
            return
        day = datetime.fromtimestamp(self.clock(), tz=timezone.utc).strftime("%Y%m%d")
        path = os.path.join(self.config.archive_dir, f"geo_{day}.json.gz")
        if os.path.exists(path):
            return
        try:
            raw = self._fetch_geo_full(self.config.base_url)
            with gzip.open(path, "wb") as f:
                f.write(raw)
            log.info("full geo dataset saved to %s", path)
        except Exception as exc:
            log.warning("full geo dataset not saved to %s: %s", path, exc)

    def _resolve_start_date(self) -> None:
        """Persist ``start_date`` in ``meta`` on first start; read it back on later
        starts so a restart never moves the start date forward (D5)."""
        assert self.conn is not None
        row = self.conn.execute("SELECT value FROM meta WHERE key='start_date'").fetchone()
        if row is not None:
            self.start_date = date.fromisoformat(row[0])
            return
        if self.start_date is None:
            self.start_date = board.athens_today(self.clock())
        with self.conn:
            self.conn.execute("INSERT OR IGNORE INTO meta (key, value) VALUES ('start_date', ?)",
                              (self.start_date.isoformat(),))

    def _new_reader(self, name: str) -> SSEReader:
        cfg = self.config
        path, callback = {
            "train-stream": (TRAIN_STREAM_PATH, self._on_train_stream_event),
            "telemetry": (TELEMETRY_PATH, self.state.handle_telemetry_event),
        }[name]
        return SSEReader(name, f"{cfg.base_url}{path}", callback, self.stop_event,
                         connect=self._connect, on_outage=self._on_outage,
                         on_lost=self._on_lost, now=self.clock)

    def _make_readers(self) -> None:
        self.readers = [self._new_reader("train-stream"), self._new_reader("telemetry")]

    def _ensure_readers(self) -> None:
        """Safety net: a reader thread that has died (it should not, see SSEReader.run) is
        logged as an ERROR and replaced, so a stream can never stay silently dead."""
        if self.stop_event.is_set():
            return
        for i, reader in enumerate(self.readers):
            if reader.ident is not None and not reader.is_alive():
                log.error("reader %s died, starting a replacement", reader.stream_name)
                self.readers[i] = self._new_reader(reader.stream_name)
                self.readers[i].start()

    def _on_train_stream_event(self, name: str, data: str) -> None:
        self.state.handle_train_stream_event(name, data)
        if name == "trainHistory" and self.archive is not None:
            # archive the full history once per (re)connect; never raise
            try:
                payload = json.loads(data)
            except Exception:
                return
            self.archive.write_event(int(self.clock()), "trainHistory", payload)

    def _on_lost(self, stream: str, start_ts: int, reason: str) -> None:
        # an outage is recorded when it OPENS (end_ts NULL), so a stream that never comes
        # back still leaves a trace in the database
        self.outage_queue.put((stream, start_ts, reason))

    def _on_outage(self, stream: str, start_ts: int, end_ts: int, reason: str) -> None:
        # only the writer thread touches SQLite, so just queue it
        self.outage_queue.put((stream, start_ts, end_ts, reason))

    # -------------------------------------------------------------- minute step

    def minute_step(self, now: int) -> None:
        """One minute: tick, archive snapshot, outage drain, board step.

        Each part is guarded, so one failure never stops the loop."""
        if self.conn is None:
            return
        self._guarded("tick", lambda: tick(self.state, self.conn, self.idmaps,
                                           self.last_stored_ts, now))
        self._guarded("archive snapshot", lambda: self.archive.write_snapshot(self.state, now))
        self._guarded("outage drain", self._drain_outages)
        self._guarded("reader check", self._ensure_readers)
        self._guarded("board step", lambda: self.board_step(now))
    def _drain_outages(self) -> None:
        assert self.conn is not None
        while True:
            try:
                item = self.outage_queue.get_nowait()
            except queue.Empty:
                return
            with self.conn:
                if len(item) == 3:  # (stream, start_ts, reason): outage opened
                    stream, start_ts, reason = item
                    self.conn.execute(
                        "INSERT INTO outage (stream, start_ts, end_ts, reason) VALUES (?,?,NULL,?)",
                        (stream, start_ts, reason))
                    continue
                stream, start_ts, end_ts, reason = item  # outage closed
                cur = self.conn.execute(
                    "UPDATE outage SET end_ts=?, reason=? WHERE stream=? AND start_ts=?"
                    " AND end_ts IS NULL", (end_ts, reason, stream, start_ts))
                if cur.rowcount == 0:  # no open row (for example from an earlier process)
                    self.conn.execute(
                        "INSERT INTO outage (stream, start_ts, end_ts, reason) VALUES (?,?,?,?)",
                        (stream, start_ts, end_ts, reason))

    def board_step(self, now: int) -> None:
        """Process at most ONE due board date per call, so a backlog drains at one
        date per minute and never blocks a tick (D4/D5)."""
        if self.conn is None or self.start_date is None:
            return
        today = effective_today(now, self.config.board_hour)
        for date_str in board.due_dates(self.conn, self.start_date, today):
            retry_at = self.retry_at.get(date_str)
            if retry_at is not None and now < retry_at:
                continue
            try:
                payload = self._fetch_board(date_str, self.config.base_url)
                if not payload.get("schedules"):
                    # an empty schedules list is an upstream problem: treat as a
                    # failure and keep the date due (same as board.run_catchup)
                    raise ValueError("empty schedules list")
                board.store_board(self.conn, self.idmaps, date_str, payload, now=self.clock)
            except Exception as exc:
                # back off 10 min, 20 min, ... up to 6 h; a date that keeps failing is a
                # data problem, so say so loudly. It stays due (a manual catch-up shows it).
                self.fail_count[date_str] = self.fail_count.get(date_str, 0) + 1
                delay = min(self.config.retry_after_s * 2 ** (self.fail_count[date_str] - 1),
                            MAX_BOARD_RETRY_S)
                self.retry_at[date_str] = now + delay
                log.warning("board %s failed (%d in a row), retry at %d: %s",
                            date_str, self.fail_count[date_str], self.retry_at[date_str], exc)
                if self.fail_count[date_str] == 5:
                    log.error("board %s has failed 5 times in a row, still due: check the API",
                              date_str)
            else:
                self.fail_count.pop(date_str, None)
                log.info("board %s stored", date_str)
            return  # at most one date per call, success or failure

    # --------------------------------------------------------------------- run

    def run(self) -> None:
        """Setup if needed, start the readers, then one minute_step per UTC minute
        boundary until ``stop()`` or ``max_seconds``; always ends with ``shutdown()``."""
        self.setup()
        for reader in self.readers:
            if not reader.is_alive():
                reader.start()
        started = self.clock()
        try:
            while not self.stop_event.is_set():
                now = self.clock()
                if self.config.max_seconds is not None and now - started >= self.config.max_seconds:
                    break
                boundary = (int(now) // 60 + 1) * 60
                while not self.stop_event.is_set():
                    remaining = boundary - self.clock()
                    if remaining <= 0:
                        break
                    if (self.config.max_seconds is not None
                            and self.clock() - started >= self.config.max_seconds):
                        break
                    self._sleep_for(min(1.0, remaining))  # slices of at most 1 s
                if self.stop_event.is_set():
                    break
                if (self.config.max_seconds is not None
                        and self.clock() - started >= self.config.max_seconds):
                    break
                self.minute_step(boundary)
        finally:
            self.shutdown()

    def stop(self) -> None:
        """Ask the loop to stop; idempotent and thread-safe."""
        self.stop_event.set()

    # ----------------------------------------------------------------- shutdown

    def shutdown(self) -> None:
        """Stop readers, one final minute step (no board step), a bounded final board
        catch-up over ALL due dates, then checkpoint and close. Safe to call twice."""
        with self._lifecycle_lock:
            if self._shutdown_done:
                return
            self._shutdown_done = True
        self.stop_event.set()
        for reader in self.readers:
            if reader.is_alive():
                reader.join(timeout=READER_JOIN_TIMEOUT_S)
        if self._setup_done and self.conn is not None:
            now = int(self.clock())
            self._guarded("final tick", lambda: tick(self.state, self.conn, self.idmaps,
                                                    self.last_stored_ts, now))
            self._guarded("final archive snapshot",
                          lambda: self.archive.write_snapshot(self.state, now))
            self._guarded("final outage drain", self._drain_outages)
            self._guarded("final board catch-up", self._final_board_catchup)
        self._checkpoint_and_close()

    def _final_board_catchup(self) -> None:
        """Catch up every still-due board date (D5), bounded to 120 s in total."""
        assert self.conn is not None and self.start_date is not None
        deadline = self.clock() + SHUTDOWN_CATCHUP_BUDGET_S

        def fetch(date_str: str, base_url: str) -> dict:
            if self.clock() >= deadline:
                raise RuntimeError("final board catch-up budget exhausted")
            return self._fetch_board(date_str, base_url)

        def sleep(seconds: float) -> None:
            if self.clock() >= deadline:
                raise RuntimeError("final board catch-up budget exhausted")
            self._sleep_for(seconds)

        today = effective_today(self.clock(), self.config.board_hour)
        stored = board.run_catchup(self.conn, self.idmaps, self.start_date, today,
                                   fetch=fetch, base_url=self.config.base_url, now=self.clock,
                                   sleep=sleep, pause=2.0)
        if stored:
            log.info("final board catch-up stored %d date(s): %s", len(stored), ", ".join(stored))

    def _checkpoint_and_close(self) -> None:
        conn = self.conn
        if conn is None:
            return
        try:
            conn.commit()
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error as exc:
            log.warning("wal checkpoint failed: %s", exc)
        try:
            conn.close()
        except sqlite3.Error:
            pass
        self.conn = None

    # -------------------------------------------------------------- catchup only

    def catchup_only(self, through: date | None = None, force: bool = False) -> list[str]:
        """No readers: setup, fetch and store the due board dates, close. Returns them.

        By default that is every date up to D-2. With ``through`` it is every date up to
        and including that day, which is how the last days of a run are fetched (the D-2
        rule alone would never reach them). A day whose board is not final yet
        (``board_ready_at``) is refused unless ``force``.
        """
        if through is not None and not force and self.clock() < board_ready_at(through):
            ready = datetime.fromtimestamp(board_ready_at(through), tz=ATHENS)
            raise ValueError(f"the board of {through} is not final before "
                             f"{ready:%Y-%m-%d %H:%M} Athens time; wait, or use --force")
        self._start_readers = False
        self._catchup_mode = True
        self.setup()
        assert self.conn is not None and self.start_date is not None
        # due_dates() covers start_date .. today-2, so through + 2 days makes `through` the last
        today = (through + timedelta(days=2) if through is not None
                 else effective_today(self.clock(), self.config.board_hour))
        stored = board.run_catchup(self.conn, self.idmaps, self.start_date, today,
                                   fetch=self._fetch_board, base_url=self.config.base_url,
                                   now=self.clock,
                                   sleep=self._sleep_for, pause=2.0)
        self._checkpoint_and_close()
        return stored
