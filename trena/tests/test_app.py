"""Tests for collector/app.py: the application loop.

All tests use fakes and a fake clock; no network, no sockets, no real sleeping.
"""

import gzip
import json
import os
import queue
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from datetime import date, datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collector import db
from collector.app import Collector, Config, effective_today
from collector.sampler import State
from collector.sse import iter_events_from_bytes

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLES = os.path.join(ROOT, "samples")
TRAIN_SSE = os.path.join(SAMPLES, "train_stream.sse")
TELEMETRY_SSE = os.path.join(SAMPLES, "telemetry_stream.sse")
GEO_JSON = os.path.join(SAMPLES, "geo_stations.json")

SNAPSHOT_EVENTS = {"trainPositionsUx", "scheduleCards", "scheduleTelemetry",
                   "scheduleTelemetryRaw", "scheduleTelemetryGhosts"}


def read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc).timestamp()


class FakeClock:
    def __init__(self, t):
        self.t = float(t)
        self.sleeps = []

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += s


def make_config(tmp, **over):
    cfg = dict(db_path=os.path.join(tmp, "trains.db"),
               archive_dir=os.path.join(tmp, "archive"),
               base_url="https://example.test", start_date=date(2026, 11, 19))
    cfg.update(over)
    return Config(**cfg)


def geo_payload():
    with open(GEO_JSON, encoding="utf-8") as f:
        return json.load(f)


def small_board(date_str):
    return {"schedules": [{"_id": f"b-{date_str}", "scheduleNumber": "N1",
                           "origin": {"name": "A"}, "destination": {"name": "B"},
                           "routeStations": []}]}


def make_collector(tmp, clock, *, fetch_geo=None, fetch_geo_full=None, fetch_board=None,
                   start_readers=False, **cfg_over):
    calls = {"geo": 0, "geo_full": 0, "board": []}

    def fake_geo(base_url):
        calls["geo"] += 1
        return geo_payload()

    def fake_geo_full(base_url):
        calls["geo_full"] += 1
        return b'{"full": true}'

    def fake_board(date_str, base_url):
        calls["board"].append(date_str)
        return small_board(date_str)

    col = Collector(make_config(tmp, **cfg_over), clock=clock, sleep=clock.sleep,
                    fetch_geo=fetch_geo or fake_geo,
                    fetch_geo_full=fetch_geo_full or fake_geo_full,
                    fetch_board=fetch_board or fake_board,
                    start_readers=start_readers)
    return col, calls


def feed_samples(state):
    for name, data in iter_events_from_bytes(read_bytes(TRAIN_SSE)):
        state.handle_train_stream_event(name, data)
    for name, data in iter_events_from_bytes(read_bytes(TELEMETRY_SSE)):
        state.handle_telemetry_event(name, data)


def read_archive_lines(path):
    with gzip.open(path, "rb") as f:
        raw = f.read().decode("utf-8")
    return [json.loads(line) for line in raw.splitlines() if line]


class TestEffectiveToday(unittest.TestCase):
    def test_winter_0259_vs_0300(self):
        # January: Athens = UTC+2, so 03:00 local is 01:00Z
        self.assertEqual(effective_today(utc(2026, 1, 15, 0, 59)), date(2026, 1, 14))
        self.assertEqual(effective_today(utc(2026, 1, 15, 1, 0)), date(2026, 1, 15))
        self.assertEqual(effective_today(utc(2026, 1, 15, 12, 0)), date(2026, 1, 15))

    def test_summer_dst(self):
        # July: Athens = UTC+3 (EEST), so 03:00 local is 00:00Z
        self.assertEqual(effective_today(utc(2026, 7, 14, 23, 59)), date(2026, 7, 14))
        self.assertEqual(effective_today(utc(2026, 7, 15, 0, 0)), date(2026, 7, 15))
        self.assertEqual(effective_today(utc(2026, 7, 15, 21, 0)), date(2026, 7, 15))

    def test_custom_board_hour(self):
        self.assertEqual(effective_today(utc(2026, 1, 15, 1, 0), board_hour=5), date(2026, 1, 14))
        self.assertEqual(effective_today(utc(2026, 1, 15, 3, 0), board_hour=5), date(2026, 1, 15))


class TestSetup(unittest.TestCase):
    def test_stations_loaded_from_geo(self):
        with tempfile.TemporaryDirectory() as tmp:
            clock = FakeClock(utc(2026, 11, 19, 18, 0))
            col, calls = make_collector(tmp, clock)
            col.setup()
            self.assertEqual(calls["geo"], 1)
            self.assertEqual(col.conn.execute("SELECT count(*) FROM station").fetchone()[0], 389)
            # the full geo reference file was saved once, gzipped
            day = datetime.fromtimestamp(clock.t, tz=timezone.utc).strftime("%Y%m%d")
            path = os.path.join(tmp, "archive", f"geo_{day}.json.gz")
            self.assertTrue(os.path.exists(path))
            with gzip.open(path, "rb") as f:
                self.assertEqual(f.read(), b'{"full": true}')
            self.assertEqual(calls["geo_full"], 1)
            col.shutdown()

    def test_second_setup_does_not_refetch_geo(self):
        with tempfile.TemporaryDirectory() as tmp:
            clock = FakeClock(utc(2026, 11, 19, 18, 0))
            col, calls = make_collector(tmp, clock)
            col.setup()
            col.shutdown()

            def boom(base_url):
                raise AssertionError("fetch_geo must not be called again")

            col2, calls2 = make_collector(tmp, clock, fetch_geo=boom,
                                          fetch_geo_full=lambda u: (_ for _ in ()).throw(
                                              AssertionError("no full geo either")))
            col2.setup()
            self.assertEqual(calls2["geo"], 0)
            self.assertEqual(col2.conn.execute("SELECT count(*) FROM station").fetchone()[0], 389)
            col2.shutdown()

    def test_geo_retry_then_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            clock = FakeClock(utc(2026, 11, 19, 18, 0))
            attempts = []

            def flaky(base_url):
                attempts.append(1)
                if len(attempts) < 3:
                    raise OSError("down")
                return geo_payload()

            col, _ = make_collector(tmp, clock, fetch_geo=flaky)
            col.setup()
            self.assertEqual(len(attempts), 3)
            self.assertEqual(clock.sleeps, [5.0, 10.0])  # 5 s doubling to 60 s cap
            self.assertEqual(col.conn.execute("SELECT count(*) FROM station").fetchone()[0], 389)
            col.shutdown()

    def test_geo_permanent_failure_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            clock = FakeClock(utc(2026, 11, 19, 18, 0))

            def always_down(base_url):
                raise OSError("down")

            col, _ = make_collector(tmp, clock, fetch_geo=always_down, geo_retry_total_s=12)
            with self.assertRaises(RuntimeError):
                col.setup()
            self.assertEqual(clock.sleeps, [5.0, 10.0])  # gave up once 15 s >= 12 s

    def test_geo_full_failure_is_not_fatal(self):
        with tempfile.TemporaryDirectory() as tmp:
            clock = FakeClock(utc(2026, 11, 19, 18, 0))

            def no_full(base_url):
                raise OSError("no full dataset")

            col, _ = make_collector(tmp, clock, fetch_geo_full=no_full)
            col.setup()  # must not raise
            day = datetime.fromtimestamp(clock.t, tz=timezone.utc).strftime("%Y%m%d")
            self.assertFalse(os.path.exists(os.path.join(tmp, "archive", f"geo_{day}.json.gz")))
            col.shutdown()

    def test_start_date_survives_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            clock = FakeClock(utc(2026, 11, 19, 18, 0))  # Athens day 2026-11-19
            col, _ = make_collector(tmp, clock, start_date=None)
            col.setup()
            self.assertEqual(col.start_date, date(2026, 11, 19))
            self.assertEqual(col.conn.execute(
                "SELECT value FROM meta WHERE key='start_date'").fetchone()[0], "2026-11-19")
            col.shutdown()
            # a "restart" a week later must not move the start date forward
            later = FakeClock(utc(2026, 11, 26, 18, 0))
            col2, _ = make_collector(tmp, later, start_date=None)
            col2.setup()
            self.assertEqual(col2.start_date, date(2026, 11, 19))
            # even an explicit later --start-date is ignored once persisted
            col2.shutdown()
            col3, _ = make_collector(tmp, later, start_date=date(2027, 1, 1))
            col3.setup()
            self.assertEqual(col3.start_date, date(2026, 11, 19))
            col3.shutdown()


class TestMinuteStep(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.clock = FakeClock(utc(2026, 11, 19, 18, 17, 10))
        self.col, self.calls = make_collector(self.tmp.name, self.clock)
        self.col.setup()
        feed_samples(self.col.state)
        self.now = 1795112280  # 2026-11-19T18:18:00Z, a minute boundary

    def tearDown(self):
        self.col.shutdown()
        self.tmp.cleanup()

    def archive_path(self):
        return os.path.join(self.tmp.name, "archive", "2026-11-19.jsonl.gz")

    def test_rows_and_archive_written(self):
        self.col.minute_step(self.now)
        n_rows = self.col.conn.execute("SELECT count(*) FROM position").fetchone()[0]
        self.assertGreater(n_rows, 0)
        recs = read_archive_lines(self.archive_path())
        names = {r["ev"] for r in recs}
        self.assertEqual(names, SNAPSHOT_EVENTS)  # all 5 event names
        for r in recs:
            self.assertEqual(r["t"], self.now)
            self.assertIsInstance(r["data"], (dict, list))
        # the trainHistory event from the sample was archived too (via the reader
        # callback path; here fed directly through the state handler it is not,
        # so only the snapshot events are expected above)

    def test_second_step_without_new_events_writes_nothing(self):
        self.col.minute_step(self.now)
        rows1 = self.col.conn.execute("SELECT count(*) FROM position").fetchone()[0]
        lines1 = len(read_archive_lines(self.archive_path()))
        self.assertGreater(rows1, 0)
        self.assertGreater(lines1, 0)
        self.col.minute_step(self.now + 60)
        rows2 = self.col.conn.execute("SELECT count(*) FROM position").fetchone()[0]
        lines2 = len(read_archive_lines(self.archive_path()))
        self.assertEqual(rows2, rows1)
        self.assertEqual(lines2, lines1)

    def test_outage_queue_drained_into_table(self):
        self.col.outage_queue.put(("train-stream", 100, 200, "EOF"))
        self.col.minute_step(self.now)
        row = self.col.conn.execute(
            "SELECT stream, start_ts, end_ts, reason FROM outage").fetchone()
        self.assertEqual(row, ("train-stream", 100, 200, "EOF"))
        self.assertTrue(self.col.outage_queue.empty())

    def test_one_failing_part_does_not_stop_the_others(self):
        self.col.shutdown()
        self.tmp.cleanup()
        self.tmp = tempfile.TemporaryDirectory()
        clock = FakeClock(utc(2026, 11, 21, 1, 0))  # 03:00 Athens -> 11-19 is due
        self.clock = clock
        self.col, self.calls = make_collector(self.tmp.name, clock, start_date=date(2026, 11, 17))
        self.col.setup()
        feed_samples(self.col.state)

        def bad_board(date_str, base_url):
            raise OSError("boom")

        self.col._fetch_board = bad_board
        self.col.minute_step(self.now)  # must not raise
        self.assertGreater(self.col.conn.execute("SELECT count(*) FROM position").fetchone()[0], 0)


class TestBoardStep(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        # 2026-11-21T01:00:00Z is exactly 03:00 in Athens (UTC+2 in November)
        self.after_3am = utc(2026, 11, 21, 1, 0)
        self.before_3am = utc(2026, 11, 21, 0, 59)
        self.clock = FakeClock(self.after_3am)
        self.col, self.calls = make_collector(self.tmp.name, self.clock,
                                             start_date=date(2026, 11, 19))
        self.col.setup()

    def tearDown(self):
        self.col.shutdown()
        self.tmp.cleanup()

    def test_newest_day_not_due_before_3am(self):
        # at 02:59 Athens the board day is still 2 days back: nothing due for start 11-19
        self.col.board_step(int(self.before_3am))
        self.assertEqual(self.calls["board"], [])

    def test_newest_day_due_after_3am(self):
        self.col.board_step(int(self.after_3am))  # 03:00 Athens -> today = 11-21
        self.assertEqual(self.calls["board"], ["2026-11-19"])
        self.assertEqual(self.col.conn.execute(
            "SELECT count(*) FROM board_fetch WHERE date='2026-11-19'").fetchone()[0], 1)

    def test_at_most_one_date_per_call(self):
        # due: 17, 18, 19 (start_date was persisted as 11-19 at setup, so re-setup on a
        # fresh DB with start 11-17)
        self.col.shutdown()
        self.tmp.cleanup()
        self.tmp = tempfile.TemporaryDirectory()
        self.col, self.calls = make_collector(self.tmp.name, self.clock,
                                             start_date=date(2026, 11, 17))
        self.col.setup()
        self.col.board_step(int(self.after_3am))
        self.assertEqual(self.calls["board"], ["2026-11-17"])
        self.col.board_step(int(self.after_3am))
        self.col.board_step(int(self.after_3am))
        self.assertEqual(self.calls["board"], ["2026-11-17", "2026-11-18", "2026-11-19"])
        self.col.board_step(int(self.after_3am))  # nothing left
        self.assertEqual(len(self.calls["board"]), 3)

    def test_failed_date_retried_only_after_retry_after_s(self):
        attempts = []

        def failing(date_str, base_url):
            attempts.append(date_str)
            raise OSError("board down")

        self.col._fetch_board = failing
        now = int(self.after_3am)
        self.col.config.retry_after_s = 600
        self.col.board_step(now)
        self.assertEqual(attempts, ["2026-11-19"])
        self.assertEqual(self.col.retry_at["2026-11-19"], now + 600)
        self.assertEqual(self.col.conn.execute("SELECT count(*) FROM board_fetch").fetchone()[0], 0)
        self.col.board_step(now + 599)  # still in the retry window
        self.assertEqual(attempts, ["2026-11-19"])
        self.col.board_step(now + 600)  # retry due
        self.assertEqual(attempts, ["2026-11-19", "2026-11-19"])
        # second failure in a row: the delay doubles (10 min -> 20 min), counted from the retry
        self.assertEqual(self.col.retry_at["2026-11-19"], now + 600 + 1200)

    def test_empty_schedules_counts_as_failure(self):
        self.col._fetch_board = lambda date_str, base_url: {"schedules": []}
        now = int(self.after_3am)
        self.col.board_step(now)
        self.assertEqual(self.col.conn.execute("SELECT count(*) FROM board_fetch").fetchone()[0], 0)
        self.assertEqual(self.col.retry_at["2026-11-19"], now + 600)


class TestShutdownAndCatchup(unittest.TestCase):
    def test_shutdown_twice_leaves_db_readable(self):
        with tempfile.TemporaryDirectory() as tmp:
            clock = FakeClock(utc(2026, 11, 19, 18, 0))
            col, _ = make_collector(tmp, clock)
            col.setup()
            feed_samples(col.state)
            col.minute_step(1795112280)
            col.shutdown()
            col.shutdown()  # safe twice
            self.assertIsNone(col.conn)
            conn = sqlite3.connect(os.path.join(tmp, "trains.db"))
            self.assertGreater(conn.execute("SELECT count(*) FROM position").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT count(*) FROM station").fetchone()[0], 389)
            conn.close()

    def test_shutdown_writes_the_final_positions(self):
        """Samples that arrived after the last minute boundary must not be lost on stop."""
        with tempfile.TemporaryDirectory() as tmp:
            clock = FakeClock(utc(2026, 11, 19, 18, 0))
            col, _ = make_collector(tmp, clock)
            col.setup()
            feed_samples(col.state)  # no minute_step at all
            col.shutdown()
            conn = sqlite3.connect(os.path.join(tmp, "trains.db"))
            self.assertGreater(conn.execute("SELECT count(*) FROM position").fetchone()[0], 0)
            conn.close()

    def test_catchup_only_fetches_all_due_dates(self):
        with tempfile.TemporaryDirectory() as tmp:
            clock = FakeClock(utc(2026, 11, 21, 1, 0))  # 03:00 Athens, today = 11-21
            col, calls = make_collector(tmp, clock, start_date=date(2026, 11, 17))
            stored = col.catchup_only()
            self.assertEqual(stored, ["2026-11-17", "2026-11-18", "2026-11-19"])
            self.assertEqual(calls["board"], ["2026-11-17", "2026-11-18", "2026-11-19"])
            self.assertEqual(clock.sleeps, [2.0, 2.0])  # pause between requests only
            self.assertEqual(col.readers, [])  # no readers in catchup-only mode
            self.assertIsNone(col.conn)  # closed
            conn = sqlite3.connect(os.path.join(tmp, "trains.db"))
            self.assertEqual(conn.execute("SELECT count(*) FROM board_fetch").fetchone()[0], 3)
            self.assertEqual(conn.execute("SELECT count(*) FROM schedule").fetchone()[0], 3)
            conn.close()


class FakeStreamConn:
    """Stands in for an SSE HTTP response: serves the sample bytes in chunks, then
    blocks on ``stop_event`` and raises TimeoutError (the reader treats that as
    'no data yet' and checks the stop event)."""

    def __init__(self, data, stop_event, chunk=65536):
        self.data = data
        self.pos = 0
        self.stop_event = stop_event
        self.chunk = chunk
        self.closed = False

    def read1(self, n):
        if self.pos < len(self.data):
            out = self.data[self.pos:self.pos + self.chunk]
            self.pos += len(out)
            return out
        self.stop_event.wait(0.05)
        raise TimeoutError()

    def close(self):
        self.closed = True


class TestRunIntegration(unittest.TestCase):
    """One end-to-end test: real SSEReader threads over fake connections, a fake clock
    that jumps to minute boundaries, a small max_seconds."""

    def test_run_stores_positions_and_stops_threads(self):
        with tempfile.TemporaryDirectory() as tmp:
            start = utc(2026, 11, 19, 18, 17, 10)
            clock = FakeClock(start)

            def real_sleep(s):  # advance the fake clock, give the threads a little real time
                clock.t += s
                time.sleep(0.01)

            clock.sleep = real_sleep
            train_bytes = read_bytes(TRAIN_SSE)
            tel_bytes = read_bytes(TELEMETRY_SSE)

            def fake_connect(url, timeout):
                data = train_bytes if "train-stream" in url else tel_bytes
                return FakeStreamConn(data, col.stop_event)

            col, calls = make_collector(tmp, clock, start_readers=True)
            col._connect = fake_connect
            col.config.max_seconds = 130  # two minute boundaries, then stop
            col.run()
            # rows were stored by the minute ticks (shutdown() closed the connection,
            # so reopen the file read-only for the assertions)
            conn = sqlite3.connect(os.path.join(tmp, "trains.db"))
            self.assertGreater(conn.execute("SELECT count(*) FROM position").fetchone()[0], 0)
            conn.close()
            # the reader threads are gone
            for reader in col.readers:
                self.assertFalse(reader.is_alive())
            # the archive got the snapshot events
            path = os.path.join(tmp, "archive", "2026-11-19.jsonl.gz")
            self.assertTrue(os.path.exists(path))
            names = {r["ev"] for r in read_archive_lines(path)}
            self.assertTrue({"trainPositionsUx", "scheduleTelemetryRaw"} <= names)
            # trainHistory was archived once per connect by the reader callback
            self.assertIn("trainHistory", names)
            # stop() is idempotent
            col.stop()
            col.stop()



class TestOutageRecordedAtLossTime(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.clock = FakeClock(utc(2026, 11, 19, 18, 17, 10))
        self.col, _ = make_collector(self.tmp.name, self.clock)
        self.col.setup()

    def tearDown(self):
        self.col.shutdown()
        self.tmp.cleanup()

    def rows(self):
        return self.col.conn.execute(
            "SELECT stream, start_ts, end_ts, reason FROM outage ORDER BY rowid").fetchall()

    def test_open_outage_is_in_the_database_before_recovery(self):
        self.col._on_lost("telemetry", 1000, "EOF")  # what the reader callback does
        self.col.minute_step(1795112280)
        self.assertEqual(self.rows(), [("telemetry", 1000, None, "EOF")])

    def test_recovery_closes_the_open_row_instead_of_adding_one(self):
        self.col._on_lost("telemetry", 1000, "EOF")
        self.col.minute_step(1795112280)
        self.col._on_outage("telemetry", 1000, 1300, "idle 30s")  # recovered, last reason wins
        self.col.minute_step(1795112340)
        self.assertEqual(self.rows(), [("telemetry", 1000, 1300, "idle 30s")])

    def test_recovery_without_an_open_row_still_records_it(self):
        self.col._on_outage("telemetry", 1000, 1300, "EOF")
        self.col.minute_step(1795112280)
        self.assertEqual(self.rows(), [("telemetry", 1000, 1300, "EOF")])


class TestBoardRetryEscalation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.after_3am = utc(2026, 11, 21, 1, 0)
        self.clock = FakeClock(self.after_3am)
        self.col, _ = make_collector(self.tmp.name, self.clock, start_date=date(2026, 11, 19))
        self.col.setup()
        self.col.config.retry_after_s = 600
        self.attempts = []

        def failing(date_str, base_url):
            self.attempts.append(date_str)
            raise OSError("down")

        self.col._fetch_board = failing

    def tearDown(self):
        self.col.shutdown()
        self.tmp.cleanup()

    def test_delay_doubles_and_is_capped(self):
        now = int(self.after_3am)
        delays = []
        for _ in range(8):
            self.col.board_step(now)
            delays.append(self.col.retry_at["2026-11-19"] - now)
            now = self.col.retry_at["2026-11-19"]
        self.assertEqual(delays[:4], [600, 1200, 2400, 4800])
        self.assertEqual(max(delays), 6 * 3600)  # MAX_BOARD_RETRY_S
        self.assertEqual(self.col.fail_count["2026-11-19"], 8)

    def test_fifth_failure_logs_an_error_and_success_resets_the_count(self):
        now = int(self.after_3am)
        with self.assertLogs("collector.app", level="ERROR") as cm:
            for _ in range(5):
                self.col.board_step(now)
                now = self.col.retry_at["2026-11-19"]
        self.assertEqual(len(cm.records), 1)  # exactly one ERROR, at the 5th failure
        self.col._fetch_board = lambda d, b: {"schedules": [
            {"_id": "s1", "scheduleNumber": "N", "routeStations": []}]}
        self.col.board_step(now)
        self.assertNotIn("2026-11-19", self.col.fail_count)

    def test_fetched_at_uses_the_injected_clock(self):
        self.col._fetch_board = lambda d, b: {"schedules": [
            {"_id": "s1", "scheduleNumber": "N", "routeStations": []}]}
        self.clock.t = 1234567890.0 if hasattr(self.clock, "t") else None
        self.col.clock = lambda: 1234567890.0
        self.col.board_step(int(self.after_3am))
        got = self.col.conn.execute("SELECT fetched_at FROM board_fetch").fetchone()[0]
        self.assertEqual(got, 1234567890)


class TestReaderSafetyNet(unittest.TestCase):
    def test_a_dead_reader_is_replaced_and_logged(self):
        with tempfile.TemporaryDirectory() as tmp:
            clock = FakeClock(utc(2026, 11, 19, 18, 17, 10))
            col, _ = make_collector(tmp, clock, start_readers=True)
            # the readers must never reach a network: a connection that always times out
            col._connect = lambda url, timeout: (_ for _ in ()).throw(TimeoutError("fake"))
            col.setup()
            dead = threading.Thread(target=lambda: None)
            dead.start()
            dead.join()
            old = col.readers[0]
            col.readers[0] = SSEReaderStub(old.stream_name, dead)
            try:
                with self.assertLogs("collector.app", level="ERROR") as cm:
                    col._ensure_readers()
                self.assertEqual(len(cm.records), 1)
                new = col.readers[0]
                self.assertIsNot(new, old)
                self.assertTrue(new.is_alive() or new.ident is not None)  # it was started
                self.assertEqual(new.stream_name, old.stream_name)
            finally:
                col.shutdown()

    def test_the_minute_step_runs_the_check(self):
        with tempfile.TemporaryDirectory() as tmp:
            col, _ = make_collector(tmp, FakeClock(utc(2026, 11, 19, 18, 17, 10)), start_readers=True)
            col._connect = lambda url, timeout: (_ for _ in ()).throw(TimeoutError("fake"))
            col.setup()
            dead = threading.Thread(target=lambda: None)
            dead.start()
            dead.join()
            col.readers[1] = SSEReaderStub("telemetry", dead)
            try:
                with self.assertLogs("collector.app", level="ERROR"):
                    col.minute_step(1795112280)
                self.assertNotIsInstance(col.readers[1], SSEReaderStub)
            finally:
                col.shutdown()

    def test_a_healthy_or_unstarted_reader_is_left_alone(self):
        with tempfile.TemporaryDirectory() as tmp:
            col, _ = make_collector(tmp, FakeClock(utc(2026, 11, 19, 18, 17, 10)), start_readers=True)
            col.setup()
            before = list(col.readers)  # built but never started: ident is None
            col._ensure_readers()
            self.assertEqual(col.readers, before)
            col.shutdown()


class SSEReaderStub:
    """Looks like a reader whose thread has already ended."""

    def __init__(self, stream_name, thread):
        self.stream_name = stream_name
        self.ident = thread.ident

    def is_alive(self):
        return False

    def join(self, timeout=None):
        pass


class TestLoneBoardPull(unittest.TestCase):
    """The last days of a run: the D-2 rule never reaches them, --through does."""

    def collector(self, tmp, when):
        return make_collector(tmp, FakeClock(when), start_date=date(2026, 11, 17))

    def test_board_ready_at_is_10_am_athens_on_the_next_day(self):
        from collector.app import board_ready_at
        self.assertEqual(board_ready_at(date(2026, 11, 19)), utc(2026, 11, 20, 8, 0))  # EET, UTC+2
        self.assertEqual(board_ready_at(date(2026, 7, 19)), utc(2026, 7, 20, 7, 0))    # EEST, UTC+3

    def test_through_fetches_the_last_two_days_that_the_default_rule_skips(self):
        with tempfile.TemporaryDirectory() as tmp:
            # the morning after day 19 (11:00 Athens on 11-20)
            when = utc(2026, 11, 20, 9, 0)
            col, calls = self.collector(tmp, when)
            self.assertEqual(col.catchup_only(through=date(2026, 11, 19)),
                             ["2026-11-17", "2026-11-18", "2026-11-19"])
            self.assertEqual(calls["board"], ["2026-11-17", "2026-11-18", "2026-11-19"])
        with tempfile.TemporaryDirectory() as tmp:
            col, calls = self.collector(tmp, when)
            # the default rule stops at D-2 = 11-18: the last day (11-19) is never fetched by it
            self.assertEqual(col.catchup_only(), ["2026-11-17", "2026-11-18"])

    def test_a_day_that_is_not_final_yet_is_refused_and_nothing_is_fetched(self):
        with tempfile.TemporaryDirectory() as tmp:
            col, calls = self.collector(tmp, utc(2026, 11, 20, 7, 59))  # 09:59 Athens
            with self.assertRaises(ValueError) as cm:
                col.catchup_only(through=date(2026, 11, 19))
            self.assertIn("2026-11-20 10:00", str(cm.exception))
            self.assertEqual(calls["board"], [])
            self.assertIsNone(col.conn)  # refused before the database was even opened
            self.assertFalse(os.path.exists(os.path.join(tmp, "trains.db")))

    def test_a_board_only_pull_does_not_download_the_full_geo_copy(self):
        with tempfile.TemporaryDirectory() as tmp:
            col, calls = self.collector(tmp, utc(2026, 11, 20, 9, 0))
            col.catchup_only(through=date(2026, 11, 19))
            self.assertEqual(calls["geo_full"], 0)
            self.assertFalse([f for f in os.listdir(os.path.join(tmp, "archive")) if f.startswith("geo_")])

    def test_force_overrides_the_finality_check(self):
        with tempfile.TemporaryDirectory() as tmp:
            col, calls = self.collector(tmp, utc(2026, 11, 20, 7, 59))
            self.assertEqual(col.catchup_only(through=date(2026, 11, 19), force=True),
                             ["2026-11-17", "2026-11-18", "2026-11-19"])

if __name__ == "__main__":
    unittest.main()
