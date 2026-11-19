"""Tests for the daily board puller (collector/board_daily.py). Fakes only, no network."""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collector import board_daily as bd  # noqa: E402
from collector.app import board_ready_at  # noqa: E402

ATHENS = ZoneInfo("Europe/Athens")
GEO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "samples", "geo_stations.json")


def athens(y, mo, d, h=0, mi=0) -> float:
    return datetime(y, mo, d, h, mi, tzinfo=ATHENS).timestamp()


class FakeClock:
    def __init__(self, t: float) -> None:
        self.t = t
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds


def board_payload(iso: str, n: int = 2) -> dict:
    return {"schedules": [
        {"_id": f"{iso}-{i}", "scheduleNumber": f"{i}-{iso}", "trainNumber": str(i),
         "serviceType": "Suburban", "corridor": "X", "status": "completed", "delay": i,
         "scheduledDeparture": f"{iso}T08:00:00.000Z", "scheduledArrival": f"{iso}T09:00:00.000Z",
         "actualDeparture": f"{iso}T08:01:00.000Z", "actualArrival": f"{iso}T09:0{i}:00.000Z",
         "origin": {"name": "A"}, "destination": {"name": "B"}, "routeStations": []}
        for i in range(n)]}


def geo() -> dict:
    with open(GEO, encoding="utf-8") as f:
        return json.load(f)


class Base(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "board.db")
        self.fetched: list[str] = []
        self.fail: set[str] = set()
        self.empty: set[str] = set()
        self.geo_calls = 0

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def fetch(self, iso: str, base_url: str) -> dict:
        self.fetched.append(iso)
        if iso in self.fail:
            raise OSError("down")
        return {"schedules": []} if iso in self.empty else board_payload(iso)

    def fetch_geo(self, base_url: str) -> dict:
        self.geo_calls += 1
        return geo()

    def make(self, now: float, **kw) -> tuple[bd.BoardDaily, FakeClock]:
        clock = FakeClock(now)
        settings = bd.Settings(db_path=self.db_path, **kw)
        return bd.BoardDaily(settings, clock=clock, sleep=clock.sleep, fetch=self.fetch,
                             fetch_geo=self.fetch_geo), clock

    def stored(self) -> list[str]:
        c = sqlite3.connect(self.db_path)
        try:
            return [r[0] for r in c.execute("SELECT date FROM board_fetch ORDER BY date")]
        finally:
            c.close()


class TestFinality(unittest.TestCase):
    def test_last_final_day_around_ten(self):
        self.assertEqual(bd.last_final_day(athens(2026, 11, 20, 10, 0)), date(2026, 11, 19))
        self.assertEqual(bd.last_final_day(athens(2026, 11, 20, 9, 59)), date(2026, 11, 18))
        self.assertEqual(bd.last_final_day(athens(2026, 11, 20, 0, 0)), date(2026, 11, 18))

    def test_agrees_with_board_ready_at_everywhere_incl_both_dst_changes(self):
        # d is final at `now` exactly when now >= board_ready_at(d)
        for start in (datetime(2026, 3, 27, tzinfo=timezone.utc),    # spring forward 2026-03-29
                      datetime(2026, 10, 24, tzinfo=timezone.utc),   # fall back 2026-10-25
                      datetime(2026, 7, 10, tzinfo=timezone.utc)):
            for step in range(0, 6 * 24 * 4):                         # every 15 minutes, 6 days
                now = (start + timedelta(minutes=15 * step)).timestamp()
                last = bd.last_final_day(now)
                self.assertLessEqual(board_ready_at(last), now, (start, step))
                self.assertGreater(board_ready_at(last + timedelta(days=1)), now, (start, step))


class TestTimes(unittest.TestCase):
    def test_parse_run_at(self):
        self.assertEqual(bd.parse_run_at("10:30"), (10, 30))
        self.assertEqual(bd.parse_run_at("3:05"), (3, 5))
        for bad in ("25:00", "10:61", "10", "ab:cd", ""):
            with self.assertRaises(ValueError):
                bd.parse_run_at(bad)

    def test_next_run_at_is_strictly_after_now(self):
        self.assertEqual(bd.next_run_at(athens(2026, 11, 20, 9, 0), (10, 30)), athens(2026, 11, 20, 10, 30))
        self.assertEqual(bd.next_run_at(athens(2026, 11, 20, 10, 30), (10, 30)), athens(2026, 11, 21, 10, 30))
        self.assertEqual(bd.next_run_at(athens(2026, 11, 20, 23, 59), (10, 30)), athens(2026, 11, 21, 10, 30))

    def test_next_run_at_across_a_dst_change(self):
        # the night of 2026-03-28 to 29: Athens clocks jump forward, 10:30 is still 10:30 local
        got = bd.next_run_at(athens(2026, 3, 28, 12, 0), (10, 30))
        self.assertEqual(datetime.fromtimestamp(got, tz=ATHENS).strftime("%Y-%m-%d %H:%M"), "2026-03-29 10:30")


class TestPass(Base):
    def test_only_final_days_are_fetched_and_the_rest_wait(self):
        puller, _ = self.make(athens(2026, 11, 20, 9, 30), start_date=date(2026, 11, 17))
        puller.setup()
        result = puller.run_pass()
        self.assertEqual(result.stored, ["2026-11-17", "2026-11-18"])
        self.assertEqual(self.fetched, ["2026-11-17", "2026-11-18"])
        self.assertEqual(self.stored(), ["2026-11-17", "2026-11-18"])

    def test_a_second_pass_fetches_nothing_new(self):
        puller, _ = self.make(athens(2026, 11, 20, 10, 30), start_date=date(2026, 11, 17))
        puller.setup()
        puller.run_pass()
        self.fetched.clear()
        self.assertEqual(puller.run_pass().stored, [])
        self.assertEqual(self.fetched, [])

    def test_a_day_becomes_fetchable_when_it_turns_final(self):
        puller, clock = self.make(athens(2026, 11, 20, 9, 30), start_date=date(2026, 11, 18))
        puller.setup()
        self.assertEqual(puller.run_pass().stored, ["2026-11-18"])
        clock.t = athens(2026, 11, 20, 10, 0)  # the board of 11-19 is final from now on
        self.assertEqual(puller.run_pass().stored, ["2026-11-19"])

    def test_requests_are_spaced_by_the_pause_but_not_before_the_first(self):
        puller, clock = self.make(athens(2026, 11, 21, 12, 0), start_date=date(2026, 11, 17), pause=2.0)
        puller.setup()
        puller.run_pass()
        self.assertEqual(clock.sleeps, [2.0, 2.0, 2.0])  # 4 days, 3 gaps

    def test_a_failing_day_is_skipped_and_stays_missing(self):
        self.fail = {"2026-11-18"}
        puller, _ = self.make(athens(2026, 11, 21, 12, 0), start_date=date(2026, 11, 17))
        puller.setup()
        result = puller.run_pass()
        self.assertEqual(result.stored, ["2026-11-17", "2026-11-19", "2026-11-20"])
        self.assertEqual(result.failed, ["2026-11-18"])
        self.fail = set()
        self.assertEqual(puller.run_pass().stored, ["2026-11-18"])

    def test_an_empty_answer_counts_as_a_failure(self):
        self.empty = {"2026-11-17"}
        puller, _ = self.make(athens(2026, 11, 20, 12, 0), start_date=date(2026, 11, 17))
        puller.setup()
        self.assertEqual(puller.run_pass().failed, ["2026-11-17"])
        self.assertNotIn("2026-11-17", self.stored())

    def test_stored_data_is_the_real_schema(self):
        puller, _ = self.make(athens(2026, 11, 20, 12, 0), start_date=date(2026, 11, 17))
        puller.setup()
        puller.run_pass()
        c = sqlite3.connect(self.db_path)
        self.assertEqual(c.execute("SELECT count(*) FROM schedule").fetchone()[0], 6)  # 3 days x 2
        self.assertEqual(c.execute("SELECT delay_min FROM schedule WHERE mongo_id='2026-11-17-1'").fetchone()[0], 1)
        c.close()


class TestEndDay(Base):
    def test_without_until_it_never_finishes(self):
        puller, _ = self.make(athens(2026, 11, 20, 12, 0), start_date=date(2026, 11, 17))
        puller.setup()
        puller.run_pass()
        self.assertFalse(puller.finished())  # there is no end day

    def test_with_until_it_finishes_when_every_day_through_it_is_stored(self):
        puller, _ = self.make(athens(2026, 11, 22, 12, 0), start_date=date(2026, 11, 17), until=date(2026, 11, 19))
        puller.setup()
        self.assertFalse(puller.finished())
        puller.run_pass()
        self.assertTrue(puller.finished())
        self.assertEqual(self.fetched, ["2026-11-17", "2026-11-18", "2026-11-19"])  # nothing past until

    def test_days_after_until_are_never_fetched(self):
        puller, _ = self.make(athens(2026, 12, 30, 12, 0), start_date=date(2026, 11, 17), until=date(2026, 11, 18))
        puller.setup()
        puller.run_pass()
        self.assertEqual(self.stored(), ["2026-11-17", "2026-11-18"])

    def test_until_day_not_final_yet_is_waiting_not_finished(self):
        puller, _ = self.make(athens(2026, 11, 20, 9, 30), start_date=date(2026, 11, 18), until=date(2026, 11, 19))
        puller.setup()
        result = puller.run_pass()
        self.assertEqual(result.waiting, ["2026-11-19"])
        self.assertFalse(puller.finished())

    def test_run_once_returns_0_when_days_only_wait_and_3_when_a_final_day_failed(self):
        puller, _ = self.make(athens(2026, 11, 20, 9, 30), start_date=date(2026, 11, 18), until=date(2026, 11, 19))
        self.assertEqual(puller.run(once=True), 0)
        self.tmp.cleanup()
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "board.db")
        self.fail = {"2026-11-18"}
        puller, _ = self.make(athens(2026, 11, 20, 9, 30), start_date=date(2026, 11, 18))
        self.assertEqual(puller.run(once=True), 3)

    def test_until_before_start_is_rejected(self):
        puller, _ = self.make(athens(2026, 11, 20, 9, 30), start_date=date(2026, 11, 18), until=date(2026, 11, 17))
        with self.assertRaises(ValueError):
            puller.setup()


class TestSetup(Base):
    def test_stations_are_loaded_once_and_map_stops(self):
        puller, _ = self.make(athens(2026, 11, 20, 12, 0), start_date=date(2026, 11, 19))
        puller.setup()
        puller.conn.close()
        again, _ = self.make(athens(2026, 11, 20, 12, 0))
        again.setup()
        self.assertEqual(self.geo_calls, 1)
        self.assertEqual(again.conn.execute("SELECT count(*) FROM station").fetchone()[0], 389)

    def test_station_list_retries_then_gives_up(self):
        attempts = []

        def flaky(base_url):
            attempts.append(1)
            if len(attempts) < 3:
                raise OSError("down")
            return geo()

        clock = FakeClock(athens(2026, 11, 20, 12, 0))
        puller = bd.BoardDaily(bd.Settings(db_path=self.db_path), clock=clock, sleep=clock.sleep,
                               fetch=self.fetch, fetch_geo=flaky)
        puller.setup()
        self.assertEqual(len(attempts), 3)
        puller.conn.close()
        os.remove(self.db_path)
        always = bd.BoardDaily(bd.Settings(db_path=self.db_path), clock=clock, sleep=clock.sleep,
                               fetch=self.fetch, fetch_geo=lambda b: (_ for _ in ()).throw(OSError("down")))
        with self.assertRaises(RuntimeError):
            always.setup()

    def test_start_date_default_is_today_then_stored(self):
        puller, _ = self.make(athens(2026, 11, 20, 12, 0))
        puller.setup()
        self.assertEqual(puller.start_date, date(2026, 11, 20))
        puller.conn.close()
        later, _ = self.make(athens(2026, 12, 5, 12, 0))  # a restart much later keeps the stored date
        later.setup()
        self.assertEqual(later.start_date, date(2026, 11, 20))

    def test_an_explicit_start_date_overrides_and_is_stored(self):
        puller, _ = self.make(athens(2026, 11, 20, 12, 0), start_date=date(2026, 11, 1))
        puller.setup()
        puller.conn.close()
        again, _ = self.make(athens(2026, 11, 21, 12, 0))
        again.setup()
        self.assertEqual(again.start_date, date(2026, 11, 1))


class TestLoop(Base):
    def test_runs_daily_at_the_chosen_time_and_stops_on_request(self):
        puller, clock = self.make(athens(2026, 11, 20, 10, 31), start_date=date(2026, 11, 19), run_at=(10, 30))
        seen: list[tuple[str, str]] = []

        def fetch(iso, base_url):
            seen.append((iso, datetime.fromtimestamp(clock.t, tz=ATHENS).strftime("%m-%d %H:%M")))
            if len(seen) == 3:
                puller.stop()
            return board_payload(iso)

        puller._fetch = fetch
        self.assertEqual(puller.run(), 0)
        # 11-19 is final at the first pass; 11-20 and 11-21 each on their own morning at 10:30
        self.assertEqual(seen, [("2026-11-19", "11-20 10:31"), ("2026-11-20", "11-21 10:30"),
                                ("2026-11-21", "11-22 10:30")])

    def test_a_failed_pass_is_retried_after_an_hour_not_a_day(self):
        self.fail = {"2026-11-19"}
        puller, clock = self.make(athens(2026, 11, 20, 10, 31), start_date=date(2026, 11, 19))
        times = []

        def fetch(iso, base_url):
            times.append(clock.t)
            if len(times) == 2:
                puller.stop()
            raise OSError("down")

        puller._fetch = fetch
        puller.run()
        self.assertEqual(times[1] - times[0], 3600)

    def test_a_scheduling_bug_can_never_make_it_hit_the_server_in_a_tight_loop(self):
        from unittest import mock
        puller, clock = self.make(athens(2026, 11, 20, 10, 31), start_date=date(2026, 11, 19))
        times = []

        def fetch(iso, base_url):
            times.append(clock.t)
            if len(times) == 3:
                puller.stop()
            raise OSError("down")  # keeps the day missing, so every pass fetches again

        puller._fetch = fetch
        puller.settings.failed_retry_s = 0  # and a retry delay of zero as well
        with mock.patch.object(bd, "next_run_at", lambda now, run_at: now):  # "run again right now"
            puller.run()
        self.assertGreaterEqual(min(b - a for a, b in zip(times, times[1:])), bd.MIN_PASS_INTERVAL_S)

    def test_the_run_ends_by_itself_at_until(self):
        puller, _ = self.make(athens(2026, 11, 25, 12, 0), start_date=date(2026, 11, 20), until=date(2026, 11, 22))
        self.assertEqual(puller.run(), 0)
        self.assertEqual(self.stored(), ["2026-11-20", "2026-11-21", "2026-11-22"])


class TestArguments(unittest.TestCase):
    def parse(self, *argv):
        parser = bd.build_parser()
        return bd.settings_from_args(parser, parser.parse_args(list(argv)))

    def test_defaults_never_end(self):
        s = self.parse()
        self.assertIsNone(s.until)
        self.assertEqual(s.run_at, (10, 30))
        self.assertEqual(s.db_path, "data/board.db")

    def test_all_options(self):
        s = self.parse("--db", "x.db", "--start-date", "2026-01-05", "--until", "2026-02-01",
                       "--run-at", "11:15", "--pause", "0.5")
        self.assertEqual((s.start_date, s.until, s.run_at, s.pause),
                         (date(2026, 1, 5), date(2026, 2, 1), (11, 15), 0.5))

    def test_errors_exit_with_a_message(self):
        for argv in (["--until", "01/02/2026"], ["--start-date", "nope"], ["--run-at", "25:00"],
                     ["--start-date", "2026-03-01", "--until", "2026-02-01"]):
            with self.assertRaises(SystemExit):
                self.parse(*argv)


if __name__ == "__main__":
    unittest.main()
