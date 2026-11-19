"""Tests for collector/board.py against the real 2026-11-19 board sample."""

import json
import os
import sqlite3
import sys
import unittest
from datetime import date, datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collector import db
from collector.board import (athens_today, due_dates, fetch_board, run_catchup,
                             store_board)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BOARD_JSON = os.path.join(ROOT, "samples", "schedule_board_2026-11-19.json")
GEO_JSON = os.path.join(ROOT, "samples", "geo_stations.json")


def load_board():
    with open(BOARD_JSON, encoding="utf-8") as f:
        return json.load(f)


class BoardTestBase(unittest.TestCase):
    def setUp(self):
        self.conn = db.connect(":memory:")
        db.init_schema(self.conn)
        db.load_stations(self.conn, json.load(open(GEO_JSON, encoding="utf-8")))
        self.maps = db.IdMaps(self.conn)
        self.board = load_board()


class TestAthensToday(unittest.TestCase):
    def test_around_midnight(self):
        # 2026-11-18T22:30:00Z is 2026-11-19 00:30 in Athens (UTC+2 in November)
        t = datetime(2026, 11, 18, 22, 30, tzinfo=timezone.utc).timestamp()
        self.assertEqual(athens_today(t), date(2026, 11, 19))
        # 2026-11-18T21:30:00Z is still 2026-11-18 23:30 in Athens
        t2 = datetime(2026, 11, 18, 21, 30, tzinfo=timezone.utc).timestamp()
        self.assertEqual(athens_today(t2), date(2026, 11, 18))
        # summer time UTC+3: 2026-07-01T21:30:00Z is 2026-07-02 00:30 in Athens
        t3 = datetime(2026, 7, 1, 21, 30, tzinfo=timezone.utc).timestamp()
        self.assertEqual(athens_today(t3), date(2026, 7, 2))
        t4 = datetime(2026, 7, 1, 20, 30, tzinfo=timezone.utc).timestamp()
        self.assertEqual(athens_today(t4), date(2026, 7, 1))


class TestDueDates(BoardTestBase):
    def test_basic_range_and_gaps(self):
        today = date(2026, 11, 19)
        start = date(2026, 11, 10)
        # nothing fetched yet -> 11-10 .. 11-17 (today-2) inclusive
        self.assertEqual(due_dates(self.conn, start, today),
                         [f"2026-11-{d:02d}" for d in range(10, 18)])
        # mark a few as fetched (with a gap)
        for d in ("2026-11-10", "2026-11-11", "2026-11-13"):
            self.conn.execute("INSERT INTO board_fetch (date, fetched_at, n) VALUES (?,?,?)",
                              (d, 1, 234))
        self.conn.commit()
        self.assertEqual(due_dates(self.conn, start, today),
                         ["2026-11-12", "2026-11-14", "2026-11-15", "2026-11-16",
                          "2026-11-17"])

    def test_start_too_recent_and_boundaries(self):
        today = date(2026, 11, 19)  # today-2 = 2026-11-17
        self.assertEqual(due_dates(self.conn, date(2026, 11, 18), today), [])
        self.assertEqual(due_dates(self.conn, date(2026, 11, 19), today), [])
        # exactly today-2 is included, today-1 excluded
        self.assertEqual(due_dates(self.conn, date(2026, 11, 17), today), ["2026-11-17"])
        self.assertEqual(due_dates(self.conn, date(2026, 11, 16), today),
                         ["2026-11-16", "2026-11-17"])


class TestFetchBoard(unittest.TestCase):
    def test_url_and_parsing(self):
        calls = []

        class FakeResp:
            def __init__(self, body):
                self.body = body

            def read(self):
                return self.body

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        def opener(req, timeout=None):
            calls.append((req.full_url, timeout, dict(req.header_items())))
            return FakeResp(json.dumps({"schedules": [{"_id": "x"}]}).encode())

        out = fetch_board("2026-11-19", "https://example.test", opener=opener, timeout=7)
        self.assertEqual(out, {"schedules": [{"_id": "x"}]})
        url, timeout, headers = calls[0]
        self.assertEqual(url, "https://example.test/api/public/schedule-board?date=2026-11-19")
        self.assertEqual(timeout, 7)
        self.assertEqual(headers.get("User-agent"), "trainose-collector/0.1")

    def test_error_payload_raises_valueerror(self):
        class FakeResp:
            def read(self):
                return json.dumps({"error": "Invalid date"}).encode()

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        def opener(req, timeout=None):
            return FakeResp()

        with self.assertRaises(ValueError):
            fetch_board("garbage", "https://example.test", opener=opener)


class TestStoreBoard(BoardTestBase):
    def test_store_sample(self):
        n = store_board(self.conn, self.maps, "2026-11-19", self.board)
        self.assertEqual(n, 234)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM schedule").fetchone()[0], 234)
        n_stops = sum(len(s.get("routeStations") or []) for s in self.board["schedules"])
        self.assertEqual(n_stops, 3430)
        self.assertEqual(
            self.conn.execute("SELECT count(*) FROM schedule_stop").fetchone()[0], 3430)
        row = self.conn.execute("SELECT date, n FROM board_fetch").fetchone()
        self.assertEqual(row, ("2026-11-19", 234))

    def test_columns_of_a_completed_schedule(self):
        store_board(self.conn, self.maps, "2026-11-19", self.board)
        # hand-picked completed schedule with both actual times (the first one)
        s = next(s for s in self.board["schedules"]
                 if s["status"] == "completed" and s.get("actualDeparture")
                 and s.get("actualArrival"))
        row = self.conn.execute(
            "SELECT schedule_number, train_number, service_type, corridor, origin,"
            " destination, sched_dep, sched_arr, status, delay_min, actual_dep, actual_arr"
            " FROM schedule WHERE mongo_id=?", (s["_id"],)).fetchone()
        self.assertEqual(row, (
            s["scheduleNumber"], s["trainNumber"], s["serviceType"], s["corridor"],
            s["origin"]["name"], s["destination"]["name"],
            db.iso_to_epoch(s["scheduledDeparture"]), db.iso_to_epoch(s["scheduledArrival"]),
            s["status"], s["delay"],
            db.iso_to_epoch(s["actualDeparture"]), db.iso_to_epoch(s["actualArrival"]),
        ))
        # its stops, in order, with mapped station ids
        stops = self.conn.execute(
            "SELECT idx, station_id, role, sched_arr, sched_dep FROM schedule_stop"
            " WHERE schedule_id=(SELECT id FROM schedule WHERE mongo_id=?)"
            " ORDER BY idx", (s["_id"],)).fetchall()
        self.assertEqual(len(stops), len(s["routeStations"]))
        for got, st in zip(stops, s["routeStations"]):
            self.assertEqual(got[0], st["index"])
            self.assertEqual(got[1], self.maps.station(st["stationId"]))
            self.assertEqual(got[2], st["role"])
            self.assertEqual(got[3], db.iso_to_epoch(st["scheduledArrival"]))
            self.assertEqual(got[4], db.iso_to_epoch(st["scheduledDeparture"]))

    def test_null_actuals_stay_null(self):
        store_board(self.conn, self.maps, "2026-11-19", self.board)
        s = next(s for s in self.board["schedules"] if s.get("actualDeparture") is None)
        row = self.conn.execute(
            "SELECT actual_dep, actual_arr, delay_min, status FROM schedule WHERE mongo_id=?",
            (s["_id"],)).fetchone()
        self.assertEqual(row, (None, db.iso_to_epoch(s["actualArrival"]) if s.get("actualArrival") else None, s["delay"], s["status"]))

    def test_stub_completed_in_place(self):
        # a stub created from the stream, same _id as a board schedule
        s = self.board["schedules"][0]
        stub_id = self.maps.schedule(s["_id"], schedule_number="stub-number",
                                     train_number="stub-train")
        self.assertIsNone(self.conn.execute(
            "SELECT status FROM schedule WHERE id=?", (stub_id,)).fetchone()[0])
        n = store_board(self.conn, self.maps, "2026-11-19", self.board)
        self.assertEqual(n, 234)
        row = self.conn.execute(
            "SELECT id, status, delay_min, schedule_number FROM schedule WHERE mongo_id=?",
            (s["_id"],)).fetchone()
        # same id, board values filled in
        self.assertEqual(row[0], stub_id)
        self.assertEqual(row[1], s["status"])
        self.assertEqual(row[2], s["delay"])
        self.assertEqual(row[3], s["scheduleNumber"])
        self.assertEqual(self.conn.execute("SELECT count(*) FROM schedule").fetchone()[0], 234)

    def test_idempotent(self):
        store_board(self.conn, self.maps, "2026-11-19", self.board)
        store_board(self.conn, self.maps, "2026-11-19", self.board)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM schedule").fetchone()[0], 234)
        self.assertEqual(
            self.conn.execute("SELECT count(*) FROM schedule_stop").fetchone()[0], 3430)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM board_fetch").fetchone()[0], 1)


class TestRunCatchup(BoardTestBase):
    def test_failure_on_one_date_skips_it_others_continue(self):
        today = date(2026, 11, 19)
        start = date(2026, 11, 15)  # due: 15, 16, 17
        calls = []
        boards = {f"2026-11-{d:02d}": {"schedules": [
            {"_id": f"s{d}", "scheduleNumber": f"N{d}", "trainNumber": "1",
             "serviceType": "Suburban", "corridor": "PIRAIR", "status": "completed",
             "delay": 3, "scheduledDeparture": "2026-11-15T10:00:00.000Z",
             "scheduledArrival": "2026-11-15T11:00:00.000Z",
             "actualDeparture": "2026-11-15T10:03:00.000Z",
             "actualArrival": "2026-11-15T11:05:00.000Z",
             "origin": {"name": "A", "nameGreek": "Α"}, "destination": {"name": "B", "nameGreek": "Β"},
             "routeStations": [{"stationId": None, "name": "A", "role": "origin", "index": 0,
                                "scheduledArrival": "2026-11-15T10:00:00.000Z",
                                "scheduledDeparture": "2026-11-15T10:00:00.000Z"}]}
        ]} for d in (15, 16, 17)}

        def fetch(date_str, base_url):
            calls.append(date_str)
            if date_str == "2026-11-16":
                raise OSError("boom")
            return boards[date_str]

        slept = []
        stored = run_catchup(self.conn, self.maps, start, today, fetch=fetch,
                             base_url="http://x", sleep=slept.append, pause=2.0)
        self.assertEqual(stored, ["2026-11-15", "2026-11-17"])
        self.assertEqual(calls, ["2026-11-15", "2026-11-16", "2026-11-17"])
        # pause after each request except the very last one of the due list
        self.assertEqual(slept, [2.0, 2.0])
        # the failed date stayed due, the others are recorded
        self.assertEqual(due_dates(self.conn, start, today), ["2026-11-16"])
        self.assertEqual(self.conn.execute("SELECT count(*) FROM schedule").fetchone()[0], 2)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM schedule_stop").fetchone()[0], 2)

    def test_nothing_due(self):
        today = date(2026, 11, 19)
        self.assertEqual(run_catchup(self.conn, self.maps, date(2026, 11, 19), today,
                                     fetch=lambda d, b: {}, base_url="http://x",
                                     sleep=lambda s: None), [])



class TestBoardUpdatesAndGuards(BoardTestBase):
    SCHED = {"_id": "sX", "scheduleNumber": "N1-20261119-1000", "trainNumber": "N1",
             "serviceType": "Suburban", "corridor": "PIRAIR", "status": "in_progress", "delay": 0,
             "scheduledDeparture": "2026-11-19T10:00:00.000Z",
             "scheduledArrival": "2026-11-19T11:00:00.000Z",
             "actualDeparture": "2026-11-19T10:02:00.000Z", "actualArrival": None,
             "origin": {"name": "A", "nameGreek": "A"}, "destination": {"name": "B", "nameGreek": "B"},
             "routeStations": []}

    def test_later_fetch_completes_an_unfinished_schedule(self):
        store_board(self.conn, self.maps, "2026-11-19", {"schedules": [dict(self.SCHED)]})
        row = self.conn.execute("SELECT status, delay_min, actual_arr FROM schedule").fetchone()
        self.assertEqual(row, ("in_progress", 0, None))
        done = dict(self.SCHED, status="completed", delay=7,
                    actualArrival="2026-11-19T11:07:00.000Z")
        store_board(self.conn, self.maps, "2026-11-19", {"schedules": [done]})
        row = self.conn.execute("SELECT status, delay_min, actual_arr FROM schedule").fetchone()
        self.assertEqual(row, ("completed", 7, db.iso_to_epoch("2026-11-19T11:07:00.000Z")))
        self.assertEqual(self.conn.execute("SELECT count(*) FROM schedule").fetchone()[0], 1)

    def test_empty_day_is_not_marked_fetched(self):
        fetch = lambda d, base: {"schedules": []}
        got = run_catchup(self.conn, self.maps, date(2026, 11, 15), date(2026, 11, 18),
                          fetch=fetch, base_url="x", sleep=lambda s: None)
        self.assertEqual(got, [])
        self.assertEqual(self.conn.execute("SELECT count(*) FROM board_fetch").fetchone()[0], 0)


class TestUnusableBoardDay(BoardTestBase):
    def test_all_schedules_unusable_fails_and_leaves_the_day_due(self):
        payload = {"schedules": [{"scheduleNumber": "X"}, {"scheduleNumber": "Y"}]}  # no _id
        with self.assertRaises(ValueError):
            store_board(self.conn, self.maps, "2026-11-17", payload)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM board_fetch").fetchone()[0], 0)
        self.assertEqual(due_dates(self.conn, date(2026, 11, 17), date(2026, 11, 19)), ["2026-11-17"])

    def test_n_counts_what_was_really_stored(self):
        good = {"_id": "g1", "scheduleNumber": "N", "routeStations": []}
        payload = {"schedules": [good, {"scheduleNumber": "no id"}]}
        self.assertEqual(store_board(self.conn, self.maps, "2026-11-17", payload), 1)
        self.assertEqual(self.conn.execute("SELECT n FROM board_fetch").fetchone()[0], 1)

    def test_fetched_at_uses_the_injected_clock(self):
        good = {"_id": "g1", "scheduleNumber": "N", "routeStations": []}
        store_board(self.conn, self.maps, "2026-11-17", {"schedules": [good]}, now=lambda: 42.9)
        self.assertEqual(self.conn.execute("SELECT fetched_at FROM board_fetch").fetchone()[0], 42)

if __name__ == "__main__":
    unittest.main()
