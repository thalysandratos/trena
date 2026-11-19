"""Tests for the collector core, using the real captures in samples/."""

import json
import os
import random
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collector import db
from collector.sampler import State, load_last_stored_ts, tick
from collector.sse import SSEParser, iter_events_from_bytes

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLES = os.path.join(ROOT, "samples")
TRAIN_SSE = os.path.join(SAMPLES, "train_stream.sse")
TELEMETRY_SSE = os.path.join(SAMPLES, "telemetry_stream.sse")
GEO_JSON = os.path.join(SAMPLES, "geo_stations.json")


def read(path):
    with open(path, "rb") as f:
        return f.read()


def feed_chunked(parser, data, chunk_sizes, seed=42):
    """Feed data in random chunks of the given sizes; return all events."""
    rng = random.Random(seed)
    events = []
    i = 0
    while i < len(data):
        n = rng.choice(chunk_sizes)
        events.extend(parser.feed(data[i:i + n]))
        i += n
    return events


class TestSSEParser(unittest.TestCase):
    def test_train_stream_whole_vs_chunked(self):
        raw = read(TRAIN_SSE)
        whole = iter_events_from_bytes(raw)
        chunked = feed_chunked(SSEParser(), raw, [1000, 777, 4096, 13])
        self.assertEqual(whole, chunked)
        # 9 complete events; the 10th is cut off mid-line and must not be returned
        self.assertEqual(len(whole), 9)
        names = [n for n, _ in whole]
        self.assertEqual(names[0], "trainHistory")
        self.assertIn("trainPositionsUx", names)
        self.assertIn("scheduleCards", names)
        for name, data in whole:
            json.loads(data)  # every returned event must be complete JSON

    def test_telemetry_stream_events(self):
        events = iter_events_from_bytes(read(TELEMETRY_SSE))
        self.assertEqual(len(events), 24)  # 25th is the truncated trailing event
        names = {n for n, _ in events}
        self.assertEqual(names, {"scheduleTelemetry", "scheduleTelemetryRaw",
                                 "scheduleTelemetryGhosts"})

    def test_edge_cases(self):
        p = SSEParser()
        # comment line ignored, CRLF, multi-line data, event with no name
        out = p.feed(b":keepalive 123\r\n\r\n"
                     b"event: custom\r\ndata: line1\r\ndata: line2\r\n\r\n"
                     b"data: no-name\n\n")
        self.assertEqual(out, [("custom", "line1\nline2"), ("message", "no-name")])
        # event split across chunks, including the field name itself
        p = SSEParser()
        self.assertEqual(p.feed(b"eve"), [])
        self.assertEqual(p.feed(b"nt: x\nda"), [])
        self.assertEqual(p.feed(b"ta: hi\n"), [])
        self.assertEqual(p.feed(b"\n"), [("x", "hi")])
        # a single leading space after the colon is stripped
        p = SSEParser()
        self.assertEqual(p.feed(b"data:  two\n\n"), [("message", " two")])
        # trailing incomplete event is held, not returned
        p = SSEParser()
        self.assertEqual(p.feed(b"event: cut\ndata: {\"a\": 1"), [])
        self.assertEqual(p.feed(b"}\n\n"), [("cut", '{"a": 1}')])


class TestIsoToEpoch(unittest.TestCase):
    def test_cases(self):
        self.assertEqual(db.iso_to_epoch("2026-11-19T17:06:49.000Z"), 1795108009)
        self.assertEqual(db.iso_to_epoch("1970-01-01T00:00:00.000Z"), 0)
        self.assertEqual(db.iso_to_epoch("2026-11-19T18:15:58.068Z"), 1795112158)
        self.assertIsNone(db.iso_to_epoch(None))
        self.assertIsNone(db.iso_to_epoch(""))
        self.assertIsNone(db.iso_to_epoch("not a date"))


class CollectorTestBase(unittest.TestCase):
    def setUp(self):
        self.conn = db.connect(":memory:")
        db.init_schema(self.conn)
        n = db.load_stations(self.conn, json.loads(read(GEO_JSON)))
        self.assertEqual(n, 389)
        self.state = State()
        self.maps = db.IdMaps(self.conn)
        for name, data in iter_events_from_bytes(read(TRAIN_SSE)):
            self.state.handle_train_stream_event(name, data)
        for name, data in iter_events_from_bytes(read(TELEMETRY_SSE)):
            self.state.handle_telemetry_event(name, data)


class TestLoadStations(CollectorTestBase):
    def test_count_and_rounding(self):
        n = self.conn.execute("SELECT count(*) FROM station").fetchone()[0]
        self.assertEqual(n, 389)
        # first feature: coordinates [21.7645428, 38.281354] = [lng, lat]
        row = self.conn.execute(
            "SELECT lat_e5, lng_e5, name, railway_type, status, operator"
            " FROM station WHERE mongo_id='6928c3c150ab5bc72b39cf04'").fetchone()
        self.assertEqual(row[0], round(38.281354 * 1e5))   # lat_e5
        self.assertEqual(row[1], round(21.7645428 * 1e5))   # lng_e5
        self.assertEqual(row[2:], ("Μποζαϊτικα", "halt", "active", "ΟΣΕ"))
        # idempotent
        db.load_stations(self.conn, json.loads(read(GEO_JSON)))
        self.assertEqual(self.conn.execute("SELECT count(*) FROM station").fetchone()[0], 389)

    def test_id_maps(self):
        sid = self.maps.station("6928c3c150ab5bc72b39cf04")
        self.assertIsInstance(sid, int)
        self.assertIsNone(self.maps.station("unknown-mongo-id"))
        self.assertIsNone(self.maps.station(None))
        tid = self.maps.train("train-x", "passenger", "120010", "name")
        self.assertEqual(tid, self.maps.train("train-x", "passenger", "120010", "other"))
        scid = self.maps.schedule("sched-x", schedule_number="N", sched_dep=123)
        # a later stub must not modify an existing row at all, neither the board-filled columns
        # nor the timetable ones (fresh IdMaps = after a restart); the id stays the same
        self.conn.execute("UPDATE schedule SET status='completed', delay_min=17,"
                          " actual_dep=1, actual_arr=2 WHERE id=?", (scid,))
        self.conn.commit()
        again = db.IdMaps(self.conn).schedule("sched-x", schedule_number="N2", sched_dep=456)
        self.assertEqual(again, scid)
        row = self.conn.execute("SELECT schedule_number, sched_dep, status, delay_min,"
                                " actual_dep, actual_arr FROM schedule WHERE id=?",
                                (scid,)).fetchone()
        self.assertEqual(row, ("N", 123, "completed", 17, 1, 2))


class TestTick(CollectorTestBase):
    def test_end_to_end(self):
        last = load_last_stored_ts(self.conn)
        self.assertEqual(last, {})
        n1 = tick(self.state, self.conn, self.maps, last, 1795112220)
        self.assertGreater(n1, 0)
        total = self.conn.execute("SELECT count(*) FROM position").fetchone()[0]
        self.assertEqual(total, n1)
        # second tick with the same state inserts nothing
        n2 = tick(self.state, self.conn, self.maps, last, 1795112280)
        self.assertEqual(n2, 0)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM position").fetchone()[0], total)
        # unassigned trains exist with schedule_id NULL
        self.assertGreater(self.conn.execute(
            "SELECT count(*) FROM position WHERE schedule_id IS NULL").fetchone()[0], 0)
        # at least one row got delay_tel from the telemetry stream
        self.assertGreater(self.conn.execute(
            "SELECT count(*) FROM position WHERE delay_tel IS NOT NULL").fetchone()[0], 0)
        # restart path: reload last_stored_ts from the DB, tick again -> 0 rows
        last2 = load_last_stored_ts(self.conn)
        self.assertEqual(last2, last)
        self.assertEqual(tick(self.state, self.conn, self.maps, last2, 1795112340), 0)

    def test_field_conversions(self):
        last = {}
        tick(self.state, self.conn, self.maps, last, 1795112220)
        # hand-picked source row: IC57, train 6afef3586a058a9708cff014,
        # schedule 6a94745bd16c475ca144e4fe, in the last complete trainPositionsUx event
        src = self.state.latest_pos["6afef3586a058a9708cff014"]
        self.assertEqual(src["scheduleId"], "6a94745bd16c475ca144e4fe")
        row = self.conn.execute(
            "SELECT p.ts, p.lat_e5, p.lng_e5, p.speed_x10, p.progress_x1e4, p.along_m,"
            " p.dest_m, p.next_station, p.delay, p.delay_tel, p.sig, p.src, p.schedule_id"
            " FROM position p JOIN train t ON t.id = p.train_id"
            " WHERE t.mongo_id = ?", (src["trainId"],)).fetchone()
        tel = self.state.latest_tel[src["scheduleId"]]
        self.assertEqual(row, (
            db.iso_to_epoch(src["timestamp"]),
            round(src["lat"] * 1e5), round(src["lng"] * 1e5),
            round(src["speed"] * 10), round(src["progress"] * 1e4),
            round(src["alongTrackDistance"]), round(src["distanceToDestination_m"]),
            self.maps.station(src["nextStationId"]),
            src["delay"], tel["delay"], 0, 0,
            self.maps.schedule(src["scheduleId"]),
        ))
        # schedule stub columns
        srow = self.conn.execute(
            "SELECT schedule_number, train_number, service_type, corridor, origin,"
            " destination, sched_dep, sched_arr, status, delay_min, actual_dep, actual_arr"
            " FROM schedule WHERE mongo_id = ?", (src["scheduleId"],)).fetchone()
        self.assertEqual(srow, (src["scheduleNumber"], src["trainNumber"], src["serviceType"],
                                src["corridor"], src["origin"], src["destination"],
                                db.iso_to_epoch(src["scheduledDeparture"]),
                                db.iso_to_epoch(src["scheduledArrival"]),
                                None, None, None, None))
        # an unassigned train: schedule_id NULL, delay_tel NULL, src NULL
        un = [p for p in self.state.latest_pos.values() if p.get("scheduleId") is None][0]
        urow = self.conn.execute(
            "SELECT p.schedule_id, p.delay_tel, p.src FROM position p"
            " JOIN train t ON t.id = p.train_id WHERE t.mongo_id = ?",
            (un["trainId"],)).fetchone()
        self.assertEqual(urow, (None, None, None))

    def test_malformed_json_is_skipped(self):
        events_before = dict(self.state.latest_events)
        pos_before = dict(self.state.latest_pos)
        tel_before = dict(self.state.latest_tel)
        self.state.handle_train_stream_event("trainPositionsUx", "{not json")
        self.state.handle_telemetry_event("scheduleTelemetry", "[1,2")
        # valid JSON but wrong shape: archived, but no position/telemetry update, no raise
        self.state.handle_train_stream_event("trainPositionsUx", '{"positions": "nope"}')
        self.state.handle_telemetry_event("scheduleTelemetryRaw", '{"telemetry": 42}')
        self.assertEqual(self.state.latest_pos, pos_before)
        self.assertEqual(self.state.latest_tel, tel_before)
        # only the two well-formed-but-shapeless payloads were archived
        self.assertEqual(self.state.latest_events["trainPositionsUx"], {"positions": "nope"})
        self.assertEqual(self.state.latest_events["scheduleTelemetryRaw"], {"telemetry": 42})
        self.assertEqual(self.state.latest_events["scheduleCards"],
                         events_before["scheduleCards"])



class TestSrcFromUxView(unittest.TestCase):
    """raw says 'live' for a train that lost GPS; src must still be last_known (1)."""

    def test_src_uses_ux_not_raw(self):
        conn = db.connect(":memory:")
        db.init_schema(conn)
        maps = db.IdMaps(conn)
        state = State()
        pos = {"positions": [{
            "trainId": "T1", "id": "T1", "trainType": "passenger", "scheduleId": "S1",
            "timestamp": "2026-11-19T18:06:15.000Z", "lat": 38.0, "lng": 23.0, "speed": 0.0,
            "signalStatus": "lost", "delay": 0}]}
        row = lambda src, sig: {"scheduleId": "S1", "source": src, "signalStatus": sig, "delay": 3,
                                "timestamp": "2026-11-19T18:06:15.000Z"}
        state.handle_train_stream_event("trainPositionsUx", json.dumps(pos))
        state.handle_telemetry_event("scheduleTelemetryRaw",
                                     json.dumps({"stream": "raw", "telemetry": [row("live", "ok")]}))
        state.handle_telemetry_event("scheduleTelemetry",
                                     json.dumps({"stream": "ux", "telemetry": [row("last_known", "lost")]}))
        tick(state, conn, maps, {}, 1795114400)
        got = conn.execute("SELECT sig, src, delay_tel FROM position").fetchone()
        self.assertEqual(got, (2, 1, 3))  # sig lost, src last_known, delay from raw


class TestTickIsolatesBadRows(unittest.TestCase):
    """One odd field in one train's row must not cost the other trains their samples."""

    def run_tick(self, bad_fields):
        conn = db.connect(":memory:")
        db.init_schema(conn)
        maps = db.IdMaps(conn)
        state = State()

        def pos(tid, **kw):
            r = {"trainId": tid, "id": tid, "trainType": "passenger", "scheduleId": None,
                 "timestamp": "2026-11-19T17:06:49.000Z", "lat": 38.0, "lng": 23.0,
                 "speed": 50.0, "signalStatus": "ok", "delay": 0}
            r.update(kw)
            return r

        payload = {"positions": [pos("GOOD"), pos("BAD", **bad_fields), pos("GOOD2")]}
        state.handle_train_stream_event("trainPositionsUx", json.dumps(payload))
        last = {}
        n = tick(state, conn, maps, last, 1)
        return conn, n, last

    def test_non_numeric_field_becomes_null_others_unaffected(self):
        for bad in ({"speed": "45"}, {"lat": {"x": 1}}, {"speed": True}):
            conn, n, last = self.run_tick(bad)
            self.assertEqual(n, 3, bad)
            self.assertEqual(len(last), 3, bad)
            self.assertEqual(conn.execute("SELECT count(*) FROM position").fetchone()[0], 3)

    def test_nan_becomes_null(self):
        conn = db.connect(":memory:")
        db.init_schema(conn)
        state = State()
        state.handle_train_stream_event(
            "trainPositionsUx",
            '{"positions": [{"trainId": "N", "timestamp": "2026-11-19T17:06:49.000Z", "speed": NaN}]}')
        self.assertEqual(tick(state, conn, db.IdMaps(conn), {}, 1), 1)
        self.assertIsNone(conn.execute("SELECT speed_x10 FROM position").fetchone()[0])

    def test_unusable_timestamp_skips_only_that_train(self):
        for bad in ({"timestamp": 1795112280000}, {"timestamp": "garbage"}, {"timestamp": None}):
            conn, n, last = self.run_tick(bad)
            self.assertEqual(n, 2, bad)  # GOOD and GOOD2 are stored, BAD is skipped
            self.assertEqual(len(last), 2, bad)

    def test_failed_insert_does_not_advance_last_stored_ts(self):
        """A row that could not be written must be retried next minute, not marked as stored."""
        conn = db.connect(":memory:")
        db.init_schema(conn)
        state = State()
        state.handle_train_stream_event(
            "trainPositionsUx",
            '{"positions": [{"trainId": "A", "timestamp": "2026-11-19T17:06:49.000Z"}]}')
        maps = db.IdMaps(conn)
        conn.execute("ALTER TABLE position RENAME TO position_off")  # the INSERT now fails
        last = {}
        self.assertEqual(tick(state, conn, maps, last, 1), 0)
        self.assertEqual(last, {})
        conn.execute("ALTER TABLE position_off RENAME TO position")  # the problem goes away
        self.assertEqual(tick(state, conn, maps, last, 2), 1)  # the same fix is stored now
        self.assertEqual(conn.execute("SELECT count(*) FROM position").fetchone()[0], 1)

if __name__ == "__main__":
    unittest.main()
