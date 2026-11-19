"""Tests for collector/archive.py (gzip-member JSONL archive)."""

import gzip
import json
import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collector.archive import ArchiveWriter, day_file
from collector.sampler import State

# 2026-11-19T12:00:00Z and the next UTC day
T1 = 1795099200   # 2026-11-19T12:00:00Z
T2 = 1795099200 + 86400  # 2026-11-20T12:00:00Z
T3 = 1795099200 + 3600


def read_all(path):
    """Read every line of a multi-member gzip file."""
    out = []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            out.append(json.loads(line))
    return out


class TestArchive(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = self.tmp.name
        self.w = ArchiveWriter(self.dir)

    def tearDown(self):
        self.tmp.cleanup()

    def test_round_trip_two_members(self):
        self.assertTrue(self.w.write(T1, "trainPositionsUx", {"positions": [1, 2]}))
        self.assertTrue(self.w.write(T1, "scheduleCards", {"ghosts": []}))
        path = day_file(self.dir, T1)
        self.assertTrue(path.endswith("2026-11-19.jsonl.gz"))
        recs = read_all(path)
        self.assertEqual(recs, [
            {"t": T1, "ev": "trainPositionsUx", "data": {"positions": [1, 2]}},
            {"t": T1, "ev": "scheduleCards", "data": {"ghosts": []}},
        ])
        # both lines are separate gzip members: the file must be readable member by member
        with open(path, "rb") as f:
            raw = f.read()
        self.assertEqual(raw.count(b"\x1f\x8b\x08"), 2)  # two gzip magic headers

    def test_day_rollover(self):
        self.w.write(T1, "a", {"x": 1})
        self.w.write(T2, "b", {"x": 2})
        self.assertEqual(sorted(os.listdir(self.dir)),
                         ["2026-11-19.jsonl.gz", "2026-11-20.jsonl.gz"])
        self.assertEqual(read_all(day_file(self.dir, T1))[0]["ev"], "a")
        self.assertEqual(read_all(day_file(self.dir, T2))[0]["ev"], "b")

    def test_write_event_is_write(self):
        self.assertTrue(self.w.write_event(T1, "trainHistory", {"history": {}}))
        recs = read_all(day_file(self.dir, T1))
        self.assertEqual(recs, [{"t": T1, "ev": "trainHistory", "data": {"history": {}}}])

    def test_creates_directory(self):
        sub = os.path.join(self.dir, "nested", "archive")
        w = ArchiveWriter(sub)
        self.assertTrue(w.write(T1, "e", [1]))
        self.assertEqual(len(read_all(os.path.join(sub, "2026-11-19.jsonl.gz"))), 1)

    def test_read_only_directory_returns_false_no_raise(self):
        # a regular FILE where the directory should be: makedirs fails with
        # NotADirectoryError (an OSError) -> logged, False, never raised
        blocker = os.path.join(self.dir, "blocker")
        with open(blocker, "w") as f:
            f.write("x")
        w = ArchiveWriter(os.path.join(blocker, "archive"))
        self.assertFalse(w.write(T1, "e", {"a": 1}))
        self.assertFalse(w.write_event(T1, "trainHistory", {}))
        # and the same for a genuinely read-only directory when not root
        ro = os.path.join(self.dir, "ro")
        os.makedirs(ro)
        os.chmod(ro, 0o500)
        try:
            if os.geteuid() != 0:
                self.assertFalse(ArchiveWriter(ro).write(T1, "e", {"a": 1}))
        finally:
            os.chmod(ro, 0o700)

    def test_write_snapshot_skips_unchanged_object(self):
        state = State()
        p1 = {"positions": [{"trainId": "T1"}]}
        c1 = {"ghosts": []}
        with state.lock:
            state.latest_events["trainPositionsUx"] = p1
            state.latest_events["scheduleCards"] = c1
            state.latest_events["scheduleTelemetry"] = None  # absent -> not written
        n = self.w.write_snapshot(state, T1)
        self.assertEqual(n, 2)
        # same objects again -> nothing written
        self.assertEqual(self.w.write_snapshot(state, T1 + 60), 0)
        # a NEW object for one name -> only that one is written
        p2 = {"positions": [{"trainId": "T2"}]}
        with state.lock:
            state.latest_events["trainPositionsUx"] = p2
        self.assertEqual(self.w.write_snapshot(state, T1 + 120), 1)
        recs = read_all(day_file(self.dir, T1))
        self.assertEqual([r["ev"] for r in recs],
                         ["trainPositionsUx", "scheduleCards", "trainPositionsUx"])
        self.assertEqual(recs[-1]["data"], p2)
        # a different object with EQUAL content is still written (identity, not value)
        with state.lock:
            state.latest_events["scheduleCards"] = {"ghosts": []}
        self.assertEqual(self.w.write_snapshot(state, T1 + 180), 1)

    def test_write_snapshot_missing_event_names(self):
        state = State()
        self.assertEqual(self.w.write_snapshot(state, T1), 0)
        with state.lock:
            state.latest_events["scheduleTelemetryGhosts"] = {"telemetry": []}
        self.assertEqual(self.w.write_snapshot(state, T1), 1)
        recs = read_all(day_file(self.dir, T1))
        self.assertEqual(recs[0]["ev"], "scheduleTelemetryGhosts")

    def test_thread_safe_concurrent_writes(self):
        def worker(i):
            for j in range(20):
                self.assertTrue(self.w.write(T1, f"ev{i}", {"j": j}))
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        recs = read_all(day_file(self.dir, T1))
        self.assertEqual(len(recs), 80)
        self.assertEqual(len({(r["ev"], r["data"]["j"]) for r in recs}), 80)


if __name__ == "__main__":
    unittest.main()
