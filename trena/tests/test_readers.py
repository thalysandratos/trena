"""Tests for collector/readers.py: the SSE reader thread with fake connections and clocks."""

import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collector.readers import SSEReader
from collector.sse import iter_events_from_bytes


class StopScript(Exception):
    """Raised by a test Script when it runs out of connections."""

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRAIN_SSE = os.path.join(ROOT, "samples", "train_stream.sse")


def read(path):
    with open(path, "rb") as f:
        return f.read()


class FakeClock:
    """Deterministic monotonic/wall clocks; ``advance`` moves both."""

    def __init__(self, mono=1000.0, wall=1_800_000_000.0):
        self.mono = mono
        self.wall = wall

    def monotonic(self):
        return self.mono

    def now(self):
        return self.wall

    def advance(self, dm, dw=None):
        self.mono += dm
        self.wall += dw if dw is not None else dm


class FakeConn:
    """Scripted connection: a list of steps, each either bytes, b"" (EOF),
    an exception instance to raise, or ("block",) to raise TimeoutError forever."""

    def __init__(self, steps, clock=None):
        self.steps = list(steps)
        self.clock = clock
        self.closed = False
        self.reads = 0

    def read1(self, n):
        self.reads += 1
        if not self.steps:
            # default: block "forever" (a read timeout) so the reader waits
            if self.clock is not None:
                self.clock.advance(1.0)  # each read costs 1s of fake time
            raise TimeoutError("timed out")
        step = self.steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        if step == b"":
            return b""
        return step

    def close(self):
        self.closed = True


class Script:
    """A fake ``connect`` returning FakeConns in order; records calls.

    When the script is exhausted it raises ``StopScript``, which the test
    harness turns into a stop of the reader thread.
    """

    def __init__(self, conns, clock=None):
        self.conns = list(conns)
        self.clock = clock
        self.calls = []

    def __call__(self, url, timeout):
        self.calls.append((url, timeout))
        if not self.conns:
            raise StopScript("script exhausted")
        return self.conns.pop(0)


class Recorder:
    def __init__(self):
        self.events = []          # (name, data)
        self.outages = []         # (name, lost_at, recovered_at, reason)
        self.connects = []        # name

    def on_event(self, name, data):
        self.events.append((name, data))

    def on_outage(self, name, lost_at, recovered_at, reason):
        self.outages.append((name, lost_at, recovered_at, reason))

    def on_connect(self, name):
        self.connects.append(name)


def run_reader(reader, stop_event, budget=30.0, after_start=None):
    """Run the reader thread to the end of its script, then stop it.

    The reader is started and left running; the test's ``after_start`` hook (or
    the main thread stopping the event) ends it. A ``StopScript`` from the fake
    connect also stops the reader: the script running out is the signal that the
    scenario is over.
    """
    orig_connect = reader._connect

    def connect(url, timeout):
        try:
            return orig_connect(url, timeout)
        except StopScript:
            stop_event.set()
            raise

    reader._connect = connect
    reader.start()
    if after_start is not None:
        after_start()
    if not stop_event.is_set():
        # give the reader a moment to consume the script, then stop it
        deadline = time.monotonic() + budget
        while time.monotonic() < deadline and reader.is_alive() and not stop_event.is_set():
            time.sleep(0.005)
        stop_event.set()
    reader.join(budget)
    return not reader.is_alive()


class ReaderTestBase(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.rec = Recorder()
        self.stop = threading.Event()
        self.slept = []

    def tearDown(self):
        self.stop.set()

    def make_reader(self, conns, **kw):
        script = Script(conns, self.clock)
        kw.setdefault("connect", script)
        kw.setdefault("on_outage", self.rec.on_outage)
        kw.setdefault("on_connect", self.rec.on_connect)
        kw.setdefault("sleep", lambda s: (self.slept.append(s), self.clock.advance(s)))
        kw.setdefault("monotonic", self.clock.monotonic)
        kw.setdefault("now", self.clock.now)
        reader = SSEReader("train-stream", "http://fake/stream", self.rec.on_event,
                           self.stop, **kw)
        return reader, script

    def run_scenario(self, conns, **kw):
        """Build a reader over ``conns`` and run it to completion of the script."""
        reader, script = self.make_reader(conns, **kw)
        self.assertTrue(run_reader(reader, self.stop, after_start=lambda: None),
                        "reader thread did not finish")
        return reader, script


class TestParsing(ReaderTestBase):
    def test_whole_vs_split_chunks_same_events(self):
        raw = read(TRAIN_SSE)
        whole = iter_events_from_bytes(raw)
        # feed the whole payload in one chunk, then EOF
        r, _ = self.run_scenario([FakeConn([raw, b""], self.clock)])
        self.assertEqual(self.rec.events, whole)
        # now in awkward small chunks (split anywhere must not matter)
        rec2 = Recorder()
        chunks = [raw[i:i + 7] for i in range(0, len(raw), 7)]
        stop2 = threading.Event()
        clock2 = FakeClock()
        reader2 = SSEReader("s", "u", rec2.on_event, stop2,
                            connect=Script([FakeConn(chunks + [b""], clock2)]),
                            sleep=lambda s: clock2.advance(s),
                            monotonic=clock2.monotonic, now=clock2.now)
        self.assertTrue(run_reader(reader2, stop2, after_start=lambda: None))
        self.assertEqual(rec2.events, whole)
        names = [n for n, _ in rec2.events]
        self.assertEqual(names, [n for n, _ in whole])  # same order as the whole parse
        self.assertEqual(names[0], "trainHistory")
        self.assertIn("scheduleCards", names)
        self.assertIn("trainPositionsUx", names)


class TestReconnect(ReaderTestBase):
    def test_eof_then_reconnect_one_outage(self):
        ev1 = b'event: trainHistory\ndata: {"a": 1}\n\n'
        ev2 = b'event: trainPositionsUx\ndata: {"positions": []}\n\n'
        r, _ = self.run_scenario([FakeConn([ev1, b""], self.clock),
                         FakeConn([ev2, b""], self.clock)])
        self.assertEqual([n for n, _ in self.rec.events],
                         ["trainHistory", "trainPositionsUx"])
        self.assertEqual(self.rec.connects, ["train-stream", "train-stream"])
        # exactly one outage, EOF reason, lost_at from the first connection's loss
        self.assertEqual(len(self.rec.outages), 1)
        name, lost_at, rec_at, reason = self.rec.outages[0]
        self.assertEqual(name, "train-stream")
        self.assertEqual(reason, "EOF")
        self.assertIsInstance(lost_at, int)
        self.assertIsInstance(rec_at, int)
        self.assertGreaterEqual(rec_at, lost_at)

    def test_on_lost_fires_once_at_loss_time_even_if_never_recovered(self):
        ev1 = b'event: trainHistory\ndata: {"a": 1}\n\n'
        lost = []
        # connection 1 delivers an event then ends; connections 2 and 3 end without any event
        r, _ = self.run_scenario([FakeConn([ev1, b""], self.clock), FakeConn([b""], self.clock),
                                  FakeConn([b""], self.clock)],
                                 on_lost=lambda n, t, why: lost.append((n, t, why)))
        self.assertEqual(len(lost), 1)  # one outage opened, not one per failed attempt
        self.assertEqual(lost[0][0], "train-stream")
        self.assertEqual(lost[0][2], "EOF")
        self.assertEqual(self.rec.outages, [])  # never recovered: no closing report

    def test_on_lost_then_outage_report_share_the_same_start(self):
        ev1 = b'event: trainHistory\ndata: {"a": 1}\n\n'
        ev2 = b'event: trainPositionsUx\ndata: {"positions": []}\n\n'
        lost = []
        self.run_scenario([FakeConn([ev1, b""], self.clock), FakeConn([ev2, b""], self.clock)],
                          on_lost=lambda n, t, why: lost.append((n, t, why)))
        self.assertEqual(len(lost), 2)  # the first outage, and a second one when connection 2 ends
        self.assertEqual(len(self.rec.outages), 1)  # only the first one was recovered from
        self.assertEqual(lost[0][1], self.rec.outages[0][1])  # same lost_at: the row can be closed

    def test_unexpected_exception_in_the_read_loop_costs_a_reconnect_not_the_thread(self):
        ev = b'data: {"after": true}\n\n'
        # a str chunk makes the parser raise TypeError: not a network error, must not end the thread
        r, _ = self.run_scenario([FakeConn([b'data: {"before": true}\n\n', "not bytes"], self.clock),
                                  FakeConn([ev, b""], self.clock)])
        self.assertEqual([d for _, d in self.rec.events], ['{"before": true}', '{"after": true}'])
        self.assertEqual(len(self.rec.outages), 1)
        self.assertIn("TypeError", self.rec.outages[0][3])

    def test_a_timeout_with_read_timeout_equal_to_idle_limit_reconnects_as_idle(self):
        """The production setting: after a socket timeout the connection cannot be reused."""
        ev1 = b'data: {"a": 1}\n\n'
        ev2 = b'data: {"b": 1}\n\n'

        class RealSocketLike(FakeConn):
            """Like urllib's response: any read after a timeout fails."""
            timed_out = False

            def read1(self, n):
                if self.timed_out:
                    raise OSError("cannot read from timed out object")
                try:
                    return super().read1(n)
                except TimeoutError:
                    self.timed_out = True
                    raise

        # the fake clock does NOT advance, yet a timeout must end the connection as "idle"
        r, _ = self.run_scenario([RealSocketLike([ev1, TimeoutError("t")], self.clock),
                                  FakeConn([ev2, b""], self.clock)],
                                 read_timeout=30.0, idle_limit=30.0)
        self.assertEqual([d for _, d in self.rec.events], ['{"a": 1}', '{"b": 1}'])
        self.assertEqual(len(self.rec.outages), 1)
        self.assertEqual(self.rec.outages[0][3], "idle 30s")

    def test_no_outage_for_first_connection(self):
        r, _ = self.run_scenario([FakeConn([b'data: {"x": 1}\n\n', b""], self.clock)])
        self.assertEqual(len(self.rec.events), 1)
        self.assertEqual(self.rec.outages, [])
        self.assertEqual(self.rec.connects, ["train-stream"])

    def test_oserror_reconnects_with_repr_reason(self):
        r, _ = self.run_scenario([FakeConn([OSError("reset by peer")], self.clock),
                         FakeConn([b'data: {"ok": true}\n\n', b""], self.clock)])
        self.assertEqual([d for _, d in self.rec.events], ['{"ok": true}'])
        self.assertEqual(len(self.rec.outages), 1)
        self.assertIn("reset by peer", self.rec.outages[0][3])

    def test_connect_failure_is_a_failure_too(self):
        conns = [FakeConn([b'data: {"after": 1}\n\n', b""], self.clock)]
        script = Script(conns, self.clock)
        calls = []

        def connect(url, timeout):
            calls.append(url)
            if len(calls) == 1:
                raise OSError("refused")
            return script(url, timeout)
        reader = SSEReader("s", "u", self.rec.on_event, self.stop, connect=connect,
                           on_outage=self.rec.on_outage, on_connect=self.rec.on_connect,
                           sleep=lambda s: (self.slept.append(s), self.clock.advance(s)),
                           monotonic=self.clock.monotonic, now=self.clock.now)
        self.assertTrue(run_reader(reader, self.stop, after_start=lambda: None))
        # exactly 2 connect attempts: the first refused, the second served
        self.assertEqual(calls[:2], ["u", "u"])
        self.assertLessEqual(len(calls), 3)  # at most one extra (script-exhausted) call
        self.assertEqual([d for _, d in self.rec.events], ['{"after": 1}'])
        # the very first connection never succeeded, so there is no outage to report
        self.assertEqual(self.rec.outages, [])


class TestCallbacks(ReaderTestBase):
    def test_exception_in_on_event_does_not_stop_reader(self):
        calls = []

        def bad(name, data):
            calls.append(name)
            if len(calls) == 1:
                raise ValueError("boom")
        conns = [FakeConn([b'event: a\ndata: 1\n\n', b'event: b\ndata: 2\n\n', b""], self.clock)]
        r, _ = self.make_reader(conns)
        r.on_event = bad
        self.assertTrue(run_reader(r, self.stop, after_start=lambda: None))
        self.assertEqual(calls, ["a", "b"])  # both events were delivered

    def test_exception_in_on_outage_does_not_stop_reader(self):
        def bad_outage(name, a, b, reason):
            raise RuntimeError("nope")
        r, _ = self.run_scenario([FakeConn([b""], self.clock),
                         FakeConn([b'data: {"x": 2}\n\n', b""], self.clock)],
                        on_outage=bad_outage)
        self.assertEqual([d for _, d in self.rec.events], ['{"x": 2}'])


class TestWatchdog(ReaderTestBase):
    def test_idle_watchdog_reconnects(self):
        # first connection: one event, then reads that raise TimeoutError while the
        # fake clock jumps past idle_limit -> reader must close and reconnect
        clock = self.clock
        conn1_steps = [b'data: {"first": 1}\n\n']

        class IdleConn(FakeConn):
            def read1(self, n):
                if not self.steps:
                    clock.advance(60.0)  # one read "takes" 60s: past the 30s idle limit
                    raise TimeoutError("timed out")
                return self.steps.pop(0)

        conns = [IdleConn(conn1_steps), FakeConn([b'data: {"second": 2}\n\n', b""], clock)]
        r, _ = self.run_scenario(conns, idle_limit=30.0)
        self.assertEqual([d for _, d in self.rec.events], ['{"first": 1}', '{"second": 2}'])
        self.assertEqual(len(self.rec.outages), 1)
        self.assertIn("idle", self.rec.outages[0][3])
        self.assertEqual(len(self.rec.connects), 2)

    def test_read_timeout_alone_is_not_an_outage(self):
        # TimeoutError with the clock NOT advancing past idle_limit: keep reading
        clock = self.clock

        class SlowConn(FakeConn):
            def read1(self, n):
                if not self.steps:
                    clock.advance(1.0)  # 1s per read, idle_limit 30s -> no reconnect
                    raise TimeoutError("timed out")
                step = self.steps.pop(0)
                if isinstance(step, BaseException):  # a timeout costs one second of fake time
                    clock.advance(1.0)
                    raise step
                return step

        # one event, then 29 read timeouts (29s < 30s idle limit: no reconnect),
        # then EOF: the same single connection delivered the event
        # a second event after the timeouts must arrive on the SAME connection: a watchdog that
        # reconnected on the first timeout would never deliver it
        steps = ([b'data: {"late": true}\n\n'] + [TimeoutError("t")] * 29
                 + [b'data: {"later": true}\n\n', b""])
        conn = SlowConn(steps)
        # polling mode (read_timeout < idle_limit) is only valid with fake connections, see readers.py
        r, _ = self.run_scenario([conn], read_timeout=5.0, idle_limit=30.0)
        self.assertEqual([d for _, d in self.rec.events], ['{"late": true}', '{"later": true}'])
        self.assertEqual(self.rec.outages, [])  # no reconnect happened before EOF
        self.assertEqual(len(self.rec.connects), 1)


class TestBackoff(ReaderTestBase):
    def test_backoff_sequence_and_cap_and_reset(self):
        # every connection fails immediately with EOF, no events at all
        conns = [FakeConn([b""], self.clock) for _ in range(8)]
        r, _ = self.run_scenario(conns, backoff=(1.0, 60.0))
        self.assertEqual(self.slept, [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0])
        self.assertEqual(len(self.rec.connects), 8)

    def test_reset_after_good_connection(self):
        good = FakeConn([b'data: {"ok": 1}\n\n', b""], self.clock)
        bad = [FakeConn([b""], self.clock) for _ in range(3)]
        r, _ = self.run_scenario([bad[0], bad[1], good, bad[2]], backoff=(1.0, 60.0))
        # fail, fail, succeed (resets to 1), fail -> 1, 2, 1, 2
        self.assertEqual(self.slept, [1.0, 2.0, 1.0, 2.0])
        self.assertEqual(len(self.rec.events), 1)
        # an outage is reported only when a later connection delivers an event
        self.assertEqual(len(self.rec.outages), 1)


class TestStop(ReaderTestBase):
    def test_stop_event_ends_thread_and_closes_conn(self):
        clock = self.clock

        class BlockingConn(FakeConn):
            def read1(self, n):
                clock.advance(0.5)  # a read "takes" 0.5s; stop arrives during it
                if self.stop_flag.is_set():
                    raise TimeoutError("timed out")
                raise TimeoutError("timed out")

        conn = BlockingConn([])
        conn.stop_flag = self.stop
        r, _ = self.make_reader([conn], read_timeout=5.0)
        r.start()
        time.sleep(0.05)  # let it enter the read loop
        self.stop.set()
        r.join(5.0)
        self.assertFalse(r.is_alive())
        self.assertTrue(conn.closed)

    def test_thread_is_daemon(self):
        r, _ = self.run_scenario([FakeConn([b""], self.clock)])
        self.assertTrue(r.daemon)


if __name__ == "__main__":
    unittest.main()
