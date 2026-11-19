"""One persistent SSE reader thread per stream (docs/DESIGN.md, section 3).

Each reader owns ONE stream forever: connect, parse SSE, hand events to a
callback, reconnect with exponential backoff on any failure. All I/O (the
HTTP connection) and all clocks are injectable so tests run with fakes and no
network, no real sleeping.
"""

from __future__ import annotations

import http.client
import logging
import socket
import threading
import time
import urllib.request
from collections.abc import Callable
from typing import Any

from .sse import SSEParser

log = logging.getLogger(__name__)

USER_AGENT = "trainose-collector/0.1"
READ_CHUNK = 65536


def default_connect(url: str, timeout: float) -> Any:
    """Open an SSE stream with ``urllib``; the response object has ``read1``/``close``."""
    req = urllib.request.Request(url, headers={
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
        "Accept-Encoding": "identity",
        "User-Agent": USER_AGENT,
    })
    return urllib.request.urlopen(req, timeout=timeout)


class SSEReader(threading.Thread):
    """Daemon thread reading one SSE stream until ``stop_event`` is set.

    Callbacks (never allowed to kill the thread):
      - ``on_event(name, data)`` for every parsed event,
      - ``on_connect(name)`` on every successful connect,
      - ``on_outage(name, lost_at, recovered_at, reason)`` once, when the
        connection after a lost one delivers its first event.
    """

    def __init__(
        self,
        name: str,
        url: str,
        on_event: Callable[[str, str], None],
        stop_event: threading.Event,
        *,
        connect: Callable[[str, float], Any] | None = None,
        on_outage: Callable[[str, int, int, str], None] | None = None,
        on_connect: Callable[[str], None] | None = None,
        on_lost: Callable[[str, int, str], None] | None = None,
        sleep: Callable[[float], None] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], float] = time.time,
        read_timeout: float = 30.0,
        idle_limit: float = 30.0,
        backoff: tuple[float, float] = (1.0, 60.0),
    ) -> None:
        super().__init__(name=f"sse-reader-{name}", daemon=True)
        self.stream_name = name
        self.url = url
        self.on_event = on_event
        self.stop_event = stop_event
        self._connect = connect or default_connect
        self._on_outage = on_outage
        self._on_connect = on_connect
        self._on_lost = on_lost
        self._sleep = sleep
        self._monotonic = monotonic
        self._now = now
        self._read_timeout = read_timeout
        self._idle_limit = idle_limit
        self._backoff = backoff

    def _wait(self, seconds: float) -> None:
        if self._sleep is not None:
            self._sleep(seconds)
        else:
            self.stop_event.wait(seconds)

    def _safe(self, cb: Callable | None, *args) -> None:
        if cb is None:
            return
        try:
            cb(*args)
        except Exception:
            log.exception("%s: callback %s failed", self.stream_name, getattr(cb, "__name__", cb))

    def run(self) -> None:
        delay = self._backoff[0]
        outage: tuple[int, str] | None = None  # (lost_at, reason) of the connection being lost
        ever_connected = False
        while not self.stop_event.is_set():
            conn = None
            got_event = False
            reason: str | None = None
            try:
                conn = self._connect(self.url, self._read_timeout)
            except Exception as exc:  # connect failure: same handling as a lost connection
                reason = repr(exc)
            else:
                ever_connected = True
                self._safe(self._on_connect, self.stream_name)
                parser = SSEParser()
                last_rx = self._monotonic()
                try:
                    while not self.stop_event.is_set():
                        try:
                            chunk = conn.read1(READ_CHUNK)
                        except (socket.timeout, TimeoutError):
                            if self.stop_event.is_set():
                                break
                            # A real socket cannot be read again after a timeout (urllib raises
                            # "cannot read from timed out object"), so in production
                            # read_timeout == idle_limit and a timeout means "idle": reconnect.
                            # Polling with a shorter timeout only works with fake connections.
                            if (self._read_timeout >= self._idle_limit
                                    or self._monotonic() - last_rx >= self._idle_limit):
                                reason = f"idle {self._idle_limit:g}s"
                                break
                            continue
                        if not chunk:  # b"": server closed the stream
                            reason = "EOF"
                            break
                        last_rx = self._monotonic()
                        for ev, data in parser.feed(chunk):
                            got_event = True
                            if outage is not None:
                                lost_at, why = outage
                                outage = None
                                if self._on_outage is not None:
                                    self._safe(self._on_outage, self.stream_name, lost_at,
                                               int(self._now()), why)
                            self._safe(self.on_event, ev, data)
                except Exception as exc:
                    # network errors are expected; anything else (a parser or bookkeeping bug)
                    # must still only cost a reconnect, never silently end this thread
                    reason = repr(exc)
                    if not isinstance(exc, (OSError, http.client.HTTPException)):
                        log.exception("%s: unexpected error in the read loop", self.stream_name)
                finally:
                    try:
                        conn.close()
                    except Exception:
                        log.debug("%s: error closing connection", self.stream_name, exc_info=True)
            if self.stop_event.is_set():
                break
            if reason is not None:
                log.warning("%s: connection lost (%s), reconnecting", self.stream_name, reason)
                if ever_connected:
                    # keep the earliest lost_at; the reason is the latest one
                    opening = outage is None
                    outage = (outage[0] if outage is not None else int(self._now()), reason)
                    if opening:  # tell the writer at loss time, not only after recovery
                        self._safe(self._on_lost, self.stream_name, outage[0], reason)
            if got_event:
                # a connection that delivered at least one event resets the backoff
                delay = self._backoff[0]
            self._wait(delay)
            delay = min(delay * 2, self._backoff[1])
        log.info("%s: reader stopped", self.stream_name)
