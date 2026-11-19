"""Raw archive writer (docs/DESIGN.md, section 7; temporary by design).

Once per minute the scheduler appends the latest event of each type as one
JSON line ``{"t": ..., "ev": ..., "data": ...}`` to
``archive/YYYY-MM-DD.jsonl.gz`` (UTC date of ``t``). Every line is a NEW gzip
member (``gzip.open(path, "ab")`` per call), which is valid gzip and lets a
partially written last line be ignored when reading.

Thread-safe (one lock). I/O errors are logged, never raised.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

# event types archived once per minute (section 7)
SNAPSHOT_EVENTS = (
    "trainPositionsUx",
    "scheduleCards",
    "scheduleTelemetry",
    "scheduleTelemetryRaw",
    "scheduleTelemetryGhosts",
)


def day_file(directory: str, t: int) -> str:
    """``directory/YYYY-MM-DD.jsonl.gz`` for the UTC date of epoch ``t``."""
    day = datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d")
    return os.path.join(directory, f"{day}.jsonl.gz")


class ArchiveWriter:
    """Appends JSONL records as separate gzip members to per-day files."""

    def __init__(self, directory: str) -> None:
        self.directory = directory
        self.lock = threading.Lock()
        self._last_written: dict[str, Any] = {}  # event name -> payload object written last

    def write(self, t: int, ev: str, data: Any) -> bool:
        """Append one record; returns True on success, False (logged) on I/O error."""
        line = json.dumps({"t": t, "ev": ev, "data": data}, ensure_ascii=False,
                          separators=(",", ":")) + "\n"
        path = day_file(self.directory, t)
        try:
            with self.lock:
                os.makedirs(self.directory, exist_ok=True)
                # "ab" appends a NEW gzip member, so a crash mid-line cannot
                # corrupt the members before it
                with gzip.open(path, "ab") as f:
                    f.write(line.encode("utf-8"))
        except OSError as exc:
            log.warning("archive: cannot write %s: %s", path, exc)
            return False
        return True

    # ``trainHistory`` is written once per (re)connect through the same path
    def write_event(self, t: int, ev: str, data: Any) -> bool:
        return self.write(t, ev, data)

    def write_snapshot(self, state: Any, t: int) -> int:
        """Write the latest event of each archived type that is present in ``state``.

        An event whose payload is the same OBJECT as the one written last time for that
        name is skipped, so a minute in which the stream sent nothing new writes nothing. Returns the number of records written.
        """
        with state.lock:
            events = {name: state.latest_events.get(name) for name in SNAPSHOT_EVENTS}
        written = 0
        for name, payload in events.items():
            if payload is None:
                continue
            with self.lock:
                if self._last_written.get(name) is payload:
                    continue
                self._last_written[name] = payload
            if self.write(t, name, payload):
                written += 1
        return written
