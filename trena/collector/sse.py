"""Incremental Server-Sent Events (SSE) parser.

Parses the two railway streams (``/api/train-stream`` and
``/api/public/schedule-telemetry/stream``), which use ``event:``/``data:``
lines, ``:`` keep-alive comments and LF line endings. CRLF is accepted too.

Events may be split across arbitrary chunk boundaries; a trailing incomplete
event stays in the buffer until its terminating blank line arrives.
"""

from __future__ import annotations

DEFAULT_EVENT_NAME = "message"


class SSEParser:
    """Incremental SSE parser. Feed raw bytes, get completed events."""

    def __init__(self) -> None:
        self._buf = bytearray()
        # Fields of the event currently being assembled.
        self._event_name: str | None = None
        self._data_lines: list[str] = []

    def feed(self, chunk: bytes) -> list[tuple[str, str]]:
        """Append a chunk and return the ``(event_name, data)`` pairs completed by it."""
        self._buf.extend(chunk)
        out: list[tuple[str, str]] = []
        while True:
            # An event ends at the first blank line (LF or CRLF).
            nl = self._buf.find(b"\n")
            if nl == -1:
                break
            line = bytes(self._buf[:nl])
            del self._buf[: nl + 1]
            if line.endswith(b"\r"):
                line = line[:-1]
            if line == b"":
                # Blank line: dispatch the pending event, if any.
                if self._data_lines:
                    name = self._event_name if self._event_name is not None else DEFAULT_EVENT_NAME
                    out.append((name, "\n".join(self._data_lines)))
                self._event_name = None
                self._data_lines = []
                continue
            if line.startswith(b":"):
                continue  # comment / keep-alive
            if b":" in line:
                field, _, value = line.partition(b":")
                if value.startswith(b" "):
                    value = value[1:]  # a single leading space is stripped
                if field == b"event":
                    self._event_name = value.decode("utf-8", "replace")
                elif field == b"data":
                    self._data_lines.append(value.decode("utf-8", "replace"))
                # Other fields (id:, retry:) are ignored.
            # A line without ':' is a field name with an empty value; ignored.
        return out


def iter_events_from_bytes(data: bytes) -> list[tuple[str, str]]:
    """Parse a complete byte string in one shot (tests, archive replay)."""
    parser = SSEParser()
    events = parser.feed(data)
    return events
