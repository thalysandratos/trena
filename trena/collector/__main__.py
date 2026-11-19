"""CLI entry point: ``python -m collector`` (docs/DESIGN.md, section 10).

Parses the arguments, configures logging (UTC, stderr, optionally a rotating
file), then either runs a board-only catch-up or the full collector with
SIGTERM/SIGINT handlers that stop it cleanly.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
from datetime import date

from .app import Collector, Config

log = logging.getLogger("collector")

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m collector",
        description="TRAINOSE stream collector: samples the live SSE streams once a "
                    "minute into SQLite and fetches the schedule board daily.")
    p.add_argument("--db", default="data/trains.db", metavar="PATH",
                   help="SQLite database path (default: data/trains.db)")
    p.add_argument("--archive-dir", default="data/archive", metavar="PATH",
                   help="raw archive directory (default: data/archive)")
    p.add_argument("--base-url", default="https://railway.gov.gr", metavar="URL",
                   help="base URL of the railway API (default: https://railway.gov.gr)")
    p.add_argument("--start-date", default=None, metavar="YYYY-MM-DD",
                   help="first board date to catch up (default: today in Athens; "
                        "persisted in the DB, a restart does not move it)")
    p.add_argument("--max-seconds", default=None, type=float, metavar="N",
                   help="stop by itself after N seconds (dry runs)")
    p.add_argument("--log-file", default=None, metavar="PATH",
                   help="also log to this file (rotating, 5 MB x 3)")
    p.add_argument("--board-catchup-only", action="store_true",
                   help="only fetch and store all due schedule-board dates, then exit")
    p.add_argument("--through", default=None, metavar="YYYY-MM-DD",
                   help="with --board-catchup-only: fetch every board date up to and including "
                        "this day (the normal rule stops at day before yesterday); refused while "
                        "that day's board is not final, that is before 10:00 Athens time the "
                        "next day")
    p.add_argument("--force", action="store_true",
                   help="with --through: fetch even if the board is not final yet")
    p.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    return p


def config_from_args(args: argparse.Namespace) -> Config:
    """Build the ``Config`` from parsed arguments (``--start-date`` -> ``date``)."""
    start_date = None
    if args.start_date is not None:
        start_date = date.fromisoformat(args.start_date)  # ValueError on a bad format
    return Config(db_path=args.db, archive_dir=args.archive_dir, base_url=args.base_url,
                  start_date=start_date, max_seconds=args.max_seconds)


def setup_logging(args: argparse.Namespace) -> None:
    """UTC timestamps, the fixed format, to stderr and optionally to a rotating file."""
    logging.Formatter.converter = time.gmtime
    level = logging.DEBUG if args.verbose else logging.INFO
    root = logging.getLogger()
    root.setLevel(level)
    fmt = logging.Formatter(LOG_FORMAT)
    stream = logging.StreamHandler(sys.stderr)
    stream.setFormatter(fmt)
    root.addHandler(stream)
    if args.log_file:
        parent = os.path.dirname(os.path.abspath(args.log_file))
        os.makedirs(parent, exist_ok=True)
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(args.log_file, maxBytes=5_000_000, backupCount=3)
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if (args.through or args.force) and not args.board_catchup_only:
        parser.error("--through and --force only make sense with --board-catchup-only")
    if args.force and not args.through:
        parser.error("--force needs --through")
    through = None
    if args.through:
        try:
            through = date.fromisoformat(args.through)
        except ValueError:
            parser.error(f"--through: not a date (YYYY-MM-DD): {args.through!r}")
    setup_logging(args)
    if args.board_catchup_only:
        collector = Collector(config_from_args(args))
        try:
            stored = collector.catchup_only(through=through, force=args.force)
        except ValueError as exc:  # board not final yet: a clear message, no traceback
            print(f"refused: {exc}", file=sys.stderr)
            return 2
        except Exception:
            log.exception("board catch-up failed")
            return 1
        print("stored board dates: " + (", ".join(stored) if stored else "(none due)"))
        return 0
    for path in (args.db, args.log_file):
        if path:
            parent = os.path.dirname(os.path.abspath(path))
            os.makedirs(parent, exist_ok=True)
    collector = Collector(config_from_args(args))

    def handle_signal(signum, frame):
        log.info("signal %s received, stopping", signal.Signals(signum).name)
        collector.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    try:
        collector.setup()
    except Exception:
        log.exception("setup failed")
        return 1
    collector.run()  # run() ends with shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
