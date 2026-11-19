"""Tests for collector/__main__.py: argument parsing and config building only. Signal handling is NOT tested here (it is exercised by running the program and sending it SIGTERM).
"""

import argparse
import os
import sys
import unittest
from unittest import mock
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collector.__main__ import build_parser, config_from_args, main
from collector.app import Config


class TestBuildParser(unittest.TestCase):
    def test_defaults(self):
        args = build_parser().parse_args([])
        self.assertEqual(args.db, "data/trains.db")
        self.assertEqual(args.archive_dir, "data/archive")
        self.assertEqual(args.base_url, "https://railway.gov.gr")
        self.assertIsNone(args.start_date)
        self.assertIsNone(args.max_seconds)
        self.assertIsNone(args.log_file)
        self.assertFalse(args.board_catchup_only)
        self.assertFalse(args.verbose)

    def test_all_options(self):
        args = build_parser().parse_args([
            "--db", "/tmp/x.db", "--archive-dir", "/tmp/arch", "--base-url", "http://h",
            "--start-date", "2026-11-19", "--max-seconds", "30", "--log-file", "/tmp/l.log",
            "--board-catchup-only", "-v"])
        self.assertEqual(args.db, "/tmp/x.db")
        self.assertEqual(args.archive_dir, "/tmp/arch")
        self.assertEqual(args.base_url, "http://h")
        self.assertEqual(args.start_date, "2026-11-19")
        self.assertEqual(args.max_seconds, 30.0)
        self.assertIsInstance(args.max_seconds, float)
        self.assertEqual(args.log_file, "/tmp/l.log")
        self.assertTrue(args.board_catchup_only)
        self.assertTrue(args.verbose)
        args2 = build_parser().parse_args(["--verbose"])
        self.assertTrue(args2.verbose)

    def test_bad_start_date_rejected_by_config(self):
        args = build_parser().parse_args(["--start-date", "19-11-2026"])
        with self.assertRaises(ValueError):
            config_from_args(args)


class TestConfigFromArgs(unittest.TestCase):
    def test_default_config(self):
        cfg = config_from_args(build_parser().parse_args([]))
        self.assertIsInstance(cfg, Config)
        self.assertEqual(cfg.db_path, "data/trains.db")
        self.assertEqual(cfg.archive_dir, "data/archive")
        self.assertEqual(cfg.base_url, "https://railway.gov.gr")
        self.assertIsNone(cfg.start_date)
        self.assertIsNone(cfg.max_seconds)
        # the Config defaults of the task
        self.assertEqual(cfg.board_hour, 3)
        self.assertEqual(cfg.retry_after_s, 600)
        self.assertEqual(cfg.geo_retry_total_s, 600)

    def test_full_config(self):
        args = build_parser().parse_args([
            "--db", "a.db", "--archive-dir", "arch", "--base-url", "http://x",
            "--start-date", "2026-11-19", "--max-seconds", "12.5"])
        cfg = config_from_args(args)
        self.assertEqual(cfg.db_path, "a.db")
        self.assertEqual(cfg.archive_dir, "arch")
        self.assertEqual(cfg.base_url, "http://x")
        self.assertEqual(cfg.start_date, date(2026, 11, 19))
        self.assertEqual(cfg.max_seconds, 12.5)



class TestLonePullArguments(unittest.TestCase):
    def test_through_and_force_parse(self):
        args = build_parser().parse_args(["--board-catchup-only", "--through", "2026-11-26", "--force"])
        self.assertEqual((args.through, args.force), ("2026-11-26", True))

    def test_through_without_catchup_only_is_an_error(self):
        with self.assertRaises(SystemExit):
            main(["--through", "2026-11-26"])

    def test_force_without_through_is_an_error(self):
        with self.assertRaises(SystemExit):
            main(["--board-catchup-only", "--force"])

    def test_bad_through_date_is_an_error(self):
        with self.assertRaises(SystemExit):
            main(["--board-catchup-only", "--through", "26/11/2026"])

    def test_refused_pull_returns_2_with_a_clear_message(self):
        with mock.patch("collector.__main__.Collector") as cls, \
                mock.patch("collector.__main__.setup_logging"):
            cls.return_value.catchup_only.side_effect = ValueError("the board of 2026-11-26 is not final")
            self.assertEqual(main(["--board-catchup-only", "--through", "2026-11-26"]), 2)
            cls.return_value.catchup_only.assert_called_once()
            self.assertEqual(cls.return_value.catchup_only.call_args.kwargs,
                             {"through": date(2026, 11, 26), "force": False})

if __name__ == "__main__":
    unittest.main()
