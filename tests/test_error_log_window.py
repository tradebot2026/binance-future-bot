"""Tests for 48-hour Telegram error log filtering."""

from __future__ import annotations

import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from config import Config
from database import DatabaseManager
from logger import parse_log_line_timestamp, read_recent_error_log_lines
from utils import utc_now


class TestErrorLogWindow(unittest.TestCase):
    def test_parse_log_line_timestamp(self) -> None:
        ts = parse_log_line_timestamp(
            "2026-09-20 13:54:24 | ERROR    | Error | boom"
        )
        self.assertIsNotNone(ts)
        self.assertEqual(ts.year, 2026)
        self.assertEqual(ts.month, 9)
        self.assertEqual(ts.day, 20)
        self.assertIsNone(parse_log_line_timestamp("    traceback continuation"))

    def test_read_recent_error_log_lines_drops_older_than_window(self) -> None:
        now = utc_now()
        fresh = (now - timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S")
        stale = (now - timedelta(hours=72)).strftime("%Y-%m-%d %H:%M:%S")
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp)
            (log_dir / "errors.log").write_text(
                "\n".join(
                    [
                        f"{stale} | ERROR    | Error | old failure",
                        "Traceback (most recent call last):",
                        '  File "bot.py", line 1, in <module>',
                        f"{fresh} | CRITICAL | Error | new failure",
                        "Traceback (most recent call last):",
                        '  File "bot.py", line 9, in run',
                    ]
                ),
                encoding="utf-8",
            )
            with patch.object(Config, "LOGS_DIR", str(log_dir)):
                lines = read_recent_error_log_lines(max_age_hours=48.0, limit=20)
            joined = "\n".join(lines)
            self.assertIn("new failure", joined)
            self.assertNotIn("old failure", joined)
            self.assertIn("File \"bot.py\", line 9", joined)
            self.assertNotIn("line 1, in <module>", joined)

    def test_get_recent_critical_errors_respects_48h_cutoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(Config, "DB_PATH", f"{tmp}/test.db"):
                db = DatabaseManager()
                old_ts = (utc_now() - timedelta(hours=60)).isoformat()
                new_ts = utc_now().isoformat()
                with db.connection() as conn:
                    conn.execute(
                        """
                        INSERT INTO critical_errors
                            (category, message, stack_trace, timestamp)
                        VALUES (?, ?, ?, ?)
                        """,
                        ("RATE_LIMIT", "stale ban", "", old_ts),
                    )
                    conn.execute(
                        """
                        INSERT INTO critical_errors
                            (category, message, stack_trace, timestamp)
                        VALUES (?, ?, ?, ?)
                        """,
                        ("ORDER_FAILURE", "fresh reject", "", new_ts),
                    )
                    conn.commit()
                rows = db.get_recent_critical_errors(limit=10, max_age_hours=48.0)
                messages = [row["message"] for row in rows]
                self.assertIn("fresh reject", messages)
                self.assertNotIn("stale ban", messages)


if __name__ == "__main__":
    unittest.main()
