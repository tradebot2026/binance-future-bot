"""Thread-local SQLite handles stay reusable across queries."""

from __future__ import annotations

import os
import tempfile
import threading
import unittest
from unittest.mock import patch

from database import DatabaseManager


class TestDatabaseThreadLocal(unittest.TestCase):
    def test_same_thread_reuses_connection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "bot.db")
            with patch("database.Config.DB_PATH", path):
                db = DatabaseManager()
                db._retain_connections = True
                try:
                    with db.connection() as first:
                        with db.connection() as second:
                            self.assertIs(first, second)
                            second.execute("SELECT 1")
                finally:
                    db.close()

    def test_parallel_threads_do_not_share_handles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "bot.db")
            with patch("database.Config.DB_PATH", path):
                db = DatabaseManager()
                db._retain_connections = True
                seen: dict[str, int] = {}
                lock = threading.Lock()

                def worker(name: str) -> None:
                    with db.connection() as conn:
                        conn.execute("SELECT 1")
                        with lock:
                            seen[name] = id(conn)
                    db.close_thread_connections()

                threads = [
                    threading.Thread(target=worker, args=(f"t{i}",))
                    for i in range(3)
                ]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
                db.close()
                self.assertEqual(len(set(seen.values())), 3)


class TestEntryStageClaim(unittest.TestCase):
    def test_claim_advances_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "bot.db")
            with patch("database.Config.DB_PATH", path):
                db = DatabaseManager()
                try:
                    db.log_trade(
                        {
                            "trade_id": "t-dca-1",
                            "symbol": "BTCUSDT",
                            "side": "LONG",
                            "entry_price": 8000.0,
                            "quantity": 1.0,
                            "status": "OPEN",
                            "metadata": {"entry_stage": 1, "base_entry_price": 8000.0},
                            "entry_stage": 1,
                        }
                    )
                    first = db.claim_entry_stage(
                        "t-dca-1", 1, 2, {"entry_stage": 1, "base_entry_price": 8000.0}
                    )
                    second = db.claim_entry_stage(
                        "t-dca-1", 1, 2, {"entry_stage": 1, "base_entry_price": 8000.0}
                    )
                    row = db.get_trade("t-dca-1")
                    self.assertTrue(first)
                    self.assertFalse(second)
                    self.assertEqual(int(row["entry_stage"]), 2)
                    meta = db.parse_trade_metadata(row)
                    self.assertEqual(int(meta["entry_stage"]), 2)
                finally:
                    db.close()


if __name__ == "__main__":
    unittest.main()

