"""
Thread-safe SQLite persistence layer.
Optimized for 24/7 VPS deployment with WAL mode and minimal hot-path I/O.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Generator, List, Optional

import pandas as pd

from config import Config
from constants import (
    ACTIVE_TRADE_STATUSES,
    ALLOWED_TRADE_COLUMNS,
    DAILY_STATUS_ACTIVE,
    DAILY_STATUS_PAUSED,
)
from exceptions import DatabaseError
from logger import error_logger, system_logger


class DatabaseManager:
    """SQLite access layer with thread-safe writes and optimized read helpers."""

    def __init__(self) -> None:
        self.db_path = Config.DB_PATH
        self._write_lock = threading.RLock()
        self._initialize_tables()

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        """
        Public thread-safe connection context manager.
        Safe for use from the main loop and Telegram daemon thread.
        """
        conn = self._create_connection()
        try:
            yield conn
        finally:
            conn.close()

    def _create_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            timeout=30.0,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA cache_size = -20000")
        return conn

    def _initialize_tables(self) -> None:
        try:
            with self.connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS trades (
                        trade_id TEXT PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        entry_price REAL,
                        quantity REAL,
                        status TEXT,
                        take_profit_1 REAL,
                        take_profit_2 REAL,
                        take_profit_3 REAL,
                        stop_loss REAL,
                        pnl REAL DEFAULT 0.0,
                        opened_at TEXT,
                        closed_at TEXT,
                        strategy TEXT,
                        score REAL,
                        leverage INTEGER,
                        margin REAL,
                        fee REAL DEFAULT 0.0,
                        exit_reason TEXT,
                        duration INTEGER,
                        metadata TEXT,
                        exchange_order_id TEXT
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS signals (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT,
                        timeframe TEXT,
                        direction TEXT,
                        score REAL,
                        timestamp TEXT,
                        strategy TEXT,
                        reason TEXT
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS daily_stats (
                        date TEXT PRIMARY KEY,
                        start_balance REAL,
                        current_balance REAL,
                        total_pnl REAL DEFAULT 0.0,
                        trades_count INTEGER DEFAULT 0,
                        status TEXT DEFAULT 'ACTIVE'
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS watchlist (
                        symbol TEXT PRIMARY KEY,
                        score REAL,
                        added_at TEXT,
                        direction TEXT
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS blacklist (
                        symbol TEXT PRIMARY KEY,
                        reason TEXT,
                        added_at TEXT,
                        expires_at TEXT
                    )
                    """
                )

                self._ensure_column(cursor, "trades", "metadata", "TEXT")
                self._ensure_column(cursor, "trades", "exchange_order_id", "TEXT")
                self._ensure_column(cursor, "daily_stats", "entries_count", "INTEGER DEFAULT 0")

                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_trade_symbol ON trades(symbol)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_trade_status ON trades(status)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_trade_opened_at ON trades(opened_at)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_blacklist_expiry ON blacklist(expires_at)"
                )

                conn.commit()
                system_logger.info(
                    "Database initialized (WAL mode, thread-safe connections enabled)."
                )
        except sqlite3.Error as exc:
            error_logger.error("Database initialization failed: %s", exc)
            raise DatabaseError("Failed to initialize database") from exc

    @staticmethod
    def _ensure_column(
        cursor: sqlite3.Cursor, table: str, column: str, column_type: str
    ) -> None:
        cursor.execute(f"PRAGMA table_info({table})")
        existing = {row[1] for row in cursor.fetchall()}
        if column not in existing:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")

    # ---------------- Optimized read helpers ----------------

    def get_active_trades_count(self) -> int:
        """Scalar COUNT(*) for open / partial positions — hot path for main loop."""
        placeholders = ",".join("?" for _ in ACTIVE_TRADE_STATUSES)
        query = f"SELECT COUNT(*) FROM trades WHERE status IN ({placeholders})"
        try:
            with self.connection() as conn:
                row = conn.execute(query, ACTIVE_TRADE_STATUSES).fetchone()
                return int(row[0]) if row else 0
        except sqlite3.Error as exc:
            error_logger.error("Failed to count active trades: %s", exc)
            return 0

    def get_open_trades(self) -> List[dict[str, Any]]:
        """Return active trades ordered FIFO."""
        placeholders = ",".join("?" for _ in ACTIVE_TRADE_STATUSES)
        query = (
            f"SELECT * FROM trades WHERE status IN ({placeholders}) "
            "ORDER BY opened_at ASC"
        )
        try:
            with self.connection() as conn:
                rows = conn.execute(query, ACTIVE_TRADE_STATUSES).fetchall()
                return [dict(row) for row in rows]
        except sqlite3.Error as exc:
            error_logger.error("Failed to fetch open trades: %s", exc)
            return []

    def get_trade(self, trade_id: str) -> Optional[dict[str, Any]]:
        try:
            with self.connection() as conn:
                row = conn.execute(
                    "SELECT * FROM trades WHERE trade_id = ?", (trade_id,)
                ).fetchone()
                return dict(row) if row else None
        except sqlite3.Error as exc:
            error_logger.error("Failed to fetch trade %s: %s", trade_id, exc)
            return None

    def get_all_trades_df(self) -> pd.DataFrame:
        """Full trade history — use for reporting, not hot-path loops."""
        try:
            with self.connection() as conn:
                return pd.read_sql_query("SELECT * FROM trades ORDER BY opened_at ASC", conn)
        except Exception as exc:
            error_logger.error("Failed to export trades dataframe: %s", exc)
            return pd.DataFrame()

    def get_all_daily_stats_df(self) -> pd.DataFrame:
        """Daily stats for reporter module."""
        try:
            with self.connection() as conn:
                return pd.read_sql_query(
                    "SELECT * FROM daily_stats ORDER BY date DESC", conn
                )
        except Exception as exc:
            error_logger.error("Failed to export daily stats dataframe: %s", exc)
            return pd.DataFrame()

    # ---------------- Daily statistics ----------------

    def get_daily_stats(self, date_str: str) -> Optional[dict[str, Any]]:
        try:
            with self.connection() as conn:
                row = conn.execute(
                    "SELECT * FROM daily_stats WHERE date = ?", (date_str,)
                ).fetchone()
                return dict(row) if row else None
        except sqlite3.Error as exc:
            error_logger.error("Failed to read daily stats for %s: %s", date_str, exc)
            return None

    def initialize_daily_stats(self, date_str: str, start_balance: float) -> None:
        """Create a new trading-day row (idempotent — skips if already exists)."""
        with self._write_lock:
            try:
                with self.connection() as conn:
                    existing = conn.execute(
                        "SELECT 1 FROM daily_stats WHERE date = ?", (date_str,)
                    ).fetchone()
                    if existing:
                        return
                    conn.execute(
                        """
                        INSERT INTO daily_stats (
                            date, start_balance, current_balance, total_pnl,
                            trades_count, entries_count, status
                        ) VALUES (?, ?, ?, 0.0, 0, 0, ?)
                        """,
                        (date_str, start_balance, start_balance, DAILY_STATUS_ACTIVE),
                    )
                    conn.commit()
                    system_logger.info(
                        "Initialized daily stats for %s with balance $%.2f",
                        date_str,
                        start_balance,
                    )
            except sqlite3.Error as exc:
                error_logger.error("Failed to initialize daily stats: %s", exc)
                raise DatabaseError("initialize_daily_stats failed") from exc

    def update_daily_balance(
        self,
        date_str: str,
        current_balance: float,
        status: str = DAILY_STATUS_ACTIVE,
    ) -> None:
        """Update running balance without incrementing trade count."""
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute(
                        """
                        UPDATE daily_stats
                        SET current_balance = ?, status = ?
                        WHERE date = ?
                        """,
                        (current_balance, status, date_str),
                    )
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to update daily balance: %s", exc)

    def add_daily_realized_pnl(self, date_str: str, realized_pnl: float) -> None:
        """Add realized PnL from a partial or full exit to today's running total."""
        if realized_pnl == 0:
            return
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute(
                        """
                        UPDATE daily_stats
                        SET total_pnl = total_pnl + ?
                        WHERE date = ?
                        """,
                        (realized_pnl, date_str),
                    )
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to add daily realized PnL: %s", exc)

    def increment_daily_entries(self, date_str: str) -> None:
        """Increment the number of new entries opened today."""
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute(
                        """
                        UPDATE daily_stats
                        SET entries_count = COALESCE(entries_count, 0) + 1
                        WHERE date = ?
                        """,
                        (date_str,),
                    )
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to increment daily entries: %s", exc)

    def record_closed_trade(
        self,
        date_str: str,
        current_balance: float,
    ) -> None:
        """Increment closed-trade count and refresh balance when a position fully closes."""
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute(
                        """
                        UPDATE daily_stats
                        SET current_balance = ?,
                            trades_count = trades_count + 1
                        WHERE date = ?
                        """,
                        (current_balance, date_str),
                    )
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to record closed trade stats: %s", exc)

    def set_daily_status(self, date_str: str, status: str, current_balance: float) -> None:
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute(
                        """
                        UPDATE daily_stats
                        SET status = ?, current_balance = ?
                        WHERE date = ?
                        """,
                        (status, current_balance, date_str),
                    )
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to set daily status: %s", exc)

    def reset_daily_status(self, date_str: str, balance: float) -> None:
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO daily_stats (
                            date, start_balance, current_balance, total_pnl,
                            trades_count, entries_count, status
                        ) VALUES (?, ?, ?, 0.0, 0, 0, ?)
                        """,
                        (date_str, balance, balance, DAILY_STATUS_ACTIVE),
                    )
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to reset daily stats: %s", exc)

    # ---------------- Blacklist ----------------

    def add_to_blacklist(
        self, symbol: str, reason: str, duration_hours: Optional[int] = None
    ) -> None:
        now = datetime.now(UTC)
        expires_at = (
            (now + timedelta(hours=duration_hours)).isoformat()
            if duration_hours
            else None
        )
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO blacklist (symbol, reason, added_at, expires_at)
                        VALUES (?, ?, ?, ?)
                        """,
                        (symbol, reason, now.isoformat(), expires_at),
                    )
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to blacklist %s: %s", symbol, exc)

    def cleanup_expired_blacklist(self) -> None:
        now_str = datetime.now(UTC).isoformat()
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute(
                        "DELETE FROM blacklist WHERE expires_at IS NOT NULL AND expires_at < ?",
                        (now_str,),
                    )
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to cleanup blacklist: %s", exc)

    def is_blacklisted(self, symbol: str) -> bool:
        try:
            with self.connection() as conn:
                row = conn.execute(
                    """
                    SELECT 1 FROM blacklist
                    WHERE symbol = ?
                      AND (expires_at IS NULL OR expires_at >= ?)
                    """,
                    (symbol, datetime.now(UTC).isoformat()),
                ).fetchone()
                return row is not None
        except sqlite3.Error as exc:
            error_logger.error("Blacklist check failed for %s: %s", symbol, exc)
            return False

    def remove_from_blacklist(self, symbol: str) -> None:
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute("DELETE FROM blacklist WHERE symbol = ?", (symbol,))
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to remove blacklist entry: %s", exc)

    # ---------------- Watchlist ----------------

    def update_watchlist(self, candidates: List[dict[str, Any]]) -> None:
        now = datetime.now(UTC).isoformat()
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute("DELETE FROM watchlist")
                    for candidate in candidates:
                        direction = candidate.get("direction") or candidate.get("action", "LONG")
                        conn.execute(
                            """
                            INSERT INTO watchlist (symbol, score, added_at, direction)
                            VALUES (?, ?, ?, ?)
                            """,
                            (
                                candidate["symbol"],
                                candidate.get("score", 0.0),
                                now,
                                direction,
                            ),
                        )
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to update watchlist: %s", exc)

    def get_watchlist(self) -> List[dict[str, Any]]:
        try:
            with self.connection() as conn:
                rows = conn.execute(
                    "SELECT * FROM watchlist ORDER BY score DESC"
                ).fetchall()
                return [dict(row) for row in rows]
        except sqlite3.Error as exc:
            error_logger.error("Failed to read watchlist: %s", exc)
            return []

    # ---------------- Signals ----------------

    def log_signal(self, signal_data: dict[str, Any]) -> None:
        now = datetime.now(UTC).isoformat()
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute(
                        """
                        INSERT INTO signals (
                            symbol, timeframe, direction, score, timestamp, strategy, reason
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            signal_data.get("symbol"),
                            signal_data.get("timeframe"),
                            signal_data.get("direction"),
                            signal_data.get("score"),
                            now,
                            signal_data.get("strategy"),
                            signal_data.get("reason"),
                        ),
                    )
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to log signal: %s", exc)

    # ---------------- Trades ----------------

    def log_trade(self, trade_data: dict[str, Any]) -> None:
        metadata = trade_data.get("metadata")
        if isinstance(metadata, dict):
            metadata = json.dumps(metadata)

        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute(
                        """
                        INSERT INTO trades (
                            trade_id, symbol, side, entry_price, quantity, status,
                            take_profit_1, take_profit_2, take_profit_3, stop_loss, pnl,
                            opened_at, closed_at, strategy, score, leverage, margin, fee,
                            exit_reason, duration, metadata, exchange_order_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            trade_data.get("trade_id"),
                            trade_data.get("symbol"),
                            trade_data.get("side"),
                            trade_data.get("entry_price"),
                            trade_data.get("quantity"),
                            trade_data.get("status", "OPEN"),
                            trade_data.get("take_profit_1"),
                            trade_data.get("take_profit_2"),
                            trade_data.get("take_profit_3"),
                            trade_data.get("stop_loss"),
                            trade_data.get("pnl", 0.0),
                            trade_data.get("opened_at"),
                            trade_data.get("closed_at"),
                            trade_data.get("strategy"),
                            trade_data.get("score"),
                            trade_data.get("leverage"),
                            trade_data.get("margin"),
                            trade_data.get("fee", 0.0),
                            trade_data.get("exit_reason"),
                            trade_data.get("duration"),
                            metadata,
                            trade_data.get("exchange_order_id"),
                        ),
                    )
                    conn.commit()
                    system_logger.info(
                        "Trade logged | %s | %s | %s",
                        trade_data.get("symbol"),
                        trade_data.get("side"),
                        trade_data.get("status", "OPEN"),
                    )
            except sqlite3.IntegrityError:
                error_logger.warning(
                    "Duplicate trade_id %s — insert skipped.", trade_data.get("trade_id")
                )
            except sqlite3.Error as exc:
                error_logger.error("Failed to log trade: %s", exc)
                raise DatabaseError("log_trade failed") from exc

    def update_trade(self, trade_id: str, updates: dict[str, Any]) -> bool:
        if not updates:
            return False

        filtered = {k: v for k, v in updates.items() if k in ALLOWED_TRADE_COLUMNS}
        if not filtered:
            error_logger.warning("No valid columns for trade update: %s", trade_id)
            return False

        if "metadata" in filtered and isinstance(filtered["metadata"], dict):
            filtered["metadata"] = json.dumps(filtered["metadata"])

        set_clause = ", ".join(f"{column} = ?" for column in filtered)
        values: list[Any] = list(filtered.values())
        values.append(trade_id)

        with self._write_lock:
            try:
                with self.connection() as conn:
                    cursor = conn.execute(
                        f"UPDATE trades SET {set_clause} WHERE trade_id = ?",
                        tuple(values),
                    )
                    conn.commit()
                    if cursor.rowcount == 0:
                        error_logger.warning("Trade not found for update: %s", trade_id)
                        return False
                    return True
            except sqlite3.Error as exc:
                error_logger.error("Failed to update trade %s: %s", trade_id, exc)
                return False

    @staticmethod
    def parse_trade_metadata(trade: dict[str, Any]) -> dict[str, Any]:
        raw = trade.get("metadata")
        if not raw:
            return {}
        if isinstance(raw, dict):
            return raw
        try:
            return json.loads(str(raw))
        except json.JSONDecodeError:
            return {}

    # ---------------- Maintenance ----------------

    def vacuum_database(self) -> None:
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.isolation_level = None
                    conn.execute("PRAGMA wal_checkpoint(FULL)")
                    conn.execute("VACUUM")
                    system_logger.info("Database maintenance complete (WAL checkpoint + VACUUM).")
            except sqlite3.Error as exc:
                error_logger.error("Database vacuum failed: %s", exc)
