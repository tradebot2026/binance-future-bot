"""
Thread-safe SQLite persistence layer.
Optimized for 24/7 VPS deployment with WAL mode and minimal hot-path I/O.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import timedelta
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
from utils import safe_float, utc_now


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
                self._ensure_column(cursor, "signals", "accepted", "INTEGER DEFAULT 0")
                self._ensure_column(cursor, "signals", "outcome", "TEXT")
                self._ensure_column(cursor, "signals", "structure_metadata", "TEXT")
                self._ensure_column(cursor, "signals", "rejection_reason", "TEXT")

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS signal_rejections (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT,
                        direction TEXT,
                        score REAL,
                        timestamp TEXT,
                        strategy TEXT,
                        reasons TEXT
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS symbol_cooldowns (
                        symbol TEXT PRIMARY KEY,
                        cooldown_until TEXT,
                        reason TEXT
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS critical_errors (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        category TEXT NOT NULL,
                        message TEXT NOT NULL,
                        stack_trace TEXT,
                        timestamp TEXT NOT NULL
                    )
                    """
                )

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
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_signal_rejection_symbol ON signal_rejections(symbol)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_trades_closed_at ON trades(closed_at)"
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS signal_conflicts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT,
                        strategy TEXT,
                        direction TEXT,
                        score REAL,
                        timestamp TEXT,
                        reason TEXT,
                        context TEXT
                    )
                    """
                )

                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_signal_conflicts_symbol ON signal_conflicts(symbol)"
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

    def count_active_trades_by_strategy(self, strategy: str) -> int:
        placeholders = ",".join("?" for _ in ACTIVE_TRADE_STATUSES)
        query = (
            f"SELECT COUNT(*) FROM trades WHERE status IN ({placeholders}) AND strategy = ?"
        )
        try:
            with self.connection() as conn:
                row = conn.execute(query, (*ACTIVE_TRADE_STATUSES, strategy)).fetchone()
                return int(row[0]) if row else 0
        except sqlite3.Error as exc:
            error_logger.error("Failed to count active trades for %s: %s", strategy, exc)
            return 0

    def get_strategy_daily_realized_pnl(self, date_str: str, strategy: str) -> float:
        """Sum realized PnL from closed trades for a strategy on a UTC day."""
        try:
            with self.connection() as conn:
                row = conn.execute(
                    """
                    SELECT COALESCE(SUM(pnl), 0.0) FROM trades
                    WHERE strategy = ?
                      AND status = 'CLOSED'
                      AND closed_at IS NOT NULL
                      AND closed_at LIKE ?
                    """,
                    (strategy, f"{date_str}%"),
                ).fetchone()
                return safe_float(row[0]) if row else 0.0
        except sqlite3.Error as exc:
            error_logger.error(
                "Failed to fetch strategy daily PnL for %s: %s", strategy, exc
            )
            return 0.0

    def count_consecutive_strategy_losses(
        self, strategy: str, lookback: int = 5
    ) -> int:
        if lookback <= 0:
            return 0
        try:
            with self.connection() as conn:
                rows = conn.execute(
                    """
                    SELECT pnl FROM trades
                    WHERE strategy = ?
                      AND status = 'CLOSED'
                      AND closed_at IS NOT NULL
                    ORDER BY closed_at DESC
                    LIMIT ?
                    """,
                    (strategy, lookback),
                ).fetchall()
        except sqlite3.Error as exc:
            error_logger.error(
                "Failed to count consecutive losses for %s: %s", strategy, exc
            )
            return 0

        consecutive = 0
        for row in rows:
            pnl = safe_float(row[0])
            if pnl < 0:
                consecutive += 1
            else:
                break
        return consecutive

    def is_range_entries_paused(self, date_str: str) -> tuple[bool, str]:
        """Kill-switch: pause RANGE entries after daily loss or consecutive SLs."""
        from constants import STRATEGY_RANGE_REVERSION

        return self.is_strategy_entries_paused(
            STRATEGY_RANGE_REVERSION, date_str=date_str, health_check=True
        )

    def is_global_entries_paused(self) -> tuple[bool, str]:
        """Global daily circuit breaker from scheduler."""
        from utils import utc_today_str

        stats = self.get_daily_stats(utc_today_str()) or {}
        if stats.get("status") == DAILY_STATUS_PAUSED:
            return True, "Daily PnL limit reached — entries paused for today."
        return False, ""

    def is_strategy_entries_paused(
        self,
        strategy: str,
        date_str: str | None = None,
        health_check: bool = True,
    ) -> tuple[bool, str]:
        """Per-strategy kill-switch mirroring Range pattern."""
        from config import Config
        from constants import STRATEGY_RANGE_REVERSION, STRATEGY_SMC_TREND
        from utils import utc_today_str

        if date_str is None:
            date_str = utc_today_str()

        if not health_check:
            return False, ""

        limits = {
            STRATEGY_RANGE_REVERSION: (
                Config.RANGE_DAILY_MAX_LOSS_PERCENT,
                Config.RANGE_MAX_CONSECUTIVE_LOSSES,
            ),
            STRATEGY_SMC_TREND: (
                Config.SMC_DAILY_MAX_LOSS_PERCENT,
                Config.SMC_MAX_CONSECUTIVE_LOSSES,
            ),
            "SMC_MULTITF": (
                Config.SMC_DAILY_MAX_LOSS_PERCENT,
                Config.SMC_MAX_CONSECUTIVE_LOSSES,
            ),
        }
        daily_limit, consec_limit = limits.get(strategy.upper(), (0.0, 0))
        if daily_limit <= 0 and consec_limit <= 0:
            return False, ""

        stats = self.get_daily_stats(date_str) or {}
        start_balance = safe_float(stats.get("start_balance"))
        strategy_pnl = self.get_strategy_daily_realized_pnl(date_str, strategy)

        if start_balance > 0 and daily_limit > 0:
            pnl_pct = (strategy_pnl / start_balance) * 100.0
            if pnl_pct <= -daily_limit:
                return True, (
                    f"{strategy} daily loss limit hit ({pnl_pct:.2f}% <= "
                    f"-{daily_limit:.2f}%)"
                )

        if consec_limit > 0:
            consec = self.count_consecutive_strategy_losses(strategy, consec_limit)
            if consec >= consec_limit:
                return True, (
                    f"{strategy} consecutive SL limit ({consec}/{consec_limit})"
                )
        return False, ""

    def log_signal_conflict(
        self,
        symbol: str,
        strategy: str,
        direction: str,
        score: float,
        reason: str,
        context: str = "conflict_guard",
    ) -> None:
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute(
                        """
                        INSERT INTO signal_conflicts
                            (symbol, strategy, direction, score, timestamp, reason, context)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            symbol.upper(),
                            strategy,
                            direction,
                            score,
                            utc_now().isoformat(),
                            reason,
                            context,
                        ),
                    )
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to log signal conflict: %s", exc)

    def get_open_trades_for_symbol(self, symbol: str) -> List[dict[str, Any]]:
        placeholders = ",".join("?" for _ in ACTIVE_TRADE_STATUSES)
        query = (
            f"SELECT * FROM trades WHERE symbol = ? AND status IN ({placeholders}) "
            "ORDER BY opened_at ASC"
        )
        try:
            with self.connection() as conn:
                rows = conn.execute(
                    query, (symbol.upper(), *ACTIVE_TRADE_STATUSES)
                ).fetchall()
                return [dict(row) for row in rows]
        except sqlite3.Error as exc:
            error_logger.error("Failed to fetch open trades for %s: %s", symbol, exc)
            return []

    def get_open_trades_by_strategy(self, strategy: str) -> List[dict[str, Any]]:
        placeholders = ",".join("?" for _ in ACTIVE_TRADE_STATUSES)
        query = (
            f"SELECT * FROM trades WHERE strategy = ? AND status IN ({placeholders}) "
            "ORDER BY opened_at ASC"
        )
        try:
            with self.connection() as conn:
                rows = conn.execute(
                    query, (strategy, *ACTIVE_TRADE_STATUSES)
                ).fetchall()
                return [dict(row) for row in rows]
        except sqlite3.Error as exc:
            error_logger.error(
                "Failed to fetch open trades for strategy %s: %s", strategy, exc
            )
            return []

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
        now = utc_now()
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
        now_str = utc_now().isoformat()
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
                    (symbol, utc_now().isoformat()),
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
        now = utc_now().isoformat()
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

    def log_signal(self, signal_data: dict[str, Any]) -> Optional[int]:
        now = utc_now().isoformat()
        metadata = signal_data.get("structure_metadata")
        if isinstance(metadata, dict):
            metadata = json.dumps(metadata)
        with self._write_lock:
            try:
                with self.connection() as conn:
                    cursor = conn.execute(
                        """
                        INSERT INTO signals (
                            symbol, timeframe, direction, score, timestamp, strategy, reason,
                            accepted, outcome, structure_metadata, rejection_reason
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            signal_data.get("symbol"),
                            signal_data.get("timeframe"),
                            signal_data.get("direction"),
                            signal_data.get("score"),
                            now,
                            signal_data.get("strategy"),
                            signal_data.get("reason"),
                            1 if signal_data.get("accepted") else 0,
                            signal_data.get("outcome"),
                            metadata,
                            signal_data.get("rejection_reason"),
                        ),
                    )
                    conn.commit()
                    return int(cursor.lastrowid)
            except sqlite3.Error as exc:
                error_logger.error("Failed to log signal: %s", exc)
                return None

    def log_signal_rejection(
        self,
        symbol: str,
        direction: str,
        score: float,
        reasons: list[str],
        strategy: str = "SMC_MULTITF",
    ) -> None:
        now = utc_now().isoformat()
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute(
                        """
                        INSERT INTO signal_rejections (
                            symbol, direction, score, timestamp, strategy, reasons
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            symbol,
                            direction,
                            score,
                            now,
                            strategy,
                            json.dumps(reasons),
                        ),
                    )
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to log signal rejection: %s", exc)

    def set_symbol_cooldown(self, symbol: str, minutes: int, reason: str) -> None:
        until = (utc_now() + timedelta(minutes=minutes)).isoformat()
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO symbol_cooldowns (symbol, cooldown_until, reason)
                        VALUES (?, ?, ?)
                        """,
                        (symbol, until, reason),
                    )
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to set symbol cooldown for %s: %s", symbol, exc)

    def is_symbol_on_cooldown(self, symbol: str) -> tuple[bool, str]:
        try:
            with self.connection() as conn:
                row = conn.execute(
                    "SELECT cooldown_until, reason FROM symbol_cooldowns WHERE symbol = ?",
                    (symbol,),
                ).fetchone()
                if not row:
                    return False, ""
                until = str(row[0])
                reason = str(row[1] or "cooldown")
                if until <= utc_now().isoformat():
                    return False, ""
                return True, reason
        except sqlite3.Error as exc:
            error_logger.error("Cooldown check failed for %s: %s", symbol, exc)
            return False, ""

    def cleanup_expired_cooldowns(self) -> None:
        now = utc_now().isoformat()
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute(
                        "DELETE FROM symbol_cooldowns WHERE cooldown_until <= ?",
                        (now,),
                    )
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to cleanup cooldowns: %s", exc)

    def record_signal_outcome(self, trade_id: str, outcome: str) -> None:
        """Link latest accepted signal for trade symbol to win/loss outcome."""
        trade = self.get_trade(trade_id)
        if not trade:
            return
        symbol = trade.get("symbol")
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute(
                        """
                        UPDATE signals
                        SET outcome = ?
                        WHERE id = (
                            SELECT id FROM signals
                            WHERE symbol = ? AND accepted = 1
                            ORDER BY timestamp DESC
                            LIMIT 1
                        )
                        """,
                        (outcome, symbol),
                    )
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to record signal outcome: %s", exc)

    def get_daily_trade_analytics(self, date_str: str) -> dict[str, Any]:
        """Win rate, W/L breakdown, profit factor for closed trades on a UTC day."""
        try:
            with self.connection() as conn:
                rows = conn.execute(
                    """
                    SELECT pnl FROM trades
                    WHERE status = 'CLOSED'
                      AND closed_at IS NOT NULL
                      AND closed_at LIKE ?
                    """,
                    (f"{date_str}%",),
                ).fetchall()
        except sqlite3.Error as exc:
            error_logger.error("Failed to fetch daily analytics: %s", exc)
            return {
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "total_pnl": 0.0,
            }

        pnls = [safe_float(row[0]) for row in rows]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        win_count = len(wins)
        loss_count = len(losses)
        total = win_count + loss_count
        win_rate = (win_count / total * 100.0) if total > 0 else 0.0
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        if gross_loss > 0:
            profit_factor = gross_profit / gross_loss
        elif gross_profit > 0:
            profit_factor = float("inf")
        else:
            profit_factor = 0.0

        return {
            "wins": win_count,
            "losses": loss_count,
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "total_pnl": sum(pnls),
        }

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

    # ---------------- Critical errors ----------------

    def log_critical_error(
        self,
        category: str,
        message: str,
        stack_trace: str = "",
    ) -> None:
        now = utc_now().isoformat()
        with self._write_lock:
            try:
                with self.connection() as conn:
                    conn.execute(
                        """
                        INSERT INTO critical_errors (category, message, stack_trace, timestamp)
                        VALUES (?, ?, ?, ?)
                        """,
                        (category, message, stack_trace, now),
                    )
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to log critical error: %s", exc)

    def get_recent_critical_errors(self, limit: int = 15) -> list[dict[str, Any]]:
        try:
            with self.connection() as conn:
                rows = conn.execute(
                    """
                    SELECT category, message, timestamp
                    FROM critical_errors
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
                return [
                    {
                        "category": str(row[0]),
                        "message": str(row[1]),
                        "timestamp": str(row[2]),
                    }
                    for row in rows
                ]
        except sqlite3.Error as exc:
            error_logger.error("Failed to fetch critical errors: %s", exc)
            return []

    # ---------------- Maintenance ----------------

    def purge_old_records(self, retention_days: int | None = None) -> dict[str, int]:
        """Delete log-style records older than retention_days."""
        days = retention_days if retention_days is not None else Config.DB_RETENTION_DAYS
        cutoff = (utc_now() - timedelta(days=days)).isoformat()
        purged: dict[str, int] = {}

        with self._write_lock:
            try:
                with self.connection() as conn:
                    for table, column in (
                        ("signal_rejections", "timestamp"),
                        ("critical_errors", "timestamp"),
                        ("signals", "timestamp"),
                    ):
                        cursor = conn.execute(
                            f"DELETE FROM {table} WHERE {column} < ?",
                            (cutoff,),
                        )
                        purged[table] = cursor.rowcount
                    conn.commit()
            except sqlite3.Error as exc:
                error_logger.error("Failed to purge old records: %s", exc)

        if any(purged.values()):
            system_logger.info(
                "Purged old DB records (>%sd): %s",
                days,
                ", ".join(f"{k}={v}" for k, v in purged.items() if v),
            )
        return purged

    def run_maintenance(
        self,
        retention_days: int | None = None,
        vacuum: bool = True,
    ) -> dict[str, Any]:
        """Purge stale logs and optionally VACUUM the database."""
        purged = self.purge_old_records(retention_days)
        self.cleanup_expired_cooldowns()
        self.cleanup_expired_blacklist()
        if vacuum:
            self.vacuum_database()
        return {"purged": purged, "vacuum": vacuum}

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
