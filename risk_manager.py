"""
Portfolio risk manager module.
Pre-trade gates for max positions, daily entry count, consecutive losses,
and PnL-based drawdown. Works alongside DailyScheduler (daily PnL pause).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Optional

from config import Config
from constants import DAILY_STATUS_PAUSED, TRADE_STATUS_CLOSED
from database import DatabaseManager
from exchange import BinanceExchangeManager
from logger import performance_logger, trade_logger
from utils import safe_float


@dataclass
class DailyPnLMetrics:
    """Daily performance based on realized + unrealized PnL, not wallet balance."""

    start_balance: float
    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float
    pnl_percent: float


@dataclass
class RiskSnapshot:
    """Point-in-time portfolio risk metrics."""

    open_positions: int
    daily_entries: int
    daily_trades: int
    consecutive_losses: int
    drawdown_percent: float
    current_balance: float
    daily_pnl: float
    daily_pnl_percent: float
    unrealized_pnl: float
    entries_allowed: bool
    block_reason: str


def compute_daily_pnl_metrics(
    exchange: BinanceExchangeManager,
    db: DatabaseManager,
    date_str: str,
) -> DailyPnLMetrics:
    """
    Compute today's PnL from realized closes/partials plus open-position unrealized PnL.
    Margin allocation for new entries does not affect this metric.
    """
    stats = db.get_daily_stats(date_str) or {}
    start_balance = safe_float(stats.get("start_balance"))
    realized_pnl = safe_float(stats.get("total_pnl"))
    unrealized_pnl = exchange.get_unrealized_pnl_total()
    total_pnl = realized_pnl + unrealized_pnl

    if start_balance > 0:
        pnl_percent = (total_pnl / start_balance) * 100.0
    else:
        pnl_percent = 0.0

    return DailyPnLMetrics(
        start_balance=start_balance,
        realized_pnl=realized_pnl,
        unrealized_pnl=unrealized_pnl,
        total_pnl=total_pnl,
        pnl_percent=pnl_percent,
    )


class RiskManager:
    """Enforces portfolio-level constraints before new entries are executed."""

    def __init__(self, exchange: BinanceExchangeManager, db: DatabaseManager) -> None:
        self.exchange = exchange
        self.db = db
        self._peak_daily_pnl = 0.0
        self._reference_balance = 0.0
        balance = self.exchange.get_futures_balance(force_refresh=True)
        if balance > 0:
            self._reference_balance = balance

    # ---------------- Public API ----------------

    def can_open_trade(self, symbol: Optional[str] = None) -> tuple[bool, str]:
        """
        Return (True, '') if a new entry is permitted, else (False, reason).
        Intended to be called immediately before execute_trade().
        """
        snapshot = self.get_risk_snapshot()
        if not snapshot.entries_allowed:
            return False, snapshot.block_reason

        if symbol and self._has_active_trade_for_symbol(symbol):
            return False, f"Active trade already tracked for {symbol}."

        return True, ""

    def is_daily_pnl_limit_reached(self) -> tuple[bool, str]:
        """Return whether daily profit target or max loss has been hit (PnL-based)."""
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        stats = self.db.get_daily_stats(today) or {}
        if stats.get("status") == DAILY_STATUS_PAUSED:
            return True, "Daily limit already reached — entries paused for today."

        metrics = compute_daily_pnl_metrics(self.exchange, self.db, today)
        if metrics.start_balance <= 0:
            return False, ""

        if metrics.pnl_percent >= Config.DAILY_TARGET_PERCENT:
            reason = (
                f"Daily profit target reached (+{metrics.pnl_percent:.2f}%). "
                f"Target={Config.DAILY_TARGET_PERCENT:.2f}%."
            )
            return True, reason

        if metrics.pnl_percent <= -Config.DAILY_STOP_PERCENT:
            reason = (
                f"Daily max loss reached ({metrics.pnl_percent:.2f}%). "
                f"Limit=-{Config.DAILY_STOP_PERCENT:.2f}%."
            )
            return True, reason

        return False, ""

    def get_daily_pnl_metrics(self, date_str: Optional[str] = None) -> DailyPnLMetrics:
        if date_str is None:
            date_str = datetime.now(UTC).strftime("%Y-%m-%d")
        return compute_daily_pnl_metrics(self.exchange, self.db, date_str)

    def get_risk_snapshot(self) -> RiskSnapshot:
        """Compute current risk metrics and whether entries are allowed."""
        open_positions = self.db.get_active_trades_count()
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        daily_stats = self.db.get_daily_stats(today) or {}

        daily_entries = int(daily_stats.get("entries_count", 0) or 0)
        daily_trades = int(daily_stats.get("trades_count", 0))
        consecutive_losses = self._count_consecutive_losses(Config.MAX_CONSECUTIVE_LOSSES)

        pnl_metrics = compute_daily_pnl_metrics(self.exchange, self.db, today)
        if pnl_metrics.total_pnl > self._peak_daily_pnl:
            self._peak_daily_pnl = pnl_metrics.total_pnl

        drawdown = self._calculate_pnl_drawdown_percent(
            current_pnl=pnl_metrics.total_pnl,
            reference_balance=pnl_metrics.start_balance or self._reference_balance,
        )

        current_balance = self.exchange.get_futures_balance(force_refresh=False)
        block_reason = ""

        if daily_stats.get("status") == DAILY_STATUS_PAUSED:
            block_reason = "Daily PnL limit reached — entries paused for today."
        elif open_positions >= Config.MAX_POSITIONS:
            block_reason = (
                f"Max open positions reached ({open_positions}/{Config.MAX_POSITIONS})."
            )
        elif daily_entries >= Config.MAX_DAILY_TRADES:
            block_reason = (
                f"Max daily entries reached ({daily_entries}/{Config.MAX_DAILY_TRADES})."
            )
        elif consecutive_losses >= Config.MAX_CONSECUTIVE_LOSSES:
            block_reason = (
                f"Max consecutive losses reached ({consecutive_losses}/"
                f"{Config.MAX_CONSECUTIVE_LOSSES})."
            )
        elif drawdown >= Config.MAX_ACCOUNT_DRAWDOWN:
            block_reason = (
                f"PnL drawdown limit reached ({drawdown:.2f}% >= "
                f"{Config.MAX_ACCOUNT_DRAWDOWN:.2f}%)."
            )
        elif pnl_metrics.pnl_percent >= Config.DAILY_TARGET_PERCENT:
            block_reason = (
                f"Daily profit target reached (+{pnl_metrics.pnl_percent:.2f}%)."
            )
        elif pnl_metrics.pnl_percent <= -Config.DAILY_STOP_PERCENT:
            block_reason = (
                f"Daily max loss reached ({pnl_metrics.pnl_percent:.2f}%)."
            )

        entries_allowed = block_reason == ""
        if block_reason:
            trade_logger.warning("Entry blocked | %s", block_reason)

        return RiskSnapshot(
            open_positions=open_positions,
            daily_entries=daily_entries,
            daily_trades=daily_trades,
            consecutive_losses=consecutive_losses,
            drawdown_percent=drawdown,
            current_balance=current_balance,
            daily_pnl=pnl_metrics.total_pnl,
            daily_pnl_percent=pnl_metrics.pnl_percent,
            unrealized_pnl=pnl_metrics.unrealized_pnl,
            entries_allowed=entries_allowed,
            block_reason=block_reason,
        )

    def notify_trade_event(self) -> None:
        """Refresh PnL peak tracking after entries, exits, or partial closes."""
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        metrics = compute_daily_pnl_metrics(self.exchange, self.db, today)
        if metrics.total_pnl > self._peak_daily_pnl:
            self._peak_daily_pnl = metrics.total_pnl

        balance = self.exchange.get_futures_balance(force_refresh=True)
        if balance > 0 and self._reference_balance <= 0:
            self._reference_balance = balance

    def record_entry_opened(self) -> None:
        """Track a newly opened entry against the daily entry cap."""
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        self.db.increment_daily_entries(today)

    def log_snapshot(self) -> None:
        """Write a concise risk summary to the performance log."""
        snap = self.get_risk_snapshot()
        performance_logger.info(
            "Risk | open=%s/%s | daily_entries=%s/%s | consec_losses=%s/%s | "
            "daily_pnl=$%.2f (%.2f%%) | unrealized=$%.2f | drawdown=%.2f%%/%.2f%% | allowed=%s",
            snap.open_positions,
            Config.MAX_POSITIONS,
            snap.daily_entries,
            Config.MAX_DAILY_TRADES,
            snap.consecutive_losses,
            Config.MAX_CONSECUTIVE_LOSSES,
            snap.daily_pnl,
            snap.daily_pnl_percent,
            snap.unrealized_pnl,
            snap.drawdown_percent,
            Config.MAX_ACCOUNT_DRAWDOWN,
            snap.entries_allowed,
        )

    # ---------------- Internal helpers ----------------

    def _calculate_pnl_drawdown_percent(
        self,
        current_pnl: float,
        reference_balance: float,
    ) -> float:
        """
        Drawdown from the session's peak total PnL, normalized by reference balance.
        Uses PnL performance rather than raw wallet balance movement.
        """
        if reference_balance <= 0 or self._peak_daily_pnl <= 0:
            return 0.0
        if current_pnl >= self._peak_daily_pnl:
            return 0.0
        pnl_drop = self._peak_daily_pnl - current_pnl
        return (pnl_drop / reference_balance) * 100.0

    def _count_consecutive_losses(self, lookback: int) -> int:
        if lookback <= 0:
            return 0

        try:
            with self.db.connection() as conn:
                rows = conn.execute(
                    """
                    SELECT pnl FROM trades
                    WHERE status = ? AND closed_at IS NOT NULL
                    ORDER BY closed_at DESC
                    LIMIT ?
                    """,
                    (TRADE_STATUS_CLOSED, lookback),
                ).fetchall()
        except Exception:
            return 0

        consecutive = 0
        for row in rows:
            pnl = safe_float(row[0])
            if pnl < 0:
                consecutive += 1
            else:
                break
        return consecutive

    def _has_active_trade_for_symbol(self, symbol: str) -> bool:
        try:
            with self.db.connection() as conn:
                row = conn.execute(
                    """
                    SELECT 1 FROM trades
                    WHERE symbol = ?
                      AND status IN ('OPEN', 'TP1_HIT', 'TP2_HIT')
                    LIMIT 1
                    """,
                    (symbol,),
                ).fetchone()
                return row is not None
        except Exception:
            return False
