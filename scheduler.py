"""
Daily scheduler module.
Handles UTC day rollover, PnL-based daily profit/loss circuit breakers, and cached checks.
Pause state blocks new entries only — open-position monitoring continues in main.py.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Optional, TYPE_CHECKING

from config import Config
from constants import DAILY_STATUS_ACTIVE, DAILY_STATUS_PAUSED
from database import DatabaseManager
from exchange import BinanceExchangeManager
from logger import performance_logger, system_logger, trade_logger
from risk_manager import compute_daily_pnl_metrics
from utils import safe_float

if TYPE_CHECKING:
    from telegram_bot import TelegramManager


class DailyScheduler:
    """
    Tracks daily PnL (realized + unrealized) against configured targets.
    Uses the exchange balance cache to avoid REST calls every loop iteration.
    """

    def __init__(
        self,
        exchange: BinanceExchangeManager,
        db: DatabaseManager,
        telegram: Optional["TelegramManager"] = None,
    ) -> None:
        self.exchange = exchange
        self.db = db
        self.telegram = telegram
        self.today_str = datetime.now(UTC).strftime("%Y-%m-%d")
        self._last_limit_check_at = 0.0
        self._limit_check_interval = float(Config.BALANCE_CACHE_TTL_SECONDS)

        self._initialize_trading_day(force_balance_refresh=True)

    # ---------------- Public API ----------------

    def is_entry_paused(self) -> tuple[bool, str]:
        """
        Returns whether new scans/entries should be skipped.
        Does NOT affect trade monitoring — main loop must always call manager first.
        """
        return self.check_daily_limits()

    def check_daily_limits(self) -> tuple[bool, str]:
        """Evaluate day rollover and PnL-based daily profit/loss thresholds."""
        self._handle_day_rollover()

        stats = self.db.get_daily_stats(self.today_str)
        if not stats:
            self._initialize_trading_day(force_balance_refresh=False)
            stats = self.db.get_daily_stats(self.today_str)
            if not stats:
                return False, ""

        if stats.get("status") == DAILY_STATUS_PAUSED:
            return True, "Daily limit already reached — entries paused for today."

        now = time.monotonic()
        if now - self._last_limit_check_at < self._limit_check_interval:
            return False, ""

        self._last_limit_check_at = now
        metrics = compute_daily_pnl_metrics(self.exchange, self.db, self.today_str)
        if metrics.start_balance <= 0:
            system_logger.warning(
                "Daily PnL check skipped: invalid start balance for %s.",
                self.today_str,
            )
            return False, ""

        current_balance = self._get_balance(force_refresh=False)
        self.db.update_daily_balance(self.today_str, current_balance, DAILY_STATUS_ACTIVE)

        if metrics.pnl_percent >= Config.DAILY_TARGET_PERCENT:
            reason = (
                f"Daily profit target reached (+{metrics.pnl_percent:.2f}%). "
                f"Target={Config.DAILY_TARGET_PERCENT:.2f}%."
            )
            self._pause_entries(current_balance, reason, metrics)
            return True, reason

        if metrics.pnl_percent <= -Config.DAILY_STOP_PERCENT:
            reason = (
                f"Daily max loss reached ({metrics.pnl_percent:.2f}%). "
                f"Limit=-{Config.DAILY_STOP_PERCENT:.2f}%."
            )
            self._pause_entries(current_balance, reason, metrics)
            return True, reason

        performance_logger.info(
            "Daily PnL $%.2f (%.2f%%) | realized=$%.2f | unrealized=$%.2f | ref=$%.2f",
            metrics.total_pnl,
            metrics.pnl_percent,
            metrics.realized_pnl,
            metrics.unrealized_pnl,
            metrics.start_balance,
        )
        return False, ""

    def notify_trade_event(self) -> None:
        """
        Called after trade entry, exit, or partial close.
        Invalidates balance cache and allows an immediate limit re-check next cycle.
        """
        self.exchange.invalidate_balance_cache()
        self._last_limit_check_at = 0.0

    def get_today_stats(self) -> Optional[dict]:
        """Return today's daily_stats row for Telegram /status commands."""
        stats = self.db.get_daily_stats(self.today_str)
        if not stats:
            return stats

        metrics = compute_daily_pnl_metrics(self.exchange, self.db, self.today_str)
        enriched = dict(stats)
        enriched["computed_total_pnl"] = metrics.total_pnl
        enriched["computed_pnl_percent"] = metrics.pnl_percent
        enriched["unrealized_pnl"] = metrics.unrealized_pnl
        return enriched

    # ---------------- Internal helpers ----------------

    def _handle_day_rollover(self) -> None:
        current_day = datetime.now(UTC).strftime("%Y-%m-%d")
        if current_day == self.today_str:
            return

        system_logger.info("UTC day rollover detected: %s -> %s", self.today_str, current_day)
        self.today_str = current_day
        self._last_limit_check_at = 0.0
        self._initialize_trading_day(force_balance_refresh=True)

        if self.telegram:
            self.telegram.send_message(
                f"🔄 <b>New trading day started</b>: {current_day} (UTC)"
            )

    def _initialize_trading_day(self, force_balance_refresh: bool) -> None:
        balance = self._get_balance(force_refresh=force_balance_refresh)
        if balance <= 0:
            system_logger.warning(
                "Could not initialize daily stats for %s: balance unavailable.",
                self.today_str,
            )
            return

        self.db.initialize_daily_stats(self.today_str, balance)
        self.db.update_daily_balance(self.today_str, balance, DAILY_STATUS_ACTIVE)
        system_logger.info(
            "Trading day %s initialized with reference balance $%.2f.",
            self.today_str,
            balance,
        )

    def _get_balance(self, force_refresh: bool) -> float:
        return self.exchange.get_futures_balance(force_refresh=force_refresh)

    def _pause_entries(
        self,
        current_balance: float,
        reason: str,
        metrics: Optional[object] = None,
    ) -> None:
        self.db.set_daily_status(self.today_str, DAILY_STATUS_PAUSED, current_balance)
        trade_logger.warning("ENTRY PAUSE | %s", reason)

        if self.telegram:
            pnl_line = ""
            if metrics is not None:
                pnl_line = (
                    f"\n📈 <b>Total PnL:</b> ${safe_float(getattr(metrics, 'total_pnl', 0)):.2f} "
                    f"({safe_float(getattr(metrics, 'pnl_percent', 0)):.2f}%)"
                )
            self.telegram.send_message(
                "⏸ <b>Entries paused</b>\n"
                f"{reason}"
                f"{pnl_line}\n"
                f"Balance: ${current_balance:.2f}\n"
                "<i>Open positions continue to be managed.</i>"
            )
