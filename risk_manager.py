"""
Portfolio risk manager module.
Pre-trade gates for max positions, daily entry count, consecutive losses,
and realized-PnL-based drawdown. Works alongside DailyScheduler (daily pause).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from config import Config
from constants import (
    DAILY_STATUS_ACTIVE,
    DAILY_STATUS_PAUSED,
    STRATEGY_RANGE_REVERSION,
    TRADE_STATUS_CLOSED,
    is_range_strategy,
)
from core.daily_balance_manager import fetch_daily_wallet_baseline
from database import DatabaseManager
from exceptions import DatabaseError
from exchange import BinanceExchangeManager
from logger import performance_logger, system_logger, trade_logger
from utils import minimum_order_quantity, safe_float, utc_now, utc_today_str

if TYPE_CHECKING:
    from bot_controller import BotController


@dataclass
class DailyPnLMetrics:
    """Daily performance metrics — circuit breakers use wallet equity vs daily start."""

    start_balance: float
    current_wallet: float
    equity_day_pnl: float
    equity_day_pnl_percent: float
    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float
    realized_pnl_percent: float
    total_pnl_percent: float


def resolve_current_wallet_balance(
    exchange: BinanceExchangeManager,
    *,
    force: bool = False,
) -> float:
    """Live USDT wallet balance for equity-based daily PnL."""
    return fetch_daily_wallet_baseline(exchange, force=force)


def daily_profit_target_reached(metrics: DailyPnLMetrics) -> bool:
    return metrics.equity_day_pnl_percent >= Config.DAILY_TARGET_PERCENT


def daily_max_loss_reached(metrics: DailyPnLMetrics) -> bool:
    """Never trip max loss while wallet equity is above the day's starting balance."""
    if metrics.start_balance <= 0:
        return False
    if metrics.current_wallet > metrics.start_balance:
        return False
    return metrics.equity_day_pnl_percent <= -Config.DAILY_STOP_PERCENT


@dataclass
class RiskSnapshot:
    """Point-in-time portfolio risk metrics."""

    open_positions: int
    exchange_open_positions: int
    daily_entries: int
    daily_trades: int
    consecutive_losses: int
    drawdown_percent: float
    current_balance: float
    daily_realized_pnl: float
    daily_realized_pnl_percent: float
    unrealized_pnl: float
    entries_allowed: bool
    block_reason: str


def compute_daily_pnl_metrics(
    exchange: BinanceExchangeManager,
    db: DatabaseManager,
    date_str: str,
    *,
    force_wallet_refresh: bool = False,
) -> DailyPnLMetrics:
    """
    Compute today's PnL metrics.
    Day PnL = current wallet balance - daily starting balance (equity difference).
    """
    stats = db.get_daily_stats(date_str) or {}
    start_balance = safe_float(stats.get("start_balance"))
    current_wallet = resolve_current_wallet_balance(
        exchange, force=force_wallet_refresh
    )

    equity_day_pnl = 0.0
    equity_day_pnl_percent = 0.0
    if start_balance > 0 and current_wallet > 0:
        equity_day_pnl = current_wallet - start_balance
        equity_day_pnl_percent = (equity_day_pnl / start_balance) * 100.0

    if (
        start_balance > 0
        and current_wallet > start_balance
        and stats.get("status") == DAILY_STATUS_PAUSED
    ):
        db.set_daily_status(date_str, DAILY_STATUS_ACTIVE, current_wallet)
        system_logger.info(
            "Daily status auto-cleared to ACTIVE — wallet $%.2f above start $%.2f.",
            current_wallet,
            start_balance,
        )

    trade_analytics = db.get_daily_trade_analytics(date_str)
    db_realized_pnl = safe_float(trade_analytics.get("total_pnl"))
    if db_realized_pnl == 0:
        db_realized_pnl = safe_float(stats.get("total_pnl"))

    unrealized_pnl = exchange.get_unrealized_pnl_total()
    if unrealized_pnl == 0 and exchange.get_open_positions_count() > 0:
        unrealized_pnl = exchange.get_unrealized_pnl_total(force_refresh=True)

    db_total_pnl = db_realized_pnl + unrealized_pnl
    db_realized_percent = (
        (db_realized_pnl / start_balance) * 100.0 if start_balance > 0 else 0.0
    )

    return DailyPnLMetrics(
        start_balance=start_balance,
        current_wallet=current_wallet,
        equity_day_pnl=equity_day_pnl,
        equity_day_pnl_percent=equity_day_pnl_percent,
        realized_pnl=db_realized_pnl,
        unrealized_pnl=unrealized_pnl,
        total_pnl=equity_day_pnl,
        realized_pnl_percent=db_realized_percent,
        total_pnl_percent=equity_day_pnl_percent,
    )


class RiskManager:
    """Enforces portfolio-level constraints before new entries are executed."""

    def __init__(
        self,
        exchange: BinanceExchangeManager,
        db: DatabaseManager,
        controller: Optional["BotController"] = None,
    ) -> None:
        self.exchange = exchange
        self.db = db
        self.controller = controller
        self._peak_realized_pnl = 0.0
        self._reference_balance = 0.0
        # Only closed trades after this timestamp count toward consecutive-loss blocks.
        self._consecutive_loss_reset_at = utc_now().isoformat()
        balance = self.exchange.get_futures_balance(force_refresh=False)
        if balance > 0:
            self._reference_balance = balance
        system_logger.info(
            "Risk manager initialized — consecutive loss streak reset (0/%s).",
            Config.MAX_CONSECUTIVE_LOSSES,
        )

    def reset_consecutive_loss_block(self) -> None:
        """
        Clear the consecutive-loss entry gate.
        Historical losses before this point no longer block new entries.
        """
        self._consecutive_loss_reset_at = utc_now().isoformat()
        trade_logger.info(
            "Consecutive loss block cleared — counter reset to 0/%s.",
            Config.MAX_CONSECUTIVE_LOSSES,
        )

    # ---------------- Public API ----------------

    def get_exchange_open_positions_count(self) -> int:
        """Return live open position count from Binance (source of truth)."""
        return self.exchange.get_open_positions_count()

    def can_open_trade(
        self,
        symbol: Optional[str] = None,
        strategy: Optional[str] = None,
        entry_price: Optional[float] = None,
    ) -> tuple[bool, str]:
        """
        Return (True, '') if a new entry is permitted, else (False, reason).
        Intended to be called immediately before execute_trade().
        """
        snapshot = self.get_risk_snapshot()
        if not snapshot.entries_allowed:
            return False, snapshot.block_reason

        if symbol and entry_price and entry_price > 0:
            floor_ok, floor_reason = self.validate_minimum_order_floor(
                symbol, entry_price
            )
            if not floor_ok:
                return False, floor_reason

        if strategy and is_range_strategy(strategy):
            paused, pause_reason = self.db.is_strategy_entries_paused(
                strategy, health_check=True
            )
            if paused:
                return False, pause_reason

            try:
                range_open = self.db.count_active_trades_by_strategy(
                    STRATEGY_RANGE_REVERSION
                )
            except DatabaseError as exc:
                return False, f"RANGE slot check unavailable ({exc})"
            if range_open >= Config.MAX_RANGE_POSITIONS:
                return False, (
                    f"Max RANGE positions reached ({range_open}/{Config.MAX_RANGE_POSITIONS})"
                )

        if strategy:
            from constants import is_smc_strategy

            if is_smc_strategy(strategy):
                paused, pause_reason = self.db.is_strategy_entries_paused(
                    strategy, health_check=True
                )
                if paused:
                    return False, pause_reason
                try:
                    smc_open = self.db.count_active_trades_by_strategy(strategy)
                except DatabaseError as exc:
                    return False, f"SMC slot check unavailable ({exc})"
                if smc_open >= Config.MAX_SMC_POSITIONS:
                    return False, (
                        f"Max SMC positions reached ({smc_open}/{Config.MAX_SMC_POSITIONS})"
                    )

        if symbol:
            if self._has_active_trade_for_symbol(symbol):
                return False, f"Active trade already tracked for {symbol}."
            on_cooldown, cooldown_reason = self.db.is_symbol_on_cooldown(symbol)
            if on_cooldown:
                return False, f"{symbol} on cooldown ({cooldown_reason})."
            for side in ("LONG", "SHORT"):
                if self.exchange.has_open_position(symbol, side):
                    return False, (
                        f"Exchange position already open for {symbol} {side}."
                    )

        return True, ""

    def validate_minimum_order_floor(
        self, symbol: str, entry_price: float
    ) -> tuple[bool, str]:
        """
        Reject early when the max position cap cannot fit a valid exchange order.
        Prevents approved signals from failing with zero-quantity at execution.
        """
        balance = self.exchange.get_futures_balance(force_refresh=False)
        if balance <= 0:
            balance = self.exchange.get_futures_balance(force_refresh=True)
        if balance <= 0:
            return False, "Balance unavailable for minimum order check."

        max_notional = balance * Config.MAX_POSITION_VALUE_MULTIPLIER
        try:
            rules = self.exchange.get_symbol_rules(symbol)
        except Exception as exc:
            trade_logger.debug(
                "Skipping min-order floor check for %s: %s", symbol, exc
            )
            return True, ""

        if max_notional < rules.min_notional:
            return (
                False,
                f"Position cap ${max_notional:.2f} below exchange min notional "
                f"${rules.min_notional:.2f}.",
            )

        min_valid_qty = minimum_order_quantity(
            entry_price,
            rules.min_qty,
            rules.min_notional,
            rules.step_size,
            rules.quantity_precision,
        )
        min_valid_notional = min_valid_qty * entry_price
        if min_valid_notional > max_notional:
            return (
                False,
                f"Minimum order ${min_valid_notional:.2f} exceeds position cap "
                f"${max_notional:.2f}.",
            )

        return True, ""

    def _daily_limit_override_active(self) -> bool:
        return bool(
            self.controller and self.controller.is_daily_limit_overridden()
        )

    def is_daily_pnl_limit_reached(self) -> tuple[bool, str]:
        """Return whether daily profit target or max loss has been hit (realized PnL)."""
        if self._daily_limit_override_active():
            return False, ""

        today = utc_today_str()
        metrics = compute_daily_pnl_metrics(self.exchange, self.db, today)
        stats = self.db.get_daily_stats(today) or {}
        if stats.get("status") == DAILY_STATUS_PAUSED:
            still_in_breach = daily_profit_target_reached(
                metrics
            ) or daily_max_loss_reached(metrics)
            if still_in_breach:
                return True, "Daily limit already reached — entries paused for today."
            balance = metrics.current_wallet if metrics.current_wallet > 0 else 0.0
            self.db.set_daily_status(today, DAILY_STATUS_ACTIVE, balance)
            system_logger.info(
                "Daily PAUSED status cleared for %s — limits not in breach.",
                today,
            )
        if metrics.start_balance <= 0:
            return False, ""

        if daily_profit_target_reached(metrics):
            reason = (
                f"Daily profit target reached (+{metrics.equity_day_pnl_percent:.2f}% equity). "
                f"Target={Config.DAILY_TARGET_PERCENT:.2f}%."
            )
            return True, reason

        if daily_max_loss_reached(metrics):
            reason = (
                f"Daily max loss reached ({metrics.equity_day_pnl_percent:.2f}% equity). "
                f"Limit=-{Config.DAILY_STOP_PERCENT:.2f}%."
            )
            return True, reason

        return False, ""

    def get_daily_pnl_metrics(self, date_str: Optional[str] = None) -> DailyPnLMetrics:
        if date_str is None:
            date_str = utc_today_str()
        return compute_daily_pnl_metrics(self.exchange, self.db, date_str)

    def get_risk_snapshot(self) -> RiskSnapshot:
        """Compute current risk metrics and whether entries are allowed."""
        exchange_open = self.get_exchange_open_positions_count()
        try:
            db_open = self.db.get_active_trades_count()
        except DatabaseError as exc:
            trade_logger.error("Entry blocked | database unavailable: %s", exc)
            return RiskSnapshot(
                open_positions=max(exchange_open, Config.MAX_POSITIONS),
                exchange_open_positions=exchange_open,
                daily_entries=0,
                daily_trades=0,
                consecutive_losses=Config.MAX_CONSECUTIVE_LOSSES,
                drawdown_percent=0.0,
                current_balance=0.0,
                daily_realized_pnl=0.0,
                daily_realized_pnl_percent=0.0,
                unrealized_pnl=0.0,
                entries_allowed=False,
                block_reason=f"Database unavailable — entries blocked ({exc})",
            )
        open_positions = max(exchange_open, db_open)

        today = utc_today_str()
        pnl_metrics = compute_daily_pnl_metrics(self.exchange, self.db, today)
        daily_stats = self.db.get_daily_stats(today) or {}

        daily_entries = int(daily_stats.get("entries_count", 0) or 0)
        trade_analytics = self.db.get_daily_trade_analytics(today)
        daily_trades = int(
            trade_analytics.get("closes", daily_stats.get("trades_count", 0))
        )
        consecutive_losses = self._count_consecutive_losses(Config.MAX_CONSECUTIVE_LOSSES)
        if pnl_metrics.realized_pnl > self._peak_realized_pnl:
            self._peak_realized_pnl = pnl_metrics.realized_pnl

        drawdown = self._calculate_realized_drawdown_percent(
            current_realized=pnl_metrics.realized_pnl,
            reference_balance=pnl_metrics.start_balance or self._reference_balance,
        )

        current_balance = self.exchange.get_futures_balance(force_refresh=False)
        if current_balance <= 0:
            current_balance = self.exchange.get_futures_balance(force_refresh=True)
        block_reason = ""
        daily_override = self._daily_limit_override_active()

        if not daily_override and daily_stats.get("status") == DAILY_STATUS_PAUSED:
            still_in_breach = daily_profit_target_reached(
                pnl_metrics
            ) or daily_max_loss_reached(pnl_metrics)
            if still_in_breach:
                block_reason = "Daily PnL limit reached — entries paused for today."
            else:
                self.db.set_daily_status(
                    today,
                    DAILY_STATUS_ACTIVE,
                    pnl_metrics.current_wallet if pnl_metrics.current_wallet > 0 else current_balance,
                )

        if block_reason:
            pass
        elif exchange_open >= Config.MAX_POSITIONS:
            block_reason = (
                f"Max open positions reached on exchange "
                f"({exchange_open}/{Config.MAX_POSITIONS})."
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
                f"Realized PnL drawdown limit reached ({drawdown:.2f}% >= "
                f"{Config.MAX_ACCOUNT_DRAWDOWN:.2f}%)."
            )
        elif not daily_override and daily_profit_target_reached(pnl_metrics):
            block_reason = (
                f"Daily profit target reached (+{pnl_metrics.equity_day_pnl_percent:.2f}%)."
            )
        elif not daily_override and daily_max_loss_reached(pnl_metrics):
            block_reason = (
                f"Daily max loss reached ({pnl_metrics.equity_day_pnl_percent:.2f}%)."
            )

        entries_allowed = block_reason == ""
        if block_reason:
            trade_logger.warning("Entry blocked | %s", block_reason)

        return RiskSnapshot(
            open_positions=open_positions,
            exchange_open_positions=exchange_open,
            daily_entries=daily_entries,
            daily_trades=daily_trades,
            consecutive_losses=consecutive_losses,
            drawdown_percent=drawdown,
            current_balance=current_balance,
            daily_realized_pnl=pnl_metrics.equity_day_pnl,
            daily_realized_pnl_percent=pnl_metrics.equity_day_pnl_percent,
            unrealized_pnl=pnl_metrics.unrealized_pnl,
            entries_allowed=entries_allowed,
            block_reason=block_reason,
        )

    def notify_trade_event(self) -> None:
        """Refresh wallet + daily metrics immediately after entries, exits, or partial closes."""
        if hasattr(self.exchange, "refresh_wallet_after_trade"):
            self.exchange.refresh_wallet_after_trade()
        else:
            self.exchange.invalidate_balance_cache()
            self.exchange.get_futures_balance(force_refresh=True)

        today = utc_today_str()
        metrics = compute_daily_pnl_metrics(self.exchange, self.db, today)
        if metrics.realized_pnl > self._peak_realized_pnl:
            self._peak_realized_pnl = metrics.realized_pnl

        balance = metrics.current_wallet or self.exchange.get_futures_balance(
            force_refresh=False
        )
        if balance > 0 and self._reference_balance <= 0:
            self._reference_balance = balance

    def record_entry_opened(self) -> None:
        """Track a newly opened entry against the daily entry cap."""
        self.db.increment_daily_entries(utc_today_str())

    def log_snapshot(self) -> None:
        """Write a concise risk summary to the performance log."""
        snap = self.get_risk_snapshot()
        performance_logger.info(
            "Risk | exchange_open=%s/%s | daily_entries=%s/%s | consec_losses=%s/%s | "
            "realized_pnl=$%.2f (%.2f%%) | unrealized=$%.2f | drawdown=%.2f%%/%.2f%% | allowed=%s",
            snap.exchange_open_positions,
            Config.MAX_POSITIONS,
            snap.daily_entries,
            Config.MAX_DAILY_TRADES,
            snap.consecutive_losses,
            Config.MAX_CONSECUTIVE_LOSSES,
            snap.daily_realized_pnl,
            snap.daily_realized_pnl_percent,
            snap.unrealized_pnl,
            snap.drawdown_percent,
            Config.MAX_ACCOUNT_DRAWDOWN,
            snap.entries_allowed,
        )

    # ---------------- Internal helpers ----------------

    def _calculate_realized_drawdown_percent(
        self,
        current_realized: float,
        reference_balance: float,
    ) -> float:
        """Drawdown from session peak realized PnL (closed trades only)."""
        if reference_balance <= 0 or self._peak_realized_pnl <= 0:
            return 0.0
        if current_realized >= self._peak_realized_pnl:
            return 0.0
        pnl_drop = self._peak_realized_pnl - current_realized
        return (pnl_drop / reference_balance) * 100.0

    def _count_consecutive_losses(self, lookback: int) -> int:
        if lookback <= 0:
            return 0

        try:
            with self.db.connection() as conn:
                rows = conn.execute(
                    """
                    SELECT pnl FROM trades
                    WHERE status = ?
                      AND closed_at IS NOT NULL
                      AND closed_at >= ?
                    ORDER BY closed_at DESC
                    LIMIT ?
                    """,
                    (
                        TRADE_STATUS_CLOSED,
                        self._consecutive_loss_reset_at,
                        lookback,
                    ),
                ).fetchall()
        except Exception:
            return lookback

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
            return True
