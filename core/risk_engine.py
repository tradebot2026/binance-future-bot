"""Absolute risk authority — hard guard before order execution."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from config import Config
from core.strategy_registry import StrategyRegistry
from exceptions import DatabaseError
from logger import trade_logger

if TYPE_CHECKING:
    from database import DatabaseManager
    from exchange import BinanceExchangeManager
    from risk_manager import RiskManager
    from scheduler import DailyScheduler


class RiskEngine:
    """
    Final gate: regardless of strategy score, reject when portfolio limits fail.
    Wraps RiskManager + scheduler gates without replacing existing logic.
    """

    def __init__(
        self,
        risk_manager: "RiskManager",
        db: "DatabaseManager",
        exchange: "BinanceExchangeManager",
        scheduler: Optional["DailyScheduler"] = None,
        registry: Optional[StrategyRegistry] = None,
    ) -> None:
        self.risk = risk_manager
        self.db = db
        self.exchange = exchange
        self.scheduler = scheduler
        self.registry = registry

    def approve_entry(
        self,
        symbol: str,
        strategy: str,
        entry_price: float,
        *,
        score: float = 0.0,
    ) -> tuple[bool, str]:
        """
        Hard reject when any risk rule fails.
        Score is informational only — never bypasses limits.
        """
        if self.scheduler is not None:
            paused, pause_reason = self.scheduler.is_entry_paused()
            if paused:
                return False, pause_reason or (
                    "Scheduler paused entries (daily limit or manual pause)"
                )

        snapshot = self.risk.get_risk_snapshot()
        if not snapshot.entries_allowed:
            return False, snapshot.block_reason

        allowed, reason = self.risk.can_open_trade(
            symbol,
            strategy=strategy,
            entry_price=entry_price,
        )
        if not allowed:
            return False, reason

        try:
            slot_ok, slot_reason = self._check_strategy_slot(strategy)
        except DatabaseError as exc:
            return False, f"Strategy slot check unavailable ({exc})"
        if not slot_ok:
            return False, slot_reason

        if score > 0 and score < Config.STRATEGY_MIN_SCORE:
            trade_logger.debug(
                "RiskEngine note: %s score %.1f below global min (strategy floor may differ)",
                symbol,
                score,
            )

        return True, ""

    def _check_strategy_slot(self, strategy: str) -> tuple[bool, str]:
        """Enforce per-strategy position caps via registry when available."""
        if not self.registry:
            return True, ""
        module = self.registry.get(strategy)
        if module is None:
            return True, ""
        cap = module.max_concurrent_positions()
        if cap <= 0:
            return True, ""
        open_count = self.db.count_active_trades_by_strategy(strategy)
        if open_count >= cap:
            return (
                False,
                f"Max {strategy} positions reached ({open_count}/{cap})",
            )
        return True, ""
