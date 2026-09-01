"""Cross-strategy directional conflict and symbol ownership guard."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from config import Config
from core.types import SignalCandidate
from logger import signal_logger

if TYPE_CHECKING:
    from database import DatabaseManager
    from exchange import BinanceExchangeManager


class SymbolConflictGuard:
    """
    Prevent opposing directions and duplicate cross-strategy entries on one symbol.
    ALLOW_CROSS_STRATEGY_SCALE_IN=False (safest mode) by default.
    """

    def __init__(
        self,
        exchange: "BinanceExchangeManager",
        db: "DatabaseManager",
    ) -> None:
        self.exchange = exchange
        self.db = db
        self._cycle_claims: dict[str, SignalCandidate] = {}

    def reset_cycle(self) -> None:
        """Clear in-memory claims at the start of each scan cycle."""
        self._cycle_claims.clear()

    def approve(self, candidate: SignalCandidate) -> tuple[bool, str]:
        symbol = candidate.symbol.upper()
        action = candidate.action.upper()

        cycle_existing = self._cycle_claims.get(symbol)
        if cycle_existing:
            if cycle_existing.action != action:
                return False, (
                    f"Cycle conflict: {symbol} already claimed "
                    f"{cycle_existing.action} by {cycle_existing.strategy}"
                )
            if cycle_existing.strategy != candidate.strategy:
                if not Config.ALLOW_CROSS_STRATEGY_SCALE_IN:
                    return False, (
                        f"Scale-in disabled: {symbol} already claimed by "
                        f"{cycle_existing.strategy}"
                    )

        for side in ("LONG", "SHORT"):
            if side != action and self.exchange.has_open_position(symbol, side):
                return False, (
                    f"Opposing position open: {symbol} has {side} on exchange"
                )

        if self.exchange.has_open_position(symbol, action):
            if not Config.ALLOW_CROSS_STRATEGY_SCALE_IN:
                return False, f"Position already open: {symbol} {action}"

        db_trades = self.db.get_open_trades_for_symbol(symbol)
        for trade in db_trades:
            trade_side = str(trade.get("side", "")).upper()
            trade_strategy = str(trade.get("strategy", ""))
            if trade_side and trade_side != action:
                return False, (
                    f"DB conflict: {symbol} tracked as {trade_side} "
                    f"({trade_strategy})"
                )
            if (
                trade_side == action
                and trade_strategy
                and trade_strategy != candidate.strategy
                and not Config.ALLOW_CROSS_STRATEGY_SCALE_IN
            ):
                return False, (
                    f"DB scale-in blocked: {symbol} {action} owned by {trade_strategy}"
                )

        on_cooldown, cooldown_reason = self.db.is_symbol_on_cooldown(symbol)
        if on_cooldown:
            return False, f"{symbol} on cooldown ({cooldown_reason})"

        self._cycle_claims[symbol] = candidate
        return True, ""

    def reject_with_log(
        self,
        candidate: SignalCandidate,
        reason: str,
        context: str = "conflict_guard",
    ) -> None:
        signal_logger.info(
            "CONFLICT_REJECTED %s %s | strategy=%s | reason=%s",
            candidate.symbol,
            candidate.action,
            candidate.strategy,
            reason,
        )
        self.db.log_signal_conflict(
            symbol=candidate.symbol,
            strategy=candidate.strategy,
            direction=candidate.action,
            score=candidate.score,
            reason=reason,
            context=context,
        )

    def claim_for_execution(self, candidate: SignalCandidate) -> None:
        """Reserve symbol after passing downstream gates."""
        self._cycle_claims[candidate.symbol.upper()] = candidate
