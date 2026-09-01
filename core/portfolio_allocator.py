"""Dynamic margin and per-strategy budget allocation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from config import Config
from core.types import AllocationResult, SignalCandidate
from utils import safe_float

if TYPE_CHECKING:
    from database import DatabaseManager
    from exchange import BinanceExchangeManager


class PortfolioAllocator:
    """Approve risk budget per candidate without breaching margin limits."""

    STRATEGY_BUDGET_KEYS = {
        "SMC_TREND": "STRATEGY_BUDGET_SMC",
        "SMC_MULTITF": "STRATEGY_BUDGET_SMC",
        "RANGE_REVERSION": "STRATEGY_BUDGET_RANGE",
        "LIQUIDITY_SWEEP_CONT": "STRATEGY_BUDGET_LSC",
        "VWAP_PULLBACK": "STRATEGY_BUDGET_VWAP",
        "VOLUME_PROFILE_BREAKOUT": "STRATEGY_BUDGET_VPB",
        "VOL_EXPANSION_MR": "STRATEGY_BUDGET_VEMR",
    }

    STRATEGY_SLOT_KEYS = {
        "SMC_TREND": "MAX_SMC_POSITIONS",
        "SMC_MULTITF": "MAX_SMC_POSITIONS",
        "RANGE_REVERSION": "MAX_RANGE_POSITIONS",
        "LIQUIDITY_SWEEP_CONT": "MAX_LSC_POSITIONS",
        "VWAP_PULLBACK": "MAX_VWAP_POSITIONS",
        "VOLUME_PROFILE_BREAKOUT": "MAX_VPB_POSITIONS",
        "VOL_EXPANSION_MR": "MAX_VEMR_POSITIONS",
    }

    def __init__(
        self,
        exchange: "BinanceExchangeManager",
        db: "DatabaseManager",
    ) -> None:
        self.exchange = exchange
        self.db = db

    def approve(self, candidate: SignalCandidate) -> AllocationResult:
        balance = self.exchange.get_futures_balance(force_refresh=False)
        if balance <= 0:
            return AllocationResult(False, reason="balance_unavailable")

        if Config.ENABLE_GLOBAL_STRATEGY_KILL_SWITCH:
            paused, reason = self.db.is_global_entries_paused()
            if paused:
                return AllocationResult(False, reason=reason)

        paused, reason = self.db.is_strategy_entries_paused(
            candidate.strategy, health_check=True
        )
        if paused:
            return AllocationResult(False, reason=reason)

        open_for_strategy = self.db.count_active_trades_by_strategy(candidate.strategy)
        max_slots = self._max_slots(candidate.strategy)
        if open_for_strategy >= max_slots:
            return AllocationResult(
                False,
                reason=f"Strategy slot limit ({open_for_strategy}/{max_slots})",
            )

        margin_used = self._estimate_margin_used()
        margin_util = margin_used / balance if balance > 0 else 1.0
        if margin_util >= Config.MAX_ACCOUNT_MARGIN_UTILIZATION:
            return AllocationResult(
                False,
                reason=(
                    f"Margin utilization {margin_util:.1%} >= "
                    f"{Config.MAX_ACCOUNT_MARGIN_UTILIZATION:.1%}"
                ),
            )

        strategy_open_risk = self._strategy_open_risk(candidate.strategy, balance)
        strategy_budget_pct = self._strategy_budget_pct(candidate.strategy)
        strategy_budget = balance * strategy_budget_pct
        if strategy_open_risk >= strategy_budget:
            return AllocationResult(
                False,
                reason=f"Strategy budget exhausted ({candidate.strategy})",
            )

        base_risk = balance * (Config.RISK_PER_TRADE_PERCENT / 100.0)
        size_mult = self._strategy_size_multiplier(candidate)
        trade_risk = base_risk * size_mult

        global_headroom = balance * Config.MAX_GROSS_EXPOSURE_PCT
        portfolio_risk = self._portfolio_open_risk(balance)
        available_headroom = max(global_headroom - portfolio_risk, 0.0)
        strategy_headroom = max(strategy_budget - strategy_open_risk, 0.0)

        risk_budget = min(trade_risk, strategy_headroom, available_headroom)
        if risk_budget <= 0:
            return AllocationResult(False, reason="no_risk_headroom")

        net_ok, net_reason = self._check_net_exposure(candidate, balance)
        if not net_ok:
            return AllocationResult(False, reason=net_reason)

        return AllocationResult(
            approved=True,
            risk_budget_usdt=risk_budget,
            size_multiplier=size_mult,
        )

    def _strategy_budget_pct(self, strategy: str) -> float:
        key = self.STRATEGY_BUDGET_KEYS.get(strategy.upper(), "")
        if key and hasattr(Config, key):
            return max(0.0, safe_float(getattr(Config, key)))
        return Config.RISK_PER_TRADE_PERCENT / 100.0

    def _max_slots(self, strategy: str) -> int:
        key = self.STRATEGY_SLOT_KEYS.get(strategy.upper(), "")
        if key and hasattr(Config, key):
            return max(1, int(getattr(Config, key)))
        if strategy.upper() == "RANGE_REVERSION":
            return Config.MAX_RANGE_POSITIONS
        return Config.MAX_POSITIONS

    def _strategy_size_multiplier(self, candidate: SignalCandidate) -> float:
        from constants import STRATEGY_RANGE_REVERSION, STRATEGY_SMC_TREND

        if candidate.strategy == STRATEGY_RANGE_REVERSION:
            return (
                Config.RANGE_SIZE_MULTIPLIER
                if candidate.score >= Config.RANGE_MIN_SCORE
                else 0.0
            )
        if candidate.strategy in (STRATEGY_SMC_TREND, "SMC_MULTITF"):
            if candidate.score >= Config.SCORE_FULL_SIZE:
                return 1.0
            if candidate.score >= Config.STRATEGY_MIN_SCORE:
                return Config.HALF_SIZE_MULTIPLIER
        return 1.0

    def _estimate_margin_used(self) -> float:
        total = 0.0
        for pos in self.exchange.get_all_open_positions(force_refresh=False):
            qty = safe_float(pos.get("quantity"))
            entry = safe_float(pos.get("entryPrice"))
            lev = safe_float(pos.get("leverage"), 1.0) or 1.0
            notional = qty * entry
            total += notional / lev
        return total

    def _strategy_open_risk(self, strategy: str, balance: float) -> float:
        trades = self.db.get_open_trades_by_strategy(strategy)
        risk = 0.0
        for trade in trades:
            entry = safe_float(trade.get("entry_price"))
            sl = safe_float(trade.get("stop_loss"))
            qty = safe_float(trade.get("quantity"))
            risk += abs(entry - sl) * qty
        return min(risk, balance)

    def _portfolio_open_risk(self, balance: float) -> float:
        trades = self.db.get_open_trades()
        risk = 0.0
        for trade in trades:
            entry = safe_float(trade.get("entry_price"))
            sl = safe_float(trade.get("stop_loss"))
            qty = safe_float(trade.get("quantity"))
            risk += abs(entry - sl) * qty
        return min(risk, balance)

    def _check_net_exposure(
        self, candidate: SignalCandidate, balance: float
    ) -> tuple[bool, str]:
        long_notional = 0.0
        short_notional = 0.0
        for pos in self.exchange.get_all_open_positions(force_refresh=False):
            qty = safe_float(pos.get("quantity"))
            entry = safe_float(pos.get("entryPrice"))
            notional = qty * entry
            side = str(pos.get("positionSide", "")).upper()
            if side == "LONG":
                long_notional += notional
            elif side == "SHORT":
                short_notional += notional

        est_notional = candidate.price * (balance * 0.01)
        if candidate.action == "LONG":
            long_notional += est_notional
        else:
            short_notional += est_notional

        max_long = balance * Config.MAX_NET_LONG_EXPOSURE_PCT
        max_short = balance * Config.MAX_NET_SHORT_EXPOSURE_PCT
        gross = long_notional + short_notional
        max_gross = balance * Config.MAX_GROSS_EXPOSURE_PCT

        if gross > max_gross:
            return False, f"Gross exposure limit ({gross:.0f}>{max_gross:.0f})"
        if long_notional > max_long:
            return False, f"Net long exposure limit ({long_notional:.0f}>{max_long:.0f})"
        if short_notional > max_short:
            return False, f"Net short exposure limit ({short_notional:.0f}>{max_short:.0f})"
        return True, ""
