"""SMC trend strategy module."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Set

from config import Config
from constants import STRATEGY_SMC_TREND
from core.directional import pick_directional_candidate
from core.strategy_base import StrategyModule
from core.strategy_registry import strategy_enable_flag
from core.types import MarketSnapshot, RegimeLabel, SignalCandidate
from indicators.market_analyzer import MarketAnalyzer
from logger import signal_logger
from smc_engine import (
    effective_smc_min_score,
    evaluate_confluence_gate,
    score_setup,
    validate_retest_entry,
)
from utils import safe_float

if TYPE_CHECKING:
    from database import DatabaseManager


class SMCStrategy(StrategyModule):
    tag = STRATEGY_SMC_TREND
    display_name = "SMC Trend"

    def __init__(self, db: Optional["DatabaseManager"] = None) -> None:
        self.db = db
        self.analyzer = MarketAnalyzer()
        self.entry_tf = Config.ENTRY_TIMEFRAME
        self.confirm_tf = Config.CONFIRM_TIMEFRAME
        self.trend_tf = Config.TREND_TIMEFRAME

    def is_enabled(self) -> bool:
        return strategy_enable_flag(self.tag)

    def default_timeframes(self) -> tuple[str, ...]:
        return (self.entry_tf, self.confirm_tf, self.trend_tf)

    def allowed_regimes(self) -> Set[str]:
        return {
            RegimeLabel.STRONG_TREND.value,
            RegimeLabel.UNCLEAR.value,
            RegimeLabel.COMPRESSION.value,
        }

    def regime_fit(self, snapshot: MarketSnapshot) -> float:
        if snapshot.regime == RegimeLabel.STRONG_TREND:
            return 1.0
        if snapshot.regime == RegimeLabel.UNCLEAR:
            return 0.85
        if snapshot.regime == RegimeLabel.COMPRESSION:
            return 0.6
        if snapshot.regime == RegimeLabel.RANGE_CHOP:
            return 0.3
        return 0.2

    def max_concurrent_positions(self) -> int:
        return Config.MAX_SMC_POSITIONS

    def priority_weight(self) -> float:
        return Config.STRATEGY_PRIORITY_SMC

    def size_multiplier(self, score: float) -> float:
        if score >= Config.SCORE_FULL_SIZE:
            return 1.0
        if score >= Config.STRATEGY_MIN_SCORE:
            return Config.HALF_SIZE_MULTIPLIER
        return 0.0

    def evaluate(self, snapshot: MarketSnapshot) -> Optional[SignalCandidate]:
        df_entry = snapshot.candles.get(self.entry_tf)
        df_confirm = snapshot.candles.get(self.confirm_tf)
        df_trend = snapshot.candles.get(self.trend_tf)
        if df_entry is None or df_confirm is None or df_trend is None:
            return None
        if df_entry.empty or df_confirm.empty or df_trend.empty:
            return None

        df_entry = self.analyzer.apply_all_indicators(df_entry)
        df_confirm = self.analyzer.apply_all_indicators(df_confirm)
        df_trend = self.analyzer.apply_all_indicators(df_trend)
        if df_entry.empty or df_confirm.empty or df_trend.empty:
            return None

        price = snapshot.price if snapshot.price > 0 else float(df_entry.iloc[-1]["close"])
        long_c = self._evaluate_direction(
            snapshot.symbol, df_entry, df_confirm, df_trend, "LONG", price, snapshot
        )
        short_c = self._evaluate_direction(
            snapshot.symbol, df_entry, df_confirm, df_trend, "SHORT", price, snapshot
        )
        return pick_directional_candidate(
            snapshot.symbol,
            long_c,
            short_c,
            win_margin=Config.DIRECTION_WIN_MARGIN,
            resolve_margin=Config.DIRECTION_EQUILIBRIUM_MIN_MARGIN,
            min_score_fn=effective_smc_min_score,
            enable_equilibrium_resolve=Config.ENABLE_DIRECTION_EQUILIBRIUM_RESOLVE,
        )

    def _evaluate_direction(
        self,
        symbol: str,
        df_entry,
        df_confirm,
        df_trend,
        action: str,
        price: float,
        snapshot: MarketSnapshot,
    ) -> Optional[SignalCandidate]:
        latest, previous = self.analyzer.extract_latest_signals(df_entry)
        atr = self.analyzer.get_latest_atr(df_entry)
        if atr <= 0 or price <= 0:
            return None

        gate = evaluate_confluence_gate(action, df_entry, df_trend, df_confirm, price, atr)
        if not gate.passed:
            if self.db:
                self.db.log_signal_rejection(symbol, action, 0.0, gate.reasons)
            return None

        retest_ok, retest_reason = validate_retest_entry(action, price, gate.structure, atr)
        if not retest_ok:
            if self.db:
                self.db.log_signal_rejection(symbol, action, 0.0, [retest_reason])
            return None

        score = score_setup(action, latest, previous, gate.structure, gate.structure.macro_trend)
        min_score = effective_smc_min_score(
            gate.structure.confluence_type, gate.structure.macro_trend
        )
        if score < min_score:
            if self.db:
                reason = f"score_below_min_{score:.1f}_required_{min_score:.1f}"
                self.db.log_signal_rejection(symbol, action, score, [reason])
            return None

        signal_logger.info(
            "APPROVED %s %s | strategy=%s | score=%.1f | macro=%s | confluence=%s",
            symbol,
            action,
            self.tag,
            score,
            gate.structure.macro_trend,
            gate.structure.confluence_type,
        )
        fit = self.regime_fit(snapshot)
        meta = gate.structure.to_dict()
        meta["volume_24h"] = snapshot.volume_24h
        return SignalCandidate(
            symbol=symbol,
            action=action,  # type: ignore[arg-type]
            strategy=self.tag,
            score=score,
            price=price,
            atr=atr,
            timeframe=self.entry_tf,
            regime=snapshot.regime.value,
            confluence=gate.structure.confluence_type,
            macro_trend=gate.structure.macro_trend,
            structure_metadata=meta,
            regime_fit=fit,
            priority_weight=self.priority_weight(),
        )
