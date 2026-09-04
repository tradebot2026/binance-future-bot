"""Multi-strategy scoring engine — evaluates symbols against all strategies."""

from __future__ import annotations

from typing import Optional

from config import Config
from constants import (
    STRATEGY_LIQUIDITY_SWEEP,
    STRATEGY_RANGE_REVERSION,
    STRATEGY_SMC_TREND,
    STRATEGY_VOL_EXPANSION,
    STRATEGY_VP_BREAKOUT,
    STRATEGY_VWAP_PULLBACK,
)
from core.candidate_arbitrator import CandidateArbitrator
from core.strategy_registry import StrategyRegistry
from core.types import MarketSnapshot, SignalCandidate, StrategyScore
from logger import error_logger
from smc_engine import effective_smc_min_score


class ScoringEngine:
    """Score one symbol against all enabled strategies (pure WS, no REST)."""

    def __init__(self, registry: StrategyRegistry) -> None:
        self.registry = registry

    @staticmethod
    def compute_normalized_score(raw_score: float, min_score: float) -> float:
        """
        Strategy-relative performance: percent of usable score range above min.
        normalized = (raw - min) / (100 - min) * 100
        """
        if raw_score <= min_score:
            return 0.0
        span = 100.0 - min_score
        if span <= 0:
            return 0.0
        return (raw_score - min_score) / span * 100.0

    @staticmethod
    def strategy_min_score(
        strategy: str,
        signal: Optional[SignalCandidate] = None,
    ) -> float:
        """Per-strategy execution floor used for normalization."""
        if strategy == STRATEGY_RANGE_REVERSION:
            return Config.RANGE_MIN_SCORE
        if strategy == STRATEGY_SMC_TREND:
            if signal is not None:
                return effective_smc_min_score(
                    signal.confluence, signal.macro_trend or "NEUTRAL"
                )
            return Config.STRATEGY_MIN_SCORE
        if strategy == STRATEGY_LIQUIDITY_SWEEP:
            return Config.LSC_MIN_SCORE
        if strategy == STRATEGY_VWAP_PULLBACK:
            return Config.VWAP_MIN_SCORE
        if strategy == STRATEGY_VP_BREAKOUT:
            return Config.VPB_MIN_SCORE
        if strategy == STRATEGY_VOL_EXPANSION:
            return Config.VEMR_MIN_SCORE
        return Config.STRATEGY_MIN_SCORE

    @staticmethod
    def scaled_adjusted_threshold(
        min_score: float,
        promote_normalized: float,
        regime_fit: float,
        priority_weight: float,
    ) -> float:
        """Option B: map normalized promote bar onto adjusted_score space."""
        required_raw = min_score + (100.0 - min_score) * (promote_normalized / 100.0)
        return required_raw * regime_fit * priority_weight

    def evaluate_symbol(
        self,
        snapshot: MarketSnapshot,
        *,
        bar_open_ms: int = 0,
        timeframe: str = "",
    ) -> list[StrategyScore]:
        scores: list[StrategyScore] = []
        for strategy in self.registry.enabled():
            if strategy.requires_top_volume() and not snapshot.is_top_volume:
                continue

            fit = strategy.regime_fit(snapshot)
            if fit <= 0:
                continue

            allowed = strategy.allowed_regimes()
            if allowed and snapshot.regime.value not in allowed:
                continue

            try:
                signal = strategy.evaluate(snapshot)
            except Exception as exc:
                error_logger.error(
                    "Strategy %s scoring failed on %s: %s",
                    strategy.tag,
                    snapshot.symbol,
                    exc,
                )
                continue

            raw_score = float(signal.score) if signal else 0.0
            action = signal.action if signal else "NEUTRAL"
            min_score = self.strategy_min_score(strategy.tag, signal)
            if signal:
                signal = CandidateArbitrator.apply_regime_fit(signal, fit)
                adjusted = float(signal.adjusted_score)
            else:
                adjusted = 0.0

            normalized = self.compute_normalized_score(raw_score, min_score)

            scores.append(
                StrategyScore(
                    symbol=snapshot.symbol,
                    strategy=strategy.tag,
                    score=raw_score,
                    adjusted_score=adjusted,
                    min_score=min_score,
                    normalized_score=normalized,
                    regime_fit=fit,
                    priority_weight=strategy.priority_weight(),
                    action=action,  # type: ignore[arg-type]
                    bar_open_ms=bar_open_ms,
                    timeframe=timeframe or Config.ENTRY_TIMEFRAME,
                )
            )
        return scores

    @staticmethod
    def pick_best(scores: list[StrategyScore]) -> Optional[StrategyScore]:
        valid = [s for s in scores if s.normalized_score > 0]
        if not valid:
            return None
        return max(
            valid,
            key=lambda s: (
                s.normalized_score,
                s.adjusted_score,
                s.priority_weight,
                s.score,
            ),
        )

    @staticmethod
    def qualifies_for_tier2(
        best: StrategyScore,
        *,
        promote_normalized: Optional[float] = None,
    ) -> bool:
        """
        Tier-2 promotion: normalized performance OR scaled adjusted threshold.
        Option A: normalized_score >= promote bar (strategy-relative).
        Option B: raw >= min AND adjusted >= dynamically scaled threshold.
        """
        promote = promote_normalized or Config.TIER2_PROMOTE_NORMALIZED
        if best.normalized_score >= promote:
            return True
        if best.score < best.min_score:
            return False
        required_adjusted = ScoringEngine.scaled_adjusted_threshold(
            best.min_score,
            promote,
            best.regime_fit,
            best.priority_weight,
        )
        return best.adjusted_score >= required_adjusted

    def signal_for_assignment(
        self,
        snapshot: MarketSnapshot,
        assignment: StrategyScore,
    ) -> Optional[SignalCandidate]:
        """Re-evaluate assigned strategy for execution-ready signal."""
        strategy = self.registry.get(assignment.strategy)
        if strategy is None or not strategy.is_enabled():
            return None

        fit = strategy.regime_fit(snapshot)
        if fit <= 0:
            return None

        try:
            signal = strategy.evaluate(snapshot)
        except Exception as exc:
            error_logger.error(
                "Strategy %s signal fetch failed on %s: %s",
                strategy.tag,
                snapshot.symbol,
                exc,
            )
            return None

        if signal is None:
            return None

        signal = CandidateArbitrator.apply_regime_fit(signal, fit)
        if not self._passes_min_score(signal):
            return None
        return signal

    @staticmethod
    def _passes_min_score(signal: SignalCandidate) -> bool:
        return signal.score >= ScoringEngine.strategy_min_score(
            signal.strategy, signal
        )
