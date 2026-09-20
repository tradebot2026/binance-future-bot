"""Multi-strategy scoring engine — evaluates symbols against all strategies."""

from __future__ import annotations

from typing import Optional

from config import Config
from constants import (
    STRATEGY_BREAKOUT_RETEST,
    STRATEGY_FALSE_BREAKOUT_SFP,
    STRATEGY_LIQUIDITY_SWEEP,
    STRATEGY_PRICE_ACTION_REVERSAL,
    STRATEGY_RANGE_REVERSION,
    STRATEGY_SMC_TREND,
    STRATEGY_TREND_MOMENTUM,
    STRATEGY_VOL_EXPANSION,
    STRATEGY_VOL_SQUEEZE,
    STRATEGY_VP_BREAKOUT,
    STRATEGY_VWAP_PULLBACK,
)
from core.candidate_arbitrator import CandidateArbitrator
from core.confluence_scorer import ConfluenceScorer
from core.context.institutional_context import InstitutionalContextEvaluator
from core.strategy_evaluator import StrategyEvaluator
from core.strategy_registry import StrategyRegistry
from core.types import MarketSnapshot, SignalCandidate, StrategyScore
from logger import error_logger
from engines.smc_engine import effective_smc_min_score

if False:  # TYPE_CHECKING
    from exchange import BinanceExchangeManager


class ScoringEngine:
    """Score one symbol against all enabled strategies (pure WS, no REST)."""

    def __init__(
        self,
        registry: StrategyRegistry,
        exchange: "BinanceExchangeManager | None" = None,
    ) -> None:
        self.registry = registry
        self.institutional = InstitutionalContextEvaluator(exchange=exchange)
        self.evaluator = StrategyEvaluator(
            registry,
            institutional=self.institutional,
        )
        self.confluence = self.evaluator.confluence_scorer

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
            base = Config.RANGE_MIN_SCORE
        elif strategy == STRATEGY_SMC_TREND:
            if signal is not None:
                base = effective_smc_min_score(
                    signal.confluence, signal.macro_trend or "NEUTRAL"
                )
            else:
                base = Config.STRATEGY_MIN_SCORE
        elif strategy == STRATEGY_LIQUIDITY_SWEEP:
            base = Config.LSC_MIN_SCORE
        elif strategy == STRATEGY_VWAP_PULLBACK:
            base = Config.VWAP_MIN_SCORE
        elif strategy == STRATEGY_VP_BREAKOUT:
            base = Config.VPB_MIN_SCORE
        elif strategy == STRATEGY_VOL_EXPANSION:
            base = Config.VEMR_MIN_SCORE
        elif strategy == STRATEGY_BREAKOUT_RETEST:
            base = Config.BREAKOUT_RETEST_MIN_SCORE
        elif strategy == STRATEGY_FALSE_BREAKOUT_SFP:
            base = Config.FALSE_BREAKOUT_SFP_MIN_SCORE
        elif strategy == STRATEGY_VOL_SQUEEZE:
            base = Config.VOL_SQUEEZE_MIN_SCORE
        elif strategy == STRATEGY_TREND_MOMENTUM:
            base = Config.TREND_MOMENTUM_MIN_SCORE
        elif strategy == STRATEGY_PRICE_ACTION_REVERSAL:
            base = Config.PRICE_ACTION_REVERSAL_MIN_SCORE
        else:
            from constants import (
                STRATEGY_MTF_ALIGNMENT,
                STRATEGY_OI_FUNDING,
                STRATEGY_ORDER_FLOW,
                STRATEGY_VP_KEYLEVEL,
            )

            if strategy == STRATEGY_MTF_ALIGNMENT:
                base = Config.MTF_ALIGNMENT_MIN_SCORE
            elif strategy == STRATEGY_VP_KEYLEVEL:
                base = Config.VP_KEYLEVEL_MIN_SCORE
            elif strategy == STRATEGY_ORDER_FLOW:
                base = Config.ORDER_FLOW_MIN_SCORE
            elif strategy == STRATEGY_OI_FUNDING:
                base = Config.OI_FUNDING_MIN_SCORE
            else:
                base = Config.STRATEGY_MIN_SCORE
        return Config.effective_min_score(base)

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
        return self.evaluator.evaluate(
            snapshot,
            bar_open_ms=bar_open_ms,
            timeframe=timeframe,
        )

    @staticmethod
    def pick_best(scores: list[StrategyScore]) -> Optional[StrategyScore]:
        return ConfluenceScorer.pick_best(scores)

    @staticmethod
    def pick_best_for_tier2(scores: list[StrategyScore]) -> Optional[StrategyScore]:
        """Best scoring result for Tier-2 tracking (includes raw scores at/above min)."""
        valid = [
            s
            for s in scores
            if (s.final_score or s.score) >= s.min_score and s.score > 0
        ]
        if not valid:
            valid = [s for s in scores if s.score > 0]
        if not valid:
            return None
        return max(
            valid,
            key=lambda s: (
                s.final_score or s.score,
                s.normalized_score,
                s.score,
                s.adjusted_score,
                s.priority_weight,
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
        """
        effective = best.final_score or best.score
        if effective < best.min_score:
            return False
        if effective >= Config.tier2_promote_score():
            return True
        promote = promote_normalized or Config.tier2_promote_normalized()
        if best.normalized_score >= promote:
            return True
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
