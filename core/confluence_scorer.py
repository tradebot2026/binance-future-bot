"""Weighted scoring and multi-strategy confluence bonus."""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Optional

from config import Config
from core.context.context_types import InstitutionalContext
from core.strategy_base import StrategyModule
from core.types import MarketSnapshot, StrategyResult, StrategyScore


class ConfluenceScorer:
    """
    Convert StrategyResult rows into weighted StrategyScore objects.
    Applies a unified confluence bonus when uncorrelated strategies agree.
    """

    def __init__(self, strategies: dict[str, StrategyModule]) -> None:
        self._strategies = strategies

    def score_results(
        self,
        snapshot: MarketSnapshot,
        results: Iterable[StrategyResult],
        *,
        bar_open_ms: int = 0,
        timeframe: str = "",
        institutional_context: InstitutionalContext | None = None,
    ) -> list[StrategyScore]:
        rows = [r for r in results if r.is_actionable]
        if not rows:
            return []

        direction_groups: dict[str, list[StrategyResult]] = defaultdict(list)
        for row in rows:
            direction_groups[str(row.direction)].append(row)

        confluence_bonus = self._compute_confluence_bonus(direction_groups)
        scores: list[StrategyScore] = []

        for result in rows:
            strategy = self._strategies.get(result.strategy)
            if strategy is None:
                continue

            fit = strategy.regime_fit(snapshot)
            if fit <= 0:
                continue

            from core.strategy_base import BaseStrategy

            signal = BaseStrategy.result_to_signal(result, snapshot)
            min_score = strategy.min_score(signal)

            raw = float(result.score)
            priority = strategy.priority_weight()
            adjusted = raw * fit * priority

            bonus = 0.0
            if result.direction in confluence_bonus:
                bonus = confluence_bonus[result.direction]
                if result.correlated_with:
                    bonus *= max(
                        1.0 - Config.CORRELATION_BONUS_PENALTY * len(result.correlated_with),
                        0.0,
                    )

            ctx_bonus = 0.0
            ctx_mult = 1.0
            if institutional_context is not None and Config.ENABLE_INSTITUTIONAL_CONTEXT:
                ctx_bonus = institutional_context.bonus_for(result.direction)
                ctx_mult = institutional_context.multiplier_for(result.direction)
                conflict = institutional_context.conflict_penalty(result.direction)
                if conflict > 0:
                    ctx_mult *= max(
                        1.0 - conflict * Config.INSTITUTIONAL_CONTEXT_CONFLICT_PENALTY,
                        Config.INSTITUTIONAL_CONTEXT_MULT_MIN,
                    )

            pre_mult = min(raw + bonus + ctx_bonus, 100.0)
            final = min(pre_mult * ctx_mult, 100.0)
            normalized = self._normalize_score(final, min_score)

            scores.append(
                StrategyScore(
                    symbol=result.symbol,
                    strategy=result.strategy,
                    score=raw,
                    adjusted_score=(adjusted + bonus * fit * priority) * ctx_mult,
                    min_score=min_score,
                    normalized_score=normalized,
                    regime_fit=fit,
                    priority_weight=priority,
                    action=result.direction,  # type: ignore[arg-type]
                    bar_open_ms=bar_open_ms,
                    timeframe=timeframe or result.timeframe,
                    confluence_bonus=bonus,
                    context_bonus=ctx_bonus,
                    context_multiplier=ctx_mult,
                    final_score=final,
                )
            )
        return scores

    def _compute_confluence_bonus(
        self,
        direction_groups: dict[str, list[StrategyResult]],
    ) -> dict[str, float]:
        """Bonus per direction when multiple uncorrelated strategies agree."""
        bonuses: dict[str, float] = {}
        max_bonus = Config.CONFLUENCE_BONUS_MAX
        step = Config.CONFLUENCE_BONUS_PER_STRATEGY

        for direction, group in direction_groups.items():
            independent = [
                r for r in group if not r.correlated_with
            ] or group
            unique_strategies = {r.strategy for r in independent}
            if len(unique_strategies) < 2:
                continue
            bonus = min(
                (len(unique_strategies) - 1) * step,
                max_bonus,
            )
            bonuses[direction] = bonus
        return bonuses

    @staticmethod
    def _normalize_score(raw_score: float, min_score: float) -> float:
        if raw_score <= min_score:
            return 0.0
        span = 100.0 - min_score
        if span <= 0:
            return 0.0
        return (raw_score - min_score) / span * 100.0

    @staticmethod
    def pick_best(scores: list[StrategyScore]) -> Optional[StrategyScore]:
        valid = [s for s in scores if s.normalized_score > 0 or s.final_score > s.min_score]
        if not valid:
            valid = [s for s in scores if s.final_score > 0]
        if not valid:
            return None
        return max(
            valid,
            key=lambda s: (
                s.final_score,
                s.normalized_score,
                s.adjusted_score,
                s.priority_weight,
                s.score,
            ),
        )
