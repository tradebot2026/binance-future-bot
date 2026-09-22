"""Strategy correlation & indicator overlap protection."""

from __future__ import annotations

from dataclasses import replace
from typing import Iterable

from config import Config
from core.types import StrategyResult
from executor import log_execution_rejected
from utils import safe_float

# Tag groups that describe the same structural zone — only highest score counts.
_OVERLAP_TAG_GROUPS: tuple[frozenset[str], ...] = (
    frozenset({"order_block", "ob", "smc_ob"}),
    frozenset({"hvn", "volume_profile", "vp_hvn", "poc"}),
    frozenset({"liquidity_sweep", "sweep", "lsc"}),
    frozenset({"vwap", "vwap_band", "mean_reversion"}),
    frozenset({"breakout", "breakout_retest", "vp_breakout"}),
    frozenset({"false_breakout", "sfp", "failed_auction"}),
)


class CorrelationGuard:
    """
    Prevent artificial score inflation when multiple strategies fire on
    the same price level / indicator cluster.
    """

    def __init__(
        self,
        *,
        level_tolerance_atr: float | None = None,
    ) -> None:
        self.level_tolerance_atr = (
            level_tolerance_atr
            if level_tolerance_atr is not None
            else Config.CORRELATION_LEVEL_TOLERANCE_ATR
        )

    def deduplicate(self, results: Iterable[StrategyResult]) -> list[StrategyResult]:
        """Return results with overlapping setups merged — keep strongest per cluster."""
        actionable = [r for r in results if r.is_actionable]
        if len(actionable) <= 1:
            return list(results)

        kept: list[StrategyResult] = []
        suppressed: set[str] = set()

        sorted_results = sorted(actionable, key=lambda r: r.score, reverse=True)
        for candidate in sorted_results:
            if candidate.strategy in suppressed:
                continue

            cluster = [candidate]
            for other in sorted_results:
                if other.strategy == candidate.strategy:
                    continue
                if other.strategy in suppressed:
                    continue
                if self._overlaps(candidate, other):
                    cluster.append(other)
                    suppressed.add(other.strategy)

            if len(cluster) > 1:
                merged = replace(
                    candidate,
                    correlated_with=[c.strategy for c in cluster[1:]],
                    structure_metadata={
                        **candidate.structure_metadata,
                        "correlated_strategies": [c.strategy for c in cluster],
                    },
                )
                kept.append(merged)
                for dropped in cluster[1:]:
                    log_execution_rejected(
                        dropped.symbol,
                        (
                            f"Dropped by CorrelationGuard — overlapped "
                            f"{candidate.strategy} {candidate.direction} "
                            f"score={candidate.score:.1f}"
                        ),
                        strategy=dropped.strategy,
                    )
            else:
                kept.append(candidate)

        neutrals = [r for r in results if not r.is_actionable]
        return kept + neutrals

    def _overlaps(self, a: StrategyResult, b: StrategyResult) -> bool:
        if a.direction != b.direction:
            return False
        if self._tags_overlap(a.level_tags, b.level_tags):
            return True
        return self._levels_overlap(a, b)

    @staticmethod
    def _tags_overlap(tags_a: list[str], tags_b: list[str]) -> bool:
        if not tags_a or not tags_b:
            return False
        set_a = {t.lower() for t in tags_a}
        set_b = {t.lower() for t in tags_b}
        if set_a & set_b:
            return True
        for group in _OVERLAP_TAG_GROUPS:
            if (set_a & group) and (set_b & group):
                return True
        return False

    def _levels_overlap(self, a: StrategyResult, b: StrategyResult) -> bool:
        levels_a = [safe_float(v) for v in a.key_levels if safe_float(v) > 0]
        levels_b = [safe_float(v) for v in b.key_levels if safe_float(v) > 0]
        if not levels_a or not levels_b:
            return False

        atr = max(safe_float(a.atr), safe_float(b.atr), 1e-9)
        tolerance = atr * self.level_tolerance_atr
        for la in levels_a:
            for lb in levels_b:
                if abs(la - lb) <= tolerance:
                    return True
        return False
