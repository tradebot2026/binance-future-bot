"""Two-tier universe: watchlist scoring vs hot active pipeline."""

from __future__ import annotations

import time
from typing import Callable, Optional

from config import Config
from core.scoring_engine import ScoringEngine
from core.types import CoinAssignment, StrategyScore
from logger import scanner_logger


class AssignmentManager:
    """
    Tier 1: all watchlist symbols scored at candle closes.
    Tier 2: strategy-normalized high performers kept hot for execution.
    """

    def __init__(
        self,
        *,
        promote_normalized: Optional[float] = None,
        demote_normalized: Optional[float] = None,
        max_hot: Optional[int] = None,
    ) -> None:
        self._promote_normalized = (
            promote_normalized or Config.TIER2_PROMOTE_NORMALIZED
        )
        self._demote_normalized = demote_normalized or Config.TIER2_DEMOTE_NORMALIZED
        self._max_hot = max_hot or Config.TIER2_HOT_SIZE
        self._tier2: dict[str, CoinAssignment] = {}
        self._last_best: dict[str, StrategyScore] = {}

    @property
    def tier2_size(self) -> int:
        return len(self._tier2)

    def is_hot(self, symbol: str) -> bool:
        return symbol.upper() in self._tier2

    def hot_symbols(self) -> list[str]:
        return list(self._tier2.keys())

    def get_assignment(self, symbol: str) -> Optional[CoinAssignment]:
        return self._tier2.get(symbol.upper())

    def tier2_summary(self) -> list[tuple[str, str, float]]:
        """Return (symbol, strategy, normalized_score) sorted by normalized desc."""
        rows = [
            (a.symbol, a.strategy, a.normalized_score)
            for a in self._tier2.values()
        ]
        return sorted(rows, key=lambda row: row[2], reverse=True)

    def last_best_score(self, symbol: str) -> Optional[StrategyScore]:
        return self._last_best.get(symbol.upper())

    def update(
        self,
        best: StrategyScore,
        *,
        open_symbols: set[str],
    ) -> tuple[bool, bool, Optional[str]]:
        """
        Apply hysteresis promotion/demotion on normalized strategy performance.
        Returns (promoted, demoted, demoted_symbol).
        """
        symbol = best.symbol.upper()
        open_symbols = {s.upper() for s in open_symbols}
        frozen = symbol in open_symbols
        self._last_best[symbol] = best

        existing = self._tier2.get(symbol)
        if existing is not None:
            if frozen:
                existing.frozen = True
                existing.score = best.score
                existing.adjusted_score = best.adjusted_score
                existing.normalized_score = best.normalized_score
                existing.min_score = best.min_score
                existing.strategy = best.strategy
                return False, False, None

            if best.normalized_score < self._demote_normalized:
                del self._tier2[symbol]
                scanner_logger.info(
                    "Tier2 demote %s | strategy=%s norm=%.1f raw=%.1f < %.1f",
                    symbol,
                    existing.strategy,
                    best.normalized_score,
                    best.score,
                    self._demote_normalized,
                )
                return False, True, symbol

            existing.strategy = best.strategy
            existing.score = best.score
            existing.adjusted_score = best.adjusted_score
            existing.normalized_score = best.normalized_score
            existing.min_score = best.min_score
            existing.assigned_bar_open_ms = best.bar_open_ms
            existing.frozen = False
            return False, False, None

        if self._qualifies_for_promotion(best):
            if len(self._tier2) >= self._max_hot:
                evicted = self._evict_lowest(unless=symbol)
                if evicted:
                    return True, False, evicted

            now_ms = int(time.time() * 1000)
            self._tier2[symbol] = CoinAssignment(
                symbol=symbol,
                strategy=best.strategy,
                score=best.score,
                adjusted_score=best.adjusted_score,
                normalized_score=best.normalized_score,
                min_score=best.min_score,
                assigned_bar_open_ms=best.bar_open_ms,
                assigned_at_ms=now_ms,
                frozen=frozen,
            )
            scanner_logger.info(
                "Tier2 promote %s | strategy=%s norm=%.1f raw=%.1f min=%.1f",
                symbol,
                best.strategy,
                best.normalized_score,
                best.score,
                best.min_score,
            )
            return True, False, None

        return False, False, None

    def gc_demoted(
        self,
        symbol: str,
        flush_fn: Callable[[str], None],
    ) -> None:
        """Flush memory and WS resources for a demoted symbol."""
        flush_fn(symbol.upper())

    def _qualifies_for_promotion(self, best: StrategyScore) -> bool:
        return ScoringEngine.qualifies_for_tier2(
            best,
            promote_normalized=self._promote_normalized,
        )

    def _evict_lowest(self, unless: str) -> Optional[str]:
        if not self._tier2:
            return None
        evictable = [
            (sym, assign)
            for sym, assign in self._tier2.items()
            if sym != unless.upper() and not assign.frozen
        ]
        if not evictable:
            return None
        sym, assign = min(evictable, key=lambda item: item[1].normalized_score)
        del self._tier2[sym]
        scanner_logger.info(
            "Tier2 evict %s (norm=%.1f raw=%.1f) — hot pool full (%s).",
            sym,
            assign.normalized_score,
            assign.score,
            self._max_hot,
        )
        return sym
