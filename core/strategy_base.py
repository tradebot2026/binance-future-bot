"""Abstract base classes for pluggable trading strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Set

import pandas as pd

if TYPE_CHECKING:
    from core.types import MarketSnapshot, SignalCandidate, StrategyResult


class StrategyModule(ABC):
    """Legacy strategy contract — preserved for backward compatibility."""

    tag: str
    display_name: str

    @abstractmethod
    def is_enabled(self) -> bool:
        """Return True when strategy is enabled via config."""

    @abstractmethod
    def default_timeframes(self) -> tuple[str, ...]:
        """Timeframes required for evaluation."""

    @abstractmethod
    def regime_fit(self, snapshot: "MarketSnapshot") -> float:
        """Return 0.0–1.0 fit multiplier for the current regime."""

    @abstractmethod
    def evaluate(self, snapshot: "MarketSnapshot") -> Optional["SignalCandidate"]:
        """Evaluate symbol and return a signal or None."""

    @abstractmethod
    def max_concurrent_positions(self) -> int:
        """Maximum open positions for this strategy."""

    @abstractmethod
    def priority_weight(self) -> float:
        """Arbitration priority weight (higher = preferred on ties)."""

    @abstractmethod
    def size_multiplier(self, score: float) -> float:
        """Position size multiplier for this strategy."""

    def allowed_regimes(self) -> Set[str]:
        """Regime labels where this strategy may run (empty = all)."""
        return set()

    def requires_top_volume(self) -> bool:
        """When True, strategy only runs on top-N volume symbols."""
        return False

    def top_volume_limit(self) -> int:
        """Top-N volume rank required when requires_top_volume is True."""
        return 0

    def min_score(self, signal: Optional["SignalCandidate"] = None) -> float:
        """Execution floor — override per strategy."""
        from core.scoring_engine import ScoringEngine

        return ScoringEngine.strategy_min_score(self.tag, signal)


class BaseStrategy(StrategyModule):
    """
    Standard multi-strategy interface:
    scan(pair, dataframes, market_data) -> StrategyResult
    evaluate() bridges to SignalCandidate for legacy consumers.
    """

    def scan(
        self,
        pair: str,
        dataframe: dict[str, pd.DataFrame],
        market_data: "MarketSnapshot",
    ) -> "StrategyResult":
        """Default: delegate to legacy _evaluate_legacy hook."""
        from core.types import StrategyResult

        signal = self._evaluate_legacy(market_data)
        if signal is not None:
            return StrategyResult.from_signal(signal)
        return StrategyResult.neutral(pair, self.tag)

    def _evaluate_legacy(
        self, snapshot: "MarketSnapshot"
    ) -> Optional["SignalCandidate"]:
        """Override in migrated strategies (renamed from evaluate)."""
        return None

    def evaluate(self, snapshot: "MarketSnapshot") -> Optional["SignalCandidate"]:
        result = self.scan(snapshot.symbol, snapshot.candles, snapshot)
        return self.result_to_signal(result, snapshot)

    @staticmethod
    def result_to_signal(
        result: "StrategyResult",
        snapshot: "MarketSnapshot",
    ) -> Optional["SignalCandidate"]:
        from config import Config
        from core.types import SignalCandidate

        if not result.is_actionable or result.direction not in ("LONG", "SHORT"):
            return None

        metadata = dict(result.structure_metadata or {})
        if result.key_levels:
            metadata.setdefault("key_levels", list(result.key_levels))
        if result.level_tags:
            metadata.setdefault("level_tags", list(result.level_tags))
        if result.correlated_with:
            metadata["correlated_with"] = list(result.correlated_with)

        return SignalCandidate(
            symbol=result.symbol,
            action=result.direction,  # type: ignore[arg-type]
            strategy=result.strategy,
            score=float(result.score),
            price=float(snapshot.price),
            atr=float(result.atr or metadata.get("atr", 0.0)),
            timeframe=result.timeframe or Config.ENTRY_TIMEFRAME,
            regime=str(snapshot.regime.value),
            confluence=result.confluence,
            macro_trend=result.macro_trend,
            structure_metadata=metadata,
        )
