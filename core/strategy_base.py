"""Abstract base class for pluggable trading strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, Set

if TYPE_CHECKING:
    from core.types import MarketSnapshot, SignalCandidate


class StrategyModule(ABC):
    """Every strategy module implements this contract."""

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
