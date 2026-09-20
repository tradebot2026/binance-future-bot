"""Shared types for institutional context layers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

Action = Literal["LONG", "SHORT", "NEUTRAL"]


@dataclass
class ContextModuleResult:
    """Output from a single context engine (MTF, VP, OI, order flow)."""

    module: str
    direction: Action = "NEUTRAL"
    score: float = 0.0
    multiplier: float = 1.0
    bonus: float = 0.0
    key_levels: list[float] = field(default_factory=list)
    level_tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_actionable(self) -> bool:
        return self.direction in ("LONG", "SHORT") and self.score > 0


@dataclass
class InstitutionalContext:
    """Aggregated institutional confluence for one symbol snapshot."""

    symbol: str
    modules: list[ContextModuleResult] = field(default_factory=list)
    long_multiplier: float = 1.0
    short_multiplier: float = 1.0
    long_bonus: float = 0.0
    short_bonus: float = 0.0

    def multiplier_for(self, direction: str) -> float:
        if direction == "LONG":
            return self.long_multiplier
        if direction == "SHORT":
            return self.short_multiplier
        return 1.0

    def bonus_for(self, direction: str) -> float:
        if direction == "LONG":
            return self.long_bonus
        if direction == "SHORT":
            return self.short_bonus
        return 0.0

    def conflict_penalty(self, direction: str) -> float:
        """Return 0–1 penalty when opposing modules dominate."""
        if direction not in ("LONG", "SHORT"):
            return 0.0
        oppose = "SHORT" if direction == "LONG" else "LONG"
        aligned = sum(m.score for m in self.modules if m.direction == direction)
        opposed = sum(m.score for m in self.modules if m.direction == oppose)
        if opposed <= 0 or aligned >= opposed:
            return 0.0
        return min((opposed - aligned) / 100.0, 1.0)

    @classmethod
    def empty(cls, symbol: str) -> "InstitutionalContext":
        return cls(symbol=symbol.upper())
