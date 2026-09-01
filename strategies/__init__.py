"""Bundled strategy module registration."""

from __future__ import annotations

from typing import Optional

from core.strategy_registry import StrategyRegistry
from strategies.strategy_range import RangeStrategy
from strategies.strategy_smc import SMCStrategy

if False:  # TYPE_CHECKING placeholder for optional future imports
    from database import DatabaseManager


def build_strategy_registry(db: Optional["DatabaseManager"] = None) -> StrategyRegistry:
    """Create registry with all implemented strategy modules."""
    registry = StrategyRegistry()
    registry.register(SMCStrategy(db=db))
    registry.register(RangeStrategy(db=db))
    return registry
