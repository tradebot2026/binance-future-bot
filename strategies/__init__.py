"""Bundled strategy module registration."""

from __future__ import annotations

from typing import Optional

from core.strategy_registry import StrategyRegistry
from strategies.strategy_lsc import LiquiditySweepStrategy
from strategies.strategy_range import RangeStrategy
from strategies.strategy_smc import SMCStrategy
from strategies.strategy_vemr import VolExpansionStrategy
from strategies.strategy_vpb import VpBreakoutStrategy
from strategies.strategy_vwap import VwapPullbackStrategy

if False:  # TYPE_CHECKING placeholder for optional future imports
    from database import DatabaseManager


def build_strategy_registry(db: Optional["DatabaseManager"] = None) -> StrategyRegistry:
    """Create registry with all implemented strategy modules."""
    registry = StrategyRegistry()
    registry.register(SMCStrategy(db=db))
    registry.register(RangeStrategy(db=db))
    registry.register(LiquiditySweepStrategy(db=db))
    registry.register(VwapPullbackStrategy(db=db))
    registry.register(VpBreakoutStrategy(db=db))
    registry.register(VolExpansionStrategy(db=db))
    return registry
