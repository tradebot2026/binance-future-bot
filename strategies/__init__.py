"""Bundled strategy module registration."""

from __future__ import annotations

from typing import Optional

from core.strategy_registry import StrategyRegistry
from strategies.breakout_retest import BreakoutRetestStrategy
from strategies.candle_reversals import CandleReversalStrategy
from strategies.false_breakout_sfp import FalseBreakoutSfpStrategy
from strategies.mtf_alignment import MtfAlignmentStrategy
from strategies.oi_funding_context import OiFundingContextStrategy
from strategies.order_flow_imbalance import OrderFlowImbalanceStrategy
from strategies.strategy_extensions import EXTENSION_STRATEGIES
from strategies.volume_profile import VolumeProfileStrategy
from strategies.strategy_lsc import LiquiditySweepStrategy
from strategies.strategy_range import RangeStrategy
from strategies.strategy_smc import SMCStrategy
from strategies.strategy_vemr import VolExpansionStrategy
from strategies.strategy_vpb import VpBreakoutStrategy
from strategies.strategy_vwap import VwapPullbackStrategy
from strategies.trend_continuation import TrendContinuationStrategy
from strategies.volatility_squeeze import VolatilitySqueezeStrategy

if False:  # TYPE_CHECKING placeholder for optional future imports
    from database import DatabaseManager
    from exchange import BinanceExchangeManager


def build_strategy_registry(
    db: Optional["DatabaseManager"] = None,
    exchange: Optional["BinanceExchangeManager"] = None,
) -> StrategyRegistry:
    """Create registry with core, Phase 2/3, and extension strategy modules."""
    registry = StrategyRegistry()
    registry.register(SMCStrategy(db=db))
    registry.register(RangeStrategy(db=db))
    registry.register(LiquiditySweepStrategy(db=db))
    registry.register(VwapPullbackStrategy(db=db))
    registry.register(VpBreakoutStrategy(db=db))
    registry.register(VolExpansionStrategy(db=db))
    registry.register(BreakoutRetestStrategy())
    registry.register(FalseBreakoutSfpStrategy())
    registry.register(VolatilitySqueezeStrategy())
    registry.register(TrendContinuationStrategy())
    registry.register(CandleReversalStrategy())
    registry.register(MtfAlignmentStrategy())
    registry.register(VolumeProfileStrategy())
    registry.register(OrderFlowImbalanceStrategy())
    registry.register(OiFundingContextStrategy(exchange=exchange))
    for strategy_cls in EXTENSION_STRATEGIES:
        registry.register(strategy_cls())
    return registry
