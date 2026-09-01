"""Strategy plugin registry with enable flags and kill-switch awareness."""

from __future__ import annotations

from typing import Iterable, Optional

from config import Config
from core.strategy_base import StrategyModule
from core.types import RegimeLabel
from logger import system_logger


class StrategyRegistry:
    """Central registry for all strategy modules."""

    def __init__(self) -> None:
        self._strategies: dict[str, StrategyModule] = {}

    def register(self, strategy: StrategyModule) -> None:
        self._strategies[strategy.tag] = strategy
        system_logger.debug(
            "Registered strategy %s (%s)", strategy.tag, strategy.display_name
        )

    def get(self, tag: str) -> Optional[StrategyModule]:
        return self._strategies.get(tag)

    def all(self) -> list[StrategyModule]:
        return list(self._strategies.values())

    def enabled(self) -> list[StrategyModule]:
        return [s for s in self._strategies.values() if s.is_enabled()]

    def enabled_for_regime(self, regime: RegimeLabel) -> list[StrategyModule]:
        active: list[StrategyModule] = []
        for strategy in self.enabled():
            allowed = strategy.allowed_regimes()
            if allowed and regime.value not in allowed:
                continue
            active.append(strategy)
        return active

    def filter_tags(self, tags: Optional[Iterable[str]]) -> list[StrategyModule]:
        if not tags:
            return self.enabled()
        tag_set = {str(t).upper() for t in tags}
        return [s for s in self.enabled() if s.tag in tag_set]

    def tags(self) -> list[str]:
        return list(self._strategies.keys())


def build_default_registry(db=None) -> StrategyRegistry:
    """Construct registry with all bundled strategy modules."""
    from strategies import build_strategy_registry

    return build_strategy_registry(db=db)


def strategy_enable_flag(tag: str) -> bool:
    """Resolve per-strategy enable flag from Config."""
    flags = {
        "SMC_TREND": Config.ENABLE_STRATEGY_SMC,
        "SMC_MULTITF": Config.ENABLE_STRATEGY_SMC,
        "RANGE_REVERSION": Config.ENABLE_STRATEGY_RANGE,
        "LIQUIDITY_SWEEP_CONT": Config.ENABLE_STRATEGY_LIQUIDITY_SWEEP,
        "VWAP_PULLBACK": Config.ENABLE_STRATEGY_VWAP_PULLBACK,
        "VOLUME_PROFILE_BREAKOUT": Config.ENABLE_STRATEGY_VP_BREAKOUT,
        "VOL_EXPANSION_MR": Config.ENABLE_STRATEGY_VOL_EXPANSION,
    }
    legacy_range = Config.ENABLE_RANGE_REGIME and tag == "RANGE_REVERSION"
    return bool(flags.get(tag, False) or legacy_range)
