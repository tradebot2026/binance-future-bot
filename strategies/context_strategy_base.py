"""Base class for Phase 3 institutional context strategy modules."""

from __future__ import annotations

from typing import Callable, Optional, Set

import pandas as pd

from config import Config
from core.context.context_types import ContextModuleResult
from core.strategy_base import BaseStrategy
from core.strategy_registry import strategy_enable_flag
from core.types import MarketSnapshot, StrategyResult
from indicators.market_analyzer import MarketAnalyzer
from strategies.pa_common import prepare_df, resolve_price, setup_to_result
from strategies.pa_common import PaSetupResult, build_r_targets


ContextFn = Callable[..., ContextModuleResult]


class ContextStrategyBase(BaseStrategy):
    """Wrap a context engine as an actionable strategy module."""

    _min_score_attr: str = ""
    _max_positions_attr: str = ""
    _priority_attr: str = ""
    _context_fn: ContextFn | None = None

    def __init__(self) -> None:
        self.analyzer = MarketAnalyzer()
        self.entry_tf = Config.ENTRY_TIMEFRAME
        self.confirm_tf = Config.CONFIRM_TIMEFRAME
        self.trend_tf = Config.TREND_TIMEFRAME

    def is_enabled(self) -> bool:
        return strategy_enable_flag(self.tag)

    def default_timeframes(self) -> tuple[str, ...]:
        return (self.entry_tf, self.confirm_tf, self.trend_tf)

    def min_score(self, signal=None) -> float:
        if self._min_score_attr:
            return float(getattr(Config, self._min_score_attr, Config.STRATEGY_MIN_SCORE))
        return Config.STRATEGY_MIN_SCORE

    def max_concurrent_positions(self) -> int:
        if self._max_positions_attr:
            return int(getattr(Config, self._max_positions_attr, 2))
        return 2

    def priority_weight(self) -> float:
        if self._priority_attr:
            return float(getattr(Config, self._priority_attr, 1.0))
        return 1.0

    def size_multiplier(self, score: float) -> float:
        if score >= Config.SCORE_FULL_SIZE:
            return 1.0
        if score >= self.min_score():
            return Config.HALF_SIZE_MULTIPLIER
        return 0.0

    def allowed_regimes(self) -> Set[str]:
        return set()

    def regime_fit(self, snapshot: MarketSnapshot) -> float:
        return 1.0

    def scan(
        self,
        pair: str,
        dataframe: dict[str, pd.DataFrame],
        market_data: MarketSnapshot,
    ) -> StrategyResult:
        ctx = self._evaluate_context(market_data, dataframe)
        if not ctx.is_actionable or ctx.score < self.min_score():
            return StrategyResult.neutral(pair, self.tag)

        setup = self._context_to_setup(ctx, market_data, dataframe)
        if setup is None:
            return StrategyResult.neutral(pair, self.tag)

        return setup_to_result(
            pair,
            self.tag,
            setup,
            timeframe=self.entry_tf,
            snapshot=market_data,
            macro_trend=ctx.metadata.get("trend_bias", "NEUTRAL"),
        )

    def _evaluate_context(
        self,
        snapshot: MarketSnapshot,
        dataframe: dict[str, pd.DataFrame],
    ) -> ContextModuleResult:
        raise NotImplementedError

    def _context_to_setup(
        self,
        ctx: ContextModuleResult,
        snapshot: MarketSnapshot,
        dataframe: dict[str, pd.DataFrame],
    ) -> Optional[PaSetupResult]:
        df_entry = prepare_df(dataframe.get(self.entry_tf), self.analyzer)
        if df_entry is None:
            return None

        price = resolve_price(snapshot, df_entry)
        atr = self.analyzer.get_latest_atr(df_entry)
        if atr <= 0 or price <= 0 or ctx.direction not in ("LONG", "SHORT"):
            return None

        sl, tp1, tp2, tp3 = build_r_targets(ctx.direction, price, atr)
        reasons = list(ctx.metadata.get("reasons", []))
        return PaSetupResult(
            passed=True,
            score=ctx.score,
            direction=ctx.direction,
            reasons=reasons,
            entry=price,
            stop_loss=sl,
            tp1=tp1,
            tp2=tp2,
            tp3=tp3,
            key_levels=list(ctx.key_levels),
            level_tags=list(ctx.level_tags),
            confluence=ctx.module,
            atr=atr,
        )
