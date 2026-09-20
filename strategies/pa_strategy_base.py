"""Base class for Phase 2 price-action strategies."""

from __future__ import annotations

from typing import Callable, Optional, Set

import pandas as pd

from config import Config
from core.strategy_base import BaseStrategy
from core.strategy_registry import strategy_enable_flag
from core.types import MarketSnapshot, StrategyResult
from indicators.market_analyzer import MarketAnalyzer
from logger import signal_logger
from strategies.pa_common import (
    PaSetupResult,
    pick_directional_setup,
    prepare_df,
    resolve_price,
    setup_to_result,
)
from utils import safe_float

SetupFn = Callable[[str, pd.DataFrame, float, float], PaSetupResult]


class PaStrategyBase(BaseStrategy):
    """Shared scan/evaluate flow for price-action engines."""

    _min_score_attr: str = ""
    _max_positions_attr: str = ""
    _priority_attr: str = ""
    _enable_tag: str = ""

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

    def scan(
        self,
        pair: str,
        dataframe: dict[str, pd.DataFrame],
        market_data: MarketSnapshot,
    ) -> StrategyResult:
        setup = self._evaluate_setup(market_data, dataframe)
        if setup is None:
            return StrategyResult.neutral(pair, self.tag)
        macro = self._macro_trend(market_data)
        return setup_to_result(
            pair,
            self.tag,
            setup,
            timeframe=self.entry_tf,
            snapshot=market_data,
            macro_trend=macro,
        )

    def _evaluate_setup(
        self,
        snapshot: MarketSnapshot,
        dataframe: dict[str, pd.DataFrame],
    ) -> Optional[PaSetupResult]:
        df_entry = prepare_df(dataframe.get(self.entry_tf), self.analyzer)
        if df_entry is None:
            return None

        price = resolve_price(snapshot, df_entry)
        atr = self.analyzer.get_latest_atr(df_entry)
        if atr <= 0:
            return None

        long_setup = self._eval_direction("LONG", snapshot, df_entry, price, atr)
        short_setup = self._eval_direction("SHORT", snapshot, df_entry, price, atr)
        return pick_directional_setup(
            long_setup,
            short_setup,
            min_score=self.min_score(),
            win_margin=Config.DIRECTION_WIN_MARGIN,
        )

    def _eval_direction(
        self,
        action: str,
        snapshot: MarketSnapshot,
        df_entry: pd.DataFrame,
        price: float,
        atr: float,
    ) -> PaSetupResult:
        raise NotImplementedError

    def _macro_trend(self, snapshot: MarketSnapshot) -> str:
        df = prepare_df(snapshot.candles.get(self.trend_tf), self.analyzer)
        if df is None or df.empty:
            return "NEUTRAL"
        row = df.iloc[-1]
        if bool(row.get("trend_bullish")):
            return "BULLISH"
        if bool(row.get("trend_bearish")):
            return "BEARISH"
        return "NEUTRAL"

    def _log_approval(self, snapshot: MarketSnapshot, setup: PaSetupResult) -> None:
        signal_logger.info(
            "APPROVED %s %s | strategy=%s | score=%.1f | entry=%.6f sl=%.6f",
            snapshot.symbol,
            setup.direction,
            self.tag,
            setup.score,
            setup.entry,
            setup.stop_loss,
        )
