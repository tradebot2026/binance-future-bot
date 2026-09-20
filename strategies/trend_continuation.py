"""Trend continuation / EMA momentum strategy module."""

from __future__ import annotations

from typing import Optional, Set

import pandas as pd

from config import Config
from constants import STRATEGY_TREND_MOMENTUM
from core.types import MarketSnapshot, RegimeLabel
from strategies.pa_common import (
    PaSetupResult,
    pick_directional_setup,
    prepare_df,
    resolve_price,
)
from strategies.pa_strategy_base import PaStrategyBase
from engines.trend_continuation_engine import evaluate_trend_continuation


class TrendContinuationStrategy(PaStrategyBase):
    tag = STRATEGY_TREND_MOMENTUM
    display_name = "Trend Continuation / Momentum"
    _min_score_attr = "TREND_MOMENTUM_MIN_SCORE"
    _max_positions_attr = "MAX_TREND_MOMENTUM_POSITIONS"
    _priority_attr = "STRATEGY_PRIORITY_TREND_MOMENTUM"

    def allowed_regimes(self) -> Set[str]:
        return {
            RegimeLabel.STRONG_TREND.value,
            RegimeLabel.EXPANSION_SPIKE.value,
            RegimeLabel.UNCLEAR.value,
        }

    def regime_fit(self, snapshot: MarketSnapshot) -> float:
        if snapshot.regime == RegimeLabel.STRONG_TREND:
            return 1.0
        if snapshot.regime == RegimeLabel.EXPANSION_SPIKE:
            return 0.85
        if snapshot.regime == RegimeLabel.UNCLEAR:
            return 0.65
        return 0.25

    def _evaluate_setup(
        self,
        snapshot: MarketSnapshot,
        dataframe: dict[str, pd.DataFrame],
    ) -> Optional[PaSetupResult]:
        df_entry = prepare_df(dataframe.get(self.entry_tf), self.analyzer)
        df_trend = prepare_df(dataframe.get(self.trend_tf), self.analyzer)
        if df_entry is None or df_trend is None:
            return None

        price = resolve_price(snapshot, df_entry)
        atr = self.analyzer.get_latest_atr(df_entry)
        if atr <= 0:
            return None

        long_setup = evaluate_trend_continuation("LONG", df_entry, df_trend, price, atr)
        short_setup = evaluate_trend_continuation("SHORT", df_entry, df_trend, price, atr)
        setup = pick_directional_setup(
            long_setup,
            short_setup,
            min_score=self.min_score(),
            win_margin=Config.DIRECTION_WIN_MARGIN,
        )
        if setup and setup.passed:
            self._log_approval(snapshot, setup)
        return setup

    def _eval_direction(
        self,
        action: str,
        snapshot: MarketSnapshot,
        df_entry: pd.DataFrame,
        price: float,
        atr: float,
    ) -> PaSetupResult:
        df_trend = prepare_df(snapshot.candles.get(self.trend_tf), self.analyzer)
        if df_trend is None:
            return PaSetupResult(direction=action, reasons=["trend_tf_missing"])
        return evaluate_trend_continuation(action, df_entry, df_trend, price, atr)
