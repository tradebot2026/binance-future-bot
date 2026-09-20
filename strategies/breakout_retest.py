"""Breakout + retest strategy module."""

from __future__ import annotations

from typing import Set

import pandas as pd

from engines.breakout_retest_engine import evaluate_breakout_retest
from constants import STRATEGY_BREAKOUT_RETEST
from core.types import MarketSnapshot, RegimeLabel
from strategies.pa_common import PaSetupResult
from strategies.pa_strategy_base import PaStrategyBase


class BreakoutRetestStrategy(PaStrategyBase):
    tag = STRATEGY_BREAKOUT_RETEST
    display_name = "Breakout + Retest"
    _min_score_attr = "BREAKOUT_RETEST_MIN_SCORE"
    _max_positions_attr = "MAX_BREAKOUT_RETEST_POSITIONS"
    _priority_attr = "STRATEGY_PRIORITY_BREAKOUT_RETEST"

    def allowed_regimes(self) -> Set[str]:
        return {
            RegimeLabel.COMPRESSION.value,
            RegimeLabel.UNCLEAR.value,
            RegimeLabel.STRONG_TREND.value,
            RegimeLabel.EXPANSION_SPIKE.value,
        }

    def regime_fit(self, snapshot: MarketSnapshot) -> float:
        if snapshot.regime == RegimeLabel.COMPRESSION:
            return 1.0
        if snapshot.regime == RegimeLabel.EXPANSION_SPIKE:
            return 0.95
        if snapshot.regime == RegimeLabel.UNCLEAR:
            return 0.85
        if snapshot.regime == RegimeLabel.STRONG_TREND:
            return 0.75
        return 0.35

    def _eval_direction(
        self,
        action: str,
        snapshot: MarketSnapshot,
        df_entry: pd.DataFrame,
        price: float,
        atr: float,
    ) -> PaSetupResult:
        result = evaluate_breakout_retest(action, df_entry, price, atr)
        if result.passed:
            self._log_approval(snapshot, result)
        return result
