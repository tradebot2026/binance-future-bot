"""Candle / price-action reversal strategy module."""

from __future__ import annotations

from typing import Set

import pandas as pd

from engines.candle_reversals_engine import evaluate_candle_reversal
from config import Config
from constants import STRATEGY_PRICE_ACTION_REVERSAL
from core.types import MarketSnapshot, RegimeLabel
from strategies.pa_common import PaSetupResult
from strategies.pa_strategy_base import PaStrategyBase


class CandleReversalStrategy(PaStrategyBase):
    tag = STRATEGY_PRICE_ACTION_REVERSAL
    display_name = "Price Action Reversal"
    _min_score_attr = "PRICE_ACTION_REVERSAL_MIN_SCORE"
    _max_positions_attr = "MAX_PRICE_ACTION_REVERSAL_POSITIONS"
    _priority_attr = "STRATEGY_PRIORITY_PRICE_ACTION_REVERSAL"

    def allowed_regimes(self) -> Set[str]:
        return {
            RegimeLabel.RANGE_CHOP.value,
            RegimeLabel.UNCLEAR.value,
            RegimeLabel.STRONG_TREND.value,
        }

    def regime_fit(self, snapshot: MarketSnapshot) -> float:
        if snapshot.regime == RegimeLabel.RANGE_CHOP:
            return 1.0
        if snapshot.regime == RegimeLabel.UNCLEAR:
            return 0.9
        if snapshot.regime == RegimeLabel.STRONG_TREND:
            return 0.55
        return 0.4

    def _eval_direction(
        self,
        action: str,
        snapshot: MarketSnapshot,
        df_entry: pd.DataFrame,
        price: float,
        atr: float,
    ) -> PaSetupResult:
        result = evaluate_candle_reversal(action, df_entry, price, atr)
        if result.passed:
            self._log_approval(snapshot, result)
        return result
