"""Volatility squeeze (BB/KC) strategy module."""

from __future__ import annotations

from typing import Set

import pandas as pd

from constants import STRATEGY_VOL_SQUEEZE
from core.types import MarketSnapshot, RegimeLabel
from strategies.pa_common import PaSetupResult
from strategies.pa_strategy_base import PaStrategyBase
from engines.volatility_squeeze_engine import evaluate_volatility_squeeze


class VolatilitySqueezeStrategy(PaStrategyBase):
    tag = STRATEGY_VOL_SQUEEZE
    display_name = "Volatility Squeeze"
    _min_score_attr = "VOL_SQUEEZE_MIN_SCORE"
    _max_positions_attr = "MAX_VOL_SQUEEZE_POSITIONS"
    _priority_attr = "STRATEGY_PRIORITY_VOL_SQUEEZE"

    def allowed_regimes(self) -> Set[str]:
        return {
            RegimeLabel.COMPRESSION.value,
            RegimeLabel.EXPANSION_SPIKE.value,
            RegimeLabel.UNCLEAR.value,
        }

    def regime_fit(self, snapshot: MarketSnapshot) -> float:
        if snapshot.regime == RegimeLabel.COMPRESSION:
            return 1.0
        if snapshot.regime == RegimeLabel.EXPANSION_SPIKE:
            return 0.95
        if snapshot.regime == RegimeLabel.UNCLEAR:
            return 0.7
        return 0.3

    def _eval_direction(
        self,
        action: str,
        snapshot: MarketSnapshot,
        df_entry: pd.DataFrame,
        price: float,
        atr: float,
    ) -> PaSetupResult:
        result = evaluate_volatility_squeeze(action, df_entry, price, atr)
        if result.passed:
            self._log_approval(snapshot, result)
        return result
