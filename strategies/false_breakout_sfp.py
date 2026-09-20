"""False breakout / Swing Failure Pattern strategy module."""

from __future__ import annotations

from typing import Set

import pandas as pd

from constants import STRATEGY_FALSE_BREAKOUT_SFP
from core.types import MarketSnapshot, RegimeLabel
from engines.false_breakout_sfp_engine import evaluate_false_breakout_sfp
from strategies.pa_common import PaSetupResult
from strategies.pa_strategy_base import PaStrategyBase


class FalseBreakoutSfpStrategy(PaStrategyBase):
    tag = STRATEGY_FALSE_BREAKOUT_SFP
    display_name = "False Breakout / SFP"
    _min_score_attr = "FALSE_BREAKOUT_SFP_MIN_SCORE"
    _max_positions_attr = "MAX_FALSE_BREAKOUT_SFP_POSITIONS"
    _priority_attr = "STRATEGY_PRIORITY_FALSE_BREAKOUT_SFP"

    def allowed_regimes(self) -> Set[str]:
        return {
            RegimeLabel.RANGE_CHOP.value,
            RegimeLabel.UNCLEAR.value,
            RegimeLabel.COMPRESSION.value,
        }

    def regime_fit(self, snapshot: MarketSnapshot) -> float:
        if snapshot.regime == RegimeLabel.RANGE_CHOP:
            return 1.0
        if snapshot.regime == RegimeLabel.COMPRESSION:
            return 0.9
        if snapshot.regime == RegimeLabel.UNCLEAR:
            return 0.85
        return 0.4

    def _eval_direction(
        self,
        action: str,
        snapshot: MarketSnapshot,
        df_entry: pd.DataFrame,
        price: float,
        atr: float,
    ) -> PaSetupResult:
        result = evaluate_false_breakout_sfp(action, df_entry, price, atr)
        if result.passed:
            self._log_approval(snapshot, result)
        return result
