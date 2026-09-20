"""Multi-timeframe alignment strategy module."""

from __future__ import annotations

from typing import Set

import pandas as pd

from config import Config
from constants import STRATEGY_MTF_ALIGNMENT
from core.context.mtf_alignment_engine import evaluate_mtf_alignment
from core.context.context_types import ContextModuleResult
from core.types import MarketSnapshot, RegimeLabel
from strategies.context_strategy_base import ContextStrategyBase


class MtfAlignmentStrategy(ContextStrategyBase):
    tag = STRATEGY_MTF_ALIGNMENT
    display_name = "Multi-Timeframe Alignment"
    _min_score_attr = "MTF_ALIGNMENT_MIN_SCORE"
    _max_positions_attr = "MAX_MTF_ALIGNMENT_POSITIONS"
    _priority_attr = "STRATEGY_PRIORITY_MTF_ALIGNMENT"

    def allowed_regimes(self) -> Set[str]:
        return {
            RegimeLabel.STRONG_TREND.value,
            RegimeLabel.UNCLEAR.value,
            RegimeLabel.COMPRESSION.value,
        }

    def regime_fit(self, snapshot: MarketSnapshot) -> float:
        if snapshot.regime == RegimeLabel.STRONG_TREND:
            return 1.0
        if snapshot.regime == RegimeLabel.UNCLEAR:
            return 0.9
        if snapshot.regime == RegimeLabel.COMPRESSION:
            return 0.85
        return 0.5

    def _evaluate_context(
        self,
        snapshot: MarketSnapshot,
        dataframe: dict[str, pd.DataFrame],
    ) -> ContextModuleResult:
        return evaluate_mtf_alignment(
            dataframe,
            entry_tf=self.entry_tf,
            confirm_tf=self.confirm_tf,
            trend_tf=self.trend_tf,
        )
