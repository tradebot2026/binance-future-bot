"""Volume profile key level strategy module."""

from __future__ import annotations

from typing import Set

import pandas as pd

from config import Config
from constants import STRATEGY_VP_KEYLEVEL
from core.context.context_types import ContextModuleResult
from core.context.volume_profile_engine import evaluate_volume_profile_context
from core.types import MarketSnapshot, RegimeLabel
from strategies.context_strategy_base import ContextStrategyBase
from strategies.pa_common import prepare_df, resolve_price


class VolumeProfileStrategy(ContextStrategyBase):
    tag = STRATEGY_VP_KEYLEVEL
    display_name = "Volume Profile Key Level"
    _min_score_attr = "VP_KEYLEVEL_MIN_SCORE"
    _max_positions_attr = "MAX_VP_KEYLEVEL_POSITIONS"
    _priority_attr = "STRATEGY_PRIORITY_VP_KEYLEVEL"

    def allowed_regimes(self) -> Set[str]:
        return {
            RegimeLabel.RANGE_CHOP.value,
            RegimeLabel.UNCLEAR.value,
            RegimeLabel.COMPRESSION.value,
        }

    def regime_fit(self, snapshot: MarketSnapshot) -> float:
        if snapshot.regime == RegimeLabel.RANGE_CHOP:
            return 1.0
        if snapshot.regime == RegimeLabel.UNCLEAR:
            return 0.9
        if snapshot.regime == RegimeLabel.COMPRESSION:
            return 0.85
        return 0.6

    def _evaluate_context(
        self,
        snapshot: MarketSnapshot,
        dataframe: dict[str, pd.DataFrame],
    ) -> ContextModuleResult:
        df_entry = prepare_df(dataframe.get(self.entry_tf), self.analyzer)
        if df_entry is None:
            return ContextModuleResult(module=self.tag)
        price = resolve_price(snapshot, df_entry)
        atr = self.analyzer.get_latest_atr(df_entry)
        return evaluate_volume_profile_context(df_entry, price, atr)
