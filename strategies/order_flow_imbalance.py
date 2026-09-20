"""Order flow imbalance strategy module."""

from __future__ import annotations

from typing import Set

import pandas as pd

from config import Config
from constants import STRATEGY_ORDER_FLOW
from core.context.context_types import ContextModuleResult
from core.context.order_flow_imbalance_engine import evaluate_order_flow_imbalance
from core.context.volume_profile_engine import compute_volume_profile
from core.types import MarketSnapshot, RegimeLabel
from strategies.context_strategy_base import ContextStrategyBase
from strategies.pa_common import prepare_df, resolve_price


class OrderFlowImbalanceStrategy(ContextStrategyBase):
    tag = STRATEGY_ORDER_FLOW
    display_name = "Order Flow Imbalance"
    _min_score_attr = "ORDER_FLOW_MIN_SCORE"
    _max_positions_attr = "MAX_ORDER_FLOW_POSITIONS"
    _priority_attr = "STRATEGY_PRIORITY_ORDER_FLOW"

    def allowed_regimes(self) -> Set[str]:
        return {
            RegimeLabel.EXPANSION_SPIKE.value,
            RegimeLabel.STRONG_TREND.value,
            RegimeLabel.UNCLEAR.value,
        }

    def regime_fit(self, snapshot: MarketSnapshot) -> float:
        if snapshot.regime == RegimeLabel.EXPANSION_SPIKE:
            return 1.0
        if snapshot.regime == RegimeLabel.STRONG_TREND:
            return 0.95
        return 0.85

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

        lookback = min(Config.VP_CONTEXT_LOOKBACK_BARS, len(df_entry))
        profile = compute_volume_profile(df_entry.iloc[-lookback:])
        key_levels = [profile.poc, profile.vah, profile.val]
        key_levels.extend(profile.hvn_levels[:3])

        return evaluate_order_flow_imbalance(
            df_entry,
            price,
            atr,
            key_levels=[v for v in key_levels if v > 0],
        )
