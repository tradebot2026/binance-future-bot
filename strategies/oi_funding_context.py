"""Open interest + funding context strategy module."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Set

import pandas as pd

from config import Config
from constants import STRATEGY_OI_FUNDING
from core.context.context_types import ContextModuleResult
from core.context.oi_funding_context_engine import evaluate_oi_funding_context
from core.context.institutional_context import InstitutionalContextEvaluator
from core.types import MarketSnapshot, RegimeLabel
from strategies.context_strategy_base import ContextStrategyBase
from strategies.pa_common import prepare_df

if TYPE_CHECKING:
    from exchange import BinanceExchangeManager


class OiFundingContextStrategy(ContextStrategyBase):
    tag = STRATEGY_OI_FUNDING
    display_name = "Open Interest + Funding"
    _min_score_attr = "OI_FUNDING_MIN_SCORE"
    _max_positions_attr = "MAX_OI_FUNDING_POSITIONS"
    _priority_attr = "STRATEGY_PRIORITY_OI_FUNDING"

    def __init__(self, exchange: Optional["BinanceExchangeManager"] = None) -> None:
        super().__init__()
        self._exchange = exchange
        self._ctx_helper = InstitutionalContextEvaluator(exchange=exchange)

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
            return 0.9
        return 0.85

    def _evaluate_context(
        self,
        snapshot: MarketSnapshot,
        dataframe: dict[str, pd.DataFrame],
    ) -> ContextModuleResult:
        derivatives = self._resolve_derivatives(snapshot)
        df_entry = prepare_df(dataframe.get(self.entry_tf), self.analyzer)
        price_change = InstitutionalContextEvaluator._price_change_pct(df_entry)
        return evaluate_oi_funding_context(derivatives, price_change)

    def _resolve_derivatives(self, snapshot: MarketSnapshot) -> dict[str, Any]:
        if snapshot.derivatives:
            return dict(snapshot.derivatives)
        if self._exchange is not None:
            return self._exchange.fetch_derivatives_context(snapshot.symbol)
        return {}
