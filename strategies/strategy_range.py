"""Range mean-reversion strategy module."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Set

from config import Config
from constants import STRATEGY_RANGE_REVERSION
from core.directional import pick_directional_candidate
from core.strategy_base import StrategyModule
from core.strategy_registry import strategy_enable_flag
from core.types import MarketSnapshot, RegimeLabel, SignalCandidate
from indicators.market_analyzer import MarketAnalyzer
from logger import signal_logger
from range_engine import evaluate_range_setup
from utils import safe_float

if TYPE_CHECKING:
    from database import DatabaseManager


class RangeStrategy(StrategyModule):
    tag = STRATEGY_RANGE_REVERSION
    display_name = "Range Reversion"

    def __init__(self, db: Optional["DatabaseManager"] = None) -> None:
        self.db = db
        self.analyzer = MarketAnalyzer()
        self.entry_tf = Config.ENTRY_TIMEFRAME
        self.confirm_tf = Config.CONFIRM_TIMEFRAME
        self.trend_tf = Config.TREND_TIMEFRAME

    def is_enabled(self) -> bool:
        return strategy_enable_flag(self.tag)

    def default_timeframes(self) -> tuple[str, ...]:
        return (self.entry_tf, self.confirm_tf, self.trend_tf)

    def allowed_regimes(self) -> Set[str]:
        return {
            RegimeLabel.RANGE_CHOP.value,
            RegimeLabel.UNCLEAR.value,
        }

    def regime_fit(self, snapshot: MarketSnapshot) -> float:
        if snapshot.regime == RegimeLabel.RANGE_CHOP:
            return 1.0
        if snapshot.regime == RegimeLabel.UNCLEAR:
            return 0.75
        return 0.25

    def max_concurrent_positions(self) -> int:
        return Config.MAX_RANGE_POSITIONS

    def priority_weight(self) -> float:
        return Config.STRATEGY_PRIORITY_RANGE

    def size_multiplier(self, score: float) -> float:
        return Config.RANGE_SIZE_MULTIPLIER if score >= Config.RANGE_MIN_SCORE else 0.0

    def evaluate(self, snapshot: MarketSnapshot) -> Optional[SignalCandidate]:
        df_entry = snapshot.candles.get(self.entry_tf)
        df_confirm = snapshot.candles.get(self.confirm_tf)
        df_trend = snapshot.candles.get(self.trend_tf)
        if df_entry is None or df_confirm is None or df_trend is None:
            return None
        if df_entry.empty or df_confirm.empty or df_trend.empty:
            return None

        df_entry = self.analyzer.apply_all_indicators(df_entry)
        df_confirm = self.analyzer.apply_all_indicators(df_confirm)
        df_trend = self.analyzer.apply_all_indicators(df_trend)
        if df_entry.empty or df_confirm.empty or df_trend.empty:
            return None

        price = snapshot.price if snapshot.price > 0 else float(df_entry.iloc[-1]["close"])
        long_c = self._evaluate_direction(
            snapshot.symbol, df_entry, df_confirm, df_trend, "LONG", price, snapshot
        )
        short_c = self._evaluate_direction(
            snapshot.symbol, df_entry, df_confirm, df_trend, "SHORT", price, snapshot
        )

        def _range_min_score(_confluence: str, _macro: str) -> float:
            return Config.RANGE_MIN_SCORE

        return pick_directional_candidate(
            snapshot.symbol,
            long_c,
            short_c,
            win_margin=Config.DIRECTION_EQUILIBRIUM_MIN_MARGIN,
            resolve_margin=Config.DIRECTION_EQUILIBRIUM_MIN_MARGIN,
            min_score_fn=_range_min_score,
            enable_equilibrium_resolve=False,
        )

    def _evaluate_direction(
        self,
        symbol: str,
        df_entry,
        df_confirm,
        df_trend,
        action: str,
        price: float,
        snapshot: MarketSnapshot,
    ) -> Optional[SignalCandidate]:
        atr = self.analyzer.get_latest_atr(df_entry)
        if atr <= 0 or price <= 0:
            return None

        result = evaluate_range_setup(action, df_entry, df_trend, df_confirm, price, atr)
        if not result.passed:
            if self.db:
                self.db.log_signal_rejection(
                    symbol,
                    action,
                    result.score,
                    result.reasons,
                    strategy=self.tag,
                )
            return None

        signal_logger.info(
            "APPROVED %s %s | strategy=%s | score=%.1f | edge=%s",
            symbol,
            action,
            self.tag,
            result.score,
            result.metadata.edge,
        )
        fit = self.regime_fit(snapshot)
        meta = result.metadata.to_dict()
        meta["volume_24h"] = snapshot.volume_24h
        return SignalCandidate(
            symbol=symbol,
            action=action,  # type: ignore[arg-type]
            strategy=self.tag,
            score=result.score,
            price=price,
            atr=atr,
            timeframe=self.entry_tf,
            regime=snapshot.regime.value,
            confluence=result.metadata.edge,
            macro_trend=result.metadata.macro_trend,
            structure_metadata=meta,
            regime_fit=fit,
            priority_weight=self.priority_weight(),
        )
