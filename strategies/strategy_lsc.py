"""Liquidity sweep continuation strategy module."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Set

from config import Config
from constants import STRATEGY_LIQUIDITY_SWEEP
from core.directional import pick_directional_candidate
from core.strategy_base import StrategyModule
from core.strategy_registry import strategy_enable_flag
from core.types import MarketSnapshot, RegimeLabel, SignalCandidate
from indicators.market_analyzer import MarketAnalyzer
from logger import signal_logger
from lsc_engine import evaluate_lsc_setup
from utils import safe_float

if TYPE_CHECKING:
    from database import DatabaseManager


class LiquiditySweepStrategy(StrategyModule):
    tag = STRATEGY_LIQUIDITY_SWEEP
    display_name = "Liquidity Sweep Continuation"

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
            RegimeLabel.STRONG_TREND.value,
            RegimeLabel.UNCLEAR.value,
            RegimeLabel.EXPANSION_SPIKE.value,
        }

    def regime_fit(self, snapshot: MarketSnapshot) -> float:
        if snapshot.regime == RegimeLabel.STRONG_TREND:
            return 1.0
        if snapshot.regime == RegimeLabel.EXPANSION_SPIKE:
            return 0.85
        if snapshot.regime == RegimeLabel.UNCLEAR:
            return 0.7
        return 0.2

    def max_concurrent_positions(self) -> int:
        return Config.MAX_LSC_POSITIONS

    def priority_weight(self) -> float:
        return Config.STRATEGY_PRIORITY_LSC

    def size_multiplier(self, score: float) -> float:
        if score >= Config.SCORE_FULL_SIZE:
            return 1.0
        if score >= Config.LSC_MIN_SCORE:
            return Config.HALF_SIZE_MULTIPLIER
        return 0.0

    def evaluate(self, snapshot: MarketSnapshot) -> Optional[SignalCandidate]:
        df_entry = snapshot.candles.get(self.entry_tf)
        df_confirm = snapshot.candles.get(self.confirm_tf)
        if df_entry is None or df_confirm is None or df_entry.empty:
            return None

        df_entry = self.analyzer.apply_all_indicators(df_entry)
        df_confirm = self.analyzer.apply_all_indicators(df_confirm)
        if df_entry.empty:
            return None

        price = snapshot.price if snapshot.price > 0 else float(df_entry.iloc[-1]["close"])
        long_c = self._eval_dir(snapshot, df_entry, df_confirm, "LONG", price)
        short_c = self._eval_dir(snapshot, df_entry, df_confirm, "SHORT", price)
        return pick_directional_candidate(
            snapshot.symbol,
            long_c,
            short_c,
            win_margin=Config.DIRECTION_WIN_MARGIN,
            resolve_margin=Config.DIRECTION_EQUILIBRIUM_MIN_MARGIN,
            min_score_fn=lambda _c, _m: Config.LSC_MIN_SCORE,
            enable_equilibrium_resolve=False,
        )

    def _eval_dir(
        self,
        snapshot: MarketSnapshot,
        df_entry,
        df_confirm,
        action: str,
        price: float,
    ) -> Optional[SignalCandidate]:
        atr = self.analyzer.get_latest_atr(df_entry)
        if atr <= 0:
            return None
        result = evaluate_lsc_setup(action, df_entry, df_confirm, price, atr)
        if not result.passed:
            if self.db:
                self.db.log_signal_rejection(
                    snapshot.symbol, action, result.score, result.reasons, strategy=self.tag
                )
            return None
        signal_logger.info(
            "APPROVED %s %s | strategy=%s | score=%.1f | sweep=%s",
            snapshot.symbol,
            action,
            self.tag,
            result.score,
            result.sweep_type,
        )
        return SignalCandidate(
            symbol=snapshot.symbol,
            action=action,  # type: ignore[arg-type]
            strategy=self.tag,
            score=result.score,
            price=price,
            atr=atr,
            timeframe=self.entry_tf,
            regime=snapshot.regime.value,
            confluence=result.sweep_type,
            structure_metadata={"volume_24h": snapshot.volume_24h, "sweep": result.sweep_type},
            regime_fit=self.regime_fit(snapshot),
            priority_weight=self.priority_weight(),
        )
