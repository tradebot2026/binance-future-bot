"""Volume profile breakout strategy module."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Set

from config import Config
from constants import STRATEGY_VP_BREAKOUT
from core.directional import pick_directional_candidate
from core.strategy_base import StrategyModule
from core.strategy_registry import strategy_enable_flag
from core.types import MarketSnapshot, RegimeLabel, SignalCandidate
from indicators.market_analyzer import MarketAnalyzer
from logger import signal_logger
from vp_breakout_engine import evaluate_vp_breakout

if TYPE_CHECKING:
    from database import DatabaseManager


class VpBreakoutStrategy(StrategyModule):
    tag = STRATEGY_VP_BREAKOUT
    display_name = "Volume Profile Breakout"

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
            RegimeLabel.COMPRESSION.value,
            RegimeLabel.UNCLEAR.value,
        }

    def regime_fit(self, snapshot: MarketSnapshot) -> float:
        if snapshot.regime == RegimeLabel.COMPRESSION:
            return 1.0
        if snapshot.regime == RegimeLabel.STRONG_TREND:
            return 0.9
        if snapshot.regime == RegimeLabel.UNCLEAR:
            return 0.75
        return 0.2

    def requires_top_volume(self) -> bool:
        return True

    def top_volume_limit(self) -> int:
        return Config.VP_BREAKOUT_TOP_VOLUME_LIMIT

    def max_concurrent_positions(self) -> int:
        return Config.MAX_VPB_POSITIONS

    def priority_weight(self) -> float:
        return Config.STRATEGY_PRIORITY_VPB

    def size_multiplier(self, score: float) -> float:
        if score >= Config.SCORE_FULL_SIZE:
            return 1.0
        if score >= Config.VPB_MIN_SCORE:
            return Config.HALF_SIZE_MULTIPLIER
        return 0.0

    def evaluate(self, snapshot: MarketSnapshot) -> Optional[SignalCandidate]:
        if not snapshot.is_top_volume:
            return None

        df_entry = snapshot.candles.get(self.entry_tf)
        if df_entry is None or df_entry.empty:
            return None

        df_entry = self.analyzer.apply_all_indicators(df_entry)
        if df_entry.empty or len(df_entry) < 2:
            return None

        price = snapshot.price if snapshot.price > 0 else float(df_entry.iloc[-1]["close"])
        long_c = self._eval_dir(snapshot, df_entry, "LONG", price)
        short_c = self._eval_dir(snapshot, df_entry, "SHORT", price)
        return pick_directional_candidate(
            snapshot.symbol,
            long_c,
            short_c,
            win_margin=Config.DIRECTION_WIN_MARGIN,
            resolve_margin=Config.DIRECTION_EQUILIBRIUM_MIN_MARGIN,
            min_score_fn=lambda _c, _m: Config.VPB_MIN_SCORE,
            enable_equilibrium_resolve=False,
        )

    def _eval_dir(
        self,
        snapshot: MarketSnapshot,
        df_entry,
        action: str,
        price: float,
    ) -> Optional[SignalCandidate]:
        atr = self.analyzer.get_latest_atr(df_entry)
        if atr <= 0:
            return None
        result = evaluate_vp_breakout(action, df_entry, price, atr)
        if not result.passed:
            if self.db:
                self.db.log_signal_rejection(
                    snapshot.symbol, action, result.score, result.reasons, strategy=self.tag
                )
            return None
        signal_logger.info(
            "APPROVED %s %s | strategy=%s | score=%.1f | poc=%.6f",
            snapshot.symbol,
            action,
            self.tag,
            result.score,
            result.poc,
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
            confluence="vp_breakout",
            structure_metadata={
                "poc": result.poc,
                "vah": result.vah,
                "val": result.val,
                "volume_24h": snapshot.volume_24h,
                "volume_rank": snapshot.volume_rank,
            },
            regime_fit=self.regime_fit(snapshot),
            priority_weight=self.priority_weight(),
        )
