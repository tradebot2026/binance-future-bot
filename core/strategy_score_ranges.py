"""Per-strategy Super / Hot / Normal score thresholds.

Override any strategy with env:
  {STRATEGY}_SUPER_SCORE=80
  {STRATEGY}_HOT_SCORE=70
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from config import Config
from constants import (
    ALL_STRATEGY_TAGS,
    STRATEGY_RANGE_REVERSION,
    STRATEGY_SMC_LEGACY,
    STRATEGY_SMC_TREND,
)


@dataclass(frozen=True)
class StrategyScoreRange:
    """Inclusive Hot band is [hot, super). Super is >= super. Normal is < hot."""

    strategy: str
    super_score: float
    hot_score: float

    def classify(self, score: float) -> str:
        if score >= self.super_score:
            return "SUPER"
        if score >= self.hot_score:
            return "HOT"
        return "NORMAL"

    def meets_hot(self, score: float) -> bool:
        return score >= self.hot_score

    def meets_super(self, score: float) -> bool:
        return score >= self.super_score


_DEFAULT_SUPER: dict[str, float] = {
    STRATEGY_SMC_TREND: 80.0,
    STRATEGY_SMC_LEGACY: 80.0,
    STRATEGY_RANGE_REVERSION: 75.0,
}

_DEFAULT_HOT: dict[str, float] = {
    STRATEGY_SMC_TREND: 70.0,
    STRATEGY_SMC_LEGACY: 70.0,
    STRATEGY_RANGE_REVERSION: 65.0,
}

_MIN_SCORE_ATTR: dict[str, str] = {
    "SMC_TREND": "STRATEGY_MIN_SCORE",
    "SMC_MULTITF": "STRATEGY_MIN_SCORE",
    "RANGE_REVERSION": "RANGE_MIN_SCORE",
    "LIQUIDITY_SWEEP_CONT": "LSC_MIN_SCORE",
    "VWAP_PULLBACK": "VWAP_MIN_SCORE",
    "VOLUME_PROFILE_BREAKOUT": "VPB_MIN_SCORE",
    "VOL_EXPANSION_MR": "VEMR_MIN_SCORE",
    "BREAKOUT_RETEST": "BREAKOUT_RETEST_MIN_SCORE",
    "FALSE_BREAKOUT_SFP": "FALSE_BREAKOUT_SFP_MIN_SCORE",
    "VOL_SQUEEZE": "VOL_SQUEEZE_MIN_SCORE",
    "TREND_MOMENTUM": "TREND_MOMENTUM_MIN_SCORE",
    "PRICE_ACTION_REVERSAL": "PRICE_ACTION_REVERSAL_MIN_SCORE",
    "MTF_ALIGNMENT": "MTF_ALIGNMENT_MIN_SCORE",
    "VP_KEYLEVEL": "VP_KEYLEVEL_MIN_SCORE",
    "ORDER_FLOW": "ORDER_FLOW_MIN_SCORE",
    "OI_FUNDING": "OI_FUNDING_MIN_SCORE",
}


def _env_float(key: str) -> Optional[float]:
    raw = os.getenv(key)
    if raw is None or not str(raw).strip():
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _hot_default(strategy: str) -> float:
    if strategy in _DEFAULT_HOT:
        return _DEFAULT_HOT[strategy]
    attr = _MIN_SCORE_ATTR.get(strategy, "STRATEGY_MIN_SCORE")
    return float(getattr(Config, attr, Config.STRATEGY_MIN_SCORE))


def _super_default(strategy: str, hot: float) -> float:
    if strategy in _DEFAULT_SUPER:
        return _DEFAULT_SUPER[strategy]
    return min(99.0, hot + 10.0)


def score_range_for(strategy: str) -> StrategyScoreRange:
    """Resolved Super/Hot floors for one strategy (Testnet relax applied)."""
    tag = str(strategy or "").upper()
    if tag == STRATEGY_SMC_LEGACY:
        tag = STRATEGY_SMC_TREND
    env_key = tag.replace(" ", "_")
    hot = _env_float(f"{env_key}_HOT_SCORE")
    super_score = _env_float(f"{env_key}_SUPER_SCORE")
    if hot is None:
        hot = _hot_default(tag)
    if super_score is None:
        super_score = _super_default(tag, hot)
    hot = Config.effective_min_score(hot)
    super_score = Config.effective_min_score(super_score)
    if super_score <= hot:
        super_score = min(99.0, hot + 5.0)
    return StrategyScoreRange(strategy=tag, super_score=super_score, hot_score=hot)


def classify_score(strategy: str, score: float) -> str:
    return score_range_for(strategy).classify(score)


def all_score_ranges() -> dict[str, StrategyScoreRange]:
    return {tag: score_range_for(tag) for tag in ALL_STRATEGY_TAGS}
