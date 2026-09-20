"""Shared helpers for price-action strategy modules."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional

import pandas as pd

from config import Config
from core.candle_prep import prepare_df, resolve_price as _resolve_price
from core.types import MarketSnapshot, StrategyResult

Action = Literal["LONG", "SHORT"]


@dataclass
class PaSetupResult:
    passed: bool = False
    score: float = 0.0
    direction: str = "NEUTRAL"
    reasons: list[str] = field(default_factory=list)
    entry: float = 0.0
    stop_loss: float = 0.0
    tp1: float = 0.0
    tp2: float = 0.0
    tp3: float = 0.0
    key_levels: list[float] = field(default_factory=list)
    level_tags: list[str] = field(default_factory=list)
    confluence: str = ""
    atr: float = 0.0


def build_r_targets(
    action: Action,
    entry: float,
    atr: float,
    *,
    sl_atr: float = 1.0,
    tp1_r: float = 1.0,
    tp2_r: float = 2.0,
    tp3_r: float = 3.0,
) -> tuple[float, float, float, float]:
    """Return (stop_loss, tp1, tp2, tp3) from ATR-multiple risk."""
    risk = max(atr * sl_atr, entry * 0.001)
    if action == "LONG":
        sl = entry - risk
        return sl, entry + risk * tp1_r, entry + risk * tp2_r, entry + risk * tp3_r
    sl = entry + risk
    return sl, entry - risk * tp1_r, entry - risk * tp2_r, entry - risk * tp3_r


def pick_directional_setup(
    long_setup: PaSetupResult,
    short_setup: PaSetupResult,
    *,
    min_score: float,
    win_margin: float = 5.0,
) -> Optional[PaSetupResult]:
    long_score = long_setup.score if long_setup.passed else 0.0
    short_score = short_setup.score if short_setup.passed else 0.0
    if long_score < min_score and short_score < min_score:
        return None
    if long_score >= min_score and short_score < min_score:
        return long_setup
    if short_score >= min_score and long_score < min_score:
        return short_setup
    if abs(long_score - short_score) < win_margin:
        return None
    return long_setup if long_score > short_score else short_setup


def setup_to_result(
    symbol: str,
    strategy: str,
    setup: PaSetupResult,
    *,
    timeframe: str,
    snapshot: MarketSnapshot,
    macro_trend: str = "NEUTRAL",
) -> StrategyResult:
    confidence = min(max(setup.score / 100.0, 0.0), 1.0)
    return StrategyResult(
        symbol=symbol.upper(),
        strategy=strategy,
        direction=setup.direction,  # type: ignore[arg-type]
        score=setup.score,
        confidence=confidence,
        atr=setup.atr,
        timeframe=timeframe,
        key_levels=list(setup.key_levels),
        level_tags=list(setup.level_tags),
        confluence=setup.confluence,
        macro_trend=macro_trend,
        structure_metadata={
            "entry": setup.entry,
            "stop_loss": setup.stop_loss,
            "take_profit_1": setup.tp1,
            "take_profit_2": setup.tp2,
            "take_profit_3": setup.tp3,
            "atr": setup.atr,
            "key_levels": list(setup.key_levels),
            "level_tags": list(setup.level_tags),
            "reasons": setup.reasons,
            "volume_24h": snapshot.volume_24h,
        },
    )


def resolve_price(snapshot: MarketSnapshot, df: pd.DataFrame) -> float:
    return _resolve_price(snapshot, df)
