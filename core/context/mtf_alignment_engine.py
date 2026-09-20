"""Multi-timeframe trend and structure alignment."""

from __future__ import annotations

from typing import Optional

import pandas as pd

from config import Config
from core.context.context_types import ContextModuleResult
from core.candle_prep import prepare_df
from indicators.market_analyzer import MarketAnalyzer
from utils import safe_float


def _tf_bias(df: Optional[pd.DataFrame], analyzer: MarketAnalyzer) -> tuple[str, float]:
    """Return (BULLISH|BEARISH|NEUTRAL, strength 0-1) for one timeframe."""
    prepared = prepare_df(df, analyzer)
    if prepared is None or prepared.empty:
        return "NEUTRAL", 0.0

    row = prepared.iloc[-1]
    close = safe_float(row.get("close"))
    ema20 = safe_float(row.get("ema_20"))
    ema50 = safe_float(row.get("ema_50"))
    ema200 = safe_float(row.get("ema_200"))
    adx = safe_float(row.get("adx"))

    bullish = bool(row.get("trend_bullish")) or (
        close > ema200 and ema50 > ema200 and ema20 > ema50
    )
    bearish = bool(row.get("trend_bearish")) or (
        close < ema200 and ema50 < ema200 and ema20 < ema50
    )

    strength = min(max(adx / 40.0, 0.25), 1.0) if adx > 0 else 0.5
    if bullish and not bearish:
        return "BULLISH", strength
    if bearish and not bullish:
        return "BEARISH", strength
    return "NEUTRAL", 0.0


def _structure_bias(df: Optional[pd.DataFrame]) -> str:
    """Simple HH/HL vs LH/LL structure on confirm timeframe."""
    if df is None or len(df) < 10:
        return "NEUTRAL"
    window = df.iloc[-10:]
    highs = window["high"].values
    lows = window["low"].values
    mid = len(highs) // 2
    if mid < 2:
        return "NEUTRAL"

    first_high = float(max(highs[:mid]))
    second_high = float(max(highs[mid:]))
    first_low = float(min(lows[:mid]))
    second_low = float(min(lows[mid:]))

    if second_high > first_high and second_low > first_low:
        return "BULLISH"
    if second_high < first_high and second_low < first_low:
        return "BEARISH"
    return "NEUTRAL"


def evaluate_mtf_alignment(
    candles: dict[str, pd.DataFrame],
    *,
    entry_tf: str,
    confirm_tf: str,
    trend_tf: str,
) -> ContextModuleResult:
    """
    HTF trend + confirm structure must align with LTF entry direction.
    """
    module = "MTF_ALIGNMENT"
    analyzer = MarketAnalyzer()

    trend_bias, trend_strength = _tf_bias(candles.get(trend_tf), analyzer)
    confirm_bias, confirm_strength = _tf_bias(candles.get(confirm_tf), analyzer)
    entry_bias, entry_strength = _tf_bias(candles.get(entry_tf), analyzer)
    structure = _structure_bias(candles.get(confirm_tf))

    aligned_long = (
        trend_bias == "BULLISH"
        and confirm_bias in ("BULLISH", "NEUTRAL")
        and entry_bias in ("BULLISH", "NEUTRAL")
        and structure in ("BULLISH", "NEUTRAL")
    )
    aligned_short = (
        trend_bias == "BEARISH"
        and confirm_bias in ("BEARISH", "NEUTRAL")
        and entry_bias in ("BEARISH", "NEUTRAL")
        and structure in ("BEARISH", "NEUTRAL")
    )

    score = 0.0
    direction: str = "NEUTRAL"
    reasons: list[str] = []

    if aligned_long and not aligned_short:
        direction = "LONG"
        score = 58.0
        reasons.append("htf_ltf_long_align")
        if trend_bias == "BULLISH" and confirm_bias == "BULLISH":
            score += 12.0 * trend_strength
            reasons.append("trend_confirm_bull")
        if structure == "BULLISH":
            score += 8.0
            reasons.append("hh_hl_structure")
        if entry_bias == "BULLISH":
            score += 6.0 * entry_strength
            reasons.append("entry_tf_bull")
    elif aligned_short and not aligned_long:
        direction = "SHORT"
        score = 58.0
        reasons.append("htf_ltf_short_align")
        if trend_bias == "BEARISH" and confirm_bias == "BEARISH":
            score += 12.0 * trend_strength
            reasons.append("trend_confirm_bear")
        if structure == "BEARISH":
            score += 8.0
            reasons.append("lh_ll_structure")
        if entry_bias == "BEARISH":
            score += 6.0 * entry_strength
            reasons.append("entry_tf_bear")

    score = min(score, 92.0)
    bonus = 0.0
    multiplier = 1.0
    if score >= Config.CONTEXT_MODULE_MIN_SCORE:
        bonus = (score / 100.0) * Config.INSTITUTIONAL_CONTEXT_BONUS_PER_MODULE
        multiplier = 1.0 + min(
            (score - Config.CONTEXT_MODULE_MIN_SCORE) / 180.0,
            Config.INSTITUTIONAL_CONTEXT_MULT_BOOST,
        )

    return ContextModuleResult(
        module=module,
        direction=direction,  # type: ignore[arg-type]
        score=score,
        multiplier=multiplier,
        bonus=bonus,
        level_tags=["mtf_alignment"],
        metadata={
            "trend_bias": trend_bias,
            "confirm_bias": confirm_bias,
            "entry_bias": entry_bias,
            "structure": structure,
            "reasons": reasons,
        },
    )
