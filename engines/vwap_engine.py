"""Session VWAP pullback setup evaluation."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from config import Config
from utils import safe_float


@dataclass
class VwapResult:
    passed: bool = False
    score: float = 0.0
    reasons: list[str] = field(default_factory=list)
    vwap: float = 0.0
    distance_atr: float = 0.0


def _session_vwap(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    vol = df["volume"].replace(0, pd.NA)
    cum_vol = vol.cumsum()
    cum_pv = (typical * vol).cumsum()
    if cum_vol.iloc[-1] is pd.NA or safe_float(cum_vol.iloc[-1]) <= 0:
        return safe_float(df["close"].iloc[-1])
    return safe_float(cum_pv.iloc[-1] / cum_vol.iloc[-1])


def evaluate_vwap_pullback(
    action: str,
    df_entry: pd.DataFrame,
    df_trend: pd.DataFrame,
    price: float,
    atr: float,
) -> VwapResult:
    """Pullback to session VWAP in macro trend direction."""
    result = VwapResult()
    if df_entry.empty or atr <= 0 or price <= 0:
        result.reasons.append("insufficient_data")
        return result

    lookback = min(48, len(df_entry))
    window = df_entry.iloc[-lookback:]
    vwap = _session_vwap(window)
    result.vwap = vwap
    if vwap <= 0:
        result.reasons.append("vwap_unavailable")
        return result

    dist = abs(price - vwap)
    result.distance_atr = dist / atr if atr > 0 else 999.0
    action = action.upper()

    macro_bull = not df_trend.empty and bool(df_trend.iloc[-1].get("trend_bullish"))
    macro_bear = not df_trend.empty and bool(df_trend.iloc[-1].get("trend_bearish"))

    if action == "LONG":
        if not macro_bull:
            result.reasons.append("macro_not_bullish")
            return result
        if price < vwap:
            result.reasons.append("price_below_vwap")
            return result
        if result.distance_atr > 0.6:
            result.reasons.append("too_far_from_vwap")
            return result
    else:
        if not macro_bear:
            result.reasons.append("macro_not_bearish")
            return result
        if price > vwap:
            result.reasons.append("price_above_vwap")
            return result
        if result.distance_atr > 0.6:
            result.reasons.append("too_far_from_vwap")
            return result

    latest = df_entry.iloc[-1]
    rsi = safe_float(latest.get("rsi"))
    score = 68.0
    if result.distance_atr <= 0.25:
        score += 12.0
    elif result.distance_atr <= 0.4:
        score += 6.0
    if action == "LONG" and 40 <= rsi <= 55:
        score += 8.0
    if action == "SHORT" and 45 <= rsi <= 60:
        score += 8.0
    if bool(latest.get("vol_spike")):
        score += 5.0

    result.score = min(score, 92.0)
    result.passed = result.score >= Config.VWAP_MIN_SCORE
    if not result.passed:
        result.reasons.append(f"score_below_min_{result.score:.1f}")
    return result
