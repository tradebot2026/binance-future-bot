"""Swing Failure Pattern (false breakout) evaluation."""

from __future__ import annotations

import pandas as pd

from config import Config
from strategies.pa_common import PaSetupResult, build_r_targets
from utils import safe_float


def evaluate_false_breakout_sfp(
    action: str,
    df: pd.DataFrame,
    price: float,
    atr: float,
) -> PaSetupResult:
    """
    Detect SFP: sweep beyond range/swing boundary with close back inside.
    """
    result = PaSetupResult(direction=action.upper())
    if df.empty or len(df) < 20 or atr <= 0 or price <= 0:
        result.reasons.append("insufficient_data")
        return result

    lookback = min(32, len(df) - 2)
    boundary_window = df.iloc[-lookback:-1]
    range_high = float(boundary_window["high"].max())
    range_low = float(boundary_window["low"].min())

    latest = df.iloc[-1]
    action = action.upper()
    open_p = safe_float(latest["open"])
    close_p = safe_float(latest["close"])
    high_p = safe_float(latest["high"])
    low_p = safe_float(latest["low"])
    body = abs(close_p - open_p)

    if action == "LONG":
        swept = low_p < range_low - atr * 0.05
        reclaimed = close_p > range_low and close_p > open_p
        wick = min(open_p, close_p) - low_p
        if not swept:
            result.reasons.append("no_low_sweep")
            return result
        if not reclaimed:
            result.reasons.append("no_reclaim")
            return result
        entry = price
        sl, tp1, tp2, tp3 = build_r_targets("LONG", entry, atr, sl_atr=0.9)
        sl = min(sl, low_p - atr * 0.1)
        result.key_levels = [range_low, range_high, low_p]
        result.level_tags = ["sfp", "liquidity_sweep", "range_low"]
        result.confluence = "sfp_bullish"
        wick_score = min(wick / max(body, atr * 0.05), 3.0) * 4.0
    else:
        swept = high_p > range_high + atr * 0.05
        reclaimed = close_p < range_high and close_p < open_p
        wick = high_p - max(open_p, close_p)
        if not swept:
            result.reasons.append("no_high_sweep")
            return result
        if not reclaimed:
            result.reasons.append("no_reclaim")
            return result
        entry = price
        sl, tp1, tp2, tp3 = build_r_targets("SHORT", entry, atr, sl_atr=0.9)
        sl = max(sl, high_p + atr * 0.1)
        result.key_levels = [range_high, range_low, high_p]
        result.level_tags = ["sfp", "false_breakout", "range_high"]
        result.confluence = "sfp_bearish"
        wick_score = min(wick / max(body, atr * 0.05), 3.0) * 4.0

    score = 68.0 + wick_score
    if bool(latest.get("vol_spike")):
        score += 6.0
    if body >= atr * 0.35:
        score += 5.0
    rsi = safe_float(latest.get("rsi"))
    if action == "LONG" and rsi < 40:
        score += 4.0
    if action == "SHORT" and rsi > 60:
        score += 4.0

    result.score = min(score, 95.0)
    result.passed = result.score >= Config.FALSE_BREAKOUT_SFP_MIN_SCORE
    result.entry = entry
    result.stop_loss = sl
    result.tp1 = tp1
    result.tp2 = tp2
    result.tp3 = tp3
    result.atr = atr
    if not result.passed:
        result.reasons.append(f"score_below_min_{result.score:.1f}")
    return result
