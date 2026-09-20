"""Breakout + retest setup evaluation."""

from __future__ import annotations

import pandas as pd

from config import Config
from strategies.pa_common import PaSetupResult, build_r_targets
from utils import safe_float


def _range_bounds(df: pd.DataFrame, lookback: int) -> tuple[float, float, float]:
    window = df.iloc[-lookback:]
    high = float(window["high"].max())
    low = float(window["low"].min())
    mid = (high + low) / 2.0
    return high, low, mid


def evaluate_breakout_retest(
    action: str,
    df: pd.DataFrame,
    price: float,
    atr: float,
) -> PaSetupResult:
    """Detect range consolidation, breakout, and retest of broken level."""
    result = PaSetupResult(direction=action.upper())
    if df.empty or len(df) < 30 or atr <= 0 or price <= 0:
        result.reasons.append("insufficient_data")
        return result

    lookback = min(36, len(df) - 5)
    range_high, range_low, range_mid = _range_bounds(df.iloc[:-3], lookback)
    range_width = range_high - range_low
    if range_width <= 0:
        result.reasons.append("invalid_range")
        return result

    latest = df.iloc[-1]
    prev = df.iloc[-2]
    action = action.upper()

    consolidation = range_width <= atr * 3.5
    adx = safe_float(latest.get("adx"))
    if not consolidation and adx > 28:
        result.reasons.append("not_consolidating")
        return result

    retest_tol = atr * 0.45
    body = abs(safe_float(latest["close"]) - safe_float(latest["open"]))

    if action == "LONG":
        broke = any(
            safe_float(row["close"]) > range_high and body_at >= atr * 0.35
            for row, body_at in (
                (df.iloc[-4], abs(safe_float(df.iloc[-4]["close"]) - safe_float(df.iloc[-4]["open"]))),
                (df.iloc[-3], abs(safe_float(df.iloc[-3]["close"]) - safe_float(df.iloc[-3]["open"]))),
                (prev, abs(safe_float(prev["close"]) - safe_float(prev["open"]))),
            )
        )
        retesting = (
            safe_float(latest["low"]) <= range_high + retest_tol
            and safe_float(latest["close"]) >= range_high - retest_tol * 0.5
            and price >= range_high - retest_tol
        )
        if not broke:
            result.reasons.append("no_breakout")
            return result
        if not retesting:
            result.reasons.append("no_retest")
            return result
        entry = price
        sl, tp1, tp2, tp3 = build_r_targets("LONG", entry, atr, sl_atr=1.0)
        sl = min(sl, range_low - atr * 0.15)
        result.key_levels = [range_high, range_mid, range_low]
        result.level_tags = ["breakout", "range_high", "retest"]
        result.confluence = "breakout_retest_long"
    else:
        broke = any(
            safe_float(row["close"]) < range_low and body_at >= atr * 0.35
            for row, body_at in (
                (df.iloc[-4], abs(safe_float(df.iloc[-4]["close"]) - safe_float(df.iloc[-4]["open"]))),
                (df.iloc[-3], abs(safe_float(df.iloc[-3]["close"]) - safe_float(df.iloc[-3]["open"]))),
                (prev, abs(safe_float(prev["close"]) - safe_float(prev["open"]))),
            )
        )
        retesting = (
            safe_float(latest["high"]) >= range_low - retest_tol
            and safe_float(latest["close"]) <= range_low + retest_tol * 0.5
            and price <= range_low + retest_tol
        )
        if not broke:
            result.reasons.append("no_breakout")
            return result
        if not retesting:
            result.reasons.append("no_retest")
            return result
        entry = price
        sl, tp1, tp2, tp3 = build_r_targets("SHORT", entry, atr, sl_atr=1.0)
        sl = max(sl, range_high + atr * 0.15)
        result.key_levels = [range_low, range_mid, range_high]
        result.level_tags = ["breakout", "range_low", "retest"]
        result.confluence = "breakout_retest_short"

    score = 66.0
    if consolidation:
        score += 8.0
    if body >= atr * 0.5:
        score += 6.0
    if bool(latest.get("vol_spike")):
        score += 5.0
    if adx >= 18:
        score += 4.0
    touch_quality = 1.0 - min(abs(price - (range_high if action == "LONG" else range_low)) / atr, 1.0)
    score += touch_quality * 8.0

    result.score = min(score, 94.0)
    result.passed = result.score >= Config.BREAKOUT_RETEST_MIN_SCORE
    result.entry = entry
    result.stop_loss = sl
    result.tp1 = tp1
    result.tp2 = tp2
    result.tp3 = tp3
    result.atr = atr
    if not result.passed:
        result.reasons.append(f"score_below_min_{result.score:.1f}")
    return result
