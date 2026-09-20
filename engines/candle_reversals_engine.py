"""Candle / price-action reversal pattern evaluation."""

from __future__ import annotations

import pandas as pd

from config import Config
from strategies.pa_common import PaSetupResult, build_r_targets
from utils import safe_float


def _at_key_level(
    price: float,
    level: float,
    atr: float,
    *,
    tolerance_atr: float = 0.35,
) -> bool:
    if level <= 0 or atr <= 0:
        return False
    return abs(price - level) <= atr * tolerance_atr


def evaluate_candle_reversal(
    action: str,
    df: pd.DataFrame,
    price: float,
    atr: float,
) -> PaSetupResult:
    """Pin bars, engulfing, and velocity reversals at swing levels."""
    result = PaSetupResult(direction=action.upper())
    if df.empty or len(df) < 5 or atr <= 0 or price <= 0:
        result.reasons.append("insufficient_data")
        return result

    latest = df.iloc[-1]
    prev = df.iloc[-2]
    action = action.upper()

    open_p = safe_float(latest["open"])
    close_p = safe_float(latest["close"])
    high_p = safe_float(latest["high"])
    low_p = safe_float(latest["low"])
    body = abs(close_p - open_p)
    upper_wick = high_p - max(open_p, close_p)
    lower_wick = min(open_p, close_p) - low_p

    swing_high = safe_float(latest.get("last_swing_high"))
    swing_low = safe_float(latest.get("last_swing_low"))

    prev_open = safe_float(prev["open"])
    prev_close = safe_float(prev["close"])
    prev_body = abs(prev_close - prev_open)

    pin_bar = False
    engulfing = False
    velocity = body >= atr * 0.7

    if action == "LONG":
        pin_bar = lower_wick >= max(body * 2.0, atr * 0.35) and close_p > open_p
        engulfing = (
            prev_close < prev_open
            and close_p > open_p
            and close_p >= prev_open
            and open_p <= prev_close
            and body > prev_body
        )
        at_level = _at_key_level(price, swing_low, atr) or _at_key_level(price, swing_low, atr, tolerance_atr=0.5)
        if not (pin_bar or engulfing or velocity):
            result.reasons.append("no_bullish_pattern")
            return result
        if not at_level and not _at_key_level(low_p, swing_low, atr):
            result.reasons.append("not_at_support")
            return result
        entry = price
        sl, tp1, tp2, tp3 = build_r_targets("LONG", entry, atr, sl_atr=0.85)
        sl = min(sl, low_p - atr * 0.08)
        result.key_levels = [level for level in (swing_low, low_p) if level > 0]
        result.level_tags = ["reversal", "swing_low"]
        if pin_bar:
            result.level_tags.append("pin_bar")
        if engulfing:
            result.level_tags.append("engulfing")
        if velocity:
            result.level_tags.append("displacement")
        result.confluence = "candle_rev_long"
    else:
        pin_bar = upper_wick >= max(body * 2.0, atr * 0.35) and close_p < open_p
        engulfing = (
            prev_close > prev_open
            and close_p < open_p
            and close_p <= prev_open
            and open_p >= prev_close
            and body > prev_body
        )
        at_level = _at_key_level(price, swing_high, atr) or _at_key_level(high_p, swing_high, atr)
        if not (pin_bar or engulfing or velocity):
            result.reasons.append("no_bearish_pattern")
            return result
        if not at_level and not _at_key_level(high_p, swing_high, atr):
            result.reasons.append("not_at_resistance")
            return result
        entry = price
        sl, tp1, tp2, tp3 = build_r_targets("SHORT", entry, atr, sl_atr=0.85)
        sl = max(sl, high_p + atr * 0.08)
        result.key_levels = [level for level in (swing_high, high_p) if level > 0]
        result.level_tags = ["reversal", "swing_high"]
        if pin_bar:
            result.level_tags.append("pin_bar")
        if engulfing:
            result.level_tags.append("engulfing")
        if velocity:
            result.level_tags.append("displacement")
        result.confluence = "candle_rev_short"

    score = 62.0
    if pin_bar:
        score += 12.0
    if engulfing:
        score += 14.0
    if velocity:
        score += 8.0
    if bool(latest.get("vol_spike")):
        score += 5.0
    rsi = safe_float(latest.get("rsi"))
    if action == "LONG" and rsi <= 38:
        score += 5.0
    if action == "SHORT" and rsi >= 62:
        score += 5.0

    result.score = min(score, 95.0)
    result.passed = result.score >= Config.PRICE_ACTION_REVERSAL_MIN_SCORE
    result.entry = entry
    result.stop_loss = sl
    result.tp1 = tp1
    result.tp2 = tp2
    result.tp3 = tp3
    result.atr = atr
    if not result.passed:
        result.reasons.append(f"score_below_min_{result.score:.1f}")
    return result
