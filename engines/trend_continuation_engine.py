"""Trend continuation / EMA stack + displacement evaluation."""

from __future__ import annotations

import pandas as pd

from config import Config
from strategies.pa_common import PaSetupResult, build_r_targets
from utils import safe_float


def evaluate_trend_continuation(
    action: str,
    df: pd.DataFrame,
    df_trend: pd.DataFrame,
    price: float,
    atr: float,
) -> PaSetupResult:
    """Score entries aligned with EMA 20/50/200 stack and displacement."""
    result = PaSetupResult(direction=action.upper())
    if df.empty or df_trend.empty or atr <= 0 or price <= 0:
        result.reasons.append("insufficient_data")
        return result

    latest = df.iloc[-1]
    trend_row = df_trend.iloc[-1]
    action = action.upper()

    ema20 = safe_float(latest.get("ema_20"))
    ema50 = safe_float(latest.get("ema_50"))
    ema200 = safe_float(latest.get("ema_200"))
    if min(ema20, ema50, ema200) <= 0:
        result.reasons.append("ema_unavailable")
        return result

    open_p = safe_float(latest["open"])
    close_p = safe_float(latest["close"])
    body = abs(close_p - open_p)
    displacement = body >= atr * 0.55

    adx = safe_float(latest.get("adx"))
    trend_adx = safe_float(trend_row.get("adx"))

    if action == "LONG":
        stack = ema20 > ema50 > ema200
        above = close_p > ema20 and price > ema50
        macro = bool(trend_row.get("trend_bullish")) or close_p > safe_float(trend_row.get("ema_200", 0))
        if not stack:
            result.reasons.append("ema_stack_bull_invalid")
            return result
        if not above:
            result.reasons.append("price_not_above_emas")
            return result
        if not displacement:
            result.reasons.append("no_displacement")
            return result
        entry = price
        sl, tp1, tp2, tp3 = build_r_targets("LONG", entry, atr, sl_atr=1.0)
        sl = min(sl, ema50 - atr * 0.1)
        result.key_levels = [ema20, ema50, ema200]
        result.level_tags = ["ema_stack", "displacement", "trend_continuation"]
        result.confluence = "trend_cont_long"
    else:
        stack = ema20 < ema50 < ema200
        below = close_p < ema20 and price < ema50
        macro = bool(trend_row.get("trend_bearish")) or close_p < safe_float(trend_row.get("ema_200", 0))
        if not stack:
            result.reasons.append("ema_stack_bear_invalid")
            return result
        if not below:
            result.reasons.append("price_not_below_emas")
            return result
        if not displacement:
            result.reasons.append("no_displacement")
            return result
        entry = price
        sl, tp1, tp2, tp3 = build_r_targets("SHORT", entry, atr, sl_atr=1.0)
        sl = max(sl, ema50 + atr * 0.1)
        result.key_levels = [ema20, ema50, ema200]
        result.level_tags = ["ema_stack", "displacement", "trend_continuation"]
        result.confluence = "trend_cont_short"

    score = 65.0
    if macro:
        score += 10.0
    if displacement:
        score += 8.0
    if adx >= 22 or trend_adx >= 22:
        score += 7.0
    if bool(latest.get("vol_spike")):
        score += 5.0
    pullback = abs(price - ema20) / atr
    if pullback <= 0.6:
        score += 5.0

    result.score = min(score, 94.0)
    result.passed = result.score >= Config.TREND_MOMENTUM_MIN_SCORE
    result.entry = entry
    result.stop_loss = sl
    result.tp1 = tp1
    result.tp2 = tp2
    result.tp3 = tp3
    result.atr = atr
    if not result.passed:
        result.reasons.append(f"score_below_min_{result.score:.1f}")
    return result
