"""Volatility expansion mean-reversion setup evaluation."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from config import Config
from utils import safe_float


@dataclass
class VolExpansionResult:
    passed: bool = False
    score: float = 0.0
    reasons: list[str] = field(default_factory=list)
    extension_atr: float = 0.0


def evaluate_vol_expansion_mr(
    action: str,
    df_entry: pd.DataFrame,
    df_confirm: pd.DataFrame,
    price: float,
    atr: float,
) -> VolExpansionResult:
    """Fade overextended move after volatility expansion spike."""
    result = VolExpansionResult()
    if df_entry.empty or len(df_entry) < 25 or atr <= 0 or price <= 0:
        result.reasons.append("insufficient_data")
        return result

    atr_series = df_entry["atr"].dropna()
    if len(atr_series) < 20:
        result.reasons.append("atr_history_short")
        return result

    current_atr = safe_float(atr_series.iloc[-1])
    median_atr = safe_float(atr_series.tail(20).median())
    if median_atr <= 0:
        result.reasons.append("atr_median_zero")
        return result

    expansion = current_atr >= median_atr * 1.35
    if not expansion:
        result.reasons.append("no_vol_expansion")
        return result

    ema20 = safe_float(df_entry.iloc[-1].get("ema_20"))
    if ema20 <= 0:
        result.reasons.append("ema_unavailable")
        return result

    result.extension_atr = abs(price - ema20) / atr
    action = action.upper()

    if action == "LONG":
        if price >= ema20:
            result.reasons.append("not_extended_down")
            return result
        if result.extension_atr < 1.2:
            result.reasons.append("insufficient_extension")
            return result
        rsi = safe_float(df_entry.iloc[-1].get("rsi"))
        if rsi > 35:
            result.reasons.append("rsi_not_oversold_enough")
            return result
    else:
        if price <= ema20:
            result.reasons.append("not_extended_up")
            return result
        if result.extension_atr < 1.2:
            result.reasons.append("insufficient_extension")
            return result
        rsi = safe_float(df_entry.iloc[-1].get("rsi"))
        if rsi < 65:
            result.reasons.append("rsi_not_overbought_enough")
            return result

    adx_15m = safe_float(df_confirm.iloc[-1].get("adx")) if not df_confirm.empty else 0.0
    if adx_15m > 30:
        result.reasons.append("trend_too_strong_for_mr")

    score = 64.0
    if result.extension_atr >= 1.8:
        score += 12.0
    elif result.extension_atr >= 1.4:
        score += 6.0
    if current_atr >= median_atr * 1.6:
        score += 8.0
    if adx_15m <= 25:
        score += 6.0

    result.score = min(score, 90.0)
    result.passed = result.score >= Config.VEMR_MIN_SCORE and adx_15m <= 30
    if not result.passed:
        result.reasons.append(f"score_or_adx_fail_{result.score:.1f}")
    return result
