"""Liquidity sweep continuation setup evaluation."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from config import Config
from utils import safe_float


@dataclass
class LscResult:
    passed: bool = False
    score: float = 0.0
    reasons: list[str] = field(default_factory=list)
    sweep_type: str = ""


def evaluate_lsc_setup(
    action: str,
    df_entry: pd.DataFrame,
    df_confirm: pd.DataFrame,
    price: float,
    atr: float,
) -> LscResult:
    """Detect liquidity sweep + continuation in trend direction."""
    result = LscResult()
    if df_entry.empty or len(df_entry) < 5 or atr <= 0 or price <= 0:
        result.reasons.append("insufficient_data")
        return result

    latest = df_entry.iloc[-1]
    prev = df_entry.iloc[-2]
    action = action.upper()

    if action == "LONG":
        swept = bool(latest.get("liq_sweep_bullish")) or bool(prev.get("liq_sweep_bullish"))
        continuation = bool(latest.get("bos_bullish")) or bool(latest.get("break_high"))
        trend_ok = bool(latest.get("trend_bullish")) or safe_float(latest.get("adx")) >= 18
        result.sweep_type = "bull_sweep"
    else:
        swept = bool(latest.get("liq_sweep_bearish")) or bool(prev.get("liq_sweep_bearish"))
        continuation = bool(latest.get("bos_bearish")) or bool(latest.get("break_low"))
        trend_ok = bool(latest.get("trend_bearish")) or safe_float(latest.get("adx")) >= 18
        result.sweep_type = "bear_sweep"

    if not swept:
        result.reasons.append("no_liquidity_sweep")
        return result
    if not continuation:
        result.reasons.append("no_continuation_bos")
        return result
    if not trend_ok:
        result.reasons.append("weak_trend")

    vol_ok = bool(latest.get("vol_spike")) or safe_float(latest.get("volume")) > safe_float(
        latest.get("vol_sma")
    )
    body = abs(safe_float(latest.get("close")) - safe_float(latest.get("open")))
    displacement = body >= atr * 0.4

    score = 62.0
    if vol_ok:
        score += 8.0
    if displacement:
        score += 10.0
    if trend_ok:
        score += 8.0
    if safe_float(latest.get("adx")) >= 22:
        score += 5.0

    result.score = min(score, 95.0)
    result.passed = result.score >= Config.LSC_MIN_SCORE
    if not result.passed:
        result.reasons.append(f"score_below_min_{result.score:.1f}")
    return result
