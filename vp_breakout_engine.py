"""Volume profile breakout setup evaluation."""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from config import Config
from utils import safe_float


@dataclass
class VpBreakoutResult:
    passed: bool = False
    score: float = 0.0
    reasons: list[str] = field(default_factory=list)
    poc: float = 0.0
    vah: float = 0.0
    val: float = 0.0


def _volume_profile_levels(df: pd.DataFrame, bins: int = 24) -> tuple[float, float, float]:
    if len(df) < 20:
        return 0.0, 0.0, 0.0
    low = float(df["low"].min())
    high = float(df["high"].max())
    if high <= low:
        return 0.0, 0.0, 0.0

    edges = np.linspace(low, high, bins + 1)
    vol_at_price = np.zeros(bins)
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    for tp, vol in zip(typical, df["volume"]):
        idx = int((float(tp) - low) / (high - low) * (bins - 1))
        idx = max(0, min(bins - 1, idx))
        vol_at_price[idx] += safe_float(vol)

    poc_idx = int(np.argmax(vol_at_price))
    poc = float((edges[poc_idx] + edges[poc_idx + 1]) / 2.0)
    total = vol_at_price.sum()
    if total <= 0:
        return poc, high, low

    target = total * 0.70
    acc = vol_at_price[poc_idx]
    lo_i = hi_i = poc_idx
    while acc < target and (lo_i > 0 or hi_i < bins - 1):
        vol_lo = vol_at_price[lo_i - 1] if lo_i > 0 else -1
        vol_hi = vol_at_price[hi_i + 1] if hi_i < bins - 1 else -1
        if vol_hi >= vol_lo and hi_i < bins - 1:
            hi_i += 1
            acc += vol_at_price[hi_i]
        elif lo_i > 0:
            lo_i -= 1
            acc += vol_at_price[lo_i]
        else:
            break

    val = float(edges[lo_i])
    vah = float(edges[hi_i + 1])
    return poc, vah, val


def evaluate_vp_breakout(
    action: str,
    df_entry: pd.DataFrame,
    price: float,
    atr: float,
) -> VpBreakoutResult:
    """Breakout above VAH or below VAL with volume confirmation."""
    result = VpBreakoutResult()
    lookback = min(Config.RANGE_LOOKBACK_BARS, len(df_entry))
    if lookback < 20 or atr <= 0 or price <= 0:
        result.reasons.append("insufficient_data")
        return result

    window = df_entry.iloc[-lookback:]
    poc, vah, val = _volume_profile_levels(window)
    result.poc, result.vah, result.val = poc, vah, val
    if vah <= val <= 0:
        result.reasons.append("profile_unavailable")
        return result

    latest = df_entry.iloc[-1]
    prev = df_entry.iloc[-2]
    action = action.upper()
    buffer = atr * 0.1

    if action == "LONG":
        broke = price > vah + buffer and safe_float(prev.get("close")) <= vah
        if not broke:
            result.reasons.append("no_vah_breakout")
            return result
    else:
        broke = price < val - buffer and safe_float(prev.get("close")) >= val
        if not broke:
            result.reasons.append("no_val_breakdown")
            return result

    vol_ok = bool(latest.get("vol_spike")) or safe_float(latest.get("volume")) > safe_float(
        latest.get("vol_sma")
    ) * 1.2
    if not vol_ok:
        result.reasons.append("volume_not_confirming")

    score = 70.0
    if vol_ok:
        score += 12.0
    range_width = vah - val
    if range_width > 0 and range_width / price * 100 >= 0.5:
        score += 8.0
    if safe_float(latest.get("adx")) >= 20:
        score += 5.0

    result.score = min(score, 94.0)
    result.passed = result.score >= Config.VPB_MIN_SCORE and vol_ok
    if not result.passed and "score_below" not in str(result.reasons):
        result.reasons.append(f"score_below_min_{result.score:.1f}")
    return result
