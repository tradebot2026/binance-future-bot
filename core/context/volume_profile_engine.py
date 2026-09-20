"""Volume profile — POC, value area, HVN/LVN detection."""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from config import Config
from core.context.context_types import ContextModuleResult
from utils import safe_float


@dataclass
class VolumeProfileLevels:
    poc: float = 0.0
    vah: float = 0.0
    val: float = 0.0
    hvn_levels: list[float] = field(default_factory=list)
    lvn_levels: list[float] = field(default_factory=list)
    bin_centers: list[float] = field(default_factory=list)
    bin_volumes: list[float] = field(default_factory=list)


def compute_volume_profile(
    df: pd.DataFrame,
    *,
    bins: int | None = None,
    value_area_pct: float = 0.70,
) -> VolumeProfileLevels:
    """Build volume-at-price profile with POC, value area, and HVN/LVN nodes."""
    result = VolumeProfileLevels()
    bin_count = bins or Config.VP_CONTEXT_BINS
    if len(df) < 20:
        return result

    low = float(df["low"].min())
    high = float(df["high"].max())
    if high <= low:
        return result

    edges = np.linspace(low, high, bin_count + 1)
    vol_at_price = np.zeros(bin_count)
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    span = high - low

    for tp, vol in zip(typical, df["volume"]):
        idx = int((float(tp) - low) / span * (bin_count - 1))
        idx = max(0, min(bin_count - 1, idx))
        vol_at_price[idx] += safe_float(vol)

    centers = [(edges[i] + edges[i + 1]) / 2.0 for i in range(bin_count)]
    result.bin_centers = [float(c) for c in centers]
    result.bin_volumes = [float(v) for v in vol_at_price]

    total = vol_at_price.sum()
    if total <= 0:
        return result

    poc_idx = int(np.argmax(vol_at_price))
    result.poc = float(centers[poc_idx])

    target = total * value_area_pct
    acc = vol_at_price[poc_idx]
    lo_i = hi_i = poc_idx
    while acc < target and (lo_i > 0 or hi_i < bin_count - 1):
        vol_lo = vol_at_price[lo_i - 1] if lo_i > 0 else -1.0
        vol_hi = vol_at_price[hi_i + 1] if hi_i < bin_count - 1 else -1.0
        if vol_hi >= vol_lo and hi_i < bin_count - 1:
            hi_i += 1
            acc += vol_at_price[hi_i]
        elif lo_i > 0:
            lo_i -= 1
            acc += vol_at_price[lo_i]
        else:
            break

    result.val = float(edges[lo_i])
    result.vah = float(edges[hi_i + 1])

    mean_vol = float(np.mean(vol_at_price))
    std_vol = float(np.std(vol_at_price))
    hvn_thresh = mean_vol + std_vol * Config.VP_HVN_STD_MULT
    lvn_thresh = max(mean_vol - std_vol * Config.VP_LVN_STD_MULT, 0.0)

    for idx, vol in enumerate(vol_at_price):
        center = centers[idx]
        if vol >= hvn_thresh:
            result.hvn_levels.append(float(center))
        elif vol <= lvn_thresh and vol > 0:
            result.lvn_levels.append(float(center))

    return result


def evaluate_volume_profile_context(
    df: pd.DataFrame,
    price: float,
    atr: float,
) -> ContextModuleResult:
    """
    Score proximity to POC/HVN/LVN and value-area boundaries.
    LONG bias near VAL/POC support; SHORT bias near VAH/POC resistance.
    """
    module = "VP_KEYLEVEL"
    lookback = min(Config.VP_CONTEXT_LOOKBACK_BARS, len(df))
    if lookback < 20 or atr <= 0 or price <= 0:
        return ContextModuleResult(module=module)

    profile = compute_volume_profile(df.iloc[-lookback:])
    if profile.poc <= 0:
        return ContextModuleResult(module=module)

    tolerance = atr * Config.VP_KEYLEVEL_ATR_TOLERANCE
    key_levels = [profile.poc, profile.vah, profile.val]
    key_levels.extend(profile.hvn_levels[:3])
    level_tags = ["poc", "vah", "val"]
    level_tags.extend(["hvn"] * min(len(profile.hvn_levels), 3))

    score = 0.0
    direction: str = "NEUTRAL"
    reasons: list[str] = []

    near_val = abs(price - profile.val) <= tolerance
    near_poc_support = price >= profile.poc - tolerance and price <= profile.poc + tolerance
    near_vah = abs(price - profile.vah) <= tolerance
    in_value = profile.val <= price <= profile.vah

    if near_val or (near_poc_support and price <= profile.poc):
        direction = "LONG"
        score = 62.0
        reasons.append("val_poc_support")
        if near_val:
            score += 8.0
        if profile.hvn_levels and any(abs(price - h) <= tolerance for h in profile.hvn_levels):
            score += 6.0
            reasons.append("hvn_support")
    elif near_vah or (near_poc_support and price >= profile.poc):
        direction = "SHORT"
        score = 62.0
        reasons.append("vah_poc_resistance")
        if near_vah:
            score += 8.0
        if profile.hvn_levels and any(abs(price - h) <= tolerance for h in profile.hvn_levels):
            score += 6.0
            reasons.append("hvn_resistance")
    elif in_value:
        if price < profile.poc:
            direction = "LONG"
            score = 55.0
            reasons.append("below_poc_in_value")
        elif price > profile.poc:
            direction = "SHORT"
            score = 55.0
            reasons.append("above_poc_in_value")

    if profile.lvn_levels:
        lvn_near = any(abs(price - lv) <= tolerance for lv in profile.lvn_levels)
        if lvn_near:
            score += 4.0
            reasons.append("lvn_magnet")

    score = min(score, 88.0)
    bonus = 0.0
    multiplier = 1.0
    if score >= Config.CONTEXT_MODULE_MIN_SCORE:
        bonus = (score / 100.0) * Config.INSTITUTIONAL_CONTEXT_BONUS_PER_MODULE
        multiplier = 1.0 + min(
            (score - Config.CONTEXT_MODULE_MIN_SCORE) / 200.0,
            Config.INSTITUTIONAL_CONTEXT_MULT_BOOST,
        )

    return ContextModuleResult(
        module=module,
        direction=direction,  # type: ignore[arg-type]
        score=score,
        multiplier=multiplier,
        bonus=bonus,
        key_levels=key_levels,
        level_tags=level_tags + ["volume_profile"],
        metadata={
            "poc": profile.poc,
            "vah": profile.vah,
            "val": profile.val,
            "hvn_count": len(profile.hvn_levels),
            "lvn_count": len(profile.lvn_levels),
            "reasons": reasons,
        },
    )


def volume_profile_levels(df: pd.DataFrame, bins: int = 24) -> tuple[float, float, float]:
    """Backward-compatible POC/VAH/VAL tuple for vp_breakout_engine."""
    profile = compute_volume_profile(df, bins=bins)
    return profile.poc, profile.vah, profile.val
