"""
Range / mean-reversion regime engine for sideways markets.
Activates when 1h ADX is low and SMC yields no trend setups.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from config import Config
from constants import STRATEGY_RANGE_REVERSION
from smc_engine import resolve_confirm_trend, resolve_macro_trend
from utils import safe_float


@dataclass
class RangeMetadata:
    range_high: float = 0.0
    range_low: float = 0.0
    equilibrium: float = 0.0
    adx_1h: float = 0.0
    adx_15m: float = 0.0
    macro_trend: str = "NEUTRAL"
    confirm_trend: str = "NEUTRAL"
    edge: str = ""
    regime_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "range_high": self.range_high,
            "range_low": self.range_low,
            "equilibrium": self.equilibrium,
            "adx_1h": self.adx_1h,
            "adx_15m": self.adx_15m,
            "macro_trend": self.macro_trend,
            "confirm_trend": self.confirm_trend,
            "edge": self.edge,
            "regime_reason": self.regime_reason,
        }


@dataclass
class RangeGateResult:
    passed: bool
    reasons: list[str] = field(default_factory=list)
    metadata: RangeMetadata = field(default_factory=RangeMetadata)
    score: float = 0.0


def compute_range_boundaries(df_trend: pd.DataFrame) -> tuple[float, float, float]:
    lookback = min(Config.RANGE_LOOKBACK_BARS, len(df_trend))
    if lookback < 10:
        return 0.0, 0.0, 0.0
    window = df_trend.iloc[-lookback:]
    range_high = float(window["high"].max())
    range_low = float(window["low"].min())
    if range_high <= range_low:
        return 0.0, 0.0, 0.0
    equilibrium = (range_high + range_low) / 2.0
    return range_low, range_high, equilibrium


def detect_range_regime(
    df_trend: pd.DataFrame,
    df_confirm: pd.DataFrame,
) -> tuple[bool, str, RangeMetadata]:
    meta = RangeMetadata()
    if df_trend.empty:
        return False, "missing_trend_data", meta

    latest_trend = df_trend.iloc[-1]
    meta.adx_1h = safe_float(latest_trend.get("adx"))
    meta.macro_trend = resolve_macro_trend(df_trend, df_confirm)
    meta.confirm_trend = resolve_confirm_trend(df_confirm) if not df_confirm.empty else "NEUTRAL"

    if not df_confirm.empty:
        meta.adx_15m = safe_float(df_confirm.iloc[-1].get("adx"))

    if meta.adx_1h > Config.RANGE_REGIME_MAX_ADX_1H:
        return False, f"adx_1h_{meta.adx_1h:.1f}_above_{Config.RANGE_REGIME_MAX_ADX_1H}", meta

    range_low, range_high, equilibrium = compute_range_boundaries(df_trend)
    if range_low <= 0 or range_high <= 0:
        return False, "invalid_range_boundaries", meta

    meta.range_low = range_low
    meta.range_high = range_high
    meta.equilibrium = equilibrium
    meta.regime_reason = "range_regime_active"
    return True, meta.regime_reason, meta


def evaluate_range_setup(
    action: str,
    df_entry: pd.DataFrame,
    df_trend: pd.DataFrame,
    df_confirm: pd.DataFrame,
    price: float,
    atr: float,
) -> RangeGateResult:
    """Mean-reversion at range edges when 1h market is ranging."""
    reasons: list[str] = []
    regime_ok, regime_reason, base_meta = detect_range_regime(df_trend, df_confirm)
    meta = base_meta

    if not regime_ok:
        return RangeGateResult(False, [regime_reason], meta)

    if action not in ("LONG", "SHORT"):
        return RangeGateResult(False, ["invalid_action"], meta)

    if atr <= 0 or price <= 0:
        return RangeGateResult(False, ["invalid_atr_or_price"], meta)

    edge_tol = atr * Config.RANGE_EDGE_ATR_TOLERANCE
    range_size = meta.range_high - meta.range_low
    if range_size <= 0:
        return RangeGateResult(False, ["zero_range_size"], meta)

    score = 60.0
    if meta.adx_1h <= Config.RANGE_REGIME_MAX_ADX_1H - 4:
        score += 5.0
    if meta.macro_trend == "NEUTRAL":
        score += 5.0

    latest = df_entry.iloc[-1] if not df_entry.empty else {}
    rsi = safe_float(latest.get("rsi"), 50.0)

    if action == "LONG":
        near_low = price <= meta.range_low + edge_tol
        if not near_low:
            reasons.append(
                f"long_requires_range_low price={price:.6f} low={meta.range_low:.6f}"
            )
        else:
            meta.edge = "RANGE_LOW"
            score += 10.0
            if rsi <= 42:
                score += 5.0
            if meta.confirm_trend in ("LONG", "NEUTRAL"):
                score += 5.0
    else:
        near_high = price >= meta.range_high - edge_tol
        if not near_high:
            reasons.append(
                f"short_requires_range_high price={price:.6f} high={meta.range_high:.6f}"
            )
        else:
            meta.edge = "RANGE_HIGH"
            score += 10.0
            if rsi >= 58:
                score += 5.0
            if meta.confirm_trend in ("SHORT", "NEUTRAL"):
                score += 5.0

    room_to_eq = abs(price - meta.equilibrium)
    min_room = atr * Config.MIN_OPPOSING_RR * 0.5
    if room_to_eq < min_room:
        reasons.append(f"insufficient_room_to_equilibrium_{room_to_eq:.6f}")

    if score < Config.RANGE_MIN_SCORE:
        reasons.append(f"score_below_min_{score:.1f}_required_{Config.RANGE_MIN_SCORE:.1f}")

    passed = len(reasons) == 0
    return RangeGateResult(passed, reasons, meta, min(score, 100.0))


def compute_range_sl_tp(
    action: str,
    entry_price: float,
    atr: float,
    meta: RangeMetadata,
) -> tuple[float, float, float, float]:
    """Tight range SL beyond boundary; TPs toward equilibrium and opposite edge."""
    buffer = atr * Config.SL_BUFFER_ATR
    max_dist = atr * Config.MAX_SL_ATR_MULTIPLIER

    if action == "LONG":
        boundary_sl = meta.range_low - buffer
        sl = min(boundary_sl, entry_price - buffer)
        sl = max(sl, entry_price - max_dist)
        tp1 = meta.equilibrium
        tp2 = meta.equilibrium + (meta.range_high - meta.equilibrium) * 0.5
        tp3 = meta.range_high - buffer
    else:
        boundary_sl = meta.range_high + buffer
        sl = max(boundary_sl, entry_price + buffer)
        sl = min(sl, entry_price + max_dist)
        tp1 = meta.equilibrium
        tp2 = meta.equilibrium - (meta.equilibrium - meta.range_low) * 0.5
        tp3 = meta.range_low + buffer

    r = abs(entry_price - sl)
    if r > 0:
        if action == "LONG":
            tp1 = max(tp1, entry_price + r * Config.TP1_R_MULTIPLE)
        else:
            tp1 = min(tp1, entry_price - r * Config.TP1_R_MULTIPLE)

    return sl, tp1, tp2, tp3


def strategy_tag() -> str:
    return STRATEGY_RANGE_REVERSION
