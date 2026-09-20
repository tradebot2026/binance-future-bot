"""Order flow microstructure — volume delta, aggression, absorption."""

from __future__ import annotations

import pandas as pd

from config import Config
from core.context.context_types import ContextModuleResult
from utils import safe_float


def _bar_delta(row: pd.Series) -> float:
    """Approximate buy/sell delta from candle geometry."""
    high = safe_float(row.get("high"))
    low = safe_float(row.get("low"))
    close = safe_float(row.get("close"))
    volume = safe_float(row.get("volume"))
    span = max(high - low, 1e-12)
    buy_ratio = (close - low) / span
    return volume * (2.0 * buy_ratio - 1.0)


def evaluate_order_flow_imbalance(
    df: pd.DataFrame,
    price: float,
    atr: float,
    *,
    key_levels: list[float] | None = None,
) -> ContextModuleResult:
    """
    Detect cumulative volume delta imbalance, aggressive market orders,
    and absorption at key levels.
    """
    module = "ORDER_FLOW"
    lookback = min(Config.ORDER_FLOW_LOOKBACK_BARS, len(df))
    if lookback < 15 or atr <= 0 or price <= 0:
        return ContextModuleResult(module=module)

    window = df.iloc[-lookback:]
    deltas = [_bar_delta(row) for _, row in window.iterrows()]
    cum_delta = sum(deltas)
    total_vol = sum(safe_float(row.get("volume")) for _, row in window.iterrows())
    if total_vol <= 0:
        return ContextModuleResult(module=module)

    delta_ratio = cum_delta / total_vol
    latest = window.iloc[-1]
    prev = window.iloc[-2] if len(window) >= 2 else latest

    body = abs(safe_float(latest.get("close")) - safe_float(latest.get("open")))
    range_ = max(
        safe_float(latest.get("high")) - safe_float(latest.get("low")),
        1e-12,
    )
    body_pct = body / range_
    vol = safe_float(latest.get("volume"))
    vol_sma = safe_float(latest.get("vol_sma"))
    if vol_sma <= 0:
        vol_sma = window["volume"].rolling(20).mean().iloc[-1]
    vol_spike = vol > vol_sma * Config.ORDER_FLOW_VOL_SPIKE_MULT if vol_sma > 0 else False

    aggressive_long = (
        body_pct >= Config.ORDER_FLOW_AGGRESSIVE_BODY_PCT
        and safe_float(latest.get("close")) > safe_float(latest.get("open"))
        and vol_spike
        and delta_ratio > Config.ORDER_FLOW_DELTA_THRESHOLD
    )
    aggressive_short = (
        body_pct >= Config.ORDER_FLOW_AGGRESSIVE_BODY_PCT
        and safe_float(latest.get("close")) < safe_float(latest.get("open"))
        and vol_spike
        and delta_ratio < -Config.ORDER_FLOW_DELTA_THRESHOLD
    )

    absorption = False
    tolerance = atr * 0.25
    levels = key_levels or []
    if vol_spike and range_ < atr * Config.ORDER_FLOW_ABSORPTION_RANGE_ATR:
        for level in levels:
            if abs(price - safe_float(level)) <= tolerance:
                absorption = True
                break

    score = 0.0
    direction: str = "NEUTRAL"
    reasons: list[str] = []

    if delta_ratio >= Config.ORDER_FLOW_DELTA_THRESHOLD:
        direction = "LONG"
        score = 55.0 + min(abs(delta_ratio) * 40.0, 25.0)
        reasons.append("positive_delta")
    elif delta_ratio <= -Config.ORDER_FLOW_DELTA_THRESHOLD:
        direction = "SHORT"
        score = 55.0 + min(abs(delta_ratio) * 40.0, 25.0)
        reasons.append("negative_delta")

    if aggressive_long and direction in ("LONG", "NEUTRAL"):
        direction = "LONG"
        score = max(score, 68.0) + 8.0
        reasons.append("aggressive_buying")
    elif aggressive_short and direction in ("SHORT", "NEUTRAL"):
        direction = "SHORT"
        score = max(score, 68.0) + 8.0
        reasons.append("aggressive_selling")

    if absorption:
        score += 10.0
        reasons.append("absorption_at_level")
        if safe_float(latest.get("close")) >= safe_float(prev.get("close")):
            direction = direction if direction != "NEUTRAL" else "LONG"
        else:
            direction = direction if direction != "NEUTRAL" else "SHORT"

    score = min(score, 90.0)
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
        key_levels=list(levels[:5]),
        level_tags=["order_flow", "delta"],
        metadata={
            "cum_delta": cum_delta,
            "delta_ratio": delta_ratio,
            "vol_spike": vol_spike,
            "absorption": absorption,
            "reasons": reasons,
        },
    )
