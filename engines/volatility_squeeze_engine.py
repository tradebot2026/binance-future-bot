"""Bollinger / Keltner squeeze and expansion evaluation."""

from __future__ import annotations

import pandas as pd

from config import Config
from strategies.pa_common import PaSetupResult, build_r_targets
from utils import safe_float


def _ensure_bb_kc(df: pd.DataFrame) -> pd.DataFrame:
    if "bb_upper" not in df.columns or "kc_upper" not in df.columns:
        import ta

        bb = ta.volatility.BollingerBands(df["close"], window=20, window_dev=2)
        df = df.copy()
        df["bb_upper"] = bb.bollinger_hband()
        df["bb_lower"] = bb.bollinger_lband()
        df["bb_mid"] = bb.bollinger_mavg()
        kc = ta.volatility.KeltnerChannel(
            high=df["high"], low=df["low"], close=df["close"], window=20
        )
        df["kc_upper"] = kc.keltner_channel_hband()
        df["kc_lower"] = kc.keltner_channel_lband()
    return df


def evaluate_volatility_squeeze(
    action: str,
    df: pd.DataFrame,
    price: float,
    atr: float,
) -> PaSetupResult:
    """Squeeze (BB inside KC) followed by expansion breakout."""
    result = PaSetupResult(direction=action.upper())
    if df.empty or len(df) < 25 or atr <= 0 or price <= 0:
        result.reasons.append("insufficient_data")
        return result

    df = _ensure_bb_kc(df)
    latest = df.iloc[-1]
    action = action.upper()

    squeeze_bars = 0
    for i in range(-6, -1):
        row = df.iloc[i]
        bb_u = safe_float(row.get("bb_upper"))
        bb_l = safe_float(row.get("bb_lower"))
        kc_u = safe_float(row.get("kc_upper"))
        kc_l = safe_float(row.get("kc_lower"))
        if bb_u <= kc_u and bb_l >= kc_l:
            squeeze_bars += 1

    if squeeze_bars < 2:
        result.reasons.append("no_squeeze")
        return result

    bb_u = safe_float(latest.get("bb_upper"))
    bb_l = safe_float(latest.get("bb_lower"))
    bb_mid = safe_float(latest.get("bb_mid"))
    close_p = safe_float(latest["close"])
    bb_width = (bb_u - bb_l) / max(bb_mid, 1e-9)

    prev_widths = []
    for i in range(-8, -1):
        row = df.iloc[i]
        mid = safe_float(row.get("bb_mid"))
        if mid > 0:
            prev_widths.append(
                (safe_float(row.get("bb_upper")) - safe_float(row.get("bb_lower"))) / mid
            )
    avg_width = sum(prev_widths) / len(prev_widths) if prev_widths else bb_width
    expanding = bb_width > avg_width * 1.15

    if not expanding:
        result.reasons.append("no_expansion")
        return result

    if action == "LONG":
        if close_p <= bb_u:
            result.reasons.append("no_upper_break")
            return result
        entry = price
        sl, tp1, tp2, tp3 = build_r_targets("LONG", entry, atr, sl_atr=1.1)
        sl = min(sl, bb_mid)
        result.key_levels = [bb_u, bb_mid, bb_l]
        result.level_tags = ["squeeze", "bb_breakout", "expansion"]
        result.confluence = "vol_squeeze_long"
    else:
        if close_p >= bb_l:
            result.reasons.append("no_lower_break")
            return result
        entry = price
        sl, tp1, tp2, tp3 = build_r_targets("SHORT", entry, atr, sl_atr=1.1)
        sl = max(sl, bb_mid)
        result.key_levels = [bb_l, bb_mid, bb_u]
        result.level_tags = ["squeeze", "bb_breakdown", "expansion"]
        result.confluence = "vol_squeeze_short"

    score = 64.0 + squeeze_bars * 3.0
    if expanding:
        score += 8.0
    if bool(latest.get("vol_spike")):
        score += 7.0
    body = abs(safe_float(latest["close"]) - safe_float(latest["open"]))
    if body >= atr * 0.45:
        score += 6.0

    result.score = min(score, 93.0)
    result.passed = result.score >= Config.VOL_SQUEEZE_MIN_SCORE
    result.entry = entry
    result.stop_loss = sl
    result.tp1 = tp1
    result.tp2 = tp2
    result.tp3 = tp3
    result.atr = atr
    if not result.passed:
        result.reasons.append(f"score_below_min_{result.score:.1f}")
    return result
