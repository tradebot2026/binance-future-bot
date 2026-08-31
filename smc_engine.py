"""
SMC structure analysis, confluence gates, and retest-based entry validation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd

from config import Config
from utils import safe_float


@dataclass
class StructureMetadata:
    """Actionable structure levels persisted with each trade/signal."""

    macro_trend: str = "NEUTRAL"
    confirm_trend: str = "NEUTRAL"
    confluence_type: str = ""
    sweep_level: float = 0.0
    ob_zone_low: float = 0.0
    ob_zone_high: float = 0.0
    fvg_zone_low: float = 0.0
    fvg_zone_high: float = 0.0
    equilibrium: float = 0.0
    opposing_liquidity: float = 0.0
    setup_type: str = ""
    bos_signal: bool = False
    choch_signal: bool = False
    volume_bonus: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "macro_trend": self.macro_trend,
            "confirm_trend": self.confirm_trend,
            "confluence_type": self.confluence_type,
            "sweep_level": self.sweep_level,
            "ob_zone_low": self.ob_zone_low,
            "ob_zone_high": self.ob_zone_high,
            "fvg_zone_low": self.fvg_zone_low,
            "fvg_zone_high": self.fvg_zone_high,
            "equilibrium": self.equilibrium,
            "opposing_liquidity": self.opposing_liquidity,
            "setup_type": self.setup_type,
            "bos_signal": self.bos_signal,
            "choch_signal": self.choch_signal,
            "volume_bonus": self.volume_bonus,
        }


@dataclass
class GateResult:
    passed: bool
    reasons: list[str] = field(default_factory=list)
    structure: StructureMetadata = field(default_factory=StructureMetadata)


def _recent_bars(df: pd.DataFrame, n: int = 12) -> pd.DataFrame:
    if df.empty:
        return df
    return df.iloc[-n:].copy()


def _price_in_zone(price: float, low: float, high: float, tolerance: float) -> bool:
    if low <= 0 or high <= 0:
        return False
    zone_low = min(low, high) - tolerance
    zone_high = max(low, high) + tolerance
    return zone_low <= price <= zone_high


def compute_premium_discount(df_trend: pd.DataFrame, price: float) -> tuple[float, str]:
    """Return (equilibrium, zone) where zone is DISCOUNT, PREMIUM, or EQUILIBRIUM."""
    lookback = min(Config.PD_LOOKBACK_BARS, len(df_trend))
    if lookback < 10:
        return 0.0, "EQUILIBRIUM"
    window = df_trend.iloc[-lookback:]
    range_high = float(window["high"].max())
    range_low = float(window["low"].min())
    if range_high <= range_low:
        return 0.0, "EQUILIBRIUM"
    equilibrium = (range_high + range_low) / 2.0
    if price < equilibrium:
        return equilibrium, "DISCOUNT"
    if price > equilibrium:
        return equilibrium, "PREMIUM"
    return equilibrium, "EQUILIBRIUM"


def resolve_confirm_trend(df_confirm: pd.DataFrame) -> str:
    if df_confirm.empty:
        return "NEUTRAL"
    latest = df_confirm.iloc[-1]
    if bool(latest.get("trend_bullish")):
        return "LONG"
    if bool(latest.get("trend_bearish")):
        return "SHORT"
    return "NEUTRAL"


def resolve_macro_trend(df_trend: pd.DataFrame, df_confirm: pd.DataFrame) -> str:
    if df_trend.empty:
        return "NEUTRAL"
    trend_latest = df_trend.iloc[-1]
    confirm_trend = resolve_confirm_trend(df_confirm)

    macro = "NEUTRAL"
    if bool(trend_latest.get("trend_bullish")):
        macro = "LONG"
    elif bool(trend_latest.get("trend_bearish")):
        macro = "SHORT"

    if macro == "LONG" and confirm_trend == "SHORT":
        return "NEUTRAL"
    if macro == "SHORT" and confirm_trend == "LONG":
        return "NEUTRAL"
    return macro


def _find_active_fvg_zone(df: pd.DataFrame, bullish: bool) -> tuple[float, float]:
    """Most recent valid FVG zone in lookback (for retest detection)."""
    recent = _recent_bars(df, Config.RETEST_LOOKBACK_BARS)
    for idx in range(len(recent) - 1, 1, -1):
        row = recent.iloc[idx]
        if bullish and bool(row.get("fvg_bullish_raw")):
            gap_low = safe_float(recent.iloc[idx - 2]["high"] if idx >= 2 else row.get("high"))
            gap_high = safe_float(row.get("low"))
            if gap_high > gap_low:
                return gap_low, gap_high
        if not bullish and bool(row.get("fvg_bearish_raw")):
            gap_high = safe_float(recent.iloc[idx - 2]["low"] if idx >= 2 else row.get("low"))
            gap_low = safe_float(row.get("high"))
            if gap_high > gap_low:
                return gap_low, gap_high
    return 0.0, 0.0


def _find_ob_zone(df: pd.DataFrame, bullish: bool) -> tuple[float, float]:
    """Order block zone from candle before displacement."""
    recent = _recent_bars(df, Config.RETEST_LOOKBACK_BARS)
    for idx in range(len(recent) - 1, 0, -1):
        row = recent.iloc[idx]
        if bullish and bool(row.get("ob_bullish_formed")):
            prev = recent.iloc[idx - 1]
            return min(safe_float(prev.get("open")), safe_float(prev.get("close"))), max(
                safe_float(prev.get("open")), safe_float(prev.get("close"))
            )
        if not bullish and bool(row.get("ob_bearish_formed")):
            prev = recent.iloc[idx - 1]
            return min(safe_float(prev.get("open")), safe_float(prev.get("close"))), max(
                safe_float(prev.get("open")), safe_float(prev.get("close"))
            )
    return 0.0, 0.0


def _find_sweep_level(df: pd.DataFrame, bullish: bool) -> float:
    recent = _recent_bars(df, Config.RETEST_LOOKBACK_BARS)
    for idx in range(len(recent) - 1, -1, -1):
        row = recent.iloc[idx]
        if bullish and bool(row.get("liq_sweep_bullish")):
            return safe_float(row.get("low"))
        if not bullish and bool(row.get("liq_sweep_bearish")):
            return safe_float(row.get("high"))
    return 0.0


def resolve_entry_trend(df_entry: pd.DataFrame) -> str:
    if df_entry.empty:
        return "NEUTRAL"
    latest = df_entry.iloc[-1]
    if bool(latest.get("trend_bullish")):
        return "LONG"
    if bool(latest.get("trend_bearish")):
        return "SHORT"
    return "NEUTRAL"


def is_pd_zone_acceptable(
    action: str,
    pd_zone: str,
    price: float,
    equilibrium: float,
    range_high: float,
    range_low: float,
) -> tuple[bool, str]:
    """Premium/discount filter with optional equilibrium tolerance band."""
    if action == "LONG":
        if pd_zone == "DISCOUNT":
            return True, "pd_discount"
        if pd_zone == "PREMIUM":
            return False, f"premium_discount_long_requires_discount got_{pd_zone}"
    else:
        if pd_zone == "PREMIUM":
            return True, "pd_premium"
        if pd_zone == "DISCOUNT":
            return False, f"premium_discount_short_requires_premium got_{pd_zone}"

    if not Config.ALLOW_PD_EQUILIBRIUM or equilibrium <= 0 or range_high <= range_low:
        return False, f"premium_discount_{action.lower()}_requires_strict_zone got_{pd_zone}"

    range_size = range_high - range_low
    tolerance = range_size * (Config.PD_EQUILIBRIUM_TOLERANCE_PCT / 100.0)
    if action == "LONG" and price <= equilibrium + tolerance:
        return True, "pd_equilibrium_tolerance_long"
    if action == "SHORT" and price >= equilibrium - tolerance:
        return True, "pd_equilibrium_tolerance_short"
    return False, f"premium_discount_{action.lower()}_outside_tolerance got_{pd_zone}"


def check_mtf_alignment(
    action: str,
    macro: str,
    confirm: str,
    entry_trend: str = "NEUTRAL",
) -> tuple[bool, str]:
    if macro in ("LONG", "SHORT"):
        if macro != action:
            return False, f"macro_{macro}_opposes_{action}"
        return True, "macro_aligned"

    if confirm == action:
        return True, "confirm_aligned_neutral_macro"

    if Config.ALLOW_NEUTRAL_MACRO_SETUPS and entry_trend == action:
        if confirm == "NEUTRAL" or confirm == action:
            return True, "entry_aligned_neutral_macro"
        return False, f"confirm_{confirm}_opposes_{action}"

    if confirm == "NEUTRAL":
        return False, "neutral_macro_requires_15m_or_5m_structure"
    if confirm != action:
        return False, f"confirm_{confirm}_opposes_{action}"
    return True, "confirm_aligned_neutral_macro"


def evaluate_confluence_gate(
    action: str,
    df_entry: pd.DataFrame,
    df_trend: pd.DataFrame,
    df_confirm: pd.DataFrame,
    price: float,
    atr: float,
) -> GateResult:
    """Hard gate: real SMC setup + premium/discount + MTF + retest tap."""
    reasons: list[str] = []
    structure = StructureMetadata()

    if action not in ("LONG", "SHORT"):
        return GateResult(False, ["invalid_action"], structure)

    macro = resolve_macro_trend(df_trend, df_confirm)
    confirm = resolve_confirm_trend(df_confirm)
    entry_trend = resolve_entry_trend(df_entry)
    structure.macro_trend = macro
    structure.confirm_trend = confirm

    mtf_ok, mtf_reason = check_mtf_alignment(action, macro, confirm, entry_trend)
    if not mtf_ok:
        reasons.append(mtf_reason)

    lookback = min(Config.PD_LOOKBACK_BARS, len(df_trend))
    range_high = range_low = equilibrium = 0.0
    if lookback >= 10:
        window = df_trend.iloc[-lookback:]
        range_high = float(window["high"].max())
        range_low = float(window["low"].min())
    equilibrium, pd_zone = compute_premium_discount(df_trend, price)
    structure.equilibrium = equilibrium

    pd_ok, pd_reason = is_pd_zone_acceptable(
        action, pd_zone, price, equilibrium, range_high, range_low
    )
    if not pd_ok:
        reasons.append(pd_reason)

    bullish = action == "LONG"
    tolerance = atr * Config.RETEST_ZONE_ATR_TOLERANCE
    nearby_tolerance = tolerance * Config.STRUCTURE_NEARBY_ATR_MULT

    sweep_level = _find_sweep_level(df_entry, bullish)
    fvg_low, fvg_high = _find_active_fvg_zone(df_entry, bullish)
    ob_low, ob_high = _find_ob_zone(df_entry, bullish)

    structure.sweep_level = sweep_level
    structure.fvg_zone_low = fvg_low
    structure.fvg_zone_high = fvg_high
    structure.ob_zone_low = ob_low
    structure.ob_zone_high = ob_high

    latest = df_entry.iloc[-1]
    structure.bos_signal = bool(
        latest.get("bos_bullish") if bullish else latest.get("bos_bearish")
    )
    structure.choch_signal = bool(
        latest.get("choch_bullish") if bullish else latest.get("choch_bearish")
    )
    structure.volume_bonus = bool(latest.get("vol_spike"))

    def _zone_hit(
        present: bool,
        low: float,
        high: float,
        strict_tol: float,
        nearby_tol: float,
    ) -> bool:
        if not present:
            return False
        if low == high and low > 0:
            return _price_in_zone(price, low, high, strict_tol) or _price_in_zone(
                price, low, high, nearby_tol
            )
        return _price_in_zone(price, low, high, strict_tol) or _price_in_zone(
            price, low, high, nearby_tol
        )

    sweep_retest = sweep_level > 0 and _zone_hit(
        True, sweep_level, sweep_level, tolerance, nearby_tolerance
    )
    fvg_retest = (fvg_low > 0 and fvg_high > 0) and _zone_hit(
        True, fvg_low, fvg_high, tolerance, nearby_tolerance
    )
    ob_retest = (ob_low > 0 and ob_high > 0) and _zone_hit(
        True, ob_low, ob_high, tolerance, nearby_tolerance
    )

    confluence_hits: list[str] = []
    if sweep_retest:
        confluence_hits.append("SWEEP_RETEST")
    if fvg_retest:
        confluence_hits.append("FVG_RETEST")
    if ob_retest:
        confluence_hits.append("OB_RETEST")

    if not confluence_hits:
        reasons.append("no_confluence_retest")
    else:
        structure.confluence_type = "+".join(confluence_hits)
        structure.setup_type = confluence_hits[0]

    if bullish:
        structure.opposing_liquidity = safe_float(latest.get("last_swing_high"))
    else:
        structure.opposing_liquidity = safe_float(latest.get("last_swing_low"))

    passed = len(reasons) == 0
    return GateResult(passed, reasons, structure)


def score_setup(
    action: str,
    latest: dict[str, Any],
    previous: dict[str, Any],
    structure: StructureMetadata,
    macro_trend: str,
) -> float:
    """Soft score — volume is bonus only; BOS/CHoCH separated."""
    score = 0.0
    bullish = action == "LONG"

    if bullish and latest.get("trend_bullish"):
        score += 15.0
    if not bullish and latest.get("trend_bearish"):
        score += 15.0
    if bullish and safe_float(latest.get("ema_20")) > safe_float(latest.get("ema_50")):
        score += 8.0
    if not bullish and safe_float(latest.get("ema_20")) < safe_float(latest.get("ema_50")):
        score += 8.0
    if safe_float(latest.get("adx")) >= 20.0:
        score += 5.0

    rsi = safe_float(latest.get("rsi"), 50.0)
    if bullish and 38 <= rsi <= 62:
        score += 8.0
    if not bullish and 38 <= rsi <= 58:
        score += 8.0

    if bullish and safe_float(latest.get("macd")) > safe_float(latest.get("macd_signal")):
        score += 8.0
    if not bullish and safe_float(latest.get("macd")) < safe_float(latest.get("macd_signal")):
        score += 8.0

    if structure.setup_type == "SWEEP_RETEST":
        score += 18.0
    elif structure.setup_type == "FVG_RETEST":
        score += 14.0
    elif structure.setup_type == "OB_RETEST":
        score += 14.0

    if structure.bos_signal and macro_trend == action:
        score += 10.0
    if structure.choch_signal and structure.setup_type == "SWEEP_RETEST":
        score += 8.0

    if structure.volume_bonus:
        score += Config.VOLUME_BONUS_POINTS

    if macro_trend == action:
        score += 5.0
    elif macro_trend != "NEUTRAL" and macro_trend != action:
        score = max(score - 8.0, 0.0)

    if structure.confirm_trend == action and macro_trend == "NEUTRAL":
        score += 5.0

    return min(score, 100.0)


def validate_retest_entry(
    action: str,
    live_price: float,
    structure: StructureMetadata,
    atr: float,
) -> tuple[bool, str]:
    """Ensure live price taps structure zone and has not chased too far."""
    tolerance = atr * Config.RETEST_ZONE_ATR_TOLERANCE
    nearby_tolerance = tolerance * Config.STRUCTURE_NEARBY_ATR_MULT

    zones: list[tuple[float, float]] = []
    if structure.sweep_level > 0:
        zones.append((structure.sweep_level, structure.sweep_level))
    if structure.fvg_zone_low > 0 and structure.fvg_zone_high > 0:
        zones.append((structure.fvg_zone_low, structure.fvg_zone_high))
    if structure.ob_zone_low > 0 and structure.ob_zone_high > 0:
        zones.append((structure.ob_zone_low, structure.ob_zone_high))

    if not zones:
        return False, "no_entry_zone"

    tapped = any(
        _price_in_zone(live_price, lo, hi, tolerance)
        or _price_in_zone(live_price, lo, hi, nearby_tolerance)
        for lo, hi in zones
    )
    if not tapped:
        return False, "price_not_in_retest_zone"

    ideal = sum((lo + hi) / 2.0 for lo, hi in zones) / len(zones)
    chase = abs(live_price - ideal)
    if chase > atr * Config.MAX_CHASE_ATR:
        return False, f"chase_too_far_{chase:.6f}"

    return True, "retest_ok"


def compute_structural_sl(
    action: str,
    entry_price: float,
    atr: float,
    structure: StructureMetadata,
) -> float:
    """SL below/above swept liquidity with ATR buffer, capped by max ATR multiple."""
    buffer = atr * Config.SL_BUFFER_ATR
    max_dist = atr * Config.MAX_SL_ATR_MULTIPLIER

    if action == "LONG":
        if structure.sweep_level > 0:
            structural = structure.sweep_level - buffer
        else:
            structural = entry_price - max_dist
        atr_sl = entry_price - max_dist
        sl = max(structural, atr_sl)
        return max(sl, entry_price * 0.985)
    else:
        if structure.sweep_level > 0:
            structural = structure.sweep_level + buffer
        else:
            structural = entry_price + max_dist
        atr_sl = entry_price + max_dist
        sl = min(structural, atr_sl)
        return sl


def compute_rr_ladder(
    action: str,
    entry_price: float,
    sl_price: float,
) -> tuple[float, float, float, float]:
    """Return (sl, tp1, tp2, tp3) using R-multiples."""
    r = abs(entry_price - sl_price)
    if r <= 0:
        r = entry_price * 0.005

    if action == "LONG":
        tp1 = entry_price + r * Config.TP1_R_MULTIPLE
        tp2 = entry_price + r * Config.TP2_R_MULTIPLE
        tp3 = entry_price + r * Config.TP3_R_MULTIPLE
    else:
        tp1 = entry_price - r * Config.TP1_R_MULTIPLE
        tp2 = entry_price - r * Config.TP2_R_MULTIPLE
        tp3 = entry_price - r * Config.TP3_R_MULTIPLE

    return sl_price, tp1, tp2, tp3


def check_opposing_liquidity_rr(
    action: str,
    entry_price: float,
    sl_price: float,
    opposing_liquidity: float,
) -> tuple[bool, str]:
    """Skip if room to opposing liquidity is smaller than minimum R multiple."""
    r = abs(entry_price - sl_price)
    if r <= 0 or opposing_liquidity <= 0:
        return True, "rr_check_skipped"

    if action == "LONG":
        room = opposing_liquidity - entry_price
    else:
        room = entry_price - opposing_liquidity

    if room <= 0:
        return False, "opposing_liquidity_already_passed"

    if room < r * Config.MIN_OPPOSING_RR:
        return False, f"insufficient_rr_room_{room / r:.2f}R"

    return True, "rr_ok"


def is_multi_confluence(confluence_type: str) -> bool:
    """True when two or more SMC retest confluences are stacked (e.g. SWEEP+OB)."""
    if not confluence_type:
        return False
    hits = [part.strip() for part in str(confluence_type).split("+") if part.strip()]
    return len(hits) >= 2


def effective_smc_min_score(confluence_type: str, macro_trend: str) -> float:
    """
    Tiered SMC score floor: multi-confluence setups may use a lower threshold
    because stacked structure retests carry higher historical edge.
    """
    if is_multi_confluence(confluence_type):
        min_score = Config.MULTI_CONFLUENCE_MIN_SCORE
    else:
        min_score = Config.STRATEGY_MIN_SCORE
        if macro_trend == "NEUTRAL" and Config.ALLOW_NEUTRAL_MACRO_SETUPS:
            min_score = max(min_score, Config.NEUTRAL_MACRO_MIN_SCORE)
    return min_score


def size_multiplier_for_score(score: float) -> float:
    if score >= Config.SCORE_FULL_SIZE:
        return 1.0
    if score >= Config.STRATEGY_MIN_SCORE:
        return Config.HALF_SIZE_MULTIPLIER
    if score >= Config.MULTI_CONFLUENCE_MIN_SCORE:
        return Config.HALF_SIZE_MULTIPLIER
    return 0.0
