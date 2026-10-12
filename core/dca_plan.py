"""3-step multi-entry (DCA) with a persisted entry_stage state machine.

Triggers and the final SL use structural pullback levels from the Entry-1
base fill. Binance's averaged position price is never used. TP1/TP2/TP3 stay
frozen. The fixed SL is wallet-capped so full-ladder loss stays near 4%.
"""

from __future__ import annotations

from typing import Any, Optional

from config import Config
from utils import safe_float

_STRUCT_LEVEL_KEYS: tuple[str, ...] = (
    "sweep_level",
    "ob_zone_low",
    "ob_zone_high",
    "fvg_zone_low",
    "fvg_zone_high",
    "equilibrium",
    "last_swing_low",
    "last_swing_high",
    "range_low",
    "range_high",
    "vwap",
)


def dca_enabled() -> bool:
    return bool(getattr(Config, "ENABLE_DCA_LADDER", True))


def dca_plan_active(metadata: Optional[dict[str, Any]]) -> bool:
    """True only when this trade actually has a stamped 3-step plan."""
    if not dca_enabled() or not metadata:
        return False
    return safe_float(metadata.get("base_entry_price")) > 0


def dca_max_entries() -> int:
    return min(max(int(getattr(Config, "DCA_MAX_ENTRIES", 3)), 1), 3)


def dca_add_wallet_fraction() -> float:
    return min(max(float(getattr(Config, "DCA_ADD_WALLET_PERCENT", 1.0)), 0.1), 5.0) / 100.0


def dca_entry2_fraction() -> float:
    return min(max(float(getattr(Config, "DCA_ENTRY2_SL_FRACTION", 0.45)), 0.05), 0.95)


def dca_entry3_fraction() -> float:
    return min(max(float(getattr(Config, "DCA_ENTRY3_SL_FRACTION", 0.75)), 0.10), 0.99)


def dca_sl_wallet_cap_percent() -> float:
    """Account-protection ceiling for full-ladder stop loss vs wallet."""
    raw = float(getattr(Config, "DCA_SL_WALLET_CAP_PERCENT", 4.0) or 0.0)
    if raw <= 0:
        raw = 4.0
    return min(max(raw, 2.0), 6.0)


def interpolate_toward_sl(base_entry: float, planned_sl: float, fraction: float) -> float:
    """Price sitting `fraction` of the way from base entry toward the original SL."""
    return float(base_entry) + (float(planned_sl) - float(base_entry)) * float(fraction)


def structural_sl_distance(base_entry: float, planned_sl: float) -> float:
    return abs(float(base_entry) - float(planned_sl))


def is_long_plan(base_entry: float, planned_sl: float) -> bool:
    return float(planned_sl) < float(base_entry)


def final_sl_from_entry3(
    entry3_trigger: float,
    sl_distance: float,
    *,
    is_long: bool,
) -> float:
    """Legacy helper — production SL is the capped base plan, not Entry-3 re-anchor."""
    if entry3_trigger <= 0 or sl_distance <= 0:
        return 0.0
    if is_long:
        return float(entry3_trigger) - float(sl_distance)
    return float(entry3_trigger) + float(sl_distance)


def _as_structure_dict(structure: Any) -> dict[str, Any]:
    if structure is None:
        return {}
    if isinstance(structure, dict):
        return dict(structure)
    to_dict = getattr(structure, "to_dict", None)
    if callable(to_dict):
        try:
            return dict(to_dict() or {})
        except Exception:
            return {}
    out: dict[str, Any] = {}
    for key in _STRUCT_LEVEL_KEYS + ("key_levels",):
        if hasattr(structure, key):
            out[key] = getattr(structure, key)
    return out


def _progress_toward_sl(base: float, sl: float, price: float) -> float:
    span = float(sl) - float(base)
    if abs(span) < 1e-12:
        return 0.0
    return (float(price) - float(base)) / span


def collect_pullback_levels(
    base_entry: float,
    planned_sl: float,
    structure: Any = None,
) -> list[float]:
    """Structural prices strictly between base fill and planned SL."""
    base = float(base_entry)
    sl = float(planned_sl)
    lo, hi = (min(base, sl), max(base, sl))
    data = _as_structure_dict(structure)
    found: list[float] = []
    for key in _STRUCT_LEVEL_KEYS:
        px = safe_float(data.get(key))
        if lo < px < hi:
            found.append(px)
    raw_levels = data.get("key_levels") or []
    if isinstance(raw_levels, (list, tuple)):
        for value in raw_levels:
            px = safe_float(value)
            if lo < px < hi:
                found.append(px)
    uniq: list[float] = []
    seen: set[float] = set()
    for px in sorted(found, key=lambda item: abs(item - base)):
        token = round(px, 8)
        if token in seen:
            continue
        seen.add(token)
        uniq.append(px)
    return uniq


def pick_structural_entries(
    base_entry: float,
    planned_sl: float,
    structure: Any = None,
) -> tuple[float, float, bool]:
    """Entry 2 (near) and Entry 3 (deep) from pullback structure, else interpolation."""
    base = float(base_entry)
    sl = float(planned_sl)
    frac2 = min(dca_entry2_fraction(), dca_entry3_fraction() - 0.05)
    frac3 = max(dca_entry3_fraction(), frac2 + 0.05)
    fallback2 = interpolate_toward_sl(base, sl, frac2)
    fallback3 = interpolate_toward_sl(base, sl, frac3)
    levels = collect_pullback_levels(base, sl, structure)
    if not levels:
        return fallback2, fallback3, False

    def _farther(left: float, right: float) -> bool:
        return abs(left - base) > abs(right - base) + 1e-12

    shallow = [px for px in levels if 0.18 <= _progress_toward_sl(base, sl, px) <= 0.55]
    deep = [px for px in levels if 0.55 <= _progress_toward_sl(base, sl, px) <= 0.88]
    entry2 = shallow[0] if shallow else fallback2
    if deep:
        farther = [px for px in deep if _farther(px, entry2)]
        entry3 = farther[-1] if farther else fallback3
    else:
        farther = [px for px in levels if _farther(px, entry2)]
        entry3 = farther[-1] if farther else fallback3
    if not _farther(entry3, entry2):
        entry3 = fallback3 if _farther(fallback3, entry2) else fallback3
    if not _farther(entry3, entry2):
        return fallback2, fallback3, False
    used_structure = entry2 in levels or entry3 in levels
    return entry2, entry3, used_structure


def _add_qty(wallet: float, price: float, fraction: float) -> float:
    if price <= 0 or wallet <= 0 or fraction <= 0:
        return 0.0
    return (wallet * fraction) / price


def ladder_stop_loss_usd(
    base_entry: float,
    sl: float,
    entry2: float,
    entry3: float,
    entry1_qty: float,
    wallet: float,
    *,
    is_long: bool,
    add_fraction: Optional[float] = None,
) -> float:
    """USDT loss if Entry 1+2+3 all fill and the fixed SL is hit. Ignores Binance avg."""
    frac = dca_add_wallet_fraction() if add_fraction is None else float(add_fraction)
    qty1 = max(float(entry1_qty), 0.0)
    qty2 = _add_qty(wallet, entry2, frac)
    qty3 = _add_qty(wallet, entry3, frac)
    if is_long:
        return (
            qty1 * max(base_entry - sl, 0.0)
            + qty2 * max(entry2 - sl, 0.0)
            + qty3 * max(entry3 - sl, 0.0)
        )
    return (
        qty1 * max(sl - base_entry, 0.0)
        + qty2 * max(sl - entry2, 0.0)
        + qty3 * max(sl - entry3, 0.0)
    )


def _clamp_between(base: float, sl: float, price: float) -> float:
    lo, hi = (min(base, sl), max(base, sl))
    if price <= lo or price >= hi:
        return 0.0
    return price


def cap_sl_to_wallet(
    base_entry: float,
    planned_sl: float,
    entry2: float,
    entry3: float,
    entry1_qty: float,
    wallet: float,
    *,
    is_long: bool,
) -> tuple[float, float, float, bool]:
    """Pull SL toward entry until full-ladder loss <= wallet cap. Re-clamp DCA levels."""
    base = float(base_entry)
    sl = float(planned_sl)
    e2, e3 = float(entry2), float(entry3)
    cap_usd = max(float(wallet), 0.0) * (dca_sl_wallet_cap_percent() / 100.0)
    if wallet <= 0 or entry1_qty <= 0 or cap_usd <= 0:
        return sl, e2, e3, False
    loss = ladder_stop_loss_usd(base, sl, e2, e3, entry1_qty, wallet, is_long=is_long)
    if loss <= cap_usd + 1e-9:
        return sl, e2, e3, False

    far, near = sl, base
    best_sl = near
    for _ in range(48):
        mid = (far + near) / 2.0
        if abs(base - mid) < 1e-12:
            break
        mid2 = _clamp_between(base, mid, e2) or interpolate_toward_sl(base, mid, dca_entry2_fraction())
        mid3 = _clamp_between(base, mid, e3) or interpolate_toward_sl(base, mid, dca_entry3_fraction())
        if abs(mid3 - base) <= abs(mid2 - base):
            mid3 = interpolate_toward_sl(base, mid, max(dca_entry3_fraction(), dca_entry2_fraction() + 0.05))
        mid_loss = ladder_stop_loss_usd(
            base, mid, mid2, mid3, entry1_qty, wallet, is_long=is_long
        )
        if mid_loss <= cap_usd:
            best_sl = mid
            near = mid
        else:
            far = mid
    e2 = _clamp_between(base, best_sl, e2) or interpolate_toward_sl(
        base, best_sl, dca_entry2_fraction()
    )
    e3 = _clamp_between(base, best_sl, e3) or interpolate_toward_sl(
        base, best_sl, dca_entry3_fraction()
    )
    if abs(e3 - base) <= abs(e2 - base):
        e3 = interpolate_toward_sl(
            base, best_sl, max(dca_entry3_fraction(), dca_entry2_fraction() + 0.05)
        )
    return best_sl, e2, e3, True


def build_dca_plan(
    base_entry: float,
    planned_sl: float,
    *,
    structure: Any = None,
    wallet: float = 0.0,
    entry1_qty: float = 0.0,
    leverage: float = 0.0,
    side: str = "",
) -> dict[str, Any]:
    """Store Entry-1 references. TP is never written here."""
    base = float(base_entry)
    sl = float(planned_sl)
    long_side = is_long_plan(base, sl) if str(side).upper() not in ("LONG", "SHORT") else (
        str(side).upper() == "LONG"
    )
    entry2, entry3, used_structure = pick_structural_entries(base, sl, structure)
    capped_sl, entry2, entry3, was_capped = cap_sl_to_wallet(
        base,
        sl,
        entry2,
        entry3,
        entry1_qty,
        wallet,
        is_long=long_side,
    )
    sl_dist = structural_sl_distance(base, capped_sl)
    return {
        "base_entry_price": base,
        "entry_stage": 1,
        "dca_entries": 1,
        "dca_planned_sl": sl,
        "dca_sl_distance": sl_dist,
        "dca_sl_armed": False,
        "dca_entry2_px": entry2,
        "dca_entry3_px": entry3,
        "dca_final_sl": capped_sl,
        "dca_sl_capped": was_capped,
        "dca_structural_entries": used_structure,
        "dca_wallet_cap_pct": dca_sl_wallet_cap_percent(),
        "dca_leverage": float(leverage or 0.0),
        "dca_uncapped_sl": sl,
    }


def apply_initial_dca_plan(
    metadata: dict[str, Any],
    base_entry: float,
    planned_sl: float,
    *,
    structure: Any = None,
    wallet: float = 0.0,
    entry1_qty: float = 0.0,
    leverage: float = 0.0,
    side: str = "",
) -> dict[str, Any]:
    """Stamp Entry-1 references once. Leaves existing TP metadata untouched."""
    if base_entry <= 0 or planned_sl <= 0:
        return metadata
    if safe_float(metadata.get("base_entry_price")) > 0:
        _ensure_stage_fields(metadata)
        return metadata
    metadata.update(
        build_dca_plan(
            base_entry,
            planned_sl,
            structure=structure,
            wallet=wallet,
            entry1_qty=entry1_qty,
            leverage=leverage,
            side=side,
        )
    )
    return metadata


def _ensure_stage_fields(metadata: dict[str, Any]) -> dict[str, Any]:
    stage = entry_stage_of(None, metadata)
    metadata["entry_stage"] = stage
    metadata["dca_entries"] = stage
    if safe_float(metadata.get("dca_final_sl")) <= 0:
        planned = planned_sl_price(metadata)
        if planned > 0:
            metadata["dca_final_sl"] = planned
    return metadata


def base_entry_price(metadata: dict[str, Any], fallback: float = 0.0) -> float:
    price = safe_float(metadata.get("base_entry_price"))
    return price if price > 0 else float(fallback or 0.0)


def planned_sl_price(metadata: dict[str, Any], fallback: float = 0.0) -> float:
    price = safe_float(metadata.get("dca_planned_sl"))
    return price if price > 0 else float(fallback or 0.0)


def entry3_trigger_price(metadata: dict[str, Any]) -> float:
    return safe_float(metadata.get("dca_entry3_px"))


def resolved_final_sl(metadata: dict[str, Any], side: str = "LONG") -> float:
    """Final SL from the Entry-1 plan (wallet-capped). Ignores Binance avg entry."""
    stored = safe_float(metadata.get("dca_final_sl"))
    if stored > 0:
        return stored
    planned = planned_sl_price(metadata)
    if planned > 0:
        return planned
    _ = side
    return 0.0


def telegram_dca_levels(metadata: Optional[dict[str, Any]]) -> dict[str, float]:
    """Pre-calculated Entry 2 / Entry 3 / fixed SL for the open-trade alert."""
    meta = metadata or {}
    return {
        "base_entry": base_entry_price(meta),
        "entry2": safe_float(meta.get("dca_entry2_px")),
        "entry3": safe_float(meta.get("dca_entry3_px")),
        "final_sl": resolved_final_sl(meta),
    }


def entry_stage_of(
    trade: Optional[dict[str, Any]],
    metadata: Optional[dict[str, Any]] = None,
) -> int:
    """Canonical stage 1/2/3 — column first, then metadata (survives restarts)."""
    candidates: list[int] = []
    if trade:
        try:
            if trade.get("entry_stage") is not None:
                candidates.append(int(trade.get("entry_stage")))
        except (TypeError, ValueError):
            pass
    meta = metadata or {}
    for key in ("entry_stage", "dca_entries"):
        try:
            if meta.get(key) is not None:
                candidates.append(int(meta.get(key)))
        except (TypeError, ValueError):
            pass
    if not candidates:
        return 1
    return min(max(max(candidates), 1), dca_max_entries())


def dca_entry_count(metadata: dict[str, Any]) -> int:
    return entry_stage_of(None, metadata)


def next_dca_step(
    metadata: dict[str, Any],
    trade: Optional[dict[str, Any]] = None,
) -> Optional[int]:
    filled = entry_stage_of(trade, metadata)
    nxt = filled + 1
    if nxt > dca_max_entries():
        return None
    return nxt


def sl_is_armed(
    metadata: dict[str, Any],
    trade: Optional[dict[str, Any]] = None,
) -> bool:
    if bool(metadata.get("dca_sl_armed")):
        return True
    return entry_stage_of(trade, metadata) >= dca_max_entries()


def should_trigger_add(
    side: str,
    price: float,
    metadata: dict[str, Any],
    step: int,
) -> bool:
    if price <= 0:
        return False
    key = "dca_entry2_px" if step == 2 else "dca_entry3_px"
    trigger = safe_float(metadata.get(key))
    if trigger <= 0:
        return False
    if str(side).upper() == "LONG":
        return price <= trigger
    return price >= trigger


def last_resort_sl_hit(side: str, price: float, sl_price: float) -> bool:
    """True when price has blown through the safety SL."""
    if price <= 0 or sl_price <= 0:
        return False
    if str(side).upper() == "LONG":
        return price <= sl_price
    return price >= sl_price


def mark_dca_filled(metadata: dict[str, Any], step: int) -> dict[str, Any]:
    stage = min(max(int(step), 1), dca_max_entries())
    metadata["entry_stage"] = stage
    metadata["dca_entries"] = stage
    if stage >= dca_max_entries():
        metadata["dca_sl_armed"] = True
    return metadata
