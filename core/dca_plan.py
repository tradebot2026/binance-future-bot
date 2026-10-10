"""3-step multi-entry (DCA) with a persisted entry_stage state machine.

Triggers and the final SL use structural levels (Base Entry + Entry 3 trigger).
Binance's averaged position price is never used. TP1/TP2/TP3 stay frozen.
"""

from __future__ import annotations

from typing import Any, Optional

from config import Config
from utils import safe_float


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
    """Fixed SL anchored on the Entry 3 trigger — never on Binance avg entry."""
    if entry3_trigger <= 0 or sl_distance <= 0:
        return 0.0
    if is_long:
        return float(entry3_trigger) - float(sl_distance)
    return float(entry3_trigger) + float(sl_distance)


def build_dca_plan(base_entry: float, planned_sl: float) -> dict[str, Any]:
    """Store Entry-1 references. TP is never written here."""
    base = float(base_entry)
    sl = float(planned_sl)
    frac2 = min(dca_entry2_fraction(), dca_entry3_fraction() - 0.05)
    frac3 = max(dca_entry3_fraction(), frac2 + 0.05)
    sl_dist = structural_sl_distance(base, sl)
    entry3 = interpolate_toward_sl(base, sl, frac3)
    long_side = is_long_plan(base, sl)
    return {
        "base_entry_price": base,
        "entry_stage": 1,
        "dca_entries": 1,
        "dca_planned_sl": sl,
        "dca_sl_distance": sl_dist,
        "dca_sl_armed": False,
        "dca_entry2_px": interpolate_toward_sl(base, sl, frac2),
        "dca_entry3_px": entry3,
        "dca_final_sl": final_sl_from_entry3(entry3, sl_dist, is_long=long_side),
    }


def apply_initial_dca_plan(
    metadata: dict[str, Any],
    base_entry: float,
    planned_sl: float,
) -> dict[str, Any]:
    """Stamp Entry-1 references once. Leaves existing TP metadata untouched."""
    if base_entry <= 0 or planned_sl <= 0:
        return metadata
    if safe_float(metadata.get("base_entry_price")) > 0:
        _ensure_stage_fields(metadata)
        return metadata
    metadata.update(build_dca_plan(base_entry, planned_sl))
    return metadata


def _ensure_stage_fields(metadata: dict[str, Any]) -> dict[str, Any]:
    stage = entry_stage_of(None, metadata)
    metadata["entry_stage"] = stage
    metadata["dca_entries"] = stage
    if safe_float(metadata.get("dca_final_sl")) <= 0:
        entry3 = safe_float(metadata.get("dca_entry3_px"))
        sl_dist = safe_float(metadata.get("dca_sl_distance"))
        if sl_dist <= 0:
            sl_dist = structural_sl_distance(
                safe_float(metadata.get("base_entry_price")),
                safe_float(metadata.get("dca_planned_sl")),
            )
        base = safe_float(metadata.get("base_entry_price"))
        planned = safe_float(metadata.get("dca_planned_sl"))
        metadata["dca_final_sl"] = final_sl_from_entry3(
            entry3, sl_dist, is_long=is_long_plan(base, planned) if planned else True
        )
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
    """Final SL from the Entry 3 trigger. Ignores any averaged exchange entry."""
    stored = safe_float(metadata.get("dca_final_sl"))
    if stored > 0:
        return stored
    entry3 = entry3_trigger_price(metadata)
    sl_dist = safe_float(metadata.get("dca_sl_distance"))
    if sl_dist <= 0:
        sl_dist = structural_sl_distance(
            base_entry_price(metadata),
            planned_sl_price(metadata),
        )
    return final_sl_from_entry3(
        entry3, sl_dist, is_long=str(side).upper() == "LONG"
    )


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
    """True when price has blown through the Entry-3-anchored safety SL."""
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
