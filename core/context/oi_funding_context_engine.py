"""Open interest buildup + funding rate positioning context."""

from __future__ import annotations

from typing import Any

from config import Config
from core.context.context_types import ContextModuleResult
from utils import safe_float


def evaluate_oi_funding_context(
    derivatives: dict[str, Any],
    price_change_pct: float = 0.0,
) -> ContextModuleResult:
    """
    Detect crowded positioning and squeeze setups from OI + funding.

    Expected derivatives keys (from exchange cache):
      funding_rate, open_interest, oi_change_pct, mark_price
    """
    module = "OI_FUNDING"
    if not derivatives:
        return ContextModuleResult(module=module)

    funding = safe_float(derivatives.get("funding_rate"))
    oi_change = safe_float(derivatives.get("oi_change_pct"))
    oi = safe_float(derivatives.get("open_interest"))

    if oi <= 0 and funding == 0:
        return ContextModuleResult(module=module)

    score = 0.0
    direction: str = "NEUTRAL"
    reasons: list[str] = []

    funding_extreme_long_crowd = funding >= Config.OI_FUNDING_EXTREME_POSITIVE
    funding_extreme_short_crowd = funding <= Config.OI_FUNDING_EXTREME_NEGATIVE
    oi_buildup = oi_change >= Config.OI_BUILDUP_MIN_PCT
    oi_flush = oi_change <= -Config.OI_BUILDUP_MIN_PCT

    # Short squeeze: shorts paying funding, OI rising, price stabilizing/rising
    if funding_extreme_long_crowd and oi_buildup and price_change_pct >= -0.3:
        direction = "LONG"
        score = 65.0 + min(abs(funding) * 8000.0, 15.0)
        reasons.append("short_crowded_oi_buildup")

    # Long squeeze: longs paying funding, OI rising, price stabilizing/falling
    elif funding_extreme_short_crowd and oi_buildup and price_change_pct <= 0.3:
        direction = "SHORT"
        score = 65.0 + min(abs(funding) * 8000.0, 15.0)
        reasons.append("long_crowded_oi_buildup")

    # Liquidation cascade confirmation
    elif oi_flush and price_change_pct >= 1.0:
        direction = "LONG"
        score = 60.0
        reasons.append("oi_flush_short_liquidation")
    elif oi_flush and price_change_pct <= -1.0:
        direction = "SHORT"
        score = 60.0
        reasons.append("oi_flush_long_liquidation")

    # Moderate funding skew without extreme OI
    elif abs(funding) >= Config.OI_FUNDING_MODERATE and not oi_buildup:
        if funding > 0:
            direction = "SHORT"
            score = 52.0
            reasons.append("positive_funding_mean_revert")
        else:
            direction = "LONG"
            score = 52.0
            reasons.append("negative_funding_mean_revert")

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
        level_tags=["oi_funding", "derivatives"],
        metadata={
            "funding_rate": funding,
            "open_interest": oi,
            "oi_change_pct": oi_change,
            "price_change_pct": price_change_pct,
            "reasons": reasons,
        },
    )
