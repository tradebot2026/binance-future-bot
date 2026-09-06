"""Shared helper functions and precise rounding utilities."""

from __future__ import annotations

import html
import math
from datetime import datetime, timezone
from typing import Union

Number = Union[int, float]


def utc_now() -> datetime:
    """Return the current UTC-aware datetime."""
    return datetime.now(timezone.utc)


def utc_today_str() -> str:
    """Return today's date string in UTC (YYYY-MM-DD)."""
    return utc_now().strftime("%Y-%m-%d")


def round_step_size(value: float, step_size: float, precision: int) -> float:
    """
    Floor a value to the nearest valid exchange step size.
    Prevents LOT_SIZE / PRICE_FILTER rejections on Binance Futures.
    """
    if step_size <= 0:
        return round(value, precision)
    rounded = math.floor(value / step_size) * step_size
    return round(rounded, precision)


def round_step_size_up(value: float, step_size: float, precision: int) -> float:
    """Ceil a value to the next valid exchange step size."""
    if step_size <= 0:
        return round(value, precision)
    if value <= 0:
        return round(step_size, precision)
    rounded = math.ceil(value / step_size - 1e-12) * step_size
    return round(rounded, precision)


def amount_to_precision(
    quantity: float, step_size: float, quantity_precision: int
) -> float:
    """
    CCXT-compatible quantity precision: floor to step size and decimal precision.
    """
    return round_step_size(quantity, step_size, quantity_precision)


def minimum_order_quantity(
    entry_price: float, min_qty: float, min_notional: float, step_size: float, precision: int
) -> float:
    """Smallest valid quantity satisfying Binance min_qty and min_notional."""
    if entry_price <= 0:
        return round_step_size_up(min_qty, step_size, precision)
    notional_qty = min_notional / entry_price
    raw_min = max(min_qty, notional_qty)
    return round_step_size_up(raw_min, step_size, precision)


def cap_quantity_to_notional(
    quantity: float,
    entry_price: float,
    max_notional: float,
    min_qty: float,
    min_notional: float,
    step_size: float,
    precision: int,
) -> tuple[float, float]:
    """
    Cap quantity so notional <= max_notional while respecting LOT_SIZE / minQty.
    Floors to step size within the cap; bumps up to exchange minimum when it still fits.
    Returns (quantity, notional). quantity=0 when no valid size fits the cap.
    """
    if entry_price <= 0 or max_notional <= 0:
        return 0.0, 0.0

    min_valid_qty = minimum_order_quantity(
        entry_price, min_qty, min_notional, step_size, precision
    )
    min_valid_notional = min_valid_qty * entry_price

    if min_valid_notional > max_notional + 1e-9:
        return 0.0, 0.0

    current_notional = quantity * entry_price
    if current_notional <= max_notional + 1e-9:
        if quantity >= min_valid_qty and current_notional >= min_notional - 1e-9:
            return quantity, current_notional
        if min_valid_notional <= max_notional + 1e-9:
            return min_valid_qty, min_valid_notional
        return 0.0, 0.0

    capped_qty = round_step_size(max_notional / entry_price, step_size, precision)
    if capped_qty <= 0 and step_size > 0:
        one_step_notional = step_size * entry_price
        if one_step_notional <= max_notional + 1e-9:
            capped_qty = round_step_size_up(step_size, step_size, precision)

    if capped_qty < min_valid_qty:
        capped_qty = min_valid_qty

    capped_notional = capped_qty * entry_price
    if capped_notional > max_notional + 1e-9:
        capped_qty = round_step_size(max_notional / entry_price, step_size, precision)
        capped_notional = capped_qty * entry_price

    if (
        capped_qty <= 0
        or capped_qty < min_qty
        or capped_notional < min_notional - 1e-9
        or capped_notional > max_notional + 1e-9
    ):
        return 0.0, 0.0

    return capped_qty, capped_notional


def escape_html(text: object) -> str:
    """Escape dynamic text for Telegram HTML parse mode."""
    return html.escape(str(text), quote=False)


def safe_float(value: object, default: float = 0.0) -> float:
    """Convert a value to float with a safe fallback."""
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
