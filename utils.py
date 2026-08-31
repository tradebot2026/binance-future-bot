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


def escape_html(text: object) -> str:
    """Escape dynamic text for Telegram HTML parse mode."""
    return html.escape(str(text), quote=False)


def safe_float(value: object, default: float = 0.0) -> float:
    """Convert a value to float with a safe fallback."""
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
