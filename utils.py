"""Shared helper functions and precise rounding utilities."""

from __future__ import annotations

import html
import math
from typing import Union

Number = Union[int, float]


def round_step_size(value: float, step_size: float, precision: int) -> float:
    """
    Floor a value to the nearest valid exchange step size.
    Prevents LOT_SIZE / PRICE_FILTER rejections on Binance Futures.
    """
    if step_size <= 0:
        return round(value, precision)
    rounded = math.floor(value / step_size) * step_size
    return round(rounded, precision)


def escape_html(text: object) -> str:
    """Escape dynamic text for Telegram HTML parse mode."""
    return html.escape(str(text), quote=False)


def safe_float(value: object, default: float = 0.0) -> float:
    """Convert a value to float with a safe fallback."""
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
