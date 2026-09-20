"""Reserved for future extension strategies.

VWAP_MEAN_REVERSION is intentionally unregistered so it cannot consume
allocator slots until a real signal engine exists.
"""

from __future__ import annotations

from core.strategy_base import BaseStrategy

EXTENSION_STRATEGIES: tuple[type[BaseStrategy], ...] = ()
