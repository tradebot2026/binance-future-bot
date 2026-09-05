"""Central REST budget manager — caps background REST below Binance limits."""

from __future__ import annotations

import enum
import threading
import time
from collections import deque
from typing import Optional

from config import Config


class RestLane(enum.Enum):
    EXECUTION = "execution"
    BACKGROUND = "background"
    BOOTSTRAP = "bootstrap"


class RestBudgetManager:
    """
    Sliding-window weight budget for non-execution REST.
    Execution lane bypasses the per-minute cap (still uses token bucket).
    Background/bootstrap calls fail fast when remaining budget < reserve floor.
    """

    def __init__(self, max_weight_per_minute: Optional[int] = None) -> None:
        self._max_per_minute = max(
            max_weight_per_minute or Config.REST_BUDGET_WEIGHT_PER_MINUTE, 1
        )
        self._window: deque[tuple[float, int]] = deque()
        self._lock = threading.Lock()

    @property
    def max_weight_per_minute(self) -> int:
        return self._max_per_minute

    def current_window_weight(self) -> int:
        self._purge_old()
        return sum(weight for _, weight in self._window)

    def remaining_fraction(self) -> float:
        """Fraction of the per-minute budget still available (0.0–1.0)."""
        self._purge_old()
        used = sum(weight for _, weight in self._window)
        return max(0.0, (self._max_per_minute - used) / self._max_per_minute)

    def has_budget_for(
        self,
        weight: int,
        lane: RestLane,
        *,
        min_remaining_fraction: Optional[float] = None,
    ) -> bool:
        """Check whether a call may proceed without breaching the reserve floor."""
        if lane == RestLane.EXECUTION:
            return True

        reserve = (
            Config.REST_BUDGET_MIN_REMAINING_FRACTION
            if min_remaining_fraction is None
            else min_remaining_fraction
        )
        reserve = max(min(reserve, 1.0), 0.0)
        weight = max(weight, 1)

        with self._lock:
            self._purge_old()
            used = sum(w for _, w in self._window)
            if used + weight > self._max_per_minute:
                return False
            remaining_after = (self._max_per_minute - used - weight) / self._max_per_minute
            return remaining_after >= reserve

    def try_acquire(
        self,
        weight: int,
        lane: RestLane,
        *,
        min_remaining_fraction: Optional[float] = None,
    ) -> bool:
        """Reserve budget if available; never blocks."""
        if lane == RestLane.EXECUTION:
            return True
        if not self.has_budget_for(weight, lane, min_remaining_fraction=min_remaining_fraction):
            return False
        weight = max(weight, 1)
        with self._lock:
            self._window.append((time.monotonic(), weight))
            return True

    def acquire(self, weight: int, lane: RestLane) -> bool:
        """
        Reserve budget for a non-execution REST call.
        Returns False when reserve floor would be breached (fail fast, no sleep).
        """
        return self.try_acquire(weight, lane)

    def _purge_old(self) -> None:
        cutoff = time.monotonic() - 60.0
        while self._window and self._window[0][0] < cutoff:
            self._window.popleft()
