"""Per-component REST budget — Order 400 / Hot 600 / Normal 400 under a 1500 cap."""

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
    Sliding-window weight budget per lane.
    Execution is never fail-closed here (hard cap lives in RestUsageTracker).
    Normal/background freeze first when the shared IP total hits 1200.
    """

    def __init__(self, max_weight_per_minute: Optional[int] = None) -> None:
        self._max_per_minute = max(
            int(max_weight_per_minute or Config.rest_hard_weight_cap()), 1
        )
        self._windows: dict[RestLane, deque[tuple[float, int]]] = {
            RestLane.EXECUTION: deque(),
            RestLane.BACKGROUND: deque(),
            RestLane.BOOTSTRAP: deque(),
        }
        self._lock = threading.Lock()

    @property
    def max_weight_per_minute(self) -> int:
        return self._max_per_minute

    def _cap_for(self, lane: RestLane) -> int:
        if lane == RestLane.EXECUTION:
            return Config.rest_budget_order_weight()
        if lane == RestLane.BOOTSTRAP:
            return Config.rest_budget_hot_weight()
        return Config.rest_budget_normal_weight()

    def current_window_weight(self) -> int:
        self._purge_old()
        return sum(weight for window in self._windows.values() for _, weight in window)

    def remaining_fraction(self) -> float:
        self._purge_old()
        used = self.current_window_weight()
        cap = max(self._max_per_minute, 1)
        return max(0.0, (cap - used) / cap)

    def has_budget_for(
        self,
        weight: int,
        lane: RestLane,
        *,
        min_remaining_fraction: Optional[float] = None,
    ) -> bool:
        if lane == RestLane.EXECUTION:
            return True
        _ = min_remaining_fraction
        weight = max(int(weight), 1)
        cap = self._cap_for(lane)
        with self._lock:
            self._purge_old()
            used = sum(w for _, w in self._windows[lane])
            return used + weight <= cap

    def try_acquire(
        self,
        weight: int,
        lane: RestLane,
        *,
        min_remaining_fraction: Optional[float] = None,
    ) -> bool:
        if lane == RestLane.EXECUTION:
            return True
        if not self.has_budget_for(
            weight, lane, min_remaining_fraction=min_remaining_fraction
        ):
            return False
        weight = max(int(weight), 1)
        with self._lock:
            self._windows[lane].append((time.monotonic(), weight))
            return True

    def acquire(self, weight: int, lane: RestLane) -> bool:
        return self.try_acquire(weight, lane)

    def _purge_old(self) -> None:
        cutoff = time.monotonic() - 60.0
        for window in self._windows.values():
            while window and window[0][0] < cutoff:
                window.popleft()
