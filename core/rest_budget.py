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

    def acquire(self, weight: int, lane: RestLane) -> None:
        weight = max(weight, 1)
        if lane == RestLane.EXECUTION:
            return

        while True:
            with self._lock:
                self._purge_old()
                used = sum(w for _, w in self._window)
                if used + weight <= self._max_per_minute:
                    self._window.append((time.monotonic(), weight))
                    return
                if self._window:
                    oldest_at = self._window[0][0]
                    wait = max(60.0 - (time.monotonic() - oldest_at), 0.05)
                else:
                    wait = 0.05
            time.sleep(min(wait, 5.0))

    def _purge_old(self) -> None:
        cutoff = time.monotonic() - 60.0
        while self._window and self._window[0][0] < cutoff:
            self._window.popleft()
