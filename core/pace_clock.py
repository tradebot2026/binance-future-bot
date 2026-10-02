"""Per-minute token windows for sequential tier pacing."""

from __future__ import annotations

import time
from collections import deque


class MinuteWindow:
    """Allow up to `per_minute` takes inside a rolling 60-second window."""

    def __init__(self, per_minute: int) -> None:
        self.per_minute = max(int(per_minute), 0)
        self._stamps: deque[float] = deque()

    def remaining(self, now: float | None = None) -> int:
        self._trim(now)
        return max(self.per_minute - len(self._stamps), 0)

    def take(self, n: int = 1, now: float | None = None) -> int:
        granted = min(max(int(n), 0), self.remaining(now))
        stamp = time.monotonic() if now is None else now
        for _ in range(granted):
            self._stamps.append(stamp)
        return granted

    def _trim(self, now: float | None = None) -> None:
        stamp = time.monotonic() if now is None else now
        while self._stamps and (stamp - self._stamps[0]) >= 60.0:
            self._stamps.popleft()
