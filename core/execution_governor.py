"""Cap new-entry order REST requests (does not gate protective market closes)."""

from __future__ import annotations

import threading
import time
from collections import deque

from config import Config
from logger import trade_logger


class ExecutionGovernor:
    """At most N entry-order REST submissions per rolling minute."""

    def __init__(self, max_per_minute: int | None = None) -> None:
        self._max = max(
            int(
                max_per_minute
                if max_per_minute is not None
                else Config.MAX_ORDER_REQUESTS_PER_MINUTE
            ),
            1,
        )
        self._stamps: deque[float] = deque()
        self._lock = threading.Lock()

    def allow_entry_order(self) -> tuple[bool, str]:
        now = time.monotonic()
        with self._lock:
            while self._stamps and (now - self._stamps[0]) >= 60.0:
                self._stamps.popleft()
            if len(self._stamps) >= self._max:
                return False, (
                    f"entry order cap {self._max}/min reached — defer candidate"
                )
            self._stamps.append(now)
            return True, ""

    def note_blocked(self, symbol: str, reason: str) -> None:
        trade_logger.info(
            "[EXECUTION_PACED] %s — %s",
            symbol,
            reason,
        )
