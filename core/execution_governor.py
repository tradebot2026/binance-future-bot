"""Cap new-entry order REST requests (does not gate protective market closes)."""

from __future__ import annotations

import threading
import time
from collections import deque

from config import Config
from logger import trade_logger


class ExecutionGovernor:
    """At most N entry-order REST submissions per rolling minute.

    N follows the open-position book (default 12) so Super can fill every
    slot. Binance IP safety still comes from the 1s ORDER REST lane and
    used-weight ceilings — this governor only prevents a burst above the book.
    """

    def __init__(self, max_per_minute: int | None = None) -> None:
        self._override = (
            max(int(max_per_minute), 1) if max_per_minute is not None else None
        )
        self._stamps: deque[float] = deque()
        self._lock = threading.Lock()

    def _limit(self) -> int:
        if self._override is not None:
            return self._override
        return Config.max_entry_orders_per_minute()

    def allow_entry_order(self) -> tuple[bool, str]:
        now = time.monotonic()
        limit = self._limit()
        with self._lock:
            while self._stamps and (now - self._stamps[0]) >= 60.0:
                self._stamps.popleft()
            if len(self._stamps) >= limit:
                return False, (
                    f"entry order cap {limit}/min reached — defer candidate"
                )
            self._stamps.append(now)
            return True, ""

    def note_blocked(self, symbol: str, reason: str) -> None:
        trade_logger.info(
            "[EXECUTION_PACED] %s — %s",
            symbol,
            reason,
        )
