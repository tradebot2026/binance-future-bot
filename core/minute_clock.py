"""Wall-clock helper: run the scan once per UTC minute after the 1m close."""

from __future__ import annotations

import time


class MinuteScanClock:
    """Fire once per UTC minute after ``offset_seconds`` (default :01)."""

    def __init__(
        self,
        *,
        offset_seconds: float = 1.0,
        enabled: bool = True,
    ) -> None:
        self.offset_seconds = min(max(float(offset_seconds), 0.0), 59.0)
        self.enabled = bool(enabled)
        self._last_minute_id: int | None = None

    def due(self, now: float | None = None) -> bool:
        """True once for the current minute after the offset. Consumes the slot."""
        now = time.time() if now is None else float(now)
        if not self.enabled:
            return True
        if (now % 60.0) < self.offset_seconds:
            return False
        minute_id = int(now // 60.0)
        if self._last_minute_id == minute_id:
            return False
        self._last_minute_id = minute_id
        return True

    def seconds_until_due(self, now: float | None = None) -> float:
        """How long to sleep before the next aligned mark (0 if already due)."""
        now = time.time() if now is None else float(now)
        if not self.enabled:
            return 0.0
        sec = now % 60.0
        if sec < self.offset_seconds:
            return self.offset_seconds - sec
        if self._last_minute_id != int(now // 60.0):
            return 0.0
        return 60.0 - sec + self.offset_seconds

    def skip_current_minute(self, now: float | None = None) -> None:
        """Wait for the next 1m close instead of scanning mid-minute."""
        now = time.time() if now is None else float(now)
        self._last_minute_id = int(now // 60.0)
