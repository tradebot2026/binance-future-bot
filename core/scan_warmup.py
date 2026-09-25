"""Startup scan warm-up — populate WS caches before the first evaluation."""

from __future__ import annotations

import time

from logger import system_logger


class ScanWarmupGate:
    """Hold the scanner idle while WebSocket caches fill (3–5 minutes)."""

    MODE_WARMUP = "WARMUP_MODE"
    MODE_ACTIVE = "ACTIVE_SCANNING_MODE"

    def __init__(self, duration_seconds: float) -> None:
        self.duration_seconds = float(duration_seconds)
        self._started_at = time.monotonic()
        self._active_announced = False
        self._last_progress_log_at = 0.0
        if self.duration_seconds > 0:
            system_logger.info(
                "[%s] Populating Cache — scanner idle for %.0fs "
                "(WebSocket only, no trade evaluation).",
                self.MODE_WARMUP,
                self.duration_seconds,
            )

    def in_warmup(self) -> bool:
        if self.duration_seconds <= 0:
            return False
        return (time.monotonic() - self._started_at) < self.duration_seconds

    def remaining_seconds(self) -> float:
        if self.duration_seconds <= 0:
            return 0.0
        return max(self.duration_seconds - (time.monotonic() - self._started_at), 0.0)

    def just_finished(self) -> bool:
        """True once when warm-up elapses. Safe to call every loop tick."""
        if self._active_announced:
            return False
        if self.duration_seconds <= 0:
            self._active_announced = True
            return False
        if self.in_warmup():
            return False
        self._active_announced = True
        system_logger.info(
            "[%s] Warm-up complete — scanning on 1-minute candle closes.",
            self.MODE_ACTIVE,
        )
        return True

    def maybe_log_progress(self, interval_seconds: float = 60.0) -> None:
        if not self.in_warmup():
            return
        now = time.monotonic()
        if (
            self._last_progress_log_at > 0
            and (now - self._last_progress_log_at) < interval_seconds
        ):
            return
        self._last_progress_log_at = now
        system_logger.info(
            "[%s] Populating Cache — %.0fs remaining (scanner idle).",
            self.MODE_WARMUP,
            self.remaining_seconds(),
        )
