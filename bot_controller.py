"""
Shared runtime control flags for graceful shutdown, restart, and manual pause.
Used by the main loop and Telegram command handlers.
"""

from __future__ import annotations

import threading


class BotController:
    """Thread-safe control surface for bot lifecycle and entry pause/resume."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._shutdown = threading.Event()
        self._restart = threading.Event()
        self._manual_pause = False
        self._manual_pause_reason = ""

    # ---------------- Lifecycle ----------------

    def request_shutdown(self) -> None:
        with self._lock:
            self._shutdown.set()

    def request_restart(self) -> None:
        with self._lock:
            self._restart.set()
            self._shutdown.set()

    def is_shutdown_requested(self) -> bool:
        return self._shutdown.is_set()

    def is_restart_requested(self) -> bool:
        return self._restart.is_set()

    def clear_restart_flag(self) -> None:
        with self._lock:
            self._restart.clear()

    def reset(self) -> None:
        """Reset flags after a fresh process start."""
        with self._lock:
            self._shutdown.clear()
            self._restart.clear()

    # ---------------- Manual entry pause ----------------

    def pause_entries(self, reason: str = "Manual pause via Telegram") -> None:
        with self._lock:
            self._manual_pause = True
            self._manual_pause_reason = reason

    def resume_entries(self) -> None:
        with self._lock:
            self._manual_pause = False
            self._manual_pause_reason = ""

    def is_manually_paused(self) -> tuple[bool, str]:
        with self._lock:
            if self._manual_pause:
                return True, self._manual_pause_reason or "Entries manually paused."
            return False, ""
