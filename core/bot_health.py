"""
In-process bot health metrics for /health and /pulse Telegram diagnostics.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

_bot_started_mono: float = 0.0
_last_scan_mono: float = 0.0
_last_scanned_count: int = 0
_universe_total: int = 0
_monitor_stall_recoveries: int = 0
_monitor_watchdog_alive: bool = False


def mark_bot_started() -> None:
    """Record process start time for uptime reporting."""
    global _bot_started_mono
    _bot_started_mono = time.monotonic()


def get_uptime_seconds() -> float:
    if _bot_started_mono <= 0:
        return 0.0
    return max(time.monotonic() - _bot_started_mono, 0.0)


def touch_scan_cycle(*, scanned: int, universe_total: int) -> None:
    """Update scanner diagnostics after a market scan cycle."""
    global _last_scan_mono, _last_scanned_count, _universe_total
    _last_scan_mono = time.monotonic()
    _last_scanned_count = max(int(scanned), 0)
    _universe_total = max(int(universe_total), 0)


def get_scan_age_seconds() -> Optional[float]:
    if _last_scan_mono <= 0:
        return None
    return max(time.monotonic() - _last_scan_mono, 0.0)


def get_scan_counts() -> tuple[int, int]:
    return _last_scanned_count, _universe_total


def note_monitor_stall_recovery() -> None:
    global _monitor_stall_recoveries
    _monitor_stall_recoveries += 1


def get_monitor_stall_recoveries() -> int:
    return _monitor_stall_recoveries


def set_monitor_watchdog_active(active: bool) -> None:
    global _monitor_watchdog_alive
    _monitor_watchdog_alive = active


def is_monitor_watchdog_active() -> bool:
    return _monitor_watchdog_alive


@dataclass
class ScanHealthSnapshot:
    last_scan_seconds_ago: Optional[float]
    scanned: int
    universe_total: int
