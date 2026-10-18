"""
In-process bot health metrics for /health and /pulse Telegram diagnostics.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Optional

_bot_started_mono: float = 0.0
_last_scan_mono: float = 0.0
_last_scanned_count: int = 0
_universe_total: int = 0
_last_incomplete_klines: int = 0
_scan_history_ready: bool = True
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


def touch_scan_cycle(
    *,
    scanned: int,
    universe_total: int,
    incomplete_klines: int = 0,
) -> None:
    """Update scanner diagnostics after a market scan cycle.

    A live scan tick is not the same as complete history. Incomplete kline
    counts are recorded so callers do not treat skipped evals as healthy Super.
    """
    global _last_scan_mono, _last_scanned_count, _universe_total
    global _last_incomplete_klines, _scan_history_ready
    _last_scan_mono = time.monotonic()
    _last_scanned_count = max(int(scanned), 0)
    _universe_total = max(int(universe_total), 0)
    _last_incomplete_klines = max(int(incomplete_klines), 0)
    _scan_history_ready = _last_incomplete_klines == 0
    try:
        from core.ops_heartbeat import touch_scan_loop

        touch_scan_loop(
            source="scanner",
            history_ready=_scan_history_ready,
            incomplete_klines=_last_incomplete_klines,
        )
    except Exception:
        pass


def get_scan_age_seconds() -> Optional[float]:
    if _last_scan_mono <= 0:
        return None
    return max(time.monotonic() - _last_scan_mono, 0.0)


def get_scan_counts() -> tuple[int, int]:
    return _last_scanned_count, _universe_total


def get_scan_history_ready() -> bool:
    return bool(_scan_history_ready)


def get_incomplete_kline_count() -> int:
    return int(_last_incomplete_klines)


def watchdog_sibling_status(*, now: Optional[float] = None) -> dict[str, Any]:
    """Detect a separate watchdog.py process from its state file.

    main.py must never spawn watchdog.py as a child — it would die with main.
    """
    from config import Config

    path = str(getattr(Config, "WATCHDOG_STATE_FILE", "") or "")
    interval = float(getattr(Config, "WATCHDOG_INTERVAL_SECONDS", 30) or 30)
    stale_after = max(interval * 4.0, 90.0)
    stamp = float(now if now is not None else time.time())
    if not path or not os.path.isfile(path):
        return {
            "fresh": False,
            "age_seconds": None,
            "pid": None,
            "path": path,
            "stale_after": stale_after,
        }
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {
            "fresh": False,
            "age_seconds": None,
            "pid": None,
            "path": path,
            "stale_after": stale_after,
        }
    saved_at = float((data or {}).get("saved_at") or 0.0)
    pid = (data or {}).get("pid")
    age = None if saved_at <= 0 else max(stamp - saved_at, 0.0)
    return {
        "fresh": age is not None and age <= stale_after,
        "age_seconds": age,
        "pid": pid,
        "path": path,
        "stale_after": stale_after,
    }


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
