"""
Shared main-bot heartbeat for watchdog health checks.
Written from the main loop and position monitor so stalls are detected quickly.
"""

from __future__ import annotations

import json
import os
import time
from typing import Optional

from config import Config
from logger import system_logger

_last_write_monotonic: float = 0.0
_monitor_last_tick_mono: float = 0.0
_main_last_tick_mono: float = 0.0


def touch_monitor_loop(*, source: str = "position_monitor") -> None:
    """Record that the TP/SL monitor loop executed (in-memory, never throttled)."""
    global _monitor_last_tick_mono
    _monitor_last_tick_mono = time.monotonic()
    write_bot_heartbeat(source=source)


def touch_main_loop(*, cycle: Optional[int] = None) -> None:
    """Record that the main trading loop completed an iteration."""
    global _main_last_tick_mono
    _main_last_tick_mono = time.monotonic()
    write_bot_heartbeat(cycle=cycle, source="main_loop")


def is_monitor_loop_stale(threshold_seconds: float) -> bool:
    """True when the position monitor has not ticked within threshold_seconds."""
    if _monitor_last_tick_mono <= 0:
        return False
    return (time.monotonic() - _monitor_last_tick_mono) > threshold_seconds


def get_monitor_stall_seconds() -> float:
    if _monitor_last_tick_mono <= 0:
        return 0.0
    return max(time.monotonic() - _monitor_last_tick_mono, 0.0)


def write_bot_heartbeat(*, cycle: Optional[int] = None, source: str = "main") -> None:
    """Touch heartbeat file (throttled to avoid excessive disk I/O)."""
    global _last_write_monotonic

    min_interval = max(float(Config.MONITOR_HEARTBEAT_SECONDS), 5.0)
    now_mono = time.monotonic()
    if (now_mono - _last_write_monotonic) < min_interval:
        return
    _last_write_monotonic = now_mono

    try:
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        payload = {
            "timestamp": time.time(),
            "cycle": cycle,
            "source": source,
            "monitor_tick_mono": _monitor_last_tick_mono,
            "main_tick_mono": _main_last_tick_mono,
        }
        path = Config.WATCHDOG_HEARTBEAT_FILE
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(tmp_path, path)
    except OSError as exc:
        system_logger.debug("Heartbeat file write failed: %s", exc)
