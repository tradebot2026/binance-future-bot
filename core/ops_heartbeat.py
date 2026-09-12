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
        }
        path = Config.WATCHDOG_HEARTBEAT_FILE
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(tmp_path, path)
    except OSError as exc:
        system_logger.debug("Heartbeat file write failed: %s", exc)
