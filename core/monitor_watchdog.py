"""
Detects a stalled position-monitor loop and triggers WS stream recovery.
Runs in its own daemon thread — independent of Telegram and the scan loop.
"""

from __future__ import annotations

import threading
import time
import traceback
from typing import Any, Optional

from config import Config
from core.ops_heartbeat import get_monitor_stall_seconds, is_monitor_loop_stale
from logger import error_logger, system_logger


def start_monitor_watchdog(
    market_data: Any,
    *,
    telegram: Any = None,
    stop_event: Optional[threading.Event] = None,
) -> threading.Thread:
    """Start background watchdog; returns the daemon thread handle."""

    def _loop() -> None:
        system_logger.info(
            "Monitor watchdog started | stall_threshold=%ss | check_interval=%ss",
            Config.MONITOR_LOOP_STALL_SECONDS,
            Config.MONITOR_WATCHDOG_INTERVAL_SECONDS,
        )
        last_alert_at = 0.0
        while stop_event is None or not stop_event.is_set():
            try:
                if is_monitor_loop_stale(Config.MONITOR_LOOP_STALL_SECONDS):
                    stall = get_monitor_stall_seconds()
                    error_logger.critical(
                        "Position monitor loop stalled for %.1fs (threshold=%ss) — "
                        "reconnecting market data streams.",
                        stall,
                        Config.MONITOR_LOOP_STALL_SECONDS,
                    )
                    if hasattr(market_data, "reconnect_stale_streams"):
                        market_data.reconnect_stale_streams(
                            f"monitor loop stall {stall:.0f}s"
                        )
                    elif hasattr(market_data, "_request_reconnect"):
                        market_data._request_reconnect(
                            f"monitor loop stall {stall:.0f}s"
                        )

                    now = time.monotonic()
                    if telegram is not None and (now - last_alert_at) >= 300:
                        last_alert_at = now
                        try:
                            telegram.send_message(
                                "⚠️ <b>Monitor stall recovered</b>\n"
                                f"Position monitor was silent for {stall:.0f}s.\n"
                                "<i>WebSocket streams reconnected automatically.</i>"
                            )
                        except Exception as exc:
                            error_logger.warning(
                                "Monitor watchdog Telegram alert failed: %s", exc
                            )
            except Exception as exc:
                error_logger.error("Monitor watchdog error: %s", exc)
                error_logger.error(traceback.format_exc())

            interval = max(float(Config.MONITOR_WATCHDOG_INTERVAL_SECONDS), 1.0)
            if stop_event is not None:
                stop_event.wait(interval)
            else:
                time.sleep(interval)

    thread = threading.Thread(target=_loop, name="monitor-watchdog", daemon=True)
    thread.start()
    return thread
