"""
Shared main-bot heartbeat for watchdog health checks.
Written from the main loop and position monitor so stalls are detected quickly.

Wall-clock fields are required for the *separate* watchdog.py process:
monotonic stamps are process-local and cannot be compared across processes.
"""

from __future__ import annotations

import json
import os
import time
import sys
from typing import Any, Optional

from config import Config
from logger import system_logger

_last_write_monotonic: float = 0.0
_monitor_last_tick_mono: float = 0.0
_main_last_tick_mono: float = 0.0
_reconcile_last_tick_mono: float = 0.0
_scan_last_tick_mono: float = 0.0
_monitor_wall: float = 0.0
_main_wall: float = 0.0
_reconcile_wall: float = 0.0
_scan_wall: float = 0.0
_execution_wall: float = 0.0
_market_wall: float = 0.0
_protection_wall: float = 0.0
_last_scan_duration_ms: float = 0.0
_last_rss_mb: float = 0.0
_scan_history_ready: bool = True
_scan_incomplete_klines: int = 0
_native_open: int = 0
_native_verified: int = 0
_ws_health_state: str = ""
_last_api_error: str = ""
_last_api_error_at: float = 0.0


def touch_monitor_loop(*, source: str = "position_monitor") -> None:
    """Record that the TP/SL monitor loop executed (in-memory, never throttled)."""
    global _monitor_last_tick_mono, _monitor_wall
    _monitor_last_tick_mono = time.monotonic()
    _monitor_wall = time.time()
    write_bot_heartbeat(source=source)


def touch_main_loop(*, cycle: Optional[int] = None) -> None:
    """Record that the main trading loop completed an iteration."""
    global _main_last_tick_mono, _main_wall
    _main_last_tick_mono = time.monotonic()
    _main_wall = time.time()
    write_bot_heartbeat(cycle=cycle, source="main_loop")


def touch_scan_loop(
    *,
    source: str = "scanner",
    duration_ms: Optional[float] = None,
    history_ready: Optional[bool] = None,
    incomplete_klines: Optional[int] = None,
) -> None:
    """Record that a scan cycle finished (does not imply protection is healthy)."""
    global _scan_last_tick_mono, _scan_wall, _last_scan_duration_ms
    global _scan_history_ready, _scan_incomplete_klines
    _scan_last_tick_mono = time.monotonic()
    _scan_wall = time.time()
    if duration_ms is not None:
        _last_scan_duration_ms = max(float(duration_ms), 0.0)
    if history_ready is not None:
        _scan_history_ready = bool(history_ready)
    if incomplete_klines is not None:
        _scan_incomplete_klines = max(int(incomplete_klines), 0)
    write_bot_heartbeat(source=source)


def touch_execution(*, source: str = "executor") -> None:
    global _execution_wall
    _execution_wall = time.time()
    write_bot_heartbeat(source=source)


def touch_market_data(*, source: str = "market_data") -> None:
    global _market_wall
    _market_wall = time.time()
    write_bot_heartbeat(source=source)


def touch_protection(*, source: str = "protection") -> None:
    global _protection_wall
    _protection_wall = time.time()
    write_bot_heartbeat(source=source)


def record_scan_duration_ms(duration_ms: float) -> None:
    global _last_scan_duration_ms
    _last_scan_duration_ms = max(float(duration_ms), 0.0)


def sample_rss_mb() -> float:
    """Best-effort process RSS in MiB. 0.0 when the platform sample is unavailable."""
    global _last_rss_mb
    rss = 0.0
    try:
        import psutil  # type: ignore

        rss = float(psutil.Process(os.getpid()).memory_info().rss) / (1024.0 * 1024.0)
    except Exception:
        rss = 0.0
    if rss <= 0 and os.name == "nt":
        try:
            import ctypes
            from ctypes import wintypes

            class PROCESS_MEMORY_COUNTERS_EX(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                    ("PrivateUsage", ctypes.c_size_t),
                ]

            counters = PROCESS_MEMORY_COUNTERS_EX()
            counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS_EX)
            handle = ctypes.windll.kernel32.GetCurrentProcess()
            if ctypes.windll.psapi.GetProcessMemoryInfo(
                handle, ctypes.byref(counters), counters.cb
            ):
                rss = float(counters.WorkingSetSize) / (1024.0 * 1024.0)
        except Exception:
            rss = 0.0
    if rss <= 0:
        try:
            import resource

            usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            rss = float(usage) / 1024.0
            if os.name == "posix" and sys.platform == "darwin":
                rss = float(usage) / (1024.0 * 1024.0)
        except Exception:
            rss = 0.0
    _last_rss_mb = max(rss, 0.0)
    return _last_rss_mb


def is_monitor_loop_stale(threshold_seconds: float) -> bool:
    """True when the position monitor has not ticked within threshold_seconds."""
    if _monitor_last_tick_mono <= 0:
        return False
    return (time.monotonic() - _monitor_last_tick_mono) > threshold_seconds


def get_monitor_stall_seconds() -> float:
    if _monitor_last_tick_mono <= 0:
        return float("inf")
    return max(time.monotonic() - _monitor_last_tick_mono, 0.0)


def get_main_stall_seconds() -> float:
    if _main_last_tick_mono <= 0:
        return float("inf")
    return max(time.monotonic() - _main_last_tick_mono, 0.0)


def touch_reconcile_loop(*, source: str = "reconcile_worker") -> None:
    """Record that the background reconcile/maintenance worker ticked."""
    global _reconcile_last_tick_mono, _reconcile_wall
    _reconcile_last_tick_mono = time.monotonic()
    _reconcile_wall = time.time()
    write_bot_heartbeat(source=source)


def get_reconcile_stall_seconds() -> float:
    if _reconcile_last_tick_mono <= 0:
        return float("inf")
    return max(time.monotonic() - _reconcile_last_tick_mono, 0.0)


def read_heartbeat_payload() -> dict:
    """Read the latest on-disk heartbeat written by the main/monitor loops."""
    path = Config.WATCHDOG_HEARTBEAT_FILE
    try:
        if not os.path.isfile(path):
            return {}
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}


def evaluate_heartbeat_health(
    payload: dict,
    *,
    now: Optional[float] = None,
    stale_seconds: Optional[float] = None,
    process_running: bool = False,
) -> dict[str, Any]:
    """
    Cross-process health from the heartbeat file.

    Protection health is driven by monitor_wall (TP/SL worker), not by
    scanner/reconcile writes. A live PID with a stale monitor stamp is unhealthy.
    Auto-restart is allowed only when the process is not running.
    """
    now_ts = float(now if now is not None else time.time())
    limit = float(
        stale_seconds
        if stale_seconds is not None
        else getattr(Config, "WATCHDOG_MAIN_STALE_SECONDS", 180)
    )
    data = payload if isinstance(payload, dict) else {}
    monitor_wall = float(data.get("monitor_wall") or 0.0)
    main_wall = float(data.get("main_wall") or 0.0)
    reconcile_wall = float(data.get("reconcile_wall") or 0.0)
    scan_wall = float(data.get("scan_wall") or 0.0)
    file_ts = float(data.get("timestamp") or 0.0)

    def _age(stamp: float) -> Optional[float]:
        if stamp <= 0:
            return None
        return max(now_ts - stamp, 0.0)

    if "monitor_wall" in data:
        monitor_age = _age(monitor_wall)
    else:
        monitor_age = _age(file_ts)
    subsystems = {
        "monitor": monitor_age,
        "main": _age(main_wall),
        "reconcile": _age(reconcile_wall),
        "scanner": _age(scan_wall),
        "execution": _age(float(data.get("execution_wall") or 0.0)),
        "market": _age(float(data.get("market_wall") or 0.0)),
        "protection": _age(float(data.get("protection_wall") or 0.0)),
        "file": _age(file_ts),
    }
    failing = [
        name
        for name, age in subsystems.items()
        if name in {"monitor", "main"} and age is not None and age > limit
    ]
    if monitor_age is None and not process_running:
        failing.append("missing_heartbeat")
    protection_healthy = bool(
        monitor_age is not None and monitor_age <= limit
    )
    if (
        not protection_healthy
        and process_running
        and monitor_age is None
        and _age(file_ts) is not None
        and (_age(file_ts) or 0.0) <= limit
    ):
        # First seconds after boot: manager_init wrote the file before
        # monitor_wall was a dedicated field. Do not treat as hung.
        protection_healthy = True
        failing = [name for name in failing if name != "missing_heartbeat"]
    allow_restart = (not process_running) and (not protection_healthy)
    if protection_healthy:
        reason = "ok"
    elif process_running:
        reason = (
            "hung_process_stale_heartbeat "
            + ",".join(failing or ["monitor"])
        )
    else:
        reason = "process_down " + ",".join(failing or ["missing_heartbeat"])
    return {
        "protection_healthy": protection_healthy,
        "allow_restart": allow_restart,
        "reason": reason,
        "failing": failing,
        "subsystems": subsystems,
        "process_running": process_running,
    }


def set_native_protection_inventory(open_trades: int, verified: int) -> None:
    """Open-trade vs lookup-verified native SL counts for health (not the config flag)."""
    global _native_open, _native_verified
    _native_open = max(int(open_trades or 0), 0)
    _native_verified = max(min(int(verified or 0), _native_open), 0)


def native_unprotected_open() -> int:
    return max(_native_open - _native_verified, 0)


def set_ws_health_state(state: str) -> None:
    global _ws_health_state
    _ws_health_state = str(state or "").upper()


def get_ws_health_state() -> str:
    return str(_ws_health_state or "")


def note_api_error(message: str) -> None:
    global _last_api_error, _last_api_error_at
    _last_api_error = str(message or "")[:200]
    _last_api_error_at = time.time()


def get_last_api_error() -> tuple[str, float]:
    return _last_api_error, _last_api_error_at


def write_bot_heartbeat(*, cycle: Optional[int] = None, source: str = "main") -> None:
    """Touch heartbeat file (throttled to avoid excessive disk I/O)."""
    global _last_write_monotonic

    min_interval = max(float(Config.MONITOR_HEARTBEAT_SECONDS), 1.0)
    if source in ("position_monitor", "manager_init"):
        min_interval = min(min_interval, 1.0)
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
            "pid": os.getpid(),
            "role": "main",
            "monitor_tick_mono": _monitor_last_tick_mono,
            "main_tick_mono": _main_last_tick_mono,
            "reconcile_tick_mono": _reconcile_last_tick_mono,
            "scan_tick_mono": _scan_last_tick_mono,
            "monitor_wall": _monitor_wall,
            "main_wall": _main_wall,
            "reconcile_wall": _reconcile_wall,
            "scan_wall": _scan_wall,
            "execution_wall": _execution_wall,
            "market_wall": _market_wall,
            "protection_wall": _protection_wall,
            "scan_duration_ms": round(_last_scan_duration_ms, 1),
            "scan_history_ready": bool(_scan_history_ready),
            "scan_incomplete_klines": int(_scan_incomplete_klines),
            "rss_mb": round(_last_rss_mb or sample_rss_mb(), 1),
            "native_tp_sl": bool(getattr(Config, "ENABLE_NATIVE_TP_SL", False)),
            "native_open": int(_native_open),
            "native_verified": int(_native_verified),
            "native_unprotected_open": int(native_unprotected_open()),
            "ws_state": str(_ws_health_state or ""),
            "last_api_error": _last_api_error,
            "last_api_error_at": _last_api_error_at,
        }
        path = Config.WATCHDOG_HEARTBEAT_FILE
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(tmp_path, path)
    except OSError as exc:
        system_logger.debug("Heartbeat file write failed: %s", exc)
