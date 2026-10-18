"""Dimensional health — never collapse incomplete history or virtual SL into HEALTHY."""

from __future__ import annotations

from typing import Any, Optional

from config import Config
from core.bot_health import (
    get_incomplete_kline_count,
    get_scan_history_ready,
    watchdog_sibling_status,
)
from core.ops_heartbeat import (
    get_main_stall_seconds,
    get_monitor_stall_seconds,
    get_reconcile_stall_seconds,
    read_heartbeat_payload,
)
from core.restart_state import read_restart_state


HEALTHY = "HEALTHY"
DEGRADED = "DEGRADED"
NOT_READY = "NOT_READY"
UNPROTECTED = "UNPROTECTED"


def _stall(age: float, limit: float) -> bool:
    if age < 0 or age == float("inf"):
        return True
    return age > limit


def system_health_snapshot(
    *,
    open_trades: int = 0,
    db_ok: bool = True,
    ws_state: str = "",
) -> dict[str, Any]:
    """Return overall + per-subsystem status. Protection is independent of scanner."""
    hb = read_heartbeat_payload()
    mon_limit = max(float(getattr(Config, "MONITOR_LOOP_STALL_SECONDS", 45.0)), 45.0)
    main_limit = max(float(getattr(Config, "HEARTBEAT_SECONDS", 30)) * 3.0, 90.0)
    rec_limit = max(float(getattr(Config, "RECONCILIATION_INTERVAL_SECONDS", 900)) * 1.5, 180.0)

    monitor_stall = get_monitor_stall_seconds()
    main_stall = get_main_stall_seconds()
    reconcile_stall = get_reconcile_stall_seconds()
    history_ready = get_scan_history_ready()
    incomplete = get_incomplete_kline_count()
    sibling = watchdog_sibling_status()
    native_on = bool(Config.ENABLE_NATIVE_TP_SL)
    restart = read_restart_state()
    open_n = max(int(open_trades), 0)

    dims: dict[str, str] = {}
    dims["process"] = HEALTHY
    if not hb.get("main_wall"):
        dims["main_loop"] = NOT_READY
    else:
        dims["main_loop"] = DEGRADED if _stall(main_stall, main_limit) else HEALTHY
    if not hb.get("monitor_wall"):
        dims["protection_monitor"] = NOT_READY
    else:
        dims["protection_monitor"] = (
            DEGRADED if _stall(monitor_stall, mon_limit) else HEALTHY
        )
    if not hb.get("reconcile_wall"):
        dims["reconciliation"] = NOT_READY
    else:
        dims["reconciliation"] = (
            DEGRADED if _stall(reconcile_stall, rec_limit) else HEALTHY
        )
    if history_ready:
        dims["candle_history"] = HEALTHY
        dims["scanner"] = HEALTHY
    else:
        dims["candle_history"] = NOT_READY
        dims["scanner"] = NOT_READY
    ws = str(ws_state or hb.get("ws_state") or "").upper()
    if ws in {"", "UNKNOWN", "UNAVAILABLE"}:
        dims["market_data"] = NOT_READY if not hb else DEGRADED
    elif ws in {"STALE", "STOPPED", "ERROR", "RECONNECTING", "WARMING", "DEGRADED"}:
        dims["market_data"] = DEGRADED
    else:
        dims["market_data"] = HEALTHY
    dims["database"] = HEALTHY if db_ok else DEGRADED
    dims["watchdog_sibling"] = HEALTHY if sibling.get("fresh") else DEGRADED

    if native_on:
        # Config flag is not proof. Missing inventory is DEGRADED, not HEALTHY.
        if "native_unprotected_open" not in hb and "native_verified" not in hb:
            dims["exchange_protection"] = DEGRADED
        elif int(hb.get("native_unprotected_open") or 0) > 0:
            dims["exchange_protection"] = UNPROTECTED
        else:
            dims["exchange_protection"] = HEALTHY
    elif open_n > 0:
        dims["exchange_protection"] = UNPROTECTED
    else:
        dims["exchange_protection"] = HEALTHY

    if restart.get("exhausted") and not restart.get("manual_start_at"):
        dims["process"] = DEGRADED

    overall = HEALTHY
    values = list(dims.values())
    if UNPROTECTED in values:
        overall = UNPROTECTED
    elif NOT_READY in values:
        overall = NOT_READY
    elif DEGRADED in values:
        overall = DEGRADED

    return {
        "overall": overall,
        "dimensions": dims,
        "native_tp_sl": native_on,
        "open_trades": open_n,
        "scan_history_ready": history_ready,
        "scan_incomplete_klines": incomplete,
        "watchdog_sibling_fresh": bool(sibling.get("fresh")),
        "restart_exhausted": bool(restart.get("exhausted")),
        "reasons": _reasons(dims, incomplete, native_on, open_n, sibling),
    }


def _reasons(
    dims: dict[str, str],
    incomplete: int,
    native_on: bool,
    open_n: int,
    sibling: dict[str, Any],
) -> list[str]:
    out: list[str] = []
    if dims.get("exchange_protection") == UNPROTECTED:
        out.append(
            f"Native TP/SL OFF with {open_n} open trade(s) — virtual SL dies with this process"
        )
    if dims.get("candle_history") == NOT_READY:
        out.append(f"Incomplete candle history ({incomplete} symbol(s)) — Super eval skipped")
    if not sibling.get("fresh"):
        out.append("Sibling watchdog.py not detected — start it in a separate process")
    if not native_on:
        out.append("ENABLE_NATIVE_TP_SL=false — virtual SL dies with this process")
    return out
