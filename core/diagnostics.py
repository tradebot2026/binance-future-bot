"""
Cache-only bot diagnostics for Telegram /health and /diagnostics.
Never performs Binance REST — reads memory, heartbeat files, and SQLite only.
"""

from __future__ import annotations

from typing import Any, Optional

from config import Config
from utils import escape_html, safe_float


def _format_duration(seconds: float) -> str:
    if seconds < 0 or seconds == float("inf"):
        return "n/a"
    seconds = int(seconds)
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _thread_alive(obj: Any, attr: str) -> str:
    thread = getattr(obj, attr, None) if obj is not None else None
    alive = getattr(thread, "is_alive", lambda: False)()
    return "UP" if alive else "DOWN"


def _loop_line(age: float, *, fresh_seconds: float, label: str) -> str:
    if age < 0 or age == float("inf"):
        return f"{label}: Unknown (no tick yet)"
    if age <= fresh_seconds:
        return f"{label}: Alive ({_format_duration(age)} ago)"
    return f"{label}: STALE ({_format_duration(age)} ago)"


def wal_health_snapshot(db: Any) -> dict[str, Any]:
    """SQLite ping + journal_mode. No VACUUM / FULL checkpoint."""
    out: dict[str, Any] = {
        "ok": False,
        "journal_mode": "UNKNOWN",
        "open_trades": -1,
        "error": "",
    }
    if db is None:
        out["error"] = "db not attached"
        return out
    try:
        with db.connection() as conn:
            row = conn.execute("PRAGMA journal_mode").fetchone()
            mode = str(row[0] if row else "UNKNOWN").upper()
            conn.execute("SELECT 1")
        out["journal_mode"] = mode
        try:
            out["open_trades"] = int(db.get_active_trades_count())
        except Exception:
            out["open_trades"] = -1
        out["ok"] = True
        return out
    except Exception as exc:
        out["error"] = str(exc)[:120]
        return out


def format_cache_only_health_message(
    *,
    exchange: Any = None,
    market_data: Any = None,
    scanner: Any = None,
    controller: Any = None,
    scheduler: Any = None,
    db: Any = None,
    manager: Any = None,
) -> str:
    """Build the /health|/diagnostics report from in-process caches and SQLite."""
    from core.bot_health import (
        get_monitor_stall_recoveries,
        get_scan_age_seconds,
        get_scan_counts,
        get_uptime_seconds,
        is_monitor_watchdog_active,
    )
    from core.ops_heartbeat import (
        get_main_stall_seconds,
        get_monitor_stall_seconds,
        get_reconcile_stall_seconds,
        read_heartbeat_payload,
    )
    from utils import utc_now

    now = utc_now()
    refreshed = now.strftime("%Y-%m-%d %H:%M:%S UTC")
    hb_payload = read_heartbeat_payload()
    hb_ts = safe_float(hb_payload.get("timestamp"))
    if hb_ts > 0:
        hb_age = max(now.timestamp() - hb_ts, 0.0)
    else:
        hb_age = get_main_stall_seconds()
        if hb_age == float("inf"):
            hb_age = -1.0

    main_stall = get_main_stall_seconds()
    monitor_stall = get_monitor_stall_seconds()
    reconcile_stall = get_reconcile_stall_seconds()
    heartbeat_ok = hb_age >= 0 and hb_age <= max(
        float(Config.HEARTBEAT_SECONDS) * 3, 90.0
    )
    if heartbeat_ok:
        hb_line = f"Alive (Last ping: {_format_duration(hb_age)} ago)"
    elif hb_age < 0:
        hb_line = "Unknown (no heartbeat yet)"
    else:
        hb_line = f"STALE (Last ping: {_format_duration(hb_age)} ago)"

    hub = market_data
    if hub is None and exchange is not None:
        getter = getattr(exchange, "get_market_data_hub", None)
        hub = getter() if callable(getter) else getattr(exchange, "_market_data", None)

    ws_state = "UNAVAILABLE"
    ticker_age = -1.0
    book_age = -1.0
    user_age = -1.0
    kline_streams = 0
    tickers = 0
    feeds = 0
    user_ok = False
    reconnecting = False
    if hub is not None and hasattr(hub, "get_ws_health_snapshot"):
        ws = hub.get_ws_health_snapshot() or {}
        ws_state = str(ws.get("state", "UNKNOWN")).upper()
        feeds = int(ws.get("active_feeds") or 0)
        tickers = int(ws.get("ticker_symbols") or 0)
        kline_streams = int(ws.get("kline_streams") or 0)
        ticker_age = safe_float(ws.get("ticker_age_seconds"), -1.0)
        book_age = safe_float(ws.get("book_age_seconds"), -1.0)
        user_age = safe_float(ws.get("user_stream_age_seconds"), -1.0)
        user_ok = bool(ws.get("user_stream_ok"))
        reconnecting = bool(ws.get("reconnect_in_progress"))

    user_line = "OK" if user_ok else "STALE/quiet"
    if user_age == float("inf") or user_age < 0:
        user_age_txt = "n/a"
    else:
        user_age_txt = _format_duration(user_age)

    engine_stalled = main_stall > max(float(Config.HEARTBEAT_SECONDS) * 2, 60.0)
    entries_paused = False
    pause_reason = ""
    if scheduler is not None:
        entries_paused, pause_reason = scheduler.is_entry_paused()
    elif controller is not None:
        entries_paused, pause_reason = controller.is_manually_paused()
    if engine_stalled:
        engine_line = "STALLED"
    elif entries_paused:
        engine_line = (
            f"IDLE ({escape_html(pause_reason[:60])})" if pause_reason else "IDLE"
        )
    else:
        engine_line = "RUNNING"

    scan_age = get_scan_age_seconds()
    scanned, universe_total = get_scan_counts()
    if scanner is not None and universe_total <= 0:
        pipeline = getattr(scanner, "_pipeline", None)
        if pipeline is not None:
            syms = getattr(pipeline, "_last_universe_symbols", []) or []
            universe_total = len(syms)
            scanned = universe_total
    if scan_age is None:
        scan_line = f"No scan yet | Universe: {universe_total}"
    else:
        scan_line = (
            f"Last tick {_format_duration(scan_age)} ago | "
            f"{scanned}/{universe_total} symbols"
        )

    watchdog_active = is_monitor_watchdog_active()
    stall_recoveries = get_monitor_stall_recoveries()
    uptime = _format_duration(get_uptime_seconds())
    watchdog_line = (
        f"{'ACTIVE' if watchdog_active else 'INACTIVE'} "
        f"(stall recoveries: {stall_recoveries})"
    )

    mode = "TESTNET" if Config.USE_TESTNET else "MAINNET"
    cycle = hb_payload.get("cycle")
    cycle_bit = f" cycle {cycle}" if cycle is not None else ""

    api_line = "UNKNOWN"
    rest_lanes = ""
    bucket_line = ""
    if exchange is not None and hasattr(exchange, "rest_usage_snapshot"):
        usage = exchange.rest_usage_snapshot() or {}
        api_line = (
            f"{usage.get('state', 'UNKNOWN')} | "
            f"{int(usage.get('requests_1m') or 0)} req/min | "
            f"weight {int(usage.get('used_weight_1m') or 0)}/"
            f"{int(usage.get('weight_limit') or usage.get('hard_weight_cap') or 0)}"
        )
        rest_lanes = (
            f"ORDER {int(usage.get('weight_order_1m') or 0)}  "
            f"HOT {int(usage.get('weight_hot_1m') or 0)}  "
            f"NORMAL {int(usage.get('weight_normal_1m') or 0)}"
        )
        throttle = safe_float(usage.get("weight_throttle_remaining_seconds"))
        safety = safe_float(usage.get("safety_remaining_seconds"))
        rest_lanes += (
            f" | throttle {_format_duration(throttle)} | "
            f"safety {_format_duration(safety)}"
        )
    bucket = getattr(exchange, "_rest_token_bucket", None) if exchange else None
    snap_fn = getattr(bucket, "snapshot", None)
    if callable(snap_fn):
        bsnap = snap_fn() or {}
        bucket_line = (
            f"tokens {bsnap.get('tokens', '?')}/{bsnap.get('capacity', '?')} | "
            f"hard-stop {_format_duration(safe_float(bsnap.get('hard_stop_remaining')))}"
        )

    wal = wal_health_snapshot(db)
    max_pos = int(getattr(Config, "MAX_POSITIONS", 0) or 0)
    if wal["ok"]:
        open_n = int(wal.get("open_trades") or 0)
        if open_n < 0:
            db_line = f"{wal['journal_mode']} | ping OK | open trades n/a"
        else:
            db_line = (
                f"{wal['journal_mode']} | ping OK | "
                f"open trades {open_n}/{max_pos or open_n}"
            )
    else:
        err = escape_html(str(wal.get("error") or "unavailable")[:80])
        db_line = f"{wal['journal_mode']} | FAIL ({err})"

    errors_line = "<i>none</i>"
    if db is not None and hasattr(db, "get_recent_critical_errors"):
        try:
            rows = db.get_recent_critical_errors(limit=3) or []
        except Exception:
            rows = []
        if rows:
            bits = []
            for item in rows:
                cat = escape_html(str(item.get("category", "?")))
                msg = escape_html(str(item.get("message", ""))[:80])
                bits.append(f"• <b>{cat}</b> {msg}")
            errors_line = "\n".join(bits)

    threads = (
        f"monitor {_thread_alive(manager, '_fast_monitor')} | "
        f"ticks {_thread_alive(manager, '_tick_worker')} | "
        f"closes {_thread_alive(manager, '_close_worker')} | "
        f"ws-wd {_thread_alive(hub, '_watchdog_thread')}"
    )

    hb_fresh = max(float(Config.HEARTBEAT_SECONDS) * 3, 90.0)
    rec_fresh = max(float(Config.RECONCILIATION_INTERVAL_SECONDS) * 1.5, 180.0)
    mon_fresh = max(float(Config.MONITOR_LOOP_STALL_SECONDS), 45.0)

    return (
        f"🩺 <b>BOT HEALTH AND DIAGNOSTICS</b>\n"
        f"🌐 <b>Mode:</b> {escape_html(mode)}\n"
        f"<i>Cache-only — no Binance REST</i>\n\n"
        f"⏱️ <b>Last Refreshed:</b> {refreshed}\n"
        f"💓 <b>Heartbeat:</b> {escape_html(hb_line)}\n"
        f"🔁 <b>Loops</b>\n"
        f"  {_loop_line(main_stall, fresh_seconds=hb_fresh, label='Main')}{cycle_bit}\n"
        f"  {_loop_line(monitor_stall, fresh_seconds=mon_fresh, label='Monitor')}\n"
        f"  {_loop_line(reconcile_stall, fresh_seconds=rec_fresh, label='Reconcile')}\n"
        f"🌐 <b>WebSockets:</b> {escape_html(ws_state)}"
        f"{' (reconnect)' if reconnecting else ''} | {feeds} feeds | {tickers} tickers\n"
        f"  miniTicker: {_format_duration(ticker_age)} | "
        f"book: {_format_duration(book_age)}\n"
        f"  user-data: {escape_html(user_line)} ({user_age_txt}) | "
        f"klines: {kline_streams} streams\n"
        f"🛡 <b>REST:</b> {escape_html(api_line)}\n"
        + (f"  lanes: {escape_html(rest_lanes)}\n" if rest_lanes else "")
        + (f"  bucket: {escape_html(bucket_line)}\n" if bucket_line else "")
        + (
            f"🗄 <b>Database:</b> {db_line}\n"
            f"🧠 <b>Engine:</b> {engine_line}\n"
            f"📊 <b>Scanner:</b> {escape_html(scan_line)}\n"
            f"🛡️ <b>Watchdog:</b> {escape_html(watchdog_line)}\n"
            f"🧵 <b>Workers:</b> {escape_html(threads)}\n"
            f"⏳ <b>Uptime:</b> {uptime}\n"
            f"⚠️ <b>Recent errors:</b>\n{errors_line}\n"
            f"📡 <b>Response:</b> {now.strftime('%H:%M:%S UTC')}"
        )
    )
