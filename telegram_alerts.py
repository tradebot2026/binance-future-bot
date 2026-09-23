"""Telegram message formatters for bot command replies."""

from __future__ import annotations

from typing import Any, Optional

from config import Config
from constants import strategy_display_label
from core.execution_ledger import get_execution_ledger
from database import DatabaseManager
from reconciliation import sync_active_trades_on_demand
from risk_manager import compute_daily_pnl_metrics
from logger import error_logger
from utils import escape_html, safe_float, utc_today_str


def format_runtime_health_block(exchange: Any = None, scanner: Any = None) -> str:
    """Compact WS/API/scanner/execution lines for /status and /watchlist."""
    ws_state = "UNKNOWN"
    api_state = "UNKNOWN"
    exec_state = "UNKNOWN"
    scan_line = "n/a"
    open_positions = 0
    if exchange is not None:
        hub = None
        if hasattr(exchange, "get_market_data_hub"):
            hub = exchange.get_market_data_hub()
        if hub is not None and hasattr(hub, "get_ws_health_snapshot"):
            snap = hub.get_ws_health_snapshot() or {}
            ws_state = str(snap.get("state", "UNKNOWN")).upper()
        if hasattr(exchange, "rest_usage_snapshot"):
            usage = exchange.rest_usage_snapshot() or {}
            api_state = str(usage.get("state", "UNKNOWN"))
        if hasattr(exchange, "get_execution_safety"):
            exec_state, _ = exchange.get_execution_safety()
        positions_fn = getattr(exchange, "get_all_open_positions", None)
        if callable(positions_fn):
            try:
                open_positions = len(positions_fn(force_refresh=False) or [])
            except Exception as exc:
                error_logger.debug("Open-position count for Telegram failed: %s", exc)
    if scanner is not None and hasattr(scanner, "get_watchlist_tiers"):
        tiers = scanner.get_watchlist_tiers()
        scan_line = (
            f"cycle {tiers.get('rotation_cycle', 0)} | "
            f"evaluated {tiers.get('rotation_evaluated', 0)} | "
            f"hot {len(tiers.get('tier1_hot') or [])}"
        )
    ledger = get_execution_ledger().recent_summary()
    queued = get_execution_ledger().queued_count()
    return (
        f"\n🛰 <b>RUNTIME</b>\n"
        f"WS: {escape_html(ws_state)} | API: {escape_html(api_state)}\n"
        f"RATE LIMIT: {escape_html(api_state)} | EXECUTION: {escape_html(exec_state)}\n"
        f"SCANNER: {escape_html(scan_line)}\n"
        f"OPEN POSITIONS: {open_positions} | QUEUE: {queued}\n"
        f"APPROVED: {ledger['approved']} | SUBMITTED: {ledger['submitted']} | "
        f"FILLED: {ledger['filled']} | REJECTED: {ledger['rejected']} | "
        f"FAILED: {ledger['failed']}"
    )


def _position_pnl_percent(
    side: str, entry: float, mark: float
) -> Optional[float]:
    if entry <= 0 or mark <= 0:
        return None
    if side.upper() == "LONG":
        return (mark - entry) / entry * 100.0
    if side.upper() == "SHORT":
        return (entry - mark) / entry * 100.0
    return None


def _build_exchange_position_map(
    exchange: Any,
    *,
    force_rest: bool = False,
) -> tuple[dict[tuple[str, str], dict[str, Any]], str]:
    """Return ((symbol, side) -> position dict, data source label)."""
    source = "WS/cache"
    positions: list[dict[str, Any]] = []

    if force_rest and not exchange.is_rest_blocked()[0]:
        rest_positions = exchange.fetch_all_open_positions_rest(force=True)
        if rest_positions is not None:
            positions = rest_positions
            source = "REST (live)"

    if not positions:
        positions = exchange.get_all_open_positions(force_refresh=force_rest)
        if not positions and not exchange.is_rest_blocked()[0]:
            rest_positions = exchange.fetch_all_open_positions_rest(force=force_rest)
            if rest_positions is not None:
                positions = rest_positions
                source = "REST"

    pos_map: dict[tuple[str, str], dict[str, Any]] = {}
    for pos in positions:
        symbol = str(pos.get("symbol", "")).upper()
        side = str(pos.get("positionSide", "")).upper()
        if symbol and side and safe_float(pos.get("quantity")) > 0:
            pos_map[(symbol, side)] = pos
    return pos_map, source


def _fetch_live_account(exchange: Any):
    """Account snapshot for Telegram — WS/cache first, background REST if allowed."""
    if hasattr(exchange, "fetch_live_account_snapshot"):
        return exchange.fetch_live_account_snapshot(
            include_today_income=True,
            force_refresh=False,
        )
    return None


def format_live_account_header(
    exchange: Any,
    *,
    session_ref: float = 0.0,
    db: Optional[DatabaseManager] = None,
    date_str: Optional[str] = None,
) -> str:
    """Wallet/margin/unrealized/realized block sourced from Binance fapi/v2/account."""
    snap = _fetch_live_account(exchange)
    if snap is None or snap.wallet_balance <= 0:
        balance = safe_float(exchange.get_futures_balance(force_refresh=False))
        unrealized = safe_float(exchange.get_unrealized_pnl_total(force_refresh=False))
        if balance <= 0 and unrealized == 0:
            return (
                "💵 <b>Wallet:</b> unavailable\n"
                "📉 <b>Unrealized:</b> unavailable\n"
                "<i>Source: REST unavailable — check API connectivity</i>\n"
            )
        source_note = "WS/cache fallback" if balance > 0 else "partial cache"
        return (
            f"💵 <b>Wallet:</b> ${balance:.2f}\n"
            f"📉 <b>Unrealized:</b> ${unrealized:.2f}\n"
            f"<i>Source: {escape_html(source_note)}</i>\n"
        )

    realized_today = snap.today_realized_pnl
    source_label = snap.source
    if db is not None and date_str:
        db.sync_daily_stats_from_trades(date_str)
        analytics = db.get_daily_trade_analytics(date_str)
        db_realized = safe_float(analytics.get("total_pnl"))
        closes = int(analytics.get("closes", 0))
        if abs(realized_today) < 0.005 and closes > 0 and abs(db_realized) >= 0.005:
            realized_today = db_realized
            source_label = f"{source_label} | Realized: DB fallback"

    total_pnl = realized_today + snap.unrealized_pnl
    pct_line = ""
    if session_ref > 0:
        pct_line = f" ({total_pnl / session_ref * 100.0:.2f}%)"

    return (
        f"💵 <b>Wallet Balance:</b> ${snap.wallet_balance:.2f}\n"
        f"📊 <b>Margin Balance:</b> ${snap.margin_balance:.2f}\n"
        f"📈 <b>Realized (Today):</b> ${realized_today:.2f}\n"
        f"📉 <b>Unrealized:</b> ${snap.unrealized_pnl:.2f}\n"
        f"📊 <b>Total PnL (Today):</b> ${total_pnl:.2f}{pct_line}\n"
        f"<i>Source: {escape_html(source_label)}</i>\n"
    )


def _resolve_live_wallet_unrealized(exchange: Any) -> tuple[float, float]:
    """Fetch wallet and unrealized PnL once for compact status display."""
    if hasattr(exchange, "fetch_live_account_snapshot"):
        snap = exchange.fetch_live_account_snapshot(
            include_today_income=False,
            force_refresh=False,
        )
        if snap.wallet_balance > 0:
            return snap.wallet_balance, snap.unrealized_pnl
    snap = _fetch_live_account(exchange)
    if snap is not None and snap.wallet_balance > 0:
        return snap.wallet_balance, snap.unrealized_pnl
    wallet = safe_float(exchange.get_futures_balance(force_refresh=False))
    unrealized = safe_float(exchange.get_unrealized_pnl_total(force_refresh=False))
    return wallet, unrealized


def format_daily_status_message(
    exchange: Any,
    db: DatabaseManager,
    stats: dict,
    *,
    today: Optional[str] = None,
    engine_status: str = "RUNNING",
) -> str:
    """Build /status reply — single cohesive daily performance summary."""
    date_str = today or utc_today_str()
    db.sync_daily_stats_from_trades(date_str)
    analytics = db.get_daily_trade_analytics(date_str)
    pf = analytics.get("profit_factor", 0.0)
    pf_display = "∞" if pf == float("inf") else f"{pf:.2f}"

    metrics = compute_daily_pnl_metrics(exchange, db, date_str, force_wallet_refresh=True)
    daily_start = metrics.start_balance
    day_pnl = metrics.equity_day_pnl
    daily_pct = metrics.equity_day_pnl_percent
    wallet = metrics.current_wallet
    unrealized = metrics.unrealized_pnl

    wallet_line = f"${wallet:,.2f}" if wallet > 0 else "unavailable"

    if day_pnl >= 0:
        day_pnl_line = (
            f"📈 <b>Day PnL:</b> +${day_pnl:.2f} ({daily_pct:+.2f}%)"
        )
    else:
        day_pnl_line = (
            f"📉 <b>Day PnL:</b> -${abs(day_pnl):.2f} ({daily_pct:.2f}%)"
        )

    daily_status = escape_html(str(stats.get("status", "ACTIVE")))
    engine_label = escape_html(engine_status.upper())
    closes = int(analytics.get("closes", stats.get("trades_count", 0)))
    entries = int(stats.get("entries_count", 0))

    return (
        f"📊 <b>DAILY PERFORMANCE STATUS</b>\n"
        f"<i>{escape_html(date_str)} UTC</i>\n"
        f"───────────────────────\n"
        f"💵 <b>Wallet Balance:</b> {wallet_line}\n"
        f"📈 <b>Daily Start (UTC):</b> ${daily_start:,.2f}\n"
        f"{day_pnl_line}\n"
        f"📊 <b>Unrealized PnL:</b> ${unrealized:.2f}\n"
        f"\n"
        f"🎯 <b>TRADE STATS (TODAY)</b>\n"
        f"───────────────────────\n"
        f"🏆 <b>Win Rate:</b> {analytics.get('win_rate', 0.0):.1f}% "
        f"({analytics.get('wins', 0)}W / {analytics.get('losses', 0)}L)\n"
        f"⚖️ <b>Profit Factor:</b> {pf_display}\n"
        f"🚀 <b>Entries:</b> {entries}/{Config.MAX_DAILY_TRADES} | "
        f"<b>Closes:</b> {closes}\n"
        f"⚙️ <b>Bot Status:</b> {engine_label} / {daily_status}\n"
        f"{format_runtime_health_block(exchange)}"
    )


def format_daily_stats_footer(db: DatabaseManager, date_str: str) -> str:
    """Compact realized-PnL / win-rate line sourced from closed trades in DB."""
    db.sync_daily_stats_from_trades(date_str)
    analytics = db.get_daily_trade_analytics(date_str)
    pf = analytics.get("profit_factor", 0.0)
    pf_display = "∞" if pf == float("inf") else f"{pf:.2f}"
    return (
        f"\n📊 <b>Today</b> | Realized: ${safe_float(analytics.get('total_pnl')):.2f} | "
        f"Closes: {int(analytics.get('closes', 0))} | "
        f"Win: {analytics.get('win_rate', 0.0):.1f}% "
        f"({analytics.get('wins', 0)}W/{analytics.get('losses', 0)}L) | "
        f"PF: {pf_display}"
    )


def format_active_positions_message(
    db: DatabaseManager,
    exchange: Any,
    telegram: Any = None,
) -> str:
    """Format /active — reconcile DB vs Binance then list verified open trades."""
    from utils import utc_today_str

    manager = getattr(telegram, "manager", None) if telegram is not None else None
    sync_summary = sync_active_trades_on_demand(
        exchange,
        db,
        telegram=telegram,
        manager=manager,
        force_rest=True,
    )
    trades = db.get_open_trades()
    stats_footer = format_daily_stats_footer(db, utc_today_str())
    account_header = format_live_account_header(exchange).rstrip()
    if not trades:
        closed_n = len(sync_summary.get("closed", []))
        if closed_n:
            return (
                f"{account_header}\n\n"
                "📭 <b>Active Positions</b>\n"
                f"<i>Synced with exchange — {closed_n} stale DB trade(s) purged.</i>"
                f"{stats_footer}"
            )
        return (
            f"{account_header}\n\n"
            "📭 <b>Active Positions</b>\n"
            "<i>No open trades in database.</i>"
            f"{stats_footer}"
        )

    pos_map, source = _build_exchange_position_map(exchange, force_rest=True)
    lines = [
        account_header,
        "",
        f"📂 <b>Active Positions ({len(trades)})</b>",
        f"<i>Positions source: {escape_html(source)}</i>",
    ]
    if sync_summary.get("closed"):
        lines.append(
            f"<i>Reconciled: removed {len(sync_summary['closed'])} manually closed trade(s).</i>"
        )
    lines.append("")

    tracked_keys: set[tuple[str, str]] = set()
    for trade in trades[:20]:
        symbol = str(trade.get("symbol", "?")).upper()
        side = str(trade.get("side", "LONG")).upper()
        tracked_keys.add((symbol, side))

        entry = safe_float(trade.get("entry_price"))
        mark = safe_float(exchange.get_live_mark_price(symbol, side, allow_rest=True))
        if mark <= 0:
            mark = safe_float(exchange.fetch_mark_price_rest(symbol))
        tp1 = safe_float(trade.get("take_profit_1"))
        tp2 = safe_float(trade.get("take_profit_2"))
        tp3 = safe_float(trade.get("take_profit_3"))
        sl = safe_float(trade.get("stop_loss"))
        strategy = str(trade.get("strategy", ""))

        exchange_pos = pos_map.get((symbol, side))
        exchange_qty = safe_float(exchange_pos.get("quantity")) if exchange_pos else 0.0
        if exchange_pos:
            mark_from_pos = safe_float(exchange_pos.get("mark_price"))
            if mark_from_pos > 0:
                mark = mark_from_pos
        if exchange_pos and mark <= 0:
            mark = entry

        pnl_pct = _position_pnl_percent(side, entry, mark)
        if exchange_pos and entry > 0 and exchange_qty > 0:
            unrealized = safe_float(exchange_pos.get("unrealized_pnl"))
            notional = entry * exchange_qty
            if notional > 0 and unrealized != 0:
                pnl_pct = unrealized / notional * 100.0

        if exchange_qty > 0:
            sync_icon = "✅"
            sync_note = f"qty={exchange_qty:.4f}"
        else:
            sync_icon = "⚠️"
            sync_note = "missing on exchange (pending sync)"

        pnl_display = f"{pnl_pct:+.2f}%" if pnl_pct is not None else "n/a"
        mark_display = f"{mark:.6f}" if mark > 0 else "n/a"

        lines.append(
            f"{sync_icon} <b>{escape_html(symbol)}</b> {escape_html(side)}"
        )
        lines.append(
            f"   {escape_html(strategy_display_label(strategy))} | "
            f"{escape_html(sync_note)}"
        )
        lines.append(f"   Entry: {entry:.6f} | Mark: {mark_display} | uPnL: {pnl_display}")
        tp_parts = [f"TP1 {tp1:.6f}"] if tp1 > 0 else []
        if tp2 > 0:
            tp_parts.append(f"TP2 {tp2:.6f}")
        if tp3 > 0:
            tp_parts.append(f"TP3 {tp3:.6f}")
        tp_line = " | ".join(tp_parts) if tp_parts else "TP n/a"
        sl_line = f"SL {sl:.6f}" if sl > 0 else "SL n/a"
        lines.append(f"   {tp_line} | {sl_line}\n")

    orphans = [
        (sym, side, pos)
        for (sym, side), pos in pos_map.items()
        if (sym, side) not in tracked_keys
    ]
    lines.append(format_daily_stats_footer(db, utc_today_str()))

    if orphans:
        lines.append(f"🚨 <b>Untracked on exchange ({len(orphans)})</b>")
        for sym, side, pos in orphans[:5]:
            qty = safe_float(pos.get("quantity"))
            lines.append(f"• {escape_html(sym)} {escape_html(side)} qty={qty:.4f}")
        if len(orphans) > 5:
            lines.append(f"<i>…and {len(orphans) - 5} more</i>")

    return "\n".join(lines)


def format_watchlist_message(
    *,
    tier1_hot: list[str],
    tier1_background: list[str],
    tier1_full: list[str],
    tier2_rows: list[tuple[str, str, float]],
    hot_scan_interval: float,
    tier2_display_limit: int = 20,
    tier2_near_miss: Optional[list[tuple[str, str, float, float]]] = None,
    rotation_cycle: int = 0,
    rotation_evaluated: int = 0,
    exchange: Any = None,
) -> str:
    """Format /watchlist — Tier 1 hot scan universe + Tier 2 execution candidates."""
    lines = ["📡 <b>Scan Watchlist</b>\n"]
    if rotation_cycle or rotation_evaluated:
        lines.append(
            f"🔄 <b>Rotation:</b> batch/cycle {rotation_cycle} | "
            f"evaluated {rotation_evaluated}"
        )

    lines.append(
        f"🔥 <b>Tier 1 — Hot Scan</b> ({len(tier1_hot)}) "
        f"<i>every {hot_scan_interval:.0f}s</i>"
    )
    if tier1_hot:
        hot_preview = ", ".join(escape_html(s) for s in tier1_hot[:20])
        if len(tier1_hot) > 20:
            hot_preview += f" … +{len(tier1_hot) - 20} more"
        lines.append(hot_preview)
    else:
        lines.append("<i>No hot symbols — universe not refreshed yet.</i>")

    bg_count = len(tier1_background)
    full_count = len(tier1_full)
    lines.append(
        f"\n📋 <b>Tier 1 — Background Rotation</b> ({bg_count}) "
        f"| full universe {full_count}"
    )
    if tier1_background:
        bg_preview = ", ".join(escape_html(s) for s in tier1_background[:15])
        if bg_count > 15:
            bg_preview += f" … +{bg_count - 15} more"
        lines.append(bg_preview)
    else:
        lines.append("<i>No background symbols.</i>")

    lines.append(f"\n⭐ <b>Tier 2 — Execution Candidates</b> ({len(tier2_rows)})")
    if tier2_rows:
        for sym, strat, score in tier2_rows[:tier2_display_limit]:
            lines.append(
                f"• {escape_html(sym)} | {escape_html(strategy_display_label(strat))} "
                f"| norm={score:.0f}"
            )
    else:
        lines.append("<i>No Tier 2 candidates promoted yet.</i>")

    near_miss = tier2_near_miss or []
    if near_miss and not tier2_rows:
        lines.append(f"\n📊 <b>Recent scan scores (not yet promoted)</b>")
        for sym, strat, norm, raw in near_miss[:8]:
            lines.append(
                f"• {escape_html(sym)} | {escape_html(strategy_display_label(strat))} "
                f"| raw={raw:.1f} norm={norm:.0f}"
            )

    lines.append(format_runtime_health_block(exchange))
    return "\n".join(lines)
