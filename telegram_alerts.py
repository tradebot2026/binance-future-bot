"""Telegram message formatters for bot command replies."""

from __future__ import annotations

from typing import Any, Optional

from constants import strategy_display_label
from database import DatabaseManager
from reconciliation import sync_active_trades_on_demand
from utils import escape_html, safe_float


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

    sync_summary = sync_active_trades_on_demand(
        exchange,
        db,
        telegram=telegram,
        force_rest=True,
    )
    trades = db.get_open_trades()
    stats_footer = format_daily_stats_footer(db, utc_today_str())
    if not trades:
        closed_n = len(sync_summary.get("closed", []))
        if closed_n:
            return (
                "📭 <b>Active Positions</b>\n"
                f"<i>Synced with exchange — {closed_n} stale DB trade(s) purged.</i>"
                f"{stats_footer}"
            )
        return (
            "📭 <b>Active Positions</b>\n"
            "<i>No open trades in database.</i>"
            f"{stats_footer}"
        )

    pos_map, source = _build_exchange_position_map(exchange, force_rest=True)
    lines = [
        f"📂 <b>Active Positions ({len(trades)})</b>",
        f"<i>Exchange source: {escape_html(source)}</i>",
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
        mark = safe_float(exchange.get_market_price(symbol, side))
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
) -> str:
    """Format /watchlist — Tier 1 hot scan universe + Tier 2 execution candidates."""
    lines = ["📡 <b>Scan Watchlist</b>\n"]

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

    return "\n".join(lines)
