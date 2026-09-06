"""Telegram message formatters for bot command replies."""

from __future__ import annotations

from typing import Any, Optional

from constants import strategy_display_label
from database import DatabaseManager
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
) -> tuple[dict[tuple[str, str], dict[str, Any]], str]:
    """Return ((symbol, side) -> position dict, data source label)."""
    rest_positions = exchange.fetch_all_open_positions_rest()
    if rest_positions is not None:
        positions = rest_positions
        source = "REST"
    else:
        positions = exchange.get_all_open_positions(force_refresh=False)
        source = "cache/WS"

    pos_map: dict[tuple[str, str], dict[str, Any]] = {}
    for pos in positions:
        symbol = str(pos.get("symbol", "")).upper()
        side = str(pos.get("positionSide", "")).upper()
        if symbol and side:
            pos_map[(symbol, side)] = pos
    return pos_map, source


def format_active_positions_message(
    db: DatabaseManager,
    exchange: Any,
) -> str:
    """Format /active — open trades cross-verified against Binance positions."""
    trades = db.get_open_trades()
    if not trades:
        return "📭 <b>Active Positions</b>\n<i>No open trades in database.</i>"

    pos_map, source = _build_exchange_position_map(exchange)
    lines = [
        f"📂 <b>Active Positions ({len(trades)})</b>",
        f"<i>Exchange source: {escape_html(source)}</i>\n",
    ]

    tracked_keys: set[tuple[str, str]] = set()
    for trade in trades[:20]:
        symbol = str(trade.get("symbol", "?")).upper()
        side = str(trade.get("side", "LONG")).upper()
        tracked_keys.add((symbol, side))

        entry = safe_float(trade.get("entry_price"))
        mark = safe_float(exchange.get_market_price(symbol))
        tp1 = safe_float(trade.get("take_profit_1"))
        tp2 = safe_float(trade.get("take_profit_2"))
        tp3 = safe_float(trade.get("take_profit_3"))
        sl = safe_float(trade.get("stop_loss"))
        strategy = str(trade.get("strategy", ""))

        exchange_pos = pos_map.get((symbol, side))
        exchange_qty = safe_float(exchange_pos.get("quantity")) if exchange_pos else 0.0
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
            sync_note = "missing on exchange"

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

    return "\n".join(lines)
