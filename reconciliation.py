"""
Startup and runtime position reconciliation between exchange and database.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any, Optional, TYPE_CHECKING

from config import Config
from constants import TRADE_STATUS_CLOSED
from logger import error_logger, system_logger
from utils import safe_float

if TYPE_CHECKING:
    from database import DatabaseManager
    from exchange import BinanceExchangeManager
    from telegram_bot import TelegramManager


def reconcile_positions_at_startup(
    exchange: "BinanceExchangeManager",
    db: "DatabaseManager",
    telegram: Optional["TelegramManager"] = None,
) -> None:
    """
    Align DB state with exchange positions on boot.
    - DB open + exchange flat  → mark CLOSED (external/manual close)
    - Exchange open + DB missing → log critical orphan (manual action required)
    """
    system_logger.info("Running startup position reconciliation...")

    recovered = recover_orphan_fills_from_disk(db)
    if recovered:
        system_logger.info("Recovered %s orphan fill(s) from disk into DB.", recovered)

    exchange_positions = exchange.get_all_open_positions()
    db_trades = db.get_open_trades()

    exchange_keys = {
        (pos["symbol"], pos["positionSide"]) for pos in exchange_positions
    }
    db_keys = {(trade["symbol"], trade["side"]) for trade in db_trades}

    closed_externally = 0
    for trade in db_trades:
        key = (trade["symbol"], trade["side"])
        if key in exchange_keys:
            continue

        db.update_trade(
            str(trade["trade_id"]),
            {
                "status": TRADE_STATUS_CLOSED,
                "closed_at": datetime.now(UTC).isoformat(),
                "exit_reason": "RECONCILED_EXTERNAL_CLOSE",
            },
        )
        closed_externally += 1
        system_logger.warning(
            "Reconciled stale DB trade (exchange flat): %s %s | id=%s",
            trade["symbol"],
            trade["side"],
            str(trade["trade_id"])[:8],
        )

    orphan_exchange: list[dict[str, Any]] = []
    for pos in exchange_positions:
        key = (pos["symbol"], pos["positionSide"])
        if key not in db_keys:
            orphan_exchange.append(pos)

    if orphan_exchange:
        _persist_orphan_alert(orphan_exchange)
        for pos in orphan_exchange:
            error_logger.error(
                "ORPHAN EXCHANGE POSITION (no DB record): %s %s qty=%s entry=%.6f",
                pos["symbol"],
                pos["positionSide"],
                pos["quantity"],
                safe_float(pos.get("entry_price")),
            )

        if telegram:
            lines = [
                "🚨 <b>Orphan exchange positions detected</b>",
                "These exist on Binance but are not tracked in the DB:",
            ]
            for pos in orphan_exchange[:10]:
                lines.append(
                    f"• {pos['symbol']} {pos['positionSide']} "
                    f"qty={pos['quantity']} entry={safe_float(pos.get('entry_price')):.4f}"
                )
            lines.append("<i>Manual review or close recommended.</i>")
            telegram.send_message("\n".join(lines))

    system_logger.info(
        "Reconciliation complete | exchange_open=%s | db_open=%s | "
        "closed_externally=%s | orphan_exchange=%s",
        len(exchange_positions),
        len(db_trades),
        closed_externally,
        len(orphan_exchange),
    )


def _persist_orphan_alert(positions: list[dict[str, Any]]) -> None:
    """Append orphan exchange positions to a recovery file for ops review."""
    path = os.path.join(Config.DATA_DIR, "orphan_exchange_positions.jsonl")
    try:
        with open(path, "a", encoding="utf-8") as handle:
            for pos in positions:
                record = {
                    "timestamp": datetime.now(UTC).isoformat(),
                    **pos,
                }
                handle.write(json.dumps(record) + "\n")
    except OSError as exc:
        error_logger.error("Failed to persist orphan exchange alert file: %s", exc)


def recover_orphan_fills_from_disk(db: "DatabaseManager") -> int:
    """
    Attempt to replay orphan fill records saved when DB logging failed.
    Returns the number of successfully recovered trades.
    """
    path = os.path.join(Config.DATA_DIR, "orphan_fills.jsonl")
    if not os.path.exists(path):
        return 0

    recovered = 0
    remaining_lines: list[str] = []

    try:
        with open(path, "r", encoding="utf-8") as handle:
            lines = handle.readlines()
    except OSError as exc:
        error_logger.error("Failed to read orphan fills file: %s", exc)
        return 0

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            trade_data = json.loads(line)
        except json.JSONDecodeError:
            remaining_lines.append(line)
            continue

        trade_id = str(trade_data.get("trade_id", ""))
        if not trade_id:
            remaining_lines.append(line)
            continue

        if db.get_trade(trade_id) is not None:
            continue

        try:
            db.log_trade(trade_data)
            recovered += 1
            system_logger.info(
                "Recovered orphan fill into DB: %s %s | id=%s",
                trade_data.get("symbol"),
                trade_data.get("side"),
                trade_id[:8],
            )
        except Exception as exc:
            error_logger.error("Failed to recover orphan fill %s: %s", trade_id, exc)
            remaining_lines.append(line)

    try:
        with open(path, "w", encoding="utf-8") as handle:
            for line in remaining_lines:
                handle.write(line + "\n")
    except OSError as exc:
        error_logger.error("Failed to rewrite orphan fills file: %s", exc)

    return recovered
