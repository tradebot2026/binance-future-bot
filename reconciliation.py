"""
Startup and periodic position reconciliation between exchange and database.
Purges phantom DB records and stale local recovery files.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Optional, TYPE_CHECKING

from config import Config
from constants import TRADE_STATUS_CLOSED
from logger import error_logger, system_logger
from utils import safe_float, utc_now

if TYPE_CHECKING:
    from database import DatabaseManager
    from exchange import BinanceExchangeManager
    from telegram_bot import TelegramManager


def trade_open_age_seconds(trade: dict[str, Any]) -> float:
    """Seconds since the trade was opened (0 when timestamp missing/invalid)."""
    opened_at_raw = trade.get("opened_at")
    if not opened_at_raw:
        return 0.0
    try:
        opened_at = datetime.fromisoformat(str(opened_at_raw))
        if opened_at.tzinfo is None:
            opened_at = opened_at.replace(tzinfo=timezone.utc)
        return max((utc_now() - opened_at).total_seconds(), 0.0)
    except ValueError:
        return 0.0


def is_within_position_grace_period(trade: dict[str, Any]) -> bool:
    """True while a newly opened trade may not yet appear on exchange/WS feeds."""
    grace = max(Config.POSITION_GRACE_PERIOD_SECONDS, 0.0)
    if grace <= 0:
        return False
    return trade_open_age_seconds(trade) < grace


class PositionReconcileGuard:
    """
    Tracks consecutive 'position missing on exchange' observations.
    Prevents instant RECONCILED_EXTERNAL_CLOSE on API/WS sync lag.
    """

    def __init__(self) -> None:
        self._miss_counts: dict[str, int] = {}

    def note_present(self, trade_id: str) -> None:
        self._miss_counts.pop(str(trade_id), None)

    def note_missing(self, trade_id: str) -> int:
        key = str(trade_id)
        self._miss_counts[key] = self._miss_counts.get(key, 0) + 1
        return self._miss_counts[key]

    def miss_count(self, trade_id: str) -> int:
        return self._miss_counts.get(str(trade_id), 0)

    def should_confirm_external_close(self, trade: dict[str, Any]) -> bool:
        if is_within_position_grace_period(trade):
            return False
        threshold = max(Config.POSITION_RECONCILE_MISS_THRESHOLD, 1)
        return self.miss_count(str(trade["trade_id"])) >= threshold

    def register_missing(self, trade: dict[str, Any]) -> tuple[int, bool]:
        """
        Record one missing observation.
        Returns (miss_count, should_close).
        """
        if is_within_position_grace_period(trade):
            return 0, False
        count = self.note_missing(str(trade["trade_id"]))
        threshold = max(Config.POSITION_RECONCILE_MISS_THRESHOLD, 1)
        return count, count >= threshold


position_reconcile_guard = PositionReconcileGuard()

def reconcile_positions_at_startup(
    exchange: "BinanceExchangeManager",
    db: "DatabaseManager",
    telegram: Optional["TelegramManager"] = None,
) -> None:
    """Run full reconciliation on boot."""
    reconcile_positions(exchange, db, telegram=telegram, context="startup")


def reconcile_positions(
    exchange: "BinanceExchangeManager",
    db: "DatabaseManager",
    telegram: Optional["TelegramManager"] = None,
    context: str = "periodic",
) -> dict[str, int]:
    """
    Align DB state with live exchange positions.
    - DB open + exchange flat  → mark CLOSED (phantom purge)
    - Exchange open + DB missing → log orphan alert
    - Replay orphan fills from disk when possible
    - Purge resolved entries from local recovery files
    """
    system_logger.info("Running %s position reconciliation...", context)

    recovered = recover_orphan_fills_from_disk(db)
    if recovered:
        system_logger.info("Recovered %s orphan fill(s) from disk into DB.", recovered)

    exchange_positions = exchange.fetch_open_positions(force_refresh=False)
    db_trades = db.get_open_trades()

    exchange_keys = {
        (pos["symbol"], pos["positionSide"]) for pos in exchange_positions
    }
    db_keys = {(trade["symbol"], trade["side"]) for trade in db_trades}

    closed_externally = 0
    for trade in db_trades:
        key = (trade["symbol"], trade["side"])
        trade_id = str(trade["trade_id"])
        if key in exchange_keys:
            position_reconcile_guard.note_present(trade_id)
            continue

        if is_within_position_grace_period(trade):
            system_logger.debug(
                "Deferring phantom purge — position grace period: %s %s | id=%s",
                trade["symbol"],
                trade["side"],
                trade_id[:8],
            )
            continue

        miss_count, should_close = position_reconcile_guard.register_missing(trade)
        if not should_close:
            system_logger.warning(
                "Trade missing on exchange (%s/%s) — deferring phantom purge: %s %s | id=%s",
                miss_count,
                Config.POSITION_RECONCILE_MISS_THRESHOLD,
                trade["symbol"],
                trade["side"],
                trade_id[:8],
            )
            continue

        db.update_trade(
            trade_id,
            {
                "status": TRADE_STATUS_CLOSED,
                "closed_at": utc_now().isoformat(),
                "exit_reason": "RECONCILED_PHANTOM_PURGE",
            },
        )
        position_reconcile_guard.note_present(trade_id)
        closed_externally += 1
        system_logger.warning(
            "Purged phantom DB trade (exchange flat): %s %s | id=%s",
            trade["symbol"],
            trade["side"],
            trade_id[:8],
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

    purged_files = purge_stale_recovery_files(db, exchange)

    summary = {
        "recovered_fills": recovered,
        "exchange_open": len(exchange_positions),
        "db_open_before": len(db_trades),
        "phantoms_purged": closed_externally,
        "orphan_exchange": len(orphan_exchange),
        "recovery_lines_purged": purged_files,
    }

    system_logger.info(
        "Reconciliation complete (%s) | exchange_open=%s | phantoms_purged=%s | "
        "orphan_exchange=%s | recovery_lines_purged=%s",
        context,
        summary["exchange_open"],
        summary["phantoms_purged"],
        summary["orphan_exchange"],
        summary["recovery_lines_purged"],
    )
    return summary


def _persist_orphan_alert(positions: list[dict[str, Any]]) -> None:
    """Append orphan exchange positions to a recovery file for ops review."""
    path = os.path.join(Config.DATA_DIR, "orphan_exchange_positions.jsonl")
    try:
        with open(path, "a", encoding="utf-8") as handle:
            for pos in positions:
                record = {
                    "timestamp": utc_now().isoformat(),
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


def purge_stale_recovery_files(
    db: "DatabaseManager",
    exchange: "BinanceExchangeManager",
) -> int:
    """
    Remove resolved entries from local orphan recovery files.
    Returns the number of lines purged across all files.
    """
    purged = 0
    purged += _purge_orphan_fills_file(db)
    purged += _purge_orphan_exchange_file(exchange)
    return purged


def _purge_orphan_fills_file(db: "DatabaseManager") -> int:
    path = os.path.join(Config.DATA_DIR, "orphan_fills.jsonl")
    if not os.path.exists(path):
        return 0

    kept: list[str] = []
    purged = 0
    try:
        with open(path, "r", encoding="utf-8") as handle:
            lines = handle.readlines()
    except OSError:
        return 0

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        try:
            trade_data = json.loads(stripped)
            trade_id = str(trade_data.get("trade_id", ""))
            if trade_id and db.get_trade(trade_id) is not None:
                purged += 1
                continue
        except json.JSONDecodeError:
            pass
        kept.append(stripped)

    try:
        with open(path, "w", encoding="utf-8") as handle:
            for line in kept:
                handle.write(line + "\n")
    except OSError as exc:
        error_logger.error("Failed to purge orphan fills file: %s", exc)

    return purged


def _purge_orphan_exchange_file(exchange: "BinanceExchangeManager") -> int:
    path = os.path.join(Config.DATA_DIR, "orphan_exchange_positions.jsonl")
    if not os.path.exists(path):
        return 0

    live_keys = {
        (pos["symbol"], pos["positionSide"])
        for pos in exchange.fetch_open_positions(force_refresh=False)
    }

    kept: list[str] = []
    purged = 0
    try:
        with open(path, "r", encoding="utf-8") as handle:
            lines = handle.readlines()
    except OSError:
        return 0

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        try:
            record = json.loads(stripped)
            key = (str(record.get("symbol", "")), str(record.get("positionSide", "")))
            if key in live_keys:
                kept.append(stripped)
            else:
                purged += 1
        except json.JSONDecodeError:
            kept.append(stripped)

    try:
        with open(path, "w", encoding="utf-8") as handle:
            for line in kept:
                handle.write(line + "\n")
    except OSError as exc:
        error_logger.error("Failed to purge orphan exchange file: %s", exc)

    return purged
