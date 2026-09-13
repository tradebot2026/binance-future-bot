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
from exchange import ClosedPositionPnl
from logger import error_logger, system_logger, trade_logger
from utils import safe_float, utc_now

if TYPE_CHECKING:
    from database import DatabaseManager
    from exchange import BinanceExchangeManager
    from telegram_bot import TelegramManager


def trade_opened_at_ms(trade: dict[str, Any]) -> int:
    """Parse trade opened_at to epoch milliseconds (0 when missing)."""
    opened_at_raw = trade.get("opened_at")
    if not opened_at_raw:
        return 0
    try:
        opened_at = datetime.fromisoformat(str(opened_at_raw))
        if opened_at.tzinfo is None:
            opened_at = opened_at.replace(tzinfo=timezone.utc)
        return int(opened_at.timestamp() * 1000)
    except ValueError:
        return 0


def is_exchange_sync_close_reason(reason: str) -> bool:
    """True when the position was closed on Binance outside the bot close worker."""
    upper = str(reason).upper()
    return upper.startswith("RECONCILED")


def resolve_exchange_close_pnl(
    exchange: "BinanceExchangeManager",
    db: "DatabaseManager",
    trade: dict[str, Any],
) -> ClosedPositionPnl:
    """
    Fetch authoritative realized PnL from Binance userTrades/income.
    Falls back to local price estimate only when REST history is unavailable.
    """
    symbol = str(trade.get("symbol", "")).upper()
    side = str(trade.get("side", "LONG")).upper()
    opened_ms = trade_opened_at_ms(trade)

    if opened_ms > 0:
        api_pnl = exchange.fetch_closed_position_realized_pnl(
            symbol, side, opened_ms
        )
        if api_pnl.source in ("userTrades", "income"):
            if api_pnl.exit_price <= 0:
                mark = safe_float(exchange.get_market_price(symbol, side))
                if mark > 0:
                    api_pnl.exit_price = mark
            trade_logger.info(
                "[%s] Exchange PnL from %s | realized=%.4f | fills=%s | exit=%.6f",
                symbol,
                api_pnl.source,
                api_pnl.realized_pnl,
                api_pnl.fill_count,
                api_pnl.exit_price,
            )
            return api_pnl

    exit_price = safe_float(exchange.get_market_price(symbol, side))
    estimated = db.estimate_trade_pnl(trade, exit_price)
    trade_logger.warning(
        "[%s] Exchange PnL unavailable — using local estimate %.4f (exit=%.6f)",
        symbol,
        estimated,
        exit_price,
    )
    return ClosedPositionPnl(
        realized_pnl=estimated,
        exit_price=exit_price,
        source="estimate",
    )


def finalize_reconciled_trade_close(
    exchange: "BinanceExchangeManager",
    db: "DatabaseManager",
    trade: dict[str, Any],
    exit_reason: str,
) -> float:
    """Close a DB trade using Binance-reported realized PnL when the exchange is flat."""
    symbol = str(trade.get("symbol", "")).upper()
    side = str(trade.get("side", "LONG")).upper()
    resolved = resolve_exchange_close_pnl(exchange, db, trade)
    exit_price = resolved.exit_price
    if exit_price <= 0:
        exit_price = safe_float(exchange.get_market_price(symbol, side))

    balance = exchange.get_futures_balance(force_refresh=False)
    total_pnl = db.close_trade_and_sync_stats(
        trade,
        exit_price=exit_price,
        exit_reason=exit_reason,
        pnl=resolved.realized_pnl,
        book_daily_pnl=True,
        current_balance=balance if balance > 0 else None,
    )
    exchange.clear_position_cache(symbol, side)
    return total_pnl


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


def rest_position_quantity(
    exchange: "BinanceExchangeManager",
    symbol: str,
    position_side: str,
) -> Optional[float]:
    """REST-backed quantity; None means verification unavailable."""
    if exchange.is_rest_blocked()[0]:
        return None
    return exchange.get_position_quantity_rest(symbol, position_side)


def confirm_external_close_allowed(
    exchange: "BinanceExchangeManager",
    trade: dict[str, Any],
) -> bool:
    """
    True only when miss threshold is met AND REST confirms positionAmt == 0.
    Never closes on WS/cache misses alone.
    """
    trade_id = str(trade["trade_id"])
    symbol = str(trade["symbol"]).upper()
    position_side = str(trade.get("side", "LONG")).upper()

    if is_within_position_grace_period(trade):
        return False

    if not position_reconcile_guard.should_confirm_external_close(trade):
        return False

    rest_qty = rest_position_quantity(exchange, symbol, position_side)
    if rest_qty is None:
        system_logger.warning(
            "Deferring external close — REST verification unavailable: %s %s | id=%s",
            symbol,
            position_side,
            trade_id[:8],
        )
        return False

    if rest_qty > 0:
        position_reconcile_guard.note_present(trade_id)
        system_logger.info(
            "External close cancelled — REST confirms open position: %s %s qty=%.8f",
            symbol,
            position_side,
            rest_qty,
        )
        return False

    return True


def sync_active_trades_on_demand(
    exchange: "BinanceExchangeManager",
    db: "DatabaseManager",
    telegram: Optional["TelegramManager"] = None,
    *,
    force_rest: bool = True,
) -> dict[str, Any]:
    """
    Authoritative sync for /active and on-demand checks.
    Closes DB trades immediately when REST confirms the exchange is flat.
    """
    summary: dict[str, Any] = {
        "closed": [],
        "exchange_open": 0,
        "rest_used": False,
        "rest_blocked": exchange.is_rest_blocked()[0],
    }

    exchange_keys: set[tuple[str, str]] = set()
    rest_blocked = exchange.is_rest_blocked()[0]

    if force_rest and not rest_blocked:
        rest_positions = exchange.fetch_all_open_positions_rest(force=True)
        summary["rest_used"] = True
        if rest_positions is not None:
            for pos in rest_positions:
                symbol = str(pos.get("symbol", "")).upper()
                side = str(pos.get("positionSide", "")).upper()
                qty = safe_float(pos.get("quantity"))
                if symbol and side and qty > 0:
                    exchange_keys.add((symbol, side))
                    exchange.seed_position_after_fill(
                        symbol,
                        side,
                        qty,
                        safe_float(pos.get("entry_price")),
                    )

    if not exchange_keys:
        for pos in exchange.get_all_open_positions(force_refresh=not rest_blocked):
            symbol = str(pos.get("symbol", "")).upper()
            side = str(pos.get("positionSide", "")).upper()
            qty = safe_float(pos.get("quantity"))
            if symbol and side and qty > 0:
                exchange_keys.add((symbol, side))

    summary["exchange_open"] = len(exchange_keys)

    for trade in db.get_open_trades():
        symbol = str(trade.get("symbol", "")).upper()
        side = str(trade.get("side", "LONG")).upper()
        trade_id = str(trade["trade_id"])
        key = (symbol, side)

        if key in exchange_keys:
            position_reconcile_guard.note_present(trade_id)
            continue

        if is_within_position_grace_period(trade):
            continue

        rest_qty: Optional[float] = None
        if not rest_blocked:
            rest_qty = exchange.get_position_quantity_rest(symbol, side)
            if rest_qty is None:
                raw = exchange.fetch_symbol_positions_rest(symbol, force=True)
                if raw is not None:
                    rest_qty = 0.0
                    for pos in raw:
                        if str(pos.get("positionSide", "")).upper() == side:
                            rest_qty = abs(safe_float(pos.get("positionAmt")))
                            break

        if rest_qty is None:
            system_logger.warning(
                "On-demand sync deferred close — REST unavailable: %s %s | id=%s",
                symbol,
                side,
                trade_id[:8],
            )
            continue

        if rest_qty > 0:
            position_reconcile_guard.note_present(trade_id)
            exchange.seed_position_after_fill(
                symbol,
                side,
                rest_qty,
                safe_float(trade.get("entry_price")),
            )
            exchange_keys.add(key)
            continue

        closed_pnl = finalize_reconciled_trade_close(
            exchange, db, trade, "RECONCILED_MANUAL_CLOSE"
        )
        position_reconcile_guard.note_present(trade_id)
        summary["closed"].append(
            {"symbol": symbol, "side": side, "pnl": closed_pnl}
        )
        system_logger.warning(
            "Purged manually closed trade from DB: %s %s | id=%s | exchange_pnl=%.4f",
            symbol,
            side,
            trade_id[:8],
            closed_pnl,
        )

    summary["exchange_open"] = len(exchange_keys)

    if summary["closed"] and telegram:
        lines = [
            "🔄 <b>Position sync</b>",
            f"Closed {len(summary['closed'])} DB trade(s) no longer on exchange:",
        ]
        for row in summary["closed"][:10]:
            if isinstance(row, dict):
                sym = row.get("symbol", "?")
                side = row.get("side", "?")
                pnl = safe_float(row.get("pnl"))
                lines.append(f"• {sym} {side} | PnL ${pnl:.4f}")
            else:
                lines.append(f"• {row}")
        telegram.send_message("\n".join(lines))

    return summary


def symbol_blocked_for_new_entry(
    exchange: "BinanceExchangeManager",
    db: "DatabaseManager",
    symbol: str,
) -> tuple[bool, str]:
    """
    True when a new entry must not be opened on this symbol.
    REST is authoritative; WS/cache used only when REST is unavailable.
    """
    symbol = symbol.upper()

    if db.get_open_trades_for_symbol(symbol):
        return True, f"Active DB trade exists for {symbol}"

    rest_open = exchange.symbol_has_open_position_rest(symbol)
    if rest_open is True:
        return True, f"Exchange REST confirms open position on {symbol}"

    for side in ("LONG", "SHORT"):
        if exchange.has_open_position(symbol, side):
            return True, f"{side} position visible on {symbol} (WS/cache)"

    return False, ""


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

    if exchange.rest_account_reads_blocked():
        system_logger.info(
            "Reconciliation REST limited — using WS/cache; REST verify on close only."
        )

    recovered = recover_orphan_fills_from_disk(db)
    if recovered:
        system_logger.info("Recovered %s orphan fill(s) from disk into DB.", recovered)

    if context == "startup" and not exchange.rest_account_reads_blocked():
        rest_positions = exchange.fetch_all_open_positions_rest(force=True)
        if rest_positions is not None:
            exchange_positions = rest_positions
        else:
            exchange_positions = exchange.fetch_open_positions(force_refresh=False)
            system_logger.warning(
                "Startup reconciliation using cached/WS positions — REST unavailable."
            )
    else:
        exchange_positions = exchange.fetch_open_positions(force_refresh=False)
    db_trades = db.get_open_trades()

    exchange_keys = {
        (pos["symbol"], pos["positionSide"]) for pos in exchange_positions
    }
    db_keys = {(trade["symbol"], trade["side"]) for trade in db_trades}

    closed_externally = 0
    try:
        for trade in db_trades:
            try:
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

                if not confirm_external_close_allowed(exchange, trade):
                    system_logger.warning(
                        "Phantom purge deferred — REST did not confirm flat: %s %s | id=%s",
                        trade["symbol"],
                        trade["side"],
                        trade_id[:8],
                    )
                    continue

                closed_pnl = finalize_reconciled_trade_close(
                    exchange, db, trade, "RECONCILED_PHANTOM_PURGE"
                )
                position_reconcile_guard.note_present(trade_id)
                closed_externally += 1
                system_logger.warning(
                    "Purged phantom DB trade (exchange flat): %s %s | id=%s | exchange_pnl=%.4f",
                    trade["symbol"],
                    trade["side"],
                    trade_id[:8],
                    closed_pnl,
                )
            except Exception as exc:
                error_logger.error(
                    "Reconciliation failed for trade %s: %s",
                    trade.get("trade_id", "?"),
                    exc,
                )
    except Exception as exc:
        error_logger.error("Reconciliation phantom purge loop failed: %s", exc)

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
