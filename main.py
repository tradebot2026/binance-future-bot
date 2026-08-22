"""
Main application entry point.
Orchestrates monitoring, scheduling, scanning, execution, and alerting.
Open-position management ALWAYS runs — pause only blocks new entries.
Includes auto-restart on recoverable failures.
"""

from __future__ import annotations

import time
import traceback
from typing import Any, Optional

from config import Config
from database import DatabaseManager
from exchange import BinanceExchangeManager
from exceptions import DatabaseError, ExchangeError, ExchangeRateLimitError
from executor import TradeExecutor
from logger import error_logger, system_logger
from manager import TradeManager
from risk_manager import RiskManager
from reconciliation import reconcile_positions_at_startup
from scheduler import DailyScheduler
from telegram_bot import TelegramManager
from utils import safe_float

try:
    from reporter import ReportGenerator

    REPORTER_AVAILABLE = True
except ImportError:
    REPORTER_AVAILABLE = False

try:
    from scanner import MarketScanner

    SCANNER_AVAILABLE = True
except ImportError:
    SCANNER_AVAILABLE = False


def _startup_banner() -> None:
    mode = "TESTNET" if Config.USE_TESTNET else "MAINNET"
    system_logger.info("=" * 60)
    system_logger.info("Binance Futures Trading Bot starting (%s)", mode)
    system_logger.info(
        "Loop interval=%ss | max_positions=%s | scan=%s | reporter=%s",
        Config.SCAN_INTERVAL_SECONDS,
        Config.MAX_POSITIONS,
        "ready" if SCANNER_AVAILABLE else "MISSING",
        "ready" if REPORTER_AVAILABLE else "MISSING",
    )
    system_logger.info("=" * 60)


def _validate_startup() -> bool:
    if not Config.validate_config():
        error_logger.error(
            "Invalid configuration. Check BINANCE_API_KEY / BINANCE_API_SECRET in .env."
        )
        return False
    return True


def _maybe_export_daily_report(reporter: Any, last_report_day: str) -> str:
    if not REPORTER_AVAILABLE or reporter is None:
        return last_report_day
    if not Config.ENABLE_DAILY_REPORT:
        return last_report_day

    from datetime import UTC, datetime

    today = datetime.now(UTC).strftime("%Y-%m-%d")
    if today == last_report_day:
        return last_report_day

    path = reporter.export_daily_stats()
    if path:
        system_logger.info("Daily report exported: %s", path)
    perf_path = reporter.export_performance_summary()
    if perf_path:
        system_logger.info("Performance summary exported: %s", perf_path)
    return today


def _validate_candidate(candidate: dict[str, Any]) -> tuple[bool, str, dict[str, Any]]:
    """Validate scanner output before attempting execution."""
    symbol = str(candidate.get("symbol", "")).strip().upper()
    action = str(candidate.get("action", "NEUTRAL")).upper()
    atr = safe_float(candidate.get("atr"))
    price = safe_float(candidate.get("price"))
    score = safe_float(candidate.get("score"))
    strategy = str(candidate.get("strategy", "DEFAULT"))

    if not symbol:
        return False, "missing symbol", {}
    if action not in ("LONG", "SHORT"):
        return False, f"invalid action {action}", {}
    if atr <= 0:
        return False, f"invalid ATR ({atr})", {}
    if price <= 0:
        return False, f"invalid price ({price})", {}
    if score < Config.STRATEGY_MIN_SCORE:
        return False, f"score {score:.1f} below minimum {Config.STRATEGY_MIN_SCORE:.1f}", {}

    normalized = {
        "symbol": symbol,
        "action": action,
        "atr": atr,
        "price": price,
        "score": score,
        "strategy": strategy,
    }
    return True, "", normalized


def _entries_allowed(
    scheduler: DailyScheduler,
    risk: RiskManager,
    db: DatabaseManager,
) -> tuple[bool, str]:
    is_paused, pause_reason = scheduler.is_entry_paused()
    if is_paused:
        return False, pause_reason

    if db.get_active_trades_count() >= Config.MAX_POSITIONS:
        return False, (
            f"Max open positions reached ({db.get_active_trades_count()}/"
            f"{Config.MAX_POSITIONS})."
        )

    allowed, reason = risk.can_open_trade()
    if not allowed:
        return False, reason

    return True, ""


def _execute_candidates(
    candidates: list[dict[str, Any]],
    executor: TradeExecutor,
    risk: RiskManager,
    scheduler: DailyScheduler,
    db: DatabaseManager,
    tg: TelegramManager,
) -> None:
    entries_this_cycle = 0

    for candidate in candidates:
        if entries_this_cycle >= Config.MAX_ENTRIES_PER_CYCLE:
            system_logger.info(
                "Entry cap reached for this cycle (%s/%s).",
                entries_this_cycle,
                Config.MAX_ENTRIES_PER_CYCLE,
            )
            break

        try:
            valid, validation_reason, normalized = _validate_candidate(candidate)
            if not valid:
                system_logger.info(
                    "Skipping invalid candidate %s: %s",
                    candidate.get("symbol", "?"),
                    validation_reason,
                )
                continue

            symbol = normalized["symbol"]
            action = normalized["action"]

            allowed, gate_reason = _entries_allowed(scheduler, risk, db)
            if not allowed:
                system_logger.info("Entry gate closed: %s", gate_reason)
                break

            allowed, reason = risk.can_open_trade(symbol)
            if not allowed:
                system_logger.info("Skipping %s: %s", symbol, reason)
                continue

            result: Optional[dict[str, Any]] = executor.execute_trade(
                symbol=symbol,
                action=action,
                atr=normalized["atr"],
                current_price=normalized["price"],
                strategy=normalized["strategy"],
                score=normalized["score"],
            )
            if not result:
                continue

            entries_this_cycle += 1
            risk.record_entry_opened()

            if result.get("orphan_fill"):
                tg.send_message(
                    "🚨 <b>CRITICAL: Orphan fill</b>\n"
                    f"Order filled on exchange but DB logging failed.\n"
                    f"Symbol: {symbol} | Side: {action}\n"
                    f"Order ID: {result.get('exchange_order_id', 'unknown')}\n"
                    f"Trade ID: {result.get('trade_id', 'unknown')}\n"
                    "<i>Check data/orphan_fills.jsonl — will auto-recover on next restart.</i>"
                )
            else:
                tg.send_trade_alert(
                    action=result["action"],
                    symbol=result["symbol"],
                    price=float(result["entry_price"]),
                    tp1=float(result["take_profit_1"]),
                    sl=float(result["stop_loss"]),
                    tp2=float(result.get("take_profit_2", 0.0)),
                    tp3=float(result.get("take_profit_3", 0.0)),
                    score=normalized["score"],
                    strategy=normalized["strategy"],
                )

            scheduler.notify_trade_event()
            risk.notify_trade_event()

        except Exception as exc:
            error_logger.error(
                "Candidate execution failed for %s: %s",
                candidate.get("symbol", "?"),
                exc,
            )
            error_logger.error(traceback.format_exc())
            continue


def _handle_loop_error(
    exc: Exception,
    consecutive_errors: int,
    tg: Optional[TelegramManager],
) -> int:
    consecutive_errors += 1
    error_logger.error(
        "Error in main loop (consecutive=%s): %s",
        consecutive_errors,
        exc,
    )
    error_logger.error(traceback.format_exc())

    if isinstance(exc, ExchangeRateLimitError):
        sleep_for = min(Config.RESTART_DELAY_SECONDS * 3, 120)
    elif isinstance(exc, (ExchangeError, DatabaseError)):
        sleep_for = min(Config.RESTART_DELAY_SECONDS * 2, 90)
    else:
        sleep_for = Config.RESTART_DELAY_SECONDS

    if tg and consecutive_errors >= Config.ERROR_ALERT_THRESHOLD:
        tg.send_message(
            "⚠️ <b>Bot loop error</b>\n"
            f"Consecutive errors: {consecutive_errors}\n"
            f"Type: {type(exc).__name__}\n"
            f"Message: {exc}\n"
            f"Retrying in {sleep_for}s…"
        )

    time.sleep(sleep_for)
    return consecutive_errors


def main() -> None:
    if not _validate_startup():
        return

    _startup_banner()

    db = DatabaseManager()
    exchange = BinanceExchangeManager()

    scheduler = DailyScheduler(exchange, db, telegram=None)
    risk = RiskManager(exchange, db)

    tg = TelegramManager(
        db=db,
        scheduler=scheduler,
        risk_manager=risk,
        exchange=exchange,
    )
    scheduler.telegram = tg

    tg.start_listening()
    mode = "TESTNET" if Config.USE_TESTNET else "MAINNET"
    tg.send_message(f"🚀 <b>Bot started</b> and connected to Binance ({mode}).")

    scanner = MarketScanner(exchange, db) if SCANNER_AVAILABLE else None
    executor = TradeExecutor(exchange, db)
    manager = TradeManager(
        exchange=exchange,
        db=db,
        telegram=tg,
        scheduler=scheduler,
        risk_manager=risk,
    )
    reporter = ReportGenerator(db) if REPORTER_AVAILABLE else None

    reconcile_positions_at_startup(exchange, db, tg)

    system_logger.info("Initialization complete. Entering main trading loop.")

    last_heartbeat = time.monotonic()
    last_report_day = ""
    cycle = 0
    consecutive_errors = 0

    while True:
        cycle += 1
        loop_started = time.monotonic()

        try:
            # Step 1 — ALWAYS monitor open trades (even when entries paused)
            try:
                manager.monitor_open_trades()
            except Exception as exc:
                consecutive_errors = _handle_loop_error(exc, consecutive_errors, tg)
                continue

            # Step 2 — Scan and execute only when entries are allowed
            if scanner is None:
                if cycle == 1:
                    system_logger.warning(
                        "scanner.py not found — entries disabled until scanner is added."
                    )
            else:
                allowed, gate_reason = _entries_allowed(scheduler, risk, db)
                if allowed:
                    candidates = scanner.scan_market()
                    _execute_candidates(
                        candidates=candidates,
                        executor=executor,
                        risk=risk,
                        scheduler=scheduler,
                        db=db,
                        tg=tg,
                    )
                elif gate_reason:
                    system_logger.info("Entries paused: %s", gate_reason)

            # Step 3 — Optional daily CSV export (once per UTC day)
            last_report_day = _maybe_export_daily_report(reporter, last_report_day)

            # Step 4 — Heartbeat
            now = time.monotonic()
            if now - last_heartbeat >= Config.HEARTBEAT_SECONDS:
                snap = risk.get_risk_snapshot()
                is_paused, _ = scheduler.is_entry_paused()
                system_logger.info(
                    "Heartbeat | cycle=%s | open=%s | paused=%s | "
                    "daily_pnl=$%.2f (%.2f%%) | unrealized=$%.2f | drawdown=%.2f%%",
                    cycle,
                    snap.open_positions,
                    is_paused,
                    snap.daily_pnl,
                    snap.daily_pnl_percent,
                    snap.unrealized_pnl,
                    snap.drawdown_percent,
                )
                last_heartbeat = now

            consecutive_errors = 0

        except KeyboardInterrupt:
            raise
        except Exception as exc:
            consecutive_errors = _handle_loop_error(exc, consecutive_errors, tg)
            continue

        elapsed = time.monotonic() - loop_started
        sleep_for = max(Config.SCAN_INTERVAL_SECONDS - elapsed, 0.5)
        time.sleep(sleep_for)


def run_with_auto_restart() -> None:
    """Restart the bot after recoverable crashes or network failures."""
    restart_count = 0

    while True:
        try:
            main()
            break
        except KeyboardInterrupt:
            system_logger.info("Bot stopped manually (KeyboardInterrupt).")
            break
        except Exception as exc:
            restart_count += 1
            error_logger.critical(
                "Fatal crash (restart #%s): %s",
                restart_count,
                exc,
            )
            error_logger.critical(traceback.format_exc())

            if Config.MAX_AUTO_RESTARTS > 0 and restart_count >= Config.MAX_AUTO_RESTARTS:
                error_logger.critical(
                    "Max auto-restarts (%s) reached — stopping bot.",
                    Config.MAX_AUTO_RESTARTS,
                )
                break

            delay = min(
                Config.RESTART_DELAY_SECONDS * restart_count,
                Config.MAX_RESTART_DELAY_SECONDS,
            )
            system_logger.warning("Auto-restarting in %ss…", delay)
            time.sleep(delay)


if __name__ == "__main__":
    run_with_auto_restart()
