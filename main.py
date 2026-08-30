"""
Main application entry point.
Orchestrates monitoring, scheduling, scanning, execution, and alerting.
Open-position management ALWAYS runs — pause only blocks new entries.
Includes auto-restart on recoverable failures.
"""

from __future__ import annotations

import threading
import time
import traceback
from typing import Any, Optional

from bot_controller import BotController
from config import Config
from critical_alerts import CriticalAlertService
from constants import is_range_strategy
from database import DatabaseManager
from exchange import BinanceExchangeManager
from exceptions import DatabaseError, ExchangeError, ExchangeRateLimitError, OrderExecutionError
from executor import TradeExecutor
from logger import error_logger, system_logger
from manager import TradeManager
from risk_manager import RiskManager
from reconciliation import reconcile_positions, reconcile_positions_at_startup
from scheduler import DailyScheduler
from telegram_bot import TelegramManager
from utils import safe_float, utc_today_str

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

    today = utc_today_str()
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
    if score < (Config.RANGE_MIN_SCORE if is_range_strategy(strategy) else Config.STRATEGY_MIN_SCORE):
        min_required = (
            Config.RANGE_MIN_SCORE if is_range_strategy(strategy) else Config.STRATEGY_MIN_SCORE
        )
        return False, f"score {score:.1f} below minimum {min_required:.1f}", {}

    normalized = {
        "symbol": symbol,
        "action": action,
        "atr": atr,
        "price": price,
        "score": score,
        "strategy": strategy,
        "structure_metadata": candidate.get("structure_metadata") or {},
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

    if risk.get_exchange_open_positions_count() >= Config.MAX_POSITIONS:
        return False, (
            f"Max open positions reached on exchange "
            f"({risk.get_exchange_open_positions_count()}/{Config.MAX_POSITIONS})."
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
    critical_alerts: Optional[CriticalAlertService] = None,
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

            allowed, reason = risk.can_open_trade(
                symbol, strategy=normalized["strategy"]
            )
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
                structure_metadata=normalized.get("structure_metadata"),
            )
            if not result:
                continue

            entries_this_cycle += 1
            risk.record_entry_opened()

            if result.get("orphan_fill"):
                if critical_alerts:
                    critical_alerts.notify(
                        "ORPHAN_FILL",
                        f"Orphan fill on {symbol} {action} orderId={result.get('exchange_order_id')}",
                        force=True,
                    )
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

        except OrderExecutionError as exc:
            error_logger.error(
                "Order execution failed for %s: %s",
                candidate.get("symbol", "?"),
                exc,
            )
            if critical_alerts:
                critical_alerts.notify(
                    "ORDER_FAILURE",
                    f"Candidate execution failed for {candidate.get('symbol', '?')}: {exc}",
                    exc=exc,
                )
            continue
        except Exception as exc:
            error_logger.error(
                "Candidate execution failed for %s: %s",
                candidate.get("symbol", "?"),
                exc,
            )
            error_logger.error(traceback.format_exc())
            if critical_alerts:
                critical_alerts.notify(
                    "UNHANDLED_EXCEPTION",
                    f"Candidate loop error for {candidate.get('symbol', '?')}: {exc}",
                    exc=exc,
                )
            continue


def _start_position_monitor(
    manager: TradeManager,
    stop_event: threading.Event,
) -> threading.Thread:
    """Run position monitoring in a background thread every MONITOR_INTERVAL_SECONDS."""

    def _monitor_loop() -> None:
        while not stop_event.is_set():
            try:
                manager.monitor_open_trades()
            except Exception as exc:
                error_logger.error("Background monitor error: %s", exc)
                error_logger.error(traceback.format_exc())
            stop_event.wait(Config.MONITOR_INTERVAL_SECONDS)

    thread = threading.Thread(target=_monitor_loop, name="position-monitor", daemon=True)
    thread.start()
    system_logger.info(
        "Background position monitor started (interval=%ss).",
        Config.MONITOR_INTERVAL_SECONDS,
    )
    return thread


def _handle_loop_error(
    exc: Exception,
    consecutive_errors: int,
    tg: Optional[TelegramManager],
    critical_alerts: Optional[CriticalAlertService] = None,
    market_data: Any = None,
) -> int:
    consecutive_errors += 1
    error_logger.error(
        "Error in main loop (consecutive=%s): %s",
        consecutive_errors,
        exc,
    )
    error_logger.error(traceback.format_exc())

    if critical_alerts:
        if isinstance(exc, ExchangeRateLimitError):
            critical_alerts.notify("RATE_LIMIT", str(exc), exc=exc)
        elif isinstance(exc, ExchangeError):
            critical_alerts.notify("API_DISCONNECT", str(exc), exc=exc)
        elif isinstance(exc, DatabaseError):
            critical_alerts.notify("DATABASE", str(exc), exc=exc)
        else:
            critical_alerts.notify("UNHANDLED_EXCEPTION", str(exc), exc=exc)

    if isinstance(exc, ExchangeRateLimitError):
        if tg and market_data:
            ban = market_data.get_ban_status()
            if ban and ban.is_banned:
                tg.send_message(market_data.format_ban_message(ban))
            else:
                halted, reason = market_data.is_scan_halted()
                if halted:
                    tg.send_message(
                        "🚫 <b>Binance rate limit</b>\n\n"
                        f"{reason}\n\n"
                        "<i>Scanning paused; position monitoring continues.</i>"
                    )
        sleep_for = Config.RATE_LIMIT_HALT_SECONDS
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


def main(controller: Optional[BotController] = None) -> str:
    """
    Run the trading loop until shutdown is requested.
    Returns 'restart' if a graceful restart was requested, else 'stop'.
    """
    if not _validate_startup():
        return "stop"

    _startup_banner()

    controller = controller or BotController()
    controller.reset()

    db = DatabaseManager()
    exchange = BinanceExchangeManager()

    from market_data_hub import MarketDataHub

    market_data = MarketDataHub(exchange.client)
    exchange.attach_market_data(market_data)

    critical_alerts = CriticalAlertService(db)
    exchange.attach_critical_alerts(critical_alerts)

    scheduler = DailyScheduler(exchange, db, telegram=None, controller=controller)
    risk = RiskManager(exchange, db)

    tg = TelegramManager(
        db=db,
        scheduler=scheduler,
        risk_manager=risk,
        exchange=exchange,
        controller=controller,
    )
    scheduler.telegram = tg
    critical_alerts.attach_telegram(tg)

    startup_ban = exchange.get_startup_ban_status()
    if startup_ban and startup_ban.is_banned:
        ban_msg = market_data.format_ban_message(startup_ban)
        error_logger.critical("Startup ban check: %s", startup_ban.message)
        system_logger.warning(
            "Binance IP ban active — scanning halted until ban expires (~%ss).",
            startup_ban.seconds_remaining,
        )
        tg.send_message(ban_msg)

    market_data.start()
    system_logger.info(
        "Market data hub online (websocket=%s, candle_cache=bar-aligned).",
        Config.ENABLE_WEBSOCKET_STREAMS,
    )

    manager = TradeManager(
        exchange=exchange,
        db=db,
        telegram=tg,
        scheduler=scheduler,
        risk_manager=risk,
    )
    tg.manager = manager

    tg.start_listening()
    mode = "TESTNET" if Config.USE_TESTNET else "MAINNET"
    tg.send_message(f"🚀 <b>Bot started</b> and connected to Binance ({mode}).")

    scanner = MarketScanner(exchange, db) if SCANNER_AVAILABLE else None
    executor = TradeExecutor(exchange, db)
    reporter = ReportGenerator(db) if REPORTER_AVAILABLE else None

    reconcile_positions_at_startup(exchange, db, tg)

    monitor_stop = threading.Event()
    _start_position_monitor(manager, monitor_stop)

    system_logger.info("Initialization complete. Entering main trading loop.")

    last_heartbeat = time.monotonic()
    last_report_day = ""
    last_reconciliation = time.monotonic()
    last_maintenance = time.monotonic()
    cycle = 0
    consecutive_errors = 0

    while not controller.is_shutdown_requested():
        cycle += 1
        loop_started = time.monotonic()

        try:
            # Step 0 — Complete deferred exchange init once ban/halt clears
            exchange.ensure_initialized()

            # Step 1 — Periodic DB/exchange reconciliation (every 15 min)
            now_mono = time.monotonic()
            if now_mono - last_reconciliation >= Config.RECONCILIATION_INTERVAL_SECONDS:
                reconcile_positions(exchange, db, tg, context="periodic")
                db.cleanup_expired_cooldowns()
                last_reconciliation = now_mono

            # Step 1b — Periodic DB purge + VACUUM (default: daily)
            if now_mono - last_maintenance >= Config.DB_MAINTENANCE_INTERVAL_SECONDS:
                db.run_maintenance(retention_days=Config.DB_RETENTION_DAYS, vacuum=True)
                last_maintenance = now_mono

            # Step 2 — Scan and execute only when entries are allowed
            if scanner is None:
                if cycle == 1:
                    system_logger.warning(
                        "scanner.py not found — entries disabled until scanner is added."
                    )
            else:
                allowed, gate_reason = _entries_allowed(scheduler, risk, db)
                if allowed:
                    smc_candidates = scanner.scan_market()
                    _execute_candidates(
                        candidates=smc_candidates,
                        executor=executor,
                        risk=risk,
                        scheduler=scheduler,
                        db=db,
                        tg=tg,
                        critical_alerts=critical_alerts,
                    )

                    if Config.ENABLE_RANGE_REGIME:
                        range_candidates = scanner.scan_range_market()
                        if range_candidates:
                            system_logger.info(
                                "RANGE scan found %s candidate(s) — SMC has priority; "
                                "filling up to %s RANGE slots.",
                                len(range_candidates),
                                Config.MAX_RANGE_POSITIONS,
                            )
                        _execute_candidates(
                            candidates=range_candidates,
                            executor=executor,
                            risk=risk,
                            scheduler=scheduler,
                            db=db,
                            tg=tg,
                            critical_alerts=critical_alerts,
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
                    "Heartbeat | cycle=%s | exchange_open=%s | paused=%s | "
                    "realized_pnl=$%.2f (%.2f%%) | unrealized=$%.2f | drawdown=%.2f%%",
                    cycle,
                    snap.exchange_open_positions,
                    is_paused,
                    snap.daily_realized_pnl,
                    snap.daily_realized_pnl_percent,
                    snap.unrealized_pnl,
                    snap.drawdown_percent,
                )
                last_heartbeat = now

            consecutive_errors = 0

        except KeyboardInterrupt:
            raise
        except Exception as exc:
            consecutive_errors = _handle_loop_error(
                exc, consecutive_errors, tg, critical_alerts, market_data
            )
            continue

        if controller.is_shutdown_requested():
            break

        elapsed = time.monotonic() - loop_started
        sleep_for = max(Config.SCAN_INTERVAL_SECONDS - elapsed, 0.5)
        slept = 0.0
        while slept < sleep_for and not controller.is_shutdown_requested():
            chunk = min(1.0, sleep_for - slept)
            time.sleep(chunk)
            slept += chunk

    # Graceful shutdown
    monitor_stop.set()
    market_data.stop()
    tg.send_message("🛑 <b>Bot stopped</b> — trading loop shut down safely.")
    tg.stop_listening()
    system_logger.info("Trading loop stopped gracefully.")

    if controller.is_restart_requested():
        controller.clear_restart_flag()
        return "restart"

    return "stop"


def run_with_auto_restart() -> None:
    """Restart the bot after recoverable crashes or network failures."""
    restart_count = 0
    controller = BotController()

    while True:
        try:
            outcome = main(controller)
            if outcome == "restart":
                restart_count += 1
                system_logger.info(
                    "Graceful restart requested via Telegram (restart #%s).",
                    restart_count,
                )
                controller.reset()
                time.sleep(Config.RESTART_DELAY_SECONDS)
                continue
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
            controller.reset()
            time.sleep(delay)


if __name__ == "__main__":
    run_with_auto_restart()
