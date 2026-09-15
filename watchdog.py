#!/usr/bin/env python3
"""
Independent watchdog for Binance Futures bot safety.

Runs alongside main.py in a separate process/screen tab:
  - Monitors open positions via REST (mark price vs DB TP/SL levels)
  - Emergency market-closes if a breach persists while main bot is inactive
  - Checks main bot heartbeat / process health and alerts via Telegram

Usage:
  python watchdog.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from config import Config
from constants import (
    TRADE_STATUS_CLOSED,
    TRADE_STATUS_TP1_HIT,
    TRADE_STATUS_TP2_HIT,
)
from database import DatabaseManager
from exchange import BinanceExchangeManager
from exit_coordinator import claim_exit, exit_claim_active, release_exit
from exceptions import OrderExecutionError, PositionAlreadyClosedError
from logger import setup_logger
from telegram_bot import TelegramManager
from utils import escape_html, safe_float, utc_now, utc_today_str

watchdog_logger = setup_logger("Watchdog", "watchdog.log")


@dataclass
class ExitSignal:
    level: str
    reason: str
    partial: bool
    quantity: float


class WatchdogState:
    """Persistent breach timers and alert deduplication."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.breach_first_seen: dict[str, float] = {}
        self.alert_at: dict[str, float] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.isfile(self.path):
            return
        try:
            with open(self.path, encoding="utf-8") as handle:
                data = json.load(handle)
            self.breach_first_seen = {
                str(k): float(v) for k, v in (data.get("breach_first_seen") or {}).items()
            }
            self.alert_at = {
                str(k): float(v) for k, v in (data.get("alert_at") or {}).items()
            }
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
            watchdog_logger.warning("Could not load watchdog state: %s", exc)

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        payload = {
            "breach_first_seen": self.breach_first_seen,
            "alert_at": self.alert_at,
            "saved_at": time.time(),
        }
        with open(self.path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)

    def note_breach(self, key: str) -> float:
        now = time.monotonic()
        if key not in self.breach_first_seen:
            self.breach_first_seen[key] = now
        return now - self.breach_first_seen[key]

    def clear_breach(self, prefix: str) -> None:
        drop = [k for k in self.breach_first_seen if k.startswith(prefix)]
        for key in drop:
            self.breach_first_seen.pop(key, None)

    def should_alert(self, key: str, interval_seconds: float) -> bool:
        now = time.monotonic()
        last = self.alert_at.get(key, 0.0)
        if (now - last) < interval_seconds:
            return False
        self.alert_at[key] = now
        return True


class PositionWatchdog:
    """Self-healing safety layer for virtual TP/SL when main.py stalls."""

    def __init__(self) -> None:
        Config.setup_directories()
        if not Config.validate_config():
            watchdog_logger.error("Invalid configuration — check API keys and .env.")
            sys.exit(1)

        self.db = DatabaseManager(Config.DB_PATH)
        self.exchange = BinanceExchangeManager()
        self.telegram = TelegramManager(db=self.db, exchange=self.exchange)
        self.state = WatchdogState(Config.WATCHDOG_STATE_FILE)
        self._running = True

    def run(self) -> None:
        watchdog_logger.info(
            "Watchdog started | interval=%ss | breach_grace=%ss | main_stale=%ss | "
            "auto_restart=%s | emergency_only_when_main_down=%s",
            Config.WATCHDOG_INTERVAL_SECONDS,
            Config.WATCHDOG_BREACH_GRACE_SECONDS,
            Config.WATCHDOG_MAIN_STALE_SECONDS,
            Config.WATCHDOG_AUTO_RESTART_MAIN,
            Config.WATCHDOG_EMERGENCY_ONLY_WHEN_MAIN_DOWN,
        )
        self.telegram.send_message(
            "🛡 <b>Watchdog online</b> — safety net active.\n"
            f"<i>Auto-restart main: {'ON' if Config.WATCHDOG_AUTO_RESTART_MAIN else 'OFF'} | "
            f"Emergency close when main stalled &gt; {Config.WATCHDOG_MAIN_STALE_SECONDS}s</i>"
        )

        while self._running:
            loop_start = time.monotonic()
            try:
                main_healthy = self.check_main_bot_health()
                self.check_open_positions(main_bot_healthy=main_healthy)
            except KeyboardInterrupt:
                watchdog_logger.info("Watchdog stopped by user.")
                self._running = False
                break
            except Exception as exc:
                watchdog_logger.error("Watchdog cycle error: %s", exc)
                watchdog_logger.error(traceback.format_exc())

            self.state.save()

            elapsed = time.monotonic() - loop_start
            sleep_for = max(Config.WATCHDOG_INTERVAL_SECONDS - elapsed, 1.0)
            time.sleep(sleep_for)

        self.state.save()
        watchdog_logger.info("Watchdog shutdown complete.")

    def check_main_bot_health(self) -> bool:
        """Return True when main bot appears alive and heartbeating."""
        stale_seconds = self.main_bot_stale_seconds()
        process_running = self.is_main_process_running()

        if stale_seconds is None and not process_running:
            self._alert_main_down(
                "Main bot process not found and no heartbeat file detected."
            )
            self._maybe_restart_main()
            return False

        if stale_seconds is not None and stale_seconds > Config.WATCHDOG_MAIN_STALE_SECONDS:
            self._alert_main_down(
                f"Main bot heartbeat stale ({stale_seconds:.0f}s > "
                f"{Config.WATCHDOG_MAIN_STALE_SECONDS}s)."
            )
            self._maybe_restart_main()
            return False

        if not process_running:
            if self.state.should_alert("main_process_missing", 600):
                watchdog_logger.warning(
                    "Main bot heartbeat OK but process not detected — continuing watchdog."
                )
        return True

    def main_bot_stale_seconds(self) -> Optional[float]:
        """Seconds since last heartbeat file update; None if missing."""
        path = Config.WATCHDOG_HEARTBEAT_FILE
        if not os.path.isfile(path):
            return self._stale_from_system_log()
        try:
            with open(path, encoding="utf-8") as handle:
                data = json.load(handle)
            ts = float(data.get("timestamp", 0))
            if ts <= 0:
                return None
            return max(time.time() - ts, 0.0)
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            return None

    def _stale_from_system_log(self) -> Optional[float]:
        """Fallback: parse last 'Heartbeat |' line from system.log."""
        log_path = Path(Config.LOGS_DIR) / "system.log"
        if not log_path.is_file():
            return None
        try:
            raw = log_path.read_bytes()
            tail = raw[-65536:].decode("utf-8", errors="ignore")
            for line in reversed(tail.splitlines()):
                if "Heartbeat |" not in line:
                    continue
                # Format: 2026-09-12 18:00:00 | INFO | System | Heartbeat | ...
                stamp = line.split("|", 1)[0].strip()
                dt = datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=timezone.utc
                )
                return max(time.time() - dt.timestamp(), 0.0)
        except (OSError, ValueError):
            return None
        return None

    def is_main_process_running(self) -> bool:
        script = Config.WATCHDOG_MAIN_SCRIPT
        try:
            if sys.platform == "win32":
                cmd = [
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    (
                        "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" "
                        f"| Where-Object {{ $_.CommandLine -like '*{script}*' -and "
                        "$_.CommandLine -notlike '*watchdog.py*' }} "
                        "| Select-Object -First 1 -ExpandProperty ProcessId"
                    ),
                ]
            else:
                cmd = ["pgrep", "-f", script]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            if result.returncode != 0:
                return False
            output = (result.stdout or "").strip()
            return bool(output)
        except (subprocess.SubprocessError, OSError) as exc:
            watchdog_logger.debug("Process check failed: %s", exc)
            return True

    def _alert_main_down(self, detail: str) -> None:
        if not self.state.should_alert("main_bot_down", 300):
            return
        watchdog_logger.critical("MAIN BOT UNHEALTHY: %s", detail)
        self.telegram.send_message(
            "🚨 <b>Watchdog Alert</b>\n"
            f"Main bot appears <b>down or stalled</b>.\n"
            f"<i>{escape_html(detail)}</i>\n"
            "Watchdog is monitoring positions independently."
        )

    def _maybe_restart_main(self) -> None:
        if not Config.WATCHDOG_AUTO_RESTART_MAIN:
            return
        if not self.state.should_alert("main_restart_attempt", 600):
            return
        script = Config.WATCHDOG_MAIN_SCRIPT
        if not os.path.isfile(script):
            watchdog_logger.error("Auto-restart skipped — %s not found.", script)
            return
        try:
            subprocess.Popen(
                [sys.executable, script],
                cwd=os.getcwd(),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            watchdog_logger.warning("Attempted auto-restart of %s.", script)
            self.telegram.send_message(
                f"♻️ <b>Watchdog</b> attempted to restart <code>{escape_html(script)}</code>."
            )
        except OSError as exc:
            watchdog_logger.error("Auto-restart failed: %s", exc)

    def check_open_positions(self, *, main_bot_healthy: bool) -> None:
        """Compare REST positions + mark prices against DB TP/SL levels."""
        if self.exchange.is_rest_blocked()[0]:
            watchdog_logger.warning("REST blocked — skipping position check this cycle.")
            return

        rest_positions = self.exchange.fetch_all_open_positions_rest(
            force=Config.WATCHDOG_FORCE_REST
        )
        if rest_positions is None:
            watchdog_logger.warning("Could not fetch REST positions.")
            return

        pos_map: dict[tuple[str, str], dict[str, Any]] = {}
        for pos in rest_positions:
            symbol = str(pos.get("symbol", "")).upper()
            side = str(pos.get("positionSide", "")).upper()
            qty = safe_float(pos.get("quantity"))
            if symbol and side and qty > 0:
                pos_map[(symbol, side)] = pos

        db_trades = self.db.get_open_trades()
        if not db_trades:
            return

        for trade in db_trades:
            symbol = str(trade.get("symbol", "")).upper()
            side = str(trade.get("side", "LONG")).upper()
            trade_id = str(trade.get("trade_id", ""))
            key = (symbol, side)
            prefix = f"{trade_id}:"

            exchange_pos = pos_map.get(key)
            if not exchange_pos:
                self.state.clear_breach(prefix)
                continue

            mark = safe_float(exchange_pos.get("mark_price"))
            if mark <= 0:
                mark = self.exchange.fetch_mark_price_rest(symbol) or 0.0
            if mark <= 0:
                watchdog_logger.warning("[%s] No mark price — skip this cycle.", symbol)
                continue

            signal = self._evaluate_exit_signal(trade, mark)
            if signal is None:
                self.state.clear_breach(prefix)
                continue

            breach_key = f"{trade_id}:{signal.level}"
            age = self.state.note_breach(breach_key)
            watchdog_logger.info(
                "[%s] Breach detected | %s | mark=%.6f | age=%.0fs | main_healthy=%s",
                symbol,
                signal.reason,
                mark,
                age,
                main_bot_healthy,
            )

            if Config.WATCHDOG_EMERGENCY_ONLY_WHEN_MAIN_DOWN and main_bot_healthy:
                watchdog_logger.debug(
                    "[%s] Breach observed but main bot healthy — deferring to soft monitor.",
                    symbol,
                )
                continue

            grace = Config.WATCHDOG_BREACH_GRACE_SECONDS
            if age < grace:
                continue

            if exit_claim_active(trade_id):
                watchdog_logger.debug(
                    "[%s] Emergency close skipped — main bot holds exit claim.",
                    symbol,
                )
                continue

            self._emergency_close(trade, exchange_pos, signal, mark)

    def _evaluate_exit_signal(
        self, trade: dict[str, Any], mark_price: float
    ) -> Optional[ExitSignal]:
        side = str(trade.get("side", "LONG")).upper()
        metadata = self.db.parse_trade_metadata(trade)
        sl = safe_float(trade.get("stop_loss"))
        tp1 = safe_float(trade.get("take_profit_1"))
        tp2 = safe_float(trade.get("take_profit_2"))
        tp3 = safe_float(trade.get("take_profit_3"))

        runner_mode = bool(metadata.get("runner_mode", Config.ENABLE_TP3_RUNNER))

        if side == "LONG":
            if sl > 0 and mark_price <= sl:
                qty = self._close_quantity(trade, metadata, full=True)
                return ExitSignal("SL", "STOP_LOSS", False, qty)
            if (
                tp3 > 0
                and not runner_mode
                and mark_price >= tp3
                and not metadata.get("tp3_executed")
            ):
                qty = self._close_quantity(trade, metadata, full=True)
                return ExitSignal("TP3", "TP3_FULL_CLOSE", True, qty)
            if tp2 > 0 and mark_price >= tp2 and not metadata.get("tp2_executed"):
                qty = self._close_quantity(trade, metadata, level="TP2")
                return ExitSignal("TP2", "TP2", True, qty)
            if tp1 > 0 and mark_price >= tp1 and not metadata.get("tp1_executed"):
                qty = self._close_quantity(trade, metadata, level="TP1")
                return ExitSignal("TP1", "TP1", True, qty)
        else:
            if sl > 0 and mark_price >= sl:
                qty = self._close_quantity(trade, metadata, full=True)
                return ExitSignal("SL", "STOP_LOSS", False, qty)
            if (
                tp3 > 0
                and not runner_mode
                and mark_price <= tp3
                and not metadata.get("tp3_executed")
            ):
                qty = self._close_quantity(trade, metadata, full=True)
                return ExitSignal("TP3", "TP3_FULL_CLOSE", True, qty)
            if tp2 > 0 and mark_price <= tp2 and not metadata.get("tp2_executed"):
                qty = self._close_quantity(trade, metadata, level="TP2")
                return ExitSignal("TP2", "TP2", True, qty)
            if tp1 > 0 and mark_price <= tp1 and not metadata.get("tp1_executed"):
                qty = self._close_quantity(trade, metadata, level="TP1")
                return ExitSignal("TP1", "TP1", True, qty)
        return None

    def _close_quantity(
        self,
        trade: dict[str, Any],
        metadata: dict[str, Any],
        *,
        level: Optional[str] = None,
        full: bool = False,
    ) -> float:
        live = safe_float(trade.get("quantity"))
        if full or not Config.ENABLE_PARTIAL_TP or not level:
            return live
        qty_key = f"{level.lower()}_quantity"
        partial = safe_float(metadata.get(qty_key))
        return partial if partial > 0 else live

    def _emergency_close(
        self,
        trade: dict[str, Any],
        exchange_pos: dict[str, Any],
        signal: ExitSignal,
        mark_price: float,
    ) -> None:
        symbol = str(trade.get("symbol", "")).upper()
        side = str(trade.get("side", "LONG")).upper()
        trade_id = str(trade.get("trade_id", ""))
        live_qty = safe_float(exchange_pos.get("quantity"))
        close_qty = min(signal.quantity, live_qty) if live_qty > 0 else signal.quantity

        if close_qty <= 0:
            watchdog_logger.warning("[%s] Emergency close skipped — qty=0.", symbol)
            return

        if not claim_exit(trade_id, "watchdog"):
            watchdog_logger.debug(
                "[%s] Emergency close aborted — exit claim not acquired.", symbol
            )
            return

        watchdog_logger.critical(
            "[WATCHDOG_CLOSE] %s %s | %s | mark=%.6f | qty=%s",
            symbol,
            side,
            signal.reason,
            mark_price,
            close_qty,
        )

        try:
            order_response: Optional[dict[str, Any]] = None
            try:
                order_response = self.exchange.close_position_quantity(
                    symbol, side, close_qty
                )
            except PositionAlreadyClosedError:
                self._mark_trade_closed_db(trade, signal.reason, mark_price)
                self.state.clear_breach(f"{trade_id}:")
                return
            except OrderExecutionError as exc:
                watchdog_logger.error("[%s] Watchdog close failed: %s", symbol, exc)
                if self.state.should_alert(f"close_fail:{trade_id}", 300):
                    self.telegram.send_message(
                        f"🚨 <b>Watchdog close FAILED</b>\n"
                        f"{escape_html(symbol)} {escape_html(side)} | {escape_html(signal.reason)}\n"
                        f"<i>{escape_html(str(exc))}</i>"
                    )
                return

            self._apply_post_close_db(
                trade, signal, mark_price, close_qty, order_response=order_response
            )
            self.state.clear_breach(f"{trade_id}:")
        finally:
            release_exit(trade_id, "watchdog")

        stale = self.main_bot_stale_seconds()
        stale_note = (
            f"Main heartbeat stale {stale:.0f}s."
            if stale is not None
            else "Main process/heartbeat unavailable."
        )
        self.telegram.send_message(
            "🛡 <b>Watchdog Emergency Close</b>\n"
            f"Pair: <b>{escape_html(symbol)}</b> {escape_html(side)}\n"
            f"Level: <b>{escape_html(signal.level)}</b> ({escape_html(signal.reason)})\n"
            f"Mark: {mark_price:.6f} | Qty: {close_qty}\n"
            f"<i>{escape_html(stale_note)} Watchdog executed reduce-only market close.</i>"
        )

    def _apply_post_close_db(
        self,
        trade: dict[str, Any],
        signal: ExitSignal,
        mark_price: float,
        close_qty: float,
        *,
        order_response: Optional[dict[str, Any]] = None,
    ) -> None:
        trade_id = str(trade.get("trade_id", ""))
        metadata = self.db.parse_trade_metadata(trade)
        symbol = str(trade.get("symbol", "")).upper()
        side = str(trade.get("side", "LONG")).upper()
        prior_pnl = safe_float(trade.get("realized_pnl")) or safe_float(trade.get("pnl"))

        leg_pnl = 0.0
        if order_response:
            fill = self.exchange.resolve_order_fill_pnl(
                symbol,
                str(order_response.get("orderId", "")),
                position_side=side,
            )
            if fill.fill_count > 0 and fill.source != "unknown":
                leg_pnl = fill.realized_pnl
                if fill.exit_price > 0:
                    mark_price = fill.exit_price

        if signal.partial and signal.level in ("TP1", "TP2"):
            cumulative = prior_pnl + leg_pnl
        else:
            from reconciliation import trade_opened_at_ms

            lifecycle = self.exchange.fetch_closed_position_realized_pnl(
                symbol, side, trade_opened_at_ms(trade)
            )
            if lifecycle.source in ("userTrades", "income", "ws") and (
                lifecycle.fill_count > 0 or abs(lifecycle.realized_pnl) > 0
            ):
                cumulative = lifecycle.realized_pnl
            else:
                cumulative = prior_pnl + leg_pnl

        realized = cumulative - prior_pnl
        updates: dict[str, Any] = {
            "pnl": cumulative,
            "realized_pnl": cumulative,
        }

        if signal.partial and signal.level in ("TP1", "TP2"):
            metadata[f"{signal.level.lower()}_executed"] = True
            updates["metadata"] = metadata
            entry = safe_float(trade.get("entry_price"))
            if signal.level == "TP1":
                updates["status"] = TRADE_STATUS_TP1_HIT
                if Config.ENABLE_BREAK_EVEN and entry > 0:
                    updates["stop_loss"] = entry
            elif signal.level == "TP2":
                updates["status"] = TRADE_STATUS_TP2_HIT
                tp1 = safe_float(trade.get("take_profit_1"))
                if tp1 > 0:
                    updates["stop_loss"] = tp1
            self.db.update_trade(trade_id, updates)
            self.db.add_daily_realized_pnl(utc_today_str(), realized)
            self.db.sync_daily_stats_from_trades(utc_today_str())
        else:
            self._mark_trade_closed_db(
                trade,
                signal.reason,
                mark_price,
                pnl=cumulative,
            )

    def _mark_trade_closed_db(
        self,
        trade: dict[str, Any],
        reason: str,
        exit_price: float,
        *,
        pnl: Optional[float] = None,
    ) -> None:
        self.db.close_trade_and_sync_stats(
            trade,
            exit_price=exit_price,
            exit_reason=f"WATCHDOG_{reason}",
            pnl=pnl,
            book_daily_pnl=True,
            daily_pnl_delta=None,
        )
        symbol = str(trade.get("symbol", "")).upper()
        if symbol:
            self.db.set_symbol_cooldown(
                symbol,
                Config.POST_TRADE_COOLDOWN_MINUTES,
                reason=f"WATCHDOG_{reason}",
            )
        watchdog_logger.info(
            "[POSITION_CLOSED] Pair: %s %s | Reason: Watchdog %s | exit=%.6f",
            trade.get("symbol"),
            trade.get("side"),
            reason,
            exit_price,
        )


def main() -> None:
    watchdog = PositionWatchdog()
    watchdog.run()


if __name__ == "__main__":
    main()
