"""
Telegram notification and command module.
HTML-safe messaging, background polling, and authorized chat commands.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from typing import TYPE_CHECKING, Any, Callable, Optional

import telebot

from config import Config
from constants import TP1_PORTION, TP2_PORTION, TP3_PORTION, strategy_display_label
from database import DatabaseManager
from logger import error_logger, read_recent_error_log_lines, system_logger
from reporter import format_bot_health_message
from telegram_alerts import (
    format_active_positions_message,
    format_daily_status_message,
    format_live_account_header,
    format_watchlist_message,
)
from utils import escape_html, safe_float, utc_today_str

if TYPE_CHECKING:
    from bot_controller import BotController
    from exchange import BinanceExchangeManager
    from manager import TradeManager
    from risk_manager import RiskManager
    from scheduler import DailyScheduler

TELEGRAM_PLACEHOLDERS = {
    "",
    "your_telegram_bot_token",
    "your_chat_id",
}


def _leg_pnl_usd(side: str, entry: float, target: float, quantity: float) -> float:
    """Estimated USD PnL for a partial leg at target price."""
    if entry <= 0 or target <= 0 or quantity <= 0:
        return 0.0
    if side.upper() == "LONG":
        return (target - entry) * quantity
    return (entry - target) * quantity


def _format_sl_pnl(amount: float) -> str:
    if amount >= 0:
        return f"+${amount:.2f}"
    return f"-${abs(amount):.2f} Loss"


def _format_tp_pnl(amount: float, portion_pct: float) -> str:
    sign = "+" if amount >= 0 else "-"
    return f"{sign}${abs(amount):.2f} Profit on {portion_pct:.0f}%"


class TelegramManager:
    """Sends trade alerts and handles authorized user commands in a daemon thread."""

    def __init__(
        self,
        db: DatabaseManager,
        scheduler: Optional["DailyScheduler"] = None,
        risk_manager: Optional["RiskManager"] = None,
        exchange: Optional["BinanceExchangeManager"] = None,
        manager: Optional["TradeManager"] = None,
        controller: Optional["BotController"] = None,
    ) -> None:
        self.db = db
        self.scheduler = scheduler
        self.risk_manager = risk_manager
        self.exchange = exchange
        self.manager = manager
        self.controller = controller
        self.scanner: Any = None
        self.market_data: Any = None

        self.token = Config.TELEGRAM_BOT_TOKEN.strip()
        self.chat_id = str(Config.TELEGRAM_CHAT_ID).strip()
        self.enabled = self._credentials_valid()

        self._stop_event = threading.Event()
        self._listener_thread: Optional[threading.Thread] = None

        if self.enabled:
            self.bot = telebot.TeleBot(self.token, parse_mode="HTML")
            self._register_handlers()
        else:
            self.bot = None
            system_logger.warning(
                "Telegram disabled: missing or placeholder credentials in .env."
            )

    # ---------------- Lifecycle ----------------

    def start_listening(self) -> None:
        if not self.enabled or self.bot is None:
            return

        if self._listener_thread and self._listener_thread.is_alive():
            return

        self._stop_event.clear()
        self._listener_thread = threading.Thread(
            target=self._polling_loop,
            name="TelegramPolling",
            daemon=True,
        )
        self._listener_thread.start()

    def stop_listening(self) -> None:
        self._stop_event.set()
        if self.bot is not None:
            try:
                self.bot.stop_polling()
            except Exception:
                pass

    def _polling_loop(self) -> None:
        assert self.bot is not None
        while not self._stop_event.is_set():
            try:
                system_logger.info("Telegram listener started.")
                self.bot.infinity_polling(
                    timeout=10,
                    long_polling_timeout=5,
                    skip_pending=True,
                )
            except Exception as exc:
                error_logger.error("Telegram polling error: %s", exc)
                if self._stop_event.is_set():
                    break
                time.sleep(5)

    # ---------------- Messaging ----------------

    def send_message(self, text: str) -> None:
        if not self.enabled or self.bot is None:
            return
        try:
            self.bot.send_message(chat_id=self.chat_id, text=text, parse_mode="HTML")
        except Exception as exc:
            error_logger.error("Failed to send Telegram message: %s", exc)

    def send_trade_alert(
        self,
        action: str,
        symbol: str,
        price: float,
        tp1: float,
        sl: float,
        tp2: Optional[float] = None,
        tp3: Optional[float] = None,
        score: float = 0.0,
        strategy: str = "DEFAULT",
        quantity: float = 0.0,
    ) -> None:
        emoji = "🟢" if action == "LONG" else "🔴"
        tp1_qty = quantity * TP1_PORTION if quantity > 0 else 0.0
        tp2_qty = quantity * TP2_PORTION if quantity > 0 else 0.0
        tp3_qty = quantity * TP3_PORTION if quantity > 0 else 0.0

        sl_pnl = _leg_pnl_usd(action, price, sl, quantity) if quantity > 0 else 0.0
        tp1_pnl = _leg_pnl_usd(action, price, tp1, tp1_qty) if tp1_qty > 0 else 0.0
        tp2_pnl = (
            _leg_pnl_usd(action, price, tp2, tp2_qty)
            if tp2 is not None and tp2 > 0 and tp2_qty > 0
            else 0.0
        )
        tp3_est = (
            _leg_pnl_usd(action, price, tp3, tp3_qty)
            if tp3 is not None and tp3 > 0 and tp3_qty > 0
            else 0.0
        )

        msg = (
            f"{emoji} <b>NEW TRADE EXECUTED</b>\n\n"
            f"🪙 <b>Pair:</b> {escape_html(symbol)}\n"
            f"🎯 <b>Action:</b> {escape_html(action)}\n"
            f"🧠 <b>Strategy:</b> {escape_html(strategy_display_label(strategy))}\n"
            f"🏷 <b>Tag:</b> {escape_html(strategy)}\n"
            f"📊 <b>Score:</b> {score:.1f}\n"
            f"💵 <b>Entry:</b> {price:.6f}\n"
        )
        if quantity > 0:
            msg += f"📦 <b>Size:</b> {quantity:.4f}\n"

        msg += f"🛑 <b>SL:</b> {sl:.6f} ({_format_sl_pnl(sl_pnl)})\n"
        msg += (
            f"🎯 <b>TP1:</b> {tp1:.6f} "
            f"({_format_tp_pnl(tp1_pnl, TP1_PORTION * 100)})\n"
        )
        if tp2 is not None and tp2 > 0:
            msg += (
                f"🎯 <b>TP2:</b> {tp2:.6f} "
                f"({_format_tp_pnl(tp2_pnl, TP2_PORTION * 100)})\n"
            )
        if Config.ENABLE_TP3_RUNNER:
            est = max(tp3_est, 0.0)
            msg += (
                f"🚀 <b>TP3:</b> Dynamic Trailing "
                f"(Runner {TP3_PORTION * 100:.0f}% | Est. +${est:.2f}+)\n"
            )
            if tp3 is not None and tp3 > 0:
                msg += f"   <i>Expansion target ~{tp3:.6f}</i>\n"
        elif tp3 is not None and tp3 > 0:
            msg += (
                f"🎯 <b>TP3:</b> {tp3:.6f} "
                f"({_format_tp_pnl(tp3_est, TP3_PORTION * 100)})\n"
            )

        self.send_message(msg)

    def send_close_alert(
        self,
        symbol: str,
        reason: str,
        pnl: Optional[float] = None,
        strategy: str = "",
        *,
        trade_id: str = "",
    ) -> None:
        from core.close_notification_guard import try_claim_close_notification

        if trade_id and not try_claim_close_notification(trade_id):
            system_logger.debug(
                "Skipping duplicate close alert for trade %s.", trade_id[:8]
            )
            return

        reason_upper = reason.upper()
        if pnl is not None and pnl > 0:
            if "STOP" in reason_upper:
                emoji = "🎯"
                title = "TRAILING / PROFIT SL HIT"
            elif "TP" in reason_upper:
                emoji = "✅"
                title = "TAKE PROFIT HIT"
            else:
                emoji = "✅"
                title = "POSITION CLOSED (PROFIT)"
        elif pnl is not None and pnl < 0:
            emoji = "🛑"
            title = "STOP LOSS HIT" if "STOP" in reason_upper else "POSITION CLOSED (LOSS)"
        elif "TP" in reason_upper and "STOP" not in reason_upper:
            emoji = "✅"
            title = "TAKE PROFIT HIT"
        elif "STOP" in reason_upper:
            emoji = "🛑"
            title = "STOP LOSS HIT"
        else:
            emoji = "ℹ️"
            title = "POSITION CLOSED"

        msg = (
            f"{emoji} <b>{escape_html(title)}</b>\n\n"
            f"🪙 <b>Pair:</b> {escape_html(symbol)}\n"
            f"📌 <b>Trigger:</b> {escape_html(reason)}"
        )
        if strategy:
            msg += f"\n🧠 <b>Strategy:</b> {escape_html(strategy_display_label(strategy))}"
        if pnl is not None:
            if pnl > 0:
                msg += f"\n💰 <b>Realized Profit:</b> +${pnl:.4f}"
            elif pnl < 0:
                msg += f"\n💰 <b>Realized PnL:</b> -${abs(pnl):.4f}"
            else:
                msg += f"\n💰 <b>Realized PnL:</b> $0.0000"
        self.send_message(msg)

    def send_tp_level_alert(
        self,
        symbol: str,
        level: str,
        reason: str,
        new_sl: Optional[float] = None,
        pnl: Optional[float] = None,
    ) -> None:
        msg = (
            f"✅ <b>{escape_html(level)} HIT</b>\n\n"
            f"🪙 <b>Pair:</b> {escape_html(symbol)}\n"
            f"📌 <b>Action:</b> {escape_html(reason)}"
        )
        if new_sl is not None:
            msg += f"\n🛑 <b>New SL:</b> {new_sl:.6f}"
        if pnl is not None:
            msg += f"\n💰 <b>Partial PnL:</b> ${pnl:.4f}"
        self.send_message(msg)

    # ---------------- Command helpers ----------------

    def _format_market_snapshot(self) -> str:
        if not self.exchange:
            return "Exchange not attached."

        btc_symbol = f"BTC{Config.QUOTE_ASSET}"
        try:
            from scanner import MarketAnalyzer
            from engines.smc_engine import compute_premium_discount, resolve_macro_trend, resolve_confirm_trend

            df_trend = self.exchange.fetch_historical_candles(
                btc_symbol, Config.TREND_TIMEFRAME, limit=Config.CANDLE_FETCH_LIMIT, allow_rest=False
            )
            df_confirm = self.exchange.fetch_historical_candles(
                btc_symbol, Config.CONFIRM_TIMEFRAME, limit=Config.CANDLE_FETCH_LIMIT, allow_rest=False
            )
            if df_trend.empty:
                return "⚠️ Could not fetch BTC market data."

            analyzer = MarketAnalyzer()
            df_trend = analyzer.apply_all_indicators(df_trend)
            df_confirm = analyzer.apply_all_indicators(df_confirm) if not df_confirm.empty else df_confirm

            latest = df_trend.iloc[-1]
            price = safe_float(latest.get("close"))
            atr = safe_float(latest.get("atr"))
            atr_pct = (atr / price * 100.0) if price > 0 else 0.0

            macro = resolve_macro_trend(df_trend, df_confirm)
            confirm = resolve_confirm_trend(df_confirm) if not df_confirm.empty else "NEUTRAL"
            _, pd_zone = compute_premium_discount(df_trend, price)

            ticker_map = self.exchange.get_futures_ticker_map()
            ticker = ticker_map.get(btc_symbol, {})
            change_pct = safe_float(ticker.get("priceChangePercent"))
            volume_24h = safe_float(ticker.get("quoteVolume"))

            entries_status = "OPEN"
            block_reason = ""
            if self.scheduler:
                paused, block_reason = self.scheduler.is_entry_paused()
                entries_status = "PAUSED" if paused else "OPEN"
            elif self.risk_manager:
                snap = self.risk_manager.get_risk_snapshot()
                entries_status = "OPEN" if snap.entries_allowed else "PAUSED"
                block_reason = snap.block_reason

            mode = "TESTNET" if Config.USE_TESTNET else "MAINNET"
            return (
                f"🌍 <b>Market Snapshot ({escape_html(btc_symbol)})</b>\n\n"
                f"🌐 <b>Mode:</b> {escape_html(mode)}\n"
                f"💵 <b>Price:</b> ${price:,.2f}\n"
                f"📈 <b>24h Change:</b> {change_pct:+.2f}%\n"
                f"💧 <b>24h Volume:</b> ${volume_24h / 1_000_000:.1f}M\n"
                f"📊 <b>1h Macro Trend:</b> {escape_html(macro)}\n"
                f"⏱ <b>15m Structure:</b> {escape_html(confirm)}\n"
                f"⚖️ <b>PD Zone:</b> {escape_html(pd_zone)}\n"
                f"🌊 <b>ATR (1h):</b> ${atr:,.2f} ({atr_pct:.2f}%)\n"
                f"🚦 <b>Bot Entries:</b> {escape_html(entries_status)}"
                + (f"\n⛔ <i>{escape_html(block_reason)}</i>" if block_reason else "")
            )
        except Exception as exc:
            error_logger.error("Market snapshot failed: %s", exc)
            return f"⚠️ Market snapshot error: {escape_html(str(exc))}"

    def _format_recent_errors(self, limit: int = 12) -> str:
        hours = max(int(Config.TELEGRAM_ERROR_LOG_MAX_AGE_HOURS), 1)
        lines = [f"🚨 <b>Critical Errors (last {hours}h)</b>\n"]
        db_errors = self.db.get_recent_critical_errors(
            limit=limit,
            max_age_hours=Config.TELEGRAM_ERROR_LOG_MAX_AGE_HOURS,
        )

        if db_errors:
            for item in db_errors:
                ts = escape_html(item.get("timestamp", "")[:19])
                cat = escape_html(item.get("category", "?"))
                msg = escape_html(str(item.get("message", ""))[:180])
                lines.append(f"• [{ts}] <b>{cat}</b>\n  {msg}")
        else:
            lines.append(
                f"<i>No critical errors recorded in the last {hours} hours.</i>"
            )

        try:
            tail = read_recent_error_log_lines(
                max_age_hours=Config.TELEGRAM_ERROR_LOG_MAX_AGE_HOURS,
                limit=8,
            )
            if tail:
                lines.append(f"\n📄 <b>errors.log (last {hours}h)</b>")
                for raw in tail:
                    lines.append(escape_html(raw.rstrip())[:200])
        except OSError as exc:
            lines.append(f"\n⚠️ Could not read errors.log: {escape_html(str(exc))}")

        return "\n".join(lines)

    def _graceful_restart(self) -> None:
        """Re-exec the bot process after a short delay (works with systemd/docker too)."""
        if self.controller:
            self.controller.request_restart()
            return

        script = os.path.abspath(sys.argv[0])
        cwd = os.getcwd()
        time.sleep(2)
        subprocess.Popen([sys.executable, script, *sys.argv[1:]], cwd=cwd)
        os._exit(0)

    # ---------------- Command handlers ----------------

    def _register_handlers(self) -> None:
        assert self.bot is not None

        def authorized(handler: Callable[..., None]) -> Callable[..., None]:
            def wrapper(message: telebot.types.Message) -> None:
                if not self._authorized(message):
                    return
                handler(message)

            return wrapper

        @self.bot.message_handler(commands=["ping"])
        @authorized
        def ping_handler(message: telebot.types.Message) -> None:
            mode = "TESTNET" if Config.USE_TESTNET else "MAINNET"
            dry = " | DRY_RUN" if Config.DRY_RUN else ""
            self.bot.reply_to(
                message,
                f"🟢 <b>Bot online</b> — actively monitoring markets "
                f"({escape_html(mode)}{dry}).",
            )

        @self.bot.message_handler(commands=["health", "pulse"])
        @authorized
        def health_handler(message: telebot.types.Message) -> None:
            text = format_bot_health_message(
                exchange=self.exchange,
                market_data=self.market_data,
                scanner=self.scanner,
                controller=self.controller,
                scheduler=self.scheduler,
            )
            self.bot.reply_to(message, text)

        @self.bot.message_handler(commands=["status"])
        @authorized
        def status_handler(message: telebot.types.Message) -> None:
            today = utc_today_str()
            stats = (
                self.scheduler.get_today_stats()
                if self.scheduler
                else self.db.get_daily_stats(today)
            )
            if not stats:
                self.bot.reply_to(message, "⚠️ No daily stats recorded yet today.")
                return

            if not self.exchange:
                self.bot.reply_to(message, "⚠️ Exchange not attached.")
                return

            engine_status = "RUNNING"
            if self.scheduler:
                paused, _ = self.scheduler.is_entry_paused()
                engine_status = "PAUSED" if paused else "RUNNING"

            msg = format_daily_status_message(
                self.exchange,
                self.db,
                stats,
                today=today,
                engine_status=engine_status,
            )
            self.bot.reply_to(message, msg)

        @self.bot.message_handler(commands=["risk"])
        @authorized
        def risk_handler(message: telebot.types.Message) -> None:
            if not self.risk_manager:
                self.bot.reply_to(message, "Risk manager not attached.")
                return

            snap = self.risk_manager.get_risk_snapshot()
            today = utc_today_str()
            analytics = self.db.get_daily_trade_analytics(today)
            pf = analytics.get("profit_factor", 0.0)
            pf_display = "∞" if pf == float("inf") else f"{pf:.2f}"

            msg = (
                "🛡 <b>Risk Snapshot</b>\n\n"
                f"📂 <b>Exchange open:</b> {snap.exchange_open_positions}/{Config.MAX_POSITIONS}\n"
                f"🆕 <b>Daily entries:</b> {snap.daily_entries}/{Config.MAX_DAILY_TRADES}\n"
                f"✅ <b>Daily closes:</b> {snap.daily_trades}\n"
                f"🏆 <b>Win Rate:</b> {analytics.get('win_rate', 0.0):.1f}% "
                f"({analytics.get('wins', 0)}W / {analytics.get('losses', 0)}L)\n"
                f"📐 <b>Profit Factor:</b> {pf_display}\n"
                f"📉 <b>Consecutive losses:</b> {snap.consecutive_losses}/"
                f"{Config.MAX_CONSECUTIVE_LOSSES}\n"
                f"📈 <b>Realized PnL:</b> ${snap.daily_realized_pnl:.2f} "
                f"({snap.daily_realized_pnl_percent:.2f}%)\n"
                f"📊 <b>Unrealized:</b> ${snap.unrealized_pnl:.2f}\n"
                f"📉 <b>Realized drawdown:</b> {snap.drawdown_percent:.2f}% / "
                f"{Config.MAX_ACCOUNT_DRAWDOWN:.2f}%\n"
                f"💵 <b>Balance:</b> ${snap.current_balance:.2f}\n"
                f"✅ <b>Entries allowed:</b> {snap.entries_allowed}"
            )
            if snap.block_reason:
                msg += f"\n⛔ <b>Block:</b> {escape_html(snap.block_reason)}"
            self.bot.reply_to(message, msg)

        @self.bot.message_handler(commands=["positions"])
        @authorized
        def positions_handler(message: telebot.types.Message) -> None:
            trades = self.db.get_open_trades()
            if not trades:
                self.bot.reply_to(message, "📭 No open positions.")
                return

            lines = ["📂 <b>Open Positions</b>\n"]
            for trade in trades[:20]:
                lines.append(
                    f"• {escape_html(trade.get('symbol', '?'))} "
                    f"{escape_html(trade.get('side', '?'))} | "
                    f"{escape_html(strategy_display_label(str(trade.get('strategy', ''))))} | "
                    f"status={escape_html(str(trade.get('status')))} | "
                    f"entry={safe_float(trade.get('entry_price')):.4f}"
                )
            self.bot.reply_to(message, "\n".join(lines))

        @self.bot.message_handler(commands=["pause"])
        @authorized
        def pause_handler(message: telebot.types.Message) -> None:
            if not self.scheduler:
                self.bot.reply_to(message, "Scheduler not attached.")
                return
            self.scheduler.pause_entries_manual("Manual pause via /pause")
            self.bot.reply_to(
                message,
                "⏸ <b>Entries paused.</b>\n"
                "<i>Open positions continue to be managed.</i>",
            )

        @self.bot.message_handler(commands=["resume"])
        @authorized
        def resume_handler(message: telebot.types.Message) -> None:
            if not self.scheduler:
                self.bot.reply_to(message, "Scheduler not attached.")
                return
            self.scheduler.resume_entries_manual()
            if self.risk_manager:
                self.risk_manager.reset_consecutive_loss_block()

            paused, reason = self.scheduler.is_entry_paused()
            if self.risk_manager:
                snap = self.risk_manager.get_risk_snapshot()
                consec_line = (
                    f"\n📉 <b>Consecutive losses:</b> "
                    f"{snap.consecutive_losses}/{Config.MAX_CONSECUTIVE_LOSSES}"
                )
                allowed_line = (
                    f"\n✅ <b>Entries allowed:</b> {snap.entries_allowed}"
                )
            else:
                consec_line = ""
                allowed_line = ""

            if paused:
                self.bot.reply_to(
                    message,
                    f"⚠️ Manual pause cleared, but entries still blocked:\n"
                    f"{escape_html(reason)}"
                    f"{consec_line}{allowed_line}",
                )
            else:
                self.bot.reply_to(
                    message,
                    "▶️ <b>Entries resumed.</b> Scanning will continue."
                    f"{consec_line}{allowed_line}",
                )

        @self.bot.message_handler(commands=["forceresume", "force_start"])
        @authorized
        def forceresume_handler(message: telebot.types.Message) -> None:
            if not self.scheduler:
                self.bot.reply_to(message, "Scheduler not attached.")
                return

            note = self.scheduler.force_resume_entries()
            if note.startswith("BLOCKED"):
                self.bot.reply_to(message, f"🚫 {escape_html(note)}")
                return

            if self.risk_manager:
                self.risk_manager.reset_consecutive_loss_block()
                snap = self.risk_manager.get_risk_snapshot()
                status_line = (
                    f"\n⚙️ <b>Status:</b> RUNNING"
                    f"\n✅ <b>Entries allowed:</b> {snap.entries_allowed}"
                )
            else:
                status_line = "\n⚙️ <b>Status:</b> RUNNING"

            self.bot.reply_to(
                message,
                "🚀 <b>Force resume activated</b>\n"
                f"{escape_html(note)}"
                f"{status_line}\n"
                "<i>Daily max-loss lock cleared. Override resets at next UTC day.</i>",
            )

        @self.bot.message_handler(commands=["closeall"])
        @authorized
        def closeall_handler(message: telebot.types.Message) -> None:
            if not self.manager:
                self.bot.reply_to(message, "Trade manager not attached.")
                return
            self.bot.reply_to(message, "⏳ Closing all open positions…")
            result = self.manager.close_all_positions(reason="MANUAL_CLOSE_ALL")
            closed = result.get("closed", [])
            failed = result.get("failed", [])
            msg = (
                f"✅ <b>Close-all complete</b>\n\n"
                f"Closed: {len(closed)}\n"
                f"Failed: {len(failed)}"
            )
            if closed:
                msg += "\n\n" + "\n".join(f"• {escape_html(c)}" for c in closed[:15])
            if failed:
                msg += "\n\n<b>Failures:</b>\n" + "\n".join(
                    f"• {escape_html(str(f))}" for f in failed[:10]
                )
            self.bot.reply_to(message, msg)

        @self.bot.message_handler(commands=["stop"])
        @authorized
        def stop_handler(message: telebot.types.Message) -> None:
            self.bot.reply_to(
                message,
                "🛑 <b>Shutdown requested.</b>\n"
                "Stopping the trading loop safely…",
            )
            if self.controller:
                self.controller.request_shutdown()
            else:
                os._exit(0)

        @self.bot.message_handler(commands=["restart"])
        @authorized
        def restart_handler(message: telebot.types.Message) -> None:
            self.bot.reply_to(
                message,
                "🔄 <b>Restart requested.</b>\n"
                "Bot will restart gracefully…",
            )
            if self.controller:
                self.controller.request_restart()
            else:
                threading.Thread(
                    target=self._graceful_restart,
                    name="bot-restart",
                    daemon=True,
                ).start()

        @self.bot.message_handler(commands=["market"])
        @authorized
        def market_handler(message: telebot.types.Message) -> None:
            self.bot.reply_to(message, self._format_market_snapshot())

        @self.bot.message_handler(commands=["errors"])
        @authorized
        def errors_handler(message: telebot.types.Message) -> None:
            text = self._format_recent_errors()
            if len(text) > 4000:
                text = text[:3990] + "\n…"
            self.bot.reply_to(message, text)

        @self.bot.message_handler(commands=["balance"])
        @authorized
        def balance_handler(message: telebot.types.Message) -> None:
            if not self.exchange:
                self.bot.reply_to(message, "Exchange not attached.")
                return
            header = format_live_account_header(
                self.exchange,
                db=self.db,
                date_str=utc_today_str(),
            ).rstrip()
            self.bot.reply_to(message, header)

        @self.bot.message_handler(commands=["active"])
        @authorized
        def active_handler(message: telebot.types.Message) -> None:
            if not self.exchange:
                self.bot.reply_to(message, "⚠️ Exchange not attached.")
                return
            text = format_active_positions_message(
                self.db, self.exchange, telegram=self
            )
            if len(text) > 4000:
                text = text[:3990] + "\n…"
            self.bot.reply_to(message, text)

        @self.bot.message_handler(commands=["watchlist"])
        @authorized
        def watchlist_handler(message: telebot.types.Message) -> None:
            scanner = getattr(self, "scanner", None)
            if scanner is None or not hasattr(scanner, "get_watchlist_tiers"):
                self.bot.reply_to(message, "⚠️ Event scan not available.")
                return
            tiers = scanner.get_watchlist_tiers()
            orchestrator = getattr(scanner, "orchestrator", None)
            if orchestrator is not None and not tiers.get("tier2"):
                try:
                    orchestrator.process_hot_scan_cycle()
                    tiers = scanner.get_watchlist_tiers()
                except Exception as exc:
                    error_logger.warning("/watchlist hot scan refresh failed: %s", exc)
            text = format_watchlist_message(
                tier1_hot=tiers.get("tier1_hot", []),
                tier1_background=tiers.get("tier1_background", []),
                tier1_full=tiers.get("tier1_full", []),
                tier2_rows=tiers.get("tier2", []),
                hot_scan_interval=Config.HOT_SCAN_INTERVAL_SECONDS,
                tier2_display_limit=Config.TIER2_HOT_SIZE,
                tier2_near_miss=tiers.get("tier2_near_miss", []),
            )
            if len(text) > 4000:
                text = text[:3990] + "\n…"
            self.bot.reply_to(message, text)

        @self.bot.message_handler(commands=["help"])
        @authorized
        def help_handler(message: telebot.types.Message) -> None:
            self.bot.reply_to(
                message,
                "<b>Available commands</b>\n"
                "/status — daily performance\n"
                "/risk — portfolio risk snapshot\n"
                "/positions — open trades\n"
                "/market — BTC macro trend & volatility\n"
                "/pause — pause new entries\n"
                "/resume — resume new entries\n"
                "/forceresume — clear daily stop (testnet only)\n"
                "/closeall — close all open positions\n"
                "/stop — safe bot shutdown\n"
                "/restart — graceful bot restart\n"
                f"/errors — critical errors from the last "
                f"{int(Config.TELEGRAM_ERROR_LOG_MAX_AGE_HOURS)} hours\n"
                "/balance — live futures balance\n"
                "/active — open positions (DB + Binance REST)\n"
                "/watchlist — Tier 1 hot scan + Tier 2 candidates\n"
                "/health — system diagnostics (alias /pulse)\n"
                "/ping — quick online check\n"
                "/help — this message",
            )

    def _authorized(self, message: telebot.types.Message) -> bool:
        return str(message.chat.id) == self.chat_id

    def _credentials_valid(self) -> bool:
        if self.token in TELEGRAM_PLACEHOLDERS or self.chat_id in TELEGRAM_PLACEHOLDERS:
            return False
        return bool(self.token and self.chat_id)
