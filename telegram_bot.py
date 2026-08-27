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
from database import DatabaseManager
from logger import error_logger, system_logger
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
    ) -> None:
        emoji = "🟢" if action == "LONG" else "🔴"
        msg = (
            f"{emoji} <b>NEW TRADE EXECUTED</b>\n\n"
            f"🪙 <b>Pair:</b> {escape_html(symbol)}\n"
            f"🎯 <b>Action:</b> {escape_html(action)}\n"
            f"📊 <b>Score:</b> {score:.1f}\n"
            f"🧠 <b>Strategy:</b> {escape_html(strategy)}\n"
            f"💵 <b>Entry:</b> {price:.6f}\n"
            f"✅ <b>TP1:</b> {tp1:.6f}\n"
        )
        if tp2 is not None:
            msg += f"✅ <b>TP2:</b> {tp2:.6f}\n"
        if tp3 is not None:
            msg += f"✅ <b>TP3:</b> {tp3:.6f}\n"
        msg += f"🛑 <b>SL:</b> {sl:.6f}"
        self.send_message(msg)

    def send_close_alert(
        self,
        symbol: str,
        reason: str,
        pnl: Optional[float] = None,
    ) -> None:
        if "TP" in reason.upper():
            emoji = "✅"
            title = "TAKE PROFIT HIT"
        elif "STOP" in reason.upper():
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
        if pnl is not None:
            msg += f"\n💰 <b>Realized PnL:</b> ${pnl:.4f}"
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
            from smc_engine import compute_premium_discount, resolve_macro_trend, resolve_confirm_trend

            df_trend = self.exchange.fetch_historical_candles(
                btc_symbol, Config.TREND_TIMEFRAME, limit=Config.CANDLE_FETCH_LIMIT
            )
            df_confirm = self.exchange.fetch_historical_candles(
                btc_symbol, Config.CONFIRM_TIMEFRAME, limit=Config.CANDLE_FETCH_LIMIT
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
        lines = ["🚨 <b>Recent Critical Errors</b>\n"]
        db_errors = self.db.get_recent_critical_errors(limit=limit)

        if db_errors:
            for item in db_errors:
                ts = escape_html(item.get("timestamp", "")[:19])
                cat = escape_html(item.get("category", "?"))
                msg = escape_html(str(item.get("message", ""))[:180])
                lines.append(f"• [{ts}] <b>{cat}</b>\n  {msg}")
        else:
            lines.append("<i>No critical errors recorded in database.</i>")

        log_path = os.path.join(Config.LOGS_DIR, "errors.log")
        if os.path.isfile(log_path):
            try:
                with open(log_path, "r", encoding="utf-8", errors="replace") as handle:
                    tail = handle.readlines()[-8:]
                if tail:
                    lines.append("\n📄 <b>errors.log (tail)</b>")
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
            self.bot.reply_to(
                message,
                f"🟢 <b>Bot online</b> — actively monitoring markets ({escape_html(mode)}).",
            )

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

            analytics = self.db.get_daily_trade_analytics(today)
            pf = analytics.get("profit_factor", 0.0)
            pf_display = "∞" if pf == float("inf") else f"{pf:.2f}"

            msg = (
                f"📊 <b>Daily Status ({escape_html(today)})</b>\n\n"
                f"💰 <b>Reference:</b> ${safe_float(stats.get('start_balance')):.2f}\n"
                f"💵 <b>Balance:</b> ${safe_float(stats.get('current_balance')):.2f}\n"
                f"📈 <b>Realized PnL:</b> ${safe_float(stats.get('total_pnl')):.2f}\n"
                f"📊 <b>Total PnL:</b> ${safe_float(stats.get('computed_total_pnl', stats.get('total_pnl'))):.2f} "
                f"({safe_float(stats.get('computed_pnl_percent', 0)):.2f}%)\n"
                f"📉 <b>Unrealized:</b> ${safe_float(stats.get('unrealized_pnl', 0)):.2f}\n"
                f"🏆 <b>Win Rate:</b> {analytics.get('win_rate', 0.0):.1f}% "
                f"({analytics.get('wins', 0)}W / {analytics.get('losses', 0)}L)\n"
                f"📐 <b>Profit Factor:</b> {pf_display}\n"
                f"🆕 <b>Entries:</b> {int(stats.get('entries_count', 0))}/{Config.MAX_DAILY_TRADES}\n"
                f"🔄 <b>Closes:</b> {int(stats.get('trades_count', 0))}\n"
                f"⚙️ <b>Status:</b> {escape_html(str(stats.get('status', 'UNKNOWN')))}"
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
            balance = self.exchange.get_futures_balance(force_refresh=True)
            self.bot.reply_to(message, f"💵 <b>Available balance:</b> ${balance:.2f}")

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
                "/closeall — close all open positions\n"
                "/stop — safe bot shutdown\n"
                "/restart — graceful bot restart\n"
                "/errors — recent critical errors\n"
                "/balance — live futures balance\n"
                "/ping — bot health\n"
                "/help — this message",
            )

    def _authorized(self, message: telebot.types.Message) -> bool:
        return str(message.chat.id) == self.chat_id

    def _credentials_valid(self) -> bool:
        if self.token in TELEGRAM_PLACEHOLDERS or self.chat_id in TELEGRAM_PLACEHOLDERS:
            return False
        return bool(self.token and self.chat_id)
