"""
Telegram notification and command module.
HTML-safe messaging, background polling, and authorized chat commands.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Optional

import telebot

from config import Config
from database import DatabaseManager
from logger import error_logger, system_logger
from utils import escape_html, safe_float, utc_today_str

if TYPE_CHECKING:
    from exchange import BinanceExchangeManager
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
    ) -> None:
        self.db = db
        self.scheduler = scheduler
        self.risk_manager = risk_manager
        self.exchange = exchange

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

    # ---------------- Command handlers ----------------

    def _register_handlers(self) -> None:
        assert self.bot is not None

        @self.bot.message_handler(commands=["ping"])
        def ping_handler(message: telebot.types.Message) -> None:
            if not self._authorized(message):
                return
            mode = "TESTNET" if Config.USE_TESTNET else "MAINNET"
            self.bot.reply_to(
                message,
                f"🟢 <b>Bot online</b> — actively monitoring markets ({escape_html(mode)}).",
            )

        @self.bot.message_handler(commands=["status"])
        def status_handler(message: telebot.types.Message) -> None:
            if not self._authorized(message):
                return
            today = utc_today_str()
            stats = (
                self.scheduler.get_today_stats()
                if self.scheduler
                else self.db.get_daily_stats(today)
            )
            if not stats:
                self.bot.reply_to(message, "⚠️ No daily stats recorded yet today.")
                return

            msg = (
                f"📊 <b>Daily Status ({escape_html(today)})</b>\n\n"
                f"💰 <b>Reference:</b> ${safe_float(stats.get('start_balance')):.2f}\n"
                f"💵 <b>Balance:</b> ${safe_float(stats.get('current_balance')):.2f}\n"
                f"📈 <b>Realized PnL:</b> ${safe_float(stats.get('total_pnl')):.2f}\n"
                f"📊 <b>Total PnL:</b> ${safe_float(stats.get('computed_total_pnl', stats.get('total_pnl'))):.2f} "
                f"({safe_float(stats.get('computed_pnl_percent', 0)):.2f}%)\n"
                f"📉 <b>Unrealized:</b> ${safe_float(stats.get('unrealized_pnl', 0)):.2f}\n"
                f"🆕 <b>Entries:</b> {int(stats.get('entries_count', 0))}\n"
                f"🔄 <b>Closes:</b> {int(stats.get('trades_count', 0))}\n"
                f"⚙️ <b>Status:</b> {escape_html(str(stats.get('status', 'UNKNOWN')))}"
            )
            self.bot.reply_to(message, msg)

        @self.bot.message_handler(commands=["risk"])
        def risk_handler(message: telebot.types.Message) -> None:
            if not self._authorized(message):
                return
            if not self.risk_manager:
                self.bot.reply_to(message, "Risk manager not attached.")
                return

            snap = self.risk_manager.get_risk_snapshot()
            msg = (
                "🛡 <b>Risk Snapshot</b>\n\n"
                f"📂 <b>Exchange open:</b> {snap.exchange_open_positions}/{Config.MAX_POSITIONS}\n"
                f"🆕 <b>Daily entries:</b> {snap.daily_entries}/{Config.MAX_DAILY_TRADES}\n"
                f"✅ <b>Daily closes:</b> {snap.daily_trades}\n"
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
        def positions_handler(message: telebot.types.Message) -> None:
            if not self._authorized(message):
                return
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

        @self.bot.message_handler(commands=["balance"])
        def balance_handler(message: telebot.types.Message) -> None:
            if not self._authorized(message):
                return
            if not self.exchange:
                self.bot.reply_to(message, "Exchange not attached.")
                return
            balance = self.exchange.get_futures_balance(force_refresh=True)
            self.bot.reply_to(message, f"💵 <b>Available balance:</b> ${balance:.2f}")

        @self.bot.message_handler(commands=["help"])
        def help_handler(message: telebot.types.Message) -> None:
            if not self._authorized(message):
                return
            self.bot.reply_to(
                message,
                "<b>Available commands</b>\n"
                "/ping — bot health\n"
                "/status — daily performance\n"
                "/risk — portfolio risk snapshot\n"
                "/positions — open trades\n"
                "/balance — live futures balance\n"
                "/help — this message",
            )

    def _authorized(self, message: telebot.types.Message) -> bool:
        return str(message.chat.id) == self.chat_id

    def _credentials_valid(self) -> bool:
        if self.token in TELEGRAM_PLACEHOLDERS or self.chat_id in TELEGRAM_PLACEHOLDERS:
            return False
        return bool(self.token and self.chat_id)
