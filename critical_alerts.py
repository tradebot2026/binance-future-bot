"""
Immediate Telegram alerts for critical system failures.
Persists events to SQLite and deduplicates repeated alerts within a cooldown window.
"""

from __future__ import annotations

import traceback
from typing import TYPE_CHECKING, Optional

from config import Config
from logger import error_logger
from utils import utc_now

if TYPE_CHECKING:
    from database import DatabaseManager
    from telegram_bot import TelegramManager


class CriticalAlertService:
    """Send and persist CRITICAL alerts (API disconnect, order failures, unhandled errors)."""

    CATEGORIES = frozenset(
        {
            "API_DISCONNECT",
            "RATE_LIMIT",
            "ORDER_FAILURE",
            "UNHANDLED_EXCEPTION",
            "DATABASE",
            "ORPHAN_FILL",
        }
    )

    def __init__(
        self,
        db: "DatabaseManager",
        telegram: Optional["TelegramManager"] = None,
    ) -> None:
        self.db = db
        self.telegram = telegram
        self._recent_keys: dict[str, float] = {}

    def attach_telegram(self, telegram: "TelegramManager") -> None:
        self.telegram = telegram

    def notify(
        self,
        category: str,
        message: str,
        *,
        exc: Optional[BaseException] = None,
        force: bool = False,
    ) -> None:
        """Log, persist, and Telegram-notify a critical error (with deduplication)."""
        category = category.upper()
        if category not in self.CATEGORIES:
            category = "UNHANDLED_EXCEPTION"

        detail = message
        if exc is not None:
            detail = f"{message} | {type(exc).__name__}: {exc}"

        dedup_key = f"{category}:{detail[:120]}"
        now_mono = utc_now().timestamp()
        last_sent = self._recent_keys.get(dedup_key, 0.0)
        cooldown = float(Config.CRITICAL_ALERT_COOLDOWN_SECONDS)

        if not force and (now_mono - last_sent) < cooldown:
            return

        self._recent_keys[dedup_key] = now_mono
        if len(self._recent_keys) > 200:
            self._recent_keys = dict(list(self._recent_keys.items())[-100:])

        stack = ""
        if exc is not None:
            stack = traceback.format_exc()

        try:
            self.db.log_critical_error(category, detail, stack)
        except Exception as db_exc:
            error_logger.error("Failed to persist critical error: %s", db_exc)

        error_logger.critical("[%s] %s", category, detail)
        if stack:
            error_logger.critical(stack)

        if self.telegram and self.telegram.enabled:
            safe_detail = detail.replace("<", "&lt;").replace(">", "&gt;")
            msg = (
                "🚨 <b>CRITICAL ERROR</b>\n\n"
                f"📂 <b>Category:</b> {category}\n"
                f"📝 <b>Detail:</b> {safe_detail}\n"
                f"🕐 <b>Time (UTC):</b> {utc_now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            self.telegram.send_message(msg)
