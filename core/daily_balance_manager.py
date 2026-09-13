"""
Dynamic daily starting balance — resets at 00:00 UTC using account equity (margin balance).
Persisted to disk so mid-day restarts do not overwrite the day's baseline.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from config import Config
from constants import DAILY_STATUS_ACTIVE
from logger import system_logger
from utils import safe_float, utc_today_str

if TYPE_CHECKING:
    from database import DatabaseManager
    from exchange import BinanceExchangeManager


_STATE_PATH = os.path.join(Config.DATA_DIR, "daily_balance_state.json")


@dataclass
class DailyBalanceState:
    utc_day: str
    daily_starting_balance: float


def _load_state() -> Optional[DailyBalanceState]:
    try:
        if not os.path.isfile(_STATE_PATH):
            return None
        with open(_STATE_PATH, encoding="utf-8") as handle:
            raw = json.load(handle)
        day = str(raw.get("utc_day", "")).strip()
        balance = safe_float(raw.get("daily_starting_balance"))
        if day and balance > 0:
            return DailyBalanceState(day, balance)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        pass
    return None


def _save_state(state: DailyBalanceState) -> None:
    try:
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        tmp = f"{_STATE_PATH}.tmp"
        payload = {
            "utc_day": state.utc_day,
            "daily_starting_balance": state.daily_starting_balance,
        }
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(tmp, _STATE_PATH)
    except OSError as exc:
        system_logger.warning("Failed to persist daily balance state: %s", exc)


def fetch_account_equity(exchange: "BinanceExchangeManager", *, force: bool = False) -> float:
    """Total margin balance (wallet + unrealized) — daily compounding baseline."""
    snap = exchange.fetch_live_account_snapshot(include_today_income=False)
    equity = safe_float(snap.margin_balance)
    if equity <= 0:
        equity = safe_float(snap.wallet_balance)
    if equity <= 0 and force:
        equity = exchange.get_futures_balance(force_refresh=True)
    return equity


def ensure_daily_baseline(
    exchange: "BinanceExchangeManager",
    db: "DatabaseManager",
    *,
    utc_day: Optional[str] = None,
    force_new_day: bool = False,
) -> float:
    """
    Return today's daily_starting_balance.
    - New UTC day: snapshot current equity and persist.
    - Same day restart: reuse persisted baseline (never overwrite mid-day).
    """
    today = utc_day or utc_today_str()

    if force_new_day:
        equity = fetch_account_equity(exchange, force=True)
        if equity <= 0:
            persisted = _load_state()
            if persisted and persisted.daily_starting_balance > 0:
                equity = persisted.daily_starting_balance
        if equity <= 0:
            system_logger.warning(
                "Could not resolve account equity for new day %s.", today
            )
            return 0.0
        _save_state(DailyBalanceState(today, equity))
        db.initialize_daily_stats(today, equity)
        db.update_daily_balance(today, equity, DAILY_STATUS_ACTIVE)
        system_logger.info(
            "UTC day rollover — new daily starting balance for %s: $%.2f",
            today,
            equity,
        )
        return equity

    persisted = _load_state()
    stats = db.get_daily_stats(today)

    if (
        persisted
        and persisted.utc_day == today
        and persisted.daily_starting_balance > 0
    ):
        baseline = persisted.daily_starting_balance
        if not stats:
            db.initialize_daily_stats(today, baseline)
            db.update_daily_balance(today, baseline, DAILY_STATUS_ACTIVE)
        system_logger.debug(
            "Daily baseline restored from disk for %s: $%.2f",
            today,
            baseline,
        )
        return baseline

    if stats and safe_float(stats.get("start_balance")) > 0 and not force_new_day:
        baseline = safe_float(stats.get("start_balance"))
        _save_state(DailyBalanceState(today, baseline))
        return baseline

    equity = fetch_account_equity(exchange, force=True)
    if equity <= 0 and persisted and persisted.daily_starting_balance > 0:
        equity = persisted.daily_starting_balance

    if equity <= 0:
        system_logger.warning(
            "Could not resolve account equity for daily baseline on %s.",
            today,
        )
        return 0.0

    _save_state(DailyBalanceState(today, equity))
    db.initialize_daily_stats(today, equity)
    db.update_daily_balance(today, equity, DAILY_STATUS_ACTIVE)
    system_logger.info(
        "Daily starting balance set for %s: $%.2f (margin/equity snapshot).",
        today,
        equity,
    )
    return equity


def on_utc_day_rollover(
    exchange: "BinanceExchangeManager",
    db: "DatabaseManager",
    new_day: str,
) -> float:
    """Begin a new UTC trading day — baseline = current account equity."""
    return ensure_daily_baseline(
        exchange, db, utc_day=new_day, force_new_day=True
    )
